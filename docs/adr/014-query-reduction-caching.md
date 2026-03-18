# ADR-014: Query Reduction Caching Strategy

**Status:** Accepted
**Date:** 2026-02-25
**Author:** Mike Freedman

## Context

Each NFS RPC triggers multiple PostgreSQL queries -- schema resolution, table/view listing, column metadata, primary key lookup, and row data. With go-nfs processing RPCs sequentially and remote databases at 10+ ms latency per round-trip, even a simple `ls` can take hundreds of milliseconds.

For example, `ls -l` on a synth view directory was generating ~37 identical `SELECT * FROM "schema"."table" LIMIT 10000` queries. Each of the 5 visible entries gets Stat'd 6-8 times by NFS (LOOKUP, GETATTR, Apply, re-requests), and each Stat performed 1-2 full table scans.

A singleflight approach was attempted but rejected because go-nfs processes RPCs sequentially -- calls never overlap in time, so there were no concurrent requests to deduplicate.

## Decision

A three-tier caching strategy, where each tier matches the staleness tolerance of the data it caches:

### Tier 1: Schema Resolution (sync.Once -- immutable for mount session)

The current PostgreSQL schema (`current_schema()`) doesn't change during a mount session. Resolved once via `sync.Once` and reused for all subsequent operations.

- **Location:** `Operations.schemaOnce` / `cachedSchema` in `operations.go`
- **Invalidation:** Never (immutable for mount lifetime)
- **Impact:** Eliminates ~4-6 `SELECT current_schema()` queries per NFS RPC

### Tier 2: Metadata Cache (TTL-based -- catalog and structural metadata)

Caches database catalog metadata that changes infrequently: schema/table/view lists, column definitions, primary keys, row count estimates, and permissions.

- **Location:** `MetadataCache` in `metadata_cache.go`
- **Two sub-tiers:**
  - *Catalog* (fast TTL, default 10s): schemas, tables, views -- changes when DDL runs
  - *Structural* (slow TTL, default 5m): row counts, permissions, PKs -- changes rarely
- **Invalidation:** Explicit `Invalidate()` resets both tiers; also refreshes on TTL expiry
- **Impact:** Reduces catalog queries from per-RPC to every ~10s; structural queries to every ~5m

### Tier 3: Synth Stat Cache (ReadDir-primed, 2s TTL)

ReadDir already computes all Entry metadata (name, isDir, size, modtime, mode) that subsequent Stats need. This tier caches those results so Stats return instantly without querying the DB.

- **Location:** `synthStatCache` in `operations.go`, primed from `synth_ops.go`
- **Invalidation:** Write operations (create/update/delete/rename/mkdir) invalidate immediately; 2s TTL as safety net for cross-mount visibility
- **ReadFile bypass:** Always queries DB fresh (content must be current)
- **Impact:** `ls -l` goes from ~37 full table scans to 1 (ReadDir) + 0 (Stats cached)

### Targeted SQL WHERE Queries

Complements the Stat cache for cache-miss cases (`ls filename` without prior ReadDir). Replaces full table scans with indexed lookups:

- `synthRowExists`: `SELECT 1 FROM ... WHERE filename=$1 AND filetype=$2 LIMIT 1`
- `getSynthRow`: `SELECT * FROM ... WHERE filename=$1 LIMIT 1`
- `getSynthRowPKByFiletype`: `SELECT * FROM ... WHERE filename=$1 AND filetype=$2 LIMIT 1`

FS filenames are the raw DB `filename` column values (only 3 chars sanitized: `\`, `\x00`, `:`). A fallback full scan handles the rare case where the filename column is NULL (name derived from PK) or contains sanitized characters.

## Consistency Model

| Data | Cache Tier | Staleness | Refresh Trigger |
|------|-----------|-----------|-----------------|
| Current schema | Tier 1 (Once) | Never stale | Mount restart |
| Table/view lists | Tier 2 (10s TTL) | Up to 10s | TTL expiry, explicit invalidate |
| Row counts, PKs, permissions | Tier 2 (5m TTL) | Up to 5m | TTL expiry, explicit invalidate |
| Synth file metadata (Stat) | Tier 3 (2s TTL) | Up to 2s | Write invalidation, TTL expiry |
| Synth file content (ReadFile) | None | Always fresh | Always queries DB |
| ReadDir results | None | Always fresh | Always queries DB, primes Tier 3 |

This matches NFS semantics where attribute caching is expected. ReadFile and ReadDir always hit the database for authoritative data.

## Performance Impact

| Scenario | Before (uncached) | After |
|----------|-------------------|-------|
| Schema resolution per RPC | 4-6 queries | 0 (cached once) |
| `ls` (table listing) | Catalog queries per RPC | 1 query per 10s |
| `ls -l` synth (5 entries) | ~37 full table scans | 1 ReadDir + 0 Stats |
| `ls filename` (Stat, no prior ReadDir) | 1-2 full table scans | 1-2 targeted WHERE queries |
| `cat filename` | 1-2 full table scans | 1-2 targeted WHERE queries |
| Write then `ls -l` | N/A | Cache invalidated, 1 fresh ReadDir |

## Consequences

### Benefits

1. **Dramatic query reduction**: Simple `ls -l` goes from ~40+ queries to ~1-2
2. **Layered staleness**: Each tier's TTL matches its data's change frequency
3. **Write correctness**: All caches invalidated on writes; reads always fresh

### Costs

1. **Stale metadata windows**: 2s for Stat, 10s for catalog, 5m for structural
2. **Memory**: Bounded by DirListingLimit (Tier 3) and number of tables (Tier 2)
3. **Complexity**: Three cache tiers to reason about

## Alternatives Considered

### Singleflight Deduplication

Deduplicate concurrent `GetAllRows` calls so multiple Stats share one in-flight query.

**Rejected:** go-nfs processes RPCs sequentially. Calls never overlap, so singleflight found nothing to deduplicate.

### Database-Level Caching

Use PostgreSQL's query cache or materialized views.

**Rejected:** Doesn't reduce network round-trips, which are the dominant cost with remote databases.

### No TTL (invalidate-only)

Cache indefinitely, only invalidate on writes.

**Rejected:** Cross-mount writes would never be visible. TTLs balance freshness with performance.

## Implementation

### Tier 1 (Schema)

| File | What |
|------|------|
| `internal/tigerfs/fs/operations.go` | `schemaOnce` / `cachedSchema` fields, resolved in `resolveSchema()` |

### Tier 2 (Metadata)

| File | What |
|------|------|
| `internal/tigerfs/fs/metadata_cache.go` | `MetadataCache` struct with catalog/structural sub-tiers |

### Tier 3 (Synth Stat) + Targeted Queries

| File | What |
|------|------|
| `internal/tigerfs/fs/operations.go` | `synthStatCache` type with `lookup`/`prime`/`invalidate` |
| `internal/tigerfs/fs/synth_ops.go` | Cache prime in ReadDir, cache check in Stat, targeted queries, write invalidation |
| `internal/tigerfs/db/export.go` | `RowExistsByColumns` and `GetRowByColumns` methods |
| `internal/tigerfs/db/interfaces.go` | Added methods to `ExportReader` interface |

## References

- [ADR-008: Synthesized Apps](008-synthesized-apps.md) - Synth view architecture
- [ADR-009: Shared Core Library](009-shared-core-library.md) - NFS backend and Operations layer
