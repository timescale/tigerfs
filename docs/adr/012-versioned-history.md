# ADR-012: Versioned History

**Status:** Accepted
**Date:** 2026-02-12
**Author:** Mike Freedman

## Context

TigerFS synthesized apps (markdown, plain text) present database rows as files. When
multiple agents share a directory of skills or memory files, one agent can overwrite
another's work with no way to recover short of database snapshots.

Users need a lightweight, filesystem-accessible versioning mechanism that:
- Captures every edit and delete automatically
- Exposes past versions as readable files (for diff, recovery)
- Is composable — can be added to any synth app, at creation or later
- Uses TimescaleDB hypertables for efficient time-partitioned storage

## Decision

### Companion History Table

Each history-enabled synth app gets a companion table (e.g., `_memory_history` for
`_memory`) that mirrors the source table's columns plus history metadata. A PostgreSQL
BEFORE UPDATE/DELETE trigger copies the OLD row to the history table on every change.

The history table is a **TimescaleDB hypertable** partitioned by a UUIDv7 primary key
(`_history_id`), which embeds a timestamp for natural time-ordering and chunk partitioning.

### Activation

**At creation:**
```bash
echo "markdown,history" > /mnt/db/.build/memory
```

**Add to existing app:**
```bash
echo "history" > /mnt/db/.build/memory
```

Both paths store the feature flag in the view comment: `COMMENT ON VIEW memory IS 'tigerfs:md,history'`.

### Filesystem Layout

```
memory/
├── foo.md                          ← current version (from _memory)
├── .history/
│   ├── foo.md/
│   │   ├── .id                     ← row UUID (cat to discover)
│   │   ├── 2026-02-12T013000Z     ← past version
│   │   └── 2026-02-12T021500Z
│   └── .by/                        ← lookup by row UUID (across renames)
│       └── a1b2c3d4-..../
│           ├── 2026-02-12T013000Z
│           └── 2026-02-12T021500Z
```

`.history/` is **read-only** (mode 0555/0444). Writes return EACCES.

**UUID discovery:** `cat memory/.history/foo.md/.id` returns the row's UUID.

**Cross-rename tracking:** `ls memory/.history/.by/<uuid>/` shows all versions of a row
regardless of filename changes. Uses the row's `id` column as stable identity.

### Detection

1. **View comment** (primary): `tigerfs:md,history` parsed for `,history` flag
2. **Companion table** (fallback): if `_<viewName>_history` table exists

### History Table Schema

```sql
CREATE TABLE _memory_history (
    -- Mirrored columns from source (no UNIQUE on filename)
    id UUID, filename TEXT NOT NULL, title TEXT, author TEXT,
    headers JSONB, body TEXT, created_at TIMESTAMPTZ, modified_at TIMESTAMPTZ,
    -- History metadata
    _history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    _operation TEXT NOT NULL  -- 'update' or 'delete'
);
```

Hypertable: partitioned on `_history_id` (UUIDv7), 1-month chunks, compressed after 1 day,
`segment_by='filename'`, `order_by='_history_id DESC'`.

### Trigger

BEFORE UPDATE OR DELETE on source table. Copies OLD.* plus `uuidv7()` and `TG_OP` to
history table. Returns OLD (delete) or NEW (update).

### Version IDs

Timestamp extracted from UUIDv7, formatted as `2006-01-02T150405Z` (filesystem-safe,
no colons). Lookup uses timestamp range matching (second precision → millisecond UUIDv7).

## Consequences

### Benefits
1. **Zero-config versioning** — trigger captures changes automatically
2. **Composable** — works with any synth format (markdown, text, future)
3. **Filesystem-native recovery** — `diff`, `cat`, `cp` from `.history/`
4. **TimescaleDB optimized** — compression, time-partitioned chunks, retention policies
5. **Add/remove anytime** — companion table is independent of source

### Limitations
1. **TimescaleDB required** — won't work on vanilla PostgreSQL
2. **Storage overhead** — full row copy per change (mitigated by compression)
3. **No branch/merge** — linear history only
4. **Second-precision version IDs** — sub-second edits may collide in filesystem paths

### Tradeoffs

| Decision | Benefit | Cost |
|----------|---------|------|
| Separate history table | Existing synth code unchanged | Two tables per app |
| UUIDv7 partition key | Single-column PK, natural time ordering | Requires pg18+ or extension |
| Trigger-based capture | Transparent, no app code changes | Trigger overhead per write |
| Read-only .history/ | Data integrity, simple mental model | Can't "restore" via filesystem write |
| TimescaleDB required | Compression, retention, hypertable benefits | Hard dependency |
