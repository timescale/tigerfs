# ADR-007: Pipeline Query Architecture

**Status:** Accepted
**Date:** 2026-01-30
**Author:** Mike Freedman

## Context

TigerFS exposes PostgreSQL databases as mountable filesystems. Users interact with tables, rows, and columns using Unix tools. The current implementation provides several query capabilities:

- `.by/<col>/<val>/` - Filter by indexed column value
- `.filter/<col>/<val>/` - Filter by any column value (planned)
- `.order/<col>/` - Order results by column
- `.first/N/` - Limit to first N rows
- `.last/N/` - Limit to last N rows
- `.sample/N/` - Random sample of N rows
- `.export/<format>` - Export in CSV, TSV, or JSON format

These capabilities exist as separate node types with limited composition. Some nesting exists (e.g., `.by/<col>/<val>/.first/N/`), but capabilities are not exposed as children of each other in a general way. Users cannot build complex queries like:

```
.filter/status/active/.first/1000/.sample/50/.export/csv
```

## Decision

Implement a **Pipeline Query Architecture** that enables composable capability chaining. Each capability adds its constraint to a cumulative query context and exposes valid next capabilities based on semantic rules.

### Core Design: PipelineContext

A `PipelineContext` struct accumulates query state as users navigate deeper into the path:

```go
type PipelineContext struct {
    Schema    string
    TableName string
    PKColumn  string

    // Filters (from .by and .filter)
    Filters   []FilterCondition  // AND-combined

    // Ordering (from .order)
    OrderBy   string
    OrderDesc bool

    // Limit (from .first, .last, .sample)
    Limit     int
    LimitType LimitType  // None, First, Last, Sample

    // For nested limit operations (subquery needed)
    PreviousLimit     int
    PreviousLimitType LimitType
}

type FilterCondition struct {
    Column  string
    Value   string
    Indexed bool  // true = from .by/, false = from .filter/
}

type LimitType int
const (
    LimitNone LimitType = iota
    LimitFirst
    LimitLast
    LimitSample
)
```

### Capability Exposure Rules (Permissive Model)

The system uses a **permissive model** that allows all semantically meaningful combinations, disallowing only redundant operations.

#### Capability Matrix

| Parent ↓ / Child → | `.by/` | `.filter/` | `.order/` | `.first/` | `.last/` | `.sample/` | `.export/` |
|--------------------|:------:|:----------:|:---------:|:---------:|:--------:|:----------:|:----------:|
| **Table root**           | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **`.by/<col>/<val>/`**   | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **`.filter/<col>/<val>/`** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **`.order/<col>/`**      | ⛔ | ⛔ | ⛔ | ✅ | ✅ | ✅ | ✅ |
| **`.first/N/`**          | ✅ | ✅ | ✅ | ⛔ | ✅ | ✅ | ✅ |
| **`.last/N/`**           | ✅ | ✅ | ✅ | ✅ | ⛔ | ✅ | ✅ |
| **`.sample/N/`**         | ✅ | ✅ | ✅ | ⛔ | ⛔ | ⛔ | ✅ |
| **`.export/<fmt>`**      | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ |

**Legend:** ✅ = Allowed, ⛔ = Disallowed

#### Disallow Rationale

| Parent | Disallowed Children | Rationale |
|--------|---------------------|-----------|
| `.order/<col>/` | `.by/`, `.filter/`, `.order/` | Filter before ordering; second order is redundant |
| `.first/N/` | `.first/` | No double-first (redundant—just use smaller N) |
| `.last/N/` | `.last/` | No double-last (redundant—just use smaller N) |
| `.sample/N/` | `.first/`, `.last/`, `.sample/` | No limit after sample (just sample fewer) |
| `.export/<fmt>` | Everything | Terminal—export produces files, not directories |

### Allowed Combinations

```
# Filter chains
.by/status/active/.by/tier/premium/              # AND-combined indexed filters
.filter/name/Alice/.filter/role/admin/           # AND-combined non-indexed filters
.by/customer_id/123/.filter/notes/urgent/        # Mixed indexed + non-indexed

# Composite index lookup
.by/last_name.first_name/Smith.John/             # Single composite lookup

# Nested pagination
.first/100/.last/50/                             # Rows 51-100 (OFFSET 50 LIMIT 50)
.last/100/.first/50/                             # Rows (n-99) to (n-50)
.first/1000/.sample/50/                          # Sample 50 from first 1000
.last/1000/.sample/50/                           # Sample 50 from last 1000

# Post-limit operations
.first/100/.filter/status/active/                # Filter the first 100
.last/100/.order/name/                           # Sort the last 100
.sample/50/.order/name/                          # Sort the sampled 50
.sample/50/.filter/status/active/                # Filter the sampled 50

# Full pipelines
.by/customer_id/123/.order/created_at/.last/10/.export/json
.filter/status/active/.first/1000/.sample/50/.export/csv
.by/a/1/.by/b/2/.first/100/.last/50/.export/json
```

### Disallowed (Redundant)

```
.first/100/.first/50/      # Use .first/50/
.last/100/.last/50/        # Use .last/50/
.sample/50/.sample/25/     # Use .sample/25/
.sample/50/.first/25/      # Use .sample/25/
.sample/50/.last/25/       # Use .sample/25/
.order/a/.order/b/         # Use .order/b/
.export/csv/.anything/     # Export is terminal
```

### `.by/` vs `.filter/` Distinction

| Capability | Shows Columns | Value Listing | Performance | Use Case |
|------------|---------------|---------------|-------------|----------|
| `.by/` | Indexed columns only | Fast (index scan) | Guaranteed fast | Efficient lookups |
| `.filter/` | All columns | May be slow/limited | May scan table | Ad-hoc filtering |

### `.filter/` Safety Mechanisms

**Problem:** `SELECT DISTINCT col FROM table` requires full table scan even for low-cardinality columns.

**Solution:** Index-aware + size-based value listing:

| Column Type | Table Size | Value Listing Behavior |
|-------------|------------|------------------------|
| Indexed | Any | DISTINCT query (fast index scan) |
| Non-indexed | < DirFilterLimit | DISTINCT query (respects timeout) |
| Non-indexed | >= DirFilterLimit | No listing, show `.table-too-large` indicator |

**Configuration:**
- `DirFilterLimit`: Threshold for large tables (default: 100,000 rows)
- `QueryTimeout`: Global statement timeout (default: 30 seconds)

**Large Table Indicator:**
```bash
$ ls /mnt/db/events/.filter/type/
.table-too-large    # Indicates no value listing available
```

Direct path access still works:
```bash
cat /mnt/db/events/.filter/type/click/.first/100/.export/json  # Works
```

### Composite Index Support

Both sequential and composite syntax supported:
- `.by/last_name/Smith/.by/first_name/John/` - Two separate filters, AND-combined
- `.by/last_name.first_name/Smith.John/` - Single composite lookup

Composite syntax rules:
- Column names joined with `.`: `.by/col1.col2.col3/`
- Values joined with `.`: `.../val1.val2.val3/`
- Must match a composite index exactly (in order)
- Fallback: If no composite index, decompose into separate filters

### SQL Generation

Simple cases use flat queries:
```sql
-- .filter/status/active/.order/name/.first/100/
SELECT * FROM t WHERE status = 'active' ORDER BY name LIMIT 100
```

Nested limit operations use subqueries:
```sql
-- .first/100/.last/50/
SELECT * FROM (
    SELECT * FROM t ORDER BY pk LIMIT 100
) sub ORDER BY pk DESC LIMIT 50

-- .first/1000/.sample/50/
SELECT * FROM (
    SELECT * FROM t ORDER BY pk LIMIT 1000
) sub ORDER BY RANDOM() LIMIT 50

-- .first/100/.filter/status/active/
SELECT * FROM (
    SELECT * FROM t ORDER BY pk LIMIT 100
) sub WHERE status = 'active'
```

### Global Query Timeout

- All database queries have statement timeout (default: 30 seconds)
- Configurable via `TIGERFS_QUERY_TIMEOUT`, config file, or `--query-timeout` flag
- Returns `EIO` on timeout with descriptive log message
- Protects against any slow query, not just DISTINCT

## Consequences

### Benefits

1. **Composability**: Users can build complex queries using familiar filesystem navigation
2. **Safety**: Large table protections prevent filesystem hangs
3. **Flexibility**: Both indexed (fast) and non-indexed (flexible) filtering
4. **Discoverability**: `ls` shows available capabilities at each level
5. **Backwards Compatible**: Existing paths continue to work

### Limitations

1. **Single-column ordering**: No `.order/col1.col2/` multi-column sort initially
2. **Equality filters only**: No `.by/age/>/25/` range syntax
3. **Sample semantics**: `.sample/N/.order/col/` samples first, then sorts (not "N sorted rows at random")
4. **Large exports**: Still subject to safety checks
5. **No import pipeline**: `.import/` remains separate (not composable)
6. **Composite index order**: Must match index column order exactly

### Tradeoffs

| Decision | Benefit | Cost |
|----------|---------|------|
| Permissive model | Maximum flexibility | More complex SQL generation |
| Subqueries for nested limits | Correct semantics | Potential performance overhead |
| Export is terminal | Clear mental model | Can't filter after export |
| `.by/` indexed only | Guaranteed fast | Users must know what's indexed |
| `.filter/` any column | Flexible querying | May be slow on large tables |
| Global query timeout | Prevents filesystem hangs | Long queries fail |

## Implementation

See `docs/implementation-tasks.md` Phase 5: Pipeline Query Architecture for detailed implementation tasks.

### Files to Create
- `internal/tigerfs/fuse/pipeline.go` - PipelineContext and PipelineNode
- `internal/tigerfs/fuse/filter.go` - FilterDirNode with safety mechanisms
- `internal/tigerfs/fuse/pipeline_test.go` - Unit tests

### Files to Modify
- `internal/tigerfs/fuse/pagination.go` - Add pipeline capability exposure
- `internal/tigerfs/fuse/order.go` - Add pipeline capability exposure
- `internal/tigerfs/fuse/index.go` - Add pipeline capability exposure, composite index support
- `internal/tigerfs/fuse/table.go` - Route through PipelineNode, expose `.filter/`
- `internal/tigerfs/db/interfaces.go` - Add PipelineReader interface
- `internal/tigerfs/db/query.go` - Add QueryRowsPipeline with subquery support
- `internal/tigerfs/config/config.go` - Add QueryTimeout and DirFilterLimit settings

## Verification

### Unit Tests
- PipelineContext accumulation and cloning
- QueryRowsPipeline SQL generation with various filter/order/limit combos
- Nested pagination SQL generation
- Capability availability rules
- Table size estimation for `.filter/` safety
- Composite index detection and parsing

### Integration Tests
```bash
# .by/ filter + limit (indexed)
ls /mnt/db/users/.by/status/active/.first/50/

# .filter/ on small table (non-indexed)
ls /mnt/db/users/.filter/name/Alice/.first/10/

# .filter/ on large table (type value directly)
ls /mnt/db/events/.filter/type/           # Shows .table-too-large
cat /mnt/db/events/.filter/type/click/.first/100/.export/json

# Mixed .by/ and .filter/
ls /mnt/db/orders/.by/customer_id/123/.filter/notes/urgent/.first/20/

# Composite index lookup
ls /mnt/db/users/.by/last_name.first_name/Smith.John/

# Nested pagination
ls /mnt/db/users/.first/100/.last/50/     # Rows 51-100
ls /mnt/db/users/.last/100/.first/50/     # Rows (n-100) to (n-50)
ls /mnt/db/users/.first/1000/.sample/50/  # Sample from first 1000

# Full pipelines
cat /mnt/db/users/.first/100/.export/csv
cat /mnt/db/orders/.by/customer_id/123/.by/status/pending/.order/created_at/.last/10/.export/json
cat /mnt/db/products/.sample/100/.order/price/.export/csv
```

### Backward Compatibility
Verify existing paths still work:
- Direct `.export/csv` at table root
- `.first/N/` listing rows
- `.by/<col>/<val>/` filtering
- `.order/<col>/` ordering
