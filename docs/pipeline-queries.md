# Pipeline Queries in TigerFS

TigerFS supports **composable pipeline queries** that let you chain multiple operations together in a single filesystem path. Instead of writing SQL, you build queries by navigating directories—each step adds a constraint, and the final result reflects all accumulated operations.

**Key benefit:** All filtering, ordering, and limiting operations are pushed down to the database as a single SQL query. This means the database optimizer can use indexes efficiently, and only matching rows are transferred—not the entire table.

## Quick Example

```bash
# Find the last 10 pending orders for customer 123, sorted by date, exported as JSON
cat /mnt/db/orders/.by/customer_id/123/.by/status/pending/.order/created_at/.last/10/.export/json
```

This single path generates one optimized SQL query:
```sql
SELECT * FROM orders
WHERE customer_id = '123' AND status = 'pending'
ORDER BY created_at DESC
LIMIT 10
```

## Available Capabilities

| Capability | Purpose | Example |
|------------|---------|---------|
| `.by/<col>/<val>/` | Filter by indexed column (fast) | `.by/status/active/` |
| `.filter/<col>/<val>/` | Filter by any column | `.filter/notes/urgent/` |
| `.order/<col>/` | Sort results | `.order/created_at/` |
| `.first/N/` | First N rows | `.first/100/` |
| `.last/N/` | Last N rows | `.last/100/` |
| `.sample/N/` | Random sample of N rows | `.sample/50/` |
| `.export/<fmt>` | Export as csv, tsv, or json | `.export/json` |

## Chaining Rules

Capabilities can be chained in many combinations. Each step operates on the result of the previous step.

### What's Allowed

```bash
# Multiple filters (AND-combined)
.by/status/active/.by/tier/premium/           # status='active' AND tier='premium'
.by/customer_id/123/.filter/notes/urgent/     # Mix indexed + non-indexed filters

# Filter then paginate
.by/status/pending/.first/100/                # First 100 pending items
.filter/category/books/.last/50/              # Last 50 books

# Filter, order, then paginate
.by/customer_id/123/.order/amount/.last/10/   # Customer's 10 largest orders

# Nested pagination (operates on previous result)
.first/100/.last/50/                          # Rows 51-100 (last 50 of first 100)
.last/1000/.first/100/                        # First 100 of the last 1000
.first/1000/.sample/50/                       # Random 50 from first 1000

# Post-limit operations
.first/100/.filter/status/active/             # Filter within the first 100
.sample/50/.order/name/                       # Sort the sampled rows

# Full pipeline with export
.by/customer_id/123/.order/date/.last/10/.export/json
```

### What's Disallowed (Redundant)

```bash
.first/100/.first/50/      # Just use .first/50/
.last/100/.last/50/        # Just use .last/50/
.sample/50/.sample/25/     # Just use .sample/25/
.order/a/.order/b/         # Second order replaces first; just use .order/b/
.export/csv/.first/10/     # Export is terminal—nothing after it
```

## `.by/` vs `.filter/`

Both filter rows, but with different tradeoffs:

| Aspect | `.by/` | `.filter/` |
|--------|--------|------------|
| **Columns shown** | Only indexed columns | All columns |
| **Value listing** | Fast (index scan) | May be slow or unavailable |
| **Performance** | Guaranteed fast | May scan full table |
| **Use case** | Efficient lookups | Ad-hoc filtering |

### When to Use Each

**Use `.by/`** when:
- The column has an index
- You want fast, predictable performance
- You're filtering on common lookup columns (status, customer_id, etc.)

**Use `.filter/`** when:
- The column isn't indexed
- You need ad-hoc filtering on any column
- You know the value you want (don't need to browse values)

### Large Table Safety

For `.filter/` on non-indexed columns, TigerFS protects against slow queries:

```bash
$ ls /mnt/db/large_events/.filter/type/
.table-too-large    # Indicates value listing unavailable
```

Direct access still works—just type the value:
```bash
cat /mnt/db/large_events/.filter/type/click/.first/100/.export/json
```

**Configuration:**
- `DirFilterLimit`: Tables larger than this skip value listing (default: 100,000 rows)
- `QueryTimeout`: Maximum query time before failure (default: 30 seconds)

## Ordering

The `.order/<col>/` capability sorts results:

```bash
# Sort by column (ascending by default)
ls /mnt/db/users/.order/name/.first/50/

# After ordering, use .first for ascending, .last for descending
ls /mnt/db/products/.order/price/.first/10/   # 10 cheapest
ls /mnt/db/products/.order/price/.last/10/    # 10 most expensive
```

**Important:** Once you add `.order/`, you can only add `.first/`, `.last/`, `.sample/`, or `.export/` next. Add all filters before ordering.

## Pagination Semantics

### Single-Level Pagination

```bash
.first/100/     # First 100 rows (by primary key, ascending)
.last/100/      # Last 100 rows (by primary key, descending)
.sample/100/    # Random 100 rows
```

### Nested Pagination

Each pagination step operates on the previous result:

| Path | Result |
|------|--------|
| `.first/100/.last/50/` | Last 50 of first 100 = rows 51-100 |
| `.last/100/.first/50/` | First 50 of last 100 = rows (n-99) to (n-50) |
| `.first/1000/.sample/50/` | Random 50 from first 1000 |
| `.last/500/.sample/25/` | Random 25 from last 500 |

### Post-Pagination Filtering

You can filter or order after pagination:

```bash
# Get first 100, then keep only active ones
.first/100/.filter/status/active/

# Sample 50, then sort them
.sample/50/.order/name/

# Get last 1000, filter, then take first 100 of those
.last/1000/.filter/priority/high/.first/100/
```

## Export Formats

The `.export/` capability produces a file containing all matching rows:

```bash
cat /mnt/db/users/.first/100/.export/csv    # CSV with headers
cat /mnt/db/users/.first/100/.export/tsv    # TSV with headers
cat /mnt/db/users/.first/100/.export/json   # JSON array
```

**Export is terminal**—nothing can follow `.export/`.

## Composite Indexes

For tables with multi-column indexes, two syntaxes are supported:

```bash
# Sequential (two separate .by/ filters)
.by/last_name/Smith/.by/first_name/John/

# Composite (single lookup matching index order)
.by/last_name.first_name/Smith.John/
```

The composite syntax (`.by/col1.col2/val1.val2/`) is more efficient when it matches an existing composite index.

## SQL Generation

TigerFS generates optimized SQL for each pipeline. Simple pipelines produce flat queries:

```bash
# Path: .by/status/active/.order/name/.first/100/
# SQL:  SELECT * FROM t WHERE status = 'active' ORDER BY name LIMIT 100
```

Nested pagination uses subqueries:

```bash
# Path: .first/100/.last/50/
# SQL:  SELECT * FROM (SELECT * FROM t ORDER BY pk LIMIT 100) sub
#       ORDER BY pk DESC LIMIT 50

# Path: .first/1000/.sample/50/
# SQL:  SELECT * FROM (SELECT * FROM t ORDER BY pk LIMIT 1000) sub
#       ORDER BY RANDOM() LIMIT 50
```

## Performance Considerations

1. **Use `.by/` for indexed columns** - Guarantees index usage
2. **Filter early** - Add filters before pagination to reduce data
3. **Avoid large `.filter/` scans** - Use `.by/` when possible
4. **Mind the timeout** - Queries exceeding `QueryTimeout` return EIO
5. **Export wisely** - Large exports still transfer all data

## Discovering Capabilities

Use `ls` to see available capabilities at each level:

```bash
$ ls /mnt/db/orders/
.by/  .filter/  .first/  .last/  .sample/  .order/  .export/  .info/
1  2  3  4  5  ...

$ ls /mnt/db/orders/.by/
customer_id/  status/  created_at/    # Only indexed columns

$ ls /mnt/db/orders/.by/status/
active/  pending/  completed/  .first/  .last/

$ ls /mnt/db/orders/.by/status/active/
.by/  .filter/  .order/  .first/  .last/  .sample/  .export/
1  2  3  ...                               # Matching row IDs
```

## Examples

### Find Recent High-Value Orders

```bash
# Orders over $1000 from the last 30 days, sorted by amount
cat /mnt/db/orders/.filter/amount_gt/1000/.order/amount/.last/50/.export/json
```

### Customer Order Summary

```bash
# All pending orders for customer 42, most recent first
ls /mnt/db/orders/.by/customer_id/42/.by/status/pending/.order/created_at/.last/100/
```

### Random Sample for Analysis

```bash
# Random 100 active users for A/B test
cat /mnt/db/users/.by/status/active/.sample/100/.export/csv
```

### Paginated Browse

```bash
# Page through large results
ls /mnt/db/events/.first/100/           # Page 1
ls /mnt/db/events/.first/200/.last/100/ # Page 2 (rows 101-200)
ls /mnt/db/events/.first/300/.last/100/ # Page 3 (rows 201-300)
```

### Complex Filter Chain

```bash
# Premium customers in California with recent activity
cat /mnt/db/customers/.by/tier/premium/.by/state/CA/.filter/last_active_gt/2024-01-01/.first/50/.export/json
```

## Limitations

1. **Equality filters only** - No range queries (`.by/age/>/25/`)
2. **Single-column ordering** - No `.order/col1.col2/`
3. **Sample then sort** - `.sample/N/.order/col/` samples first, then sorts the sample
4. **Export is terminal** - Cannot chain after `.export/`
5. **No import pipeline** - `.import/` is separate and not composable

## Configuration

```yaml
# ~/.config/tigerfs/config.yaml
filesystem:
  dir_filter_limit: 100000   # Skip value listing for tables larger than this
  query_timeout: 30s         # Maximum query execution time
```

Environment variables:
```bash
export TIGERFS_DIR_FILTER_LIMIT=100000
export TIGERFS_QUERY_TIMEOUT=30s
```

## See Also

- [ADR-007: Pipeline Query Architecture](adr/007-pipeline-query-architecture.md) - Technical design document
- [spec.md](spec.md) - Complete TigerFS specification
