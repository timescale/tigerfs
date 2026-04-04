# Native Table Access

Access PostgreSQL tables as directories of files — read rows, write columns, navigate indexes, chain pipeline queries, and manage schema, all from the command line.

## What It Does

TigerFS maps every table to a directory. Each row appears as a file (in your choice of format) or as a subdirectory of column files. Primary keys become filenames, columns become file contents, and standard Unix tools replace SQL.

```
/mnt/db/public/users/
├── 1.json                  # Row as JSON file
├── 1.tsv                   # Row as TSV file
├── 1/                      # Row as directory
│   ├── id                  # integer column (no extension)
│   ├── email.txt           # text column
│   ├── metadata.json       # jsonb column
│   └── avatar.bin          # bytea column
├── .by/                    # Index navigation
├── .first/  .last/  .sample/   # Pagination
├── .columns/  .filter/  .order/  .export/ # Pipeline queries
├── .info/                  # Table metadata
├── .create/  .modify/  .delete/ # Schema management
└── .indexes/               # Index metadata
```

## Row Formats

Access any row as a file with different format extensions:

```bash
cat /mnt/db/users/123.tsv     # TSV (tab-separated)
cat /mnt/db/users/123.json    # JSON object
cat /mnt/db/users/123.csv     # CSV with header
cat /mnt/db/users/123.yaml    # YAML
```

Or access individual columns via the row-as-directory view:

```bash
ls /mnt/db/users/123/              # List columns
cat /mnt/db/users/123/email.txt    # Read single column
cat /mnt/db/users/123/.json        # Entire row as JSON (dot-file)
cat /mnt/db/users/123/.csv         # Entire row as CSV
cat /mnt/db/users/123/.yaml        # Entire row as YAML
```

Column files use type-based extensions: `.txt` for text, `.json` for jsonb, `.bin` for bytea, no extension for numeric types.

## Reading Data

```bash
# List tables in default schema
ls /mnt/db/

# List rows in a table (shows primary keys)
ls /mnt/db/users/

# Read entire row as JSON
cat /mnt/db/users/123.json

# Read a single column
cat /mnt/db/users/123/email.txt

# Navigate by index
cat /mnt/db/users/.by/email/foo@example.com.json
```

## Writing Data

All format extensions (`.json`, `.yaml`, `.csv`, `.tsv`) use **PATCH semantics** — only the columns you specify are updated. Omitted columns retain their existing values.

```bash
# Update a column
echo 'new@example.com' > /mnt/db/users/123/email.txt

# Update multiple columns (JSON) — only specified keys are updated
echo '{"email":"new@example.com","name":"New Name"}' > /mnt/db/users/123.json

# Update via TSV — header row specifies which columns
echo -e 'email\tname\nnew@example.com\tNew Name' > /mnt/db/users/123.tsv

# Create new row
mkdir /mnt/db/users/456
echo 'user@example.com' > /mnt/db/users/456/email.txt

# Delete row
rm -r /mnt/db/users/456/
```

## Index Navigation

Navigate tables using indexed columns via `.by/`:

```bash
# List available indexes
ls /mnt/db/users/.by/

# Browse by indexed column
ls /mnt/db/users/.by/email/
cat /mnt/db/users/.by/email/foo@example.com.json

# Ordered access within indexes
ls /mnt/db/users/.by/created_at/.first/10/    # First 10 (ascending)
ls /mnt/db/users/.by/created_at/.last/10/     # Last 10 (descending)

# Pagination within a specific value
ls /mnt/db/users/.by/status/active/.first/50/
```

### Table Pagination

Handle large tables without loading everything:

```bash
ls /mnt/db/events/.first/100/       # First 100 rows by primary key
ls /mnt/db/events/.last/100/        # Last 100 rows
ls /mnt/db/events/.sample/100/      # Random sample
cat /mnt/db/events/.info/count      # Total row count
```

## Pipeline Queries

Chain multiple operations into a single path. TigerFS pushes the entire pipeline down to the database as one optimized SQL query — the database optimizer uses indexes efficiently and only matching rows are transferred.

### Quick Example

```bash
# Find the last 10 pending orders for customer 123, sorted by date, as JSON
cat /mnt/db/orders/.by/customer_id/123/.by/status/pending/.order/created_at/.last/10/.export/json
```

This generates one SQL query:

```sql
SELECT * FROM orders
WHERE customer_id = '123' AND status = 'pending'
ORDER BY created_at DESC
LIMIT 10
```

### Available Capabilities

| Capability | Purpose | Example |
|------------|---------|---------|
| `.by/<col>/<val>/` | Filter by indexed column (fast) | `.by/status/active/` |
| `.filter/<col>/<val>/` | Filter by any column | `.filter/notes/urgent/` |
| `.order/<col>/` | Sort results | `.order/created_at/` |
| `.columns/col1,col2/` | Select specific columns | `.columns/id,name,email/` |
| `.first/N/` | First N rows | `.first/100/` |
| `.last/N/` | Last N rows | `.last/100/` |
| `.sample/N/` | Random sample of N rows | `.sample/50/` |
| `.export/<fmt>` | Export as csv, tsv, or json | `.export/json` |

### Chaining Rules

Capabilities can be chained in many combinations. Each step operates on the result of the previous step.

**What's allowed:**

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

# Column projection (select specific columns)
.columns/id,name,email/.export/csv                                # Select columns then export
.filter/status/active/.columns/id,email/.export/json              # Filter then project

# Full pipeline with export
.by/customer_id/123/.order/date/.last/10/.export/json
```

**What's disallowed (redundant):**

```bash
.first/100/.first/50/      # Just use .first/50/
.last/100/.last/50/        # Just use .last/50/
.sample/50/.sample/25/     # Just use .sample/25/
.order/a/.order/b/         # Second order replaces first; just use .order/b/
.columns/a,b/.columns/c/   # Just use .columns/a,b,c/
.export/csv/.first/10/     # Export is terminal—nothing after it
```

### `.by/` vs `.filter/`

Both filter rows, but with different tradeoffs:

| Aspect | `.by/` | `.filter/` |
|--------|--------|------------|
| **Columns shown** | Only indexed columns | All columns |
| **Value listing** | Fast (index scan) | May be slow or unavailable |
| **Performance** | Guaranteed fast | May scan full table |
| **Use case** | Efficient lookups | Ad-hoc filtering |

**Use `.by/`** when the column has an index and you want fast, predictable performance.

**Use `.filter/`** when the column isn't indexed or you need ad-hoc filtering on any column.

For `.filter/` on non-indexed columns of large tables, TigerFS protects against slow queries:

```bash
$ ls /mnt/db/large_events/.filter/type/
.table-too-large    # Indicates value listing unavailable
```

Direct access still works — just type the value:

```bash
cat /mnt/db/large_events/.filter/type/click/.first/100/.export/json
```

### Column Projection

The `.columns/` capability selects specific columns, producing `SELECT "col1", "col2"` instead of `SELECT *`:

```bash
# Export only id, name, and email
cat /mnt/db/users/.columns/id,name,email/.export/csv

# Combine with filters
cat /mnt/db/orders/.filter/status/shipped/.columns/id,total,created_at/.export/json

# Browse available columns
ls /mnt/db/users/.columns/
```

**Important:** After `.columns/`, only `.export/` is available. Add all filters and ordering before column selection.

### Ordering

The `.order/<col>/` capability sorts results:

```bash
# Sort by column (ascending by default)
ls /mnt/db/users/.order/name/.first/50/

# After ordering, use .first for ascending, .last for descending
ls /mnt/db/products/.order/price/.first/10/   # 10 cheapest
ls /mnt/db/products/.order/price/.last/10/    # 10 most expensive
```

**Important:** Once you add `.order/`, you can only add `.first/`, `.last/`, `.sample/`, or `.export/` next. Add all filters before ordering.

### Pagination Semantics

**Single-level:**

```bash
.first/100/     # First 100 rows (by primary key, ascending)
.last/100/      # Last 100 rows (by primary key, descending)
.sample/100/    # Random 100 rows
```

**Nested pagination** — each step operates on the previous result:

| Path | Result |
|------|--------|
| `.first/100/.last/50/` | Last 50 of first 100 = rows 51-100 |
| `.last/100/.first/50/` | First 50 of last 100 = rows (n-99) to (n-50) |
| `.first/1000/.sample/50/` | Random 50 from first 1000 |
| `.last/500/.sample/25/` | Random 25 from last 500 |

**Post-pagination filtering:**

```bash
.first/100/.filter/status/active/              # Get first 100, keep only active
.sample/50/.order/name/                        # Sample 50, then sort them
.last/1000/.filter/priority/high/.first/100/   # Last 1000, filter, take first 100
```

### Export Formats

The `.export/` capability produces a file containing all matching rows:

```bash
cat /mnt/db/users/.first/100/.export/csv    # CSV with headers
cat /mnt/db/users/.first/100/.export/tsv    # TSV with headers
cat /mnt/db/users/.first/100/.export/json   # JSON array
```

**Export is terminal** — nothing can follow `.export/`.

### Composite Indexes

For tables with multi-column indexes, two syntaxes are supported:

```bash
# Sequential (two separate .by/ filters)
.by/last_name/Smith/.by/first_name/John/

# Composite (single lookup matching index order)
.by/last_name.first_name/Smith.John/
```

The composite syntax is more efficient when it matches an existing composite index.

### SQL Generation

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

### Discovering Capabilities

Use `ls` to see available capabilities at each level:

```bash
$ ls /mnt/db/orders/
.by/  .columns/  .filter/  .first/  .last/  .sample/  .order/  .export/  .info/
1  2  3  4  5  ...

$ ls /mnt/db/orders/.by/
customer_id/  status/  created_at/    # Only indexed columns

$ ls /mnt/db/orders/.by/status/
active/  pending/  completed/  .first/  .last/

$ ls /mnt/db/orders/.by/status/active/
.by/  .columns/  .filter/  .order/  .first/  .last/  .sample/  .export/
1  2  3  ...                               # Matching row IDs
```

### Examples

```bash
# Orders over $1000 from the last 30 days, sorted by amount
cat /mnt/db/orders/.filter/amount_gt/1000/.order/amount/.last/50/.export/json

# All pending orders for customer 42, most recent first
ls /mnt/db/orders/.by/customer_id/42/.by/status/pending/.order/created_at/.last/100/

# Random 100 active users for A/B test
cat /mnt/db/users/.by/status/active/.sample/100/.export/csv

# Page through large results
ls /mnt/db/events/.first/100/           # Page 1
ls /mnt/db/events/.first/200/.last/100/ # Page 2 (rows 101-200)
ls /mnt/db/events/.first/300/.last/100/ # Page 3 (rows 201-300)

# Premium customers in California with recent activity
cat /mnt/db/customers/.by/tier/premium/.by/state/CA/.filter/last_active_gt/2024-01-01/.first/50/.export/json
```

### Limitations

1. **Equality filters only** — no range queries (`.by/age/>/25/`)
2. **Single-column ordering** — no `.order/col1.col2/`
3. **Sample then sort** — `.sample/N/.order/col/` samples first, then sorts the sample
4. **Export is terminal** — cannot chain after `.export/`
5. **Columns is near-terminal** — after `.columns/`, only `.export/` is available
6. **No import pipeline** — `.import/` is separate and not composable
7. **Text PK format extension clash** — text PK values ending in `.json`, `.csv`, `.tsv`, or `.yaml` are misinterpreted as format extensions

## Table Metadata

Inspect table structure without querying the database directly:

```bash
cat /mnt/db/users/.info/ddl         # Full CREATE TABLE statement
cat /mnt/db/users/.info/schema      # Column details (name, type, nullable)
cat /mnt/db/users/.info/columns     # Column names (one per line)
cat /mnt/db/users/.info/count       # Total row count
ls /mnt/db/users/.indexes/          # Index names
```

## Schema Management

TigerFS supports creating, modifying, and deleting database tables, indexes, and other objects via a unified staging pattern.

### Staging Pattern

All DDL operations follow the same workflow:

| Step | Action | Effect |
|------|--------|--------|
| 1 | Read `sql` | See template with context (current schema, examples) |
| 2 | Write `sql` | Stage your DDL (stored in memory) |
| 3 | Touch `.test` | Validate via BEGIN/ROLLBACK (optional) |
| 3b | Read `test.log` | See validation result (optional) |
| 4 | Touch `.commit` | Execute DDL |
| — | Touch `.abort` | Cancel and clear staging |

### Operations

| Object | Create | Modify | Delete |
|--------|--------|--------|--------|
| Table | `.create/<name>/` | `<table>/.modify/` | `<table>/.delete/` |
| Index | `<table>/.indexes/.create/<idx>/` | — | `<table>/.indexes/<idx>/.delete/` |
| Schema | `.schemas/.create/<name>/` | — | `.schemas/<name>/.delete/` |
| View | `.views/.create/<name>/` | — | `.views/<name>/.delete/` |

### Human Workflow (Interactive)

Templates help you write correct DDL:

```bash
mkdir /mnt/db/.create/orders           # Create staging directory
cat /mnt/db/.create/orders/sql         # See template with hints
vi /mnt/db/.create/orders/sql          # Edit template
touch /mnt/db/.create/orders/.test     # Validate (optional)
cat /mnt/db/.create/orders/test.log    # See validation result
touch /mnt/db/.create/orders/.commit   # Execute
```

### Script Workflow (Programmatic)

```bash
# Create table
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (id serial PRIMARY KEY, name text)" > /mnt/db/.create/orders/sql
touch /mnt/db/.create/orders/.commit

# Modify table
echo "ALTER TABLE users ADD COLUMN status text" > /mnt/db/users/.modify/sql
touch /mnt/db/users/.modify/.commit

# Delete table (template shows row count and foreign keys)
cat /mnt/db/users/.delete/sql          # Review impact
echo "DROP TABLE users CASCADE" > /mnt/db/users/.delete/sql
touch /mnt/db/users/.delete/.commit

# Create index
mkdir /mnt/db/users/.indexes/.create/email_idx && echo "CREATE INDEX email_idx ON users(email)" > /mnt/db/users/.indexes/.create/email_idx/sql
touch /mnt/db/users/.indexes/.create/email_idx/.commit

# Create view
mkdir /mnt/db/.views/.create/active_users && echo "CREATE VIEW active_users AS SELECT * FROM users WHERE active" > /mnt/db/.views/.create/active_users/sql
touch /mnt/db/.views/.create/active_users/.commit
```

## Configuration

```yaml
# ~/.config/tigerfs/config.yaml
filesystem:
  dir_listing_limit: 10000   # Max rows in directory listing
  dir_filter_limit: 100000   # Skip value listing for tables larger than this
  query_timeout: 30s         # Maximum query execution time
  no_filename_extensions: false  # Set true to disable .txt/.json/.bin extensions
```

Environment variables:

```bash
export TIGERFS_DIR_LISTING_LIMIT=50000
export TIGERFS_DIR_FILTER_LIMIT=100000
export TIGERFS_QUERY_TIMEOUT=30s
export TIGERFS_NO_FILENAME_EXTENSIONS=true
```

## See Also

- [Pipeline Query Architecture (ADR-007)](adr/007-pipeline-query-architecture.md) — technical design document
- [spec.md](spec.md) — complete TigerFS specification
