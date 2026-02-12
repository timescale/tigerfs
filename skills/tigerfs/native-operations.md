# Native Operations Reference

Detailed reference for interacting with TigerFS native table access — the row-as-file and row-as-directory interface.

## Path Hierarchy

```
mount/                          # Database root (tables in public schema)
├── table_name/                 # One directory per table
│   ├── .info/                  # Metadata (read-only)
│   │   ├── count               # Total row count
│   │   ├── schema              # CREATE TABLE DDL
│   │   ├── ddl                 # Extended DDL
│   │   ├── columns             # Column names, one per line
│   │   └── indexes             # Index information
│   ├── .by/                    # Index-based lookups
│   │   └── column/value/       # Rows matching index value
│   ├── .first/N/               # First N rows (ascending PK)
│   ├── .last/N/                # Last N rows (descending PK)
│   ├── .sample/N/              # Random N rows
│   ├── .filter/column/value/   # Filter by any column (may scan)
│   ├── .order/column/          # Sort results
│   ├── .export/json|csv|tsv    # Bulk export
│   ├── .import/json|csv|tsv    # Bulk import
│   ├── .all/                   # Escape hatch for large tables (lists all PKs)
│   ├── .indexes/               # Index management (DDL)
│   ├── .modify/                # Table modification (DDL)
│   ├── .delete/                # Table deletion (DDL)
│   ├── pk/                     # Row as directory
│   │   ├── column1             # Individual column value
│   │   ├── column2
│   │   └── ...
│   ├── pk.json                 # Row as JSON
│   ├── pk.csv                  # Row as CSV
│   └── pk.tsv                  # Row as TSV (also accessible without extension)
├── .build/                     # Create new synthesized apps
├── .schemas/                   # Access non-public schemas
└── .information_schema/        # Global metadata
```

## Metadata Files (.info/)

All metadata files are read-only. Access via `Read "mount/table/.info/FILE"`.

| File | Content | Example |
|------|---------|---------|
| `count` | Total row count | `1000` |
| `schema` | CREATE TABLE statement | `CREATE TABLE users (id SERIAL PRIMARY KEY, ...)` |
| `ddl` | Extended DDL with indexes | Full DDL including CREATE INDEX statements |
| `columns` | Column names, one per line | `id\nname\nemail\nage` |
| `indexes` | Index descriptions | `PRIMARY KEY: id\nUNIQUE: email` |

**Workflow:** Always read `count` before accessing data to choose the right strategy.

## Reading Data

### Row-as-File

Read an entire row in a chosen format:

```
Read "mount/users/1.json"     →  {"id":1,"name":"Alice","email":"alice@ex.com","age":28}
Read "mount/users/1.csv"      →  1,Alice,alice@ex.com,28
Read "mount/users/1.tsv"      →  1\tAlice\talice@ex.com\t28
Read "mount/users/1"          →  (same as .tsv)
```

**When to use each format:**

| Format | Best For |
|--------|----------|
| `.json` | Parsing structured data, when you need field names with values |
| `.csv` | Tabular processing, data pipelines |
| `.tsv` / no extension | Simple text processing, quick inspection |
| column access | When you only need one or two fields |

### Row-as-Directory

Access individual column values:

```
Read "mount/users/1/email"    →  alice@example.com
Read "mount/users/1/name"     →  Alice
Read "mount/users/1/age"      →  28
```

Column files return the raw value as text, with a trailing newline.

### Listing Rows

For small tables (under 10,000 rows):

```
Glob "mount/users/*"           # Lists PK directories: 1/, 2/, 3/, ...
Glob "mount/users/*.json"      # Lists row files: 1.json, 2.json, ...
```

For large tables, `Glob` on the table root will fail. Use navigation paths instead.

## Large Table Navigation

### .first/N/ — First N Rows

Returns the first N rows ordered by primary key ascending:

```
Glob "mount/orders/.first/20/*"     # First 20 order PKs
Read "mount/orders/.first/20/1.json" # Won't work — read via direct path
```

After getting PKs from `.first/N/`, read rows by their PK:

```
Glob "mount/orders/.first/20/*"     # → 1/, 2/, 3/, ... 20/
Read "mount/orders/1.json"          # Read the actual row
```

### .last/N/ — Last N Rows

Returns the last N rows ordered by primary key descending (most recent for auto-increment PKs):

```
Glob "mount/orders/.last/10/*"      # Last 10 orders
```

### .sample/N/ — Random Sample

Returns N randomly selected rows:

```
Glob "mount/orders/.sample/50/*"    # 50 random orders
```

### .all/ — Escape Hatch

For large tables, `.all/` bypasses the 10,000-row limit and lists all PKs. Use with caution:

```
Glob "mount/big_table/.all/*"       # All PKs (may be very large)
```

## Index Lookups (.by/)

Indexes enable fast lookups without scanning all rows.

### Discover Available Indexes

```
Read "mount/users/.info/indexes"
# PRIMARY KEY: id
# UNIQUE: email
```

Indexed columns appear under `.by/`:

```
Glob "mount/users/.by/*"           # → email/
```

### Single-Column Lookup

```
Glob "mount/users/.by/email/alice@example.com/*"    # Row PKs matching this email
Read "mount/users/.by/email/alice@example.com/1.json"  # Won't work — use direct path
```

The result is a directory listing of matching PKs. Read the rows by PK:

```
Glob "mount/users/.by/email/alice@example.com/*"   # → 42/
Read "mount/users/42.json"                          # Read the row
```

### Composite Index Lookup

For multi-column indexes, use dot-separated column names and values:

```
Glob "mount/users/.by/last_name.first_name/Smith.John/*"
```

### Index Pagination

Indexes support `.first/N/` and `.last/N/` to browse distinct values:

```
Glob "mount/users/.by/created_at/.first/10/*"    # 10 oldest distinct dates
Glob "mount/users/.by/created_at/.last/10/*"     # 10 newest distinct dates
```

And within a value's results:

```
Glob "mount/orders/.by/status/pending/.first/50/*"   # First 50 pending orders
Glob "mount/orders/.by/status/pending/.last/50/*"    # Last 50 pending orders
```

## Pipeline Queries

Capabilities can be chained to build complex queries:

```
# Last 10 pending orders for customer 123
Glob "mount/orders/.by/customer_id/123/.by/status/pending/.last/10/*"

# Random sample of active users
Glob "mount/users/.by/status/active/.sample/100/*"

# Filter by non-indexed column
Glob "mount/orders/.filter/priority/high/.first/20/*"
```

**Chaining rules:**

| Capability | Can Follow |
|------------|------------|
| `.by/col/val/` | `.by/`, `.filter/`, `.order/`, `.first/`, `.last/`, `.sample/`, `.export/` |
| `.filter/col/val/` | `.by/`, `.filter/`, `.order/`, `.first/`, `.last/`, `.sample/`, `.export/` |
| `.order/col/` | `.first/`, `.last/`, `.sample/`, `.export/` |
| `.first/N/` | `.by/`, `.filter/`, `.order/`, `.last/`, `.sample/`, `.export/` |
| `.last/N/` | `.by/`, `.filter/`, `.order/`, `.first/`, `.sample/`, `.export/` |

Multiple filters are AND-combined:

```
.by/status/active/.by/tier/premium/    →  status='active' AND tier='premium'
```

### Bulk Export

Export filtered results in a single file:

```
Read "mount/orders/.by/status/pending/.first/100/.export/json"
Read "mount/users/.by/status/active/.export/csv"
```

## Writing Data

### Update a Single Column

```
Write "mount/users/1/email" with content "new@example.com"
```

Maps to: `UPDATE users SET email = 'new@example.com' WHERE id = 1`

### Update Multiple Columns (PATCH)

All format writes use **PATCH semantics** — only specified columns are updated. Unspecified columns retain their values.

**JSON:**
```
Write "mount/users/1.json" with content '{"name":"Alice Smith","email":"new@example.com"}'
```

**TSV (header row required):**
```
Write "mount/users/1.tsv" with content "name\temail\nAlice Smith\tnew@example.com"
```

**CSV (header row required):**
```
Write "mount/users/1.csv" with content "name,email\nAlice Smith,new@example.com"
```

### Insert a New Row

Write to `new.FORMAT` to insert:

```
Write "mount/products/new.json" with content '{"name":"Widget","price":9.99,"in_stock":true}'
```

Maps to: `INSERT INTO products (name, price, in_stock) VALUES ('Widget', 9.99, true)`

### Delete a Row

```bash
rm mount/users/999
```

Maps to: `DELETE FROM users WHERE id = 999`

Deletions are permanent. Database constraints (foreign keys) are enforced.

### Bulk Import

Write data to `.import/FORMAT`:

```
Write "mount/users/.import/json" with content '[{"name":"Bob","email":"bob@ex.com"},{"name":"Carol","email":"carol@ex.com"}]'
```

## Searching

### Grep Across Rows

```
Grep pattern="alice" path="mount/users/"                    # All data in users
Grep pattern="urgent" path="mount/orders/" glob="*/notes"   # Only the notes column
Grep pattern="2024-01" path="mount/orders/" glob="*.json"   # JSON rows matching date
```

### Performance Tips

1. **Use indexes first** — `.by/` lookups are O(1), grep is O(n)
2. **Limit scope** — search specific columns (`glob="*/email"`) instead of whole rows
3. **Sample first** — `Grep` inside `.sample/100/` to test patterns before scanning widely
4. **Check count** — if `.info/count` returns millions, grep will be slow; use `.by/` or `.filter/`
