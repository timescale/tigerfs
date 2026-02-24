# Native Operations Reference

Detailed reference for TigerFS native table access — the row-as-file and row-as-directory interface.

## Path Hierarchy

```
mount/
├── table_name/                 # One directory per table
│   ├── .info/                  # Metadata (read-only)
│   │   ├── count               # Total row count
│   │   ├── schema              # CREATE TABLE DDL
│   │   ├── ddl                 # Extended DDL with indexes
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
│   ├── .all/                   # Escape hatch for large tables
│   ├── .indexes/               # Index management (DDL)
│   ├── .modify/                # Table modification (DDL)
│   ├── .delete/                # Table deletion (DDL)
│   ├── pk/                     # Row as directory
│   │   ├── column1             # Individual column value
│   │   └── column2
│   ├── pk.json                 # Row as JSON
│   ├── pk.csv                  # Row as CSV
│   └── pk.tsv                  # Row as TSV (also accessible without extension)
├── .build/                     # Create new synthesized apps
├── .schemas/                   # Access non-public schemas
└── .information_schema/        # Global metadata
```

## Metadata Files (.info/)

All metadata files are read-only.

| File | Content | Example |
|------|---------|---------|
| `count` | Total row count | `1000` |
| `schema` | CREATE TABLE statement | `CREATE TABLE users (id SERIAL PRIMARY KEY, ...)` |
| `ddl` | Extended DDL with indexes | Full DDL including CREATE INDEX statements |
| `columns` | Column names, one per line | `id\nname\nemail\nage` |
| `indexes` | Index descriptions | `PRIMARY KEY: id\nUNIQUE: email` |

**Always read `count` before accessing data to choose the right strategy.**

## Reading Data

### Row-as-File

```
Read "mount/users/1.json"     →  {"id":1,"name":"Alice","email":"alice@ex.com"}
Read "mount/users/1.csv"      →  1,Alice,alice@ex.com
Read "mount/users/1"           →  1\tAlice\talice@ex.com  (TSV, default)
```

| Format | Best For |
|--------|----------|
| `.json` | Structured data, when you need field names with values |
| `.csv` | Tabular processing, data pipelines |
| `.tsv` / no extension | Quick inspection, simple text |
| column access | When you only need one or two fields |

### Row-as-Directory

```
Read "mount/users/1/email"    →  alice@example.com
Read "mount/users/1/name"     →  Alice
```

Column files return the raw value as text, with a trailing newline.

### Listing Rows

For small tables (under 10,000 rows):

```
Glob "mount/users/*.json"      # Lists row files: 1.json, 2.json, ...
```

For large tables, `Glob` on the table root will fail. Use navigation paths instead.

## Large Table Navigation

### .first/N/ and .last/N/

```
Glob "mount/orders/.first/20/*"     # First 20 order PKs (ascending)
Glob "mount/orders/.last/10/*"      # Last 10 orders (descending, most recent)
```

After getting PKs, read rows by their PK:

```
Read "mount/orders/1.json"
```

### .sample/N/ and .all/

```
Glob "mount/orders/.sample/50/*"    # 50 random orders
Glob "mount/big_table/.all/*"       # All PKs (use with caution)
```

## Index Lookups (.by/)

### Discover Indexes

```
Read "mount/users/.info/indexes"
Glob "mount/users/.by/*"           # → email/
```

### Single-Column Lookup

```
Glob "mount/users/.by/email/alice@example.com/*"   # → 42/
Read "mount/users/42.json"                          # Read the row
```

### Composite Index Lookup

```
Glob "mount/users/.by/last_name.first_name/Smith.John/*"
```

### Index Pagination

```
Glob "mount/users/.by/created_at/.first/10/*"          # 10 oldest distinct dates
Glob "mount/orders/.by/status/pending/.first/50/*"      # First 50 pending orders
```

## Pipeline Queries

Capabilities chain to build complex queries:

```
Glob "mount/orders/.by/customer_id/123/.by/status/pending/.last/10/*"
Glob "mount/users/.by/status/active/.sample/100/*"
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

Multiple filters are AND-combined: `.by/status/active/.by/tier/premium/` → `status='active' AND tier='premium'`

### Bulk Export

```
Read "mount/orders/.by/status/pending/.first/100/.export/json"
Read "mount/users/.by/status/active/.export/csv"
```

## Writing Data

### Update a Single Column

```
Write "mount/users/1/email" with content "new@example.com"
```

### Update Multiple Columns (PATCH)

All format writes use **PATCH semantics** — only specified columns are updated.

```
Write "mount/users/1.json" with content '{"name":"Alice Smith","email":"new@example.com"}'
```

### Insert a New Row

```
Write "mount/products/new.json" with content '{"name":"Widget","price":9.99}'
```

### Delete a Row

```bash
Bash "rm mount/users/999"
```

### Bulk Import

```
Write "mount/users/.import/json" with content '[{"name":"Bob","email":"bob@ex.com"},{"name":"Carol","email":"carol@ex.com"}]'
```

## Searching

```
Grep pattern="alice" path="mount/users/"
Grep pattern="urgent" path="mount/orders/" glob="*/notes"
```

**Performance tips:**
1. **Use indexes first** — `.by/` lookups are O(1), grep is O(n)
2. **Limit scope** — search specific columns (`glob="*/email"`) instead of whole rows
3. **Sample first** — `Grep` inside `.sample/100/` to test patterns before scanning widely
4. **Check count** — if `.info/count` returns millions, use `.by/` or `.filter/`
