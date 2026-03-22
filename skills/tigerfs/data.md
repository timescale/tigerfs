# Data-First Reference

Explore and manipulate database tables using file tools. Each table is a directory, each row is a file, each column is accessible individually.

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
│   ├── .columns/col1,col2/     # Column projection
│   ├── .export/json|csv|tsv    # Bulk export
│   ├── .import/json|csv|tsv    # Bulk import
│   ├── .all/                   # Access all rows (bypasses 10,000 row default limit)
│   ├── .indexes/               # Index management (DDL)
│   ├── .modify/                # Table modification (DDL)
│   ├── .delete/                # Table deletion (DDL)
│   ├── pk/                     # Row as directory
│   │   ├── column1             # Individual column value
│   │   └── column2
│   ├── pk.json                 # Row as JSON
│   ├── pk.csv                  # Row as CSV
│   └── pk.tsv                  # Row as TSV (also accessible without extension)
├── .build/                     # Create file-first apps
├── .create/                    # Create new tables, views, schemas (DDL)
└── .schemas/                   # Access non-public schemas
```

## Metadata (.info/)

Read-only files describing the table. **Check `.info/count` first** to choose the right access strategy (see SKILL.md).

| File | Content | Example |
|------|---------|---------|
| `count` | Total row count | `1000` |
| `schema` | CREATE TABLE statement | `CREATE TABLE users (id SERIAL PRIMARY KEY, ...)` |
| `ddl` | Extended DDL with indexes | Full DDL including CREATE INDEX statements |
| `columns` | Column names, one per line | `id\nname\nemail\nage` |
| `indexes` | Index descriptions | `PRIMARY KEY: id\nUNIQUE: email` |

## Data Formats

All rows can be read and written in four formats by using the corresponding file extension:

| Extension | Format | Best For |
|-----------|--------|----------|
| `.json` | JSON | Structured data, when you need field names with values |
| `.yaml` | YAML | Human-readable structured data, multi-document bulk operations |
| `.csv` | CSV | Tabular processing, data pipelines |
| `.tsv` / none | TSV | Quick inspection, simple text (default) |

These extensions work on individual rows (`pk.json`), exports (`.export/json`), and imports (`.import/json`).

## Reading Data

### Individual Rows

```
Read "mount/users/1.json"        # Row as JSON (see Data Formats for other extensions)
Read "mount/users/1/email"       # Single column value (raw text)
```

### Navigating Tables

Directory listings are limited to 10,000 rows by default. Use `.all/` to bypass this limit.

```
Glob "mount/users/*.json"             # List rows (small tables only)
Glob "mount/orders/.first/20/*"       # First 20 PKs (ascending)
Glob "mount/orders/.last/10/*"        # Last 10 PKs (descending, most recent)
Glob "mount/orders/.sample/50/*"      # 50 random PKs
Glob "mount/big_table/.all/*"         # All PKs (bypasses 10,000 row limit)
```

After getting PKs, read individual rows: `Read "mount/orders/1.json"`

### Index Lookups (.by/)

Use `.by/` for efficient lookups on indexed columns. Discover available indexes with `Read "mount/users/.info/indexes"` or `Glob "mount/users/.by/*"`.

```
Glob "mount/users/.by/email/alice@example.com/*"          # Single-column lookup
Glob "mount/users/.by/last_name.first_name/Smith.John/*"  # Composite index
Glob "mount/orders/.by/status/pending/.first/50/*"        # Index + pagination
```

## Pipeline Queries

Chain capabilities to build complex queries. Each path segment maps to a SQL clause, executed as a single query:

```
.by/customer_id/123/.by/status/pending/.order/created_at/.last/10/.export/json
 └─ WHERE            └─ AND              └─ ORDER BY       └─ LIMIT  └─ format
```

Prefer `.by/` over `.filter/` -- `.by/` uses indexes and is fast, `.filter/` scans the table and is slow on large tables.

```
Glob "mount/orders/.by/customer_id/123/.by/status/pending/.last/10/*"
Read "mount/orders/.by/status/pending/.columns/id,total/.export/json"
Read "mount/users/.by/status/active/.export/csv"
```

Multiple filters are AND-combined: `.by/status/active/.by/tier/premium/` becomes `status='active' AND tier='premium'`. Note: `.by/` and `.filter/` support **equality only**, not range queries.

When listing `.filter/column/` values on a large unindexed column, TigerFS returns `.table-too-large` instead of scanning. You can still access rows directly by specifying the value: `.filter/column/value/`.

**Chaining rules:**

| Capability | Can Follow |
|------------|------------|
| `.by/col/val/` | `.by/`, `.filter/`, `.order/`, `.first/`, `.last/`, `.sample/`, `.columns/`, `.export/` |
| `.filter/col/val/` | `.by/`, `.filter/`, `.order/`, `.first/`, `.last/`, `.sample/`, `.columns/`, `.export/` |
| `.order/col/` | `.first/`, `.last/`, `.sample/`, `.columns/`, `.export/` |
| `.first/N/` | `.by/`, `.filter/`, `.order/`, `.last/`, `.sample/`, `.columns/`, `.export/` |
| `.last/N/` | `.by/`, `.filter/`, `.order/`, `.first/`, `.sample/`, `.columns/`, `.export/` |
| `.columns/col1,col2/` | `.export/` only |

`.columns/` selects specific columns before export. Can only be used once.

For CSV/TSV exports, add `.with-headers/` to include a header row: `Read "mount/t/.export/.with-headers/csv"`. For imports without a header row, add `.no-headers/`: `Write "mount/t/.import/.append/.no-headers/csv"`.

## Writing Data

All format writes use **PATCH semantics** -- only specified columns are updated.

```
Write "mount/users/1/email" with content "new@example.com"                    # Update single column
Write "mount/users/1.json" with content '{"name":"Alice Smith"}'              # Update via JSON (PATCH)
Write "mount/products/new.json" with content '{"name":"Widget","price":9.99}' # Insert new row
Bash "rm mount/users/999"                                                     # Delete row
```

### Bulk Import

Three import modes control how incoming data interacts with existing rows:

| Mode | Path | Behavior |
|------|------|----------|
| **Append** | `.import/.append/json` | Insert new rows only. Fails on PK conflicts. |
| **Sync** | `.import/.sync/json` | Upsert by primary key. Updates existing rows, inserts new ones. |
| **Overwrite** | `.import/.overwrite/json` | Replace all rows. Deletes existing data, then inserts. |

```
Write "mount/users/.import/.append/json" with content '[{"name":"Bob","email":"bob@ex.com"}]'
Write "mount/users/.import/.sync/csv" with content (CSV with header row)
Write "mount/users/.import/.overwrite/json" with content (full dataset)
```

## DDL Operations (.create/, .modify/, .delete/, .indexes/)

Schema changes use a staging workflow:

```bash
Bash "mkdir mount/.create/products"                    # Start a CREATE session
Write "mount/.create/products/sql" with DDL content    # Write the SQL
Bash "touch mount/.create/products/.test"              # Dry-run (check for errors)
Bash "touch mount/.create/products/.commit"            # Apply the change
```

| Path | Purpose |
|------|---------|
| `mount/.create/<name>/` | Create new tables, views, or schemas |
| `mount/<table>/.modify/<name>/` | ALTER existing table |
| `mount/<table>/.delete/<name>/` | DROP table |
| `mount/<table>/.indexes/<name>/` | Create or drop indexes |

Each session has: `sql` (the DDL statement), `.test` (dry-run), `.commit` (apply), `.abort` (cancel). After touching `.test`, read `test.log` for dry-run results and any errors.

## Searching

Use Grep for text search across rows. For large tables, prefer `.by/` or `.filter/` over Grep -- index lookups are O(1), Grep scans every row. Limit scope with `glob="*/column"` to search specific columns instead of whole rows.
