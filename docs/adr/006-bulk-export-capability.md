# ADR-006: Bulk Data Capabilities (`.export/` and `.import/`)

**Date:** 2026-01-28
**Status:** Accepted
**Deciders:** Mike Freedman

## Context

TigerFS provides access to individual rows as files, but there's no way to get a single-file view of multiple rows. Users working with tables need to:

- Export table data for analysis (spreadsheets, data tools)
- Import data from files (CSV dumps, JSON exports)
- Create backups or snapshots
- Transfer data between systems
- Feed data to tools that expect single files

Currently, getting multiple rows requires iteration:
```bash
# Awkward: iterate and concatenate
for id in $(ls /mnt/db/users/); do cat /mnt/db/users/$id.json; done
```

### Design Goals

1. **Single-file materialization**: Get entire collections as one file
2. **Bidirectional**: Support both export (read) and import (write)
3. **Format flexibility**: Support common data formats (CSV, JSON, etc.)
4. **Explicit write modes**: Users must choose how imports handle conflicts
5. **Composability**: Work with any "collection" context (table, pagination, index lookup)
6. **Consistency**: Follow the capability taxonomy established in ADR-005

## Decision

Add two capabilities:
- **`.export/`** - Read collection data in various formats
- **`.import/`** - Write collection data with explicit conflict handling modes

### Capability Structure

```
.export/              # read-only: export data
├── csv
├── tsv
├── json
└── yaml

.import/              # write-only: import data
├── .overwrite/       # truncate table, then insert all rows
│   ├── csv
│   ├── tsv
│   ├── json
│   └── yaml
├── .sync/            # upsert: update existing, insert new (requires PK)
│   ├── csv
│   ├── tsv
│   ├── json
│   └── yaml
└── .append/          # insert only: fail on conflict
    ├── csv
    ├── tsv
    ├── json
    └── yaml
```

### Why This Structure?

**Symmetric naming:**
- `.export/` - get data out
- `.import/` - put data in

**Explicit write modes** (filesystem-friendly names):
| Mode | Filesystem analog | Behavior |
|------|-------------------|----------|
| `.overwrite/` | `>` redirect | Truncate + insert (destructive) |
| `.sync/` | `rsync` | Update existing, add new (merge) |
| `.append/` | `>>` append | Insert only, fail on conflict |

**Nested structure benefits:**
- Discoverable: `ls .import/` shows available modes
- Grouped: all write operations under one capability
- Clear: user must explicitly choose mode (no accidental overwrites)

### Usage Examples

**Export (read):**
```bash
cat products/.export/csv                    # entire table as CSV
cat products/.first/100/.export/json        # first 100 rows as JSON
cat products/.order/price/.last/50/.export/yaml  # top 50 by price as YAML
```

**Import (write):**
```bash
# Overwrite: table = file contents (destructive)
cat data.csv > products/.import/.overwrite/csv

# Sync: merge file with existing data (requires PK)
cat updates.json > products/.import/.sync/json

# Append: add new rows only
cat new_rows.csv > products/.import/.append/csv
```

### Composition with `.order/`

A new `.order/<column>/` capability allows specifying row ordering. This applies to **all collection operations**, not just bulk reads via `.export/`:

```bash
# List row directories ordered by created_at (first 100)
ls products/.order/created_at/.first/100/

# List row directories ordered by price (last 50 = highest prices)
ls products/.order/price/.last/50/

# Export as CSV with ordering
cat products/.order/price/.last/50/.export/csv

# Read individual row from ordered result
cat products/.order/created_at/.first/1/
```

**SQL mapping:**
| Path | SQL |
|------|-----|
| `.first/100/` | `ORDER BY pk ASC LIMIT 100` |
| `.last/100/` | `ORDER BY pk DESC LIMIT 100` |
| `.order/created_at/.first/100/` | `ORDER BY created_at ASC LIMIT 100` |
| `.order/created_at/.last/100/` | `ORDER BY created_at DESC LIMIT 100` |

**Design choices:**
- Applies to row listing (`ls`), individual row access, and bulk export
- Direction is implicit: `.first` = ASC, `.last` = DESC
- No multi-column ordering (single column only)
- No index requirement (works on any column, user accepts performance implications)

### Format Specifications

All formats are self-describing for writes—column names come from headers (CSV/TSV) or keys (JSON/YAML). No auto-detection; user specifies format explicitly.

#### CSV (read/write)
```csv
id,name,email,created_at
1,Alice,alice@example.com,2024-01-15T10:30:00Z
2,Bob,bob@example.com,2024-01-16T14:22:00Z
```
- **Header row required** for writes (maps columns by name)
- Values quoted as needed per RFC 4180
- NULL represented as empty field
- Column order on read: database order (matches `.info/columns`)
- Column order on write: determined by header row (any order allowed)

#### TSV (read/write)
```tsv
id	name	email	created_at
1	Alice	alice@example.com	2024-01-15T10:30:00Z
2	Bob	bob@example.com	2024-01-16T14:22:00Z
```
- **Header row required** for writes
- Tab-separated, no quoting
- NULL represented as empty field

#### JSON (read/write)
```json
[
  {"id": 1, "name": "Alice", "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "email": "bob@example.com"}
]
```
- Array of objects
- Keys are column names (any order allowed)
- NULL represented as JSON `null`

#### YAML (read/write)
```yaml
---
id: 1
name: Alice
email: alice@example.com
---
id: 2
name: Bob
email: bob@example.com
```
- Multi-document format with `---` separators
- Keys are column names (any order allowed)
- NULL represented as YAML `null`
- Concatenation-safe

### Read Semantics (Export)

**Size limits:**
- Limited to `DirListingLimit` rows (default 10,000)
- For larger exports, use explicit pagination: `.first/10000/.export/csv`
- Error message suggests pagination if limit exceeded

**Empty collections:** Return empty file (0 bytes) for all formats.

**Column order:** Database column order (matches `.info/columns`).

**Composition:** `.export/` appears in any collection context:
- Table level: `products/.export/csv`
- Pagination: `products/.first/100/.export/csv`
- Ordering: `products/.order/price/.last/50/.export/csv`
- Index lookup: `products/.by/category/Electronics/.export/json`

### Write Semantics (Import)

**Size limits:**
- Limited to `DirWritingLimit` rows (default 100,000)
- Error if exceeded

**Column handling:**
- All formats are self-describing (headers for CSV/TSV, keys for JSON/YAML)
- User can specify columns in any order
- Extra columns in file: ignored
- Missing columns: use database defaults

**Transaction: Atomic**
- Entire import in single transaction
- All rows succeed or all fail (rollback)
- Prevents partial imports

**Error reporting:**
- Failure returns EIO
- Details in server logs

**Write modes:**

| Mode | Behavior | PK Required? |
|------|----------|--------------|
| `.overwrite/` | Truncate table, insert all rows | No |
| `.sync/` | Update if PK exists, insert if new | **Yes** - errors without PK |
| `.append/` | Insert all, fail on any conflict | No (but fails if conflicts) |

**`.sync/` requires primary key:**
```
Error: Cannot sync to 'events' - table has no primary key or unique constraint.
Use .append/ to add rows, or .overwrite/ to replace entire table.
```

**Write context:** Only table-level imports supported. Filtered views (`.first/`, `.by/`, `.order/`) do not have `.import/` - writing to "first 100 rows" is meaningless.

### Individual Row Writes

**Change from current behavior:** CSV/TSV writes for individual rows (e.g., `echo '...' > users/123.csv`) now require a header row, matching bulk write behavior.

```bash
# Old (no longer supported for CSV/TSV):
echo 'Alice,alice@example.com' > users/123.csv

# New (header required):
echo 'name,email
Alice,alice@example.com' > users/123.csv

# JSON/YAML unchanged (already self-describing):
echo '{"name":"Alice","email":"alice@example.com"}' > users/123.json
```

This makes all formats consistently self-describing.

### Example Structure

Following ADR-005 taxonomy:

```
/mnt/db/products/
├── .info/                    # metadata
│   ├── count
│   ├── ddl
│   ├── schema
│   └── columns
├── .export/                  # capability: bulk read
│   ├── csv
│   ├── tsv
│   ├── json
│   └── yaml
├── .import/                  # capability: bulk write
│   ├── .overwrite/
│   │   └── csv, tsv, json, yaml
│   ├── .sync/
│   │   └── csv, tsv, json, yaml
│   └── .append/
│       └── csv, tsv, json, yaml
├── .by/                      # capability: lookup by index
│   └── category/
├── .order/                   # capability: specify ordering
│   └── price/
│       └── .first/
│           └── 100/
│               └── .export/
│                   └── csv
├── .first/                   # capability: pagination
│   └── 100/
│       └── .export/
│           └── csv
├── .indexes/                 # capability: index management
└── 1/                        # row by PK
```

### Configuration

New configuration option:

```yaml
# In config file
dir_writing_limit: 100000   # Max rows for bulk import (default: 100,000)
```

### Future Extensions

**Additional formats (not in initial implementation):**
- `.export/parquet`, `.import/.../parquet` - Columnar binary format
- `.export/ndjson` - Newline-delimited JSON (streaming-friendly)

**Additional capabilities:**
- `.import/.validate/csv` - Dry-run validation without import
- `.export/.schema` - Expected schema/headers for import

## Consequences

### Positive

- **Clear separation**: `.export/` for reads, `.import/` for writes
- **Explicit modes**: User must choose `.overwrite/`, `.sync/`, or `.append/`
- **Self-describing**: All formats use headers/keys for column mapping
- **Discoverable**: `ls .import/` shows available write modes
- **Composable**: `.export/` works with `.order/`, pagination, and filtering
- **Safe**: No accidental overwrites; `.sync/` requires PK
- **Atomic imports**: All-or-nothing prevents partial data
- **Filesystem-friendly names**: overwrite, sync, append (not INSERT, UPSERT)

### Negative

- **Longer write paths**: `.import/.sync/csv` vs `.sync/csv`
- **Memory usage**: Large exports buffered in memory
- **Size limits**: May frustrate users wanting full table exports
- **Breaking change**: CSV/TSV row writes now require headers

### Neutral

- Consistent with row-level format support
- Follows capability taxonomy from ADR-005
