---
name: tigerfs
description: How to discover, read, write, and search data in a TigerFS-mounted PostgreSQL database using Claude Code file tools.
---

# Using TigerFS

TigerFS mounts PostgreSQL databases as directories. You interact with tables, rows, and columns using Read, Write, Glob, and Grep — no SQL needed.

## Iron Law

**Never `ls` a table directory without checking `.info/count` first.**

Tables with more than 10,000 rows will return an error on `ls`. Always check the size, then use `.first/N/`, `.sample/N/`, or `.by/column/value/` for large tables.

## Detecting a TigerFS Mount

A directory is a TigerFS mount if table subdirectories contain `.info/` with metadata files:

```
mount/
├── users/
│   ├── .info/          # Metadata directory
│   │   ├── count       # Row count
│   │   ├── schema      # CREATE TABLE DDL
│   │   ├── columns     # Column names (one per line)
│   │   ├── ddl         # Extended DDL
│   │   └── indexes     # Index information
│   ├── .by/            # Index-based lookups
│   ├── .first/         # First N rows by PK
│   ├── .last/          # Last N rows by PK
│   ├── .sample/        # Random N rows
│   └── 1/  2/  3/ ...  # Row directories (by primary key)
├── products/
└── orders/
```

Markdown synthesized apps appear as directories containing `.md` files — see [markdown-app.md](markdown-app.md).

## Core Workflow

### 1. Discover

List the mount root to see available tables:

```
Glob "mount/*"
```

For each table of interest, read its schema and columns:

```
Read "mount/users/.info/schema"    # CREATE TABLE statement
Read "mount/users/.info/columns"   # Column names, one per line
```

### 2. Assess Size

**Always do this before reading data:**

```
Read "mount/users/.info/count"     # e.g., "42" or "1500000"
```

| Count | Strategy |
|-------|----------|
| < 1,000 | Safe to `Glob` + `Read` all rows |
| 1,000 - 10,000 | `Glob` works, but prefer targeted access |
| > 10,000 | **Must** use `.first/`, `.sample/`, or `.by/` |

### 3. Read Data

**Small tables** — read rows directly:

```
Glob "mount/users/*.json"          # List all row files
Read "mount/users/1.json"          # Full row as JSON
Read "mount/users/1/email"         # Single column value
```

**Large tables** — use navigation paths:

```
Glob "mount/orders/.first/20/*"    # First 20 rows by PK
Glob "mount/orders/.last/10/*"     # Last 10 rows (most recent)
Glob "mount/orders/.sample/50/*"   # Random 50 rows
Glob "mount/orders/.by/status/pending/*"  # Index lookup
```

Then read individual rows:

```
Read "mount/orders/12345.json"     # Full row as JSON
Read "mount/orders/12345/amount"   # Single column
```

**Format selection:**

| Format | When to Use |
|--------|-------------|
| `.json` (`Read "mount/t/1.json"`) | Structured data, multiple columns needed |
| column access (`Read "mount/t/1/col"`) | Single value needed |
| no extension (`Read "mount/t/1"`) | TSV — tab-separated, good for simple rows |
| `.csv` (`Read "mount/t/1.csv"`) | CSV format |

### 4. Search

Use Grep to find data across rows:

```
Grep pattern="alice" path="mount/users/"           # Search all user data
Grep pattern="urgent" path="mount/orders/" glob="*/notes"  # Search one column
```

For indexed columns, prefer `.by/` lookups over grep — they're faster:

```
Glob "mount/users/.by/email/alice@example.com/*"   # Instant index lookup
```

### 5. Write Data

**Update a single column:**

```
Write "mount/users/1/email" with content "new@example.com"
```

**Update multiple columns (JSON PATCH — only specified keys are changed):**

```
Write "mount/users/1.json" with content '{"name":"Alice Smith","email":"new@example.com"}'
```

**Insert a new row:**

```
Write "mount/products/new.json" with content '{"name":"Widget","price":9.99}'
```

**Delete a row:**

```bash
rm mount/users/999    # Via Bash tool
```

### 6. Markdown Files

If a directory contains `.md` files, it's a synthesized markdown app. Read and write files directly:

```
Glob "mount/notes/*.md"            # List all markdown files
Read "mount/notes/hello.md"        # Read with frontmatter
Write "mount/notes/hello.md" with updated content
```

Synth apps can contain subdirectories. Use recursive globs to list all files:

```
Glob "mount/notes/**/*.md"         # List all .md files, including subdirectories
Bash "mkdir mount/notes/tutorials" # Create a subdirectory
```

See [markdown-app.md](markdown-app.md) for full details on frontmatter, creation, directories, and column mapping.

## Quick Reference

| Goal | Tool Call |
|------|-----------|
| List tables | `Glob "mount/*"` |
| Table schema | `Read "mount/t/.info/schema"` |
| Column names | `Read "mount/t/.info/columns"` |
| Row count | `Read "mount/t/.info/count"` |
| Index info | `Read "mount/t/.info/indexes"` |
| Read row (JSON) | `Read "mount/t/pk.json"` |
| Read column | `Read "mount/t/pk/column"` |
| First N rows | `Glob "mount/t/.first/N/*"` |
| Last N rows | `Glob "mount/t/.last/N/*"` |
| Random sample | `Glob "mount/t/.sample/N/*"` |
| Index lookup | `Glob "mount/t/.by/col/val/*"` |
| Search content | `Grep pattern="term" path="mount/t/"` |
| Update column | `Write "mount/t/pk/col"` with value |
| Update row (PATCH) | `Write "mount/t/pk.json"` with JSON |
| Insert row | `Write "mount/t/new.json"` with JSON |
| Delete row | `Bash "rm mount/t/pk"` |
| List markdown files | `Glob "mount/dir/*.md"` |
| List markdown (recursive) | `Glob "mount/dir/**/*.md"` |
| Read markdown | `Read "mount/dir/file.md"` |
| Write markdown | `Write "mount/dir/file.md"` with content |

## Anti-Patterns

| Don't | Do Instead |
|-------|------------|
| `Glob "mount/big_table/**/*"` on large tables | Check `.info/count` first, use `.first/`, `.sample/`, `.by/` |
| Write full row JSON expecting replace semantics | JSON/CSV/TSV writes are **PATCH** — only specified keys update |
| `Grep` across all rows of a large table | Use `.by/` index lookups for indexed columns |
| Ignore `.info/indexes` | Check available indexes for efficient lookups |
| `ls` a table directory without checking size | Read `.info/count` first — large tables return EIO on `ls` |

## Detailed References

- **[native-operations.md](native-operations.md)** — Full path hierarchy, metadata files, all read/write operations, index navigation, pipeline queries
- **[markdown-app.md](markdown-app.md)** — Synthesized markdown apps: creation, frontmatter, column mapping, CRUD operations
