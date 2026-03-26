---
name: tigerfs
description: How to discover, read, write, and search data in a TigerFS-mounted PostgreSQL database using file tools.
---

# Using TigerFS

TigerFS mounts PostgreSQL databases as directories. You interact with data using Read, Write, Glob, and Grep -- no SQL needed. **File-first** mode gives you a transactional, shareable filesystem backed by a database. **Data-first** mode lets you explore and manipulate an existing database using file tools. Most work uses file-first.

## Which Mode?

Each directory in a TigerFS mount is either file-first or data-first:

- **File-first:** Contains `.md` or `.txt` files. Read and write files normally. See [files.md](files.md).
- **Data-first:** Contains `.info/` directory. Access rows as files or directories. See [data.md](data.md).
- **`.tables/` directory** (e.g., `.tables/notes/`): Data-first access to backing tables in the `tigerfs` schema.

## Directory Structure

```
mount/
├── notes/                  # File-first (markdown app)
│   ├── hello.md
│   ├── tutorials/
│   └── .history/           # Versioned history (if enabled)
├── snippets/               # File-first (plain text app)
│   └── bash-loop.txt
├── .tables/                # Backing tables in tigerfs schema
│   └── notes/              # Data-first access to notes backing table
├── users/                  # Data-first (standalone table)
│   ├── .info/
│   ├── .by/
│   └── 1/  2/  3/ ...
└── .build/                 # Create new apps
```

## File-First

A transactional, shareable filesystem backed by a database. Multiple users and agents can read and write concurrently. Create apps with `.build/`:

```bash
Bash "echo 'markdown' > mount/.build/notes"
Bash "echo 'markdown,history' > mount/.build/notes"    # with versioned history
Bash "echo 'plaintext' > mount/.build/snippets"         # body-only, no frontmatter
```

See [files.md](files.md) for full details on schemas, column roles, directories, and history.

### What you can build

Because the filesystem is transactional and shared, it can implement collaborative workflows:

When asked to create a **task list, kanban board, todo, or project tracker**:
> Read [recipes.md](recipes.md) Recipe 1 and follow it exactly.
  Core principle: **directories = states** (`todo/`, `doing/`, `done/`), **`mv` = transitions**. Do NOT use `status` frontmatter fields.

When asked to create a **knowledge base, wiki, or documentation store**:
> Read [recipes.md](recipes.md) Recipe 2 and follow it exactly.

When asked to **save or resume session context**:
> Read [recipes.md](recipes.md) Recipe 3 and follow it exactly.

When asked to **keep a log of what you do**:
> Read [recipes.md](recipes.md) Recipe 4 and follow it exactly.

## Data-First

Direct access to database rows as files and directories. Use when you need column-level access, index lookups, bulk export, or structured data processing.

```
Read "mount/users/.info/count"                          # Row count
Read "mount/users/1.json"                               # Row as JSON
Read "mount/users/1/email"                              # Single column
Glob "mount/users/.by/email/alice@example.com/*"        # Index lookup
Read "mount/users/.by/status/active/.export/json"       # Filtered export
```

### Access Strategy by Table Size

| Size | Strategy |
|------|----------|
| **~100 rows or less** | Glob patterns and row-as-directory access are fine |
| **100 - 10,000 rows** | Prefer `.export/` over reading individual rows to avoid 1 query per row. Use `.by/`, `.filter/`, `.first/`, `.last/`, `.sample/` for selective access when possible |
| **10,000+ rows** | Large tables are limited to 10,000 rows by default; use `.all/` if you actually need all rows. Strongly prefer `.export/` over reading individual rows. Use `.by/`, `.filter/`, `.first/`, `.last/`, `.sample/` for selective access whenever possible |

Always check `.info/count` first to choose the right strategy.

See [data.md](data.md) for the full reference.

## Quick Reference

| Goal | Tool Call |
|------|-----------|
| **File-First** | |
| List files | `Glob "mount/app/*.md"` or `Glob "mount/app/**/*.md"` (recursive) |
| Read file | `Read "mount/app/file.md"` |
| Write file | `Write "mount/app/file.md"` with content |
| Delete file | `Bash "rm mount/app/file.md"` |
| Search | `Grep pattern="term" path="mount/app/"` |
| History versions | `Glob "mount/app/.history/file.md/*"` |
| Read old version | `Read "mount/app/.history/file.md/<timestamp>"` |
| **Data-First** | |
| Row count | `Read "mount/t/.info/count"` |
| Schema / columns | `Read "mount/t/.info/schema"` or `.info/columns` |
| Read row | `Read "mount/t/pk.json"` or `Read "mount/t/pk/column"` |
| Read multiple rows (small) | `Glob "mount/t/*.json"` then read individually |
| Read multiple rows (medium/large) | `Read "mount/t/.export/tsv"` (also `.export/json`, `.csv`, `.yaml`) |
| Navigate rows | `Glob "mount/t/.first/N/*"`, `.last/N/*`, or `.sample/N/*` |
| Index lookup | `Glob "mount/t/.by/col/val/*"` |
| Filtered export | `Read "mount/t/.by/col/val/.export/json"` |
| Update | `Write "mount/t/pk/col"` or `Write "mount/t/pk.json"` (PATCH) |
| Insert row | `Write "mount/t/new.json"` with JSON |
| Delete row | `Bash "rm mount/t/pk"` |

## Anti-Patterns

| Don't | Do Instead |
|-------|------------|
| **File-First** | |
| Put `status:` in frontmatter to track state | Use directories as states (`todo/`, `doing/`, `done/`), `mv` to transition |
| **Data-First** | |
| Read individual rows in a loop for large tables | Use `.export/json` or `.export/csv` for bulk access |
| Write full row JSON expecting replace semantics | JSON/CSV/TSV writes are **PATCH** -- only specified keys update |
| `Grep` across all rows of a large table | Use `.by/` index lookups for indexed columns |
| Glob a data-first directory without checking size | Read `.info/count` first, choose strategy based on table size |

## Database Management

When asked to **create, mount, fork, or manage a database or filesystem**, see [ops.md](ops.md) for tigerfs CLI commands.

## Detailed References

- [files.md](files.md) -- File-first: markdown apps, plain text apps, directories, and history
- [data.md](data.md) -- Data-first: row-as-file, row-as-directory, metadata, indexes, pipeline queries
- [recipes.md](recipes.md) -- Recipes: kanban boards, knowledge bases, session context, snippets
- [ops.md](ops.md) -- Operations: mount, create, fork, status, unmount
