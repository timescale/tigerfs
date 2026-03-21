---
name: tigerfs
description: How to discover, read, write, and search data in a TigerFS-mounted PostgreSQL database using file tools.
---

# Using TigerFS

TigerFS mounts PostgreSQL databases as directories. You interact with data using Read, Write, Glob, and Grep — no SQL needed.

## Iron Law

**Never `ls` a table directory without checking `.info/count` first.**

Tables with more than 10,000 rows will return an error on `ls`. Always check the size, then use `.first/N/`, `.sample/N/`, or `.by/column/value/` for large tables.

## Common Workflows

When asked to create a **task list, kanban board, todo, or project tracker**:
→ Read [recipes.md](recipes.md) Recipe 1 and follow it exactly.
  Core principle: **directories = states** (`todo/`, `doing/`, `done/`), **`mv` = transitions**. Do NOT use `status` frontmatter fields.

When asked to create a **knowledge base, wiki, or documentation store**:
→ Read [recipes.md](recipes.md) Recipe 2 and follow it exactly.

When asked to **save or resume session context**:
→ Read [recipes.md](recipes.md) Recipe 3 and follow it exactly.

## Detecting a TigerFS Mount

Look for `.md`/`.txt` files (synth apps) or `.info/` in subdirectories (native tables):

```
mount/
├── notes/                  # Markdown app (synth)
│   ├── hello.md
│   ├── tutorials/
│   │   └── intro.md
│   └── .history/           # Versioned history (if enabled)
├── snippets/               # Plain text app (synth)
│   └── bash-loop.txt
├── _notes/                 # Native table backing notes/
│   ├── .info/
│   ├── .by/
│   └── 1/  2/  3/ ...
├── users/                  # Native table (no synth)
│   ├── .info/
│   ├── .by/
│   └── 1/  2/  3/ ...
└── .build/                 # Create new apps
```

## Markdown Apps (Primary Interface)

### Create an App

```bash
Bash "echo 'markdown' > mount/.build/notes"
Bash "echo 'markdown,history' > mount/.build/notes"    # with versioned history
Bash "echo 'plaintext' > mount/.build/snippets"         # body-only, no frontmatter
```

See [files.md](files.md) for full details on schemas, column roles, and plain text.

### Read and Write

```
Glob "mount/notes/*.md"
Read "mount/notes/hello.md"
```

```
Write "mount/notes/hello.md" with content:
---
title: Hello World
author: alice
---

Welcome to TigerFS.
```

```bash
Bash "rm mount/notes/old.md"
```

### Organize with Directories

```bash
Bash "mkdir mount/notes/tutorials"
Bash "mv mount/notes/draft.md mount/notes/published/draft.md"
```

```
Glob "mount/notes/**/*.md"
```

Auto-parent creation: writing `mount/notes/a/b/file.md` auto-creates `a/` and `a/b/`.

### Search

```
Grep pattern="TODO" path="mount/notes/"
Grep pattern="author: alice" path="mount/notes/" glob="*.md"
```

### History (if enabled)

List past versions of a file:

```
Glob "mount/notes/.history/hello.md/*"
```

Read an old version:

```
Read "mount/notes/.history/hello.md/2026-02-12T013000Z"
```

To answer "what changed recently?" — read the latest history version + current, compare the two. See [files.md](files.md) for version browsing, diff workflows, and recovery.

## Native Table Access (Escape Hatch)

For `.info/` metadata, `.by/` index lookups, JSON format, and column-level reads. Native tables expose the underlying database rows directly:

```
Read "mount/_notes/.info/count"     # Row count
Read "mount/_notes/.info/schema"    # CREATE TABLE DDL
Read "mount/_notes/1.json"          # Row as JSON
Read "mount/_notes/1/author"        # Single column value
Glob "mount/users/.by/email/alice@example.com/*"   # Index lookup
```

See [data.md](data.md) for the full reference.

## Quick Reference

| Goal | Tool Call |
|------|-----------|
| **Markdown Apps** | |
| Create markdown app | `Bash "echo 'markdown' > mount/.build/name"` |
| Create with history | `Bash "echo 'markdown,history' > mount/.build/name"` |
| Create plain text app | `Bash "echo 'plaintext' > mount/.build/name"` |
| List files | `Glob "mount/app/*.md"` |
| List all (recursive) | `Glob "mount/app/**/*.md"` |
| Read file | `Read "mount/app/file.md"` |
| Write file | `Write "mount/app/file.md"` with content |
| Delete file | `Bash "rm mount/app/file.md"` |
| Create directory | `Bash "mkdir mount/app/subdir"` |
| Move file/directory | `Bash "mv mount/app/old mount/app/new"` |
| Search content | `Grep pattern="term" path="mount/app/"` |
| Search frontmatter | `Grep pattern="key: val" path="mount/app/" glob="*.md"` |
| List history versions | `Glob "mount/app/.history/file.md/*"` |
| Read old version | `Read "mount/app/.history/file.md/<timestamp>"` |
| **Native Tables** | |
| Row count | `Read "mount/t/.info/count"` |
| Table schema | `Read "mount/t/.info/schema"` |
| Column names | `Read "mount/t/.info/columns"` |
| Read row (JSON) | `Read "mount/t/pk.json"` |
| Read column | `Read "mount/t/pk/column"` |
| First N rows | `Glob "mount/t/.first/N/*"` |
| Last N rows | `Glob "mount/t/.last/N/*"` |
| Random sample | `Glob "mount/t/.sample/N/*"` |
| Index lookup | `Glob "mount/t/.by/col/val/*"` |
| Update column | `Write "mount/t/pk/col"` with value |
| Update row (PATCH) | `Write "mount/t/pk.json"` with JSON |
| Insert row | `Write "mount/t/new.json"` with JSON |
| Delete row | `Bash "rm mount/t/pk"` |

## Anti-Patterns

| Don't | Do Instead |
|-------|------------|
| `Glob` on a large native table directory | Check `.info/count` first, use `.first/`, `.sample/`, `.by/` |
| Write full row JSON expecting replace semantics | JSON/CSV/TSV writes are **PATCH** — only specified keys update |
| `Grep` across all rows of a large native table | Use `.by/` index lookups for indexed columns |
| `ls` a table directory without checking size | Read `.info/count` first — large tables return EIO on `ls` |
| Put `status:` in frontmatter to track state | Use directories as states (`todo/`, `doing/`, `done/`), `mv` to transition |

## Detailed References

- [files.md](files.md) — Markdown apps, plain text apps, directories, and history
- [data.md](data.md) — Row-as-directory/row-as-file interface, metadata, indexes, pipeline queries
- [recipes.md](recipes.md) — Kanban boards, knowledge bases, session context, snippets
