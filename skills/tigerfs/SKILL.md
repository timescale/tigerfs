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
- **`.tables/` directory** (e.g., `.tables/notes/`): Data-first access to backing tables in the `tigerfs` schema. **Use `mount/.tables/<workspace>/` when you need data-first access (indexed lookups, bulk export/import, DDL) on a file-first workspace's backing table -- the workspace path itself rejects data-first capability dirs with a hint pointing here.**

## Directory Structure

```
mount/
├── notes/                  # File-first (markdown workspace)
│   ├── hello.md
│   ├── tutorials/          # Subdirectories: ls shows only real files; .history/<file>/ remains path-accessible
│   ├── .history/           # Versioned history (root level only)
│   ├── .log/               # Operation log (root level only; pipeline hidden from ls)
│   ├── .savepoint/         # Named bookmarks for undo (root level only)
│   └── .undo/              # Preview and apply undo operations (root level only)
├── snippets/               # File-first (plain text workspace)
│   └── bash-loop.txt
├── .tables/                # Backing tables in tigerfs schema
│   └── notes/              # Data-first access to notes backing table
├── users/                  # Data-first (standalone table)
│   ├── .info/
│   ├── .by/
│   └── 1/  2/  3/ ...
├── .info/                  # Mount-level metadata (user identity)
└── .build/                 # Create new workspaces
```

## File-First

A transactional, shareable filesystem backed by a database. Multiple users and agents can read and write concurrently. Create workspaces with `.build/`:

```bash
Bash "echo 'markdown' > mount/.build/notes"
Bash "echo 'markdown,history' > mount/.build/notes"    # with versioned history
Bash "echo 'plaintext' > mount/.build/snippets"         # body-only, no frontmatter
```

See [files.md](files.md) for full details on schemas, column roles, directories, and history.

### What you can build

Because the filesystem is transactional and shared, it can implement collaborative workflows:

When asked to create a **task list, kanban board, todo, or project tracker**:
> Read [recipes.md](recipes.md) Recipe 4 and follow it exactly.
  Core principle: **directories = states** (`todo/`, `doing/`, `done/`), **`mv` = transitions**. Do NOT use `status` frontmatter fields.

When asked to create a **knowledge base, wiki, or documentation store**:
> Read [recipes.md](recipes.md) Recipe 5 and follow it exactly.

When asked to **save or resume session context**:
> Read [recipes.md](recipes.md) Recipe 6 and follow it exactly.

When asked to **keep a log of what you do**:
> Read [recipes.md](recipes.md) Recipe 7 and follow it exactly.

When asked to **revert, roll back, or undo changes**:
> Use savepoints and `.undo/`. See the Safe Editing section below and [files.md](files.md) for full details.

## Safe Editing with Savepoints

Before making multiple or risky edits, always create a savepoint:

```bash
Bash "echo '{\"description\":\"Before investigating bug #42\"}' > mount/workspace/.savepoint/before-investigation.json"
```

**When to create a savepoint:** investigating a bug, debugging, refactoring, multi-file edits, or any uncertain operation.

**When to undo:** user asks to revert, agent realizes the approach isn't working, or changes were made to wrong files.

**Before undoing:** always preview first and get user confirmation. Undo is destructive.

**Undo is undoable:** undo operations are logged (type='undo'), so you can undo an undo. Create a savepoint before a major undo for extra safety -- if the result isn't what was expected, undo back to that savepoint.

### Common Workflows

**"Create a savepoint"**
```
Bash "echo '{\"description\":\"<why>\"}' > mount/workspace/.savepoint/<name>.json"
```

**"What changed since the savepoint?"**
1. Read the summary: `Read "mount/workspace/.undo/to-savepoint/<name>/.info/summary"`
2. If <= 5 files affected: for each file, read its before and current state, summarize cumulative changes in English
3. If > 5 files: present the summary table (type, filename, user, timestamp)
4. If user wants raw diffs: `Bash "cd mount/workspace && diff -ru .undo/to-savepoint/<name> . -x '.*'"`

**"What changed in this file?"**
1. Get the file's stable UUID: `Read "mount/workspace/<dir>/.history/<file>/.id"` (for a root-level file: `Read "mount/workspace/.history/<file>/.id"`)
2. Find recent edits: `Read "mount/workspace/.log/.by/file_id/<uuid>/.last/5/.export/json"`
3. For each entry, read before and after, compare them, summarize in English (e.g., "Added 'Recent Updates' section with 3 new bullet points")
4. Present with log_ids so the user can pick one to undo. Note: a single logical edit by an agent may produce multiple log entries (see [How agent writes appear in the log](#how-agent-writes-appear-in-the-log)).

**"Show me the diff"**
- Savepoint: `Bash "cd mount/workspace && diff -ru .undo/to-savepoint/<name> . -x '.*'"`
- Single file: `Bash "diff -u --color mount/workspace/.log/<id>/before mount/workspace/.log/<id>/after"`

**"Undo to the savepoint"**
1. Read the summary: `Read "mount/workspace/.undo/to-savepoint/<name>/.info/summary"`
2. If <= 5 files: summarize cumulative changes per file in English
3. Present to user: "This will undo N changes: [summary]. Go ahead?"
4. Only if confirmed: `Bash "touch mount/workspace/.undo/to-savepoint/<name>/.apply"`

**"Undo this one change"**
1. Read the log entries around the change. If multiple entries belong to the same logical write (same `file_id`, adjacent timestamps), each must be undone — see [How agent writes appear in the log](#how-agent-writes-appear-in-the-log).
2. Read the log entry summary: `Read "mount/workspace/.undo/id/<log_id>/.info/summary"`
3. Show the diff: `Bash "diff -u --color mount/workspace/.log/<id>/before mount/workspace/.log/<id>/after"`
4. Present to user: "This will revert [description]. Go ahead?"
5. Only if confirmed: `Bash "touch mount/workspace/.undo/id/<log_id>/.apply"`. For a multi-entry group, apply each entry's undo (newest log_id first).

### How agent writes appear in the log

Different write methods produce different numbers of log entries:

- **Atomic-rename writes** (Claude's `Write`/`Edit`, many editors): the operation produces multiple adjacent log entries. Typically: a `create` of a temp file like `<file>.tmp.<pid>.<hash>`, then for an overwrite a `delete` of the existing target, then a `rename` of the temp to the final name. That's **2 entries for a new file, 3 for an overwrite**. The create and rename share the temp file's `file_id`; the delete (when present) carries the original target's `file_id`.
- **Direct writes** (shell redirects like `echo "..." > file`, in-place editors): a single `create` (new file) or `edit` (existing file) log entry. No temp file, no rename.

To revert a logical operation, identify the full group of entries:

1. Read the recent log: `Read "mount/workspace/.log/.last/N/.export/json"` (N large enough to span the operation).
2. Group entries by adjacent timestamps and the `<basename>.tmp.<pid>.<hash>` -> `<basename>` filename pattern. Adjacent create+rename entries with matching `file_id` mark the temp; an interleaved `delete` of the same `<basename>` belongs to the same logical operation even though its `file_id` differs.
3. Undo each entry in the group via `touch .undo/id/<log_id>/.apply`, applying the newest entry first so each step lands on the state the next step expects.

Each undo is itself logged with `type=undo` and is itself undoable.

For advanced cases (filtered undo, per-user undo, pipeline queries), construct paths directly from [files.md](files.md). See [recipes.md](recipes.md) Recipes 1-3 for complete workflow patterns.

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
| **100 - 1,000 rows** | Prefer `.export/` over reading individual rows to avoid 1 query per row. Use `.by/`, `.filter/`, `.first/`, `.last/`, `.sample/` for selective access when possible |
| **1,000+ rows** | Large tables are limited to 1,000 rows by default; use `.all/` if you actually need all rows. Strongly prefer `.export/` over reading individual rows. Use `.by/`, `.filter/`, `.first/`, `.last/`, `.sample/` for selective access whenever possible |

Always check `.info/count` first to choose the right strategy.

See [data.md](data.md) for the full reference.

## Quick Reference

| Goal | Tool Call |
|------|-----------|
| **File-First** | |
| List files | `Glob "mount/workspace/*.md"` or `Glob "mount/workspace/**/*.md"` (recursive) |
| Read file | `Read "mount/workspace/file.md"` |
| Write file | `Write "mount/workspace/file.md"` with content |
| Delete file | `Bash "rm mount/workspace/file.md"` |
| Search | `Grep pattern="term" path="mount/workspace/"` |
| History versions | `Glob "mount/workspace/.history/file.md/*"` |
| Read old version | `Read "mount/workspace/.history/file.md/<timestamp>"` |
| **Savepoints & Undo** | |
| Create savepoint | `Bash "echo '{\"description\":\"Before refactoring\"}' > mount/workspace/.savepoint/name.json"` |
| Diff all changes since savepoint | `Bash "cd mount/workspace && diff -ru .undo/to-savepoint/name . -x '.*'"` |
| Undo to savepoint | Preview first -- see "Undo to the savepoint" in Common Workflows above |
| Undo one change | Preview first -- see "Undo this one change" in Common Workflows above |
| File drift since a change | `Bash "diff -u --color mount/workspace/.log/<id>/before mount/workspace/.log/<id>/current"` |
| View recent log | `Read "mount/workspace/.log/.last/10/.export/json"` |
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

## Directory Scanning Safety

**Virtual directory layout:** `.history/`, `.log/`, `.savepoint/`, `.undo/` only appear in `ls` at the **workspace root** level, not inside subdirectories. Subdirectories' `ls` shows only real files; `.history/<file>/` and `.history/<file>/.id` remain path-accessible from subdirectories.

**Pipeline capabilities (`.by/`, `.filter/`, `.order/`, `.export/`, etc.) behave differently depending on where they appear:**
- **Inside `.log/` or `.savepoint/`**: accessible by explicit path but hidden from `ls` (prevents recursive scanner blowup on exponential pipeline branching).
- **At a file-first workspace root or in a workspace subdirectory**: rejected with `ErrInvalidPath` and a hint pointing at `mount/.tables/<workspace>/` — that's the entry point for data-first access to a workspace's backing table.
- **On a standalone data-first table** (e.g., `mount/users/`) or under `mount/.tables/<workspace>/`: fully accessible.

**Never recursively scan these directories** -- always use targeted access patterns from the Quick Reference above:
- `.log/` -- pipeline capabilities hidden from listing, use `.log/.last/10/.export/json`
- `.savepoint/` -- same as `.log/`, use explicit paths
- `.history/` -- one entry per file version; grows with every edit
- `.undo/` -- preview trees that mirror the affected file hierarchy
- `.by/<column>/` -- lists every distinct value; each expands to filtered rows
- `.filter/<column>/` -- same as `.by/` with higher limit
- `.export/` -- reading files triggers full table/query dumps
- `.import/` -- write interface; scanning could trigger unintended operations

**Safe to scan:**
- Regular files and subdirectories in a workspace (e.g., `Glob "mount/workspace/**/*.md"`)
- `.info/` -- small, fixed set of metadata files

## Anti-Patterns

| Don't | Do Instead |
|-------|------------|
| **File-First** | |
| Put `status:` in frontmatter to track state | Use directories as states (`todo/`, `doing/`, `done/`), `mv` to transition |
| Use `Write` or `Edit` to restore prior file content -- even for a single file. The tool's write-temp-then-rename pattern creates 2-3 new log entries instead of reverting the originals. | Use TigerFS's undo machinery: `.undo/to-savepoint/<name>/.apply` for groups of changes, `.undo/id/<log_id>/.apply` for one operation. See [How agent writes appear in the log](#how-agent-writes-appear-in-the-log) for grouping multi-entry writes. |
| Undo without previewing first | Always read `.info/summary` and get user confirmation before applying |
| **Data-First** | |
| Read individual rows in a loop for large tables | Use `.export/json` or `.export/csv` for bulk access |
| Write full row JSON expecting replace semantics | JSON/CSV/TSV writes are **PATCH** -- only specified keys update |
| `Grep` across all rows of a large table | Use `.by/` index lookups for indexed columns |
| Glob a data-first directory without checking size | Read `.info/count` first, choose strategy based on table size |

## Database Management

When asked to **create, mount, fork, or manage a database or filesystem**, see [ops.md](ops.md) for tigerfs CLI commands.

## Detailed References

- [files.md](files.md) -- File-first: workspaces with files and directories, and history
- [data.md](data.md) -- Data-first: row-as-file, row-as-directory, metadata, indexes, pipeline queries
- [recipes.md](recipes.md) -- Recipes: kanban boards, knowledge bases, session context, snippets, safe exploration, compare approaches, multi-agent undo
- [ops.md](ops.md) -- Operations: mount, create, fork, status, unmount
