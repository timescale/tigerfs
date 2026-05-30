# File-First Reference

Reference for file-first mode -- reading and writing markdown and plain text files backed by a database.

## Creating Workspaces

Create a workspace when you need a new shared directory of files backed by a database.

```bash
Bash "echo 'markdown' > mount/.build/notes"          # Markdown with frontmatter
Bash "echo 'markdown,history' > mount/.build/notes"  # With versioned history
Bash "echo 'plaintext' > mount/.build/snippets"      # Plain text, no frontmatter
Bash "echo 'history' > mount/.build/notes"           # Add history to existing workspace
```

Each workspace creates a directory (`mount/notes/`) backed by a table in the `tigerfs` schema. Access the backing table via `mount/.tables/notes/`. To add file-first access to an existing data-first table: `echo 'markdown' > mount/posts/.format/markdown`

## File Structure

### Markdown

Each `.md` file has YAML frontmatter (from columns) and a body (from the body column):

```markdown
---
title: Getting Started
author: alice
tags:
  - tutorial
  - intro
draft: false
---

# Getting Started

This is the body content stored in the text column...
```

### Plain Text

Plain text files have body content only, no frontmatter:

```
This is the entire file content.
No YAML frontmatter is parsed or generated.
```

### How Frontmatter Works

Frontmatter fields map to database columns. The write behavior depends on the column type:

- **Known columns** (e.g., `title`, `author`): Omitting a key from frontmatter **keeps the old value**. To clear a field, set it explicitly: `title: ""`
- **Headers JSONB** (e.g., `tags`, `draft` -- keys with no dedicated column): **Full-replace on each write**. Omitting a key removes it from the database.
- **Body**: Always replaced with what you write.
- **Timestamps** (`created_at`, `modified_at`): File times only -- they don't appear in frontmatter and can't be set via writes.

To see which columns a table has, use `Read "mount/.tables/appname/.info/columns"`.

## Writing Files

Standard file operations work as expected: Read, Write, Glob, Grep, `mv`, `rm`, `mkdir`. Key TigerFS-specific behaviors:

### Write Example

```
Write "mount/notes/new-post.md" with content:
---
title: New Post
author: bob
tags: [update]
---

# New Post

Content goes here...
```

See [How Frontmatter Works](#how-frontmatter-works) for write semantics (known columns, headers JSONB, body).

### Auto-Parent Directories

Writing `mount/notes/a/b/file.md` auto-creates `a/` and `a/b/`. No need to `mkdir` first.

### Atomic Directory Rename

`Bash "mv mount/notes/tutorials mount/notes/guides"` -- renames all files within atomically.

## Backing Table

Every workspace has a backing table in the `tigerfs` schema. For data-first access -- indexed lookups, bulk export/import, DDL on the backing table, full row-as-directory navigation -- use `mount/.tables/<workspace>/`. The file-first workspace path (`mount/<workspace>/`) presents files; data-first capability dirs (`.by/`, `.filter/`, `.order/`, `.columns/`, `.first/`, `.last/`, `.sample/`, `.export/`, `.import/`, `.indexes/`, `.modify/`, `.delete/`) at the workspace path return `ErrInvalidPath` with a hint pointing at the `.tables/` route.

```
Read "mount/.tables/notes/.info/schema"             # Table schema
Read "mount/.tables/notes/.info/count"              # Row count
Read "mount/.tables/notes/.info/columns"            # Column names
Glob "mount/.tables/notes/.by/author/alice/*"       # Index lookup
Read "mount/.tables/notes/.export/json"             # Bulk export
```

See [data.md](data.md) for the full data-first reference.

## Versioned History

Every update and delete is captured as a read-only timestamped snapshot in `.history/`. Requires the `history` feature (see [Creating Workspaces](#creating-workspaces)).

Each directory has its own `.history/` scoped to files in that directory:

```
Glob "mount/notes/.history/*"                             # History for root-level files
Glob "mount/notes/tutorials/.history/*"                   # History for tutorial files only
Glob "mount/notes/.history/hello.md/*"                    # Versions of a specific file (newest first)
Read "mount/notes/.history/hello.md/2026-02-12T013000Z"   # Read a past version
```

Timestamps are formatted as `2006-01-02T150405Z` (filesystem-safe, no colons).

### History Across Renames and Moves

Each file has a stable UUID that persists across renames and directory moves. If you rename `hello.md` to `intro.md` or move it to `archive/`, the UUID stays the same and all history follows it.

```
Read "mount/notes/archive/.history/intro.md/.id"   # Get the file's UUID (after rename + move)
Glob "mount/notes/.history/.by/<uuid>/*"           # All versions by UUID, including before rename
```

UUID browsing (`.history/.by/<file_id>/`) is addressable from `.history/` at every level (root and subdirectories), and always returns the same rows -- the lookup is keyed only on `file_id`, so the surrounding directory does not scope the result.

### Comparing and Recovering

1. List versions: `Glob "mount/notes/.history/hello.md/*"`
2. Read the version(s) you need: `Read "mount/notes/.history/hello.md/<timestamp>"`
3. Read the current file: `Read "mount/notes/hello.md"`
4. Compare and report differences.

For single-file recovery, look up the file's stable UUID via `Read "<dir>/.history/<file>/.id"`, then find recent edits with `Read ".log/.by/file_id/<uuid>/.last/5/.export/json"` and undo a specific entry via `touch .undo/id/<log_id>/.apply`. The `file_id` route is rename-invariant and indexed; it works for nested files where filename-based lookup cannot (the log's `filename` column stores `/`-bearing full paths, and `/` is the path separator -- a value containing it can't be expressed as a single directory entry). For multi-file rollback to a known state, use `touch .undo/to-savepoint/<name>/.apply` which handles all affected files atomically. `.history/` is best for reading and comparing old versions; use `.undo/` for restoring. See SKILL.md "Common Workflows" for the full multi-step agent behavior.

## User Identity

Each mount has an optional user identity used for log entries, savepoint auto-injection, and per-user undo filtering. The identity lives at the **mount root** `.info/` (not the workspace-level `.info/`, which holds backing-table metadata like `count`/`schema`):

```
Read "mount/.info/user"                        # Read current identity (empty when --user-id not set)
Bash "echo 'agent-7' > mount/.info/user"       # Set identity at runtime
```

Set at mount time: `--user-id agent-7` or `TIGERFS_USER_ID=agent-7`. See [ops.md](ops.md).

## Operation Log

Every create, edit, rename, and delete on a history-enabled workspace is recorded in `.log/`. Each entry has a stable log_id (UUIDv7), the operation type, affected file, and the user who performed it.

```
Glob "mount/notes/.log/.last/10/*"                          # Recent entries
Read "mount/notes/.log/.last/10/.export/json"               # Recent entries as JSON
Glob "mount/notes/.log/.by/user_id/agent-7/.last/5/*"       # By user (indexed)
Glob "mount/notes/.log/.by/type/edit/.last/10/*"            # By type (indexed)
Read "mount/notes/.log/.by/file_id/<uuid>/.last/5/.export/json"  # By file (indexed)
```

The indexed columns for `.log/.by/<col>/<val>/` queries are `file_id`, `user_id`, and `type` (composite indexes paired with `log_id ASC`). Use these for fast lookups. The `filename` column is explicitly blocked because it stores `/`-bearing full paths, and `/` is the path separator -- such values can't be expressed as a single directory entry. Use the `file_id` route after `Read "<dir>/.history/<file>/.id"` for filename-based queries. Other log columns are technically path-addressable but unindexed; prefer the indexed columns above.

### Diff Symlinks

Each log entry directory contains `before`, `after`, and `current` symlinks for diffing:

```bash
Bash "diff -u --color mount/notes/.log/<id>/before mount/notes/.log/<id>/after"     # What this edit changed
Bash "diff -u --color mount/notes/.log/<id>/before mount/notes/.log/<id>/current"   # Drift since this edit
```

History paths are per-directory: `tutorials/.history/getting-started.md/` (not `.history/tutorials/getting-started.md/`).

Log entry IDs use UUIDv7 display format: `2026-04-07T143000.123Z-zzz0063hd8e5r42` (timestamp + base36 suffix, filesystem-safe).

## Savepoints

Named bookmarks for undo-to-savepoint operations. Create one before risky edits.

```bash
Bash "echo '{\"description\":\"Before investigating bug\"}' > mount/notes/.savepoint/before-investigation.json"
```

Savepoint creation requires a format suffix (`.json`, `.tsv`, `.csv`, `.yaml`). JSON is preferred for agents. If `--user-id` is set, `user_id` is auto-injected.

```
Glob "mount/notes/.savepoint/*"                             # List savepoints
Read "mount/notes/.savepoint/before-investigation/description"  # Read description
Bash "rm mount/notes/.savepoint/old-savepoint"              # Delete savepoint
```

### Auto-Savepoints

TigerFS automatically creates savepoints when it detects an inactivity gap (default 30 minutes). Named `auto-<user>-<timestamp>` or `auto-<timestamp>`. Configure via `--auto-savepoint-interval` (set to `0` to disable).

## Undo

The `.undo/` directory provides a preview-then-apply interface for reversing operations.

### Three Modes

| Mode | Purpose | Listing |
|------|---------|---------|
| `.undo/id/<log_id>/` | Undo a single operation | Summary + apply only (use `.log/<id>/before` for diffs) |
| `.undo/to-id/<log_id>/` | Undo all operations after a log entry | Preview tree of affected files |
| `.undo/to-savepoint/<name>/` | Undo all operations after a savepoint | Preview tree of affected files |

### Preview and Apply

```bash
# What would undo do?
Read "mount/notes/.undo/to-savepoint/before-investigation/.info/summary"

# Diff all affected files
Bash "cd mount/notes && diff -ru .undo/to-savepoint/before-investigation . -x '.*'"

# Diff since a specific log entry
Bash "cd mount/notes && diff -ru .undo/to-id/<log_id> . -x '.*'"

# Single-file diff (drift since a specific change)
Bash "diff -u --color mount/notes/.log/<id>/before mount/notes/.log/<id>/current"

# Apply undo (destructive -- always preview first)
Bash "touch mount/notes/.undo/to-savepoint/before-investigation/.apply"
```

### Per-User Undo

Only undo a specific user's changes, preserving other users' work:

```bash
Bash "touch mount/notes/.undo/to-savepoint/before-investigation/.by/user_id/agent-7/.apply"
```

### Undo of Undo

Undo operations are logged. You can undo an undo by targeting its log entry. Create a savepoint before a major undo for extra safety.

### Timestamps After Undo

When undo restores a row (whether undoing an edit, rename, or delete), the file's `modified_at` is reset to the **restore time**, not the original write time. This is intentional: NFS and FUSE clients use mtime to invalidate readdir and getattr caches, so a fresh mtime is what causes restored entries to reappear correctly in `ls` output. Build tools (`make`, file watchers) also expect mtime to advance when content changes.

The original timestamp is still recoverable:
- Each write is logged with its real time in `.log/<id>` (read `.log/<id>/.info/summary` or pull the JSON from `.log/.by/file_id/<uuid>/`).
- Each pre-change snapshot in `.history/<file>/` carries the source row's `modified_at` at capture time.

If you need to answer "when was this content originally written," use the log or history -- not `stat`.
