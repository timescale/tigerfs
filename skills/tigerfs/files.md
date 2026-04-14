# File-First Reference

Reference for file-first mode -- reading and writing markdown and plain text files backed by a database.

## Creating Apps

Apps create a file-first workspace -- a directory of markdown or text files backed by a database table. Create an app when you need a new shared workspace, knowledge base, or document store.

```bash
Bash "echo 'markdown' > mount/.build/notes"          # Markdown with frontmatter
Bash "echo 'markdown,history' > mount/.build/notes"  # With versioned history
Bash "echo 'plaintext' > mount/.build/snippets"      # Plain text, no frontmatter
Bash "echo 'history' > mount/.build/notes"           # Add history to existing app
```

Each app creates a file-first directory (`mount/notes/`) backed by a table in the `tigerfs` schema. Access the backing table via `mount/.tables/notes/`. To add file-first access to an existing data-first table: `echo 'markdown' > mount/posts/.format/markdown`

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

Every file-first app has a backing table in the `tigerfs` schema, accessible via the `.tables/` directory.

```
Read "mount/.tables/notes/.info/schema"       # Table schema
Read "mount/.tables/notes/.info/count"        # Row count
Read "mount/.tables/notes/.info/columns"      # Column names
Glob "mount/.tables/notes/.by/author/alice/*" # Index lookup
```

See [data.md](data.md) for the full data-first reference.

## Versioned History

Every update and delete is captured as a read-only timestamped snapshot in `.history/`. Requires the `history` feature (see [Creating Apps](#creating-apps)).

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

UUID browsing (`.by/`) is available at the root `.history/` level only, not in subdirectory `.history/` directories.

### Comparing and Recovering

1. List versions: `Glob "mount/notes/.history/hello.md/*"`
2. Read the version(s) you need: `Read "mount/notes/.history/hello.md/<timestamp>"`
3. Read the current file: `Read "mount/notes/hello.md"`
4. Compare and report differences.

For single-file recovery, use `/changes hello.md` to list recent edits with summaries, then `/undo <log_id>` to revert a specific one. For multi-file rollback to a known state, use `/undo <savepoint>` which handles all affected files atomically. `.history/` is best for reading and comparing old versions; use `.undo/` for restoring.

## User Identity

Each mount has an optional user identity used for log entries, savepoint auto-injection, and per-user undo filtering.

```
Read "mount/.info/user"                        # Read current identity
Bash "echo 'agent-7' > mount/.info/user"       # Set identity at runtime
```

Set at mount time: `--user-id agent-7` or `TIGERFS_USER_ID=agent-7`. See [ops.md](ops.md).

## Operation Log

Every create, edit, rename, and delete on a history-enabled app is recorded in `.log/`. Each entry has a stable log_id (UUIDv7), the operation type, affected file, and the user who performed it.

```
Glob "mount/notes/.log/.last/10/*"                          # Recent entries
Read "mount/notes/.log/.last/10/.export/json"               # Recent entries as JSON
Glob "mount/notes/.log/.by/user_id/agent-7/.last/5/*"      # By user
Glob "mount/notes/.log/.by/type/edit/.last/10/*"            # By type
```

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
