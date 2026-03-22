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

Each app creates two directories: `mount/notes/` (file-first) and `mount/_notes/` (backing table for data-first access). To add file-first access to an existing data-first table: `echo 'markdown' > mount/posts/.format/markdown`

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

To see which columns a table has, use `Read "mount/_appname/.info/columns"`.

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

Every file-first app has a backing table accessible for data-first operations. The backing table name is the app name with an underscore prefix (e.g., `_notes/` for `notes/`).

```
Read "mount/_notes/.info/schema"       # Table schema
Read "mount/_notes/.info/count"        # Row count
Read "mount/_notes/.info/columns"      # Column names
Glob "mount/_notes/.by/author/alice/*" # Index lookup
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

To restore: read the old version from `.history/`, then Write it back to the current path.
