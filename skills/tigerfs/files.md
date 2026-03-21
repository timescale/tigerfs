# Files Reference

Complete reference for working with TigerFS markdown apps, plain text apps, directories, and versioned history.

## Creating Apps

### Markdown App

```bash
Bash "echo 'markdown' > mount/.build/notes"
```

Creates:
- **`mount/notes/`** — synthesized markdown view (`.md` files)
- **`mount/_notes/`** — native table access (row-as-directory, `.info/`, etc.)

The view gets the clean name; the table gets an underscore prefix.

### Markdown App with History

```bash
Bash "echo 'markdown,history' > mount/.build/notes"
```

Same as above, plus a `_notes_history` hypertable and trigger that captures every update and delete. See [Versioned History](#versioned-history) below.

### Plain Text App

```bash
Bash "echo 'plaintext' > mount/.build/snippets"
```

Creates `mount/snippets/` with `.txt` files. No title, author, or headers columns — body only.

### View on Existing Table

Add a markdown view to a table that already has content:

```bash
Bash "echo 'markdown' > mount/posts/.format/markdown"
```

Creates **`mount/posts_md/`** alongside the existing `mount/posts/`. The view gets a `_md` suffix to avoid collision.

### Adding History Later

```bash
Bash "echo 'history' > mount/.build/notes"
```

Adds a history table and trigger to an existing app.

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

### Column Roles

TigerFS auto-detects column roles by naming convention (first match wins):

| Role | Detected From (priority order) | Purpose |
|------|-------------------------------|---------|
| **Filename** | `filename`, `name`, `title`, `slug` | The displayed filename (e.g., `hello.md`) |
| **Body** | `body`, `content`, `description`, `text` | File content below frontmatter |
| **Timestamps** | `modified_at`/`updated_at`, `created_at` | File mtime/ctime (not rendered as frontmatter) |
| **Extra Headers** | `headers` (JSONB column) | Merged into frontmatter as additional keys |
| **Frontmatter** | All remaining columns (excluding PK) | Rendered as YAML frontmatter fields |

**Important:** Timestamp columns set file times (visible in `ls -l`) but are **not** rendered as frontmatter.

### Custom Frontmatter (headers JSONB)

Tables created with `.build/` include a `headers` JSONB column. Any frontmatter keys that don't match a fixed column are stored here:

```markdown
---
title: My Post
author: alice
tags: [tutorial, intro]    # ← stored in headers JSONB (no "tags" column)
draft: false               # ← stored in headers JSONB (no "draft" column)
---
```

On write, unknown keys are collected into `headers`. On read, `headers` entries are merged into frontmatter after known columns, sorted alphabetically.

**Overwrite semantics:** The entire `headers` value is replaced on each write. Remove a key from frontmatter and it's removed from the database.

## Operations

### Reading

List files:

```
Glob "mount/notes/*.md"
Glob "mount/notes/**/*.md"    # recursive, includes subdirectories
```

Read a file:

```
Read "mount/notes/getting-started.md"
```

Search across files:

```
Grep pattern="TODO" path="mount/notes/"
Grep pattern="author: alice" path="mount/notes/" glob="*.md"
```

### Writing

Create a new file:

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

The filename in the path (`new-post.md`) becomes the `filename` column value. Frontmatter fields map to their respective columns.

Update an existing file:

```
Write "mount/notes/getting-started.md" with updated content
```

This is a full replacement of the file content (frontmatter + body), not a patch.

Rename a file:

```bash
Bash "mv mount/notes/old-name.md mount/notes/new-name.md"
```

Delete a file:

```bash
Bash "rm mount/notes/unwanted-post.md"
```

### Directories

Create a directory:

```bash
Bash "mkdir mount/notes/tutorials"
```

Write files in subdirectories:

```
Write "mount/notes/tutorials/intro.md" with content:
---
title: Introduction
---

Getting started with...
```

Parent directories are auto-created: writing `mount/notes/a/b/file.md` auto-creates `a/` and `a/b/`.

List files in a directory:

```
Glob "mount/notes/tutorials/*.md"
```

Rename a directory (atomically renames all files within):

```bash
Bash "mv mount/notes/tutorials mount/notes/guides"
```

## Native Table Access

The underlying table is always accessible alongside the synthesized view:

```
Read "mount/_notes/.info/schema"       # Table schema
Read "mount/_notes/.info/count"        # Row count
Read "mount/_notes/1.json"             # Row as JSON
Read "mount/_notes/1/body"             # Just the body column
Read "mount/_notes/1/headers"          # Just the headers JSONB
Glob "mount/_notes/.by/author/alice/*" # Index lookup
```

Use native access when you need: column-level access, index lookups, metadata (`.info/count`, `.info/schema`), or JSON format for structured processing.

## Schema Reference

### Markdown App Schema

```sql
CREATE TABLE _notes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);
```

### Plain Text App Schema

```sql
CREATE TABLE _snippets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);
```

Key columns:
- `filename` — the displayed filename (e.g., `hello.md`); may contain `/` for subdirectory paths
- `filetype` — `'file'` or `'directory'`; enables subdirectory support
- `title`, `author` — standard frontmatter columns (markdown only)
- `body` — the content below frontmatter (markdown) or entire content (plain text)
- `headers` — JSONB for arbitrary extra frontmatter keys (markdown only)
- `created_at`, `modified_at` — file timestamps, not rendered as frontmatter

## Tips

1. **Frontmatter is automatic** — all non-special columns become frontmatter fields
2. **Timestamps are file times** — `modified_at`/`created_at` show in `ls -l`, not in frontmatter
3. **Use Grep for discovery** — `Grep pattern="category: design" path="mount/notes/"` finds files by metadata
4. **Access native table for indexes** — use the native table path (`_notes/` or `posts/`) for `.by/` lookups
5. **headers JSONB is full-replace** — removing a key from frontmatter removes it from the database

---

# Versioned History

## Overview

Captures every UPDATE and DELETE as a timestamped snapshot. History is read-only — you cannot write to `.history/`. Requires the "history" feature on the app.

## Enabling History

At creation:

```bash
Bash "echo 'markdown,history' > mount/.build/notes"
```

Add to an existing app:

```bash
Bash "echo 'history' > mount/.build/notes"
```

How it works: a BEFORE UPDATE/DELETE trigger copies the old row into a `_notes_history` hypertable, keyed by the row's UUID and a version timestamp.

## Browsing by Filename

List files that have history:

```
Glob "mount/notes/.history/*"
```

List past versions of a specific file:

```
Glob "mount/notes/.history/hello.md/*"
```

Returns `.id` (the row UUID) followed by version timestamps, newest first.

Read a past version:

```
Read "mount/notes/.history/hello.md/2026-02-12T013000Z"
```

Returns the full file content (frontmatter + body) as it existed at that point.

## Version IDs

Timestamps are extracted from the row's UUIDv7, formatted as `2006-01-02T150405Z` (filesystem-safe, no colons). Listed newest-first.

## Row UUID (.id file)

```
Read "mount/notes/.history/hello.md/.id"
```

Returns the UUID that tracks this file across renames. If you rename `hello.md` to `intro.md`, the UUID stays the same and all history follows it.

## Browsing by UUID (Cross-Rename Tracking)

List all row UUIDs with history:

```
Glob "mount/notes/.history/.by/*"
```

List all versions for a specific row UUID:

```
Glob "mount/notes/.history/.by/<uuid>/*"
```

Read a past version by UUID:

```
Read "mount/notes/.history/.by/<uuid>/2026-02-12T013000Z"
```

`.by/` is only available at the root `.history/` level, not in subdirectory `.history/` directories.

## Subdirectory History

Each directory has its own `.history/` scoped to files in that directory:

```
Glob "mount/notes/tutorials/.history/*"         # only tutorial files
Glob "mount/notes/tutorials/.history/intro.md/*" # versions of intro.md
```

Subdirectory `.history/` does not include `.by/` (UUID browsing).

## Common Tasks

### "What changed in this file recently?"

1. List versions:
   ```
   Glob "mount/notes/.history/hello.md/*"
   ```
2. Read the most recent version (first timestamp after `.id`):
   ```
   Read "mount/notes/.history/hello.md/2026-02-24T150000Z"
   ```
3. Read the current file:
   ```
   Read "mount/notes/hello.md"
   ```
4. Compare the two — report what was added, removed, or changed.

### "What was different at time X?"

1. List versions and find the one closest to time X:
   ```
   Glob "mount/notes/.history/hello.md/*"
   ```
2. Read that version.
3. Read the current version.
4. Report the differences.

### "Show me the edit history of this file"

1. List all version timestamps:
   ```
   Glob "mount/notes/.history/hello.md/*"
   ```
2. Read each version (newest to oldest).
3. Summarize what changed at each point.

### Recovering a Past Version

Read the old version from `.history/`, then Write it back to the current file to restore it:

```
Read "mount/notes/.history/hello.md/2026-02-12T013000Z"
Write "mount/notes/hello.md" with the content from above
```

## Quick Reference

| Goal | Path |
|------|------|
| List files with history | `Glob "mount/app/.history/*"` |
| List versions of a file | `Glob "mount/app/.history/file.md/*"` |
| Read a past version | `Read "mount/app/.history/file.md/<timestamp>"` |
| Get row UUID | `Read "mount/app/.history/file.md/.id"` |
| List all row UUIDs | `Glob "mount/app/.history/.by/*"` |
| Versions by UUID | `Glob "mount/app/.history/.by/<uuid>/*"` |
| Read version by UUID | `Read "mount/app/.history/.by/<uuid>/<timestamp>"` |
| Subdirectory history | `Glob "mount/app/subdir/.history/*"` |
| Restore old version | Read from `.history/`, Write to current path |
