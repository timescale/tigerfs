# Markdown App Reference

Synthesized markdown apps present database rows as `.md` files with YAML frontmatter. Each file is a row — frontmatter maps to columns, the body maps to a text column.

## Creating a Markdown App

### New App (.build/)

Create a fresh table with a pre-configured schema:

```bash
echo "markdown" > mount/.build/notes
```

This creates:
- **`mount/notes/`** — synthesized markdown view (the `.md` files you interact with)
- **`mount/_notes/`** — native table access (row-as-directory, `.info/`, etc.)

The view gets the clean name; the table gets an underscore prefix.

### Existing Table (.format/)

Add a markdown view to a table that already has content:

```bash
echo "markdown" > mount/posts/.format/markdown
```

This creates:
- **`mount/posts_md/`** — synthesized markdown view
- **`mount/posts/`** — native table unchanged

The view gets a `_md` suffix to avoid collision with the existing table name.

## File Structure

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

### Column Roles

TigerFS auto-detects column roles by naming convention (first match wins):

| Role | Detected From (priority order) | Purpose |
|------|-------------------------------|---------|
| **Filename** | `filename`, `name`, `title`, `slug` | Becomes the `.md` filename (slugified) |
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

## Reading

### List Files

```
Glob "mount/notes/*.md"
```

### Read a File

```
Read "mount/notes/getting-started.md"
```

Returns the full file with frontmatter and body.

### Search Across Files

```
Grep pattern="TODO" path="mount/notes/"
Grep pattern="author: alice" path="mount/notes/" glob="*.md"
Grep pattern="tutorial" path="mount/notes/"
```

## Writing

### Create a New File

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

The filename in the path (`new-post`) becomes the `filename` column value. The frontmatter fields map to their respective columns.

### Update an Existing File

```
Write "mount/notes/getting-started.md" with updated content
```

The entire file content (frontmatter + body) is parsed and the corresponding row is updated. This is a full replacement of the file content, not a patch.

### Rename a File

```bash
mv mount/notes/old-name.md mount/notes/new-name.md
```

Updates the `filename` column in the database.

### Delete a File

```bash
rm mount/notes/unwanted-post.md
```

Deletes the corresponding row from the database. Permanent — no undo.

## Directories

Synth apps support subdirectories for organizing files:

### Create a Directory

```bash
Bash "mkdir mount/notes/tutorials"
```

### Write Files in Subdirectories

```
Write "mount/notes/tutorials/intro.md" with content:
---
title: Introduction
---

Getting started with...
```

Parent directories are auto-created: writing `mount/notes/a/b/file.md` auto-creates `a/` and `a/b/`.

### List Files in a Directory

```
Glob "mount/notes/tutorials/*.md"
```

### List All Files Recursively

```
Glob "mount/notes/**/*.md"
```

### Rename a Directory

```bash
Bash "mv mount/notes/tutorials mount/notes/guides"
```

Atomically renames the directory and all files within it.

## Native Table Access

The underlying table is always accessible alongside the synthesized view:

**`.build/` apps:**
```
Glob "mount/_notes/*"                  # Native row directories
Read "mount/_notes/.info/schema"       # Table schema
Read "mount/_notes/.info/count"        # Row count
Read "mount/_notes/1.json"             # Row as JSON
Read "mount/_notes/1/body"             # Just the body column
Read "mount/_notes/1/headers"          # Just the headers JSONB
```

**`.format/` views:**
```
Glob "mount/posts/*"                   # Native row directories
Read "mount/posts/.info/schema"        # Table schema
Read "mount/posts/1.json"              # Row as JSON
```

This is useful when you need:
- Column-level access to specific fields
- Index lookups on the underlying table
- Metadata (`.info/count`, `.info/schema`)
- JSON format for structured processing

## Schema for .build/ Apps

Tables created via `.build/` have this schema:

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

Key columns:
- `filename` — the `.md` filename (without extension), used for addressing; may contain `/` for subdirectory paths
- `filetype` — `'file'` or `'directory'`; enables subdirectory support
- `title`, `author` — standard frontmatter columns
- `body` — the markdown content below the frontmatter
- `headers` — JSONB for arbitrary extra frontmatter keys
- `created_at`, `modified_at` — file timestamps, not rendered as frontmatter

## Tips

1. **Frontmatter is automatic** — all non-special columns become frontmatter fields
2. **Timestamps are file times** — `modified_at`/`created_at` show in `ls -l`, not in frontmatter
3. **Use grep for discovery** — `Grep pattern="category: design" path="mount/notes/"` finds files by metadata
4. **Access native table for indexes** — if you need `.by/` lookups, use the native table path (`_notes/` or `posts/`)
5. **headers JSONB is full-replace** — removing a key from frontmatter removes it from the database
