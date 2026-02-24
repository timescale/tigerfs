# App Reference

Synthesized apps present database rows as files. Markdown apps use YAML frontmatter + body. Plain text apps are body-only.

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

Same as above, plus a `_notes_history` hypertable and trigger that captures every update and delete. See [history.md](history.md).

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
