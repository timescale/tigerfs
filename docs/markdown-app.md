# Markdown App

Store markdown files in PostgreSQL — edit them as plain files, share them across tools, and get transactional writes for free.

## What It Does

Write and organize `.md` files the way you normally would — with any text editor, shell script, or AI agent. The Markdown App stores each file as a database row with YAML frontmatter mapped to columns, so your content is:

- **Shareable** — multiple users, editors, and agents access the same files simultaneously
- **Transactionally safe** — every write is an atomic database operation; no partial saves or corrupted files
- **Searchable** — use `grep` across all files, or query metadata via SQL on the underlying table
- **Metadata-rich** — frontmatter fields (author, tags, etc.) are real columns you can index and query

Under the hood, each markdown file is stored as a database row — frontmatter fields map to columns and the body maps to a text column:

**`hello-world.md`:**
```markdown
---
title: Hello
author: alice
draft: false
---

# Hello

Welcome...
```

**Stored as:**
```
id: 1, filename: "hello-world", title: "Hello", author: "alice", headers: {"draft": false}, body: "# Hello\n\nWelcome..."
```

## Quick Start

### Option 1: Create New App

Start fresh with a pre-configured table:

```bash
# Create a new markdown app called "notes"
echo "markdown" > /mnt/db/.build/notes

# Start writing
echo "---
title: Shopping List
---

- Milk
- Eggs
- Bread" > /mnt/db/notes/shopping.md
```

### Option 2: Add to Existing Table

If you already have a table with content:

```bash
# Create a markdown view on your posts table
echo "markdown" > /mnt/db/posts/.format/markdown

# Your posts are now available as .md files
ls /mnt/db/posts_md/
# hello-world.md  my-first-post.md  announcement.md

cat /mnt/db/posts_md/hello-world.md
```

## Naming Conventions

The two creation methods use different naming conventions:

| Method | Synthesized View | Native Table | Example |
|--------|------------------|--------------|---------|
| `.build/notes` | `notes/` | `_notes/` | `/notes/hello.md` and `/_notes/1/body` |
| `posts/.format/markdown` | `posts_md/` | `posts/` | `/posts_md/hello.md` and `/posts/1/body` |

**`.build/` (new app):** View gets the clean name, table gets underscore prefix. This is the primary method.

**`.format/` (existing table):** View gets `_md` suffix to avoid collision with the existing table name.

## Usage

These examples use a `.build/` app called `notes`. The same operations work with `.format/` views (just use `posts_md/` instead of `notes/`).

### Reading Files

```bash
# List all files
ls /mnt/db/notes/

# Read a file
cat /mnt/db/notes/hello-world.md

# Search across all files
grep -r "TODO" /mnt/db/notes/
```

### Creating Files

```bash
# Create with frontmatter
cat > /mnt/db/notes/new-post.md << 'EOF'
---
author: bob
tags: [tutorial, getting-started]
---

# Getting Started

This is my new post...
EOF
```

### Editing Files

```bash
# Edit with any editor
vim /mnt/db/notes/hello-world.md

# Or append content
echo "\n## Update\n\nMore content here." >> /mnt/db/notes/hello-world.md
```

### Renaming Files

```bash
# Rename updates the filename column in the database
mv /mnt/db/notes/old-name.md /mnt/db/notes/new-name.md
```

### Deleting Files

```bash
rm /mnt/db/notes/unwanted-post.md
```

### Organizing with Directories

Synthesized apps support subdirectories. Create directories with `mkdir` and organize files into them:

```bash
# Create a directory
mkdir /mnt/db/notes/tutorials

# Create a file in the directory
cat > /mnt/db/notes/tutorials/getting-started.md << 'EOF'
---
title: Getting Started
author: alice
---

# Getting Started

Follow these steps...
EOF

# List files in the directory
ls /mnt/db/notes/tutorials/
# getting-started.md
```

**Auto-creation:** Writing a file with a path automatically creates parent directories. Writing to `notes/a/b/c.md` auto-creates the `a/` and `a/b/` directories.

**Directory rename:** Renaming a directory atomically renames all files within it:

```bash
mv /mnt/db/notes/tutorials /mnt/db/notes/guides
# All files under tutorials/ are now under guides/
```

**`.format/` views:** Directory support also works with `.format/` views, as long as the underlying table has a `filetype` column.

## Column Mapping

### Automatic Detection

TigerFS automatically detects column roles by naming convention (first match wins):

| Role | Detected From (priority order) | Required |
|------|-------------------------------|----------|
| Filename | `filename`, `name`, `title`, `slug` | Yes |
| Body | `body`, `content`, `description`, `text` | Yes |
| Timestamps | `modified_at`, `updated_at` (modification time); `created_at` (creation time) | No |
| Extra Headers | `headers` (JSONB, merged into frontmatter) | No |
| Frontmatter | All remaining columns (excluding primary key) | — |

Timestamp columns are used for file modification/creation times (visible in `ls -l`), but are **not** rendered as frontmatter fields.

### Explicit Mapping (Planned)

Currently, column roles are always auto-detected by naming convention. A future release will allow explicit mapping for tables whose column names don't match the conventions:

```bash
# Planned — not yet implemented
echo '{filename:post_slug,body:post_content}' > /mnt/db/posts/.format/markdown
```

### Checking Configuration (Planned)

A future release will allow reading the current column mapping by reading the `.format/markdown` control file:

```bash
# Planned — not yet implemented
cat /mnt/db/posts/.format/markdown
```

## Custom Frontmatter (Extra Headers)

Tables created with `.build/` include a `headers JSONB` column for storing arbitrary frontmatter keys beyond the fixed schema columns.

**How it works:**

- On **read**, entries from the `headers` column are merged into YAML frontmatter after the known columns, sorted alphabetically by key.
- On **write**, any frontmatter keys that don't match a known column are collected into the `headers` JSONB value.
- **Overwrite semantics** — the entire `headers` value is replaced on each write. If you remove a key from the frontmatter, it's removed from the database.

**Example:**

```bash
cat > /mnt/db/blog/welcome.md << 'EOF'
---
title: Welcome to My Blog
author: alice
tags: [intro, welcome]
draft: false
---

# Welcome

Thanks for visiting...
EOF
```

Here `title` and `author` are stored in their own columns. `tags` and `draft` — which don't have dedicated columns — are stored together in the `headers` JSONB column.

Reading the file back:

```markdown
---
title: Welcome to My Blog
author: alice
draft: false
tags:
  - intro
  - welcome
---

# Welcome

Thanks for visiting...
```

Known columns (`title`, `author`) appear first in schema order, then extra headers (`draft`, `tags`) appear alphabetically.

## Use Cases

### Blog or Documentation

```bash
# Set up blog
echo "markdown" > /mnt/db/.build/blog

# Write posts
cat > /mnt/db/blog/welcome.md << 'EOF'
---
title: Welcome to My Blog
author: alice
date: 2024-01-15
tags: [intro, welcome]
draft: false
---

# Welcome

Thanks for visiting...
EOF

# Find all drafts (draft and tags are stored in the headers JSONB column)
grep -l "draft: true" /mnt/db/blog/*.md
```

### Knowledge Base

```bash
# Create knowledge base
echo "markdown" > /mnt/db/.build/kb

# Organize into directories
mkdir /mnt/db/kb/getting-started
mkdir /mnt/db/kb/reference

cat > /mnt/db/kb/getting-started/setup-guide.md << 'EOF'
---
title: Setup Guide
last_updated: 2024-01-20
---

# Setup Guide

Follow these steps...
EOF

cat > /mnt/db/kb/reference/api.md << 'EOF'
---
title: API Reference
---

# API Reference

Endpoints and usage...
EOF

# List a section
ls /mnt/db/kb/getting-started/
# setup-guide.md

# Search across all sections
grep -r "TODO" /mnt/db/kb/
```

### Meeting Notes

```bash
# Create meeting notes app
echo "markdown" > /mnt/db/.build/meetings

# Record a meeting
cat > /mnt/db/meetings/2024-01-15-standup.md << 'EOF'
---
date: 2024-01-15
attendees: [alice, bob, charlie]
type: standup
---

# Daily Standup - Jan 15

## Updates
- Alice: Finished API work
- Bob: Working on frontend
- Charlie: Reviewing PRs

## Action Items
- [ ] Alice to deploy API
- [ ] Bob to demo on Friday
EOF
```

### Agent Workflows

AI agents can read and write content naturally:

```bash
# Agent reads all posts
for f in /mnt/db/blog/*.md; do
  echo "=== $f ==="
  cat "$f"
done

# Agent creates content
cat > /mnt/db/blog/ai-generated.md << 'EOF'
---
title: AI-Generated Summary
author: assistant
generated: true
---

Based on my analysis...
EOF
```

## Native Table Access

The underlying table is still accessible for SQL operations:

```bash
# For .format/ (existing table)
ls /mnt/db/posts/          # Native row-as-directory
ls /mnt/db/posts_md/       # Synthesized markdown

# For .build/ (new app)
ls /mnt/db/_notes/         # Native (underscore prefix)
ls /mnt/db/notes/          # Synthesized markdown
```

## Tips

1. **Frontmatter is automatic** — All columns except filename, body, timestamps, and primary key become frontmatter
2. **Extra headers** — Add a `headers JSONB` column to store arbitrary frontmatter keys beyond the fixed schema
3. **Timestamps are file times** — `modified_at` and `created_at` set file mtime/ctime (visible in `ls -l`), not rendered as frontmatter
4. **Arrays supported** — `tags: [a, b, c]` works with PostgreSQL array columns
