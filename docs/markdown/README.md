# Markdown App

Store and access markdown files with metadata in PostgreSQL.

## What It Does

The Markdown App presents database rows as `.md` files with YAML frontmatter. Instead of navigating row directories and column files, you work with actual markdown files that feel natural to edit.

**Database row:**
```
id: 1, slug: "hello-world", author: "alice", date: "2024-01-15", content: "# Hello\n\nWelcome..."
```

**Becomes a file** `hello-world.md`:
```markdown
---
author: alice
date: 2024-01-15
---

# Hello

Welcome...
```

## Why Use It

- **Natural editing** - Use your favorite editor, not SQL
- **Git-friendly** - Track changes to content with version control
- **Agent-compatible** - AI agents can read/write content as files
- **Metadata included** - Frontmatter keeps author, date, tags with content
- **Full-text search** - Use `grep` to find content across all posts

## Quick Start

### Option 1: Create New App

Start fresh with a pre-configured table:

```bash
# Create a new markdown app called "notes"
echo "markdown" > /mnt/db/public/.build/notes

# Start writing
echo "---
title: Shopping List
---

- Milk
- Eggs
- Bread" > /mnt/db/public/notes/shopping.md
```

### Option 2: Add to Existing Table

If you already have a table with content:

```bash
# Create a markdown view on your posts table
echo "markdown" > /mnt/db/public/posts/.format/markdown

# Your posts are now available as .md files
ls /mnt/db/public/posts_md/
# hello-world.md  my-first-post.md  announcement.md

cat /mnt/db/public/posts_md/hello-world.md
```

## Usage

### Reading Files

```bash
# List all files
ls /mnt/db/public/posts_md/

# Read a file
cat /mnt/db/public/posts_md/hello-world.md

# Search across all files
grep -r "TODO" /mnt/db/public/posts_md/
```

### Creating Files

```bash
# Create with frontmatter
cat > /mnt/db/public/posts_md/new-post.md << 'EOF'
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
vim /mnt/db/public/posts_md/hello-world.md

# Or append content
echo "\n## Update\n\nMore content here." >> /mnt/db/public/posts_md/hello-world.md
```

### Renaming Files

```bash
# Rename updates the filename column in the database
mv /mnt/db/public/posts_md/old-name.md /mnt/db/public/posts_md/new-name.md
```

### Deleting Files

```bash
rm /mnt/db/public/posts_md/unwanted-post.md
```

## Column Mapping

### Automatic Detection

TigerFS automatically detects columns by convention:

| Role | Detected From |
|------|---------------|
| Filename | `name`, `filename`, `title`, `slug` |
| Body | `body`, `content`, `description`, `text` |
| Frontmatter | All other columns |

### Explicit Mapping

If your columns don't match conventions:

```bash
echo '{filename:post_slug,body:post_content}' > /mnt/db/public/posts/.format/markdown
```

## Checking Configuration

```bash
cat /mnt/db/public/posts/.format/markdown
```

Returns:
```json
{
  "format": "markdown",
  "view": "posts_md",
  "columns": {"filename": "slug", "body": "content"},
  "frontmatter": ["author", "date", "tags"]
}
```

## Use Cases

### Blog or Documentation

```bash
# Set up blog
echo "markdown" > /mnt/db/public/.build/blog

# Write posts
cat > /mnt/db/public/blog/welcome.md << 'EOF'
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

# Find all drafts
grep -l "draft: true" /mnt/db/public/blog/*.md
```

### Knowledge Base

```bash
# Create knowledge base
echo "markdown" > /mnt/db/public/.build/kb

# Organize with categories in frontmatter
cat > /mnt/db/public/kb/setup-guide.md << 'EOF'
---
title: Setup Guide
category: getting-started
last_updated: 2024-01-20
---

# Setup Guide

Follow these steps...
EOF

# Find all getting-started articles
grep -l "category: getting-started" /mnt/db/public/kb/*.md
```

### Meeting Notes

```bash
# Create meeting notes app
echo "markdown" > /mnt/db/public/.build/meetings

# Record a meeting
cat > /mnt/db/public/meetings/2024-01-15-standup.md << 'EOF'
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
for f in /mnt/db/public/blog/*.md; do
  echo "=== $f ==="
  cat "$f"
done

# Agent creates content
cat > /mnt/db/public/blog/ai-generated.md << 'EOF'
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
ls /mnt/db/public/posts/          # Native row-as-directory
ls /mnt/db/public/posts_md/       # Synthesized markdown

# For .build/ (new app)
ls /mnt/db/public/_notes/         # Native (underscore prefix)
ls /mnt/db/public/notes/          # Synthesized markdown
```

## Tips

1. **Frontmatter is automatic** - All columns except filename/body become frontmatter
2. **Control frontmatter** - Adjust the view's SELECT to include/exclude columns
3. **Timestamps work** - `created_at`, `modified_at` appear in frontmatter if in view
4. **Arrays supported** - `tags: [a, b, c]` works with PostgreSQL array columns
