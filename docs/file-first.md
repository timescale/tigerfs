# File-First Mode

File-first mode presents database tables as directories of files. Markdown files have YAML frontmatter mapped to columns. Plain text files are body-only. Multiple users and agents access the same files concurrently with transactional writes. With history enabled, every change is versioned and reversible -- create savepoints, preview changes, and undo when needed.

## Creating Workspaces

Workspaces tell TigerFS how to present a table as files. Write a format to `.build/` to create a new workspace:

```bash
echo "markdown,history" > /mnt/db/.build/notes    # Markdown with history (recommended)
echo "markdown" > /mnt/db/.build/notes            # Markdown without history
echo "plaintext" > /mnt/db/.build/snippets        # Plain text, no frontmatter
echo "history" > /mnt/db/.build/notes             # Add history to existing workspace
```

Each workspace creates a directory backed by a table in the `tigerfs` schema:

```
/mnt/db/notes/
├── hello.md                     # Your files
├── tutorials/
│   └── getting-started.md
├── .history/                    # Past versions (with history)
├── .log/                        # Operation log (with history)
├── .savepoint/                  # Bookmarks for undo (with history)
├── .undo/                       # Preview and apply undo (with history)
└── .info/                       # Workspace metadata
```

Access the backing table via `/mnt/db/.tables/notes/`.

To add file-first access to an existing data-first table:

```bash
echo "markdown" > /mnt/db/posts/.format/markdown
# Creates posts_md/ view (appends _md to avoid collision)
```

## File Formats

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

To see which columns a table has: `cat /mnt/db/.tables/notes/.info/columns`

## Reading and Writing

Standard file operations work as expected:

```bash
# List all files
ls /mnt/db/notes/

# Read a file
cat /mnt/db/notes/hello-world.md

# Search across all files
grep -r "TODO" /mnt/db/notes/

# Create with frontmatter
cat > /mnt/db/notes/new-post.md << 'EOF'
---
title: New Post
author: bob
tags: [update]
---

Content goes here...
EOF

# Edit with any editor
vim /mnt/db/notes/hello-world.md

# Rename (updates the filename column)
mv /mnt/db/notes/old-name.md /mnt/db/notes/new-name.md

# Delete
rm /mnt/db/notes/unwanted.md
```

## Directories

Workspaces support subdirectories. Create directories with `mkdir` and organize files into them:

```bash
mkdir /mnt/db/notes/tutorials

cat > /mnt/db/notes/tutorials/getting-started.md << 'EOF'
---
title: Getting Started
author: alice
---

Follow these steps...
EOF

ls /mnt/db/notes/tutorials/
# getting-started.md
```

**Auto-creation:** Writing a file with a path automatically creates parent directories. Writing to `notes/a/b/c.md` auto-creates `a/` and `a/b/`.

**Atomic rename:** Renaming a directory atomically renames all files within it:

```bash
mv /mnt/db/notes/tutorials /mnt/db/notes/guides
# All files under tutorials/ are now under guides/
```

## Column Mapping

### Automatic Detection

TigerFS automatically detects column roles by naming convention (first match wins):

| Role | Detected From (priority order) | Required |
|------|-------------------------------|----------|
| Filename | `filename`, `name`, `title`, `slug` | Yes |
| Body | `body`, `content`, `description`, `text` | Yes |
| Timestamps | `modified_at`, `updated_at` (modification time); `created_at` (creation time) | No |
| Extra Headers | `headers` (JSONB, merged into frontmatter) | No |
| Frontmatter | All remaining columns (excluding primary key) | -- |

Timestamp columns set file modification/creation times (visible in `ls -l`) but are **not** rendered as frontmatter.

### Explicit Mapping (Planned)

Currently, column roles are always auto-detected by naming convention. A future release will allow explicit mapping for tables whose column names don't match the conventions.

### Custom Frontmatter (Extra Headers)

Tables created with `.build/` include a `headers JSONB` column for storing arbitrary frontmatter keys beyond the fixed schema columns.

- On **read**, entries from `headers` are merged into YAML frontmatter after known columns, sorted alphabetically.
- On **write**, frontmatter keys that don't match a known column are collected into `headers`.
- **Overwrite semantics** -- the entire `headers` value is replaced on each write. Omitting a key removes it.

Example: `title` and `author` are stored in their own columns. `tags` and `draft` (no dedicated columns) are stored together in `headers` JSONB.

## Backing Table Access

The underlying table is accessible for data-first operations:

```bash
# .build/ workspaces
ls /mnt/db/.tables/notes/      # Data-first access to backing table
ls /mnt/db/notes/              # File-first access

# .format/ views
ls /mnt/db/posts/              # Data-first (original table)
ls /mnt/db/posts_md/           # File-first (synthesized view)
```

See [data-first.md](data-first.md) for the full data-first reference.

## History and Undo

Workspaces created with `history` get automatic versioning, an operation log, savepoints, and undo. The dot-directories (`.history/`, `.log/`, `.savepoint/`, `.undo/`) are shown in the directory structure above.

```bash
# Create savepoint, work, review, undo if needed
echo '{"description":"Before refactoring"}' > /mnt/db/notes/.savepoint/before-refactor.json
# ... make changes ...
diff -ru /mnt/db/notes/.undo/to-savepoint/before-refactor /mnt/db/notes/ -x '.*'
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.apply
```

See [History](history.md) for the full guide on version browsing, savepoints, undo, and recovery.

## Further Reading

- [History](history.md) -- Version browsing, savepoints, undo, and recovery
- [Data-First](data-first.md) -- Direct table access via row-as-file and row-as-directory
- [Recipes](../skills/tigerfs/recipes.md) -- Blog, knowledge base, task boards, and other patterns
