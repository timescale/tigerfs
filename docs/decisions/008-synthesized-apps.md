# ADR-008: Synthesized Apps

**Status:** Accepted
**Date:** 2026-01-31
**Author:** Mike Freedman

## Context

TigerFS exposes PostgreSQL databases as mountable filesystems where tables appear as directories, rows as subdirectories, and columns as files. This "native" format works well for structured data access but doesn't match how users naturally think about certain content types.

Users want to store content like markdown files, notes, or task lists in a database and access them as actual files—not as row directories with column files. For example, a blog post should appear as `hello-world.md` with YAML frontmatter, not as a directory `1/` containing separate `title`, `author`, and `content` files.

**Current (native) format:**
```
/mnt/db/public/posts/
├── 1/
│   ├── slug
│   ├── content
│   └── author
└── 2/
    └── ...
```

**Desired (synthesized) format:**
```
/mnt/db/public/posts_md/
├── hello-world.md
├── my-first-post.md
└── announcement.md
```

Where each `.md` file contains YAML frontmatter synthesized from column values plus the body content.

## Decision

Implement **Synthesized Apps** that present database tables as synthesized files (markdown, plain text, task lists) using PostgreSQL views as the configuration mechanism.

### Supported Formats

| Format | Extension | Structure | Use Case |
|--------|-----------|-----------|----------|
| Markdown | `.md` | filename + YAML frontmatter + body | Blog posts, docs, notes with metadata |
| Plain Text | `.txt` | filename + body (no frontmatter) | Simple notes, logs, snippets |
| Tasks | `.md` | `{number}-{name}-{status}.md` | Ordered task lists with status tracking |

### Two Creation Methods

| Method | What it does | Use when |
|--------|--------------|----------|
| `.format/` | Creates view (+ triggers) on existing table | Table already exists |
| `.build/` | Creates table + view (+ triggers) from scratch | Starting fresh |

```bash
# Method 1: Add format to existing table
echo "markdown" > /mnt/db/public/posts/.format/markdown
# Creates: posts_md view

# Method 2: Build complete app from scratch
echo "markdown" > /mnt/db/public/.build/posts
# Creates: _posts table + posts view
```

### Architecture: Standalone Views

Views appear as standalone directories in the filesystem. The view definition IS the configuration—no separate metadata tables or hidden schemas.

```sql
CREATE VIEW posts_md AS
SELECT slug AS filename, content AS body, author, date, tags
FROM posts;

COMMENT ON VIEW posts_md IS 'tigerfs:md';
```

**Key properties:**
- No `_tigerfs` schema or metadata tables
- View definition controls column mapping
- Format detected by naming convention (`*_md`, `*_txt`, `*_tasks`) or column pattern
- Simple views are auto-updatable (PostgreSQL handles writes automatically)
- Each view is independent

### Format Detection

| View characteristics | Rendered as |
|---------------------|-------------|
| Name ends with `_md` | Markdown files |
| Name ends with `_txt` | Plain text files |
| Name ends with `_tasks` (or `_todo`, `_items`) | Task files |
| Has `filename` + `body` + other columns | Markdown files |
| Has `filename` + `body` only | Plain text files |
| Has `number` + `name` + `status` + `body` columns | Task files |
| None of the above | Native format |

**Precedence:** Explicit suffix takes priority over column convention detection.

### Naming Conventions

**`.format/` (existing table):** View gets suffix, table keeps name
- Table: `posts` → View: `posts_md`
- Access: `/posts_md/hello.md` (synthesized), `/posts/1/title` (native)

**`.build/` (new app):** View gets clean name, table prefixed with `_`
- Table: `_posts` → View: `posts`
- Access: `/posts/hello.md` (synthesized), `/_posts/1/title` (native)

### Markdown Synthesis

**File structure:**
```markdown
---
author: alice
date: 2024-01-15
tags: [intro, welcome]
---

# Hello World

This is the content from the body column...
```

**Column mapping:**
- `filename` column → becomes the `.md` filename
- `body` column → becomes markdown content after frontmatter
- All other columns → become YAML frontmatter key-value pairs

**Convention detection:**
- Filename: `name`, `filename`, `title`, `slug` (first match)
- Body: `body`, `content`, `description`, `text` (first match)
- Frontmatter: all remaining columns

### Tasks Format

**Filename format:** `{number}-{name}-{status_symbol}.md`

**Example:**
```
/work_tasks/
├── 1-setup-project-x.md
├── 1.01-create-repo-x.md
├── 1.02-add-readme-~.md
├── 2-implement-feature-o.md
└── 2.01-write-tests-o.md
```

**Status values:**

| Stored (canonical) | Symbol (filename) | Aliases (input) |
|--------------------|-------------------|-----------------|
| `todo` | `o` | `todo`, `pending`, `new`, `o` |
| `doing` | `~` | `doing`, `active`, `wip`, `~` |
| `done` | `x` | `done`, `complete`, `finished`, `x` |

**Automatic triggers:**
1. **Shift-on-insert:** When inserting/moving to an occupied slot, siblings shift down to make room
2. **Status timestamps:** When status changes, corresponding timestamp (`todo_at`, `doing_at`, `done_at`) is set
3. **modified_at:** Updated on any change

**No automatic gap-close:** Gaps are not closed automatically on delete or move. Users explicitly compact numbering via `.renumber` when desired.

### Hierarchical Numbering

**Numbers only:** Task numbers use integers at each level (no letters). Examples: `1`, `1.2`, `10.15`.

**Storage:** Database stores unpadded TEXT with a generated `INT[]` column for correct sorting:
- `number = '1.2'` (user-facing, stored in TEXT)
- `number_sort = {1,2}` (auto-generated for sorting)

**Display:** Filenames use dynamic per-level zero-padding based on the maximum value at each level:

| Level 0 max | Level 1 max | Filename examples |
|-------------|-------------|-------------------|
| 5 | 15 | `1.01-task-o.md`, `5.15-task-o.md` |
| 150 | 5 | `001.1-task-o.md`, `150.5-task-o.md` |

**Frontmatter:** Shows unpadded numbers (`number: "1.2"`)

**Shifting:** Scoped to siblings at the same level under the same parent.

**Cross-parent moves:** Supported. Moving `1.2` to `2.1` works; gap at old location remains until explicit renumber.

**Depth changes:** Allowed. Moving `2.1` to `2.1.1` creates an "orphan" (child of non-existent parent), which is valid for workflows where siblings will be added later.

### Explicit Renumbering

Gaps are closed explicitly via the `.renumber` file:

```bash
touch .renumber            # Compact all (close gaps, preserve hierarchy)
echo "2" > .renumber       # Compact only 2.* subtree
echo "2.1" > .renumber     # Compact only 2.1.* subtree
```

**Example:**
```
Before: 1, 3, 5, 5.1, 5.5
After:  1, 2, 3, 3.1, 3.2
```

**Future extension:** Support `echo "flatten" > .renumber` to collapse hierarchy into flat numbering (e.g., `1.1, 1.2, 2.1` → `1, 2, 3`).

### Easy Setup

**Convention-based:**
```bash
echo "markdown" > /mnt/db/posts/.format/markdown
echo "tasks" > /mnt/db/work/.format/tasks
```

**Explicit mapping (when conventions don't match):**
```bash
echo '{filename:slug,body:content}' > /mnt/db/posts/.format/markdown
```

Frontmatter always includes all remaining columns. To control which columns appear, adjust the view's SELECT clause.

### Read/Write Support

Synthesized files are **read-write**.

| Operation | SQL |
|-----------|-----|
| Edit file | `UPDATE view SET ... WHERE filename = 'x'` |
| Create file | `INSERT INTO view (filename, body, ...) VALUES (...)` |
| Delete file | `DELETE FROM view WHERE filename = 'x'` |
| Rename file | `UPDATE view SET filename = 'new' WHERE filename = 'old'` |

PostgreSQL auto-updatable views handle writes for simple views (single table, no aggregates, no GROUP BY). Complex views require INSTEAD OF triggers.

### Default Tables (`.build/`)

**Markdown:**
```sql
CREATE TABLE _posts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filename TEXT UNIQUE NOT NULL,
  title TEXT,
  author TEXT,
  body TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  modified_at TIMESTAMPTZ DEFAULT now()
);
```

**Tasks:**
```sql
CREATE TABLE _work (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  number TEXT UNIQUE NOT NULL
      CHECK (number ~ '^[1-9][0-9]*(\.[1-9][0-9]*)*$'),  -- positive integers, no leading zeros
  number_sort INT[] GENERATED ALWAYS AS (string_to_array(number, '.')::int[]) STORED,
  name TEXT NOT NULL,
  description TEXT,
  status TEXT DEFAULT 'todo',
  assignee TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  modified_at TIMESTAMPTZ DEFAULT now(),
  todo_at TIMESTAMPTZ,
  doing_at TIMESTAMPTZ,
  done_at TIMESTAMPTZ
);

CREATE INDEX ON _work (number_sort);
```

## Consequences

### Benefits

1. **Natural file access:** Content appears as actual files, not database rows
2. **PostgreSQL-native:** Uses views, triggers, and comments—no custom infrastructure
3. **Portable:** `pg_dump` captures everything; works across TigerFS instances
4. **Flexible:** Users can write custom views for complex mappings
5. **Discoverable:** Views are visible in the schema; `cat .format/markdown` shows config
6. **Read-write:** Full CRUD operations through filesystem interface

### Limitations

1. **Frontmatter is "remaining":** Cannot explicitly select frontmatter columns (future TODO)
2. **No complex filters in views:** Views with WHERE clauses need INSTEAD OF triggers for writes
3. **Single format per view:** Can't have both markdown and JSON from same view
4. **Duplicate filenames:** Require PK suffix disambiguation (e.g., `foo-123.md`)

### Tradeoffs

| Decision | Benefit | Cost |
|----------|---------|------|
| Views as config | No metadata tables, portable | Less flexible than JSONB config |
| Suffix naming for `.format/` | No name collision with table | Longer paths (`posts_md`) |
| Underscore prefix for `.build/` | Clean primary paths | Unusual table naming (`_posts`) |
| Frontmatter = remaining | Simple, view controls columns | Can't mix included/excluded |
| Tasks has fixed schema | Predictable structure | Less flexible than generic lists |

## Implementation

### Files to Create
- `internal/tigerfs/fuse/synthesized.go` - Format detection and synthesis
- `internal/tigerfs/fuse/markdown.go` - Markdown/YAML synthesis and parsing
- `internal/tigerfs/fuse/tasks.go` - Tasks format with status handling
- `internal/tigerfs/fuse/format_node.go` - `.format/` directory handling
- `internal/tigerfs/fuse/build_node.go` - `.build/` scaffolding handling

### Files to Modify
- `internal/tigerfs/fuse/schema.go` - Expose `.format/` and `.build/` directories
- `internal/tigerfs/fuse/table.go` - Detect synthesized views, route appropriately
- `internal/tigerfs/db/query.go` - Add view introspection queries
- `internal/tigerfs/db/ddl.go` - Add view/trigger generation

### Dependencies
- JSON5 parsing library for Go (e.g., `github.com/yosuke-furukawa/json5`)
- YAML serialization (e.g., `gopkg.in/yaml.v3`)

## Verification

### Unit Tests
- Format detection from view name and columns
- Markdown synthesis (frontmatter + body from row)
- Markdown parsing (split frontmatter, map to columns)
- Tasks filename synthesis (`{number}-{name}-{status}`) with dynamic padding
- Status symbol/word normalization
- Hierarchical number incrementing and decrementing
- Per-level padding width calculation

### Integration Tests
```bash
# Create markdown view on existing table
echo "markdown" > /mnt/db/posts/.format/markdown
ls /mnt/db/posts_md/
cat /mnt/db/posts_md/hello-world.md

# Build new markdown app
echo "markdown" > /mnt/db/.build/notes
echo "---\ntitle: Test\n---\nContent" > /mnt/db/notes/test.md
cat /mnt/db/notes/test.md

# Build tasks app
echo "tasks" > /mnt/db/.build/work
echo "First task" > /mnt/db/work/1-setup-o.md
mv /mnt/db/work/1-setup-o.md /mnt/db/work/1-setup-~.md  # Change status
cat /mnt/db/work/1-setup-~.md  # Verify doing_at timestamp set

# Verify shift-on-insert (filenames will show padding based on count)
echo "New task" > /mnt/db/work/1-new-task-o.md  # Should shift existing 1 to 2
ls /mnt/db/work/  # Shows: 1-new-task-o.md, 2-setup-~.md

# Read format config
cat /mnt/db/posts/.format/markdown

# Delete via native table
rm -r /mnt/db/_notes/.delete/_notes  # CASCADE drops view
```

### Backward Compatibility
- Existing tables continue to work in native format
- Views without synthesized format markers render as native
- No changes to existing `.by/`, `.filter/`, `.export/` capabilities

## Future Enhancements

1. **Explicit frontmatter selection:** `{filename:slug,body:content,frontmatter:[author,date]}`
2. **`extras` JSONB column:** Store unknown frontmatter keys
3. **`rm .format/markdown`:** Filesystem-based view deletion
4. **Auto-regeneration:** Views with `tigerfs:md,auto` comment regenerate on DDL changes
5. **Additional formats:** HTML, JSON, CSV synthesis
