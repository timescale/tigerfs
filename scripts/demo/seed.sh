#!/bin/bash
# seed.sh -- Create file-first demo apps and write content via TigerFS mount
#
# Usage: seed.sh <mountpoint>
#
# Creates three synthesized apps (blog, docs, snippets) by writing through
# the mounted TigerFS filesystem. This exercises the real .build/ code path
# instead of hand-writing SQL for backing tables, triggers, and history.

set -e

MOUNT="${1:?Usage: seed.sh <mountpoint>}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}==>${NC} $1"; }
error() { echo -e "${RED}==>${NC} $1"; }

# Verify mount is accessible
if [ ! -d "$MOUNT" ]; then
    error "Mount point $MOUNT does not exist or is not mounted"
    exit 1
fi

# ============================================================================
# App 1: blog (markdown with history, 5 posts in 2 subdirectories)
# ============================================================================
info "Creating blog app (markdown, history)..."
echo 'markdown,history' > "$MOUNT/.build/blog"
sleep 1  # let the build settle

# Create subdirectories
mkdir -p "$MOUNT/blog/tutorials"
mkdir -p "$MOUNT/blog/deep-dives"

# Blog posts
cat > "$MOUNT/blog/hello-world.md" << 'ENDOFFILE'
---
title: "Hello, World!"
author: Alice
tags:
  - intro
  - welcome
draft: false
---

Welcome to our blog! This is the first post on our new platform.

We're excited to share ideas about databases, filesystems, and the
intersection of Unix tooling with modern data infrastructure.

## What to Expect

- Tutorials on SQL and PostgreSQL
- Tips for working with markdown
- Deep dives into filesystem design

Stay tuned for more posts!
ENDOFFILE

cat > "$MOUNT/blog/tutorials/getting-started-with-sql.md" << 'ENDOFFILE'
---
title: Getting Started with SQL
author: Bob
tags:
  - sql
  - beginner
draft: false
---

SQL is the lingua franca of data. Whether you're building an app or
analyzing a dataset, knowing SQL is essential.

## Your First Query

```sql
SELECT * FROM users WHERE active = true;
```

This returns all active users from the `users` table.

## Filtering Results

Use `WHERE` clauses to narrow your results:

```sql
SELECT name, email
FROM users
WHERE age > 25
ORDER BY name;
```

## Next Steps

Try joining tables together to combine related data.
ENDOFFILE

cat > "$MOUNT/blog/tutorials/markdown-tips.md" << 'ENDOFFILE'
---
title: Markdown Tips and Tricks
author: Alice
---

Markdown is a lightweight markup language that's easy to read and write.
Here are some tips to level up your formatting.

## Headers

Use `#` for headers. More `#` signs mean smaller headers:

- `#` H1
- `##` H2
- `###` H3

## Lists

Unordered lists use `-`, `*`, or `+`:

- Item one
- Item two
  - Nested item

## Code Blocks

Wrap code in triple backticks for syntax highlighting:

```python
def hello():
    print("Hello, world!")
```

## Links and Images

- Links: `[text](url)`
- Images: `![alt](url)`
ENDOFFILE

cat > "$MOUNT/blog/deep-dives/why-postgres.md" << 'ENDOFFILE'
---
title: Why PostgreSQL?
author: Charlie
tags:
  - postgresql
  - database
category: deep-dive
---

PostgreSQL is the world's most advanced open source relational database.
Here's why we chose it as the foundation for TigerFS.

## Reliability

PostgreSQL has over 35 years of active development. It's trusted by
organizations ranging from startups to Fortune 500 companies.

## Extensibility

The extension ecosystem is unmatched:

- **PostGIS** for geospatial data
- **pgvector** for AI embeddings
- **TimescaleDB** for time-series

## Standards Compliance

PostgreSQL follows the SQL standard closely, making your skills
transferable and your queries portable.

## The Ecosystem

Tools like `psql`, `pg_dump`, and `pg_restore` make operations
straightforward. And with FUSE, you can now browse your data
as files on disk.
ENDOFFILE

cat > "$MOUNT/blog/deep-dives/working-with-views.md" << 'ENDOFFILE'
---
title: Working with Views
author: Bob
tags:
  - postgresql
  - views
draft: false
---

Views are virtual tables defined by a query. They're one of PostgreSQL's
most powerful features for organizing and securing data.

## Creating a View

```sql
CREATE VIEW active_users AS
SELECT id, name, email
FROM users
WHERE active = true;
```

## Updatable Views

Simple views (single table, no aggregation) are automatically updatable:

```sql
UPDATE active_users SET name = 'Alice B.' WHERE id = 1;
```

## Materialized Views

For expensive queries, materialized views cache the results:

```sql
CREATE MATERIALIZED VIEW user_stats AS
SELECT count(*) AS total, avg(age) AS avg_age
FROM users;
```

Refresh them with `REFRESH MATERIALIZED VIEW user_stats;`
ENDOFFILE

# Seed blog history by updating posts (captures originals as history entries)
info "Seeding blog history..."
cat > "$MOUNT/blog/hello-world.md" << 'ENDOFFILE'
---
title: "Hello, World!"
author: Alice
tags:
  - intro
  - welcome
draft: false
---

Welcome to our blog! This is the first post on our new platform.

We're excited to share ideas about databases, filesystems, and the
intersection of Unix tooling with modern data infrastructure.

## What to Expect

- Tutorials on SQL and PostgreSQL
- Tips for working with markdown
- Deep dives into filesystem design
- Guides for AI-powered workflows

Stay tuned for more posts!

## Recent Updates

We've added synthesized app support -- create markdown-backed directories
with a single command.
ENDOFFILE

cat > "$MOUNT/blog/tutorials/getting-started-with-sql.md" << 'ENDOFFILE'
---
title: Getting Started with SQL
author: Bob
tags:
  - sql
  - beginner
draft: false
---

SQL is the lingua franca of data. Whether you're building an app or
analyzing a dataset, knowing SQL is essential.

## Your First Query

```sql
SELECT * FROM users WHERE active = true;
```

This returns all active users from the `users` table.

## Filtering Results

Use `WHERE` clauses to narrow your results:

```sql
SELECT name, email
FROM users
WHERE age > 25
ORDER BY name;
```

## Joining Tables

Combine related data with `JOIN`:

```sql
SELECT u.name, o.total
FROM users u
JOIN orders o ON o.user_id = u.id
WHERE o.status = 'completed';
```

## Next Steps

Try creating views to simplify complex queries.
ENDOFFILE

# ============================================================================
# App 2: docs (markdown with history, 4 pages in 2 subdirectories)
# ============================================================================
info "Creating docs app (markdown, history)..."
echo 'markdown,history' > "$MOUNT/.build/docs"
sleep 1

# Create subdirectories
mkdir -p "$MOUNT/docs/getting-started"
mkdir -p "$MOUNT/docs/reference"

cat > "$MOUNT/docs/getting-started/installation.md" << 'ENDOFFILE'
---
title: Installation Guide
author: TigerFS Team
section: getting-started
---

# Installation

TigerFS runs on macOS and Linux. Choose your platform below.

## macOS (Homebrew)

```bash
brew install tigerfs
```

TigerFS uses NFS on macOS -- no kernel extensions required.

## Linux

Download the latest release from GitHub:

```bash
curl -L https://github.com/timescale/tigerfs/releases/latest/download/tigerfs_linux_amd64.tar.gz | tar xz
sudo mv tigerfs /usr/local/bin/
```

Ensure FUSE is available:

```bash
sudo apt install fuse3   # Debian/Ubuntu
sudo dnf install fuse3   # Fedora
```

## Verify Installation

```bash
tigerfs version
```
ENDOFFILE

cat > "$MOUNT/docs/reference/configuration.md" << 'ENDOFFILE'
---
title: Configuration Reference
author: TigerFS Team
section: reference
---

# Configuration

TigerFS reads configuration from multiple sources with the following
precedence (highest wins):

1. Command-line flags
2. Environment variables (`TIGERFS_*`)
3. Config file (`~/.config/tigerfs/config.yaml`)
4. Built-in defaults

## Config File

```yaml
format: json          # Default output format: json, tsv, csv
read_only: false      # Mount as read-only
max_ls_rows: 10000    # Threshold for large table warning
```

## Environment Variables

All config keys can be set via environment:

```bash
export TIGERFS_FORMAT=tsv
export TIGERFS_READ_ONLY=true
export TIGERFS_MAX_LS_ROWS=5000
```

## Connection String

Pass via argument or environment:

```bash
tigerfs mount postgres://user:pass@host/db /mnt/db
# or
export TIGERFS_CONNECTION=postgres://user:pass@host/db
tigerfs mount /mnt/db
```
ENDOFFILE

cat > "$MOUNT/docs/getting-started/quick-start.md" << 'ENDOFFILE'
---
title: Quick Start
author: TigerFS Team
section: getting-started
---

# Quick Start

Get up and running with TigerFS in under 5 minutes.

## 1. Start a Database

If you don't have a PostgreSQL instance, start one with Docker:

```bash
docker run -d --name pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=secret postgres:17
```

## 2. Mount the Database

```bash
mkdir -p /tmp/mydb
tigerfs mount postgres://postgres:secret@localhost/postgres /tmp/mydb
```

## 3. Browse Your Data

```bash
ls /tmp/mydb/              # List tables
ls /tmp/mydb/users/        # List rows
cat /tmp/mydb/users/1.json # Read a row
```

## 4. Make Changes

```bash
# Edit a row (opens in $EDITOR)
vim /tmp/mydb/users/1.json

# Delete a row
rm /tmp/mydb/users/42.json
```

## Next Steps

See the [Configuration](configuration.md) guide for customization options.
ENDOFFILE

cat > "$MOUNT/docs/reference/api-reference.md" << 'ENDOFFILE'
---
title: API Reference
author: TigerFS Team
section: reference
---

# API Reference

TigerFS exposes a filesystem API -- every operation maps to SQL.

## Reading Data

| Operation | SQL Equivalent |
|-----------|---------------|
| `ls tables/` | `SELECT * FROM pg_tables` |
| `ls users/` | `SELECT pk FROM users` |
| `cat users/1.json` | `SELECT * FROM users WHERE id = 1` |
| `cat users/.export/csv` | `SELECT * FROM users` (CSV format) |

## Writing Data

| Operation | SQL Equivalent |
|-----------|---------------|
| `echo '...' > users/1.json` | `UPDATE users SET ... WHERE id = 1` |
| `rm users/1.json` | `DELETE FROM users WHERE id = 1` |
| `cp template.json users/new.json` | `INSERT INTO users ...` |

## Special Directories

- `.export/` -- Bulk export in TSV, CSV, or JSON format
- `.by/` -- Index-based navigation
- `.schema` -- Table schema information

## Error Mapping

| Errno | Meaning |
|-------|---------|
| ENOENT | Row not found |
| EACCES | Read-only mount |
| EINVAL | Invalid data format |
| EIO | Database connection error |
ENDOFFILE

# Seed docs history by updating pages
info "Seeding docs history..."
cat > "$MOUNT/docs/getting-started/installation.md" << 'ENDOFFILE'
---
title: Installation Guide
author: TigerFS Team
section: getting-started
---

# Installation

TigerFS runs on macOS and Linux. Choose your platform below.

## macOS (Homebrew)

```bash
brew install tigerfs
```

TigerFS uses NFS on macOS -- no kernel extensions required.

## Linux

Download the latest release from GitHub:

```bash
curl -L https://github.com/timescale/tigerfs/releases/latest/download/tigerfs_linux_amd64.tar.gz | tar xz
sudo mv tigerfs /usr/local/bin/
```

Ensure FUSE is available:

```bash
sudo apt install fuse3   # Debian/Ubuntu
sudo dnf install fuse3   # Fedora
```

## Verify Installation

```bash
tigerfs version
```

## Upgrading

To upgrade an existing installation:

```bash
brew upgrade tigerfs     # macOS
# or re-download the latest release for Linux
```
ENDOFFILE

cat > "$MOUNT/docs/getting-started/quick-start.md" << 'ENDOFFILE'
---
title: Quick Start
author: TigerFS Team
section: getting-started
---

# Quick Start

Get up and running with TigerFS in under 5 minutes.

## 1. Start a Database

If you don't have a PostgreSQL instance, start one with Docker:

```bash
docker run -d --name pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=secret postgres:18
```

## 2. Mount the Database

```bash
mkdir -p /tmp/mydb
tigerfs mount postgres://postgres:secret@localhost/postgres /tmp/mydb
```

## 3. Browse Your Data

```bash
ls /tmp/mydb/              # List tables
ls /tmp/mydb/users/        # List rows
cat /tmp/mydb/users/1.json # Read a row
```

## 4. Make Changes

```bash
# Edit a row (opens in $EDITOR)
vim /tmp/mydb/users/1.json

# Delete a row
rm /tmp/mydb/users/42.json
```

## 5. Synthesized Apps

Create markdown-backed directories:

```bash
echo "markdown" > /tmp/mydb/.build/notes
echo "---
title: Hello
---
Content" > /tmp/mydb/notes/hello.md
```

## Next Steps

See the [Configuration](configuration.md) guide for customization options.
ENDOFFILE

# ============================================================================
# App 3: snippets (plain text with history, 3 files in 1 subdirectory)
# ============================================================================
info "Creating snippets app (txt, history)..."
echo 'txt,history' > "$MOUNT/.build/snippets"
sleep 1

# Create subdirectory
mkdir -p "$MOUNT/snippets/meetings"

cat > "$MOUNT/snippets/todo.txt" << 'ENDOFFILE'
TODO List
=========

[ ] Set up CI/CD pipeline
[ ] Write integration tests for FUSE adapter
[ ] Add CSV export support
[x] Implement JSON row format
[x] Create demo data script
[ ] Update README with examples
[ ] Benchmark large table performance
[x] Add index-based navigation
ENDOFFILE

cat > "$MOUNT/snippets/meetings/meeting-notes.txt" << 'ENDOFFILE'
Team Sync -- 2025-02-10
======================

Attendees: Alice, Bob, Charlie

Agenda:
  1. Sprint review
  2. Demo prep
  3. Release timeline

Notes:
  - Sprint went well, all planned features complete
  - Demo data needs markdown app examples
  - Targeting v0.3.0 release by end of month
  - Bob will handle the docker-demo updates
  - Charlie to review documentation

Action Items:
  - Alice: finalize synthesized app feature
  - Bob: update demo scripts
  - Charlie: review and edit docs/
ENDOFFILE

cat > "$MOUNT/snippets/scratch.txt" << 'ENDOFFILE'
Random notes and scratch pad
----------------------------

Useful psql commands:
  \dt          list tables
  \dv          list views
  \d+ table    describe table with details
  \x           toggle expanded output

Connection string format:
  postgres://user:pass@host:port/dbname?sslmode=disable

Quick test:
  SELECT version();
  SELECT current_database();
  SELECT current_user;
ENDOFFILE

# Seed snippets history by updating files
info "Seeding snippets history..."
cat > "$MOUNT/snippets/todo.txt" << 'ENDOFFILE'
TODO List
=========

[ ] Set up CI/CD pipeline
[ ] Write integration tests for FUSE adapter
[x] Add CSV export support
[x] Implement JSON row format
[x] Create demo data script
[ ] Update README with examples
[ ] Benchmark large table performance
[x] Add index-based navigation
[x] Add synthesized markdown apps
[ ] Add history browsing support
ENDOFFILE

cat > "$MOUNT/snippets/scratch.txt" << 'ENDOFFILE'
Random notes and scratch pad
----------------------------

Useful psql commands:
  \dt          list tables
  \dv          list views
  \d+ table    describe table with details
  \x           toggle expanded output
  \di          list indexes

Connection string format:
  postgres://user:pass@host:port/dbname?sslmode=disable

Quick test:
  SELECT version();
  SELECT current_database();
  SELECT current_user;

TimescaleDB:
  SELECT * FROM timescaledb_information.hypertables;
ENDOFFILE

info "Seeding complete! Created:"
echo "  blog/      - 5 markdown posts in 2 subdirectories (with history)"
echo "  docs/      - 4 markdown pages in 2 subdirectories (with history)"
echo "  snippets/  - 3 text files in 1 subdirectory (with history)"
