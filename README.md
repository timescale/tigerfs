# TigerFS

A filesystem backed by PostgreSQL, and a filesystem interface to PostgreSQL.

Every file is a real PostgreSQL row. Directories are tables. File contents are columns. Multiple agents and humans can read and write the same files concurrently with full ACID guarantees. Every change is versioned and reversible. No sync protocols. No coordination layer.  **The filesystem is the API.**

You can use TigerFS in two ways:

* **File-first**: Write markdown with frontmatter or other file types, organize into directories. Writes are atomic, changes are versioned, and everything is reversible.  Any tool that works with files -- Claude Code, Cursor, grep, vim -- just works.  Build lightweight workspaces via the filesystem: multi-agent task coordination is just `mv`'ing files between todo/doing/done directories.

* **Data-first**: Mount any Postgres database and explore it with `ls`, `cat`, `grep`, and other unix tools. For
large databases, chain filters into paths that push down to SQL:
`.by/customer_id/123/.order/created_at/.last/10/.export/json`. No database client or SQL needed, and ships with agent skills.

Both modes are backed by the same transactional database. You get real transactions, true concurrent access, and a SQL escape hatch when you need it. TigerFS mounts via FUSE on Linux and NFS on macOS, no extra dependencies needed.

### Agent Skills

TigerFS ships with agent skills for Claude Code, Gemini CLI, and Codex -- automatically installed at mount time. You don't need to learn the filesystem interface. Just ask:

- "Create a workspace for my notes"
- "What changed since the savepoint?"
- "Undo agent-7's changes"
- "Show me the last 10 orders by customer 123"

The skills teach your agent the filesystem paths, diff commands, and undo workflows. For details on what's underneath, read on.

### The Filesystem is the API

Your data lives in regular files and directories. Metadata, queries, and operations live in dot-directories -- invisible by default, always available:

```bash
$ ls /mnt/db/notes/
hello.md  tutorials/

$ ls -a /mnt/db/notes/
.  ..  .history/  .log/  .savepoint/  .undo/  hello.md  tutorials/
```

Dot-directories are the control surface. Navigate them to browse history, filter data, undo changes, and manage schemas -- all through the same filesystem interface.

| File-first | Data-first |
|------------|------------|
| `.history/` -- past versions | `.info/` -- table metadata |
| `.log/` -- operation log with diffs | `.by/` -- index lookups |
| `.savepoint/` -- bookmarks for undo | `.filter/` -- column filtering |
| `.undo/` -- preview and apply undo | `.order/`, `.first/`, `.last/` -- sort and paginate |
| `.build/` -- create workspaces | `.export/`, `.import/` -- bulk I/O |
| | `.create/`, `.modify/`, `.delete/` -- schema management |

## Quick Start

```bash
# Install (macOS requires no dependencies; Linux needs fuse3)
curl -fsSL https://install.tigerfs.io | sh
```

| Mode | You have... | You want to... |
|------|------------|----------------|
| File-first | A new project or workflow | Store markdown or other files, and build simple workspaces via the file system (e.g., task queues, agent workspaces, collaborative docs). |
| Data-first | An existing Postgres database | Explore and operate on it with `ls`, `cat`, `grep` instead of SQL. |

### File-first

```bash
# Mount a database and create a workspace
tigerfs mount postgres://localhost/mydb /mnt/db
echo "markdown,history" > /mnt/db/.build/notes

# Write a file — frontmatter becomes columns, body becomes text
cat > /mnt/db/notes/hello.md << 'EOF'
---
title: Hello World
author: alice
---
# Hello World
EOF

# Search, explore, undo
grep -l "author: alice" /mnt/db/notes/*.md
ls /mnt/db/notes/
touch /mnt/db/notes/.undo/id/<log_id>/.apply        # undo a file change
```

### Data-first

```bash
# Mount an existing database
tigerfs mount postgres://localhost/mydb /mnt/db

ls /mnt/db/                                    # list tables
ls /mnt/db/users/                              # list rows
cat /mnt/db/users/1.json                       # read a row
cat /mnt/db/users/.by/email/alice@co.com.json  # index lookup

# Chain filters, ordering, pagination — pushed down as one SQL query and bulk export
cat /mnt/db/orders/.by/customer_id/1/.order/created_at/.last/5/.export/json
```

## File-First: Transactional Workspace

### Workspaces

Workspaces tell TigerFS how to present a table as a native file format. Write "markdown" to `.build/` and the table becomes a directory of .md files with YAML frontmatter:

```bash
# Create a workspace
echo "markdown" > /mnt/db/.build/blog

# Write a post. Frontmatter becomes columns, body becomes text
cat > /mnt/db/blog/hello-world.md << 'EOF'
---
title: Hello World
author: alice
tags: [intro]
---

# Hello World

Welcome to my blog...
EOF

# Search, edit, and manage content with standard tools
grep -l "author: alice" /mnt/db/blog/*.md
```

Organize files into directories. `mkdir` creates folders, `mv` moves files between them:

```bash
mkdir /mnt/db/blog/tutorials
mv /mnt/db/blog/hello-world.md /mnt/db/blog/tutorials/
```

See [docs/file-first.md](docs/file-first.md) for column mapping, frontmatter handling, and use cases.

### History, Savepoints, and Undo

Any workspace can opt into automatic versioning. Every edit and delete is captured as a timestamped snapshot. Create savepoints before risky work, preview what changed, and undo if needed.

```bash
# Create a workspace with history enabled
echo "markdown,history" > /mnt/db/.build/notes

# Create a savepoint before making changes
echo '{"description":"Before refactoring"}' > /mnt/db/notes/.savepoint/before-refactor.json

# Work, explore, make changes...

# Review what changed since the savepoint
diff -ru /mnt/db/notes/.undo/to-savepoint/before-refactor /mnt/db/notes/ -x '.*'

# Undo everything to the savepoint (atomic, all files at once)
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.apply

# Or undo only a specific agent's changes (preserves other agents' work)
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.by/user_id/agent-7/.apply

# Diff and undo a single file change
diff -u /mnt/db/notes/.log/<id>/before /mnt/db/notes/.log/<id>/current
touch /mnt/db/notes/.undo/id/<id>/.apply

# Browse past versions of any file
ls /mnt/db/notes/.history/hello.md/
cat /mnt/db/notes/.history/hello.md/2026-02-12T013000.789Z-abc123
```

History tracks files across renames via stable row UUIDs. Savepoints and undo use TimescaleDB hypertables for compressed storage.

See [docs/history.md](docs/history.md) for the full guide on version browsing, savepoints, undo, and recovery.

### Use Cases

**Shared agent workspace.**
Multiple agents and humans operate on the same knowledge base concurrently.
Every edit is automatically versioned, so if one agent overwrites another's work, recover it from `.history/`.

```bash
# Agent A writes research findings
cat > /mnt/db/kb/auth-analysis.md << 'EOF'
---
author: agent-a
---
OAuth 2.0 is the recommended approach because...
EOF

# Agent B reads it immediately, no sync, no pull
cat /mnt/db/kb/auth-analysis.md
```

**Multi-agent task queue.** Three directories (`todo/`, `doing/`, `done/`) and `mv` is your only API. Moves are atomic database operations, so two agents can't claim the same task.

```bash
# Set up a task board
echo "markdown,history" > /mnt/db/.build/tasks
mkdir /mnt/db/tasks/todo /mnt/db/tasks/doing /mnt/db/tasks/done

# Agent claims a task by moving it to doing
mv /mnt/db/tasks/todo/fix-auth-bug.md /mnt/db/tasks/doing/fix-auth-bug.md

# Marks it complete
mv /mnt/db/tasks/doing/fix-auth-bug.md /mnt/db/tasks/done/fix-auth-bug.md

# See what everyone is working on
ls /mnt/db/tasks/doing/
grep "author:" /mnt/db/tasks/doing/*.md
```

**Collaborative docs.** A human writes a draft, an agent reviews and edits it, another agent summarizes it. All in the same directory, all visible immediately, no pull/push/merge. History shows who changed what and when.

```bash
# Human writes a draft
cat > /mnt/db/docs/proposal.md << 'EOF'
---
title: Q2 Proposal
status: draft
---
We should invest in...
EOF

# Agent reads, edits, and updates the status
cat /mnt/db/docs/proposal.md
cat > /mnt/db/docs/proposal.md << 'EOF'
---
title: Q2 Proposal
status: reviewed
reviewer: agent-b
---
We should invest in... (with agent edits)
EOF

# Human sees changes instantly. Browse the full edit trail
ls /mnt/db/docs/.history/proposal.md/
cat /mnt/db/docs/.history/proposal.md/2026-02-25T100000Z  # see previous version
```

**Safe exploration.** An agent creates a savepoint, investigates a bug, makes changes across multiple files. If the approach doesn't work, undo atomically to the savepoint -- every file reverts in one step.

```bash
# Agent creates savepoint before investigating
echo '{"description":"Before investigating auth bug"}' > /mnt/db/notes/.savepoint/pre-investigation.json

# Agent explores, edits multiple files...
# User reviews: "that's not right, roll it back"
touch /mnt/db/notes/.undo/to-savepoint/pre-investigation/.apply
# All files restored to pre-investigation state
```

## Data-First: Database as Filesystem

Mount any Postgres database and explore it with `ls`, `cat`, `grep`. Every path resolves to optimized SQL pushed down to the database.

**Explore an unfamiliar database.** Point an agent at a mounted database and it understands the schema immediately using `ls` and `cat`. No SQL, no database client, no connection strings to pass around.

**Quick data fixes.** Update a customer's email, toggle a feature flag, delete a test record. One shell command instead of opening a SQL client, remembering the table schema, and writing a `WHERE` clause.

**Export and analyze.** Chain filters, ordering, and pagination into a single path, then pipe the result into `jq`, `awk`, or export as CSV for a spreadsheet.

### Explore

```bash
ls /mnt/db/                                      # List tables
ls /mnt/db/users/                                # List rows (by primary key)
cat /mnt/db/users/123.json                       # Row as JSON
cat /mnt/db/users/123/email.txt                  # Single column
cat /mnt/db/users/.by/email/foo@example.com.json # Index lookup
```

### Modify

```bash
echo 'new@example.com' > /mnt/db/users/123/email.txt            # Update column
echo '{"email":"a@b.com","name":"A"}' > /mnt/db/users/ 123.json # Update via JSON (PATCH)
mkdir /mnt/db/users/456                                         # Create row
rm -r /mnt/db/users/456/                                        # Delete row
```

### Pipeline Queries

Chain filters, ordering, and pagination in a single path. The database executes it as one query:

```bash
cat /mnt/db/orders/.by/customer_id/123/.order/created_at/.last/10/.export/json

# Select specific columns from a filtered query
cat /mnt/db/orders/.filter/status/shipped/.columns/id,total,created_at/.export/csv
```

Pipeline segments can be chained in any order. Available segments: `.by/` (indexed filter), `.filter/` (any column), `.order/` (sort), `.columns/col1,...`  (projection), `.first/N/`, `.last/N/`, `.sample/N/` (pagination), and `.export/csv|json|tsv` (output format). 
 

### Ingest

Bulk-load data from CSV, JSON, or YAML. The write mode is part of the path: `.append/` adds rows, `.sync/` upserts by primary key, `.overwrite/` replaces the table.

```bash
cat data.csv > /mnt/db/orders/.import/.append/csv
```

### Schema Management

Create, modify, and delete tables through a staging pattern:

```bash
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (...)" > /mnt/db/.create/orders/sql
touch /mnt/db/.create/orders/.commit
```

See [docs/data-first.md](docs/data-first.md) for the full reference: row formats, index navigation, pipeline query chaining, schema management workflows, and configuration.

## Why TigerFS

### If you're building on files (file-first)

- **vs. local files:** Instead of a single-writer assumption, TigerFS supports real concurrent access with isolation guarantees.

- **vs. git:** Instead of asynchronous collaboration and merges, TigerFS provides immediate visibility with automatic version history.

- **vs. object storage (S3):** Instead of blobs, you get structured rows, ACID transactions, and query pushdown.

### If you're querying data (data-first)

- **vs. database clients / psql:** No SQL to learn. Every agent already speaks files.

- **vs. ORMs and APIs:** No schemas to define, no SDK to install. Mount and go.

- **vs. using a database directly:** Instead of clients and schemas, you use files. Every tool and every agent already understands the interface.

The result is simple: you delete coordination code from your application.

## Cloud Backends

TigerFS works with any PostgreSQL database. Just pass a connection string. It also integrates with [Tiger Cloud](https://www.timescale.com/cloud) and [Ghost](https://ghost.build) through their CLIs for credential-free mounting. Use a prefix to specify the backend:

```bash
# Mount any Postgres database
tigerfs mount postgres://user:pass@host/mydb /mnt/db

# Or mount cloud services by ID
tigerfs mount tiger:abcde12345 /mnt/db
tigerfs mount ghost:fghij67890 /mnt/db
```

TigerFS calls the backend CLI to retrieve credentials, so there are no passwords in your config. Authenticate once with `tiger auth login` or `ghost login`.

Set a default backend to skip the prefix:

```bash
# In ~/.config/tigerfs/config.yaml: default_backend: tiger
tigerfs mount abcde12345 /mnt/db    # uses tiger: implicitly
```

### Create and Fork

```bash
# Create a new cloud database (auto-mounts)
tigerfs create tiger:my-db
tigerfs create tiger:my-db /mnt/data   # custom mount path
tigerfs create ghost:my-db --no-mount  # create without mounting

# Fork (clone) for safe experimentation
tigerfs fork /mnt/db my-experiment
tigerfs fork tiger:abcde12345 my-experiment

# Inspect a mount
tigerfs info /mnt/db
tigerfs info --json /mnt/db           # JSON output for scripting
```

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Unix Tools  │────▶│  Filesystem  │────▶│   TigerFS    │────▶│  PostgreSQL  │
│  ls, cat,    │     │   Backend    │     │   Daemon     │     │   Database   │
│  echo, rm    │◀────│  (FUSE/NFS)  │◀────│              │◀────│              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

TigerFS replaces "application-level coordination" with database transactions. **The filesystem becomes the API.**

TigerFS maps filesystem paths to database queries:

```
  Filesystem                       Database
  ──────────                       ────────
  /mnt/db/                     →   schemas
  /mnt/db/public/              →   tables
  /mnt/db/public/users/        →   rows (by PK)
  /mnt/db/public/users/123/    →   columns as files
```

FUSE on Linux, NFS on macOS. No external dependencies on either platform.

## Design Principles

- **Keep the interface familiar.** If you can `ls`, you can explore a database.
- **Make concurrency safe.** Multiple writers without corruption or conflicts.
- **Push logic down.** Every path resolves to optimized SQL.
- **Make changes reversible.** Savepoints, undo, and version history mean you can always go back.
- **Remove coordination code.** The database handles it.

## Try the Demo

```bash
cd scripts/demo
./demo.sh start     # auto-detects platform (--docker or --mac)
./demo.sh shell     # explore: ls, cat users/1.json, etc.
./demo.sh stop
```

## Configuration

Config file: `~/.config/tigerfs/config.yaml`. Run `tigerfs config show` to see all options and their current values. All options support environment variables with `TIGERFS_` prefix. See [docs/spec.md](docs/spec.md) for the full reference.

## Documentation

| Guide | Description |
|-------|-------------|
| [docs/file-first.md](docs/file-first.md) | File-first mode: workspaces, column mapping, frontmatter, directories |
| [docs/history.md](docs/history.md) | History, savepoints, and undo: versioned snapshots, safe exploration, atomic rollback |
| [docs/data-first.md](docs/data-first.md) | Data-first mode: row formats, indexes, pipeline queries, schema management |
| [docs/quickstart.md](docs/quickstart.md) | Guided scenarios with sample data |

## Development

```bash
git clone https://github.com/timescale/tigerfs.git
cd tigerfs
go build -o bin/tigerfs ./cmd/tigerfs
go test ./...
```

For development guidelines, architecture details, and the full specification, see [CLAUDE.md](CLAUDE.md) and [docs/spec.md](docs/spec.md).

## Project Status

TigerFS is early, but the core idea is stable: transactional, concurrent files as the foundation for human-agent collaboration.

**v0.7.0.** Undo and recovery -- savepoints, operation log, and atomic undo for safe exploration.

**v0.6.0.** Dedicated tigerfs schema, security hardening, and unified demo.

**Highlights:**
- Markdown and plaintext workspaces with YAML frontmatter, directory hierarchies, and version history
- Savepoints, undo, and operation log -- create checkpoints, preview changes, roll back atomically
- Per-user undo -- multiple agents with separate identities, selectively undo one agent's work
- Auto-savepoints -- detect session boundaries on inactivity gaps
- Relational directory model with parent-pointer hierarchy and UUIDv7 identifiers
- Dedicated tigerfs schema with migration framework (`tigerfs migrate`)
- TLS enforcement, SQL injection hardening, and credential sanitization
- Agent skill auto-install for Claude Code, Gemini CLI, and Codex
- Cloud backends: mount, create, and fork Tiger Cloud and Ghost databases by service ID
- Pipeline queries with full database pushdown (`.by/`, `.filter/`, `.order/`, `.columns/`, chained pagination, `.export/`)
- DDL staging for tables, indexes, views, and schemas (`.create/`, `.modify/`, `.delete/`)
- Full CRUD with multiple formats (TSV, CSV, JSON, YAML), index navigation, and PATCH semantics
- Binary distribution via GoReleaser with install script
- Multi-tier stat caching and query reduction for fast operations over remote databases

**Planned:**
- Tables without primary keys (read-only via ctid)
- Windows support

## Contributing

Contributions are welcome! Please see the development guidelines in [CLAUDE.md](CLAUDE.md).

## Support

- **Issues**: https://github.com/timescale/tigerfs/issues
- **Discussions**: https://github.com/timescale/tigerfs/discussions
