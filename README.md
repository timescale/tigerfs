# TigerFS

TigerFS is a shared, transactional workspace for humans and AI agents, exposed as a native filesystem.

Every file is a real PostgreSQL row. Multiple agents and humans can read and write the same files concurrently with full ACID guarantees. No sync protocols. No coordination layer.

The filesystem is the API.

Write a markdown file with YAML frontmatter and TigerFS stores it as structured data with automatic version history. Directories map to tables. Rows map to files. Columns map to file contents. Every tool that works with files (`cat`, `grep`, your editor, Claude Code, Cursor) works out of the box.  Search with `grep`. Organize with `mv` and `mkdir`. Recover past versions from `.history/`.

You can use TigerFS in two ways:

- As an app layer, working with Markdown and other higher-level file formats.

- As a native table layer, navigating PostgreSQL tables, rows, and indexes directly through the filesystem.

Both are backed by the same transactional database. You get real transactions, true concurrent access, and a SQL escape hatch when you need it. TigerFS mounts via FUSE on Linux or NFS on macOS.

## Quick Start

In under 60 seconds, you can mount a live PostgreSQL database as a collaborative workspace.

```bash
# Install (macOS requires no dependencies; Linux needs fuse3)
curl -fsSL https://install.tigerfs.io | sh

# Mount a local database
tigerfs mount postgres://localhost/mydb /mnt/db

# Mount cloud services by ID
tigerfs mount tiger:e6ue9697jf /mnt/db

# Create a markdown app and start writing
echo "markdown" > /mnt/db/.build/notes

cat > /mnt/db/notes/hello.md << 'EOF'
---
title: Hello World
author: alice
---

# Hello World
EOF

# Search, explore, unmount
grep -l "author: alice" /mnt/db/notes/*.md
ls /mnt/db/notes/
tigerfs unmount /mnt/db
```



## Why TigerFS

Agents are concurrent. Traditional filesystems are not.

Today, agents coordinate through:

- Local files that do not sync
- Git workflows that require pull, push, and merge
- APIs that require custom coordination logic
- Object storage that is not transactional

None of these were designed for multiple autonomous writers operating in real time.

TigerFS makes files shared and transactional by backing them with PostgreSQL. Every read and write runs inside a real database transaction.

That changes the model:

- **vs. local files:** Instead of a single-writer assumption, TigerFS supports real concurrent access with isolation guarantees.

- **vs. git:** Instead of asynchronous collaboration and merges, TigerFS provides immediate visibility with automatic version history.

- **vs. object storage (S3):** Instead of blobs, you get structured rows, ACID transactions, and query pushdown.

- **vs. using a database directly:** Instead of clients and schemas, you use files. Every tool and every agent already understands the interface.

The result is simple: you delete coordination code from your application.

## Use Cases

TigerFS turns a database into a live, shared workspace.

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


## Apps

Apps present database tables as higher-level file formats. Instead of navigating row directories and column files, you work with files that feel native to their domain.

### Markdown

The markdown app presents database rows as `.md` files with YAML frontmatter. Create one with a single command:

```bash
# Create a markdown app
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

See [docs/markdown-app.md](docs/markdown-app.md) for column mapping, frontmatter handling, and use cases.

### History

Any app can opt into automatic versioning. Every edit and delete is captured as a timestamped snapshot under a read-only `.history/` directory.

```bash
# Create an app with history enabled
echo "markdown,history" > /mnt/db/.build/notes

# Browse past versions of a file
ls /mnt/db/notes/.history/hello.md/
# .id  2026-02-24T150000Z  2026-02-12T013000Z

# Read a past version
cat /mnt/db/notes/.history/hello.md/2026-02-12T013000Z
```

History tracks files across renames via stable row UUIDs and uses TimescaleDB hypertables for compressed storage.

See [docs/history.md](docs/history.md) for cross-rename tracking, subdirectory scoping, and recovery workflows.

## Cloud Backends

TigerFS works with any PostgreSQL database. Just pass a connection string. It also integrates with [Tiger Cloud](https://www.timescale.com/cloud) and [Ghost](https://ghost.dev) through their CLIs for credential-free mounting. Use a prefix to specify the backend:

```bash
# Mount any Postgres database
tigerfs mount postgres://user:pass@host/mydb /mnt/db

# Or mount cloud services by ID
tigerfs mount tiger:e6ue9697jf /mnt/db
tigerfs mount ghost:a2x6xoj0oz /mnt/db
```

TigerFS calls the backend CLI to retrieve credentials, so there are no passwords in your config. Authenticate once with `tiger auth login` or `ghost login`.

Set a default backend to skip the prefix:

```bash
# In ~/.config/tigerfs/config.yaml: default_backend: tiger
tigerfs mount e6ue9697jf /mnt/db    # uses tiger: implicitly
```

### Create and Fork

```bash
# Create a new cloud database (auto-mounts)
tigerfs create tiger:my-db
tigerfs create tiger:my-db /mnt/data   # custom mount path
tigerfs create ghost:my-db --no-mount  # create without mounting

# Fork (clone) for safe experimentation
tigerfs fork /mnt/db my-experiment
tigerfs fork tiger:e6ue9697jf my-experiment

# Inspect a mount
tigerfs info /mnt/db
tigerfs info --json /mnt/db           # JSON output for scripting
```

## Native Table Access

Agents excel at manipulating files. TigerFS lets them operate on structured data using the tools they already understand.

Below the app layer, every table is a directory of rows. Read columns, write JSON, navigate indexes, and chain pipeline queries, all pushed down to the database as optimized SQL.

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
echo 'new@example.com' > /mnt/db/users/123/email.txt           # Update column
echo '{"email":"a@b.com","name":"A"}' > /mnt/db/users/123.json # Update via JSON (PATCH)
mkdir /mnt/db/users/456                                         # Create row
rm -r /mnt/db/users/456/                                        # Delete row
```

### Pipeline Queries

Chain filters, ordering, and pagination in a single path. The database executes it as one query:

```bash
cat /mnt/db/orders/.by/customer_id/123/.order/created_at/.last/10/.export/json
```

Capabilities: `.by/` (indexed filter), `.filter/` (any column), `.order/`, `.first/N/`, `.last/N/`, `.sample/N/`, `.export/csv|json|tsv`

### Schema Management

Create, modify, and delete tables through a staging pattern:

```bash
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (...)" > /mnt/db/.create/orders/sql
touch /mnt/db/.create/orders/.commit
```

See [docs/native-tables.md](docs/native-tables.md) for the full reference: row formats, index navigation, pipeline query chaining, schema management workflows, and configuration.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Unix Tools  │────▶│  Filesystem  │────▶│   TigerFS    │────▶│  PostgreSQL  │
│  ls, cat,    │     │   Backend    │     │   Daemon     │     │   Database   │
│  echo, rm    │◀────│ (FUSE/NFS)   │◀────│              │◀────│              │
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
- **Preserve history.** Every change is recoverable.
- **Remove coordination code.** The database handles it.

## Try the Demos

### Docker Demo (any platform)

```bash
cd scripts/docker-demo
./demo.sh start
./demo.sh shell     # explore: ls, cat users/1.json, etc.
./demo.sh stop
```

### macOS Demo (native)

```bash
cd scripts/mac-demo
./demo.sh start
ls -al /tmp/tigerfs-demo
cat /tmp/tigerfs-demo/users/1.json
./demo.sh stop
```

## Configuration

Config file: `~/.config/tigerfs/config.yaml`

```yaml
connection:
  default_schema: public
  pool_size: 10
default_backend: tiger            # skip tiger: prefix on mount
filesystem:
  dir_listing_limit: 10000        # max rows in directory listing
  no_filename_extensions: false   # disable .txt/.json/.bin extensions
logging:
  level: info
```

All options support environment variables with `TIGERFS_` prefix (e.g., `TIGERFS_DIR_LISTING_LIMIT=50000`). PostgreSQL standard variables (`PGHOST`, `PGUSER`, `PGDATABASE`) are also supported.

## Documentation

| Guide | Description |
|-------|-------------|
| [docs/markdown-app.md](docs/markdown-app.md) | Markdown app: column mapping, frontmatter, directories |
| [docs/history.md](docs/history.md) | Version history: snapshots, cross-rename tracking, recovery |
| [docs/native-tables.md](docs/native-tables.md) | Native table access: row formats, indexes, pipeline queries, schema management |
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

**v0.4.0.** Cloud backends, versioned history, installer, and binary distribution.

**Highlights:**
- Markdown apps with YAML frontmatter, directory hierarchies, and automatic version history
- Cloud backends: mount, create, and fork Tiger Cloud and Ghost databases by service ID
- Pipeline queries with full database pushdown (`.by/`, `.filter/`, `.order/`, chained pagination, `.export/`)
- DDL staging for tables, indexes, views, and schemas (`.create/`, `.modify/`, `.delete/`)
- Full CRUD with multiple formats (TSV, CSV, JSON, YAML), index navigation, and PATCH semantics
- Binary distribution via GoReleaser with install script (`curl -fsSL https://install.tigerfs.io | sh`)
- Three-tier query caching for fast synth view operations over remote databases

**Planned:**
- Tables without primary keys (read-only via ctid)
- TimescaleDB hypertables (time-based navigation)
- Windows support

## Contributing

Contributions are welcome! Please see the development guidelines in [CLAUDE.md](CLAUDE.md).

## Support

- **Issues**: https://github.com/timescale/tigerfs/issues
- **Discussions**: https://github.com/timescale/tigerfs/discussions
