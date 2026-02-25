# TigerFS

TigerFS mounts a PostgreSQL database as a directory. Write markdown files, and each file becomes a database row with transactional writes, shared access, and full version history. Drop below the synthesized layer any time to work with native tables, rows, and columns using `ls`, `cat`, `grep`, and `rm` instead of SQL.

## Quick Start

```bash
# Install (macOS requires no dependencies; Linux needs fuse3)
curl -fsSL https://tigerfs.tigerdata.com | sh

# Mount a database
tigerfs mount postgres://localhost/mydb /mnt/db

# Create a markdown app and start writing
echo "markdown" > /mnt/db/.build/notes

cat > /mnt/db/notes/hello.md << 'EOF'
---
title: Hello World
author: alice
---

# Hello World

Welcome to my notes...
EOF

# Search, explore, unmount
grep -l "author: alice" /mnt/db/notes/*.md
ls /mnt/db/notes/
tigerfs unmount /mnt/db
```

## Synthesized Apps

TigerFS can synthesize higher-level file formats on top of database tables. Instead of navigating row directories and column files, you work with files that feel native to their domain.

### Markdown

The first synthesized app presents database rows as `.md` files with YAML frontmatter. Create one with a single command:

```bash
# Create a markdown app
echo "markdown" > /mnt/db/.build/blog

# Write a post — frontmatter becomes columns, body becomes text
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

Organize files into directories — `mkdir` creates folders, `mv` moves files between them:

```bash
mkdir /mnt/db/blog/tutorials
mv /mnt/db/blog/hello-world.md /mnt/db/blog/tutorials/
```

See [docs/markdown-app.md](docs/markdown-app.md) for column mapping, frontmatter handling, and use cases.

### History

Any synthesized app can opt into automatic versioning — every edit and delete is captured as a timestamped snapshot under a read-only `.history/` directory.

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

## Native Table Access

Below the synthesized layer, every table is a directory of rows. Read columns, write JSON, navigate indexes, and chain pipeline queries — all pushed down to the database as optimized SQL.

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

Chain filters, ordering, and pagination in a single path — the database executes it as one query:

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

See [docs/native-tables.md](docs/native-tables.md) for the full reference — row formats, index navigation, pipeline query chaining, schema management workflows, and configuration.

## Cloud Backends

TigerFS integrates with [Tiger Cloud](https://www.timescale.com/cloud) and [Ghost](https://ghost.dev) through their CLIs. Use a prefix to specify the backend:

```bash
# Mount cloud services
tigerfs mount tiger:e6ue9697jf /mnt/db
tigerfs mount ghost:a2x6xoj0oz /mnt/db
```

TigerFS calls the backend CLI to retrieve credentials — no passwords in your config. Authenticate once with `tiger auth login` or `ghost login`.

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

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Unix Tools  │────▶│  Filesystem  │────▶│   TigerFS    │────▶│  PostgreSQL  │
│  ls, cat,    │     │   Backend    │     │   Daemon     │     │   Database   │
│  echo, rm    │◀────│ (FUSE/NFS)   │◀────│              │◀────│              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

TigerFS maps filesystem paths to database queries:

```
  Filesystem                       Database
  ──────────                       ────────
  /mnt/db/                     →   schemas
  /mnt/db/public/              →   tables
  /mnt/db/public/users/        →   rows (by PK)
  /mnt/db/public/users/123/    →   columns as files
```

FUSE on Linux, NFS on macOS — no external dependencies on either platform.

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
| [docs/markdown-app.md](docs/markdown-app.md) | Markdown app — column mapping, frontmatter, directories |
| [docs/history.md](docs/history.md) | Version history — snapshots, cross-rename tracking, recovery |
| [docs/native-tables.md](docs/native-tables.md) | Native table access — row formats, indexes, pipeline queries, schema management |
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

**v0.3.0** — Synthesized apps (markdown views, directory hierarchies, version history) and cloud backend integration with Tiger Cloud and Ghost.

**Highlights:**
- Synthesized markdown apps with YAML frontmatter, directory hierarchies, and automatic version history
- Cloud backends — mount, create, and fork Tiger Cloud and Ghost databases by service ID
- Pipeline queries with full database pushdown (`.by/`, `.filter/`, `.order/`, chained pagination, `.export/`)
- DDL staging for tables, indexes, views, and schemas (`.create/`, `.modify/`, `.delete/`)
- Full CRUD with multiple formats (TSV, CSV, JSON, YAML), index navigation, and PATCH semantics

**Planned:**
- Tables without primary keys (read-only via ctid)
- TimescaleDB hypertables (time-based navigation)
- Distribution (install scripts, GoReleaser, daemon mode)
- Windows support

## Contributing

Contributions are welcome! Please see the development guidelines in [CLAUDE.md](CLAUDE.md).

## Support

- **Issues**: https://github.com/timescale/tigerfs/issues
- **Discussions**: https://github.com/timescale/tigerfs/discussions
