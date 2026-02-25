# TigerFS

TigerFS is a virtual filesystem that exposes PostgreSQL database contents as mountable directories, mapping tables, rows, and columns onto files and paths. This allows users, agents, and developer tools to inspect and modify data using standard Unix tools (`ls`, `cat`, `grep`, `rm`) instead of writing SQL. Unlike a traditional disk, the backing store is transactional and supports snapshots and sharing across environments, making the same mount useful as both persistent sandbox state and shared state.

## Overview

TigerFS lets tools and agents work with database state the same way they work with files. Present structured data as a directory tree, and any tool that reads files can now query your database.

For example, `cat /mnt/db/users/123/email.txt` reads a column value, and `echo 'new@example.com' > /mnt/db/users/123/email.txt` updates it.

The filesystem interface is simple and predictable. The database handles durability, consistency, and access control. The mount provides a stable interface that works across local development, sandboxed execution, and shared environments.

**Key features:**
- Mount PostgreSQL databases as filesystem directories
- Navigate schemas, tables, rows, and columns like files
- Read and write data using standard Unix tools
- Automatic file extensions based on column type (.txt, .json, .bin)
- Multiple data formats for row-based exploration (TSV, CSV, JSON, YAML)
- Index-based navigation for fast lookups
- Pipeline queries with database query pushdown—chain filters, ordering, and pagination in paths
- Full CRUD operations (create, read, update, delete)
- Respects database constraints and permissions
- Multi-user access, across local dev, containers, and in the cloud
- Cross-platform support (Linux, macOS; Windows pending)

## Synthesized Apps

Beyond raw table access, TigerFS can synthesize higher-level file formats on top of database tables. A synthesized app maps rows to domain-specific files — so instead of navigating row directories and column files, you work with files that feel native to their domain.

### Markdown

The first synthesized app presents database rows as `.md` files with YAML frontmatter. Create a markdown app with a single command:

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

Files can be organized into nested directories — `mkdir` creates folders, `mv` moves files between them, and the directory structure is stored in a path column in the database:

```bash
mkdir /mnt/db/blog/tutorials
mv /mnt/db/blog/hello-world.md /mnt/db/blog/tutorials/
ls /mnt/db/blog/tutorials/
```

Or add a markdown view to an existing table:

```bash
echo "markdown" > /mnt/db/posts/.format/markdown
ls /mnt/db/posts_md/
```

See [Markdown App](docs/markdown-app.md) for full documentation.

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

History tracks files across renames via stable row UUIDs, supports per-subdirectory scoping, and uses TimescaleDB hypertables for efficient compressed storage.

See [History](docs/history.md) for full documentation.

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
  /mnt/db/public/users/123     →   SELECT * FROM users WHERE id=123
  /mnt/db/public/users/123/    →   columns as files
  /mnt/db/public/users/123/
    ├── id                     →   integer (no extension)
    ├── email.txt              →   text column
    ├── metadata.json          →   jsonb column
    └── avatar.bin             →   bytea column
```

**Components:**
- **Filesystem Layer** - FUSE (Linux) or NFS (macOS) interface for read, write, readdir operations
- **Database Layer** - PostgreSQL client with connection pooling
- **Format Layer** - Data serialization (TSV, CSV, JSON, YAML)
- **Configuration** - Viper-based multi-source configuration
- **Logging** - Structured logging with zap

## Quick Start

```bash
# Install TigerFS
curl -fsSL https://tigerfs.tigerdata.com | sh

# Mount a PostgreSQL database
tigerfs mount postgres://localhost/mydb /mnt/db

# Or mount a Tiger Cloud / Ghost service directly
tigerfs mount tiger:<service-id> /mnt/db

# Use standard Unix tools
ls /mnt/db/public/users/
cat /mnt/db/public/users/123/email.txt
echo 'new@example.com' > /mnt/db/public/users/123/email.txt
rm /mnt/db/public/users/456

# Unmount
tigerfs unmount /mnt/db
```

## Try the Demos

Self-contained demos with sample data (1,000 users, 200 products, 8,000 orders):

### Docker Demo (any platform)

Runs both TigerFS and PostgreSQL in Docker containers:

```bash
cd scripts/docker-demo
./demo.sh start
./demo.sh shell
# Now explore: ls, cat users/1.json, etc.
./demo.sh stop
```

### macOS Demo (native)

Runs TigerFS natively on macOS with PostgreSQL in Docker:

```bash
cd scripts/mac-demo
./demo.sh start
ls -al /tmp/tigerfs-demo
cat /tmp/tigerfs-demo/users/1.json
./demo.sh stop
```

## Installation

### One-Line Installer (macOS/Linux/WSL)

```bash
curl -fsSL https://tigerfs.tigerdata.com | sh
```

### Prerequisites

**macOS:**
- No dependencies required (uses native NFS backend)

**Linux:**
- FUSE support (usually pre-installed)
- If needed: `sudo apt-get install fuse3` or `sudo yum install fuse3`

**Windows:** (pending)
- Not yet supported

## Usage

### Mount a Database

```bash
# Mount using connection string
tigerfs mount postgres://user@host:port/database /mnt/db

# Mount Tiger Cloud service
tigerfs mount tiger:<service-id> /mnt/db

# Mount Ghost database
tigerfs mount ghost:<database-id> /mnt/db

# Read-only mount
tigerfs mount --read-only postgres://host/db /mnt/db

# Create a new database and mount it
tigerfs create tiger:my-db

# Fork an existing mounted database
tigerfs fork /mnt/db my-experiment

# Show info about a mounted filesystem
tigerfs info /mnt/db
```

### Cloud Backends

TigerFS integrates with [Tiger Cloud](https://www.timescale.com/cloud) and [Ghost](https://ghost.dev) through their CLIs. Use a prefix on the connection argument to specify the backend:

```bash
# Tiger Cloud
tigerfs mount tiger:e6ue9697jf /mnt/db

# Ghost
tigerfs mount ghost:a2x6xoj0oz /mnt/db
```

TigerFS calls the backend CLI to retrieve connection credentials — no passwords in your config. Authenticate once with `tiger auth login` (or `ghost login`) and TigerFS handles the rest.

Set a default backend to skip the prefix:

```bash
# In ~/.config/tigerfs/config.yaml
# default_backend: tiger

tigerfs mount e6ue9697jf /mnt/db    # uses tiger: implicitly
```

### Creating a Database

Create a new cloud database and mount it in one step:

```bash
# Create on Tiger Cloud (auto-mounts to /tmp/my-db)
tigerfs create tiger:my-db

# Create with auto-generated name
tigerfs create tiger:

# Create at a specific mount path
tigerfs create tiger:my-db /mnt/data

# Create without mounting
tigerfs create ghost:my-db --no-mount
```

### Forking a Database

Fork (clone) an existing database for safe experimentation:

```bash
# Fork a mounted database
tigerfs fork /mnt/db my-experiment

# Fork by service ID
tigerfs fork tiger:e6ue9697jf my-experiment

# Fork to a specific path
tigerfs fork /mnt/db /mnt/experiment

# Fork without mounting
tigerfs fork /mnt/db --no-mount
```

The fork is a full copy — write freely without affecting the original.

### Inspecting a Mount

View details about a mounted filesystem and its backing service:

```bash
tigerfs info /mnt/db

# Output:
# Mountpoint:   /mnt/db
# Database:     postgres://host:5432/tsdb (password hidden)
# Backend:      tiger
# Service ID:   e6ue9697jf
# ...

# JSON output for scripting
tigerfs info --json /mnt/db
```

### Explore Data

```bash
# List tables in default schema
ls /mnt/db/

# List rows in a table (shows primary keys)
ls /mnt/db/users/

# Read entire row (TSV format)
cat /mnt/db/users/123.tsv

# Read entire row (JSON format)
cat /mnt/db/users/123.json

# Read specific column (text columns have .txt extension)
cat /mnt/db/users/123/email.txt

# Navigate by index
cat /mnt/db/users/.email/foo@example.com/name.txt
```

### Modify Data

```bash
# Update a column
echo 'newemail@example.com' > /mnt/db/users/123/email.txt

# Update entire row (JSON) - only specified keys are updated
echo '{"email":"new@example.com","name":"New Name"}' > /mnt/db/users/123.json

# Update specific columns (TSV) - header row specifies which columns to update
echo -e 'email\tname\nnew@example.com\tNew Name' > /mnt/db/users/123.tsv

# Update specific columns (CSV) - header row specifies which columns to update
echo -e 'email,name\nnew@example.com,New Name' > /mnt/db/users/123.csv

# Create new row
mkdir /mnt/db/users/456
echo 'user@example.com' > /mnt/db/users/456/email.txt
echo 'User Name' > /mnt/db/users/456/name.txt

# Delete row
rm -r /mnt/db/users/456/
```

**Write Semantics:** All format extensions (`.json`, `.yaml`, `.csv`, `.tsv`) use PATCH semantics - only the columns/keys you specify are updated. Omitted columns retain their existing values.

## Filesystem Navigation

TigerFS provides multiple ways to access and explore your data beyond simple primary key lookups.

### Row Formats

Access any row as a file with different formats:

```bash
cat /mnt/db/users/123.tsv     # TSV
cat /mnt/db/users/123.json    # JSON
cat /mnt/db/users/123.csv     # CSV
cat /mnt/db/users/123.yaml    # YAML
```

Or access individual columns via row-as-directory:

```bash
ls /mnt/db/users/123/              # List columns
cat /mnt/db/users/123/email.txt    # Read single column
cat /mnt/db/users/123/.json        # Entire row as JSON
cat /mnt/db/users/123/.csv         # Entire row as CSV
cat /mnt/db/users/123/.yaml        # Entire row as YAML
```

### Index Navigation

Navigate tables using indexed columns via `.by/`:

```bash
# List available indexes
ls /mnt/db/users/.by/                              # List indexed columns

# Browse by indexed column
ls /mnt/db/users/.by/email/                        # List distinct values
cat /mnt/db/users/.by/email/foo@example.com.json   # Get matching row

# Ordered access within indexes
ls /mnt/db/users/.by/created_at/.first/10/         # First 10 values (ascending)
ls /mnt/db/users/.by/created_at/.last/10/          # Last 10 values (descending)

# Pagination within a specific value
ls /mnt/db/users/.by/status/active/.first/50/      # First 50 active users
ls /mnt/db/users/.by/status/active/.last/50/       # Last 50 active users
```

### Table Pagination

Handle large tables without loading everything:

```bash
ls /mnt/db/events/.first/100/       # First 100 rows by primary key
ls /mnt/db/events/.last/100/        # Last 100 rows by primary key
ls /mnt/db/events/.sample/100/      # Random sample of 100 rows
cat /mnt/db/events/.info/count      # Total row count
```

### Pipeline Queries

Chain multiple operations into a single path. TigerFS pushes the entire pipeline down to the database as one optimized SQL query:

```bash
# Filter by indexed column, order, limit, and export
cat /mnt/db/orders/.by/customer_id/123/.order/created_at/.last/10/.export/json

# Multiple filters (AND-combined)
ls /mnt/db/orders/.by/status/pending/.by/priority/high/

# Filter by any column (not just indexed)
ls /mnt/db/users/.filter/role/admin/.first/50/

# Nested pagination: last 50 of first 100
ls /mnt/db/events/.first/100/.last/50/

# Sample, then sort and export
cat /mnt/db/products/.sample/100/.order/price/.export/csv
```

**Available capabilities:**
- `.by/<col>/<val>/` - Filter by indexed column (fast)
- `.filter/<col>/<val>/` - Filter by any column
- `.order/<col>/` - Sort results
- `.first/N/`, `.last/N/`, `.sample/N/` - Pagination
- `.export/csv`, `.export/json`, `.export/tsv` - Bulk export

For complete documentation, see [docs/pipeline-queries.md](docs/pipeline-queries.md).

### Table Metadata

Inspect table structure without querying the database directly:

```bash
cat /mnt/db/users/.info/ddl         # Full CREATE TABLE statement
cat /mnt/db/users/.info/schema      # Column details (name, type, nullable)
cat /mnt/db/users/.info/columns     # Column names (one per line)
ls /mnt/db/users/.indexes/          # Index names
```

### Schema Management

TigerFS supports creating, modifying, and deleting database tables, indexes, and other objects via a unified staging pattern.

#### Staging Pattern

All DDL operations follow the same workflow:

| Step | Action | Effect |
|------|--------|--------|
| 1 | Read `sql` | See template with context (current schema, examples) |
| 2 | Write `sql` | Stage your DDL (stored in memory) |
| 3 | Touch `.test` | Validate via BEGIN/ROLLBACK (optional) |
| 3b | Read `test.log` | See validation result (optional) |
| 4 | Touch `.commit` | Execute DDL |
| — | Touch `.abort` | Cancel and clear staging |

#### Operations

| Object | Create | Modify | Delete |
|--------|--------|--------|--------|
| Table | `.create/<name>/` | `<table>/.modify/` | `<table>/.delete/` |
| Index | `<table>/.indexes/.create/<idx>/` | — | `<table>/.indexes/<idx>/.delete/` |
| Schema | `.schemas/.create/<name>/` | — | `.schemas/<name>/.delete/` |
| View | `.views/.create/<name>/` | — | `.views/<name>/.delete/` |

#### Human Workflow (Interactive)

Templates help you write correct DDL:

```bash
mkdir /mnt/db/.create/orders           # Create staging directory
cat /mnt/db/.create/orders/sql         # See template with hints
vi /mnt/db/.create/orders/sql          # Edit template
touch /mnt/db/.create/orders/.test     # Validate (optional)
cat /mnt/db/.create/orders/test.log    # See validation result
touch /mnt/db/.create/orders/.commit   # Execute
```

#### Script Workflow (Programmatic)

Create staging directory and write DDL in one command:

```bash
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (id serial PRIMARY KEY, name text)" > /mnt/db/.create/orders/sql
touch /mnt/db/.create/orders/.commit
```

#### Examples

```bash
# Create table (human workflow)
mkdir /mnt/db/.create/orders           # Create staging directory
vi /mnt/db/.create/orders/sql          # Edit template: add your columns
touch /mnt/db/.create/orders/.test     # Validate (optional)
cat /mnt/db/.create/orders/test.log    # View validation result
touch /mnt/db/.create/orders/.commit   # Execute

# Create table (script workflow)
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (id serial PRIMARY KEY, name text)" > /mnt/db/.create/orders/sql
touch /mnt/db/.create/orders/.commit

# Modify table (human workflow)
vi /mnt/db/users/.modify/sql           # Edit: see current schema, add ALTER statement
touch /mnt/db/users/.modify/.test      # Validate (optional)
cat /mnt/db/users/.modify/test.log     # View validation result
touch /mnt/db/users/.modify/.commit    # Execute

# Modify table (script workflow)
echo "ALTER TABLE users ADD COLUMN status text" > /mnt/db/users/.modify/sql
touch /mnt/db/users/.modify/.commit

# Delete table (template shows row count, foreign keys)
cat /mnt/db/users/.delete/sql          # Review what will be deleted
echo "DROP TABLE users CASCADE" > /mnt/db/users/.delete/sql
touch /mnt/db/users/.delete/.commit

# Create index
mkdir /mnt/db/users/.indexes/.create/email_idx && echo "CREATE INDEX email_idx ON users(email)" > /mnt/db/users/.indexes/.create/email_idx/sql
touch /mnt/db/users/.indexes/.create/email_idx/.commit

# Create view
mkdir /mnt/db/.views/.create/active_users && echo "CREATE VIEW active_users AS SELECT * FROM users WHERE active" > /mnt/db/.views/.create/active_users/sql
touch /mnt/db/.views/.create/active_users/.commit
```

## Configuration

Configuration file: `~/.config/tigerfs/config.yaml`

```yaml
connection:
  host: localhost
  port: 5432
  user: myuser
  database: mydb
  default_schema: public
  pool_size: 10

# Cloud backend (tiger or ghost) — enables bare names without prefix
default_backend: tiger

filesystem:
  dir_listing_limit: 10000
  no_filename_extensions: false  # Set true to disable .txt/.json/.bin extensions
  attr_timeout: 1
  entry_timeout: 1

logging:
  level: info
  file: ~/.local/share/tigerfs/tigerfs.log
  format: json
```

### Environment Variables

All configuration options support environment variables with `TIGERFS_` prefix:

```bash
export TIGERFS_DIR_LISTING_LIMIT=50000
export TIGERFS_LOG_LEVEL=debug
export TIGERFS_NO_FILENAME_EXTENSIONS=true  # Disable .txt/.json extensions
```

PostgreSQL standard environment variables are also supported:

```bash
export PGHOST=localhost
export PGUSER=myuser
export PGDATABASE=mydb
```

## Documentation

- **Complete Specification**: See [docs/spec.md](docs/spec.md)
- **Design Plans**: See [docs/.plans/](docs/.plans/)

## Development

```bash
# Clone repository
git clone https://github.com/timescale/tigerfs.git
cd tigerfs

# Build
go build -o bin/tigerfs ./cmd/tigerfs

# Run
./bin/tigerfs --help

# Test
go test ./...
```

For detailed development information, see [CLAUDE.md](CLAUDE.md).

## Project Status

🚧 **Active Development** — v0.3.0 adds synthesized apps (markdown views, directory hierarchies) and cloud backend integration with Tiger Cloud and Ghost.

**Completed:**
- Virtual filesystem with full CRUD operations (read, write, create, delete)
- Multiple data formats: TSV, CSV, JSON, YAML with PATCH semantics on write
- Row-as-file and row-as-directory access patterns
- Column-level reads/writes with type-based file extensions (.txt, .json, .bin)
- All primary key types (serial, UUID, text, composite)
- Database views (read-only and updatable)
- Index navigation (`.by/column/value/`) with pagination
- Pipeline queries with database pushdown (`.by/`, `.filter/`, `.order/`, chained pagination)
- Large table handling (`.first/N/`, `.last/N/`, `.sample/N/`)
- Bulk export/import (`.export/`, `.import/`)
- DDL operations via filesystem (`.create/`, `.modify/`, `.delete/` for tables, indexes, views)
- Schema management (flattening, `.schemas/` for explicit access)
- Metadata directory (`.info/` with schema, columns, count, ddl, indexes)
- CLI commands: mount, unmount, status, list, info, create, fork, config
- Cloud backends: Tiger Cloud (`tiger:ID`) and Ghost (`ghost:ID`) with prefix scheme
- macOS native NFS backend (no dependencies required)
- Linux FUSE backend
- PostgreSQL connection pooling (pgx/v5)
- Permission mapping (PostgreSQL grants → file permissions)
- Rename/mv support (primary key updates, cross-directory moves)
- Comprehensive test coverage
- Synthesized apps: markdown (`.build/` and `.format/markdown`)
- Claude Code skills for discovering, reading, writing, and searching mounted data
- Docker demo with sample data for quick start

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
