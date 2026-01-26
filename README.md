# TigerFS

TigerFS is a FUSE-based filesystem that exposes PostgreSQL database contents as mountable directories, mapping tables, rows, and columns onto files and paths. This allows users, agents, and developer tools to inspect and modify data using standard Unix tools (`ls`, `cat`, `grep`, `rm`) instead of writing SQL. Unlike a traditional disk, the backing store is transactional and supports snapshots and sharing across environments, making the same mount useful as both persistent sandbox state and shared state.

## Overview

TigerFS lets tools and agents work with database state the same way they work with files. Present structured data as a directory tree, and any tool that reads files can now query your database.

For example, `cat /mnt/db/users/123/email` reads a column value, and `echo 'new@example.com' > /mnt/db/users/123/email` updates it.

The filesystem interface is simple and predictable. The database handles durability, consistency, and access control. The mount provides a stable interface that works across local development, sandboxed execution, and shared environments.

**Key features:**
- Mount PostgreSQL databases as filesystem directories
- Navigate schemas, tables, rows, and columns like files
- Read and write data using standard Unix tools
- Multiple data formats (TSV, CSV, JSON)
- Index-based navigation for fast lookups
- Full CRUD operations (create, read, update, delete)
- Respects database constraints and permissions
- Cross-platform support (Linux, macOS, Windows)

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Unix Tools  │────▶│    FUSE      │────▶│   TigerFS    │────▶│  PostgreSQL  │
│  ls, cat,    │     │   Kernel     │     │   Daemon     │     │   Database   │
│  echo, rm    │◀────│   Module     │◀────│              │◀────│              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

TigerFS maps filesystem paths to database queries:

```
  Filesystem                    Database
  ──────────                    ────────
  /mnt/db/                  →   schemas
  /mnt/db/public/           →   tables
  /mnt/db/public/users/     →   rows (by PK)
  /mnt/db/public/users/123  →   SELECT * FROM users WHERE id=123
  /mnt/db/public/users/123/ →   columns as files
```

**Components:**
- **FUSE Layer** - Filesystem interface (read, write, readdir operations)
- **Database Layer** - PostgreSQL client with connection pooling
- **Format Layer** - Data serialization (TSV, CSV, JSON)
- **Configuration** - Viper-based multi-source configuration
- **Logging** - Structured logging with zap

## Quick Start

```bash
# Install TigerFS
curl -fsSL https://tigerfs.tigerdata.com | sh

# Mount a PostgreSQL database
tigerfs postgres://localhost/mydb /mnt/db

# Use standard Unix tools
ls /mnt/db/public/users/
cat /mnt/db/public/users/123/email
echo 'new@example.com' > /mnt/db/public/users/123/email
rm /mnt/db/public/users/456

# Unmount
umount /mnt/db
```

## Installation

### One-Line Installer (macOS/Linux/WSL)

```bash
curl -fsSL https://tigerfs.tigerdata.com | sh
```

### Prerequisites

**macOS:**
- macFUSE must be installed (one-time setup)
- Install: `brew install --cask macfuse`

**Linux:**
- FUSE support (usually pre-installed)
- If needed: `sudo apt-get install fuse` or `sudo yum install fuse`

**Windows:**
- WinFsp must be installed
- Download: https://winfsp.dev/

## Usage

### Mount a Database

```bash
# Using connection string
tigerfs postgres://user@host:port/database /mnt/db

# Using flags
tigerfs --host=localhost --database=mydb /mnt/db

# Mount Tiger Cloud service
tigerfs --tiger-service-id=<service-id> /mnt/db

# Read-only mount
tigerfs --read-only postgres://host/db /mnt/db
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

# Read specific column
cat /mnt/db/users/123/email

# Navigate by index
cat /mnt/db/users/.email/foo@example.com/name
```

### Modify Data

```bash
# Update a column
echo 'newemail@example.com' > /mnt/db/users/123/email

# Update entire row (JSON)
echo '{"email":"new@example.com","name":"New Name"}' > /mnt/db/users/123.json

# Create new row
mkdir /mnt/db/users/456
echo 'user@example.com' > /mnt/db/users/456/email
echo 'User Name' > /mnt/db/users/456/name

# Delete row
rm -r /mnt/db/users/456/
```

### Large Tables

```bash
# First N rows
ls /mnt/db/large_table/.first/100/

# Random sample
ls /mnt/db/large_table/.sample/100/

# Row count
cat /mnt/db/large_table/.count
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

filesystem:
  dir_listing_limit: 10000
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
export TIGERFS_MAX_LS_ROWS=50000
export TIGERFS_LOG_LEVEL=debug
```

PostgreSQL standard environment variables are also supported:

```bash
export PGHOST=localhost
export PGUSER=myuser
export PGDATABASE=mydb
```

## Documentation

- **Complete Specification**: See [specs/spec.md](specs/spec.md)
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

🚧 **Active Development** - TigerFS core functionality is implemented and working:

**Completed:**
- FUSE filesystem with full CRUD operations (Read, Write, Create, Delete)
- Row-as-file and row-as-directory access patterns
- Column-level reads and writes
- PostgreSQL database layer with connection pooling (pgx/v5)
- Data format serialization (TSV, CSV, JSON) with NULL handling
- Constraint validation (NOT NULL, UNIQUE)
- Incremental row creation via mkdir
- Metadata files (.schema, .columns, .count)
- Metadata caching with configurable refresh
- CLI with mount, unmount, status, and config commands
- Comprehensive unit and integration test coverage

**Planned:**
- Index-based navigation (`.email/foo@example.com`)
- Large table pagination (`.first/N/`, `.sample/N/`)
- Composite primary key support
- Permission mapping (PostgreSQL grants → file permissions)
- Distribution (install scripts, GoReleaser packaging)

## Contributing

Contributions are welcome! Please see the development guidelines in [CLAUDE.md](CLAUDE.md).

## Support

- **Issues**: https://github.com/timescale/tigerfs/issues
- **Discussions**: https://github.com/timescale/tigerfs/discussions
