# TigerFS

TigerFS is a FUSE-based filesystem that exposes PostgreSQL database contents as mountable directories, mapping tables, rows, and columns onto files and paths. This allows users, agents, and developer tools to inspect and modify data using standard Unix tools (`ls`, `cat`, `grep`, `rm`) instead of writing SQL. Unlike a traditional disk, the backing store is transactional and supports snapshots and sharing across environments, making the same mount useful as both persistent sandbox state and shared state.

## Overview

TigerFS lets tools and agents work with database state the same way they work with files. Present structured data as a directory tree, and any tool that reads files can now query your database.

For example, `cat /mnt/db/users/123/email.txt` reads a column value, and `echo 'new@example.com' > /mnt/db/users/123/email.txt` updates it.

The filesystem interface is simple and predictable. The database handles durability, consistency, and access control. The mount provides a stable interface that works across local development, sandboxed execution, and shared environments.

**Key features:**
- Mount PostgreSQL databases as filesystem directories
- Navigate schemas, tables, rows, and columns like files
- Read and write data using standard Unix tools
- Automatic file extensions based on column type (.txt, .json, .bin)
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
cat /mnt/db/public/users/123/email.txt
echo 'new@example.com' > /mnt/db/public/users/123/email.txt
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

# Read specific column (text columns have .txt extension)
cat /mnt/db/users/123/email.txt

# Navigate by index
cat /mnt/db/users/.email/foo@example.com/name.txt
```

### Modify Data

```bash
# Update a column
echo 'newemail@example.com' > /mnt/db/users/123/email.txt

# Update entire row (JSON)
echo '{"email":"new@example.com","name":"New Name"}' > /mnt/db/users/123.json

# Create new row
mkdir /mnt/db/users/456
echo 'user@example.com' > /mnt/db/users/456/email.txt
echo 'User Name' > /mnt/db/users/456/name.txt

# Delete row
rm -r /mnt/db/users/456/
```

## Filesystem Navigation

TigerFS provides multiple ways to access and explore your data beyond simple primary key lookups.

### Row Formats

Access any row as a file with different formats:

```bash
cat /mnt/db/users/123         # TSV (default)
cat /mnt/db/users/123.tsv     # TSV (explicit)
cat /mnt/db/users/123.json    # JSON
cat /mnt/db/users/123.csv     # CSV
```

Or access individual columns via row-as-directory:

```bash
ls /mnt/db/users/123/              # List columns
cat /mnt/db/users/123/email.txt    # Read single column
cat /mnt/db/users/123/.json        # Entire row as JSON
cat /mnt/db/users/123/.csv         # Entire row as CSV
```

### Index Navigation

Navigate tables using indexed columns (shown as dotfiles):

```bash
# List available indexes
cat /mnt/db/users/.indexes

# Browse by indexed column
ls /mnt/db/users/.email/                        # List distinct values
cat /mnt/db/users/.email/foo@example.com.json   # Get matching row

# Ordered access within indexes
ls /mnt/db/users/.created_at/.first/10/         # First 10 values (ascending)
ls /mnt/db/users/.created_at/.last/10/          # Last 10 values (descending)

# Pagination within a specific value
ls /mnt/db/users/.status/active/.first/50/      # First 50 active users
ls /mnt/db/users/.status/active/.last/50/       # Last 50 active users
```

### Table Pagination

Handle large tables without loading everything:

```bash
ls /mnt/db/events/.first/100/    # First 100 rows by primary key
ls /mnt/db/events/.last/100/     # Last 100 rows by primary key
ls /mnt/db/events/.sample/100/   # Random sample of 100 rows
cat /mnt/db/events/.count        # Total row count
```

### Table Metadata

Inspect table structure without querying the database directly:

```bash
cat /mnt/db/users/.ddl           # Full CREATE TABLE statement
cat /mnt/db/users/.indexes       # Index names (one per line)
cat /mnt/db/users/.columns       # Column names (one per line)
cat /mnt/db/users/.schema        # Column details (name, type, nullable)
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
export TIGERFS_MAX_LS_ROWS=50000
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
- Row-as-file (TSV, CSV, JSON) and row-as-directory access patterns
- Column-level reads and writes with type-based file extensions
- PostgreSQL database layer with connection pooling (pgx/v5)
- Data format serialization with NULL handling
- Constraint validation (NOT NULL, UNIQUE)
- Incremental row creation via mkdir
- Metadata files (.schema, .columns, .count, .ddl, .indexes)
- Index-based navigation (`.column/value/`) with `.first/N/` and `.last/N/` pagination
- Large table handling (`.first/N/`, `.last/N/`, `.sample/N/`, `.count`)
- Schema flattening (default schema at root, `.schemas/` for explicit access)
- Metadata caching with configurable refresh
- CLI with mount, unmount, status, list, and config commands
- Tiger Cloud integration (`--tiger-service-id`)
- Permission mapping (PostgreSQL grants → file permissions)
- Docker testing environment
- Comprehensive unit and integration test coverage

**Planned:**
- Table and schema creation via filesystem (mkdir, write to .ddl)
- Non-serial primary keys (UUID, text, composite)
- Tables without primary keys (read-only via ctid)
- Database views (read-only for JOINs, updatable for simple views)
- TimescaleDB hypertables (time-based and chunk navigation)
- Distribution (install scripts, GoReleaser packaging, daemon mode)

## Contributing

Contributions are welcome! Please see the development guidelines in [CLAUDE.md](CLAUDE.md).

## Support

- **Issues**: https://github.com/timescale/tigerfs/issues
- **Discussions**: https://github.com/timescale/tigerfs/discussions
