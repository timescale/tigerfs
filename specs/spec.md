# TigerFS - Complete Specification

**Version:** 1.0
**Date:** 2026-01-23
**Status:** Implementation Ready

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Project Overview](#project-overview)
3. [Technology Stack](#technology-stack)
4. [Filesystem Structure](#filesystem-structure)
5. [Data Representation](#data-representation)
6. [Operations (CRUD)](#operations-crud)
7. [Index-Based Navigation](#index-based-navigation)
8. [Large Table Handling](#large-table-handling)
9. [Configuration System](#configuration-system)
10. [CLI Interface](#cli-interface)
11. [Connection and Authentication](#connection-and-authentication)
12. [Tiger Cloud Integration](#tiger-cloud-integration)
13. [File Metadata and Permissions](#file-metadata-and-permissions)
14. [Error Handling](#error-handling)
15. [Schema Metadata](#schema-metadata)
16. [Database Objects](#database-objects)
17. [Special PostgreSQL Types](#special-postgresql-types)
18. [Concurrency and Multi-User](#concurrency-and-multi-user)
19. [Unmounting and Shutdown](#unmounting-and-shutdown)
20. [Logging](#logging)
21. [Performance Monitoring](#performance-monitoring)
22. [Testing Strategy](#testing-strategy)
23. [Distribution and Installation](#distribution-and-installation)
24. [Documentation Plan](#documentation-plan)
25. [Implementation Priorities](#implementation-priorities)
26. [Open Questions](#open-questions)

---

## Executive Summary

**TigerFS** is a FUSE-based filesystem that exposes PostgreSQL database contents as mountable directories. Users interact with tables, rows, and columns using standard Unix tools (`ls`, `cat`, `grep`, `rm`) instead of SQL queries.

**Primary Use Case:** Enable Claude Code and developer tools to explore and manipulate database-backed data using familiar Read/Glob/Grep filesystem operations.

**Key Features:**
- Mount PostgreSQL databases as filesystem directories
- Navigate schemas, tables, rows, and columns like files
- Multiple data formats (TSV, CSV, JSON)
- Index-based navigation for fast lookups
- Full CRUD operations (create, read, update, delete)
- Respects database constraints and permissions
- Cross-platform support (Linux, macOS, Windows)

---

## Project Overview

### Motivation

**Problem:** Database exploration and manipulation requires specialized SQL tools and knowledge. Claude Code and command-line tools can't naturally work with database data.

**Solution:** Present database contents as a filesystem. Standard tools work without modification. Claude Code uses existing Read/Glob/Grep capabilities.

**Benefits:**
- Natural exploration using `ls`, `cat`, `grep`
- No context switching between filesystem and database tools
- Shareable, centralized data (database) with filesystem interface
- Performance hints via filesystem structure (indexes visible)
- Familiar Unix paradigms for database operations

### Target Users

**Primary:**
- Claude Code (AI coding assistant)
- Developers needing quick database exploration
- DevOps engineers managing databases

**Secondary:**
- Data analysts using command-line tools
- Scripts and automation requiring database access
- Tools expecting filesystem interfaces

### Target Platforms

**Supported (MVP):**
- **macOS** - macFUSE required (one-time install)
- **Linux** - Native FUSE support
- **Windows** - WinFsp required (one-time install)
- **Docker** - With FUSE device access (`--device /dev/fuse --cap-add SYS_ADMIN`)
- **VMs/Firecracker** - Full support (real kernel)
- **GitHub Actions** - Linux runners support FUSE

**Not Supported Initially:**
- Managed container platforms (AWS Fargate, Google Cloud Run)
- Kubernetes without privileged pods
- Environments where FUSE is restricted

---

## Technology Stack

### Implementation Language: Go

**Rationale:**
- Excellent PostgreSQL support via `pgx/v5` library
- Mature FUSE libraries (`bazil.org/fuse` or `jacobsa/fuse`)
- Single binary distribution (no runtime dependencies)
- Built-in concurrency model (goroutines, channels)
- Cross-platform compilation trivial
- Fast compilation and execution
- Strong ecosystem for CLI tools

**Minimum Go Version:** 1.23+

### Filesystem Layer: FUSE (Filesystem in Userspace)

**Platform Implementation:**
- **Linux:** Native FUSE (no root needed if user in `fuse` group)
- **macOS:** macFUSE (requires admin to install, then regular users can mount)
- **Windows:** WinFsp (requires admin to install, then regular users can mount)

**Why FUSE:**
- True filesystem mount point
- All tools work without modification
- Standard Unix semantics
- Well-supported across platforms

### Database: PostgreSQL

**Why PostgreSQL:**
- Rich schema introspection (information_schema, pg_catalog)
- Comprehensive index metadata
- Full SQL feature set
- Standard type system with extensions (JSONB, arrays, etc.)
- MVCC for concurrency
- Widely used in production

**Minimum PostgreSQL Version:** 12+ (flexible, may work with older)

**Go Database Library:** `github.com/jackc/pgx/v5`
- High-performance PostgreSQL driver
- Connection pooling built-in
- Full PostgreSQL type support
- Prepared statement support

### Additional Dependencies

**Core:**
- **Cobra** (`github.com/spf13/cobra`) - CLI framework
- **Viper** (`github.com/spf13/viper`) - Configuration management
- **Zap** (`go.uber.org/zap`) - Structured logging

**Testing:**
- **testcontainers-go** (`github.com/testcontainers/testcontainers-go`) - Docker-based integration tests
- **Standard library testing** - Unit tests

**Build/Release:**
- **GoReleaser** (`github.com/goreleaser/goreleaser`) - Cross-platform builds
- **GitHub Actions** - CI/CD automation

---

## Filesystem Structure

### Complete Hierarchy

```
/mount/                                   # Mount point
├── .information_schema/                  # PostgreSQL metadata (global)
│   ├── tables/
│   ├── columns/
│   └── constraints/
├── .refresh                              # Write to force metadata cache clear
├── .stats                                # Performance statistics
├── .stats/json                           # Stats in JSON format
├── table1/                               # Default schema (public) - flattened to root
│   ├── .schema                           # CREATE TABLE statement
│   ├── .columns                          # Column list
│   ├── .indexes                          # Index definitions
│   ├── .count                            # Total row count
│   ├── .sample/                          # Random samples
│   │   └── 100/                          # 100 random rows
│   │       ├── 47392/                    # Actual PKs (row-as-directory)
│   │       └── 103847.json               # Actual PK (row-as-file)
│   ├── .first/                           # First N rows
│   │   └── 50/                           # First 50 rows by PK
│   │       ├── 1/
│   │       └── 2/
│   ├── .email/                           # Single-column index (dotfile)
│   │   └── foo@example.com/              # Indexed lookup
│   ├── .last_name.first_name/            # Composite index
│   │   └── Smith/
│   │       └── Johnson/
│   ├── 123                               # Row-as-file (default: TSV)
│   ├── 123.json                          # Row-as-file (JSON)
│   ├── 123.csv                           # Row-as-file (CSV)
│   ├── 123.tsv                           # Row-as-file (TSV, explicit)
│   └── 123/                              # Row-as-directory
│       ├── email                         # Individual column
│       ├── name                          # Individual column
│       └── age                           # Individual column
├── schema_name/                          # Non-default schema
│   └── table_name/                       # Tables under explicit schema
└── .schemas/                             # Explicit schema access
    ├── public/                           # Default schema (explicit)
    │   └── table1/
    └── analytics/                        # Other schemas
        └── reports/
```

### Schema Handling

**Default Schema Flattening:**
- Tables from default schema (typically `public`) appear at root level: `/mount/table_name/`
- Other schemas require schema prefix: `/mount/schema_name/table_name/`
- All schemas explicitly accessible via `/mount/.schemas/schema_name/table_name/`

**Configuration:**
- Default schema configurable via `--default-schema` flag or config file
- Defaults to `public` if not specified

**Rationale:**
- Reduces typing for common case (public schema)
- Keeps filesystem structure clean
- Still allows explicit schema access

### Namespace Conventions

**Dotfiles (Hidden by Default):**
- `.email/` - Index-based navigation
- `.sample/` - Random row samples
- `.first/` - First N rows
- `.count` - Row count file
- `.schema` - DDL file
- `.columns` - Column list
- `.indexes` - Index list
- `.information_schema/` - Global metadata
- `.schemas/` - Explicit schema access
- `.refresh` - Cache refresh trigger
- `.stats` - Performance monitoring

**Rationale:**
- Standard Unix convention (dotfiles hidden from `ls`)
- `ls -a` reveals metadata and special paths
- Clean default view shows only data

---

## Data Representation

### Dual Representation Model

Every row accessible in **two ways simultaneously**:

#### 1. Row-as-File (Full Row Operations)

Single file containing complete row data. Format determined by file extension.

**File Extensions:**
- No extension (e.g., `/mount/users/123`) → TSV (default)
- `.tsv` → Tab-separated values
- `.csv` → Comma-separated values
- `.json` → JSON object

**Format Details:**

**TSV (Default, Headerless):**
```
foo@example.com	Foo Bar	25
```
- Columns in schema order
- Tab (`\t`) delimiter
- No header row (prevents "header as data" bugs)
- Empty fields = NULL

**CSV (Headerless):**
```
foo@example.com,Foo Bar,25
```
- Columns in schema order
- Comma delimiter
- No header row
- Empty fields = NULL

**JSON:**
```json
{"id":123,"email":"foo@example.com","name":"Foo Bar","age":25}
```
- Compact (single line), not pretty-printed
- All columns included
- `null` for NULL values or omit key

**When to Use:**
- Bulk operations (read/write entire row)
- Atomic updates (single UPDATE statement)
- Efficient for complete row replacement

#### 2. Row-as-Directory (Column-Level Operations)

Row represented as directory with individual column files.

**Structure:**
```
/mount/users/123/
├── id           # Contains: 123
├── email        # Contains: foo@example.com
├── name         # Contains: Foo Bar
└── age          # Contains: 25
```

**Column File Contents:**
- Plain text representation of value
- Empty file (0 bytes) = NULL
- JSONB/JSON = compact JSON
- Arrays = JSON array
- BYTEA = raw binary bytes

**When to Use:**
- Reading specific columns
- Updating individual columns
- Exploring data structure
- Granular access control

### NULL Handling

**Consistent across all formats:**

**TSV/CSV (Empty Fields):**
```
foo@example.com		25
# email="foo@example.com", name=NULL (empty field), age=25
```

**JSON (Omit or Explicit):**
```json
{"email":"foo@example.com","age":25}
# name omitted = NULL

{"email":"foo@example.com","name":null,"age":25}
# name explicit null = NULL
```

**Column Files (Empty File):**
```bash
cat /mount/users/123/middle_name
# (outputs nothing, file is 0 bytes = NULL)

[ -s /mount/users/123/middle_name ] && echo "has value" || echo "is NULL"
# is NULL
```

**Write Operations:**
- Empty field in TSV/CSV → NULL
- Omitted key in JSON → NULL
- Explicit `null` in JSON → NULL
- Writing empty string to column file → Empty string (not NULL)
- To set NULL via column file: `rm` the file (sets to NULL)

### Benefits of Dual Model

**Flexibility:**
- Choose appropriate representation per operation
- Full-row operations use row-as-file (efficient)
- Single-column operations use row-as-directory (granular)

**Atomic Updates:**
- Row-as-file write = single UPDATE (atomic)
- Row-as-directory writes = multiple UPDATEs (not atomic)

**Performance:**
- Reading specific columns via directory avoids fetching entire row
- Writing via file allows bulk operations

---

## Operations (CRUD)

### Read Operations

#### Full Row Read

**Operation:**
```bash
cat /mount/users/123
cat /mount/users/123.json
cat /mount/users/123.csv
```

**SQL Generated:**
```sql
SELECT * FROM users WHERE id = 123;
```

**Format:**
- No extension or `.tsv` → TSV output
- `.csv` → CSV output
- `.json` → JSON output

#### Column Read

**Operation:**
```bash
cat /mount/users/123/email
```

**SQL Generated:**
```sql
SELECT email FROM users WHERE id = 123;
```

**Output:** Plain text value (or empty for NULL)

#### Directory Listing (Small Tables)

**Operation:**
```bash
ls /mount/users/
```

**SQL Generated:**
```sql
SELECT id FROM users ORDER BY id;
```

**Output:** List of primary key values (one per line)

**Constraint:** Tables > `max_ls_rows` (default: 10,000) return EIO with helpful log message directing users to `.first/` or `.sample/`.

#### Indexed Lookups

**Single-Column Index:**
```bash
cat /mount/users/.email/foo@example.com/name
```

**SQL Generated:**
```sql
SELECT name FROM users WHERE email = 'foo@example.com';
```

**Composite Index (Full):**
```bash
cat /mount/users/.last_name.first_name/Smith/Johnson/email
```

**SQL Generated:**
```sql
SELECT email FROM users WHERE last_name = 'Smith' AND first_name = 'Johnson';
```

**Composite Index (Prefix):**
```bash
ls /mount/users/.last_name.first_name/Smith/
```

**SQL Generated:**
```sql
SELECT DISTINCT first_name FROM users WHERE last_name = 'Smith';
```

### Write Operations

#### Full Row Write (INSERT or UPDATE)

**Operation:**
```bash
# TSV
echo -e "foo@example.com\tFoo Bar\t25" > /mount/users/123

# JSON
echo '{"email":"foo@example.com","name":"Foo Bar","age":25}' > /mount/users/123.json
```

**SQL Generated (if row exists):**
```sql
UPDATE users SET email='foo@example.com', name='Foo Bar', age=25 WHERE id=123;
```

**SQL Generated (if row doesn't exist):**
```sql
INSERT INTO users (id, email, name, age) VALUES (123, 'foo@example.com', 'Foo Bar', 25);
```

**Partial Data:**
```bash
# TSV with NULLs
echo -e "foo@example.com\t\t25" > /mount/users/123
# email='foo@example.com', name=NULL, age=25

# JSON with omitted fields
echo '{"email":"foo@example.com","age":25}' > /mount/users/123.json
# name=NULL or default
```

**Write Semantics:**
- TSV/CSV must follow schema column order
- Empty fields (TSV/CSV) or omitted keys (JSON) = NULL or column default
- Constraint violations return EACCES with detailed logs

#### Column Write (UPDATE or INSERT)

**UPDATE Existing Row:**
```bash
echo 'newemail@example.com' > /mount/users/123/email
```

**SQL Generated:**
```sql
UPDATE users SET email='newemail@example.com' WHERE id=123;
```

**INSERT New Row (Incremental):**
```bash
mkdir /mount/users/124
echo 'bar@example.com' > /mount/users/124/email
echo 'Bar User' > /mount/users/124/name
```

**SQL Generated:**
```sql
-- After all column writes complete:
INSERT INTO users (id, email, name) VALUES (124, 'bar@example.com', 'Bar User');
```

**Constraint Enforcement:**
- NOT NULL columns without defaults must be provided
- Foreign key constraints validated
- Check constraints enforced
- Violations return EACCES with helpful error in logs

**Example (Constraint Violation):**
```bash
# Schema: email NOT NULL, name nullable, age default 0

mkdir /mount/users/125
echo 'Some Name' > /mount/users/125/name
# ✗ FAIL: Returns EACCES
# Log: "NOT NULL constraint violation: column 'email' cannot be NULL"
```

### DELETE Operations

#### Delete Column (Set to NULL)

**Operation:**
```bash
rm /mount/users/123/email
```

**SQL Generated:**
```sql
UPDATE users SET email = NULL WHERE id = 123;
```

**Constraint:** Fails if column is NOT NULL without default (returns EACCES)

#### Delete Row

**Row-as-Directory:**
```bash
rm -r /mount/users/123/
```

**Row-as-File:**
```bash
rm /mount/users/123
rm /mount/users/123.json
```

**SQL Generated:**
```sql
DELETE FROM users WHERE id = 123;
```

**Permissions:** Requires DELETE privilege on table

**Cascading:**
- Respects foreign key CASCADE/RESTRICT constraints
- Foreign key violations return EACCES with helpful error

### Transactions and Atomicity

**Model:** No explicit transactions (MVP)

**Behavior:**
- Each filesystem operation = one SQL statement = auto-commit
- Row-as-file write = single UPDATE (atomic at SQL level)
- Row-as-directory writes = multiple UPDATEs (not atomic across files)

**Recommendation for Users:**
- Use row-as-file format for atomic multi-column updates
- Row-as-directory suitable for independent column updates

**Future Enhancement:**
- Could add transaction control via special files (`.begin`, `.commit`, `.rollback`)
- Deferred unless clear demand emerges

---

## Index-Based Navigation

### Philosophy

Index paths are **syntactic sugar for WHERE clauses**:
- Filesystem structure signals "this query will be fast"
- PostgreSQL query planner selects optimal index execution
- Multiple paths may resolve to same data (planner optimizes)

### Index Path Generation

**Automatic Discovery:**
- Query `pg_indexes` to discover all indexes on mount
- Create dot-prefixed directory for each index
- Lazy loading (create on first access, cache)

**Naming Convention:**
- Single-column index: `.column_name/`
- Composite index: `.column1.column2.column3/`

**Example:**
```sql
-- Database indexes:
CREATE INDEX users_email_idx ON users(email);
CREATE INDEX users_name_idx ON users(last_name, first_name);
CREATE UNIQUE INDEX users_username_idx ON users(username);
```

**Filesystem exposes:**
```
/mount/users/
├── .email/              # users_email_idx
├── .last_name.first_name/  # users_name_idx
└── .username/           # users_username_idx
```

### Composite Index Behavior

**Full Composite Path:**
```bash
ls /mount/users/.last_name.first_name/Smith/Johnson/
```
**SQL:** `WHERE last_name = 'Smith' AND first_name = 'Johnson'`

**Prefix Path:**
```bash
ls /mount/users/.last_name.first_name/Smith/
```
**SQL:** `WHERE last_name = 'Smith'` (uses composite index prefix efficiently)

### Automatic Prefix Path Exposure

**Scenario:** Composite index `(last_name, first_name)` exists, but NO single-column index on `last_name`.

**Expose Both:**
- `.last_name/Smith/` → Uses composite index prefix (shortcut)
- `.last_name.first_name/Smith/` → Explicit composite path

**Rationale:**
- Intuitive navigation (use what you know)
- PostgreSQL query planner handles index selection
- Filesystem provides discovery ("what's fast?")

### Non-Indexed Search

**Option 1: Glob with Optimization**
```bash
grep "25" /mount/users/*/age
```

**Potential Optimization:**
- Detect pattern: `grep <pattern> /mount/table/*/column`
- Generate: `WHERE column::text LIKE '%pattern%'` or `WHERE column = 'pattern'`
- Falls back to full scan if pattern not optimizable

**Option 2: Full Table Listing**
```bash
ls /mount/users/ | while read id; do
  cat /mount/users/$id/email
done | grep foo@example.com
```

Straightforward, slow for large tables.

**Recommendation:**
- Support both approaches
- Optimize common grep patterns when detectable
- Large tables require index paths or `.sample/` for exploration

---

## Large Table Handling

### Problem

Tables with millions of rows make `ls /mount/users/` unusable:
- Slow query (full table scan)
- Huge output (millions of lines)
- High memory usage

### Solution: Specialized Access Patterns

**Configuration:**
```yaml
filesystem:
  max_ls_rows: 10000  # Default threshold
```

**Behavior:**
- Tables with ≤ `max_ls_rows` rows: `ls` returns all PKs
- Tables with > `max_ls_rows` rows: `ls` returns EIO
- Error log message: "Table too large (10M rows). Use .first/, .sample/, or index paths"

**Override:**
```bash
# Mount with higher limit
tigerfs --max-ls-rows=50000 postgres://host/db /mnt/db

# Or unlimited
tigerfs --unlimited-ls postgres://host/db /mnt/db

# Or force via special path
ls /mnt/db/users/.all/      # Bypasses limit (always lists all)
```

### Special Directories

#### `.first/N/` - First N Rows by Primary Key

**Usage:**
```bash
ls /mnt/db/users/.first/50/
# 1  2  3  5  7  9  ...  (first 50 PKs)
```

**SQL Generated:**
```sql
SELECT id FROM users ORDER BY id LIMIT 50;
```

**Rationale:**
- Fast (indexed lookup by PK)
- Predictable (always same rows)
- Good for "show me some data"

#### `.sample/N/` - Random N Rows

**Usage:**
```bash
ls /mnt/db/users/.sample/100/
# 47392  103847  291844  ...  (random PKs)
```

**SQL Generated:**
```sql
SELECT id FROM users TABLESAMPLE BERNOULLI(percentage) LIMIT 100;
-- Percentage calculated to approximate N rows
```

**Behavior:**
- Uses PostgreSQL TABLESAMPLE BERNOULLI
- Fast (doesn't require full table scan)
- Approximate (might return fewer than N rows)
- Good enough randomness for exploration

**Future Enhancement:**
- Could add `.random/N/` using `ORDER BY RANDOM()` for exact N rows (slower)

#### `.count` - Total Row Count

**Usage:**
```bash
cat /mnt/db/users/.count
# 10847392
```

**SQL Generated:**
```sql
-- For small tables (< 100K rows):
SELECT count(*) FROM users;

-- For large tables (estimate, fast):
SELECT reltuples::bigint FROM pg_class WHERE relname = 'users';
```

**Rationale:**
- Fast (uses table statistics for large tables)
- Helps users understand table size before exploring

### Direct PK Access Always Works

**Even if table is huge:**
```bash
cat /mnt/db/users/123456789/email
# Works (PK lookup is fast, indexed)
```

**Rationale:**
- Primary key lookups are O(log n), fast regardless of table size
- Don't need to list all rows to access specific row

---

## Configuration System

### Configuration Hierarchy (Precedence: Low to High)

1. **Default values** (in code)
2. **Config file** (`~/.config/tigerfs/config.yaml`)
3. **Environment variables** (`TIGERFS_*` prefix)
4. **Command-line flags**
5. **Connection string parameters** (for connection-specific overrides)

**Later values override earlier values.**

### Config File Format

**Location:** `~/.config/tigerfs/config.yaml`

**Example:**
```yaml
# Connection defaults
connection:
  host: localhost
  port: 5432
  user: myuser
  database: mydb
  # NO password field (use .pgpass, env vars, or password_command)
  default_schema: public
  pool_size: 10
  pool_max_idle: 5
  password_command: "vault kv get -field=password secret/dbfs/prod"

# Filesystem behavior
filesystem:
  max_ls_rows: 10000
  attr_timeout: 1           # FUSE attribute cache (seconds)
  entry_timeout: 1          # FUSE entry cache (seconds)

# Metadata refresh
metadata:
  refresh_interval: 30      # Seconds between automatic refreshes

# Logging
logging:
  level: info               # debug, info, warn, error
  file: ~/.local/share/tigerfs/tigerfs.log
  format: json              # json, text
  rotate_size_mb: 10
  rotate_keep: 5

# Formats
formats:
  default: tsv              # tsv, csv, json
  binary_encoding: raw      # raw, hex, base64 (for BYTEA)
```

### Environment Variables

**PostgreSQL Standard (Connection):**
- `PGHOST` - Database host
- `PGPORT` - Database port
- `PGUSER` - Database user
- `PGDATABASE` - Database name
- `PGPASSWORD` - Database password (not recommended in production)

**TigerFS-Specific:**
- `TIGERFS_CONFIG_DIR` - Config directory (default: `~/.config/tigerfs`)
- `TIGERFS_DEFAULT_SCHEMA` - Default schema to flatten
- `TIGERFS_MAX_LS_ROWS` - Large table threshold
- `TIGERFS_ATTR_TIMEOUT` - FUSE attribute cache timeout
- `TIGERFS_ENTRY_TIMEOUT` - FUSE entry cache timeout
- `TIGERFS_METADATA_REFRESH_INTERVAL` - Metadata refresh interval
- `TIGERFS_LOG_LEVEL` - Logging level
- `TIGERFS_LOG_FILE` - Log file path
- `TIGERFS_LOG_FORMAT` - Log format (json, text)
- `TIGERFS_DEFAULT_FORMAT` - Default row-as-file format
- `TIGERFS_BINARY_ENCODING` - BYTEA encoding
- `TIGERFS_POOL_SIZE` - Connection pool size
- `TIGERFS_PASSWORD` - Database password (alternative to PGPASSWORD)

### Command-Line Flags

**See [CLI Interface](#cli-interface) section for complete flag reference.**

---

## CLI Interface

### Primary Command: mount

**Syntax:**
```bash
tigerfs [mount] [OPTIONS] [CONNECTION] MOUNTPOINT
```

**The `mount` subcommand is optional (default action).**

**Examples:**
```bash
# Simple mount
tigerfs postgres://localhost/mydb /mnt/db

# With options
tigerfs --read-only --max-ls-rows=50000 postgres://host/db /mnt/db

# Using environment variables
export PGHOST=localhost PGDATABASE=mydb
tigerfs /mnt/db

# Foreground mode with debug
tigerfs --foreground --log-level=debug postgres://localhost/mydb /mnt/db
```

### Mount Options and Flags

**Connection/Database:**
```bash
-h, --host HOST           Database host (default: from config/env)
-p, --port PORT           Database port (default: 5432)
-U, --user USER           Database user
-d, --database DB         Database name
--password-command CMD    Execute command to get password
--default-schema SCHEMA   Schema to flatten to root (default: public)
--pool-size N             Connection pool size (default: 10)
--pool-max-idle N         Max idle connections (default: 5)
```

**Filesystem Behavior:**
```bash
--max-ls-rows N           Large table threshold (default: 10000)
--unlimited-ls            Disable row limit for ls operations
--read-only               Mount as read-only
--allow-other             Allow other users to access mount (FUSE)
--allow-root              Allow root + mounting user to access (FUSE)
```

**Caching/Performance:**
```bash
--attr-timeout SECS       FUSE attribute cache timeout (default: 1)
--entry-timeout SECS      FUSE entry cache timeout (default: 1)
--metadata-refresh SECS   Table metadata refresh interval (default: 30)
```

**Logging/Debug:**
```bash
--log-level LEVEL         Log level: debug, info, warn, error (default: info)
--log-file PATH           Log file location
--log-format FORMAT       Log format: text, json (default: json for file, text for stderr)
--foreground              Don't daemonize, run in foreground
-v, --verbose             Increase verbosity (-v, -vv, -vvv)
```

**FUSE-Specific:**
```bash
--fuse-debug              Enable FUSE debug output
-o OPTION                 Pass option to FUSE (standard FUSE flag)
```

**Utility:**
```bash
--version                 Show version information
--help                    Show help message
--config PATH             Config file path (default: ~/.config/tigerfs/config.yaml)
```

### Subcommands

#### 1. mount (default)

**Syntax:**
```bash
tigerfs [mount] [OPTIONS] CONNECTION MOUNTPOINT
```

**Description:** Mount a PostgreSQL database as a filesystem.

**Examples:** See mount options above.

---

#### 2. unmount

**Syntax:**
```bash
tigerfs unmount [OPTIONS] MOUNTPOINT
```

**Description:** Gracefully unmount a TigerFS instance.

**Options:**
```bash
-f, --force           Force unmount even if busy
-t, --timeout SECS    Wait timeout (default: 30)
```

**Examples:**
```bash
tigerfs unmount /mnt/db
tigerfs unmount --force /mnt/db
tigerfs unmount --timeout=60 /mnt/db
```

**Behavior:**
- Waits for in-flight database queries with timeout
- Stops accepting new filesystem operations
- Closes all database connections
- Cleans up mount point
- Works on both active and stale mounts

---

#### 3. status

**Syntax:**
```bash
tigerfs status [MOUNTPOINT]
```

**Description:** Show status of mounted instances.

**Without MOUNTPOINT (list all):**
```bash
tigerfs status

# Output:
MOUNTPOINT      DATABASE           STATUS    UPTIME    QUERIES
/mnt/db1        localhost/mydb     active    2h 15m    1,847
/mnt/db2        prod.example/app   active    5d 3h     892,103
```

**With MOUNTPOINT (detailed view):**
```bash
tigerfs status /mnt/db1

# Output:
Mountpoint:     /mnt/db1
Database:       postgresql://localhost:5432/mydb
User:           myuser
Status:         active
Uptime:         2h 15m 32s
Queries:        1,847 (6.1 qps)
Pool:           3/10 connections active
Config:         ~/.config/tigerfs/config.yaml
Log:            ~/.local/share/tigerfs/tigerfs.log
```

---

#### 4. list

**Syntax:**
```bash
tigerfs list
```

**Description:** List all mounted TigerFS instances (simple output for scripting).

**Output:**
```
/mnt/db1
/mnt/db2
```

---

#### 5. test-connection

**Syntax:**
```bash
tigerfs test-connection [CONNECTION]
```

**Description:** Test database connectivity and verify credentials.

**Examples:**
```bash
tigerfs test-connection postgres://localhost/mydb
tigerfs test-connection  # Uses config file / env vars
```

**Output:**
```
✓ Connected to PostgreSQL 17.2
✓ Database: mydb
✓ User: myuser
✓ Permissions: SELECT, INSERT, UPDATE, DELETE
✓ Tables accessible: 47
```

**Use Cases:**
- Verify credentials before mounting
- Debug connection issues
- Check permissions

---

#### 6. version

**Syntax:**
```bash
tigerfs version
```

**Description:** Show version information.

**Output:**
```
tigerfs v0.1.0
Go: 1.23
Platform: darwin/arm64
FUSE: macFUSE 4.7.2
PostgreSQL: libpq 17.2
```

**Also Available As:** `tigerfs --version`

---

#### 7. config

**Syntax:**
```bash
tigerfs config <subcommand>
```

**Subcommands:**

**show - Display current configuration:**
```bash
tigerfs config show

# Output (shows merged config from all sources):
connection:
  host: localhost          [env: PGHOST]
  user: myuser             [env: PGUSER]
  database: mydb           [env: PGDATABASE]
filesystem:
  max_ls_rows: 50000       [config file]
  default_schema: public   [default]
logging:
  level: info              [config file]
  file: ~/.local/share/tigerfs/tigerfs.log [default]
```

**validate - Validate config file syntax:**
```bash
tigerfs config validate

# Output:
✓ Configuration file is valid
```

**path - Show config file location:**
```bash
tigerfs config path

# Output:
~/.config/tigerfs/config.yaml
```

---

#### 8. help

**Syntax:**
```bash
tigerfs help [COMMAND]
```

**Description:** Show help information.

**Examples:**
```bash
tigerfs help
tigerfs help mount
tigerfs help unmount
```

**Also Available As:** `tigerfs --help`, `tigerfs mount --help`

---

## Connection and Authentication

### Connection Configuration

**Supported Methods:**

**1. Standard PostgreSQL Environment Variables (Primary):**
```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=myuser
export PGDATABASE=mydb
export PGPASSWORD='secret'  # Use .pgpass instead in production

tigerfs /mnt/db
```

**2. Connection String (Explicit):**
```bash
# Without password (recommended)
tigerfs postgres://user@host:port/dbname /mnt/db

# With password (NOT recommended - visible in process list)
tigerfs postgres://user:password@host:port/dbname /mnt/db
```

**3. Config File:**
```yaml
# ~/.config/tigerfs/config.yaml
connection:
  host: localhost
  port: 5432
  user: myuser
  database: mydb
```

**4. Command-Line Flags:**
```bash
tigerfs --host=localhost --user=myuser --database=mydb /mnt/db
```

### Password Handling

**Resolution Order (First Match Wins):**

1. `password_command` output (if specified in config)
2. Connection string password (if present)
3. `TIGERFS_PASSWORD` or `PGPASSWORD` environment variable
4. `~/.pgpass` file (automatic via pgx library)
5. Interactive prompt (if TTY available)
6. Error: No password available

### Password Storage Methods

**1. PostgreSQL .pgpass File (Recommended for Automation):**

**Format:** `~/.pgpass` (mode 0600)
```
hostname:port:database:username:password
localhost:5432:mydb:myuser:secret123
prod.example.com:5432:*:readonly:prodpass123
```

**Usage:**
```bash
# Create .pgpass
echo "localhost:5432:mydb:myuser:secret" >> ~/.pgpass
chmod 0600 ~/.pgpass

# Mount (password loaded automatically)
tigerfs postgres://localhost/mydb /mnt/db
```

**2. password_command (Secret Manager Integration):**

**Config:**
```yaml
connection:
  password_command: "vault kv get -field=password secret/prod-db"
  # Or: "pass show tigerfs/prod"
  # Or: "aws secretsmanager get-secret-value --secret-id dbfs-prod --query SecretString --output text"
  # Or: "kubectl get secret dbfs-creds -o jsonpath='{.data.password}' | base64 -d"
```

**Usage:**
```bash
tigerfs /mnt/db
# Executes password_command, uses stdout as password
```

**3. Environment Variables (Development/Containers):**
```bash
export PGPASSWORD='secret'
tigerfs postgres://localhost/mydb /mnt/db
```

**4. Interactive Prompt (Manual Workflows):**
```bash
tigerfs postgres://user@host/db /mnt/db
Password: ****
```

### Security Warnings

**Documentation will emphasize:**

⚠️ **Never store passwords in config files or connection strings in production.**

**Security Best Practices:**
1. **~/.pgpass file** - PostgreSQL standard, secure (mode 0600)
2. **password_command** - Best for production, integrates with secret managers
3. **Environment variables** - Good for containers/CI/CD
4. **Interactive prompt** - Safe for ad-hoc usage

**Avoid:**
- Passwords in connection strings (visible in `ps`, logs)
- Passwords in config files (risk of git commit)
- Passwords in command-line arguments (visible in shell history)

### Connection Pooling

**FUSE Daemon maintains internal connection pool:**

**Configuration:**
```yaml
connection:
  pool_size: 10          # Max connections (default: 10)
  pool_max_idle: 5       # Max idle connections (default: 5)
```

**Behavior:**
- Concurrent filesystem operations use different connections from pool
- Allows parallel query execution
- pgx library handles connection lifecycle
- Server-side pooling (pgBouncer, etc.) is orthogonal - works transparently

**Rationale:**
- Better performance for concurrent operations
- Standard pattern for database-backed applications
- Configurable based on database capacity

### Multi-Database Support

**Model:** One mount = One database

**Examples:**
```bash
# Mount multiple databases
tigerfs postgres://host/db1 /mnt/db1
tigerfs postgres://host/db2 /mnt/db2

# Access
ls /mnt/db1/public/users/
ls /mnt/db2/public/orders/
```

**Rationale:**
- Simple, predictable
- Clear permission model (credentials per database)
- Matches typical usage (connect to specific database)

**Future Enhancement:**
- Could add database-level hierarchy if demand exists
- Not planned for MVP

---

## Tiger Cloud Integration

### Overview

TigerFS provides seamless integration with Tiger Cloud, allowing users to mount database services directly using their service ID without manually managing connection credentials. This integration relies on the Tiger CLI to retrieve connection details securely.

### Design Philosophy

**Use Existing Tools:** Rather than reimplementing Tiger Cloud authentication in TigerFS, leverage the existing `tiger` CLI for credential management. Users authenticate once with `tiger auth login`, and TigerFS automatically retrieves connection details as needed.

**Explicit Integration:** Tiger Cloud integration is explicitly requested via `--tiger-service-id` flag (or environment variable/config). This avoids confusion with standard PostgreSQL connection strings.

### Command-Line Flag

**Flag Name:** `--tiger-service-id`

**Usage:**
```bash
tigerfs --tiger-service-id=e6ue9697jf /mnt/db
```

**Description:** Specifies a Tiger Cloud service ID to mount. TigerFS will call the Tiger CLI to retrieve connection details securely.

**Rationale for Naming:**
- Explicit and descriptive (not just `--service-id`)
- Clearly indicates Tiger Cloud integration
- Avoids ambiguity with other service concepts

### Environment Variable

**Variable Name:** `TIGER_SERVICE_ID`

**Usage:**
```bash
export TIGER_SERVICE_ID=e6ue9697jf
tigerfs /mnt/db
```

**Rationale for Naming:**
- Consistent with `TIGER_` prefix (not `TIGERFS_SERVICE_ID`)
- Indicates the service ID belongs to Tiger Cloud platform
- Matches naming convention of other Tiger-related environment variables

### Configuration File Option

**Config Key:** `tiger_service_id`

**Usage:**
```yaml
# ~/.config/tigerfs/config.yaml
tiger_service_id: e6ue9697jf
```

**Alternative Config Structure:**
```yaml
tiger:
  service_id: e6ue9697jf
```

**Rationale:** Underscore naming follows YAML convention, matches environment variable format.

### Configuration Precedence

**Resolution Order (First Match Wins):**

1. `--tiger-service-id` command-line flag
2. `TIGER_SERVICE_ID` environment variable
3. `tiger_service_id` config file option
4. Standard connection string/parameters (if no Tiger service ID provided)

**Example:**
```bash
# CLI flag takes precedence over environment
export TIGER_SERVICE_ID=service1
tigerfs --tiger-service-id=service2 /mnt/db
# Uses service2 (flag wins)

# Environment takes precedence over config
# Config: tiger_service_id: service3
export TIGER_SERVICE_ID=service1
tigerfs /mnt/db
# Uses service1 (env wins)

# Config used if no flag or env
# Config: tiger_service_id: service3
tigerfs /mnt/db
# Uses service3 (config)
```

### Implementation Details

#### Connection String Retrieval

**Process:**
1. TigerFS detects `--tiger-service-id`, `TIGER_SERVICE_ID`, or `tiger_service_id` config
2. Calls `tiger db connection-string --with-password --service-id=<service-id>` as subprocess
3. Parses output (PostgreSQL connection string)
4. Uses connection string for database access

**Command Execution:**
```go
cmd := exec.Command("tiger", "db", "connection-string",
    "--with-password",
    "--service-id="+serviceID)

output, err := cmd.Output()
if err != nil {
    return "", fmt.Errorf("failed to get connection string: %w", err)
}

connStr := strings.TrimSpace(string(output))
// connStr = "postgres://tsdbadmin:password@host:port/dbname?sslmode=require"
```

#### Credential Security

**Advantages:**
- Password never stored in TigerFS config files
- Credentials retrieved on-demand from Tiger CLI
- Tiger CLI handles secure storage (keyring, encrypted files)
- Users authenticate once with `tiger auth login`

**Password Handling:**
- `--with-password` flag tells Tiger CLI to include password in connection string
- Password transmitted via stdout (process isolation)
- Connection string used immediately, not persisted
- No plaintext passwords in TigerFS configuration

#### Error Handling

**Tiger CLI Not Found:**
```bash
tigerfs --tiger-service-id=e6ue9697jf /mnt/db

Error: tiger CLI not found in PATH
Suggestion: Install tiger CLI from https://docs.tiger.com/install
```

**Tiger CLI Not Authenticated:**
```bash
tigerfs --tiger-service-id=e6ue9697jf /mnt/db

Error: Not authenticated with Tiger Cloud
Suggestion: Run 'tiger auth login' to authenticate
```

**Service ID Not Found:**
```bash
tigerfs --tiger-service-id=invalid /mnt/db

Error: Service 'invalid' not found
Suggestion: Run 'tiger service list' to see available services
```

**Service Not Running:**
```bash
tigerfs --tiger-service-id=e6ue9697jf /mnt/db

Error: Service 'e6ue9697jf' is not running
Suggestion: Run 'tiger service start e6ue9697jf' to start the service
```

### User Experience Flows

#### Flow 1: First-Time Tiger Cloud User

```bash
# 1. Install tiger CLI
curl -fsSL https://cli.tigerdata.com | sh

# 2. Authenticate
tiger auth login

# 3. Create database service
tiger service create --name my-db
# Created service: e6ue9697jf (set as default)

# 4. Install TigerFS
curl -fsSL https://tigerfs.tigerdata.com | sh

# 5. Mount using service ID
tigerfs --tiger-service-id=e6ue9697jf /mnt/db

# 6. Use filesystem
ls /mnt/db/
cat /mnt/db/users/123/email
```

#### Flow 2: Using Default Service

```bash
# Set default service (done during create or manually)
tiger service list
# e6ue9697jf  my-db  us-east-1  active  (default)

# Mount without specifying service ID (uses default)
export TIGER_SERVICE_ID=$(tiger config get service_id)
tigerfs /mnt/db

# Or set in config file
echo "tiger_service_id: e6ue9697jf" >> ~/.config/tigerfs/config.yaml
tigerfs /mnt/db
```

#### Flow 3: Multiple Services

```bash
# List available services
tiger service list
# e6ue9697jf  production-db   us-east-1  active
# u8me885b93  staging-db      us-west-2  active

# Mount multiple services to different mount points
tigerfs --tiger-service-id=e6ue9697jf /mnt/prod
tigerfs --tiger-service-id=u8me885b93 /mnt/staging

# Access each independently
cat /mnt/prod/users/123/email
cat /mnt/staging/users/456/email
```

### Documentation Examples

**README.md Quick Start:**
```bash
# Mount a Tiger Cloud database
tigerfs --tiger-service-id=<service-id> /mnt/db

# Or set as environment variable
export TIGER_SERVICE_ID=<service-id>
tigerfs /mnt/db

# Or configure in config file
echo "tiger_service_id: <service-id>" >> ~/.config/tigerfs/config.yaml
tigerfs /mnt/db
```

**Help Text:**
```
Flags:
  --tiger-service-id string   Tiger Cloud service ID to mount
                              (also via TIGER_SERVICE_ID env var or
                              tiger_service_id config option)
                              Requires tiger CLI to be installed and
                              authenticated (tiger auth login)

Examples:
  # Mount Tiger Cloud service
  tigerfs --tiger-service-id=e6ue9697jf /mnt/db

  # Use environment variable
  export TIGER_SERVICE_ID=e6ue9697jf
  tigerfs /mnt/db

  # Standard PostgreSQL connection (without Tiger Cloud)
  tigerfs postgres://user@host/db /mnt/db
```

### Alternative: Direct Tiger Cloud API Integration

**Decision:** Use Tiger CLI as intermediary (chosen approach above)

**Alternative Considered:** Direct API integration with Tiger Cloud API

**Rationale for Using Tiger CLI:**
- ✅ No duplicate authentication logic
- ✅ Users authenticate once (`tiger auth login`)
- ✅ Credentials managed by Tiger CLI (keyring, secure storage)
- ✅ Single source of truth for Tiger Cloud access
- ✅ Simpler TigerFS implementation
- ✅ Automatic updates to auth flow benefit TigerFS

**Drawbacks of Direct API:**
- ❌ Duplicate authentication implementation
- ❌ Separate credential storage
- ❌ Users authenticate twice (tiger CLI + TigerFS)
- ❌ More complex, more surface area for bugs
- ❌ Must maintain compatibility with Tiger Cloud API changes

---

## File Metadata and Permissions

### Permissions (Mode Bits)

**Mapping:** PostgreSQL table grants → filesystem permissions

**Permission Check Strategy:**
- Lazy loading: Query permissions on first table access
- Cache in memory for lifetime of mount
- Query: `SELECT has_table_privilege('schema.table', 'SELECT|INSERT|UPDATE|DELETE')`

**Mapping Rules:**
- `SELECT` privilege → read permission (r--)
- `UPDATE` privilege → write permission on existing rows (-w-)
- `INSERT` privilege → write permission on new rows (-w-)
- `DELETE` privilege → can `rm` files/directories

**Example:**
```bash
# User has SELECT + UPDATE on users table
-rw-r--r-- 1 user user  25 Jan 23 10:00 /mnt/db/users/123/email

# User has only SELECT on orders table
-r--r--r-- 1 user user 100 Jan 23 10:00 /mnt/db/orders/456/total
```

**Rationale:**
- Honest representation (see capabilities before trying)
- Standard Unix behavior (`ls -l` shows permissions)
- Better UX (Claude Code knows what's writable)
- Minimal overhead (one query per table, cached)

### Timestamps

**mtime (Modification Time):**
- Check for `updated_at` or `modified_at` column (common convention)
- Use column value if exists
- Fallback to current time if column doesn't exist

**ctime (Change Time) and atime (Access Time):**
- Use same as mtime or current time
- Not critical for this use case

**Rationale:**
- Meaningful timestamps when available
- Graceful degradation for tables without timestamp columns
- No extra queries just for timestamps

### Ownership

**All files owned by user running FUSE daemon:**
```bash
-rw-r--r-- 1 mfreed staff  25 Jan 23 10:00 /mnt/db/users/123/email
```

**Rationale:**
- Simple, predictable
- PostgreSQL roles don't map cleanly to Unix users
- Single daemon process = single Unix user

### File Size

**Column files:**
- Size = byte length of text representation
- Computed on first access (acceptable overhead)

**Row files:**
- Size = byte length of serialized format (TSV/CSV/JSON)
- Computed on demand

**Directories:**
- Size = 0 or 4096 (standard, not meaningful)

**Rationale:**
- Accurate sizes for `ls -lh`, progress bars, etc.
- Small overhead for computing sizes on demand

---

## Error Handling

### Database Errors → Filesystem Errors

**Standard Mappings:**

| PostgreSQL Error                  | Filesystem errno | User Experience                              |
|-----------------------------------|------------------|----------------------------------------------|
| Row not found                     | ENOENT           | No such file or directory                    |
| Permission denied (table grants)  | EACCES           | Permission denied                            |
| NOT NULL constraint violation     | EACCES           | Permission denied (detailed error in logs)   |
| Foreign key violation             | EACCES           | Permission denied (detailed error in logs)   |
| Connection refused                | EIO              | Input/output error                           |
| Connection timeout                | EIO              | Input/output error                           |
| Query timeout                     | EIO              | Input/output error                           |
| Unexpected SQL error              | EIO              | Input/output error                           |

### Logging Strategy

**All errors logged with full detail:**

**Example Log Entry:**
```
2026-01-23 10:30:15 ERROR: NOT NULL constraint violation
  Operation: INSERT INTO users (id, name)
  Error: null value in column "email" violates not-null constraint
  Path: /mnt/db/users/125/name
  User: mfreed
  PID: 12345
```

**User Sees:**
```bash
echo 'Name' > /mnt/db/users/125/name
# (returns EACCES)
echo $?
# 13 (EACCES)
```

**Rationale:**
- Preserve filesystem abstraction (standard errno values)
- Detailed diagnostics in logs for debugging
- Avoid leaking implementation details to users

**Future Enhancement:**
- Could add extended attributes (`getfattr`) for detailed error messages
- Defer unless users request it

---

## Schema Metadata

### Table-Level Metadata Files

**Per-table special files (dotfiles):**

#### `.schema` - Full DDL

**Usage:**
```bash
cat /mnt/db/users/.schema
```

**Output:**
```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  name TEXT,
  age INTEGER DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW()
);
```

**SQL Generated:**
```sql
-- Query information_schema or use pg_dump for specific table
```

---

#### `.columns` - Column List

**Usage:**
```bash
cat /mnt/db/users/.columns
```

**Output:**
```
id
email
name
age
created_at
```

**SQL Generated:**
```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND table_name='users'
ORDER BY ordinal_position;
```

---

#### `.indexes` - Index Definitions

**Usage:**
```bash
cat /mnt/db/users/.indexes
```

**Output:**
```
users_pkey PRIMARY KEY (id)
users_email_idx UNIQUE (email)
users_name_idx (last_name, first_name)
```

**SQL Generated:**
```sql
SELECT * FROM pg_indexes WHERE tablename = 'users';
```

---

#### `.count` - Row Count

**Usage:**
```bash
cat /mnt/db/users/.count
```

**Output:**
```
1847392
```

**SQL Generated:**
```sql
-- For small tables (< 100K rows):
SELECT count(*) FROM users;

-- For large tables (estimate, fast):
SELECT reltuples::bigint FROM pg_class WHERE relname = 'users';
```

---

### Global Metadata: information_schema

**Expose PostgreSQL information_schema as filesystem:**

```bash
/mnt/db/.information_schema/
├── tables/
├── columns/
├── table_constraints/
└── key_column_usage/
```

**Example Usage:**
```bash
# Find all tables
ls /mnt/db/.information_schema/tables/

# Find columns for a table
grep "users" /mnt/db/.information_schema/columns/*
```

**Rationale:**
- Comprehensive metadata access for power users
- Standard PostgreSQL structure (familiar)
- Complements per-table metadata files

---

## Database Objects

### Tables

**Primary Focus:** Regular tables are the main object type exposed by TigerFS.

**Representation:** Tables appear as directories in the filesystem hierarchy.

**Operations:** Full CRUD support (CREATE, READ, UPDATE, DELETE) via filesystem operations.

### Views

**Representation:** Views appear like tables in filesystem

**Behavior:**
```bash
# View looks identical to table
ls /mnt/db/active_users/
cat /mnt/db/active_users/123/email
```

**Distinction via metadata:**
```bash
cat /mnt/db/active_users/.schema
# VIEW: CREATE VIEW active_users AS
#   SELECT * FROM users WHERE active = true;
```

**Write Behavior:**
- Writes to updatable views work (PostgreSQL handles)
- Writes to non-updatable views fail with EACCES
- Error log explains: "Cannot modify non-updatable view"

**Rationale:**
- Consistent interface (views queryable like tables)
- Metadata clearly identifies views
- PostgreSQL handles view update rules

### JOINs

**Approach:** Use database views (not filesystem-level JOINs)

**Rationale:**
- JOINs are complex SQL operations that don't map naturally to filesystem metaphors
- Database views provide the JOIN result as a queryable table
- PostgreSQL handles all JOIN logic, optimization, and caching
- TigerFS treats views identically to tables (consistent interface)

**Example:**
```sql
-- Create view in database (via tiger CLI or psql)
CREATE VIEW user_orders AS
SELECT
  u.id AS user_id,
  u.name AS user_name,
  o.id AS order_id,
  o.total AS order_total,
  o.created_at
FROM users u
JOIN orders o ON u.id = o.user_id;
```

**Filesystem Access:**
```bash
# View appears like any other table
ls /mnt/db/user_orders/
cat /mnt/db/user_orders/123/user_name
cat /mnt/db/user_orders/123/order_total
```

**Benefits:**
- Leverages PostgreSQL's powerful query optimizer
- Views can be materialized for performance
- Consistent filesystem interface (no special JOIN syntax)
- Updatable views support writes (where PostgreSQL allows)
- Complex JOINs, aggregations, filters all supported

**How to Create Views:**
1. Use `tiger db connect` to open psql session
2. Run `CREATE VIEW ...` SQL statement
3. TigerFS automatically discovers new view on next metadata refresh
4. Access view via filesystem like any table

**Alternative Considered:** Filesystem-level JOIN syntax (e.g., `/mnt/db/.join/users+orders/`)
- ❌ Complex to design and implement
- ❌ Limited compared to SQL JOIN capabilities
- ❌ Reinvents what databases do better
- ✅ Views provide cleaner, more powerful solution

### Sequences

**Not exposed initially.**

**Rationale:**
- Internal database mechanics
- Not data access (primary use case)
- Defer until clear use case emerges

**Future Enhancement:**
```bash
cat /mnt/db/.sequences/user_id_seq
# 12847
echo 'nextval' > /mnt/db/.sequences/user_id_seq
# Returns next value
```

### Functions and Stored Procedures

**Not exposed initially.**

**Rationale:**
- Hard to map naturally to filesystem
- Not data access (primary use case)
- Functions with side effects complicate model

**Future Enhancement:**
- Could expose as executable files or special invocation mechanism
- Defer until clear use case

### DDL Operations (Out of Scope)

**Decision:** TigerFS does NOT support DDL (Data Definition Language) operations in MVP.

**DDL Operations Excluded:**
- `CREATE TABLE` - Creating new tables
- `ALTER TABLE` - Modifying table structure (add/drop columns, constraints)
- `DROP TABLE` - Deleting tables
- `CREATE INDEX` - Creating indexes
- `CREATE VIEW` - Creating views
- `DROP VIEW` - Deleting views
- `CREATE SCHEMA` - Creating schemas

**Rationale for Exclusion:**

**1. Poor Filesystem Metaphor:**
- DDL operations don't map naturally to filesystem operations
- Creating tables via filesystem feels forced and unintuitive
- Schema changes are structural, not data-level operations

**2. Better Tools Exist:**
- Tiger CLI provides `tiger db connect` for psql access
- Full SQL DDL capabilities available via psql
- Migration tools (Flyway, Liquibase) handle versioned schema changes
- SQL is the standard, expressive language for DDL

**3. Complexity vs. Value:**
- Implementing DDL adds significant complexity
- Filesystem metaphor works well for data (DML), poorly for schema (DDL)
- Limited user demand for DDL via filesystem
- Core value proposition is data access, not schema management

**4. Schema Change Detection:**
- TigerFS automatically detects schema changes via metadata refresh
- Changes made externally (psql, Tiger CLI) reflected in filesystem
- No need for TigerFS to manage schema lifecycle

**How Users Manage Schemas:**

**Option 1: Tiger CLI (Recommended for Tiger Cloud)**
```bash
# Connect to database with psql
tiger db connect

# Run DDL statements
CREATE TABLE new_table (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

# TigerFS automatically discovers new table
ls /mnt/db/new_table/
```

**Option 2: Direct psql Connection**
```bash
# Connect with standard psql
psql postgres://user@host/database

# Run DDL statements
ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT NOW();

# Changes visible in TigerFS after metadata refresh
cat /mnt/db/users/123/created_at
```

**Option 3: Migration Tools**
```bash
# Use Flyway, Liquibase, or similar
flyway migrate

# Versioned, repeatable, auditable schema changes
# TigerFS reflects changes automatically
```

**Metadata Refresh:**
```bash
# Force immediate metadata refresh
echo 1 > /mnt/db/.refresh

# Or wait for automatic refresh (default: 30 seconds)
```

**Automatic Schema Detection:**
- TigerFS queries `information_schema` on mount
- Periodic refresh every 30 seconds (configurable)
- Manual refresh via `.refresh` file
- New tables, columns, indexes automatically discovered

**Future Consideration:**

If user demand emerges, could consider:
- **Read-only DDL exposure** - View CREATE TABLE statements via `.schema` files (already planned)
- **Simple CREATE TABLE** - Create tables via directory creation with schema file
- **Column addition** - Add columns via special operations

However, these remain deferred until clear use cases justify the complexity.

**Summary:**
- **TigerFS Focus:** Data access (DML) - SELECT, INSERT, UPDATE, DELETE
- **Schema Management:** Use Tiger CLI (`tiger db connect`), psql, or migration tools
- **Automatic Reflection:** Schema changes made externally are automatically detected
- **Clean Separation:** Schema management (DDL) stays in SQL, data access (DML) works via filesystem

---

## Special PostgreSQL Types

### Simple Types

**INTEGER, TEXT, BOOLEAN, etc.:**
```bash
cat /mnt/db/users/123/age
# 25

cat /mnt/db/users/123/active
# true
```

Straightforward text representation.

### NULL Values

**Column files (row-as-directory):**
```bash
cat /mnt/db/users/123/middle_name
# (empty, 0 bytes = NULL)

[ -s /mnt/db/users/123/middle_name ] && echo "has value" || echo "is NULL"
# is NULL
```

### JSONB/JSON

**Storage:**
```bash
cat /mnt/db/users/123/metadata
# {"preferences":{"theme":"dark","lang":"en"},"score":850}
```

**Format:** Compact JSON (single line), not pretty-printed

**Usage:**
```bash
# Pretty-print with jq
cat /mnt/db/users/123/metadata | jq .
```

**Rationale:**
- Parseable with `jq` and JSON tools
- Compact for efficiency
- Users can pretty-print on demand

### Arrays

**Storage:**
```bash
cat /mnt/db/users/123/tags
# ["developer","golang","postgresql"]
```

**Format:** JSON array

**Rationale:**
- Consistent with JSONB representation
- Parseable with standard tools

### BYTEA (Binary Data)

**Storage:** Raw binary bytes

**Usage:**
```bash
cat /mnt/db/users/123/avatar
# (raw PNG/JPEG bytes)

# Works naturally with tools
file /mnt/db/users/123/avatar
# PNG image data, 512 x 512

open /mnt/db/users/123/avatar
# Opens in image viewer

# Encode on demand if needed
cat /mnt/db/users/123/avatar | base64
cat /mnt/db/users/123/avatar | xxd
```

**Rationale:**
- Natural for binary files (images, PDFs, etc.)
- Works directly with tools
- Claude Code can read images natively
- Unix philosophy: raw data, users transform as needed

**Alternative encodings available via config (future):**
```yaml
formats:
  binary_encoding: raw  # raw, hex, base64
```

### Other Types

**TIMESTAMP, DATE, TIME:**
- ISO 8601 format strings
- Example: `2026-01-23T10:30:15Z`

**NUMERIC/DECIMAL:**
- Plain text representation
- Precision preserved

**UUID:**
- Standard UUID string format
- Example: `550e8400-e29b-41d4-a716-446655440000`

**ENUM:**
- Plain text enum value

---

## Concurrency and Multi-User

### Multiple Mounts

**Scenario:**
```bash
# Terminal 1
tigerfs postgres://host/db /mnt/db1

# Terminal 2
tigerfs postgres://host/db /mnt/db2
```

**Behavior:** Allowed, independent mounts

**Characteristics:**
- Each mount has separate FUSE daemon process
- Each daemon has separate connection pool
- No coordination between mounts
- Both see same database data (via PostgreSQL)

**Rationale:**
- Simple, no coordination needed
- Users can mount same database multiple times if desired

### Multiple Processes Accessing Same Mount

**Architecture:**
```
[User Process 1] ─┐
[User Process 2] ─┼─> [OS/VFS] ─> [FUSE Daemon] ─> [PostgreSQL]
[User Process 3] ─┘
```

**Behavior:**
- FUSE daemon is single process handling all requests
- Connection pool allows parallel query execution
- No filesystem-level locking

**Rationale:**
- Standard FUSE model
- PostgreSQL handles concurrency (MVCC)
- Connection pooling provides parallelism

### Concurrent Writes to Same Row

**Scenario:**
```bash
# Process 1
echo 'value1' > /mnt/db/users/123/email

# Process 2 (simultaneously)
echo 'value2' > /mnt/db/users/123/name
```

**Behavior:**
- Two separate UPDATE statements
- PostgreSQL handles concurrency (MVCC, row locking)
- No additional locking in TigerFS
- Last write wins per column

**Rationale:**
- Rely on PostgreSQL's proven concurrency model
- No reinventing database concurrency in filesystem layer
- Standard database semantics

### Multi-User Database Changes

**Challenge:** Other users/processes modify database while TigerFS mounted.

**Data Changes (INSERT/UPDATE/DELETE):**
- Fresh reads always see latest data (no application cache)
- Directory listings may be stale for 1-2 seconds (FUSE kernel cache)
- Direct file access bypasses directory cache (always works)

**Example:**
```bash
ls /mnt/db/users/     # Shows: 1, 2, 3 (cached 1-2 sec)
# Another user inserts row 4
ls /mnt/db/users/     # Still might show: 1, 2, 3 (cache not expired)
cat /mnt/db/users/4/email  # This WORKS (direct access, fresh query)
```

**Schema Changes (CREATE/DROP/ALTER):**
- Table list, column metadata cached until refresh
- Automatic refresh every 30 seconds (configurable)
- Manual refresh: `echo 1 > /mnt/db/.refresh`

**Configuration:**
```yaml
metadata:
  refresh_interval: 30  # Seconds
```

**Manual Refresh:**
```bash
echo 1 > /mnt/db/.refresh
# Clears all cached metadata (table lists, columns, indexes, permissions)
```

**Rationale:**
- Document 1-2 second staleness as acceptable trade-off
- Manual refresh available for immediate consistency
- Most use cases tolerate eventual consistency

---

## Unmounting and Shutdown

### Normal Unmount

**Command:**
```bash
umount /mnt/db
# Or
tigerfs unmount /mnt/db
```

**Behavior:**
1. Stop accepting new filesystem operations
2. Wait for in-flight database queries (timeout: 30 seconds, configurable)
3. Close all database connections in pool
4. Unmount filesystem cleanly
5. Exit daemon process

**If timeout exceeded:**
- Log warning about interrupted queries
- Force unmount
- PostgreSQL cleans up connections server-side

### Signal Handling

**SIGTERM/SIGINT (graceful shutdown):**
1. Stop accepting new operations
2. Wait for active queries (timeout: 30 seconds)
3. Close database connections
4. Unmount filesystem
5. Exit cleanly

**SIGKILL (force kill):**
- Immediate termination
- OS cleans up mount point
- PostgreSQL cleans up connections server-side

### Force Unmount

**Command:**
```bash
umount -f /mnt/db
# Or
tigerfs unmount --force /mnt/db
```

**Behavior:**
- Immediate termination
- May interrupt in-flight queries
- PostgreSQL handles cleanup

### Stale Mounts

**Problem:** FUSE daemon crashes, mount point becomes stale.

**Cleanup:**
```bash
fusermount -u /mnt/db    # Linux
umount /mnt/db           # macOS
# Or
tigerfs unmount /mnt/db  # Works on stale mounts too
```

**TigerFS Cleanup Utility:**
```bash
tigerfs unmount /mnt/db
# Handles both active and stale mounts
# Wrapper around fusermount/umount with better error messages
```

### Unmount Behavior

**No Special Handling for Open Files:**
- Open files are just file descriptors
- No dirty buffers (writes go to DB immediately)
- No risk of data loss
- File descriptors become invalid after unmount (EBADF)

**Real Concern:** In-flight database queries, not open file descriptors.

**Rationale:**
- TigerFS doesn't buffer writes
- Minimal state in FUSE daemon
- Safe to unmount without waiting for file handles

---

## Logging

### Log Levels

**DEBUG:**
- Every filesystem operation (open, read, write, readdir)
- Every SQL query with parameters
- Query execution times
- Cache hits/misses
- Connection pool stats
- Detailed diagnostic info

**INFO:**
- Mount/unmount events
- Configuration summary
- Database connection status
- Metadata refresh events
- Manual cache refresh (via `.refresh`)
- Periodic statistics

**WARN:**
- Slow queries (> 1 second)
- Connection pool exhaustion
- Cache staleness warnings
- Large table listing attempts
- Retry attempts

**ERROR:**
- Query failures (with SQL)
- Constraint violations (with context)
- Connection errors
- Permission denied (with operation)
- Filesystem operation errors

**FATAL:**
- Mount failures (can't connect to DB, FUSE unavailable)
- Configuration errors preventing startup

### Log Destinations

**Background Mode (Default):**
- Logs to file: `~/.local/share/tigerfs/tigerfs.log`

**Foreground Mode (`--foreground`):**
- Logs to stderr (for interactive debugging)
- Also logs to file (optional, via `--log-file`)

**Configuration:**
```yaml
logging:
  level: info                            # debug, info, warn, error
  file: ~/.local/share/tigerfs/tigerfs.log
  format: json                           # text, json
  rotate_size_mb: 10
  rotate_keep: 5
```

### Log Format

**JSON (Default for File):**
```json
{"time":"2026-01-23T10:30:15Z","level":"info","msg":"mount completed","database":"mydb","mountpoint":"/mnt/db"}
{"time":"2026-01-23T10:30:20Z","level":"debug","msg":"query","sql":"SELECT email FROM users WHERE id=123","duration_ms":15}
```

**Text (Human-Readable, Default for stderr):**
```
2026-01-23 10:30:15 INFO  mount completed database=mydb mountpoint=/mnt/db
2026-01-23 10:30:20 DEBUG query sql="SELECT email FROM users WHERE id=123" duration_ms=15
```

**Configuration:**
```bash
# Text format
tigerfs --log-format=text postgres://localhost/db /mnt/db

# JSON format
tigerfs --log-format=json postgres://localhost/db /mnt/db
```

### Log Rotation

**Automatic Rotation:**
- Rotate when log file exceeds 10MB (configurable)
- Keep last 5 rotated files (configurable)
- Rotated files: `tigerfs.log.1`, `tigerfs.log.2`, etc.

**Configuration:**
```yaml
logging:
  rotate_size_mb: 10
  rotate_keep: 5
```

### Periodic Statistics

**Logged every 5 minutes (configurable):**
```
2026-01-23 10:35:00 INFO stats queries=1847 qps=6.1 avg_query_ms=23 p95_query_ms=89 errors=12 pool_active=3/10
```

**Statistics Include:**
- Total queries executed
- Queries per second (QPS)
- Average query time
- P95 query time (95th percentile)
- Error count
- Connection pool utilization

---

## Performance Monitoring

### Filesystem-Based Stats

**Real-time statistics via special files:**

**Overall Stats:**
```bash
cat /mnt/db/.stats

# Output:
Queries: 1,847
Cache hits: 392
Cache misses: 1,455
Avg query time: 23ms
Active connections: 3/10
```

**JSON Format:**
```bash
cat /mnt/db/.stats/json

# Output:
{
  "queries": 1847,
  "cache_hits": 392,
  "cache_misses": 1455,
  "avg_query_ms": 23,
  "pool_active": 3,
  "pool_max": 10
}
```

**Reset Counters:**
```bash
cat /mnt/db/.stats/reset
# Resets all counters, returns confirmation
```

**Per-Table Stats (Optional):**
```bash
cat /mnt/db/users/.stats

# Output:
Queries: 423
Avg query time: 18ms
Most accessed columns: email (145), name (98), age (67)
```

### Metrics Available

**Query Stats:**
- Total queries executed
- Queries per second
- Query duration (avg, p50, p95, p99)
- Query errors

**Connection Pool:**
- Active connections
- Idle connections
- Pool utilization %
- Connection wait time

**Cache (if Implemented):**
- Cache hits/misses
- Cache hit rate %
- Cache size (memory usage)

**Filesystem Operations:**
- Operations per second (read, write, readdir)
- Operation latency
- Errors by type

### Future Enhancements

**HTTP Metrics Endpoint (Deferred):**
```bash
tigerfs --metrics-port=9090 postgres://localhost/db /mnt/db

curl http://localhost:9090/metrics
# Prometheus format
# tigerfs_queries_total 1847
# tigerfs_query_duration_seconds_sum 42.5
```

**Rationale for Deferring:**
- Filesystem-based stats sufficient for MVP
- Claude Code can read stats via `cat`
- HTTP endpoint adds complexity (extra port, service)
- Can add later if Prometheus/Grafana integration needed

---

## Testing Strategy

### Test Categories

**1. Unit Tests:**
- SQL generation from paths
- Permission mapping logic
- Configuration parsing
- Format conversion (TSV/CSV/JSON)
- Type handling (NULL, JSONB, arrays, BYTEA)
- Error mapping (PostgreSQL errors → errno)

**2. Integration Tests (testcontainers-go):**
- Mount/unmount
- Read operations (row-as-file, row-as-directory, indexes)
- Write operations (INSERT, UPDATE, column writes)
- DELETE operations (rm)
- Large table handling (.sample/, .first/)
- Constraint enforcement
- Error scenarios

**3. Filesystem Behavior Tests:**
- POSIX operations (open, read, write, close, unlink)
- Permissions and access control
- Concurrent access
- File metadata (size, timestamps, permissions)

**4. End-to-End Tests (Deferred):**
- Real-world workflows
- Claude Code integration
- Performance benchmarks

### Minimal Viable Integration Test Suite

**Core Operations (MVP):**
1. Mount/unmount
2. Read row-as-file (TSV format)
3. Read via row-as-directory (individual columns)
4. Write row (INSERT via row-as-file)
5. Write row (UPDATE via row-as-directory)
6. DELETE row (`rm` operation)

**Navigation:**
7. Read by single-column index (`.email/foo@example.com/`)
8. Read by composite index (`.last_name.first_name/Smith/Johnson/`)

**Large Tables:**
9. Direct `ls` on large table (should error with max_ls_rows)
10. Read via `.first/N/` (first N rows)
11. Read via `.sample/N/` (random sample)

**Error Handling:**
12. Row not found (ENOENT)
13. Constraint violation (NOT NULL, returns EACCES)

**Total:** 13 integration tests for MVP

### Test Database Schema

**Requirements:**
- Small table (~50 rows) for normal operations
- Large table (>10,000 rows) for pagination tests
- Indexes: single-column + composite
- Constraints: NOT NULL columns, foreign keys
- Various column types (TEXT, INTEGER, JSONB, array, BYTEA)

**Example Schema:**
```sql
-- Small table for testing
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name TEXT,
  age INTEGER DEFAULT 0,
  metadata JSONB,
  tags TEXT[],
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX users_email_idx ON users(email);

-- Large table for pagination testing
CREATE TABLE events (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  event_type TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Seed with > 10,000 rows
INSERT INTO events (user_id, event_type)
SELECT
  (random() * 50)::integer + 1,
  CASE (random() * 3)::integer
    WHEN 0 THEN 'login'
    WHEN 1 THEN 'logout'
    ELSE 'view'
  END
FROM generate_series(1, 15000);
```

### Test Infrastructure

**testcontainers-go Setup:**
```go
func setupTestDB(t *testing.T) (string, func()) {
    ctx := context.Background()

    // Start PostgreSQL container
    pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image: "postgres:17",
            Env: map[string]string{
                "POSTGRES_PASSWORD": "test",
                "POSTGRES_DB":       "testdb",
            },
            ExposedPorts: []string{"5432/tcp"},
        },
        Started: true,
    })
    require.NoError(t, err)

    // Get connection string
    host, _ := pgContainer.Host(ctx)
    port, _ := pgContainer.MappedPort(ctx, "5432")
    connStr := fmt.Sprintf("postgres://postgres:test@%s:%s/testdb", host, port.Port())

    // Seed database
    seedTestData(t, connStr)

    // Return connection string and cleanup function
    cleanup := func() {
        pgContainer.Terminate(ctx)
    }

    return connStr, cleanup
}
```

### Test Execution

**Local Development:**
```bash
# Unit tests (fast)
go test ./internal/...

# Integration tests (slower, needs Docker)
go test -tags=integration ./test/...

# All tests
make test
```

**CI/CD (GitHub Actions):**
```yaml
- name: Run unit tests
  run: go test ./...

- name: Start PostgreSQL
  run: docker run -d -p 5432:5432 postgres:17

- name: Install FUSE
  run: sudo apt-get install -y fuse libfuse-dev

- name: Run integration tests
  run: sudo go test -tags=integration ./test/...
```

### Coverage Goals

**Target:** 80%+ for core logic

**Critical Areas:**
- SQL generation (100%)
- Format conversion (100%)
- Error handling (90%+)
- Permission mapping (90%+)
- CRUD operations (85%+)

---

## Distribution and Installation

### Phase 1: Curl Installer (MVP)

**One-line install:**

**Unix/Linux/macOS:**
```bash
curl -fsSL https://tigerfs.tigerdata.com | sh
```

**Windows (PowerShell):**
```powershell
irm https://tigerfs.tigerdata.com/install.ps1 | iex
```

### Install Script Features

**Capabilities:**
- Auto-detects platform (OS + architecture)
- Downloads appropriate binary from release server
- Verifies SHA256 checksum (security)
- Installs to `~/.local/bin` or `~/bin`
- Makes executable
- Provides PATH instructions if needed

**Supported Platforms:**
- Linux (x86_64, i386, arm64, armv7)
- macOS (x86_64, arm64)
- Windows (x86_64)

### Release Infrastructure

**Components:**

**1. GoReleaser (`.goreleaser.yaml`):**
- Builds for multiple platforms
- Generates checksums
- Creates archives (.tar.gz for Unix, .zip for Windows)
- Uploads to GitHub Releases and S3

**2. GitHub Actions (`.github/workflows/release.yml`):**
- Triggers on semver tags (`v1.2.3`)
- Runs GoReleaser
- Publishes to all targets

**3. S3 + CloudFront CDN:**
- Hosts install scripts and binaries
- Domain: `https://tigerfs.tigerdata.com`
- Files:
  - `/install.sh` - Unix install script
  - `/install.ps1` - Windows install script
  - `/latest.txt` - Latest version number
  - `/releases/{version}/{binary}` - Binaries
  - `/releases/{version}/{binary}.sha256` - Checksums

**4. GitHub Releases:**
- Binaries attached to release
- Checksums included
- Release notes

### Phase 2: Additional Methods (Deferred)

**Homebrew (macOS/Linux):**
```bash
brew install timescale/tap/tigerfs
```

**Debian/Ubuntu (apt):**
```bash
curl -s https://packagecloud.io/install/repositories/timescale/tigerfs/script.deb.sh | sudo bash
sudo apt-get install tigerfs
```

**Red Hat/Fedora (yum):**
```bash
curl -s https://packagecloud.io/install/repositories/timescale/tigerfs/script.rpm.sh | sudo bash
sudo yum install tigerfs
```

**Go Install:**
```bash
go install github.com/timescale/tigerfs/cmd/tigerfs@latest
```

**Rationale for Phasing:**
- Curl installer sufficient for MVP
- Package managers add legitimacy but require more setup
- Can add based on user demand

---

## Documentation Plan

### Phase 1: Essential Documentation (MVP)

**1. README.md (Primary Entry Point)**

**Length:** ~300-500 lines

**Contents:**
- **Overview** - What is TigerFS? One-paragraph pitch
- **Key Features** - Bullet list
- **Quick Start:**
  ```bash
  # Install
  curl -fsSL https://tigerfs.tigerdata.com | sh

  # Mount
  tigerfs postgres://localhost/mydb /mnt/db

  # Use
  ls /mnt/db/public/users/
  cat /mnt/db/public/users/123/email
  ```
- **Installation** - One-line + link to detailed guide
- **Basic Usage** - 5-10 examples
- **Documentation Links** - Point to detailed docs
- **Community/Support** - Issues, discussions
- **License** - Apache 2.0

---

**2. docs/getting-started.md (Tutorial)**

**Length:** ~300-400 lines

**Contents:**
- **First Mount** - Connect to database
- **Exploring Filesystem** - Navigate schemas/tables
- **Reading Data:**
  - Row-as-file (TSV, CSV, JSON)
  - Row-as-directory (columns)
  - Index navigation
- **Writing Data:**
  - Creating rows
  - Updating columns
  - Deleting rows
- **Large Tables** - Using `.sample/`, `.first/`
- **Unmounting** - Clean shutdown
- **Next Steps** - Link to user guide

**Format:** Step-by-step tutorial with examples

---

**3. docs/installation.md**

**Length:** ~200-300 lines

**Contents:**
- **Prerequisites** - FUSE requirements per platform
- **Platform Instructions:**
  - **macOS** - macFUSE installation, permissions
  - **Linux** - FUSE package, user groups
  - **Windows** - WinFsp installation
  - **Docker** - Running with FUSE
  - **GitHub Actions** - CI/CD setup
- **Verification** - Test installation
- **Troubleshooting** - Common install issues
- **Uninstallation** - How to remove

---

**4. CLAUDE.md (Developer Guidance)**

**Length:** ~500-1000 lines

**Contents:**
- **Project Overview** - Architecture, components
- **Development Commands** - Building, testing, running
- **Code Structure** - Package layout
- **Development Best Practices** - Patterns, conventions
- **Testing Guidelines** - Writing/running tests
- **Contributing** - PR process
- **Specifications** - Link to design docs

**Pattern:** Follow tiger-cli's CLAUDE.md structure

---

### Phase 2: Comprehensive Documentation (Post-MVP)

Deferred:
- docs/user-guide.md (comprehensive reference)
- docs/configuration.md (detailed config options)
- docs/examples.md (common patterns)
- docs/claude-code-integration.md (AI assistant guide)

### Phase 3: Advanced Documentation

Deferred:
- docs/troubleshooting.md (based on real issues)
- docs/development.md (for contributors)
- docs/architecture.md (technical deep dive)

### Documentation Principles

1. **Progressive disclosure** - README → Getting Started → Details
2. **Examples first** - Show code before explaining
3. **Platform-specific** - Clear guidance per OS
4. **Copy-pasteable** - All examples work as-is
5. **Keep updated** - Docs with code changes
6. **Link liberally** - Cross-reference

---

## Implementation Priorities

### Phase 1: Core Foundation (Weeks 1-3)

**Week 1: Project Setup**
- Initialize Go project structure
- Set up FUSE library (bazil.org/fuse or jacobsa/fuse)
- Basic mount/unmount functionality
- CLI framework (Cobra) with version command
- Configuration system (Viper) with basic config loading
- Logging infrastructure (Zap)

**Week 2: Basic Read Operations**
- PostgreSQL connection with pgx
- Connection pooling
- Schema/table discovery
- Row-as-file read (TSV format only)
- Directory listing (small tables only)
- Basic SQL generation

**Week 3: Testing Foundation**
- Unit test framework
- testcontainers-go setup
- Integration test infrastructure
- First 5 integration tests passing
- Basic documentation (README)

### Phase 2: Full CRUD (Weeks 4-6)

**Week 4: Complete Read Operations**
- Row-as-directory read (column files)
- Multiple formats (CSV, JSON)
- NULL handling
- Metadata files (.schema, .columns, .indexes)
- Integration tests for reads

**Week 5: Write Operations**
- Row-as-file write (INSERT/UPDATE)
- Row-as-directory write (column-level)
- Constraint enforcement
- Error handling and logging
- Integration tests for writes

**Week 6: DELETE Operations**
- Row deletion (rm files/directories)
- Column deletion (set to NULL)
- Permission checks
- Integration tests for deletes
- Complete CRUD test coverage

### Phase 3: Advanced Features (Weeks 7-9)

**Week 7: Index Navigation**
- Index discovery from pg_indexes
- Single-column index paths
- Composite index paths
- Index-based queries
- Integration tests for indexes

**Week 8: Large Tables**
- max_ls_rows enforcement
- `.first/N/` implementation
- `.sample/N/` implementation (TABLESAMPLE)
- `.count` file
- Integration tests for large tables

**Week 9: Permissions and Metadata**
- Permission mapping (table grants → file permissions)
- Timestamps (updated_at detection)
- File sizes
- Complete metadata exposure
- Default schema flattening

### Phase 4: Polish and Release (Weeks 10-12)

**Week 10: CLI and Configuration**
- All CLI subcommands (unmount, status, list, test-connection, config)
- Complete configuration system
- Password handling (all methods)
- Environment variable support

**Week 11: Distribution**
- GoReleaser setup
- Install scripts (install.sh, install.ps1)
- GitHub Actions release workflow
- S3/CloudFront setup
- First release (v0.1.0)

**Week 12: Documentation and Testing**
- Complete Phase 1 documentation
- Performance testing
- Bug fixes
- User testing feedback incorporation
- v1.0.0 release

### Phase 5: Post-MVP Enhancements

**Later (Prioritize Based on Feedback):**
- Special PostgreSQL types (JSONB, arrays, BYTEA)
- Views and sequences
- Performance monitoring (`.stats`)
- Comprehensive documentation (Phase 2)
- Additional distribution methods (Homebrew, apt, yum)
- Windows support polish
- Claude Code integration guide
- Advanced features (custom views, full-text search)

---

## Open Questions

### For Implementation

**1. FUSE Library Choice:**
- **bazil.org/fuse** vs **jacobsa/fuse**
- Need benchmarks and API comparison
- Decision impacts implementation patterns

**2. Numeric Precision:**
- How to represent PostgreSQL NUMERIC/DECIMAL without loss?
- Plain text representation? Special handling?

**3. Timezone Handling:**
- How to represent TIMESTAMP WITH TIME ZONE?
- ISO 8601 with timezone? UTC conversion?

**4. Character Encoding:**
- UTF-8 everywhere? Handle other encodings?
- PostgreSQL encoding detection?

**5. Query Timeouts:**
- Default timeout for SQL queries?
- Configurable per operation or globally?

**6. Connection String Parameters:**
- Which parameters should be passable via connection string?
- Balance between flexibility and complexity?

**7. Error Recovery:**
- How to handle database disconnections during operations?
- Automatic reconnection? Fail mount?

**8. Large Result Sets:**
- How to handle queries returning millions of rows (directory listings)?
- Streaming? Pagination? Hard limit?

### For Production Use

**9. Security Audit:**
- SQL injection prevention verification
- Password handling review
- Permission model review

**10. Performance Benchmarks:**
- Baseline performance metrics
- Comparison with direct psql access
- Optimization opportunities

**11. Operational Monitoring:**
- Metrics to expose for production
- Health checks
- Alerting integration

**12. Backup/Recovery:**
- Impact on database backups
- Point-in-time recovery considerations

---

## Success Metrics

### Primary Metrics

**Functionality:**
- Claude Code can explore database using Read/Glob/Grep naturally
- All CRUD operations work correctly
- Constraints and permissions enforced
- Cross-platform support (Linux, macOS, Windows)

**Performance:**
- Indexed lookups complete in < 100ms
- Directory listings (small tables) complete in < 500ms
- No noticeable overhead vs direct SQL for single-row access
- Connection pooling enables concurrent operations

**Reliability:**
- Database constraints always enforced
- No data corruption or loss
- Graceful handling of database failures
- Proper cleanup on unmount

**Usability:**
- One-line installation
- Works with default PostgreSQL credentials
- Clear error messages
- Comprehensive documentation

### Secondary Metrics

**Adoption:**
- GitHub stars
- Installation count
- Community engagement (issues, discussions)

**Developer Experience:**
- Time to first successful mount
- Ease of contributing
- Documentation completeness

---

## Appendices

### A. Complete CLI Reference

See [CLI Interface](#cli-interface) section for full details.

**Primary Command:**
```
tigerfs [mount] [OPTIONS] [CONNECTION] MOUNTPOINT
```

**Subcommands:**
- `mount` (default) - Mount database as filesystem
- `unmount` - Unmount filesystem
- `status` - Show mount status
- `list` - List mounted instances
- `test-connection` - Test database connectivity
- `version` - Show version info
- `config` - Configuration management (show, validate, path)
- `help` - Show help

### B. Environment Variables Reference

**PostgreSQL Standard:**
- `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD`

**TigerFS-Specific:**
- All configuration options prefixed with `TIGERFS_`
- See [Configuration System](#configuration-system) for complete list

### C. File Format Specifications

**TSV (Tab-Separated Values):**
- Delimiter: Tab character (`\t`)
- No header row
- Empty fields = NULL
- Columns in schema order
- No quoting or escaping (tabs in data → space)

**CSV (Comma-Separated Values):**
- Delimiter: Comma (`,`)
- No header row
- Empty fields = NULL
- Columns in schema order
- Standard CSV quoting rules (RFC 4180)

**JSON:**
- Compact (single line)
- All columns included
- `null` for NULL or omit key
- Keys in any order

### D. SQL Query Patterns

**Examples of generated SQL:**

**Read row-as-file:**
```sql
SELECT * FROM users WHERE id = 123;
```

**Read column:**
```sql
SELECT email FROM users WHERE id = 123;
```

**Update column:**
```sql
UPDATE users SET email = 'new@example.com' WHERE id = 123;
```

**Insert row:**
```sql
INSERT INTO users (id, email, name, age)
VALUES (123, 'foo@example.com', 'Foo', 25);
```

**Delete row:**
```sql
DELETE FROM users WHERE id = 123;
```

**Indexed lookup:**
```sql
SELECT * FROM users WHERE email = 'foo@example.com';
```

**Large table sample:**
```sql
SELECT id FROM users TABLESAMPLE BERNOULLI(1.0) LIMIT 100;
```

### E. Permission Mapping Reference

| PostgreSQL Privilege | File Permission | Operations Allowed         |
|----------------------|-----------------|----------------------------|
| SELECT               | r--             | read, cat, ls              |
| UPDATE               | -w-             | write to existing files    |
| INSERT               | -w-             | create new files/dirs      |
| DELETE               | -w-             | rm files/directories       |
| None                 | ---             | Access denied              |

### F. Error Code Reference

| PostgreSQL Error           | errno   | Description                    |
|----------------------------|---------|--------------------------------|
| Row not found              | ENOENT  | No such file or directory      |
| Permission denied          | EACCES  | Permission denied              |
| NOT NULL constraint        | EACCES  | Permission denied              |
| Foreign key violation      | EACCES  | Permission denied              |
| Connection error           | EIO     | Input/output error             |
| Timeout                    | EIO     | Input/output error             |
| Other SQL errors           | EIO     | Input/output error             |

---

## Revision History

| Version | Date       | Changes                                                                                          |
|---------|------------|--------------------------------------------------------------------------------------------------|
| 1.0     | 2026-01-23 | Initial complete specification                                                                   |
| 1.1     | 2026-01-23 | Added Tiger Cloud integration (--tiger-service-id); clarified JOINs via views; DDL scope defined |

---

**End of Specification**

This document provides comprehensive guidance for implementing TigerFS. All major design decisions have been made with clear rationales. Implementation can proceed with confidence that the architecture is sound and well-considered.
