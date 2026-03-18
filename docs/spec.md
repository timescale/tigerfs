# TigerFS - Complete Specification

**Version:** 1.2
**Date:** 2026-02-25
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
9. [Pipeline Query Architecture](#pipeline-query-architecture)
10. [Synthesized Apps](#synthesized-apps)
11. [History](#history)
12. [Configuration System](#configuration-system)
13. [CLI Interface](#cli-interface)
14. [Connection and Authentication](#connection-and-authentication)
15. [Cloud Backend Integration](#cloud-backend-integration)
16. [File Metadata and Permissions](#file-metadata-and-permissions)
17. [Error Handling](#error-handling)
18. [Schema Metadata](#schema-metadata)
19. [Database Objects](#database-objects)
20. [Special PostgreSQL Types](#special-postgresql-types)
21. [Concurrency and Multi-User](#concurrency-and-multi-user)
22. [Unmounting and Shutdown](#unmounting-and-shutdown)
23. [Logging](#logging)
24. [Performance Monitoring](#performance-monitoring)
25. [Testing Strategy](#testing-strategy)
26. [Distribution and Installation](#distribution-and-installation)
27. [Documentation Plan](#documentation-plan)
28. [Implementation Priorities](#implementation-priorities)
29. [Open Questions](#open-questions)

---

## Executive Summary

**TigerFS** is a FUSE-based filesystem that exposes PostgreSQL database contents as mountable directories. Users interact with tables, rows, and columns using standard Unix tools (`ls`, `cat`, `grep`, `rm`) instead of SQL queries.

**Primary Use Case:** Enable Claude Code and developer tools to explore and manipulate database-backed data using familiar Read/Glob/Grep filesystem operations.

**Key Features:**
- Mount PostgreSQL databases as filesystem directories
- Navigate schemas, tables, rows, and columns like files
- Multiple data formats (TSV, CSV, JSON, YAML)
- Index-based navigation for fast lookups
- Full CRUD operations (create, read, update, delete)
- Synthesized apps present rows as domain-specific files (e.g., markdown with YAML frontmatter) with automatic versioning
- Cloud backend integration (Tiger Cloud, Ghost) with prefix scheme
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
- **macOS** - No dependencies (native NFS backend)
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
- **macOS:** Native NFS backend (no installation required)
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
│   ├── .by/                              # Index-based navigation
│   │   ├── email/                        # Single-column index
│   │   │   ├── foo@example.com/          # Indexed lookup
│   │   │   ├── .first/10/                # First 10 distinct values
│   │   │   └── .last/10/                 # Last 10 distinct values
│   │   └── last_name.first_name/         # Composite index
│   │       └── Smith/
│   │           └── Johnson/
│   ├── .info/                            # Table metadata
│   │   ├── count                         # Total row count
│   │   ├── ddl                           # Complete DDL (indexes, constraints, etc.)
│   │   ├── schema                        # CREATE TABLE statement
│   │   └── columns                       # Column list
│   ├── .indexes/                         # Index metadata and DDL operations
│   │   ├── .create/                      # Staging for new indexes
│   │   │   └── <idx>/                    # mkdir creates staging entry
│   │   │       ├── sql                   # Staged CREATE INDEX DDL
│   │   │       ├── .test                 # Touch to validate (optional)
│   │   │       ├── test.log              # Validation result (read-only)
│   │   │       ├── .commit               # Touch to execute
│   │   │       └── .abort                # Touch to cancel
│   │   └── <idx>/                        # Existing index directories
│   │       ├── .schema                   # CREATE INDEX DDL (read-only)
│   │       └── .delete/                  # Staging for deletion
│   │           ├── sql                   # DROP INDEX template
│   │           ├── .test                 # Touch to validate (optional)
│   │           ├── test.log              # Validation result (read-only)
│   │           ├── .commit               # Touch to execute
│   │           └── .abort                # Touch to cancel
│   ├── .sample/                          # Random samples
│   │   └── 100/                          # 100 random rows
│   │       ├── 47392/                    # Row directory
│   │       └── 103847/                   # Row directory
│   ├── .first/                           # First N rows (ascending)
│   │   └── 50/                           # First 50 rows by PK
│   │       ├── 1/                        # Row directory
│   │       └── 2/                        # Row directory
│   ├── .last/                            # Last N rows (descending)
│   │   └── 50/                           # Last 50 rows by PK
│   │       ├── 99999/                    # Row directory
│   │       └── 99998/                    # Row directory
│   └── 123/                              # Row directory - shown in ls
│       ├── email.txt                     # Individual column file
│       ├── name.txt                      # Individual column file
│       ├── age                           # Individual column (no extension for numeric)
│       ├── .json                         # Full row as JSON (inside row directory)
│       ├── .csv                          # Full row as CSV
│       └── .tsv                          # Full row as TSV
│   # Note: Row files (123.json, 123.csv) are accessible but not shown in ls
├── schema_name/                          # Non-default schema
│   └── table_name/                       # Tables under explicit schema
└── .schemas/                             # Explicit schema access
    ├── public/                           # Default schema (explicit)
    │   └── table1/
    └── analytics/                        # Other schemas
        └── reports/
```

#### Synthesized App and DDL Layer

Synthesized apps, DDL operations, and pipeline queries add additional dotfile directories at the root and table levels:

```
/mount/
├── .build/                               # Create synthesized apps
│   └── <name>                            # Write format (e.g., "markdown") to create
├── .create/                              # DDL: create new tables
│   └── <name>/                           # Staging directory (mkdir, edit sql, touch .commit)
├── .views/                               # Database views
│   └── .create/<name>/                   # Create new view
├── table1/                               # Native table
│   ├── .filter/                          # Filter by any column (pipeline query)
│   │   └── <col>/<val>/                  # Chained filter
│   ├── .order/                           # Sort results (pipeline query)
│   │   └── <col>/                        # Chained ordering
│   ├── .columns/                         # Column projection (pipeline query)
│   │   └── col1,col2,col3/              # Comma-separated column names
│   ├── .export/                          # Bulk export (pipeline query)
│   │   └── csv|json|tsv                  # Format file
│   ├── .format/                          # Add synthesized view to existing table
│   │   └── markdown                      # Write to create markdown view
│   ├── .modify/                          # DDL: ALTER table
│   │   └── sql, .test, .commit, .abort   # Staging files
│   ├── .delete/                          # DDL: DROP table
│   │   └── sql, .test, .commit, .abort   # Staging files
│   └── ...                               # (existing: .by/, .info/, .indexes/, rows)
├── notes/                                # Synthesized app (created via .build/)
│   ├── hello.md                          # Synthesized file (row as markdown)
│   ├── tutorials/                        # Subdirectory (directory hierarchy)
│   │   ├── intro.md
│   │   └── .history/                     # Per-directory history
│   │       └── intro.md/                 # Versions of intro.md
│   └── .history/                         # Version history (root level)
│       ├── .by/                          # UUID-based lookups
│       │   └── <uuid>/                   # Versions for a specific row UUID
│       └── hello.md/                     # Versions of a file
│           ├── .id                       # Row UUID
│           └── 2026-02-24T150000Z        # Timestamped snapshot (read-only)
└── _notes/                               # Backing native table (underscore prefix)
    └── 1/                                # Standard row-as-directory access
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
- `.by/` - Index-based navigation (`.by/email/`, `.by/created_at/`)
- `.build/` - Synthesized app creation (root level)
- `.columns/` - Column projection (`.columns/id,name,email/`) (pipeline query)
- `.create/` - DDL staging for new tables/indexes
- `.delete/` - DDL staging for table/object deletion
- `.export/` - Bulk export in CSV, JSON, or TSV format (pipeline query)
- `.filter/` - Non-indexed column filtering (pipeline query)
- `.first/` - First N rows (ascending by PK)
- `.format/` - Add synthesized view to existing table
- `.history/` - Version history for synthesized apps
- `.indexes/` - Index metadata directory (list indexes, view DDL, create/delete)
- `.info/` - Table metadata (count, ddl, schema, columns)
- `.information_schema/` - Global metadata
- `.last/` - Last N rows (descending by PK)
- `.modify/` - DDL staging for table modifications
- `.order/` - Result ordering (pipeline query)
- `.refresh` - Cache refresh trigger
- `.sample/` - Random row samples
- `.schemas/` - Explicit schema access
- `.stats` - Performance monitoring
- `.views/` - Database views and view creation

**Rationale:**
- Standard Unix convention (dotfiles hidden from `ls`)
- `ls -a` reveals metadata and special paths
- Clean default view shows only data

---

## Data Representation

### Dual Representation Model

Every row accessible in **two ways**:

1. **Row Directories** (`123/`) - Shown in directory listings, contain column files
2. **Row Files** (`123.json`, `123.csv`, `123.tsv`) - Accessible but not shown in listings

**Directory Listing Behavior:**
```bash
$ ls /mount/users/
.all/  .by/  .columns/  .export/  .filter/  .first/  .import/  .info/  .last/  .order/  .sample/
1/  2/  3/  ...

$ cd /mount/users/1        # Enter row directory
$ ls
email.txt  name.txt  age  .json  .csv  .tsv  .yaml

$ cat /mount/users/1.json  # Direct access to row file (not in listing)
{"id":1,"email":"alice@example.com",...}
```

Row directories are shown in listings and allow column-level operations. Row files (`1.json`, `1.csv`) are accessible via direct path or tab-completion but not shown in `ls` to keep listings clean.

#### 1. Row Files (Full Row Operations)

Single file containing complete row data. Format determined by file extension. Accessible via direct path but not shown in directory listings.

**File Extensions:**
- `.json` → JSON object
- `.csv` → Comma-separated values
- `.tsv` → Tab-separated values

**Format Details (Reading):**

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
- `null` for NULL values

**Format Details (Writing):**

All format extensions (`.tsv`, `.csv`, `.json`, `.yaml`) use **PATCH semantics**:
- Only specified columns are updated
- Omitted columns retain their existing values

**TSV/CSV Writing (Header Required):**
```bash
# First line: column names (header)
# Second line: values
echo -e "email\tname\nnew@example.com\tNew Name" > /mount/users/123.tsv
```

**JSON/YAML Writing:**
```bash
# Keys specify columns to update
echo '{"email":"new@example.com","name":"New Name"}' > /mount/users/123.json
```

**When to Use:**
- Bulk operations (read/write entire row)
- Atomic updates (single UPDATE statement)
- Partial updates (PATCH semantics with format extension)

#### 2. Row Directories (Column-Level Operations)

Row represented as directory with individual column files. Accessible via `cd /table/pk/` but not shown in table directory listings.

**Structure:**
```
/mount/users/123/
├── id              # Contains: 123 (integer, no extension)
├── email.txt       # Contains: foo@example.com
├── name.txt        # Contains: Foo Bar
├── age             # Contains: 25 (integer, no extension)
├── metadata.json   # Contains: {"role":"admin"}
├── avatar.bin      # Contains: (binary data)
├── .json           # Entire row as JSON
├── .csv            # Entire row as CSV
├── .tsv            # Entire row as TSV
└── .yaml           # Entire row as YAML
```

**Row Format Files:**
Within a row directory, `.json`, `.csv`, `.tsv`, and `.yaml` provide the entire row in that format:
```bash
cat /mnt/db/users/123/.json    # {"id":123,"email":"foo@example.com",...}
cat /mnt/db/users/123/.csv     # 123,foo@example.com,Foo Bar,25
cat /mnt/db/users/123/.tsv     # 123	foo@example.com	Foo Bar	25
cat /mnt/db/users/123/.yaml    # ---\nid: 123\nemail: foo@example.com\n...
```

**Filename Extensions:**

Column files have extensions based on PostgreSQL data type:

| PostgreSQL Type | Extension | Example |
|-----------------|-----------|---------|
| TEXT, VARCHAR, CHAR | `.txt` | `name.txt` |
| JSON, JSONB | `.json` | `metadata.json` |
| XML | `.xml` | `config.xml` |
| BYTEA | `.bin` | `avatar.bin` |
| GEOMETRY, GEOGRAPHY | `.wkb` | `location.wkb` |
| INTEGER, NUMERIC, BOOLEAN, DATE, etc. | (none) | `age`, `active` |
| Arrays (all types) | (none) | `tags` |

Both the extended filename (`name.txt`) and bare column name (`name`) work for lookups.
Use `--no-filename-extensions` to disable extensions entirely.

**Column File Contents:**
- Plain text representation of value
- Empty file (0 bytes) = NULL
- JSONB/JSON = compact JSON
- Arrays = PostgreSQL array format `{a,b,c}`
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

**Output:**
- Capability directories: `.all/`, `.by/`, `.columns/`, `.export/`, `.filter/`, `.first/`, `.import/`, `.info/`, `.last/`, `.order/`, `.sample/`
- Row directories: `1/`, `2/`, `3/`, ... (primary key values)

**Note:** Row files (`1.json`, `1.csv`, etc.) are accessible via direct path but not shown in listings.

**Constraint:** Tables > `dir_listing_limit` (default: 10,000) return EIO with helpful log message directing users to `.first/` or `.sample/`.

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
# TSV with explicit extension (PATCH semantics - header row specifies columns)
echo -e "email\tname\nfoo@example.com\tFoo Bar" > /mount/users/123.tsv

# CSV with explicit extension (PATCH semantics - header row specifies columns)
echo -e "email,name\nfoo@example.com,Foo Bar" > /mount/users/123.csv

# JSON (PATCH semantics - keys specify columns)
echo '{"email":"foo@example.com","name":"Foo Bar"}' > /mount/users/123.json

# YAML (PATCH semantics - keys specify columns)
echo -e "email: foo@example.com\nname: Foo Bar" > /mount/users/123.yaml
```

**SQL Generated (if row exists - only specified columns updated):**
```sql
UPDATE users SET email='foo@example.com', name='Foo Bar' WHERE id=123;
```

**SQL Generated (if row doesn't exist):**
```sql
INSERT INTO users (id, email, name) VALUES (123, 'foo@example.com', 'Foo Bar');
```

**Setting NULL Values:**
```bash
# TSV: empty field in value row = NULL
echo -e "email\tname\nfoo@example.com\t" > /mount/users/123.tsv
# Sets email='foo@example.com', name=NULL

# JSON: explicit null value
echo '{"email":"foo@example.com","name":null}' > /mount/users/123.json
# Sets email='foo@example.com', name=NULL
```

**Write Semantics:**
- All format extensions use **PATCH semantics** - only specified columns are updated
- `.tsv`/`.csv`: First line is header (column names), second line is values
- `.json`/`.yaml`: Keys in the object specify which columns to update
- Empty fields (TSV/CSV) or explicit null (JSON/YAML) set the column to NULL
- Omitted columns are NOT modified (retain existing value)
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

### Index Pagination

Indexes support `.first/N/` and `.last/N/` pagination at two levels:

#### Pagination of Distinct Values

**Usage:**
```bash
# First 10 distinct values (ascending order)
ls /mnt/db/users/.created_at/.first/10/
# 2024-01-01T00:00:00Z  2024-01-02T00:00:00Z  ...

# Last 10 distinct values (descending order)
ls /mnt/db/users/.created_at/.last/10/
# 2026-01-27T00:00:00Z  2026-01-26T00:00:00Z  ...
```

**SQL Generated:**
```sql
-- .first/10/
SELECT DISTINCT created_at FROM users WHERE created_at IS NOT NULL ORDER BY created_at ASC LIMIT 10;

-- .last/10/
SELECT DISTINCT created_at FROM users WHERE created_at IS NOT NULL ORDER BY created_at DESC LIMIT 10;
```

#### Pagination Within a Value

**Usage:**
```bash
# First 50 rows with status='active' (by PK ascending)
ls /mnt/db/users/.status/active/.first/50/

# Last 50 rows with status='active' (by PK descending)
ls /mnt/db/users/.status/active/.last/50/
```

**SQL Generated:**
```sql
-- .status/active/.first/50/
SELECT id FROM users WHERE status = 'active' ORDER BY id ASC LIMIT 50;

-- .status/active/.last/50/
SELECT id FROM users WHERE status = 'active' ORDER BY id DESC LIMIT 50;
```

**Rationale:**
- Enables ordered exploration of indexed data
- `.first/` shows oldest/smallest values first
- `.last/` shows newest/largest values first
- Useful for time-based indexes (see recent data)

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
  dir_listing_limit: 10000  # Default threshold
```

**Behavior:**
- Tables with ≤ `dir_listing_limit` rows: `ls` returns all PKs
- Tables with > `dir_listing_limit` rows: `ls` returns EIO
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
SELECT id FROM users ORDER BY id ASC LIMIT 50;
```

**Rationale:**
- Fast (indexed lookup by PK)
- Predictable (always same rows)
- Good for "show me some data"

#### `.last/N/` - Last N Rows by Primary Key

**Usage:**
```bash
ls /mnt/db/users/.last/50/
# 99951  99952  99953  ...  (last 50 PKs)
```

**SQL Generated:**
```sql
SELECT id FROM users ORDER BY id DESC LIMIT 50;
```

**Rationale:**
- Fast (indexed lookup by PK)
- Shows most recent data (for auto-increment PKs)
- Complement to `.first/N/`

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

#### `.info/count` - Total Row Count

**Usage:**
```bash
cat /mnt/db/users/.info/count
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

## Pipeline Query Architecture

TigerFS supports composable pipeline queries that chain multiple operations into a single path. The entire pipeline is pushed down to the database as one optimized SQL query, ensuring efficient index usage and minimal data transfer.

### Overview

Pipeline queries allow users to:
- Chain filters, ordering, and pagination in filesystem paths
- Combine indexed (`.by/`) and non-indexed (`.filter/`) filters
- Nest pagination operations (e.g., `.first/100/.last/50/`)
- Select specific columns with `.columns/` for efficient projection
- Export filtered results in multiple formats

**Example:**
```bash
# Find last 10 pending orders for customer 123, sorted by date
cat /mnt/db/orders/.by/customer_id/123/.by/status/pending/.order/created_at/.last/10/.export/json
```

**Generated SQL:**
```sql
SELECT * FROM orders
WHERE customer_id = '123' AND status = 'pending'
ORDER BY created_at DESC
LIMIT 10
```

### Available Capabilities

| Capability | Purpose | Performance |
|------------|---------|-------------|
| `.by/<col>/<val>/` | Filter by indexed column | Fast (index scan) |
| `.filter/<col>/<val>/` | Filter by any column | May scan table |
| `.order/<col>/` | Sort results | Uses index if available |
| `.columns/col1,col2/` | Select specific columns | Reduces data transfer |
| `.first/N/` | First N rows | Fast (LIMIT) |
| `.last/N/` | Last N rows | Fast (ORDER DESC + LIMIT) |
| `.sample/N/` | Random N rows | Full scan (ORDER BY RANDOM()) |
| `.export/<fmt>` | Export as csv/tsv/json | Terminal operation |

### Capability Chaining Rules

| Parent | Allowed Children |
|--------|------------------|
| Table root | `.by/`, `.filter/`, `.order/`, `.columns/`, `.first/`, `.last/`, `.sample/`, `.export/` |
| `.by/<col>/<val>/` | `.by/`, `.filter/`, `.order/`, `.columns/`, `.first/`, `.last/`, `.sample/`, `.export/` |
| `.filter/<col>/<val>/` | `.by/`, `.filter/`, `.order/`, `.columns/`, `.first/`, `.last/`, `.sample/`, `.export/` |
| `.order/<col>/` | `.columns/`, `.first/`, `.last/`, `.sample/`, `.export/` |
| `.columns/col,...` | `.export/` |
| `.first/N/` | `.by/`, `.filter/`, `.order/`, `.columns/`, `.last/`, `.sample/`, `.export/` |
| `.last/N/` | `.by/`, `.filter/`, `.order/`, `.columns/`, `.first/`, `.sample/`, `.export/` |
| `.sample/N/` | `.by/`, `.filter/`, `.order/`, `.columns/`, `.export/` |
| `.export/<fmt>` | None (terminal) |

**Disallowed (redundant):**
- `.first/N/.first/M/` - Use `.first/M/`
- `.last/N/.last/M/` - Use `.last/M/`
- `.sample/N/.sample/M/` - Use `.sample/M/`
- `.sample/N/.first/M/` or `.sample/N/.last/M/` - Use `.sample/M/`
- `.order/a/.order/b/` - Second order replaces first
- `.columns/a,b/.columns/c,d/` - Use `.columns/a,b,c,d/`

### `.by/` vs `.filter/`

| Aspect | `.by/` | `.filter/` |
|--------|--------|------------|
| Columns shown | Indexed only | All columns |
| Value listing | Always fast | May show `.table-too-large` |
| Query performance | Guaranteed fast | May scan table |
| Use case | Efficient lookups | Ad-hoc filtering |

### Filter Semantics

Multiple filters are AND-combined:
```bash
.by/status/active/.by/tier/premium/     # status='active' AND tier='premium'
.by/customer/123/.filter/notes/urgent/  # customer_id=123 AND notes='urgent'
```

### Nested Pagination Semantics

Each pagination step operates on the previous result:

| Path | Meaning | Result |
|------|---------|--------|
| `.first/100/.last/50/` | Last 50 of first 100 | Rows 51-100 |
| `.last/100/.first/50/` | First 50 of last 100 | Rows (n-99) to (n-50) |
| `.first/1000/.sample/50/` | Random 50 from first 1000 | 50 random rows |

**SQL Generation for Nested Pagination:**
```sql
-- .first/100/.last/50/
SELECT * FROM (
    SELECT * FROM t ORDER BY pk ASC LIMIT 100
) sub ORDER BY pk DESC LIMIT 50

-- .first/1000/.sample/50/
SELECT * FROM (
    SELECT * FROM t ORDER BY pk ASC LIMIT 1000
) sub ORDER BY RANDOM() LIMIT 50
```

### Column Projection

`.columns/` selects specific columns, generating `SELECT "col1", "col2"` instead of `SELECT *`:

```bash
cat /mnt/db/orders/.columns/id,status,total/.export/csv
cat /mnt/db/orders/.filter/status/shipped/.columns/id,total/.export/json
```

**Generated SQL:**
```sql
-- .columns/id,status,total/.export/csv
SELECT "id", "status", "total" FROM orders

-- .filter/status/shipped/.columns/id,total/.export/json
SELECT "id", "total" FROM orders WHERE status = 'shipped'
```

**Rules:**
- `ls .columns/` shows available column names
- Comma-separated column names: `.columns/col1,col2,col3/`
- After `.columns/`, only `.export/` is available
- No double `.columns/` (list all desired columns in one)
- Invalid column names return ENOENT

### Large Table Safety for `.filter/`

For non-indexed columns on large tables, value listing is disabled:

```bash
$ ls /mnt/db/large_events/.filter/type/
.table-too-large    # Indicates value listing unavailable
```

Direct access still works:
```bash
cat /mnt/db/large_events/.filter/type/click/.first/100/.export/json
```

**Configuration:**
- `DirFilterLimit`: Tables larger than this skip value listing (default: 100,000)
- `QueryTimeout`: Maximum query time before EIO (default: 30 seconds)

### Composite Index Support

Two syntaxes for multi-column indexes:
```bash
# Sequential filters (two separate lookups)
.by/last_name/Smith/.by/first_name/John/

# Composite syntax (single index lookup)
.by/last_name.first_name/Smith.John/
```

### Examples

```bash
# Customer's 10 most recent orders
cat /mnt/db/orders/.by/customer_id/123/.order/created_at/.last/10/.export/json

# Random sample of active users
cat /mnt/db/users/.by/status/active/.sample/100/.export/csv

# Paginated browse (page 2)
ls /mnt/db/events/.first/200/.last/100/

# Complex filter chain
cat /mnt/db/orders/.by/status/pending/.filter/priority/high/.order/amount/.last/20/.export/json
```

For complete documentation, see [docs/native-tables.md](../docs/native-tables.md).

---

## Synthesized Apps

### Overview

Synthesized apps present database rows as domain-specific files — markdown documents, plain text, or other formats — instead of the default row-as-directory structure. Each synthesized app is a PostgreSQL VIEW backed by a table, where TigerFS maps columns to file components (filename, body, frontmatter metadata).

This is the primary user interface for content-oriented workflows: blog posts, knowledge bases, meeting notes, and agent-generated documents are all managed as plain files while being stored transactionally in PostgreSQL.

For the complete user guide with examples, see [docs/markdown-app.md](../docs/markdown-app.md).

### Creation: `.build/` and `.format/`

There are two creation paths:

**`.build/<name>` — Create a new app from scratch:**

```bash
echo "markdown" > /mnt/db/.build/notes
```

Creates a backing table `_notes`, a view `notes`, and sets the view comment to `tigerfs:md`. The view gets the clean name; the table gets an underscore prefix.

**`<table>/.format/markdown` — Add a synthesized view to an existing table:**

```bash
echo "markdown" > /mnt/db/posts/.format/markdown
```

Creates a view `posts_md` over the existing `posts` table, with comment `tigerfs:md`. The view gets a `_md` suffix to avoid colliding with the existing table name.

| Method | View Name | Table Name | Example Path |
|--------|-----------|------------|--------------|
| `.build/notes` | `notes/` | `_notes/` | `/mnt/db/notes/hello.md` |
| `posts/.format/markdown` | `posts_md/` | `posts/` | `/mnt/db/posts_md/hello.md` |

### Markdown Format

The markdown format maps table columns to file components using naming conventions:

| Role | Detected From (priority order) | Required |
|------|-------------------------------|----------|
| Filename | `filename`, `name`, `title`, `slug` | Yes |
| Body | `body`, `content`, `description`, `text` | Yes |
| Timestamps | `modified_at`/`updated_at` (mtime); `created_at` (ctime) | No |
| Extra Headers | `headers` (JSONB, merged into frontmatter) | No |
| Frontmatter | All remaining columns (excluding primary key) | — |

Files are rendered with YAML frontmatter followed by the body:

```markdown
---
title: Hello World
author: alice
draft: false
tags:
  - intro
---

# Hello World

Content here...
```

Known columns appear first in schema order, then extra headers (from the `headers` JSONB column) appear alphabetically.

### Directory Hierarchies

Synthesized apps support subdirectories via the `filetype` column and slashes in the `filename` column:

- **`filetype`** — Either `'file'` or `'directory'` (default: `'file'`). When present in a table, TigerFS enables hierarchical directory support.
- **`filename`** — Encodes the full path using slashes (e.g., `tutorials/intro.md` for a file in the `tutorials/` subdirectory)

Operations:
- `mkdir` creates a row with `filetype='directory'`
- `rmdir` deletes the directory row (only if empty)
- Moving a file between directories updates the slash-separated prefix in `filename`
- Renaming a directory atomically updates the `filename` of all contained files
- Writing a file with a path auto-creates parent directories

### Backing Table Schema

The `.build/` command creates a table with this schema (for markdown apps):

```sql
CREATE TABLE "_notes" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);
```

A `BEFORE UPDATE` trigger automatically sets `modified_at = now()` on every update.

### View Architecture

Each synthesized app is exposed through a PostgreSQL VIEW (`CREATE VIEW "notes" AS SELECT * FROM "_notes"`). TigerFS identifies synthesized apps by parsing the view comment:

| Comment | Meaning |
|---------|---------|
| `tigerfs:md` | Markdown format |
| `tigerfs:txt` | Plain text format |
| `tigerfs:md,history` | Markdown with version history |
| `tigerfs:txt,history` | Plain text with version history |

The comment is set via `COMMENT ON VIEW "notes" IS 'tigerfs:md'`. TigerFS scans view comments on mount to discover all synthesized apps.

The native backing table remains accessible alongside the synthesized view — `ls /mnt/db/_notes/` shows the standard row-as-directory structure, while `ls /mnt/db/notes/` shows synthesized markdown files.

---

## History

### Overview

History provides automatic versioning for synthesized apps. A PostgreSQL `BEFORE UPDATE OR DELETE` trigger copies the old row into a companion history table on every change. Past versions appear as read-only files under `.history/` directories.

For the complete user guide, see [docs/history.md](../docs/history.md).

### Enabling History

History can be enabled at creation or added later:

```bash
# At creation
echo "markdown,history" > /mnt/db/.build/notes

# Add to existing app
echo "history" > /mnt/db/.build/notes
```

Both paths store the feature flag in the view comment (`tigerfs:md,history`).

### History Table Schema

Each history-enabled app gets a companion table named `_<name>_history`:

```sql
CREATE TABLE "_notes_history" (
    -- Mirrors all source table columns --
    id UUID,
    filename TEXT NOT NULL,
    filetype TEXT,
    title TEXT,
    author TEXT,
    headers JSONB,
    body TEXT,
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    -- History metadata --
    _history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    _operation TEXT NOT NULL  -- 'UPDATE' or 'DELETE'
);
```

Indexes are created on `(filename, _history_id DESC)` and `(id, _history_id DESC)` for efficient lookups by filename or row UUID.

### Trigger Mechanism

A `BEFORE UPDATE OR DELETE` trigger on the source table copies the `OLD` row into the history table with a UUIDv7 `_history_id` (encoding the current timestamp) and the operation type (`UPDATE` or `DELETE`).

### TimescaleDB Integration

The history table is converted to a TimescaleDB hypertable for efficient time-partitioned storage:

- **Chunk interval:** 1 month (partitioned by `_history_id`)
- **Compression:** `segment_by='filename'`, `order_by='_history_id DESC'`
- **Compression policy:** After 1 day

History requires **TimescaleDB** — it will not work on vanilla PostgreSQL.

### Filesystem Interface

The `.history/` directory appears inside each synthesized app:

| Path | Description |
|------|-------------|
| `app/.history/` | List files that have history entries |
| `app/.history/file.md/` | List versions of a file (newest first) |
| `app/.history/file.md/.id` | Read the row's stable UUID |
| `app/.history/file.md/<timestamp>` | Read a past version's full content |
| `app/.history/.by/` | List all row UUIDs with history |
| `app/.history/.by/<uuid>/` | List versions for a specific UUID |
| `app/.history/.by/<uuid>/<timestamp>` | Read a past version by UUID |
| `app/subdir/.history/` | Per-directory history (scoped to that directory) |

Version timestamps are extracted from the UUIDv7 `_history_id`, formatted as `2006-01-02T150405Z` (filesystem-safe, no colons). Versions are listed newest-first.

**Cross-rename tracking:** Every row has a stable UUID that persists across renames. The `.by/` directory enables lookups by UUID even after a file is renamed. `.by/` is only available at the root `.history/` level.

**Subdirectory history:** Each directory has its own `.history/` scoped to files in that directory. Subdirectory `.history/` does not include `.by/`.

### Detection

TigerFS detects history support via:
1. **View comment** — parsing `tigerfs:md,history` from `COMMENT ON VIEW`
2. **Companion table** — checking for a `_<name>_history` table if the comment doesn't specify history

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
  host: localhost              # Database hostname
  port: 5432                   # Database port
  user: myuser                 # Database username
  database: mydb               # Database name
  # NO password field (use .pgpass, env vars, or password_command)
  default_schema: public       # Schema to flatten to mount root
  pool_size: 10                # Max open connections
  pool_max_idle: 5             # Max idle connections
  password_command: "..."      # Command to retrieve password
  default_backend: tiger       # Default cloud backend for bare names (tiger or ghost)
  default_mount_dir: /tmp      # Base directory for auto-generated mountpoints

# Filesystem behavior
filesystem:
  dir_listing_limit: 10000           # Max rows returned by ls (prevents huge listings)
  trailing_newlines: true            # Add \n to column and .count file reads
  no_filename_extensions: false      # Disable type-based extensions (.txt, .json, etc.)
  attr_timeout: 1                    # FUSE attribute cache (seconds, FUSE backend only)
  entry_timeout: 1                   # FUSE entry cache (seconds, FUSE backend only)
  # Note: macOS NFS backend uses the system's NFS caching instead

# Metadata refresh
metadata:
  refresh_interval: 30         # Seconds between schema/column cache refreshes

# Logging
logging:
  level: info                  # debug, info, warn, error
  file: ~/.local/share/tigerfs/tigerfs.log
  format: json                 # json, text

# Formats
formats:
  default: tsv                 # Default row format: tsv, csv, json, yaml
  binary_encoding: raw         # BYTEA encoding: raw, hex, base64

# Debug
debug: false                   # Enable debug mode (verbose logging)
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
- `TIGERFS_DIR_LISTING_LIMIT` - Large table threshold
- `TIGERFS_TRAILING_NEWLINES` - Add trailing newlines to column/count reads (default: true)
- `TIGERFS_NO_FILENAME_EXTENSIONS` - Disable type-based file extensions (default: false)
- `TIGERFS_ATTR_TIMEOUT` - FUSE attribute cache timeout
- `TIGERFS_ENTRY_TIMEOUT` - FUSE entry cache timeout
- `TIGERFS_METADATA_REFRESH_INTERVAL` - Metadata refresh interval
- `TIGERFS_LOG_LEVEL` - Logging level
- `TIGERFS_LOG_FILE` - Log file path
- `TIGERFS_LOG_FORMAT` - Log format (json, text)
- `TIGERFS_DEFAULT_FORMAT` - Default row-as-file format
- `TIGERFS_BINARY_ENCODING` - BYTEA encoding
- `TIGERFS_POOL_SIZE` - Connection pool size
- `TIGERFS_POOL_MAX_IDLE` - Max idle connections in pool
- `TIGERFS_PASSWORD` - Database password (alternative to PGPASSWORD)
- `TIGERFS_PASSWORD_COMMAND` - Command to retrieve password (e.g., from secrets manager)
- `TIGERFS_DEBUG` - Enable debug mode
- `TIGERFS_DEFAULT_BACKEND` - Default cloud backend for bare names (`tiger` or `ghost`)
- `TIGERFS_DEFAULT_MOUNT_DIR` - Base directory for auto-generated mountpoints (default: `/tmp`)
- `TIGER_PUBLIC_KEY` - Tiger Cloud client credential public key (for headless auth)
- `TIGER_SECRET_KEY` - Tiger Cloud client credential secret key (for headless auth)
- `TIGER_PROJECT_ID` - Tiger Cloud project ID (for headless auth)

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
# Simple mount with connection string
tigerfs postgres://localhost/mydb /mnt/db

# Tiger Cloud / Ghost (prefix scheme)
tigerfs tiger:e6ue9697jf /mnt/db
tigerfs ghost:a2x6xoj0oz /mnt/db

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
--attr-timeout SECS       FUSE attribute cache timeout (FUSE backend only, default: 1)
--entry-timeout SECS      FUSE entry cache timeout (FUSE backend only, default: 1)
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
tigerfs [mount] [OPTIONS] [CONNECTION] MOUNTPOINT
```

**Description:** Mount a PostgreSQL database as a filesystem. CONNECTION can be a `postgres://` URL, a `tiger:<id>` or `ghost:<id>` prefix, or omitted (uses environment variables / config).

**Examples:**
```bash
tigerfs mount postgres://user@host/db /mnt/db
tigerfs mount tiger:e6ue9697jf /mnt/db
tigerfs mount ghost:a2x6xoj0oz /mnt/db
tigerfs mount /mnt/db   # uses PGHOST/PGDATABASE env vars
```

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
Backend: NFS
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
  dir_listing_limit: 50000       [config file]
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

#### 9. info

**Syntax:**
```bash
tigerfs info [MOUNTPOINT] [--json]
```

**Description:** Show information about a mounted TigerFS instance and its backing cloud service.

**Examples:**
```bash
# Info for the sole active mount (or prompts if multiple)
tigerfs info

# Info for a specific mountpoint
tigerfs info /mnt/db

# JSON output for scripting
tigerfs info --json /mnt/db
```

**Output:**
```
Mountpoint:   /mnt/db
Database:     postgres://host:5432/tsdb (password hidden)
Backend:      tiger
Service ID:   e6ue9697jf
Service Name: my-db
Region:       us-east-1
Status:       Running
Created:      2025-01-15T10:30:00Z
```

**Behavior:**
- With no arguments: uses the sole active mount, or errors if zero or multiple mounts
- Looks up mount in the mount registry to find service ID and backend
- Calls the cloud backend CLI to fetch live service info
- `--json` outputs a JSON object with all fields

---

#### 10. create

**Syntax:**
```bash
tigerfs create [BACKEND:]NAME [MOUNTPOINT] [--no-mount] [--json]
```

**Description:** Create a new database service on a cloud backend and optionally mount it.

**Examples:**
```bash
# Create on Tiger Cloud and auto-mount to /tmp/my-db
tigerfs create tiger:my-db

# Create with auto-generated name
tigerfs create tiger:

# Create and mount at a specific path
tigerfs create tiger:my-db /mnt/data

# Create without mounting
tigerfs create tiger:my-db --no-mount

# Use default_backend from config (bare name)
tigerfs create my-db
```

**Behavior:**
- Parses prefix to determine backend (`tiger:` or `ghost:`); falls back to `default_backend` config
- Calls the backend CLI to create the service (blocks until ready)
- Unless `--no-mount`, spawns a background `tigerfs mount` process
- Mountpoint defaults to `$TMPDIR/<name>` if not specified
- `--json` outputs the create result (service ID, name, connection string, mountpoint)

---

#### 11. fork

**Syntax:**
```bash
tigerfs fork SOURCE [DEST] [--name NAME] [--no-mount] [--json]
```

**Description:** Fork (clone) an existing database and optionally mount the fork.

SOURCE can be:
- A mountpoint path (`/mnt/db`) — looks up service ID from mount registry
- A prefixed ID (`tiger:e6ue9697jf`) — uses the specified backend
- A bare name — resolved via `default_backend` config

DEST can be:
- A bare name (`my-fork`) — used as service name, mounted at `$TMPDIR/my-fork`
- An absolute path (`/mnt/fork`) — mounted there, basename used as service name

**Examples:**
```bash
# Fork a mounted database
tigerfs fork /mnt/db my-experiment

# Fork by service ID
tigerfs fork tiger:e6ue9697jf my-experiment

# Fork to a specific mount path
tigerfs fork /mnt/db /mnt/experiment

# Fork with explicit name
tigerfs fork /mnt/db --name my-fork

# Fork without mounting
tigerfs fork /mnt/db --no-mount
```

**Behavior:**
- Resolves SOURCE to a backend and service ID
- Calls the backend CLI to fork the service (blocks until ready)
- Unless `--no-mount`, spawns a background `tigerfs mount` process for the fork
- `--json` outputs the fork result (source ID, fork ID, name, connection string, mountpoint)

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

**4. Cloud Backend Prefix (Tiger Cloud / Ghost):**
```bash
tigerfs tiger:e6ue9697jf /mnt/db
tigerfs ghost:a2x6xoj0oz /mnt/db
```

Connection credentials are retrieved automatically from the backend CLI. See [Cloud Backend Integration](#cloud-backend-integration).

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

## Cloud Backend Integration

### Overview

TigerFS integrates with cloud database backends (Tiger Cloud and Ghost) through their respective CLIs. Users specify a backend using a prefix scheme (`tiger:<id>`, `ghost:<id>`) on the connection argument. TigerFS delegates authentication, service management, and connection string retrieval to the backend CLI.

See [ADR-013](adr/013-backend-prefix-scheme.md) for the full design rationale.

### Design Philosophy

**Use Existing Tools:** Rather than reimplementing authentication, leverage the existing backend CLIs (`tiger`, `ghost`) for credential management. Users authenticate once (e.g., `tiger auth login`), and TigerFS retrieves connection details on demand.

**Prefix Scheme:** Backend selection is explicit via prefix on the connection reference:

| Prefix | Backend | Example |
|--------|---------|---------|
| `tiger:` | Tiger Cloud | `tiger:e6ue9697jf` |
| `ghost:` | Ghost | `ghost:a2x6xoj0oz` |
| `postgres://` | Direct PostgreSQL | `postgres://user@host/db` |
| (none) | Environment/config | (uses `PGHOST`, etc.) |

### Configuration

**Default Backend:** Set `default_backend` in config to avoid typing the prefix for every command:

```yaml
# ~/.config/tigerfs/config.yaml
default_backend: tiger   # or "ghost"
```

Or via environment: `TIGERFS_DEFAULT_BACKEND=tiger`

When set, bare names (without a prefix) are resolved using the default backend:
```bash
tigerfs create my-db          # equivalent to: tigerfs create tiger:my-db
tigerfs fork my-db my-fork    # equivalent to: tigerfs fork tiger:my-db my-fork
```

### Backend CLI Differences

| Behavior | Tiger CLI | Ghost CLI |
|----------|-----------|-----------|
| Connection string | `tiger db connection-string <id> --with-password` | `ghost connect <id>` |
| Password delivery | Via `--with-password` flag | Via `~/.pgpass` (automatic) |
| Create waits | By default (30m timeout, opt-out: `--no-wait`) | Requires explicit `--wait` |
| Fork waits | By default (30m timeout, opt-out: `--no-wait`) | Requires explicit `--wait` |
| Service info | `tiger service get <id> --json` | `ghost db get <id> --json` |

### Implementation Details

#### Connection String Retrieval

**Tiger Cloud:**
```go
cmd := exec.Command("tiger", "db", "connection-string", serviceID, "--with-password")
```

**Ghost:**
```go
cmd := exec.Command("ghost", "connect", serviceID)
```

Both return a `postgres://...` connection string on stdout.

#### Credential Security

- Password never stored in TigerFS config files
- Credentials retrieved on-demand from backend CLI
- Backend CLI handles secure storage (keyring, encrypted files, pgpass)
- Connection string used immediately, not persisted

#### Error Handling

TigerFS detects and maps common CLI errors:

| Error Pattern | Mapped Error | User Message |
|---------------|-------------|--------------|
| CLI not in PATH | `ErrCLINotFound` | Install instructions with download URL |
| "not authenticated", "unauthorized", "login" | `ErrNotAuthenticated` | Run `<cli> auth login` |
| "404", "not found" | Service not found | Check service ID |
| Other stderr | Generic CLI error | Shows stderr content |

### User Experience Flows

#### Flow 1: First-Time Tiger Cloud User

```bash
# 1. Install tiger CLI
curl -fsSL https://cli.tigerdata.com | sh

# 2. Authenticate
tiger auth login

# 3. Install TigerFS
go install github.com/timescale/tigerfs/cmd/tigerfs@latest

# 4. Create and mount a database
tigerfs create tiger:my-db

# 5. Use filesystem
ls /tmp/my-db/
cat /tmp/my-db/users/1.json
```

#### Flow 2: Multiple Services

```bash
# Mount multiple services
tigerfs mount tiger:e6ue9697jf /mnt/prod
tigerfs mount tiger:u8me885b93 /mnt/staging

# Access each independently
cat /mnt/prod/users/123/email
cat /mnt/staging/users/456/email

# Check info
tigerfs info /mnt/prod
```

#### Flow 3: Fork for Experimentation

```bash
# Fork a production database
tigerfs fork /mnt/prod my-experiment

# Experiment safely on the fork
echo '{"price": 0}' > /tmp/my-experiment/products/1.json

# Clean up
tigerfs unmount /tmp/my-experiment
```

#### Flow 4: Headless/Docker Authentication

```bash
# Set credentials (from Tiger Cloud Console)
export TIGER_PUBLIC_KEY=<your-public-key>
export TIGER_SECRET_KEY=<your-secret-key>
export TIGER_PROJECT_ID=<your-project-id>

# Authenticate using environment variables
tiger auth login

# Mount using tiger: prefix
tigerfs mount tiger:<service-id> /mnt/cloud
```

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
- Size = byte length of serialized format (TSV/CSV/JSON/YAML)
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

Table metadata is organized under the `.info/` directory:

#### `.info/schema` - CREATE TABLE Statement

**Usage:**
```bash
cat /mnt/db/users/.info/schema
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

---

#### `.info/columns` - Column List

**Usage:**
```bash
cat /mnt/db/users/.info/columns
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

#### `.indexes/` - Index Metadata and DDL Operations

The `.indexes/` directory provides index metadata and DDL operations.

**List indexes:**
```bash
ls /mnt/db/users/.indexes/
```

**Output:**
```
.create/           # Staging for new indexes
email_idx/         # Existing index
created_at_idx/    # Existing index
```

**View index DDL:**
```bash
cat /mnt/db/users/.indexes/email_idx/.schema
```

**Output:**
```sql
CREATE UNIQUE INDEX email_idx ON public.users USING btree (email)
```

**Create new index:**
```bash
mkdir /mnt/db/users/.indexes/.create/new_idx
cat /mnt/db/users/.indexes/.create/new_idx/sql        # See template
echo "CREATE INDEX new_idx ON users(column)" > /mnt/db/users/.indexes/.create/new_idx/sql
touch /mnt/db/users/.indexes/.create/new_idx/.commit  # Execute
```

**Delete index:**
```bash
cat /mnt/db/users/.indexes/email_idx/.delete/sql      # See DROP template
echo "DROP INDEX email_idx" > /mnt/db/users/.indexes/email_idx/.delete/sql
touch /mnt/db/users/.indexes/email_idx/.delete/.commit  # Execute
```

Primary key indexes are excluded from listing (accessed via row paths).

---

#### `.info/ddl` - Complete DDL

**Usage:**
```bash
cat /mnt/db/users/.info/ddl
```

**Output:**
```sql
-- Table
CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    name text,
    created_at timestamp with time zone DEFAULT now()
);

-- Primary Key
ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);

-- Indexes
CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email);
CREATE INDEX users_created_at_idx ON public.users USING btree (created_at);

-- Foreign Keys
ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);

-- Comments
COMMENT ON TABLE public.users IS 'User accounts';
COMMENT ON COLUMN public.users.email IS 'Primary contact email';
```

**Includes:**
- CREATE TABLE statement
- Primary key constraint
- All indexes
- Foreign key constraints (both directions)
- Check constraints
- Triggers
- Column and table comments

**Difference from `.info/schema`:**
- `.info/schema` - Just the CREATE TABLE statement (fast, minimal)
- `.info/ddl` - Complete DDL including indexes, constraints, triggers, comments

---

#### `.info/count` - Row Count

**Usage:**
```bash
cat /mnt/db/users/.info/count
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

### DDL Operations

TigerFS supports DDL (Data Definition Language) operations via a staging pattern that provides safety, validation, and consistent workflows for schema changes.

**Supported Operations:**
- **Tables:** Create, modify (ALTER), delete (DROP)
- **Indexes:** Create, delete
- **Schemas:** Create, delete
- **Views:** Create, delete

#### Staging Pattern Overview

All DDL operations follow the same workflow using staging directories with control files. This pattern provides:
- **Preview before commit** - See templates with context before writing DDL
- **Validation** - Test DDL via BEGIN/ROLLBACK before executing
- **Safety** - Explicit commit required; abort clears staging
- **Consistency** - Same workflow for all DDL types

**Workflow:**

| Step | Action | Effect |
|------|--------|--------|
| 1 | Read `sql` | See template with context (current schema, hints) |
| 2 | Write `sql` | Stage your DDL statement (stored in memory) |
| 3 | Touch `.test` | Validate via BEGIN/ROLLBACK (optional) |
| 3b | Read `test.log` | See validation result (optional) |
| 4 | Touch `.commit` | Execute DDL |
| — | Touch `.abort` | Cancel and clear staging |

#### Staging Directory Structure

**Table Operations:**

| Operation | Path | Purpose |
|-----------|------|---------|
| Create table | `/.create/<name>/` | Create new table |
| Modify table | `/<table>/.modify/` | ALTER existing table |
| Delete table | `/<table>/.delete/` | DROP table |

**Index Operations:**

| Operation | Path | Purpose |
|-----------|------|---------|
| Create index | `/<table>/.indexes/.create/<name>/` | Create new index |
| Delete index | `/<table>/.indexes/<name>/.delete/` | DROP index |

**Schema Operations:**

| Operation | Path | Purpose |
|-----------|------|---------|
| Create schema | `/.schemas/.create/<name>/` | Create new schema |
| Delete schema | `/.schemas/<name>/.delete/` | DROP schema |

**View Operations:**

| Operation | Path | Purpose |
|-----------|------|---------|
| Create view | `/.views/.create/<name>/` | Create new view |
| Delete view | `/.views/<name>/.delete/` | DROP view |

#### Control Files

Each staging directory contains these control files:

**`sql`** - DDL statement file
- **Read:** Returns template with context-aware hints
- **Write:** Stages the DDL statement (validated on commit)
- **Size:** Reflects staged content size (0 if empty)

**`.test`** - Validation trigger
- **Touch:** Executes `BEGIN; <staged sql>; ROLLBACK;`
- **Effect:** Validates syntax and constraints without changes
- **Result:** Written to `test.log`

**`test.log`** - Validation result
- **Read:** Returns validation output
- **Content:** "OK" on success, or error message with details
- **Empty:** If no test has been run

**`.commit`** - Execution trigger
- **Touch:** Executes the staged DDL
- **Effect:** Applies changes to database
- **On success:** Clears staging, refreshes metadata
- **On failure:** Returns error, staging preserved

**`.abort`** - Cancel trigger
- **Touch:** Clears all staged content
- **Effect:** Resets staging directory to initial state

#### Template Generation

Reading the `sql` file before writing shows a context-aware template:

**CREATE TABLE template:**
```sql
-- Create table: orders
-- Schema: public
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    -- Add your columns here
    created_at TIMESTAMP DEFAULT NOW()
);
```

**ALTER TABLE template (shows current schema):**
```sql
-- Modify table: users
-- Current schema:
--   id: integer (NOT NULL, PRIMARY KEY)
--   email: text (NOT NULL, UNIQUE)
--   name: text
--   created_at: timestamp with time zone

-- Examples:
-- ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active';
-- ALTER TABLE users DROP COLUMN old_field;
-- ALTER TABLE users ALTER COLUMN name SET NOT NULL;

ALTER TABLE users
    -- Add your modifications here
;
```

**DROP TABLE template (shows impact):**
```sql
-- Drop table: orders
-- Row count: 1,247
-- Foreign keys referencing this table:
--   order_items.order_id -> orders.id
--
-- WARNING: This will permanently delete all data.
-- Use CASCADE to also drop dependent objects.

DROP TABLE orders;
-- Or: DROP TABLE orders CASCADE;
```

#### Error Handling

**Validation Errors (via `.test`):**
```bash
touch /mnt/db/.create/orders/.test
cat /mnt/db/.create/orders/test.log
# ERROR: syntax error at or near "CREAT"
# LINE 1: CREAT TABLE orders ...
#         ^
```

**Commit Failures:**
```bash
touch /mnt/db/.create/orders/.commit
# Returns EEXIST if table already exists
# Returns EIO with logged error details
```

**Recovery:**
```bash
# After failed commit, staging is preserved
cat /mnt/db/.create/orders/sql    # See what was staged
vi /mnt/db/.create/orders/sql     # Fix the issue
touch /mnt/db/.create/orders/.test # Re-validate
touch /mnt/db/.create/orders/.commit # Retry

# Or abort and start over
touch /mnt/db/.create/orders/.abort
```

#### Examples

**Create Table (Interactive):**
```bash
# Create staging directory
mkdir /mnt/db/.create/orders

# View template
cat /mnt/db/.create/orders/sql

# Write DDL
cat > /mnt/db/.create/orders/sql << 'EOF'
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);
EOF

# Validate (optional)
touch /mnt/db/.create/orders/.test
cat /mnt/db/.create/orders/test.log
# OK

# Execute
touch /mnt/db/.create/orders/.commit

# Verify
ls /mnt/db/orders/
```

**Create Table (Script):**
```bash
mkdir /mnt/db/.create/orders && \
echo "CREATE TABLE orders (id SERIAL PRIMARY KEY, name TEXT)" > /mnt/db/.create/orders/sql && \
touch /mnt/db/.create/orders/.commit
```

**Modify Table:**
```bash
# View current schema in template
cat /mnt/db/users/.modify/sql

# Add a column
echo "ALTER TABLE users ADD COLUMN last_login TIMESTAMP" > /mnt/db/users/.modify/sql

# Validate
touch /mnt/db/users/.modify/.test
cat /mnt/db/users/.modify/test.log

# Execute
touch /mnt/db/users/.modify/.commit
```

**Delete Table:**
```bash
# Review what will be deleted
cat /mnt/db/temp_table/.delete/sql

# Write DROP statement
echo "DROP TABLE temp_table" > /mnt/db/temp_table/.delete/sql

# Execute
touch /mnt/db/temp_table/.delete/.commit
```

**Create Index:**
```bash
mkdir /mnt/db/users/.indexes/.create/email_idx && \
echo "CREATE INDEX email_idx ON users(email)" > /mnt/db/users/.indexes/.create/email_idx/sql && \
touch /mnt/db/users/.indexes/.create/email_idx/.commit
```

**Create View:**
```bash
mkdir /mnt/db/.views/.create/active_users && \
echo "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true" > /mnt/db/.views/.create/active_users/sql && \
touch /mnt/db/.views/.create/active_users/.commit
```

#### Best Practices

1. **Always validate first** - Use `.test` before `.commit` for complex DDL
2. **Review templates** - Read `sql` before writing to understand context
3. **Use transactions wisely** - Complex multi-statement DDL should be done via psql
4. **Check test.log** - Validation errors include line numbers and hints
5. **Abort on uncertainty** - Use `.abort` if unsure; staging is cleared safely

#### Limitations

- **Single statement per commit** - Multi-statement DDL requires multiple operations or psql
- **No transaction grouping** - Each `.commit` is a separate transaction
- **Read-only templates** - Cannot customize template generation

For complex schema migrations, consider using migration tools (Flyway, Liquibase) or direct psql access via `tiger db connect`

---

## NFS Write Limitations

### O(n²) Write Amplification (go-nfs)

The go-nfs library fabricates an `Open → Write → Close` cycle per NFS WRITE RPC. NFS v3 is stateless and has no open/close — go-nfs synthesizes this because its `billy.Filesystem` abstraction requires it. Each `Close()` commits the full accumulated buffer to PostgreSQL.

For multi-chunk writes, this produces **O(n²) total DB write volume**: the first chunk writes 128KB, the second writes 256KB, the third writes 384KB, etc. For a 1MB file with 128KB wsize, that's 8 commits totaling ~4.5MB of actual DB writes.

**Mitigations in place:**

1. **wsize=128KB** (macOS mount option): 4x fewer RPCs than macOS default (32KB-64KB). Larger values (e.g., 1MB) can trigger GC deadlocks in test environments where the NFS server runs in the same process as the client. 128KB is a safe balance.
2. **Schema caching**: Schema resolution is cached with `sync.Once`, eliminating ~4-6 DB queries per RPC.
3. **Stat cache**: Dirty cached files return size from memory, avoiding a DB read after each write.
4. **Per-RPC overhead**: Reduced from ~7-9 DB queries to ~1 DB write per WRITE RPC.

**Behavior:**

- Data reaches PostgreSQL immediately on every WRITE RPC — no deferred commits, no durability gap.
- Cache entries persist after Close() for Stat to use; the reaper evicts them after idle timeout.
- go-nfs FSINFO advertises `Wtpref=1GB`; actual wsize depends on the NFS client's own default (macOS ~32KB without override, Linux ~1MB).
- Files under 128KB complete in a single WRITE RPC, eliminating O(n²) entirely.

**Long-term fix:** Patch go-nfs to return `UNSTABLE` from WRITE RPCs and flush on COMMIT (the NFS v3 mechanism for signaling "I'm done writing"). This would give O(n) writes with correct durability semantics. See Task 10.4 in `docs/implementation/implementation-tasks.md`.

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
- Format conversion (TSV/CSV/JSON/YAML)
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
9. Direct `ls` on large table (should error with dir_listing_limit)
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
curl -fsSL https://install.tigerfs.io | sh
```

**Windows (PowerShell):**
```powershell
irm https://install.tigerfs.io/install.ps1 | iex
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
- Domain: `https://install.tigerfs.io`
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
  curl -fsSL https://install.tigerfs.io | sh

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
  - Row-as-file (TSV, CSV, JSON, YAML)
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
- **Prerequisites** - Platform-specific requirements
- **Platform Instructions:**
  - **macOS** - No dependencies required (NFS backend)
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

### Phase 2: Feature Documentation (Shipped)

- [docs/markdown-app.md](../docs/markdown-app.md) — Markdown synthesized app guide (creation, usage, column mapping)
- [docs/history.md](../docs/history.md) — Version history feature (browsing, recovery, UUID tracking)
- [docs/native-tables.md](../docs/native-tables.md) — Native table access reference (pipeline queries, DDL)
- [docs/quickstart.md](../docs/quickstart.md) — Guided scenarios with sample data

### Phase 3: Advanced Documentation

Deferred:
- docs/user-guide.md (comprehensive reference)
- docs/configuration.md (detailed config options)
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

See `docs/implementation-tasks.md` for detailed step-by-step tasks.

### Phase 1: Core Foundation

- Initialize Go project structure
- Set up FUSE library (hanwen/go-fuse)
- Basic mount/unmount functionality
- CLI framework (Cobra) with version command
- Configuration system (Viper) with basic config loading
- Logging infrastructure (Zap)
- PostgreSQL connection with pgx and connection pooling
- Schema/table discovery
- Row-as-file read (TSV, CSV, JSON, YAML formats)
- Directory listing (small tables only)
- Unit test framework and testcontainers-go setup

### Phase 2: Full CRUD

- Row-as-directory read (column files)
- NULL handling
- Metadata files (.schema, .columns, .count)
- Row-as-file write (INSERT/UPDATE)
- Row-as-directory write (column-level)
- Constraint enforcement
- Row deletion (rm files/directories)
- Column deletion (set to NULL)
- Complete CRUD test coverage

### Phase 3: CLI Commands

- Unmount command
- Status command
- List mounts command
- Test connection command
- Config subcommands (show, validate, path)
- Cloud backend integration (tiger:/ghost: prefix scheme, create, fork, info commands)

### Phase 4: Advanced Features

- Index discovery from pg_indexes
- Single-column and composite index paths
- Index-based queries
- Large table handling (dir_listing_limit enforcement)
- `.first/N/` and `.last/N/` pagination
- `.sample/N/` random sampling (TABLESAMPLE)
- Permission mapping (table grants → file permissions)
- Timestamps (updated_at detection)
- File sizes
- Schema flattening
- Support for non-SERIAL primary keys (UUID, text)
- Support for tables without primary keys (read-only via ctid)
- TimescaleDB hypertable support

### Phase 5: Distribution & Release

- Install scripts (install.sh, install.ps1)
- GoReleaser setup
- GitHub Actions release workflow
- Documentation (README, getting-started, installation guides)
- Performance testing
- Bug fixes and polish
- v1.0.0 release

### Phase 6: Synthesized Apps

- Synthesized app framework (`.build/`, `.format/`)
- Markdown format with column auto-detection
- Directory hierarchies with subdirectories
- Automatic version history with TimescaleDB

### Post-MVP Enhancements

**Prioritize Based on Feedback:**
- Performance monitoring (`.stats`)
- Additional distribution methods (Homebrew, apt, yum)
- Windows support polish
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

**Column projection:**
```sql
SELECT "id", "email", "name" FROM users WHERE status = 'active';
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
| 1.2     | 2026-02-25 | Added Synthesized Apps and History sections; updated filesystem structure with new dotfiles; updated implementation priorities and documentation plan |
| 1.3     | 2026-02-27 | Added `.columns/` pipeline capability for column projection |

---

**End of Specification**

This document provides comprehensive guidance for implementing TigerFS. All major design decisions have been made with clear rationales. Implementation can proceed with confidence that the architecture is sound and well-considered.
