# Database Filesystem Interface - Detailed Specification

**Date:** 2026-01-23
**Status:** Design Complete - Ready for Implementation

## Executive Summary

A FUSE-based filesystem that exposes PostgreSQL database contents as mountable directories. Tables, rows, and columns are represented as directories and files, allowing standard Unix tools (`ls`, `cat`, `grep`, `cp`, `rm`) to interact with database data. Designed primarily for Claude Code and developer tools to explore and manipulate database-backed data using familiar filesystem operations.

## Technology Stack

### Implementation Language: Go

**Rationale:**
- Excellent PostgreSQL support via `pgx` library
- Mature FUSE libraries (`bazil.org/fuse` or `jacobsa/fuse`)
- Single binary distribution (easy deployment)
- Built-in concurrency model ideal for filesystem operations
- Trivial cross-platform compilation (Linux, macOS, Windows)

### Filesystem Layer: FUSE

**Platform Support:**
- **Linux:** Native FUSE support (no root needed, user in `fuse` group)
- **macOS:** macFUSE (requires admin to install, then regular users can mount)
- **Windows:** WinFsp (requires admin to install, then regular users can mount)
- **Docker/Containers:** Works with `--device /dev/fuse --cap-add SYS_ADMIN`
- **VMs/Firecracker:** Full support (real kernel)
- **GitHub Actions:** Linux runners support FUSE

**Out of Scope (Initially):**
- Managed container platforms (AWS Fargate, Cloud Run) that restrict FUSE
- Alternative interfaces for FUSE-restricted environments (may add later)

### Database: PostgreSQL

**Why PostgreSQL:**
- Rich schema introspection capabilities
- Comprehensive index metadata
- Full SQL feature set
- Standard type system with special types (JSONB, arrays, etc.)

## Filesystem Structure

### Complete Hierarchy

```
/mount/
├── .information_schema/         # PostgreSQL metadata views
│   ├── tables/
│   ├── columns/
│   └── constraints/
├── .refresh                      # Write to this file to force cache clear
├── schema_name/                  # One directory per schema
│   ├── .tables/                 # (Optional: explicit table access)
│   ├── .views/                  # (Optional: explicit view access)
│   ├── table_name/
│   │   ├── .schema              # CREATE TABLE statement
│   │   ├── .columns             # List of column names
│   │   ├── .indexes             # List of indexes with definitions
│   │   ├── .sample/             # Random samples from table
│   │   │   └── 100/             # 100 random rows (TABLESAMPLE)
│   │   │       ├── 47392/       # Actual PKs
│   │   │       └── 103847/
│   │   ├── .first/              # First N rows by PK
│   │   │   └── 50/              # First 50 rows
│   │   │       ├── 1/           # Actual PKs
│   │   │       └── 2/
│   │   ├── .count               # File containing total row count
│   │   ├── .email/              # Single-column index (dotfile)
│   │   │   └── foo@example.com/ # Indexed lookup
│   │   ├── .last_name.first_name/  # Composite index
│   │   │   └── Smith/
│   │   │       └── Johnson/
│   │   ├── 123                  # Row-as-file (default: TSV)
│   │   ├── 123.json             # Row-as-file (JSON format)
│   │   ├── 123.csv              # Row-as-file (CSV format)
│   │   ├── 123.tsv              # Row-as-file (TSV format, explicit)
│   │   └── 123/                 # Row-as-directory
│   │       ├── email            # Individual column file
│   │       ├── name
│   │       └── age
│   └── view_name/               # Views appear like tables
└── .schemas/                     # Explicit schema access
    ├── public/                   # All schemas available here
    │   └── users/
    └── analytics/
        └── reports/
```

### Default Schema Handling

**Problem:** Typing `/mount/public/users/` is tedious when `public` is the default schema.

**Solution:** Flatten default schema to root level.

**Behavior:**
- Default schema (typically `public`) tables appear directly under `/mount/`
- Other schemas accessible as subdirectories
- All schemas explicitly accessible via `/mount/.schemas/schema_name/`

**Example:**
```bash
# Default schema (public) - direct access
ls /mount/users/
cat /mount/users/123/email

# Other schemas - schema name required
ls /mount/analytics/reports/

# Explicit schema access (including default)
ls /mount/.schemas/public/users/
ls /mount/.schemas/analytics/reports/
```

**Configuration:** Mount option can specify which schema is default (defaults to `public`).

## Data Representation

### Dual Representation Model

Every row accessible in two ways simultaneously:

#### 1. Row-as-File (Full Row Operations)

Single file containing complete row data. Format determined by file extension.

**TSV Format (Default, Headerless):**
```bash
cat /schema/users/123
# foo@example.com	Foo Bar	25
cat /schema/users/123.tsv  # Explicit extension
# foo@example.com	Foo Bar	25
```

**CSV Format (Headerless):**
```bash
cat /schema/users/123.csv
# foo@example.com,Foo Bar,25
```

**JSON Format:**
```bash
cat /schema/users/123.json
# {"id":123,"email":"foo@example.com","name":"Foo Bar","age":25}
```

**Format Selection:**
- File extension determines format (`.tsv`, `.csv`, `.json`)
- No extension = TSV (default)
- All formats are headerless to avoid "header as data" bugs

#### 2. Row-as-Directory (Column-Level Operations)

Row represented as directory with individual column files:

```bash
ls /schema/users/123/
# email  name  age

cat /schema/users/123/email
# foo@example.com

cat /schema/users/123/age
# 25
```

**Benefits of Dual Model:**
- Full-row operations use row-as-file (efficient bulk operations)
- Single-column operations use row-as-directory (granular access)
- Reading can use either approach (user preference)
- Writing can use either approach (appropriate for use case)

### NULL Handling

**Consistent across all formats:**

**TSV/CSV:**
```
foo@example.com		25    # name is NULL (empty field)
```
Empty fields between delimiters represent NULL.

**JSON:**
```json
{"email":"foo@example.com","age":25}              # name omitted = NULL
{"email":"foo@example.com","name":null,"age":25}  # explicit null
```
Either omit key or use explicit `null`.

**Column files (row-as-directory):**
```bash
cat /schema/users/123/middle_name
# (empty file, 0 bytes = NULL)

[ -s /schema/users/123/middle_name ] && echo "has value" || echo "is NULL"
# is NULL
```
Empty file (0 bytes) represents NULL. Detectable with standard shell tests.

### Special PostgreSQL Types

**Simple Types (INTEGER, TEXT, BOOLEAN, etc.):**
```bash
cat /schema/users/123/age
# 25

cat /schema/users/123/active
# true
```
Straightforward text representation.

**JSONB/JSON:**
```bash
cat /schema/users/123/metadata
# {"preferences":{"theme":"dark","lang":"en"},"score":850}
```
Compact JSON (single line). Parseable with `jq`.

**Arrays:**
```bash
cat /schema/users/123/tags
# ["developer","golang","postgresql"]
```
JSON array format. Consistent with JSONB representation.

**BYTEA (Binary Data):**
```bash
cat /schema/users/123/avatar
# (raw binary bytes - PNG/JPEG/PDF data)

file /schema/users/123/avatar
# /schema/users/123/avatar: PNG image data, 512 x 512, 8-bit/color RGBA

# Encode on demand if needed
cat /schema/users/123/avatar | base64
cat /schema/users/123/avatar | xxd
```

**Rationale for raw binary:**
- Natural for binary files (images, PDFs, etc.)
- Works directly with tools (`open`, `cp`, image viewers)
- Claude Code can read images natively
- Users can pipe to encoding tools if needed (`base64`, `xxd`)
- Unix philosophy: provide raw data, let users transform

## Read Operations

### Full Row Reads

```bash
# Default format (TSV)
cat /schema/users/123
# foo@example.com	Foo Bar	25

# Explicit format
cat /schema/users/123.json
# {"id":123,"email":"foo@example.com","name":"Foo Bar","age":25}

cat /schema/users/123.csv
# foo@example.com,Foo Bar,25
```

**SQL Generated:**
```sql
SELECT * FROM schema.users WHERE id = 123;
```

### Column Reads

```bash
cat /schema/users/123/email
# foo@example.com
```

**SQL Generated:**
```sql
SELECT email FROM schema.users WHERE id = 123;
```

### Indexed Lookups

```bash
# Single-column index
cat /schema/users/.email/foo@example.com/name
# Foo Bar

# Composite index (full)
cat /schema/users/.last_name.first_name/Smith/Johnson/email
# smith.johnson@example.com

# Composite index (prefix)
ls /schema/users/.last_name.first_name/Smith/
# Johnson  Jane  ...
```

**SQL Generated:**
```sql
-- Single-column
SELECT name FROM schema.users WHERE email = 'foo@example.com';

-- Composite (full)
SELECT email FROM schema.users WHERE last_name = 'Smith' AND first_name = 'Johnson';

-- Composite (prefix)
SELECT DISTINCT first_name FROM schema.users WHERE last_name = 'Smith';
```

**Index Path Semantics:**
- Filesystem generates WHERE clauses from paths
- PostgreSQL query planner selects optimal index
- Multiple paths may resolve to same data (planner decides)
- Exposed indexes provide "this query will be fast" signal to users

### Directory Listings

```bash
# List all rows (by PK)
ls /schema/users/
# 1  2  3  5  7  9  ...

# With index paths visible
ls -a /schema/users/
# .  ..  .email  .last_name.first_name  .sample  .first  .count  1  2  3  ...
```

**Large Table Behavior:**
- Tables > 10,000 rows (configurable): `ls` returns EIO with helpful log message
- Log message: "Table too large (10M rows). Use .first/, .sample/, or index paths"
- Use special directories for exploration:
  - `.first/N/` - First N rows by PK
  - `.sample/N/` - Random N rows (TABLESAMPLE BERNOULLI)
  - `.count` - Total row count
  - Index paths - Indexed lookups

**Rationale:**
- Forces efficient access patterns for large tables
- Guides users toward index-based navigation
- `.sample/` provides quick data structure exploration
- Threshold configurable per mount

## Write Operations

### Write Model (All Formats)

**Consistent semantics across TSV/CSV/JSON:**
- TSV/CSV must follow schema column order
- Empty fields (TSV/CSV) or omitted keys (JSON) = NULL
- Columns not provided use NULL or column defaults
- Constraint violations return filesystem errors

### Full Row Writes (INSERT or UPDATE)

```bash
# TSV (follows column order: email, name, age)
echo -e "foo@example.com\tFoo Bar\t25" > /schema/users/123

# With NULLs (empty fields)
echo -e "foo@example.com\t\t25" > /schema/users/123
# email='foo@example.com', name=NULL, age=25

# JSON
echo '{"email":"foo@example.com","name":"Foo Bar","age":25}' > /schema/users/123.json

# JSON with omitted fields (NULL)
echo '{"email":"foo@example.com","age":25}' > /schema/users/123.json
# name=NULL
```

**SQL Generated (if row exists):**
```sql
UPDATE schema.users
SET email='foo@example.com', name='Foo Bar', age=25
WHERE id=123;
```

**SQL Generated (if row doesn't exist):**
```sql
INSERT INTO schema.users (id, email, name, age)
VALUES (123, 'foo@example.com', 'Foo Bar', 25);
```

### Column Writes (UPDATE or INSERT)

**UPDATE existing row:**
```bash
echo 'newemail@example.com' > /schema/users/123/email
```

**SQL Generated:**
```sql
UPDATE schema.users SET email='newemail@example.com' WHERE id=123;
```

**INSERT new row (directory creation):**
```bash
mkdir /schema/users/124
echo 'bar@example.com' > /schema/users/124/email
echo 'Bar User' > /schema/users/124/name
# Other columns use defaults or NULL
```

**SQL Generated:**
```sql
INSERT INTO schema.users (id, email, name)
VALUES (124, 'bar@example.com', 'Bar User');
```

### Constraint Enforcement

**NOT NULL constraints:**
```bash
# Schema: email NOT NULL, name nullable, age default 0, created_at default NOW()

mkdir /schema/users/125
echo 'foo@example.com' > /schema/users/125/email
# ✓ Success: email provided, name=NULL, age=0, created_at=NOW()

mkdir /schema/users/126
echo 'Some Name' > /schema/users/126/name
# ✗ FAIL: Returns EACCES (Permission denied)
# Log: "NOT NULL constraint violation: column 'email' cannot be NULL"
```

**Foreign key constraints:**
```bash
echo '999' > /schema/orders/1/user_id
# ✗ FAIL: Returns EACCES if user_id=999 doesn't exist in users table
# Log: "Foreign key constraint violation: user 999 does not exist"
```

**Rationale:**
- Filesystem operations respect database constraints
- Errors return standard errno (EACCES for constraint violations)
- Detailed errors written to logs
- Keeps database integrity intact

## DELETE Operations

### Deleting Column Files (Sets to NULL)

```bash
rm /schema/users/123/email
```

**SQL Generated:**
```sql
UPDATE schema.users SET email = NULL WHERE id = 123;
```

**Rationale:**
- Columns must always have a value or NULL (can't "not exist")
- `rm` = clear the value = set to NULL
- Fails if column is NOT NULL without default

### Deleting Rows

**Row-as-directory:**
```bash
rm -r /schema/users/123/
```

**Row-as-file:**
```bash
rm /schema/users/123.json
rm /schema/users/123
```

**SQL Generated:**
```sql
DELETE FROM schema.users WHERE id = 123;
```

**Permission Required:**
- DELETE privilege on table (checked via `has_table_privilege()`)
- Fails with EACCES if user lacks permission

**Cascading Deletes:**
- Respects database CASCADE/RESTRICT constraints
- If foreign key prevents deletion, returns EACCES with helpful log

## Index-Based Navigation

### Philosophy

Index paths are **syntactic sugar for WHERE clauses**:
- Filesystem structure provides discovery ("what queries are fast?")
- PostgreSQL query planner selects optimal index
- Multiple paths may lead to same data (planner decides execution)

### Index Path Generation

**Only indexed columns exposed:**
- Query `pg_indexes` to discover indexes
- Create dot-prefixed directory for each index
- Single-column index: `.column_name/`
- Composite index: `.column1.column2.column3/`

**Example:**
```sql
-- Database has these indexes:
CREATE INDEX users_email_idx ON users(email);
CREATE INDEX users_name_idx ON users(last_name, first_name);
```

**Filesystem exposes:**
```
/schema/users/
├── .email/              # Single-column index
└── .last_name.first_name/  # Composite index
```

### Composite Index Prefix Matching

**Composite index allows prefix queries:**
```bash
# Full composite lookup
ls /schema/users/.last_name.first_name/Smith/Johnson/

# Prefix lookup (just last_name)
ls /schema/users/.last_name.first_name/Smith/
```

Both generate WHERE clauses; PostgreSQL uses the composite index efficiently.

### Automatic Prefix Path Exposure

**If composite index `(last_name, first_name)` exists but NO single-column index on `last_name`:**

Expose both:
- `.last_name/Smith/` (shortcut, uses composite index prefix)
- `.last_name.first_name/Smith/` (explicit composite path)

Both paths work; query planner chooses index usage.

**Rationale:**
- Intuitive navigation (use what you know)
- Let database handle optimization (what it's good at)
- Filesystem signals "this is fast" via presence of index paths

### Non-Indexed Search

**Option 1: Glob patterns with column access**
```bash
grep "25" /schema/users/*/age
```

**Optimization opportunity:**
- Detect pattern: `grep <pattern> /schema/table/*/column`
- Generate: `SELECT id FROM table WHERE column::text LIKE '%pattern%'` or `WHERE column = 'pattern'`
- Falls back to full scan if pattern not optimizable

**Option 2: Full table listing + client-side filtering**
```bash
ls /schema/users/ | while read id; do
  cat /schema/users/$id/email
done | grep foo@example.com
```

Straightforward but slow for large tables.

**Recommendation:**
- Support both approaches
- Optimize common patterns when detectable
- Large tables require index paths or `.sample/` for exploration

## Performance and Caching

### Caching Strategy: No Explicit Caching (Initially)

**Decision Rationale:**
- Start simple, measure first, optimize later (YAGNI)
- PostgreSQL queries are fast, especially indexed lookups
- Caching adds complexity (invalidation, staleness)
- Usage patterns with Claude Code likely bursty, not sustained

**Implementation:**
- No in-memory cache in FUSE daemon
- Every filesystem operation = fresh PostgreSQL query
- Always consistent (no stale reads from cache)

**FUSE Kernel Caching:**
- Set short timeouts: `attr_timeout=1s`, `entry_timeout=1s`
- Allows kernel to cache for ~1 second (reduces syscalls)
- Still reasonably fresh (1-2 second staleness acceptable)

**Future Optimization:**
- Add caching if bottlenecks emerge
- Likely candidates: directory listings, table metadata
- Can add TTL-based or NOTIFY-based invalidation later

### Multi-User Consistency

**Challenge:**
Other users/processes may modify database while filesystem is mounted.

**Scenarios:**

**1. Data changes (INSERT/UPDATE/DELETE rows):**
- Fresh reads always see latest data (no application cache)
- Directory listings may be stale for 1-2 seconds (FUSE kernel cache)
- Direct file access bypasses directory cache (always works)

**Example:**
```bash
ls /schema/users/     # Shows: 1, 2, 3 (cached for 1-2 sec)
# Another user inserts row 4
ls /schema/users/     # Still might show: 1, 2, 3 (cache not expired)
cat /schema/users/4/email  # This WORKS (direct access, fresh query)
```

**2. Schema changes (CREATE/DROP/ALTER tables, indexes):**
- Table list, column metadata cached until refresh
- Refresh triggers:
  - Periodic background refresh (every 30 seconds)
  - Manual refresh via `.refresh` file

**Manual cache refresh:**
```bash
echo 1 > /mount/.refresh
# Clears all cached metadata (table lists, columns, indexes, permissions)
```

**Recommendation:**
- Document 1-2 second staleness for directory listings
- Manual refresh available when immediate consistency needed
- For most use cases, eventual consistency is acceptable

## Database Connection and Authentication

### Connection Configuration

**Supported methods:**

**1. Standard PostgreSQL Environment Variables (Primary):**
```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=myuser
export PGDATABASE=mydb
export PGPASSWORD='secret'  # or use .pgpass

dbfs /mnt/dbfs
```

**2. Connection String (Explicit Override):**
```bash
# Without password (recommended - prompts or uses PGPASSWORD/.pgpass)
dbfs postgres://user@host:port/dbname /mnt/dbfs

# With password (NOT recommended - visible in process list)
dbfs postgres://user:password@host:port/dbname /mnt/dbfs
```

### Password Handling

**Resolution order:**
1. Password in connection string (if present) ⚠️ Not recommended
2. `PGPASSWORD` environment variable
3. `~/.pgpass` file (PostgreSQL standard)
4. Interactive prompt

**PostgreSQL .pgpass file:**
```bash
# ~/.pgpass (mode 0600)
hostname:port:database:username:password
localhost:5432:mydb:myuser:secret123
prod.example.com:5432:*:readonly:prodpass123
```

**Security Warning:**
Documentation will prominently warn against passwords in connection strings or command-line arguments (visible in process list, shell history).

**Recommendation:**
- Use `~/.pgpass` for automated scripts (most secure)
- Use `PGPASSWORD` for temporary/interactive sessions
- Interactive prompt for one-off connections

### Multi-Database Support

**Decision: One mount = One database**

**Rationale:**
- Simple, predictable
- Clear permission model (credentials per database)
- Matches typical usage (connect to specific database)
- Can mount multiple times for multiple databases

**Example:**
```bash
dbfs postgres://host/db1 /mnt/db1
dbfs postgres://host/db2 /mnt/db2

ls /mnt/db1/public/users/
ls /mnt/db2/public/orders/
```

**Future consideration:**
Could add database-level hierarchy (`/mount/db1/schema/table/`) if demand exists.

## File Metadata and Permissions

### Permissions (Mode Bits)

**Map PostgreSQL table grants to filesystem permissions:**

**Permission Check Strategy:**
- Lazy + cached: Query permissions first time table is accessed
- Cache for lifetime of mount (in-memory)
- Query: `SELECT has_table_privilege('schema.table', 'SELECT|INSERT|UPDATE|DELETE')`

**Mapping:**
- SELECT privilege → read permission (r--)
- UPDATE privilege → write permission on existing rows (-w-)
- INSERT privilege → write permission on new rows (-w-)
- DELETE privilege → can rm files/directories

**Example:**
```bash
# User has SELECT + UPDATE on users table
-rw-r--r-- 1 user user  25 Jan 23 10:00 /schema/users/123/email

# User has only SELECT on orders table
-r--r--r-- 1 user user 100 Jan 23 10:00 /schema/orders/456/total
```

**Rationale:**
- Honest representation (see capabilities before trying)
- Standard Unix behavior (`ls -l` shows what you can do)
- Better UX for Claude Code (won't attempt impossible writes)
- Minimal overhead (one query per table, cached)

### Timestamps

**mtime (modification time):**
- Check for `updated_at` or `modified_at` column (common convention)
- Use column value if exists
- Fallback to current time if column doesn't exist

**ctime (change time) and atime (access time):**
- Use same as mtime or current time
- Not critical for this use case

**Rationale:**
- Meaningful timestamps when available
- Graceful degradation for tables without timestamp columns
- No extra database queries for timestamp-only checks

### Ownership

**All files owned by user running FUSE daemon:**
```bash
-rw-r--r-- 1 mfreed staff  25 Jan 23 10:00 /schema/users/123/email
```

**Rationale:**
- Simple, predictable
- PostgreSQL roles don't map cleanly to Unix users
- Single daemon process = single Unix user

### File Size

**Column files:**
- Size = byte length of text representation
- May require computing on first access (acceptable overhead)

**Row files:**
- Size = byte length of serialized format (TSV/CSV/JSON)
- Computed on demand

**Directories:**
- Size = 0 or 4096 (standard directory size, not meaningful)

**Rationale:**
- Accurate file sizes for `ls -lh`, progress bars, etc.
- Small overhead for computing sizes on demand

## Error Handling

### Database Errors → Filesystem Errors

**Standard mappings:**

| PostgreSQL Error | Filesystem errno | Example |
|-----------------|------------------|---------|
| Row not found | ENOENT | `cat /schema/users/999/email` (row 999 doesn't exist) |
| Permission denied (table grants) | EACCES | `echo 'x' > /schema/protected/1/col` (no UPDATE privilege) |
| NOT NULL constraint | EACCES | `mkdir /schema/users/1; echo 'Name' > name` (missing required email) |
| Foreign key violation | EACCES | `echo '999' > /schema/orders/1/user_id` (user 999 doesn't exist) |
| Connection refused | EIO | PostgreSQL server down or unreachable |
| Connection timeout | EIO | Network issues, slow database |
| Query timeout | EIO | Query took too long |
| Unexpected SQL error | EIO | Syntax errors, internal errors |

### Logging Strategy

**All errors logged with full detail:**
```
2026-01-23 10:30:15 ERROR: NOT NULL constraint violation
  Operation: INSERT INTO schema.users (id, name)
  Error: null value in column "email" violates not-null constraint
  Path: /schema/users/125/name
  User: mfreed
```

**User sees:**
```bash
echo 'Name' > /schema/users/125/name
# (returns EACCES)
echo $?
# 13 (EACCES)
```

**Rationale:**
- Preserve filesystem abstraction (standard errno values)
- Detailed diagnostics in logs for debugging
- Avoid leaking implementation details to users (keep abstraction clean)

**Future consideration:**
Could add extended attributes (`getfattr`) for detailed error messages if needed.

## Schema Metadata Exposure

### Table-Level Metadata Files

**Per-table special files:**

**`.schema` - Full DDL:**
```bash
cat /schema/users/.schema
# CREATE TABLE users (
#   id INTEGER PRIMARY KEY,
#   email TEXT NOT NULL,
#   name TEXT,
#   age INTEGER DEFAULT 0,
#   created_at TIMESTAMP DEFAULT NOW()
# );
```

**`.columns` - Simple column list:**
```bash
cat /schema/users/.columns
# id
# email
# name
# age
# created_at
```

**`.indexes` - Index definitions:**
```bash
cat /schema/users/.indexes
# users_pkey PRIMARY KEY (id)
# users_email_idx UNIQUE (email)
# users_name_idx (last_name, first_name)
```

**`.count` - Row count:**
```bash
cat /schema/users/.count
# 1847392
```

**SQL Generated:**
```sql
-- .schema: Query information_schema or pg_dump
-- .columns: SELECT column_name FROM information_schema.columns WHERE ...
-- .indexes: SELECT * FROM pg_indexes WHERE tablename = 'users'
-- .count: SELECT count(*) FROM users (approximate via pg_class.reltuples for large tables)
```

### Global Metadata: information_schema

**Expose PostgreSQL information_schema as filesystem:**

```bash
/mount/.information_schema/
├── tables/
├── columns/
├── table_constraints/
└── key_column_usage/
```

**Allows queries like:**
```bash
# Find all tables
ls /mount/.information_schema/tables/

# Find columns for a table
grep "users" /mount/.information_schema/columns/*
```

**Rationale:**
- Comprehensive metadata access for power users
- Standard PostgreSQL structure (familiar)
- Complements per-table metadata files

## Database Objects Beyond Tables

### Views

**Regular and materialized views appear like tables:**

```bash
# View looks identical to table
ls /schema/active_users/
cat /schema/active_users/123/email
```

**Distinction via metadata:**
```bash
cat /schema/active_users/.schema
# VIEW: CREATE VIEW active_users AS
#   SELECT * FROM users WHERE active = true;
```

**Write behavior:**
- Writes to updatable views work (PostgreSQL handles rules)
- Writes to non-updatable views fail with EACCES
- Error log explains: "Cannot modify non-updatable view"

**Rationale:**
- Consistent interface (views are queryable like tables)
- Metadata clearly identifies views
- PostgreSQL handles view update rules

### Sequences

**Not exposed initially.**

**Rationale:**
- Internal database mechanics
- Not data access (primary use case)
- Defer until clear use case emerges

**Future consideration:**
Could expose as special files:
```bash
cat /schema/.sequences/user_id_seq
# 12847
echo 'nextval' > /schema/.sequences/user_id_seq
# Returns next value
```

### Functions and Stored Procedures

**Not exposed initially.**

**Rationale:**
- Hard to map naturally to filesystem
- Not data access (primary use case)
- Functions with side effects complicate model

**Future consideration:**
Could expose as executable files or special invocation mechanism.

## Transactions and Atomicity

### Transaction Model: No Explicit Transactions

**Each filesystem operation = one SQL statement = auto-commit:**

```bash
echo 'new@email.com' > /schema/users/123/email
# UPDATE users SET email='new@email.com' WHERE id=123; COMMIT;

echo 'Name' > /schema/users/123/name
# UPDATE users SET name='Name' WHERE id=123; COMMIT;
```

Two separate transactions. If second fails, first is already committed.

### Atomicity for Full-Row Updates

**Row-as-file provides natural atomicity:**

```bash
echo -e "new@email.com\tNew Name\t30" > /schema/users/123.tsv
# Single UPDATE statement = atomic
# UPDATE users SET email='new@email.com', name='New Name', age=30 WHERE id=123;
```

**Recommendation for users:**
- Use row-as-file format for atomic multi-column updates
- Row-as-directory column updates are independent operations

### Future Consideration: Explicit Transactions

**Could add transaction control via special files:**

```bash
cat /schema/.begin              # BEGIN TRANSACTION
echo 'x' > /schema/users/123/email
echo 'y' > /schema/users/123/name
cat /schema/.commit             # COMMIT
# or
cat /schema/.rollback           # ROLLBACK
```

**Complexity:**
- Need to track transaction state per client (FUSE doesn't have session concept)
- Multiple concurrent clients complicate state management
- Defer unless clear need emerges

**Rationale for deferring:**
- Adds significant complexity
- Most use cases satisfied by row-as-file atomicity
- Can add later if demand exists

## Special Directories for Large Tables

### Problem Statement

Tables with millions of rows make `ls /schema/users/` unusable (slow, huge output).

### Solution: Specialized Access Patterns

**`.first/N/` - First N rows by primary key:**
```bash
ls /schema/users/.first/50/
# 1  2  3  5  7  9  ...  (first 50 actual PKs)
```

**SQL Generated:**
```sql
SELECT id FROM users ORDER BY id LIMIT 50;
```

**`.sample/N/` - Random N rows:**
```bash
ls /schema/users/.sample/100/
# 47392  103847  291844  ...  (random actual PKs)
```

**SQL Generated:**
```sql
SELECT id FROM users TABLESAMPLE BERNOULLI(percentage) LIMIT 100;
-- Percentage calculated to approximate N rows
```

**Rationale for TABLESAMPLE:**
- Fast (doesn't require full table scan)
- "Good enough" randomness for exploration
- Approximate (might return fewer than N rows, acceptable trade-off)

**`.count` - Total row count:**
```bash
cat /schema/users/.count
# 10847392
```

**SQL Generated (for large tables, use estimate):**
```sql
-- Approximate (fast)
SELECT reltuples::bigint FROM pg_class WHERE relname = 'users';

-- Or exact (slow for large tables)
SELECT count(*) FROM users;
```

Use approximate by default, exact for small tables.

### Direct PK Access Always Works

**Even if table is huge, direct access succeeds:**
```bash
cat /schema/users/123456/email
# (works, indexed lookup by PK)
```

**Rationale:**
- Primary key lookups are fast (indexed)
- Don't need to list all rows to access specific row
- Encourages efficient access patterns

## Implementation Priorities

### Phase 1: Read-Only Foundation
- FUSE mount with connection handling
- Schema/table/row navigation
- Row-as-file and row-as-directory reads (TSV, JSON, CSV)
- Index-based navigation (single-column indexes)
- Basic metadata files (.schema, .columns, .indexes)
- Permission mapping (read-only initially)
- Error handling and logging

### Phase 2: Write Support
- Row-as-file writes (INSERT/UPDATE)
- Row-as-directory writes (column-level INSERT/UPDATE)
- Constraint enforcement (NOT NULL, foreign keys)
- DELETE operations (rm)
- Permission checks for write operations

### Phase 3: Large Table Support
- `.first/N/` and `.sample/N/` directories
- `.count` file
- Configurable threshold for large table listing
- Helpful error messages for oversized listings

### Phase 4: Advanced Features
- Composite index navigation
- Default schema flattening
- `.refresh` file for cache clearing
- Multi-user consistency improvements
- Extended PostgreSQL type support (arrays, JSONB, BYTEA)

### Phase 5: Polish
- `.information_schema/` exposure
- View support
- Performance profiling and optimization
- Documentation and examples
- Claude Code integration testing

## Open Questions for Implementation

1. **Connection pooling:** Single connection or pool? Thread-safety model?
2. **Large result sets:** How to handle queries returning millions of rows (directory listings)?
3. **FUSE library choice:** `bazil.org/fuse` vs `jacobsa/fuse` - benchmarks needed
4. **Numeric precision:** How to represent PostgreSQL NUMERIC/DECIMAL without loss?
5. **Timezone handling:** How to represent TIMESTAMP WITH TIME ZONE in column files?
6. **Character encoding:** UTF-8 everywhere, or handle other encodings?
7. **Query timeouts:** Default timeout for SQL queries? Configurable?
8. **Error recovery:** How to handle database disconnections during operations?

## Success Metrics

**Primary:**
- Claude Code can explore database using Read/Glob/Grep tools naturally
- Developers find it faster than switching to database tools
- Common database tasks achievable via filesystem operations

**Performance:**
- Indexed lookups complete in < 100ms
- Directory listings (small tables) complete in < 500ms
- No noticeable overhead vs direct SQL for single-row access

**Reliability:**
- Database constraints always enforced
- No data corruption or loss
- Graceful handling of database failures

## Documentation Requirements

**User Guide:**
- Quick start: mount database, explore tables
- Navigation patterns (indexes, samples, direct access)
- Read/write operations with examples
- Constraint handling and error interpretation
- Multi-format usage (TSV, CSV, JSON)

**Developer Guide:**
- Architecture overview
- Extension points (custom types, metadata)
- Performance tuning
- Troubleshooting

**Claude Code Guide:**
- Recommended usage patterns
- Example workflows (explore, query, modify)
- Performance considerations (use indexes, samples)

---

**End of Specification**

This specification provides comprehensive guidance for implementing the database filesystem interface. All major design decisions have been made, with clear rationales. Open questions noted for resolution during implementation.
