# TigerFS Implementation Tasks for Claude Code

**Purpose:** Step-by-step implementation tasks designed for autonomous execution by Claude Code.

**Format:** Each task is self-contained with clear inputs, outputs, and verification criteria.

---

## How to Use This Plan

1. **Execute tasks sequentially** - Each task builds on previous ones
2. **Verify after each task** - Run tests and checks before moving to next task
3. **Commit after completion** - Create git commit when task fully complete
4. **Ask questions if blocked** - If unclear or blocked, ask for clarification

---

## Phase 1: Core Foundation

### Task 1.1: Evaluate and Select FUSE Library

**Objective:** Choose between bazil.org/fuse and github.com/hanwen/go-fuse/v2

**Steps:**
1. Create `test/fuse_benchmark/` directory
2. Create `test/fuse_benchmark/bazil_test.go` - Basic mount/unmount/readdir test using bazil.org/fuse
3. Create `test/fuse_benchmark/hanwen_test.go` - Same test using hanwen/go-fuse/v2
4. Run benchmarks: `go test -bench=. ./test/fuse_benchmark/`
5. Compare: API complexity, performance, documentation quality
6. Document decision in `docs/.plans/fuse-library-decision.md`
7. Add chosen library to `go.mod`: `go get <chosen-library>`
8. Remove benchmark code after decision

**Files to Create:**
- `test/fuse_benchmark/bazil_test.go`
- `test/fuse_benchmark/hanwen_test.go`
- `docs/.plans/fuse-library-decision.md`

**Verification:**
```bash
go test -bench=. ./test/fuse_benchmark/
# Document shows clear rationale for chosen library
go mod tidy
```

**Completion Criteria:**
- Both libraries tested with basic operations
- Decision documented with rationale
- Chosen library added to go.mod
- Benchmark code removed

---

### Task 1.2: Implement Database Connection

**Objective:** Implement `db.NewClient()` with pgx connection pooling

**Steps:**
1. Open `internal/tigerfs/db/client.go`
2. Implement `NewClient()` function:
   - Parse connection string using `pgxpool.ParseConfig()`
   - Configure connection pool (size, max idle from config)
   - Create pool: `pgxpool.NewWithConfig(ctx, poolConfig)`
   - Ping database to verify connection
   - Return `&Client{pool: pool, cfg: cfg}`
3. Remove stub error: `return nil, fmt.Errorf("database connection not yet implemented")`
4. Add test `internal/tigerfs/db/client_test.go`:
   - Test connection with valid connection string
   - Test connection with invalid connection string
   - Test connection pool configuration

**Files to Modify:**
- `internal/tigerfs/db/client.go`

**Files to Create:**
- `internal/tigerfs/db/client_test.go`

**Verification:**
```bash
# Unit test
go test ./internal/tigerfs/db/

# Manual test with real database
export PGHOST=localhost PGDATABASE=postgres
go run ./cmd/tigerfs test-connection
```

**Completion Criteria:**
- `NewClient()` successfully connects to PostgreSQL
- Connection pool configured from config
- Unit tests pass
- Ping verifies connection

---

### Task 1.3: Implement Password Resolution

**Objective:** Support .pgpass file and environment variables for password

**Steps:**
1. Open `internal/tigerfs/db/client.go`
2. Add password resolution logic before creating connection:
   - Check if password in connection string (use as-is)
   - Check `TIGERFS_PASSWORD` or `PGPASSWORD` env var
   - pgx automatically checks `~/.pgpass` file (no code needed)
3. Create `internal/tigerfs/db/password.go`:
   - Add `resolvePassword(cfg *config.Config, connStr string) (string, error)`
   - Handle password_command execution if configured
4. Add test for password resolution

**Files to Modify:**
- `internal/tigerfs/db/client.go`

**Files to Create:**
- `internal/tigerfs/db/password.go`
- `internal/tigerfs/db/password_test.go`

**Verification:**
```bash
# Test with .pgpass
echo "localhost:5432:*:postgres:testpass" >> ~/.pgpass
chmod 0600 ~/.pgpass
go run ./cmd/tigerfs test-connection

# Test with env var
export PGPASSWORD=testpass
go run ./cmd/tigerfs test-connection
```

**Completion Criteria:**
- Passwords resolved from multiple sources
- .pgpass file works (via pgx)
- Environment variables work
- password_command support added
- Tests pass

---

### Task 1.4: Implement Basic FUSE Mount

**Objective:** Mount filesystem, handle root directory operations

**Steps:**
1. Open `internal/tigerfs/fuse/fs.go`
2. Implement `Mount()` function using chosen FUSE library:
   - Connect to database: `dbClient, err := db.NewClient(ctx, cfg, connStr)`
   - Create FUSE server instance
   - Register filesystem operations (Root, Lookup, Getattr)
   - Mount at mountpoint
   - Return `&FS{cfg: cfg, db: dbClient, mountpoint: mountpoint, server: server}`
3. Implement `Serve()` function:
   - Start FUSE server (blocks until unmount)
   - Handle context cancellation
4. Implement `Close()` function:
   - Close database connection
   - Unmount filesystem
5. Create `internal/tigerfs/fuse/root.go`:
   - Implement root directory operations
   - `Lookup()` for root (returns table names)
   - `ReadDir()` for root (lists tables)
   - `Getattr()` for root (directory attributes)

**Files to Modify:**
- `internal/tigerfs/fuse/fs.go`

**Files to Create:**
- `internal/tigerfs/fuse/root.go`
- `internal/tigerfs/fuse/fs_test.go`

**Verification:**
```bash
# Start PostgreSQL (if not running)
docker run -d -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:17

# Test mount
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount
# In another terminal:
ls /tmp/testmount/
umount /tmp/testmount
```

**Completion Criteria:**
- FUSE filesystem mounts successfully
- Root directory accessible
- `ls /tmp/testmount/` returns empty list (no tables yet)
- Unmount works cleanly

---

### Task 1.5: Implement Schema Discovery

**Objective:** Query PostgreSQL schemas and tables, list them in filesystem

**Steps:**
1. Create `internal/tigerfs/db/schema.go`
2. Implement `GetSchemas(ctx, pool)`:
   ```sql
   SELECT schema_name FROM information_schema.schemata
   WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
   ORDER BY schema_name
   ```
3. Implement `GetTables(ctx, pool, schema)`:
   ```sql
   SELECT table_name FROM information_schema.tables
   WHERE table_schema = $1 AND table_type = 'BASE TABLE'
   ORDER BY table_name
   ```
4. Create `internal/tigerfs/fuse/cache.go`:
   - Add `MetadataCache` struct to cache schemas/tables
   - Add refresh logic (TTL-based)
5. Update `internal/tigerfs/fuse/root.go`:
   - `ReadDir()` should list tables from public schema (schema flattening)
   - `Lookup()` should resolve table names

**Files to Create:**
- `internal/tigerfs/db/schema.go`
- `internal/tigerfs/db/schema_test.go`
- `internal/tigerfs/fuse/cache.go`
- `internal/tigerfs/fuse/cache_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/root.go`

**Verification:**
```bash
# Create test table
psql postgres://postgres:test@localhost/postgres -c "CREATE TABLE users (id serial primary key, email text);"

# Mount and list
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount
ls /tmp/testmount/
# Should show: users

umount /tmp/testmount
```

**Completion Criteria:**
- Schema query working
- Table query working
- `ls /mnt/db/` lists tables from public schema
- Metadata cached
- Tests pass

---

### Task 1.6: Implement Table Directory Operations

**Objective:** Navigate into table directories, see row primary keys

**Steps:**
1. Create `internal/tigerfs/fuse/table.go`
2. Implement table directory operations:
   - `Lookup()` for table names (validates table exists)
   - `ReadDir()` for table directory (lists PKs)
   - `Getattr()` for table directory (directory attributes)
3. Create `internal/tigerfs/db/keys.go`:
   - Implement `GetPrimaryKey(ctx, pool, schema, table)`:
     ```sql
     SELECT kcu.column_name
     FROM information_schema.table_constraints tc
     JOIN information_schema.key_column_usage kcu
       ON tc.constraint_name = kcu.constraint_name
     WHERE tc.table_schema = $1 AND tc.table_name = $2
       AND tc.constraint_type = 'PRIMARY KEY'
     ORDER BY kcu.ordinal_position
     ```
   - Return error if composite PK (not supported in MVP)
4. Implement `ListRows(ctx, pool, schema, table, limit)`:
   ```sql
   SELECT <pk_column> FROM <schema>.<table> ORDER BY <pk_column> LIMIT $1
   ```

**Files to Create:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/table_test.go`
- `internal/tigerfs/db/keys.go`
- `internal/tigerfs/db/keys_test.go`

**Verification:**
```bash
# Insert test data
psql postgres://postgres:test@localhost/postgres -c "INSERT INTO users (email) VALUES ('test1@example.com'), ('test2@example.com');"

# Mount and list rows
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount
ls /tmp/testmount/users/
# Should show: 1  2

umount /tmp/testmount
```

**Completion Criteria:**
- Table directories navigable
- Primary key discovered
- `ls /mnt/db/users/` lists row PKs
- dir_listing_limit enforced (return error if exceeded)
- Tests pass

---

### Task 1.7: Implement Row-as-File Read (TSV)

**Objective:** Read entire row as TSV file

**Steps:**
1. Create `internal/tigerfs/fuse/row.go`
2. Implement row file operations:
   - `Lookup()` for row files (validates row exists)
   - `Open()` for row files
   - `Read()` for row files (returns TSV data)
   - `Getattr()` for row files (file attributes, size)
3. Create `internal/tigerfs/db/query.go`:
   - Implement `GetRow(ctx, pool, schema, table, pkColumn, pkValue)`:
     ```sql
     SELECT * FROM <schema>.<table> WHERE <pk_column> = $1
     ```
   - Return all column values
4. Create `internal/tigerfs/format/tsv.go`:
   - Implement `RowToTSV(columns []string, values []interface{}) ([]byte, error)`
   - Tab-separated values
   - No header row
   - Empty field for NULL
   - Convert all types to text representation

**Files to Create:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/fuse/row_test.go`
- `internal/tigerfs/db/query.go`
- `internal/tigerfs/db/query_test.go`
- `internal/tigerfs/format/tsv.go`
- `internal/tigerfs/format/tsv_test.go`

**Verification:**
```bash
# Mount and read row
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount
cat /tmp/testmount/users/1
# Should show: 1	test1@example.com

cat /tmp/testmount/users/999
# Should return: No such file or directory

umount /tmp/testmount
```

**Completion Criteria:**
- Row files readable
- TSV format correct (tabs, no header)
- NULL values shown as empty fields
- Non-existent rows return ENOENT
- Tests pass

---

### Task 1.8: Setup Integration Test Infrastructure

**Objective:** Set up testcontainers-go for automated integration tests

**Steps:**
1. Add testcontainers-go: `go get github.com/testcontainers/testcontainers-go`
2. Add PostgreSQL module: `go get github.com/testcontainers/testcontainers-go/modules/postgres`
3. Create `test/integration/setup.go`:
   - Implement `SetupTestDB(t *testing.T) (connStr string, cleanup func())`
   - Start PostgreSQL container
   - Get connection string
   - Create test schema (users, products tables)
   - Seed test data
   - Return cleanup function
4. Create `test/integration/mount_test.go`:
   - Test: Mount → List tables → Unmount
   - Test: Mount → List rows → Read row → Unmount

**Files to Create:**
- `test/integration/setup.go`
- `test/integration/mount_test.go`

**Verification:**
```bash
# Run integration tests
go test -v ./test/integration/

# Tests should:
# - Start PostgreSQL container
# - Mount filesystem
# - Perform operations
# - Unmount and cleanup
# - All pass
```

**Completion Criteria:**
- testcontainers-go working
- Test database created and seeded
- Integration tests pass
- Cleanup works properly

---

### Task 1.9: Implement CSV Format

**Objective:** Support CSV output format

**Steps:**
1. Create `internal/tigerfs/format/csv.go`
2. Implement `RowToCSV(columns []string, values []interface{}) ([]byte, error)`:
   - Comma-separated values
   - No header row
   - RFC 4180 quoting (quote fields containing comma, quote, newline)
   - Empty field for NULL
3. Update `internal/tigerfs/fuse/row.go`:
   - Detect `.csv` extension
   - Call `format.RowToCSV()` if CSV format requested
4. Add tests for CSV format with special characters

**Files to Create:**
- `internal/tigerfs/format/csv.go`
- `internal/tigerfs/format/csv_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`

**Verification:**
```bash
# Mount
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount

# Test CSV
cat /tmp/testmount/users/1.csv
# Should show: 1,test1@example.com

# Test with special chars
psql -c "INSERT INTO users (email) VALUES ('test,with,commas@example.com');"
cat /tmp/testmount/users/3.csv
# Should show: 3,"test,with,commas@example.com"

umount /tmp/testmount
```

**Completion Criteria:**
- CSV format works
- Quoting correct per RFC 4180
- NULL values as empty fields
- Tests pass

---

### Task 1.10: Implement JSON Format

**Objective:** Support JSON output format

**Steps:**
1. Create `internal/tigerfs/format/json.go`
2. Implement `RowToJSON(columns []string, values []interface{}) ([]byte, error)`:
   - Compact JSON (single line)
   - Map of column_name: value
   - `null` for NULL values
   - Use `encoding/json` package
3. Update `internal/tigerfs/fuse/row.go`:
   - Detect `.json` extension
   - Call `format.RowToJSON()` if JSON format requested
4. Add tests for JSON format

**Files to Create:**
- `internal/tigerfs/format/json.go`
- `internal/tigerfs/format/json_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`

**Verification:**
```bash
# Mount
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount

# Test JSON
cat /tmp/testmount/users/1.json
# Should show: {"id":1,"email":"test1@example.com"}

# Verify valid JSON
cat /tmp/testmount/users/1.json | jq .
# Should pretty-print successfully

umount /tmp/testmount
```

**Completion Criteria:**
- JSON format works
- Valid JSON syntax
- NULL values as `null`
- Tests pass

---

### Task 1.11: Write Comprehensive Format Tests

**Objective:** Integration tests for all formats

**Steps:**
1. Create `test/integration/format_test.go`
2. Test scenarios:
   - Read row as TSV (default, no extension)
   - Read row as TSV (explicit .tsv)
   - Read row as CSV (.csv)
   - Read row as JSON (.json)
   - NULL value handling in each format
   - Special characters in each format (tabs, commas, quotes, newlines)
3. Create test table with diverse data types:
   - TEXT, INTEGER, BOOLEAN, TIMESTAMP, JSONB

**Files to Create:**
- `test/integration/format_test.go`

**Verification:**
```bash
go test -v ./test/integration/ -run TestFormats
# All format tests pass
```

**Completion Criteria:**
- All 3 formats tested
- NULL handling verified
- Special characters handled
- All tests pass

---

## Phase 2: Full CRUD

### Task 2.1: Implement Row-as-Directory Structure

**Objective:** Navigate into row as directory, see column names

**Steps:**
1. Update `internal/tigerfs/fuse/row.go`:
   - Handle row as directory (path ends with `/`)
   - Implement `Lookup()` for row directory
   - Implement `ReadDir()` for row directory (list column names)
   - Implement `Getattr()` for row directory
2. Update `internal/tigerfs/db/schema.go`:
   - Implement `GetColumns(ctx, pool, schema, table)`:
     ```sql
     SELECT column_name, data_type, is_nullable
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position
     ```
3. Cache column metadata

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/db/schema.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount
ls /tmp/testmount/users/1/
# Should show: id  email
umount /tmp/testmount
```

**Completion Criteria:**
- Row directories navigable
- `ls /mnt/db/users/1/` lists column names
- Column metadata cached
- Tests pass

---

### Task 2.2: Implement Column File Read

**Objective:** Read individual column values

**Steps:**
1. Create `internal/tigerfs/fuse/column.go`
2. Implement column file operations:
   - `Lookup()` for column files
   - `Open()` for column files
   - `Read()` for column files (returns column value)
   - `Getattr()` for column files (file size = value length)
3. Update `internal/tigerfs/db/query.go`:
   - Implement `GetColumn(ctx, pool, schema, table, pkColumn, pkValue, columnName)`:
     ```sql
     SELECT <column> FROM <schema>.<table> WHERE <pk> = $1
     ```
4. Handle NULL values (return empty file, 0 bytes)

**Files to Create:**
- `internal/tigerfs/fuse/column.go`
- `internal/tigerfs/fuse/column_test.go`

**Files to Modify:**
- `internal/tigerfs/db/query.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://postgres:test@localhost/postgres /tmp/testmount
cat /tmp/testmount/users/1/email
# Should show: test1@example.com

cat /tmp/testmount/users/1/id
# Should show: 1

umount /tmp/testmount
```

**Completion Criteria:**
- Column files readable
- Individual columns queried efficiently
- NULL values return empty file
- Tests pass

---

### Task 2.3: Implement Type Conversions

**Objective:** Handle various PostgreSQL data types correctly

**Steps:**
1. Create `internal/tigerfs/format/convert.go`
2. Implement `ValueToText(value interface{}, dataType string) (string, error)`:
   - TEXT, VARCHAR → as-is
   - INTEGER, BIGINT → sprintf("%d")
   - BOOLEAN → "true"/"false"
   - TIMESTAMP → RFC3339 format
   - JSONB → compact JSON
   - Array → JSON array
   - BYTEA → raw bytes (special handling)
3. Add tests for each type
4. Update column read to use type conversion

**Files to Create:**
- `internal/tigerfs/format/convert.go`
- `internal/tigerfs/format/convert_test.go`

**Verification:**
```bash
# Create table with various types
psql -c "CREATE TABLE test_types (
  id serial,
  text_col text,
  int_col integer,
  bool_col boolean,
  ts_col timestamp,
  json_col jsonb,
  array_col text[]
);"
psql -c "INSERT INTO test_types (text_col, int_col, bool_col, ts_col, json_col, array_col)
VALUES ('hello', 42, true, '2026-01-23 10:00:00', '{\"key\":\"value\"}', '{\"a\",\"b\"}');"

# Test reading each type
go run ./cmd/tigerfs postgres://... /tmp/testmount
cat /tmp/testmount/test_types/1/text_col  # hello
cat /tmp/testmount/test_types/1/int_col   # 42
cat /tmp/testmount/test_types/1/bool_col  # true
cat /tmp/testmount/test_types/1/json_col  # {"key":"value"}
cat /tmp/testmount/test_types/1/array_col # ["a","b"]
```

**Completion Criteria:**
- All common types convert correctly
- JSONB returns valid JSON
- Arrays return JSON arrays
- Timestamps in ISO 8601
- Tests pass

---

### Task 2.4: Implement Metadata Files

**Objective:** Special files for table metadata (.columns, .schema, .count)

**Steps:**
1. Create `internal/tigerfs/fuse/metadata.go`
2. Implement `.columns` file:
   - `Lookup()` detects `.columns`
   - `Read()` returns list of column names (one per line)
3. Implement `.schema` file:
   - `Lookup()` detects `.schema`
   - `Read()` returns CREATE TABLE statement
   - Use `pg_dump --schema-only --table=<table>` or construct from information_schema
4. Implement `.count` file:
   - `Lookup()` detects `.count`
   - `Read()` returns row count
   - For large tables: use `pg_class.reltuples` estimate
   - For small tables: `SELECT count(*) FROM table`

**Files to Create:**
- `internal/tigerfs/fuse/metadata.go`
- `internal/tigerfs/fuse/metadata_test.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount
cat /tmp/testmount/users/.columns
# Should show:
# id
# email

cat /tmp/testmount/users/.count
# Should show: 2

cat /tmp/testmount/users/.schema
# Should show: CREATE TABLE users (...)
```

**Completion Criteria:**
- `.columns` file works
- `.schema` file returns DDL
- `.count` file returns count
- Tests pass

---

### Task 2.5: Implement Row-as-File Write (UPDATE)

**Objective:** Update existing rows by writing to row files

**Steps:**
1. Update `internal/tigerfs/fuse/row.go`:
   - Implement `Write()` for row files
   - Parse format from file extension
   - Parse data based on format (TSV/CSV/JSON)
   - Detect if row exists (SELECT to check)
   - If exists: generate UPDATE
2. Create `internal/tigerfs/format/parse.go`:
   - Implement `ParseTSV(data []byte, columns []string) (map[string]interface{}, error)`
   - Implement `ParseCSV(data []byte, columns []string) (map[string]interface{}, error)`
   - Implement `ParseJSON(data []byte) (map[string]interface{}, error)`
3. Update `internal/tigerfs/db/query.go`:
   - Implement `UpdateRow(ctx, pool, schema, table, pkColumn, pkValue, values map[string]interface{})`:
     ```sql
     UPDATE <schema>.<table> SET col1=$1, col2=$2, ... WHERE <pk>=$N
     ```

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/db/query.go`

**Files to Create:**
- `internal/tigerfs/format/parse.go`
- `internal/tigerfs/format/parse_test.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Test TSV update
echo -e "1\tnew@example.com" > /tmp/testmount/users/1
cat /tmp/testmount/users/1/email
# Should show: new@example.com

# Test JSON update
echo '{"id":1,"email":"newer@example.com"}' > /tmp/testmount/users/1.json
cat /tmp/testmount/users/1/email
# Should show: newer@example.com

# Verify in database
psql -c "SELECT * FROM users WHERE id=1;"
# Should show updated data
```

**Completion Criteria:**
- UPDATE working for all formats
- Data parsed correctly
- Database updated
- Tests pass

---

### Task 2.6: Implement Row-as-File Write (INSERT)

**Objective:** Insert new rows by writing to row files

**Steps:**
1. Update `internal/tigerfs/fuse/row.go`:
   - In `Write()`, detect if row doesn't exist
   - If doesn't exist: generate INSERT
2. Update `internal/tigerfs/db/query.go`:
   - Implement `InsertRow(ctx, pool, schema, table, values map[string]interface{})`:
     ```sql
     INSERT INTO <schema>.<table> (col1, col2, ...) VALUES ($1, $2, ...)
     ```
   - Handle auto-generated PKs (return new PK value)
3. Handle default values and NOT NULL constraints

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/db/query.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Insert via JSON
echo '{"id":100,"email":"insert@example.com"}' > /tmp/testmount/users/100.json
cat /tmp/testmount/users/100/email
# Should show: insert@example.com

# Verify in database
psql -c "SELECT * FROM users WHERE id=100;"
# Should show inserted row
```

**Completion Criteria:**
- INSERT working for all formats
- New rows created
- Auto-generated PKs handled
- Tests pass

---

### Task 2.7: Implement Column-Level Write

**Objective:** Update individual columns by writing to column files

**Steps:**
1. Update `internal/tigerfs/fuse/column.go`:
   - Implement `Write()` for column files
   - Parse value from written data
   - Convert text to appropriate PostgreSQL type
   - Generate UPDATE for single column
2. Update `internal/tigerfs/db/query.go`:
   - Implement `UpdateColumn(ctx, pool, schema, table, pkColumn, pkValue, columnName, value)`:
     ```sql
     UPDATE <schema>.<table> SET <column>=$1 WHERE <pk>=$2
     ```

**Files to Modify:**
- `internal/tigerfs/fuse/column.go`
- `internal/tigerfs/db/query.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Update single column
echo 'column-update@example.com' > /tmp/testmount/users/1/email
cat /tmp/testmount/users/1/email
# Should show: column-update@example.com

# Verify in database
psql -c "SELECT email FROM users WHERE id=1;"
# Should show: column-update@example.com
```

**Completion Criteria:**
- Column updates working
- Type conversion correct
- Single-column UPDATE generated
- Tests pass

---

### Task 2.8: Implement Incremental Row Creation

**Objective:** Create rows by making directory and writing columns incrementally

**Steps:**
1. Update `internal/tigerfs/fuse/row.go`:
   - Implement `Mkdir()` for new row directories
   - Track partial row state in memory (map of pk → columns)
2. Update `internal/tigerfs/fuse/column.go`:
   - When writing column for new row, accumulate in partial row state
   - When enough columns provided (satisfy NOT NULL constraints), execute INSERT
3. Add constraint validation before INSERT

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/fuse/column.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Incremental row creation
mkdir /tmp/testmount/users/200
echo 'incremental@example.com' > /tmp/testmount/users/200/email
# Row should now exist in database

psql -c "SELECT * FROM users WHERE id=200;"
# Should show: 200 | incremental@example.com
```

**Completion Criteria:**
- `mkdir` creates new row directory
- Columns written accumulate
- INSERT executed when constraints satisfied
- Tests pass

---

### Task 2.9: Implement Constraint Enforcement

**Objective:** Validate constraints before write operations

**Steps:**
1. Create `internal/tigerfs/db/constraints.go`
2. Implement `ValidateConstraints(ctx, pool, schema, table, values map[string]interface{})`:
   - Query constraints from information_schema
   - Check NOT NULL constraints
   - Check UNIQUE constraints (query for duplicates)
   - Check CHECK constraints (let PostgreSQL handle)
3. Map constraint violations to EACCES
4. Log detailed error messages

**Files to Create:**
- `internal/tigerfs/db/constraints.go`
- `internal/tigerfs/db/constraints_test.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Test NOT NULL violation
psql -c "ALTER TABLE users ALTER COLUMN email SET NOT NULL;"
mkdir /tmp/testmount/users/300
echo 'test' > /tmp/testmount/users/300/id
# Writing should fail with EACCES (email is NOT NULL)

# Check error in logs
# Should see: "NOT NULL constraint violation: column 'email' cannot be NULL"
```

**Completion Criteria:**
- NOT NULL constraints enforced
- UNIQUE constraints checked
- Violations return EACCES
- Detailed error logging
- Tests pass

---

### Task 2.10: Implement Row Deletion

**Objective:** Delete rows using rm command

**Steps:**
1. Update `internal/tigerfs/fuse/row.go`:
   - Implement `Unlink()` for row files (e.g., `rm /users/123`)
   - Implement `Rmdir()` for row directories (e.g., `rm -r /users/123/`)
   - Both should generate DELETE statement
2. Update `internal/tigerfs/db/query.go`:
   - Implement `DeleteRow(ctx, pool, schema, table, pkColumn, pkValue)`:
     ```sql
     DELETE FROM <schema>.<table> WHERE <pk>=$1
     ```
3. Handle foreign key constraints (let PostgreSQL handle CASCADE/RESTRICT)

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/db/query.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Delete row
rm /tmp/testmount/users/1
ls /tmp/testmount/users/
# Should not show '1'

# Verify in database
psql -c "SELECT * FROM users WHERE id=1;"
# Should return: (0 rows)
```

**Completion Criteria:**
- Row deletion working
- Both file and directory deletion work
- Foreign key constraints respected
- Tests pass

---

### Task 2.11: Implement Column Deletion (SET NULL)

**Objective:** Set column to NULL by deleting column file

**Steps:**
1. Update `internal/tigerfs/fuse/column.go`:
   - Implement `Unlink()` for column files (e.g., `rm /users/123/email`)
   - Generate UPDATE setting column to NULL
2. Validate column is nullable before deletion
3. Return EACCES if column is NOT NULL without default

**Files to Modify:**
- `internal/tigerfs/fuse/column.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Set column to NULL
psql -c "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;"
rm /tmp/testmount/users/1/email
cat /tmp/testmount/users/1/email
# Should be empty (0 bytes)

# Verify in database
psql -c "SELECT email FROM users WHERE id=1;"
# Should show: NULL

# Test NOT NULL rejection
psql -c "ALTER TABLE users ALTER COLUMN email SET NOT NULL;"
rm /tmp/testmount/users/2/email
# Should fail with EACCES
```

**Completion Criteria:**
- Column deletion sets to NULL
- NOT NULL columns rejected
- Error messages clear
- Tests pass

---

### Task 2.12: Write Comprehensive CRUD Tests

**Objective:** Complete test coverage for all CRUD operations

**Steps:**
1. Create `test/integration/crud_test.go`
2. Test scenarios:
   - INSERT → SELECT → UPDATE → DELETE (full cycle)
   - Partial updates (only some columns)
   - NULL value handling (write, read, delete)
   - Concurrent operations (multiple writes)
   - Error scenarios (constraint violations)
   - Foreign key handling (CASCADE, RESTRICT)
3. Achieve >85% code coverage

**Files to Create:**
- `test/integration/crud_test.go`

**Verification:**
```bash
# Run CRUD tests
go test -v ./test/integration/ -run TestCRUD

# Check coverage
go test -coverprofile=coverage.txt ./...
go tool cover -html=coverage.txt
# Should show >85% coverage
```

**Completion Criteria:**
- All CRUD operations tested
- Error cases covered
- Concurrent access tested
- Coverage >85%
- All tests pass

---

### Task 2.13: Comprehensive Unit Test Coverage

**Objective:** Add missing unit tests to achieve >60% coverage across all packages

**Overview:** Current coverage analysis shows critical gaps:
- format: 67.3% (parse.go has 0%)
- db: 20.1% (query.go edge cases missing)
- fuse: 4.7% (most operations untested)
- config: 0% (all functionality untested)
- cmd: 0% (all commands untested)
- logging: 0% (all functionality untested)
- util: 100% ✅

This task adds unit tests for 8 critical areas to bring overall coverage to >60%.

---

#### Task 2.13.1: Add format/parse_test.go

**Objective:** Test parsing functions (TSV, CSV, JSON) - security-critical code

**Steps:**
1. Create `internal/tigerfs/format/parse_test.go`
2. Test TSV parsing:
   - `TestParseTSV_Basic` - Simple tab-separated values
   - `TestParseTSV_EmptyFields` - NULL handling
   - `TestParseTSV_SpecialCharacters` - Tabs, newlines in data
   - `TestParseTSV_SingleValue` - One column
   - `TestParseTSV_EmptyString` - Empty input
3. Test CSV parsing:
   - `TestParseCSV_Basic` - Simple comma-separated values
   - `TestParseCSV_QuotedFields` - Quoted fields
   - `TestParseCSV_CommasInQuotes` - Commas inside quoted fields
   - `TestParseCSV_EmptyFields` - NULL handling
   - `TestParseCSV_MalformedInput` - Invalid CSV
   - `TestParseCSV_NewlinesInQuotes` - Newlines inside quoted fields
4. Test JSON parsing:
   - `TestParseJSON_Basic` - Simple object
   - `TestParseJSON_NullValues` - Explicit null values
   - `TestParseJSON_NestedObjects` - Should flatten or error
   - `TestParseJSON_InvalidJSON` - Malformed JSON
   - `TestParseJSON_EmptyObject` - {}
   - `TestParseJSON_Arrays` - Array values
   - `TestParseJSON_Numbers` - Numeric values

**Files to Create:**
- `internal/tigerfs/format/parse_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/format/ -run TestParse
# All parse tests pass

go test -cover ./internal/tigerfs/format/
# Coverage should increase from 67.3% to >90%
```

**Completion Criteria:**
- 15+ test cases covering all parsing functions
- Edge cases tested (empty, malformed, special characters)
- NULL handling verified
- Error cases return appropriate errors
- Coverage for format package >90%

---

#### Task 2.13.2: Add fuse/partial_test.go

**Objective:** Test PartialRowTracker (incremental row creation state management)

**Steps:**
1. Create `internal/tigerfs/fuse/partial_test.go`
2. Test basic operations:
   - `TestPartialRowTracker_GetOrCreate` - Creates new partial row
   - `TestPartialRowTracker_GetOrCreate_Existing` - Returns existing
   - `TestPartialRowTracker_Get_NotFound` - Returns nil
   - `TestPartialRowTracker_SetColumn` - Sets column value
   - `TestPartialRowTracker_SetColumn_Multiple` - Sets multiple columns
3. Test TryCommit:
   - `TestPartialRowTracker_TryCommit_Success` - All required columns set
   - `TestPartialRowTracker_TryCommit_MissingNotNullColumn` - Should not commit
   - `TestPartialRowTracker_TryCommit_ConstraintViolation` - UNIQUE violation
   - `TestPartialRowTracker_TryCommit_AlreadyCommitted` - Idempotent
4. Test concurrency:
   - `TestPartialRowTracker_ConcurrentAccess` - Multiple goroutines
   - `TestPartialRowTracker_MultipleRows` - Multiple partial rows
5. Test cleanup:
   - `TestPartialRowTracker_Delete` - Remove partial row

**Files to Create:**
- `internal/tigerfs/fuse/partial_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/fuse/ -run TestPartialRowTracker
# All partial row tests pass

go test -race ./internal/tigerfs/fuse/ -run TestPartialRowTracker_Concurrent
# No race conditions
```

**Completion Criteria:**
- 12+ test cases covering all PartialRowTracker operations
- Constraint validation tested
- Concurrency safety verified with -race
- Edge cases covered
- All tests pass

---

#### Task 2.13.3: Add config/config_test.go

**Objective:** Test configuration loading (defaults, file, env vars, precedence)

**Steps:**
1. Create `internal/tigerfs/config/config_test.go`
2. Test defaults:
   - `TestConfig_Defaults` - All default values set correctly
   - `TestGetDefaultConfigDir` - Returns correct path
3. Test file loading:
   - `TestInit_ConfigFileNotFound` - Should not error
   - `TestInit_ValidConfigFile` - Loads values correctly
   - `TestInit_InvalidConfigFile` - Returns error
   - `TestLoad_UnmarshalConfig` - Maps to Config struct
4. Test environment variables:
   - `TestConfig_TigerFSEnvVars` - TIGERFS_* variables
   - `TestConfig_PostgreSQLEnvVars` - PGHOST, PGPORT, etc.
   - `TestConfig_TigerServiceIDEnv` - TIGER_SERVICE_ID
5. Test precedence:
   - `TestConfig_Precedence_EnvOverridesFile` - Env wins over file
   - `TestConfig_Precedence_DefaultsLowest` - Defaults only if nothing else
6. Test all config fields:
   - `TestConfig_ConnectionFields` - Host, Port, User, Database, etc.
   - `TestConfig_FilesystemFields` - DirListingLimit, timeouts
   - `TestConfig_LoggingFields` - LogLevel, LogFile, Debug

**Files to Create:**
- `internal/tigerfs/config/config_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/config/
# All config tests pass

# Test with env vars
TIGERFS_MAX_LS_ROWS=5000 go test -v ./internal/tigerfs/config/ -run TestConfig_TigerFSEnvVars
# Should read env var

go test -cover ./internal/tigerfs/config/
# Coverage should increase from 0% to >80%
```

**Completion Criteria:**
- 12+ test cases covering all config loading
- All config fields tested
- Precedence rules verified
- Environment variable binding tested
- Coverage for config package >80%

---

#### Task 2.13.4: Add fuse/root_test.go

**Objective:** Test RootNode operations (root directory of filesystem)

**Steps:**
1. Create `internal/tigerfs/fuse/root_test.go`
2. Test Getattr:
   - `TestRootNode_Getattr` - Returns directory attributes
3. Test Readdir:
   - `TestRootNode_Readdir` - Lists tables from default schema
   - `TestRootNode_Readdir_EmptyDatabase` - No tables case
   - `TestRootNode_Readdir_CacheRefresh` - Metadata cache works
4. Test Lookup:
   - `TestRootNode_Lookup_ValidTable` - Finds existing table
   - `TestRootNode_Lookup_InvalidTable` - Returns ENOENT
   - `TestRootNode_Lookup_CaseSensitivity` - Matches table name correctly

**Files to Create:**
- `internal/tigerfs/fuse/root_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/fuse/ -run TestRootNode
# All root node tests pass
```

**Completion Criteria:**
- 6+ test cases covering RootNode operations
- Readdir lists tables correctly
- Lookup validates table existence
- Cache integration tested
- All tests pass

---

#### Task 2.13.5: Expand db/query_test.go

**Objective:** Increase db package coverage from 20.1% to >50%

**Steps:**
1. Expand `internal/tigerfs/db/query_test.go` with edge cases:
   - Add `TestInsertRow_AutoGeneratedPK` - SERIAL primary key
   - Add `TestInsertRow_ExplicitPK` - User-provided PK
   - Add `TestInsertRow_NullValues` - NULL in nullable columns
   - Add `TestUpdateRow_PartialUpdate` - Only some columns
   - Add `TestUpdateRow_AllColumns` - Full row update
   - Add `TestUpdateRow_SetToNull` - Empty string → NULL
   - Add `TestUpdateColumn_SingleColumn` - Update one column
   - Add `TestDeleteRow_Simple` - Delete existing row
   - Add `TestDeleteRow_NotFound` - Should not error
   - Add `TestGetRow_NotFound` - Should error
   - Add `TestGetColumn_NullValue` - Returns nil
   - Add `TestListRows_Empty` - No rows
   - Add `TestListRows_Pagination` - Respects limit
   - Add `TestListRows_Ordering` - Ordered by PK
2. Test error handling:
   - Add `TestInsertRow_DuplicatePK` - UNIQUE violation
   - Add `TestUpdateRow_InvalidColumn` - Column doesn't exist
   - Add `TestGetRow_InvalidTable` - Table doesn't exist

**Files to Modify:**
- `internal/tigerfs/db/query_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/db/ -run TestInsertRow
go test -v ./internal/tigerfs/db/ -run TestUpdateRow
go test -v ./internal/tigerfs/db/ -run TestDeleteRow

go test -cover ./internal/tigerfs/db/
# Coverage should increase from 20.1% to >50%
```

**Completion Criteria:**
- 15+ new test cases added
- All query functions tested
- Edge cases covered (NULL, empty, errors)
- Coverage for db package >50%
- All tests pass

---

#### Task 2.13.6: Expand fuse/* unit tests

**Objective:** Expand existing minimal fuse test files (table, column, row, rowdir)

**Steps:**
1. Expand `internal/tigerfs/fuse/table_test.go`:
   - Add `TestTableNode_Getattr`
   - Add `TestTableNode_Readdir` (mock db.ListRows)
   - Add `TestTableNode_Lookup_ValidRow`
   - Add `TestTableNode_Lookup_InvalidRow`
   - Add `TestTableNode_Mkdir` (create row directory)
   - Add `TestTableNode_Unlink` (delete row file)
   - Add `TestTableNode_Rmdir` (delete row directory)
2. Expand `internal/tigerfs/fuse/column_test.go`:
   - Add `TestColumnFileNode_Getattr`
   - Add `TestColumnFileNode_Read` (mock db.GetColumn)
   - Add `TestColumnFileNode_Open`
   - Add `TestColumnFileNode_Write`
   - Add `TestColumnFileNode_Flush` (triggers UPDATE or INSERT)
3. Expand `internal/tigerfs/fuse/row_test.go`:
   - Add `TestRowFileNode_Getattr`
   - Add `TestRowFileNode_Open`
   - Add `TestRowFileNode_Read_TSV`
   - Add `TestRowFileNode_Read_CSV`
   - Add `TestRowFileNode_Read_JSON`
   - Add `TestRowFileNode_Write_UPDATE`
   - Add `TestRowFileNode_Write_INSERT`
4. Expand `internal/tigerfs/fuse/rowdir_test.go`:
   - Add `TestRowDirectoryNode_Getattr`
   - Add `TestRowDirectoryNode_Readdir` (lists columns)
   - Add `TestRowDirectoryNode_Lookup_ValidColumn`
   - Add `TestRowDirectoryNode_Lookup_InvalidColumn`
   - Add `TestRowDirectoryNode_Unlink` (SET NULL)

**Files to Modify:**
- `internal/tigerfs/fuse/table_test.go`
- `internal/tigerfs/fuse/column_test.go`
- `internal/tigerfs/fuse/row_test.go`
- `internal/tigerfs/fuse/rowdir_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/fuse/ -run "TestTableNode|TestColumnFileNode|TestRowFileNode|TestRowDirectoryNode"
# All expanded tests pass

go test -cover ./internal/tigerfs/fuse/
# Coverage should increase significantly toward >50%
```

**Completion Criteria:**
- 20+ new test cases added across 4 test files
- All existing minimal tests expanded with real functionality tests
- Mock database interactions where needed
- All CRUD operations covered
- All tests pass

---

#### Task 2.13.7: Add logging/logging_test.go

**Objective:** Test logging initialization and configuration

**Steps:**
1. Create `internal/tigerfs/logging/logging_test.go`
2. Test initialization:
   - `TestInit_DebugMode` - Debug=true sets debug level
   - `TestInit_ProductionMode` - Debug=false sets warn level
   - `TestInit_EncoderConfig` - Correct encoder settings
3. Test log levels:
   - `TestLogging_DebugLevel` - Debug logs appear in debug mode
   - `TestLogging_InfoLevel` - Info logs appear
   - `TestLogging_WarnLevel` - Warn logs appear
   - `TestLogging_ErrorLevel` - Error logs appear
4. Test output:
   - `TestLogging_OutputPaths` - Logs go to stderr
   - `TestSync` - Sync works without error

**Files to Create:**
- `internal/tigerfs/logging/logging_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/logging/
# All logging tests pass

go test -cover ./internal/tigerfs/logging/
# Coverage should increase from 0% to >80%
```

**Completion Criteria:**
- 8+ test cases covering logging setup
- Both debug and production modes tested
- Log level filtering verified
- Coverage for logging package >80%
- All tests pass

---

#### Task 2.13.8: Add cmd/* unit tests

**Objective:** Test command builders and flag handling

**Steps:**
1. Create `internal/tigerfs/cmd/root_test.go`:
   - `TestBuildRootCmd` - Root command created
   - `TestBuildRootCmd_PersistentFlags` - Debug, config-dir flags
   - `TestBuildRootCmd_Subcommands` - All subcommands added
2. Create `internal/tigerfs/cmd/mount_test.go`:
   - `TestBuildMountCmd` - Mount command created
   - `TestBuildMountCmd_Flags` - All mount flags present
   - `TestBuildMountCmd_Args` - Argument parsing
3. Create `internal/tigerfs/cmd/version_test.go`:
   - `TestBuildVersionCmd` - Version command created
   - `TestVersionCmd_Output` - Shows version string
4. Create `internal/tigerfs/cmd/config_test.go`:
   - `TestBuildConfigCmd` - Config command created
   - `TestBuildConfigCmd_Subcommands` - show, validate, path

**Files to Create:**
- `internal/tigerfs/cmd/root_test.go`
- `internal/tigerfs/cmd/mount_test.go`
- `internal/tigerfs/cmd/version_test.go`
- `internal/tigerfs/cmd/config_test.go`

**Verification:**
```bash
go test -v ./internal/tigerfs/cmd/
# All cmd tests pass

go test -cover ./internal/tigerfs/cmd/
# Coverage should increase from 0% to >30%
```

**Completion Criteria:**
- 10+ test cases covering command builders
- Flag presence verified
- Subcommand registration tested
- Coverage for cmd package >30%
- All tests pass

---

### Task 2.14: Docker Testing Environment

**Objective:** Create Docker-based testing environment for machines without PostgreSQL or FUSE

**Overview:** Many development machines cannot run PostgreSQL locally or don't have FUSE installed/configured. This task creates a Docker container with both PostgreSQL and FUSE support, allowing tests to run in an isolated environment.

**Steps:**

1. Create `test/docker/Dockerfile`:
   - Base image: Ubuntu 22.04 or Debian 12
   - Install PostgreSQL 17
   - Install FUSE libraries (fuse3, libfuse3-dev)
   - Install Go 1.23+
   - Create test database and user
   - Set up FUSE permissions
   - Configure PostgreSQL to accept connections
   - Add healthcheck script

2. Create `test/docker/docker-compose.yml`:
   - PostgreSQL service (postgres:17-alpine for database only)
   - TigerFS test service (custom Dockerfile with FUSE)
   - Network configuration
   - Volume mounts for code
   - Environment variables for connection strings
   - Privileged mode for FUSE

3. Create `test/docker/init-db.sh`:
   - Initialize PostgreSQL database
   - Create test schema
   - Create test tables (users, products)
   - Seed test data
   - Grant permissions

4. Create `test/docker/entrypoint.sh`:
   - Start PostgreSQL (if in same container)
   - Wait for PostgreSQL to be ready
   - Load FUSE kernel module
   - Run tests or enter shell
   - Clean up on exit

5. Create `test/docker/run-tests.sh`:
   - Wrapper script to run tests in Docker
   - Detects if local PostgreSQL/FUSE available
   - Falls back to Docker if not
   - Passes test arguments through
   - Handles cleanup

6. Update `test/integration/setup.go`:
   - Add `getTestConnectionStringWithFallback()` function
   - Try local PostgreSQL first
   - Fall back to Docker PostgreSQL if local unavailable
   - Document environment variables

7. Create `Makefile` (or update existing):
   - `make test` - Run all tests (auto-detect local vs Docker)
   - `make test-local` - Force local testing
   - `make test-docker` - Force Docker testing
   - `make docker-shell` - Open shell in test container
   - `make docker-up` - Start Docker services
   - `make docker-down` - Stop Docker services

8. Update `.github/workflows/test.yml`:
   - Use Docker-based testing in CI
   - Install FUSE in GitHub Actions runner (alternative)
   - Run both unit and integration tests
   - Upload coverage reports

**Files to Create:**
- `test/docker/Dockerfile`
- `test/docker/docker-compose.yml`
- `test/docker/init-db.sh`
- `test/docker/entrypoint.sh`
- `test/docker/run-tests.sh`
- `Makefile` (or update existing)

**Files to Modify:**
- `test/integration/setup.go` (add Docker fallback)
- `.github/workflows/test.yml` (use Docker in CI)
- `README.md` (document Docker testing option)
- `CLAUDE.md` (update testing section)

**Verification:**

```bash
# Test Docker setup
cd test/docker
docker-compose up -d
docker-compose ps
# Should show: postgres (healthy), tigerfs-test (running)

# Test PostgreSQL connection
docker-compose exec tigerfs-test psql -U postgres -c "SELECT version();"
# Should show: PostgreSQL 17.x

# Test FUSE availability
docker-compose exec tigerfs-test ls /dev/fuse
# Should show: /dev/fuse

# Run tests in Docker
./test/docker/run-tests.sh
# Should run all tests successfully

# Or via Makefile
make test-docker
# Should run all tests in Docker

# Cleanup
docker-compose down -v
```

**Dockerfile Template:**

```dockerfile
FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    postgresql-17 \
    postgresql-contrib-17 \
    fuse3 \
    libfuse3-dev \
    wget \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Go 1.23
RUN wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz \
    && rm go1.23.0.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

# Configure PostgreSQL
RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/17/main/pg_hba.conf \
    && echo "listen_addresses='*'" >> /etc/postgresql/17/main/postgresql.conf

# Create test database
USER postgres
RUN /etc/init.d/postgresql start \
    && psql -c "CREATE DATABASE tigerfs_test;" \
    && psql -c "CREATE USER tigerfs_user WITH PASSWORD 'tigerfs_pass';" \
    && psql -c "GRANT ALL PRIVILEGES ON DATABASE tigerfs_test TO tigerfs_user;"

USER root

# Set up FUSE permissions
RUN chmod 666 /dev/fuse 2>/dev/null || true

# Working directory
WORKDIR /workspace

# Healthcheck
HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
    CMD pg_isready -U postgres || exit 1

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
```

**docker-compose.yml Template:**

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: tigerfs_test
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 5
    volumes:
      - ./init-db.sh:/docker-entrypoint-initdb.d/init-db.sh

  tigerfs-test:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
    privileged: true  # Required for FUSE
    devices:
      - /dev/fuse:/dev/fuse
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
    environment:
      PGHOST: postgres
      PGPORT: 5432
      PGUSER: postgres
      PGPASSWORD: postgres
      PGDATABASE: tigerfs_test
      TEST_DATABASE_URL: postgres://postgres:postgres@postgres:5432/tigerfs_test
    volumes:
      - ../..:/workspace
    working_dir: /workspace
```

**run-tests.sh Template:**

```bash
#!/bin/bash
set -e

# Detect if we can run tests locally
can_run_local=true

# Check PostgreSQL
if ! command -v psql &> /dev/null; then
    echo "PostgreSQL not found locally"
    can_run_local=false
elif ! pg_isready -h localhost &> /dev/null; then
    echo "PostgreSQL not running locally"
    can_run_local=false
fi

# Check FUSE
if [ ! -e /dev/fuse ]; then
    echo "FUSE not available locally"
    can_run_local=false
fi

# Run tests
if [ "$can_run_local" = true ] && [ "$FORCE_DOCKER" != "1" ]; then
    echo "Running tests locally..."
    go test "$@" ./...
else
    echo "Running tests in Docker..."
    cd test/docker
    docker-compose up -d
    docker-compose exec -T tigerfs-test go test "$@" ./...
    exit_code=$?
    docker-compose down
    exit $exit_code
fi
```

**Makefile Template:**

```makefile
.PHONY: test test-local test-docker docker-up docker-down docker-shell

# Run tests (auto-detect)
test:
	@./test/docker/run-tests.sh -v ./...

# Force local testing
test-local:
	go test -v ./...

# Force Docker testing
test-docker:
	cd test/docker && docker-compose up -d
	docker-compose -f test/docker/docker-compose.yml exec -T tigerfs-test go test -v ./...
	cd test/docker && docker-compose down

# Start Docker services
docker-up:
	cd test/docker && docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	cd test/docker && docker-compose ps

# Stop Docker services
docker-down:
	cd test/docker && docker-compose down -v

# Open shell in test container
docker-shell:
	cd test/docker && docker-compose up -d
	docker-compose -f test/docker/docker-compose.yml exec tigerfs-test bash

# Run integration tests only
test-integration:
	@./test/docker/run-tests.sh -v ./test/integration/...

# Run with coverage
test-coverage:
	@./test/docker/run-tests.sh -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html
```

**Completion Criteria:**
- Dockerfile builds successfully
- docker-compose starts all services
- PostgreSQL accessible in container
- FUSE device available in container
- Tests run successfully in Docker
- Auto-detection script works (local vs Docker)
- Makefile targets functional
- CI workflow uses Docker
- Documentation updated (README, CLAUDE.md)
- All tests pass in both local and Docker environments

**Benefits:**
- Consistent testing environment across all machines
- No need to install PostgreSQL or FUSE locally
- Works on macOS, Linux, Windows (with WSL2)
- CI/CD can use same Docker setup
- Easy onboarding for new developers
- Isolated test database (no conflicts with local DBs)

---

## Phase 3: CLI Commands

### Task 3.1: Implement Unmount Command

**Objective:** Complete `tigerfs unmount` command implementation

**Steps:**
1. Open `internal/tigerfs/cmd/unmount.go`
2. Implement unmount logic:
   - Find mount by mountpoint
   - Send signal to FUSE daemon
   - Wait for graceful shutdown (with timeout)
   - Force unmount if timeout exceeded
   - Handle stale mounts
3. Add state tracking (mount registry file or process tracking)

**Files to Modify:**
- `internal/tigerfs/cmd/unmount.go`

**Files to Create:**
- `internal/tigerfs/mount/registry.go` (mount state tracking)

**Verification:**
```bash
# Mount
go run ./cmd/tigerfs postgres://... /tmp/testmount &
sleep 2

# Unmount
go run ./cmd/tigerfs unmount /tmp/testmount
# Should unmount gracefully

# Verify unmounted
ls /tmp/testmount/
# Should fail or show empty
```

**Completion Criteria:**
- Graceful unmount working
- Timeout handling implemented
- Force unmount option works
- Stale mount cleanup works
- Tests pass

---

### Task 3.2: Implement Status Command

**Objective:** Show mount status and statistics

**Steps:**
1. Open `internal/tigerfs/cmd/status.go`
2. Implement status display:
   - Read mount registry
   - Show mountpoint, database, status, uptime
   - Query statistics (total queries, QPS, connection pool usage)
   - Format output (table or detailed)
3. Store statistics in mount state

**Files to Modify:**
- `internal/tigerfs/cmd/status.go`

**Files to Create:**
- `internal/tigerfs/mount/stats.go` (statistics tracking)

**Verification:**
```bash
# Mount
go run ./cmd/tigerfs postgres://... /tmp/testmount &
sleep 2

# Status
go run ./cmd/tigerfs status /tmp/testmount
# Should show:
# Mountpoint: /tmp/testmount
# Database: postgres://localhost/postgres
# Status: active
# Uptime: 2s
# Queries: 0
```

**Completion Criteria:**
- Status shows mount info
- Statistics displayed
- Both single and list modes work
- Tests pass

---

### Task 3.3: Implement List Command

**Objective:** List all active mounts

**Steps:**
1. Open `internal/tigerfs/cmd/list.go`
2. Implement list logic:
   - Read mount registry
   - List all active mounts (mountpoint only)
   - Simple output for scripting
3. Filter out stale mounts

**Files to Modify:**
- `internal/tigerfs/cmd/list.go`

**Verification:**
```bash
# Mount multiple
go run ./cmd/tigerfs postgres://... /tmp/testmount1 &
go run ./cmd/tigerfs postgres://... /tmp/testmount2 &
sleep 2

# List
go run ./cmd/tigerfs list
# Should show:
# /tmp/testmount1
# /tmp/testmount2
```

**Completion Criteria:**
- Lists all mounts
- Simple output (one per line)
- Stale mounts filtered
- Tests pass

---

### Task 3.4: Implement Test Connection Command

**Objective:** Test database connectivity without mounting

**Steps:**
1. Open `internal/tigerfs/cmd/test_connection.go`
2. Implement connection test:
   - Parse connection string or use config
   - Create database client
   - Ping database
   - Query PostgreSQL version
   - Query accessible tables count
   - Query user permissions
   - Display results
3. Handle connection errors gracefully

**Files to Modify:**
- `internal/tigerfs/cmd/test_connection.go`

**Verification:**
```bash
go run ./cmd/tigerfs test-connection postgres://postgres:test@localhost/postgres
# Should show:
# ✓ Connected to PostgreSQL 17.2
# ✓ Database: postgres
# ✓ User: postgres
# ✓ Permissions: SELECT, INSERT, UPDATE, DELETE
# ✓ Tables accessible: 3

go run ./cmd/tigerfs test-connection postgres://badhost/baddb
# Should show clear error message
```

**Completion Criteria:**
- Connection test working
- Shows database info
- Shows permissions
- Error messages clear
- Tests pass

---

### Task 3.5: Implement Config Commands

**Objective:** Complete config show, validate, path commands

**Steps:**
1. Open `internal/tigerfs/cmd/config.go`
2. Implement `config show`:
   - Load merged configuration
   - Display with source annotations [default], [config], [env]
   - Format as YAML
3. Implement `config validate`:
   - Load config file
   - Check syntax
   - Validate values (types, ranges)
   - Report errors
4. Implement `config path`:
   - Show config file location
   - Create if doesn't exist

**Files to Modify:**
- `internal/tigerfs/cmd/config.go`

**Verification:**
```bash
# Show config
go run ./cmd/tigerfs config show
# Should display merged config with sources

# Validate config
go run ./cmd/tigerfs config validate
# Should show: ✓ Configuration file is valid

# Show path
go run ./cmd/tigerfs config path
# Should show: ~/.config/tigerfs/config.yaml
```

**Completion Criteria:**
- Config show displays merged config
- Config validate checks syntax
- Config path shows location
- Tests pass

---

### Task 3.6: Implement Tiger Cloud Integration

**Objective:** Support `--tiger-service-id` flag to mount Tiger Cloud services

**Steps:**
1. Open `internal/tigerfs/cmd/mount.go`
2. Add Tiger Cloud integration:
   - Check if `--tiger-service-id` flag provided
   - Execute: `tiger db connection-string --with-password --service-id=<id>`
   - Capture stdout (connection string)
   - Use connection string for mount
3. Handle errors:
   - Tiger CLI not found
   - Not authenticated
   - Service not found
   - Service not running
4. Add tests (mock tiger CLI execution)

**Files to Modify:**
- `internal/tigerfs/cmd/mount.go`

**Files to Create:**
- `internal/tigerfs/tiger/integration.go` (Tiger CLI integration)
- `internal/tigerfs/tiger/integration_test.go`

**Verification:**
```bash
# With real Tiger Cloud service (if available)
go run ./cmd/tigerfs --tiger-service-id=<your-service-id> /tmp/testmount
ls /tmp/testmount/
# Should list tables from Tiger Cloud database

# Without tiger CLI
mv $(which tiger) /tmp/tiger.bak
go run ./cmd/tigerfs --tiger-service-id=test /tmp/testmount
# Should error: tiger CLI not found in PATH
mv /tmp/tiger.bak $(which tiger)
```

**Completion Criteria:**
- Tiger service ID flag working
- Calls tiger CLI correctly
- Connection string parsed
- Error messages clear
- Tests pass

---

### Task 3.7: Example Workflows for Basic Functionality

**Objective:** Create documentation with real-world examples for basic TigerFS usage

**Steps:**
1. Create `docs/quickstart.md`
2. Write examples:
   - **Example 1:** First mount - connecting to a local PostgreSQL database
   - **Example 2:** Exploring database schema with ls and cat
   - **Example 3:** Reading data in different formats (TSV, CSV, JSON)
   - **Example 4:** Basic data modification (update a column, insert a row)
   - **Example 5:** Using grep and awk to query data
   - **Example 6:** Mounting a Tiger Cloud service
3. Each example should include:
   - Scenario description
   - Complete commands
   - Expected output
   - Explanation

**Files to Create:**
- `docs/quickstart.md`

**Verification:**
```bash
# Run through each example
# Verify commands work as documented
```

**Completion Criteria:**
- At least 6 examples documented
- Each example tested and works
- Clear explanations provided
- Output shown

---

## Phase 4: Advanced Features

### Task 4.1: Implement Index Discovery

**Objective:** Query indexes from PostgreSQL, cache metadata

**Steps:**
1. Create `internal/tigerfs/db/indexes.go`
2. Implement `GetIndexes(ctx, pool, schema, table)`:
   ```sql
   SELECT indexname, indexdef
   FROM pg_indexes
   WHERE schemaname=$1 AND tablename=$2
   ```
3. Parse index definition to extract columns
4. Distinguish single-column vs composite indexes
5. Cache index metadata

**Files to Create:**
- `internal/tigerfs/db/indexes.go`
- `internal/tigerfs/db/indexes_test.go`

**Verification:**
```bash
# Create indexes
psql -c "CREATE INDEX users_email_idx ON users(email);"
psql -c "CREATE INDEX users_name_idx ON users(last_name, first_name);"

# Test index discovery
go test -v ./internal/tigerfs/db/ -run TestGetIndexes
```

**Completion Criteria:**
- Indexes queried from pg_indexes
- Single-column indexes identified
- Composite indexes identified
- Index metadata cached
- Tests pass

---

### Task 4.2: Implement Single-Column Index Paths

**Objective:** Navigate via single-column indexes (e.g., `.email/`)

**Steps:**
1. Create `internal/tigerfs/fuse/index.go`
2. Implement index directory operations:
   - `Lookup()` detects dotfile index paths (`.email/`)
   - `ReadDir()` lists distinct values (limited to 100)
   - `Getattr()` returns directory attributes
3. Query distinct values:
   ```sql
   SELECT DISTINCT <column> FROM <table> ORDER BY <column> LIMIT 100
   ```

**Files to Create:**
- `internal/tigerfs/fuse/index.go`
- `internal/tigerfs/fuse/index_test.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount
ls /tmp/testmount/users/.email/
# Should show email addresses (up to 100)
```

**Completion Criteria:**
- Index paths navigable
- Distinct values listed
- Limited to avoid huge listings
- Tests pass

---

### Task 4.3: Implement Index-Based Queries

**Objective:** Query rows using index paths (e.g., `.email/foo@x.com/`)

**Steps:**
1. Update `internal/tigerfs/fuse/index.go`:
   - `Lookup()` handles value within index path
   - Query row using index column:
     ```sql
     SELECT * FROM <table> WHERE <index_col>=$1
     ```
   - If single row: behave like row access
   - If multiple rows: return directory with PKs
2. Verify PostgreSQL uses index (EXPLAIN plan)

**Files to Modify:**
- `internal/tigerfs/fuse/index.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Access via index
cat /tmp/testmount/users/.email/test1@example.com/id
# Should show: 1

# Verify index used
psql -c "EXPLAIN SELECT * FROM users WHERE email='test1@example.com';"
# Should show: Index Scan using users_email_idx
```

**Completion Criteria:**
- Index-based queries working
- Single row results accessible
- Multiple row results listed
- PostgreSQL uses index
- Tests pass

---

### Task 4.4: Implement Composite Index Paths

**Objective:** Navigate composite indexes (e.g., `.last_name.first_name/Smith/Johnson/`)

**Steps:**
1. Update `internal/tigerfs/fuse/index.go`:
   - Handle composite index paths with multiple components
   - Parse path segments as index values
   - Generate WHERE clause with multiple conditions:
     ```sql
     SELECT * FROM <table>
     WHERE col1=$1 AND col2=$2
     ```
2. Support prefix matching:
   ```sql
   -- .last_name.first_name/Smith/
   SELECT DISTINCT first_name FROM <table> WHERE last_name='Smith'
   ```

**Files to Modify:**
- `internal/tigerfs/fuse/index.go`

**Verification:**
```bash
# Create composite index
psql -c "ALTER TABLE users ADD COLUMN last_name TEXT, ADD COLUMN first_name TEXT;"
psql -c "UPDATE users SET last_name='Smith', first_name='John' WHERE id=1;"
psql -c "CREATE INDEX users_name_idx ON users(last_name, first_name);"

go run ./cmd/tigerfs postgres://... /tmp/testmount

# Navigate composite index
ls /tmp/testmount/users/.last_name.first_name/Smith/
# Should show: John

cat /tmp/testmount/users/.last_name.first_name/Smith/John/email
# Should show email for that user
```

**Completion Criteria:**
- Composite index paths work
- Multi-level navigation functional
- Prefix matching supported
- Tests pass

---

### Task 4.5: Implement Large Table Detection

**Objective:** Detect large tables, enforce dir_listing_limit limit

**Steps:**
1. Update `internal/tigerfs/db/schema.go`:
   - Implement `GetTableRowCount(ctx, pool, schema, table)`:
     ```sql
     SELECT reltuples::bigint
     FROM pg_class
     WHERE relname=$1
     ```
   - Use estimate for speed
2. Update `internal/tigerfs/fuse/table.go`:
   - In `ReadDir()`, check row count before listing
   - If count > dir_listing_limit: return EIO
   - Log helpful message: "Table too large (N rows). Use .first/, .last/, .sample/, or index paths"

**Files to Modify:**
- `internal/tigerfs/db/schema.go`
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
# Create large table
psql -c "INSERT INTO users (email) SELECT 'user'||i||'@example.com' FROM generate_series(1, 15000) i;"

go run ./cmd/tigerfs postgres://... /tmp/testmount
ls /tmp/testmount/users/
# Should fail with: Input/output error
# Log should suggest: Use .first/, .last/, .sample/, or index paths
```

**Completion Criteria:**
- Large tables detected
- dir_listing_limit enforced
- Helpful error message logged
- Tests pass

---

### Task 4.6: Implement .all/ Escape Hatch for Large Tables

**Objective:** Allow users to explicitly bypass dir_listing_limit limit via .all/ path

**Steps:**
1. Create `internal/tigerfs/fuse/all.go`:
   - Implement `AllRowsNode` struct
   - Implement `Readdir()` that:
     - Checks row count estimate from cache
     - Logs Warn if table exceeds dir_listing_limit
     - Lists ALL rows without limit
   - Implement `Lookup()` for accessing rows within .all/
2. Add `ListAllRows()` to `internal/tigerfs/db/keys.go`:
   - Same as `ListRows()` but without LIMIT clause
3. Update `internal/tigerfs/fuse/table.go`:
   - Add `.all` directory entry in `Readdir()`
   - Handle `.all` lookup in `Lookup()`

**Files to Create:**
- `internal/tigerfs/fuse/all.go`
- `internal/tigerfs/fuse/all_test.go`

**Files to Modify:**
- `internal/tigerfs/db/keys.go`
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Test .all/ on small table (no warning)
ls /tmp/testmount/users/.all/
# Should list all rows

# Test .all/ on large table (warning in logs)
ls /tmp/testmount/large_table/.all/
# Log should show: Warn: Listing all rows for large table 'large_table' (~N rows) via .all/ path

# Access row within .all/
cat /tmp/testmount/users/.all/1.json
# Should return row data
```

**Completion Criteria:**
- `.all/` directory listed in table directory
- `.all/` bypasses dir_listing_limit limit
- Warn logged for large tables
- Rows accessible within .all/
- Tests pass

---

### Task 4.7: Implement .first/N/ and .last/N/ Pagination

**Objective:** Access first N or last N rows via .first/N/ and .last/N/ paths

**Steps:**
1. Update `internal/tigerfs/fuse/table.go`:
   - Detect `.first/` in path
   - Parse N from path (e.g., `.first/100/`)
   - Implement `ReadDir()` for `.first/N/`:
     ```sql
     SELECT <pk> FROM <table> ORDER BY <pk> ASC LIMIT $1
     ```
   - List first N PKs
   - Detect `.last/` in path
   - Parse N from path (e.g., `.last/100/`)
   - Implement `ReadDir()` for `.last/N/`:
     ```sql
     SELECT <pk> FROM <table> ORDER BY <pk> DESC LIMIT $1
     ```
   - List last N PKs (in descending order by PK)

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Test .first/N/
ls /tmp/testmount/users/.first/50/
# Should show first 50 PKs

cat /tmp/testmount/users/.first/50/1/email
# Should access row normally

# Test .last/N/
ls /tmp/testmount/users/.last/50/
# Should show last 50 PKs (highest IDs)

cat /tmp/testmount/users/.last/50/14999/email
# Should access row normally
```

**Completion Criteria:**
- `.first/N/` paths work
- `.last/N/` paths work
- Returns first/last N rows by PK
- Rows accessible normally
- Tests pass

---

### Task 4.8: Implement .sample/N/ Random Sampling

**Objective:** Access random N rows via .sample/N/ path

**Steps:**
1. Update `internal/tigerfs/fuse/table.go`:
   - Detect `.sample/` in path
   - Parse N from path (e.g., `.sample/100/`)
   - Implement `ReadDir()` for `.sample/N/`:
     ```sql
     SELECT <pk> FROM <table> TABLESAMPLE BERNOULLI($1) LIMIT $2
     ```
   - Calculate percentage to approximate N rows
2. Handle small tables (fallback to ORDER BY RANDOM())

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount
ls /tmp/testmount/users/.sample/100/
# Should show ~100 random PKs (may vary)

# Run again, should get different sample
ls /tmp/testmount/users/.sample/100/
# Different PKs
```

**Completion Criteria:**
- `.sample/N/` paths work
- Returns approximately N random rows
- Uses TABLESAMPLE for performance
- Tests pass

---

### Task 4.9: Implement .count File

**Objective:** Show table row count in .count file

**Steps:**
1. Update `internal/tigerfs/fuse/metadata.go`:
   - Detect `.count` file
   - Implement `Read()` for `.count`:
     - For small tables (<100K): `SELECT count(*) FROM <table>`
     - For large tables: Use pg_class.reltuples estimate
   - Return count as text

**Files to Modify:**
- `internal/tigerfs/fuse/metadata.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount
cat /tmp/testmount/users/.count
# Should show: 15000 (or current count)
```

**Completion Criteria:**
- `.count` file readable
- Accurate for small tables
- Fast estimate for large tables
- Tests pass

---

### Task 4.10: Implement Permission Discovery

**Objective:** Query PostgreSQL table privileges

**Steps:**
1. Create `internal/tigerfs/db/permissions.go`
2. Implement `GetTablePermissions(ctx, pool, schema, table)`:
   ```sql
   SELECT
     has_table_privilege('<schema>.<table>', 'SELECT') as can_select,
     has_table_privilege('<schema>.<table>', 'INSERT') as can_insert,
     has_table_privilege('<schema>.<table>', 'UPDATE') as can_update,
     has_table_privilege('<schema>.<table>', 'DELETE') as can_delete
   ```
3. Cache permissions per table

**Files to Create:**
- `internal/tigerfs/db/permissions.go`
- `internal/tigerfs/db/permissions_test.go`

**Verification:**
```bash
# Create read-only user
psql -c "CREATE USER readonly_user WITH PASSWORD 'test';"
psql -c "GRANT SELECT ON users TO readonly_user;"

# Test permission query
go test -v ./internal/tigerfs/db/ -run TestGetTablePermissions
```

**Completion Criteria:**
- Permissions queried correctly
- All 4 privileges checked
- Permissions cached
- Tests pass

---

### Task 4.11: Implement Permission Mapping

**Objective:** Map PostgreSQL privileges to filesystem permissions

**Steps:**
1. Create `internal/tigerfs/util/permissions.go`
2. Implement `MapPermissions(canSelect, canUpdate, canInsert, canDelete bool) os.FileMode`:
   - SELECT → read (0400)
   - UPDATE/INSERT → write (0200)
   - Combine appropriately
3. Update `internal/tigerfs/fuse/row.go` and `column.go`:
   - Use permissions in `Getattr()`
   - Return appropriate mode bits

**Files to Create:**
- `internal/tigerfs/util/permissions.go`
- `internal/tigerfs/util/permissions_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/fuse/column.go`

**Verification:**
```bash
# Test with read-only user
go run ./cmd/tigerfs postgres://readonly_user:test@localhost/postgres /tmp/testmount

ls -l /tmp/testmount/users/1
# Should show: -r--r--r-- (read-only)

echo 'test' > /tmp/testmount/users/1/email
# Should fail: Permission denied
```

**Completion Criteria:**
- Permissions mapped correctly
- File modes accurate
- Write attempts fail for read-only
- Tests pass

---

### Task 4.12: Implement File Sizes (Complete)

**Status:** Complete

**Objective:** Calculate and return accurate file sizes

**Implementation Notes:**
- `internal/tigerfs/fuse/row.go:93` returns `len(r.data)` for row file sizes
- `internal/tigerfs/fuse/column.go:97` returns `len(c.data)` for column file sizes
- Sizes are calculated from the actual data content after fetching
- `Lookup` methods must populate `out.Attr` so the kernel caches correct sizes:
  - `rowdir.go`: Column files and format files (.json, .csv, .tsv)
  - `table.go`: Row files and metadata files (.columns, .schema, .count)

**Completion Criteria:**
- [x] File sizes accurate
- [x] Shown in ls -lh
- [x] Sizes derived from data content
- [x] Lookup methods populate out.Attr for correct caching
- [x] Tests pass

---

### Task 4.13: Implement Schema Flattening

**Objective:** Show public schema tables at root, other schemas with prefix

**Steps:**
1. Update `internal/tigerfs/fuse/root.go`:
   - In `ReadDir()`, list tables from default schema (config.DefaultSchema)
   - Also show other schemas as directories
   - Add `.schemas/` directory for explicit access
2. Update path parsing to handle schema prefixes:
   - `/users/` → public.users
   - `/analytics/reports/` → analytics.reports
   - `/.schemas/public/users/` → public.users (explicit)
3. Make default schema configurable via config

**Files to Modify:**
- `internal/tigerfs/fuse/root.go`
- Path parsing logic in various files

**Verification:**
```bash
# Create table in different schema
psql -c "CREATE SCHEMA analytics;"
psql -c "CREATE TABLE analytics.reports (id serial);"

go run ./cmd/tigerfs postgres://... /tmp/testmount
ls /tmp/testmount/
# Should show: users (from public), analytics (schema directory)

ls /tmp/testmount/analytics/
# Should show: reports

ls /tmp/testmount/.schemas/
# Should show: public, analytics

ls /tmp/testmount/.schemas/public/
# Should show: users
```

**Completion Criteria:**
- Public schema tables at root
- Other schemas accessible with prefix
- `.schemas/` provides explicit access
- Configurable default schema
- Tests pass

---

### Task 4.14: Support Non-SERIAL Primary Keys

**Objective:** Support tables with primary keys that aren't SERIAL/auto-incrementing integers

**Steps:**
1. Update `internal/tigerfs/db/keys.go`:
   - Handle UUID primary keys
   - Handle text/varchar primary keys
   - Handle composite primary keys (with configurable delimiter)
   - Detect PK data type from information_schema
2. Update `internal/tigerfs/fuse/table.go`:
   - Parse PK values according to detected type
   - Handle URL-safe encoding for special characters in PKs
3. Update `internal/tigerfs/fuse/row.go`:
   - Generate correct WHERE clauses for non-integer PKs
4. Add configuration option for composite PK delimiter (default: `_`)

**Files to Modify:**
- `internal/tigerfs/db/keys.go`
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/row.go`

**Verification:**
```bash
# Create table with UUID PK
psql -c "CREATE TABLE documents (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title TEXT);"
psql -c "INSERT INTO documents (title) VALUES ('Doc 1'), ('Doc 2');"

go run ./cmd/tigerfs postgres://... /tmp/testmount
ls /tmp/testmount/documents/
# Should show UUIDs

# Create table with text PK
psql -c "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);"
psql -c "INSERT INTO settings VALUES ('theme', 'dark'), ('language', 'en');"

ls /tmp/testmount/settings/
# Should show: theme, language

cat /tmp/testmount/settings/theme/value
# Should show: dark
```

**Completion Criteria:**
- UUID primary keys work
- Text primary keys work
- Composite primary keys work with delimiter
- Special characters handled (URL encoding)
- Tests pass

---

### Task 4.15: Support Tables Without Primary Keys

**Objective:** Allow read-only access to tables without primary keys using ctid

**Steps:**
1. Update `internal/tigerfs/db/keys.go`:
   - Detect tables without primary keys
   - Fall back to using `ctid` (physical row identifier)
   - Warn user that ctid-based access is unstable (changes after VACUUM)
2. Update `internal/tigerfs/fuse/table.go`:
   - List rows using ctid when no PK exists
   - Mark table directory as read-only in Getattr
3. Update `internal/tigerfs/fuse/row.go`:
   - Query rows by ctid
   - Disable write operations for ctid-based access
4. Add `.no_pk_warning` metadata file explaining the limitation

**Files to Modify:**
- `internal/tigerfs/db/keys.go`
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/fuse/metadata.go`

**Verification:**
```bash
# Create table without PK
psql -c "CREATE TABLE logs (message TEXT, created_at TIMESTAMP);"
psql -c "INSERT INTO logs VALUES ('Log 1', NOW()), ('Log 2', NOW());"

go run ./cmd/tigerfs postgres://... /tmp/testmount
ls /tmp/testmount/logs/
# Should show ctid-based identifiers (e.g., 0_1, 0_2)

cat /tmp/testmount/logs/0_1
# Should show row data

cat /tmp/testmount/logs/.no_pk_warning
# Should explain ctid limitations

# Write should fail
echo '{"message":"test"}' > /tmp/testmount/logs/0_1.json
# Should fail: Read-only table (no primary key)
```

**Completion Criteria:**
- Tables without PK are accessible (read-only)
- ctid used as row identifier
- Warning file explains limitations
- Write operations blocked
- Tests pass

---

### Task 4.16: Support Database Views

**Objective:** Expose PostgreSQL views alongside tables with appropriate read/write behavior

**Background:** Views are specified in spec.md to be treated identically to tables for reading.
This includes JOIN views (e.g., `CREATE VIEW user_orders AS SELECT ... FROM users JOIN orders ...`).
Updatable views support writes (PostgreSQL handles); non-updatable views return EACCES.

**Key Design Points:**
- Simple single-table views may have a primary key and be updatable
- JOIN views typically have no primary key, so they:
  - Use `ctid` for row identification (like Task 4.15)
  - Are read-only (JOINs are generally non-updatable)
  - Can be browsed via `.first/N/` or `.sample/N/` paths

**Steps:**
1. Update `internal/tigerfs/db/schema.go`:
   - Modify `GetTables()` to include views, or add separate `GetViews()` function
   - Add query to determine if a view is updatable:
     ```sql
     SELECT is_updatable FROM information_schema.views
     WHERE table_schema = $1 AND table_name = $2
     ```
   - Add function to get view definition for `.schema` file
2. Update `internal/tigerfs/fuse/root.go`:
   - Include views in table listing
3. Handle write operations:
   - Updatable views: writes work (PostgreSQL handles)
   - Non-updatable views: return EACCES with clear error message
4. Ensure JOIN views work:
   - No PK → falls back to ctid-based access (Task 4.15)
   - Read-only browsing via `.first/N/`, `.sample/N/`

**Files to Modify:**
- `internal/tigerfs/db/schema.go`
- `internal/tigerfs/db/schema_test.go`
- `internal/tigerfs/fuse/root.go`

**Verification:**
```bash
# Create a simple view
psql -c "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;"

# Create a JOIN view
psql -c "CREATE VIEW user_orders AS SELECT u.id as user_id, u.name, o.id as order_id, o.total FROM users u JOIN orders o ON u.id = o.user_id;"

go run ./cmd/tigerfs postgres://... /tmp/testmount

# Views should appear in listing
ls /tmp/testmount
# Should show: active_users/  user_orders/  users/  orders/  ...

# Read from simple view (has PK from underlying table)
cat /tmp/testmount/active_users/1.json

# Read from JOIN view (no PK, use .first/N/)
ls /tmp/testmount/user_orders/.first/10/
cat /tmp/testmount/user_orders/.first/10/1.json

# Check view schema shows CREATE VIEW
cat /tmp/testmount/user_orders/.schema
# Should show: CREATE VIEW user_orders AS SELECT ...

# Test write to JOIN view (should fail - non-updatable)
echo '{"name":"Test"}' > /tmp/testmount/user_orders/.first/10/1.json
# Should fail with EACCES
```

**Completion Criteria:**
- Views discovered alongside tables
- Simple views readable (with PK if available)
- JOIN views readable via .first/N/ or .sample/N/
- Updatable views support writes
- Non-updatable views (including JOINs) return EACCES on write
- View definition shown in .schema
- Tests include JOIN view scenario
- Tests pass

---

### Task 4.17: Support TimescaleDB Hypertables

**Objective:** Proper support for TimescaleDB hypertables with time-based access

**Steps:**
1. Create `internal/tigerfs/db/timescale.go`:
   - Detect TimescaleDB extension
   - Detect hypertables: `SELECT * FROM timescaledb_information.hypertables`
   - Get time column for hypertable
   - Get chunk information
2. Update `internal/tigerfs/fuse/table.go`:
   - For hypertables, add `.chunks/` virtual directory
   - Add time-based access paths: `.time/2026-01-01/` to `.time/2026-01-31/`
3. Create `internal/tigerfs/fuse/hypertable.go`:
   - Implement time-range navigation
   - Implement chunk-based navigation
   - Support continuous aggregates as virtual tables

**Files to Create:**
- `internal/tigerfs/db/timescale.go`
- `internal/tigerfs/db/timescale_test.go`
- `internal/tigerfs/fuse/hypertable.go`
- `internal/tigerfs/fuse/hypertable_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
# Create hypertable (requires TimescaleDB)
psql -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
psql -c "CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device_id INT, value DOUBLE PRECISION);"
psql -c "SELECT create_hypertable('metrics', 'time');"
psql -c "INSERT INTO metrics SELECT generate_series('2026-01-01'::timestamptz, '2026-01-10'::timestamptz, '1 hour'), 1, random();"

go run ./cmd/tigerfs postgres://... /tmp/testmount

# Access via time range
ls /tmp/testmount/metrics/.time/
# Should show date ranges

ls /tmp/testmount/metrics/.time/2026-01-01/
# Should show rows from that day

# Access via chunks
ls /tmp/testmount/metrics/.chunks/
# Should show chunk names
```

**Completion Criteria:**
- Hypertables detected
- Time-based navigation works
- Chunk navigation works
- Continuous aggregates accessible
- Tests pass

---

### Task 4.18: Example Workflows for Advanced Features

**Objective:** Create documentation with real-world examples for advanced TigerFS features

**Steps:**
1. Create `docs/examples-advanced.md`
2. Write examples:
   - **Example 1:** Navigating large tables with .first/, .last/, .sample/
   - **Example 2:** Index-based lookups for efficient queries
   - **Example 3:** Working with composite indexes
   - **Example 4:** Accessing tables without primary keys
   - **Example 5:** Working with UUID and text primary keys
   - **Example 6:** TimescaleDB hypertable time-based navigation
   - **Example 7:** Multi-schema database navigation
   - **Example 8:** Permission-based access patterns
3. Each example should include:
   - Scenario description
   - Complete commands
   - Expected output
   - Explanation

**Files to Create:**
- `docs/examples-advanced.md`

**Verification:**
```bash
# Run through each example
# Verify commands work as documented
```

**Completion Criteria:**
- At least 8 examples documented
- Each example tested and works
- Clear explanations provided
- Output shown

---

### Task 4.19: Synthesize Filename Extensions from Column Types

**Objective:** Automatically add file extensions to column files based on PostgreSQL data type

**Background:** When Claude Code (or users) interact with TigerFS, file extensions provide immediate context about content type and enable proper syntax highlighting/tooling. Extensions should be synthesized on-demand based on the column's PostgreSQL type.

**Type → Extension Mapping:**

| PostgreSQL Type | Extension |
|-----------------|-----------|
| `TEXT`, `VARCHAR`, `CHAR` | `.txt` |
| `JSON`, `JSONB` | `.json` |
| `XML` | `.xml` |
| `BYTEA` | `.bin` |
| `GEOMETRY`, `GEOGRAPHY` (PostGIS) | `.wkb` |
| Arrays (all types, including pgvector) | (none) |
| Everything else (numbers, dates, booleans, etc.) | (none) |

**Steps:**
1. Update `internal/tigerfs/db/query.go`:
   - Modify column metadata query to include `data_type` from `information_schema.columns`
   - Create type → extension mapping function
2. Update `internal/tigerfs/fuse/`:
   - `ReadDir()`: Append extension to column filenames based on type
   - `Lookup()`: Accept both `column` and `column.ext` forms, strip known extensions to find actual column
   - `Getattr()`: Handle extended filenames
3. Add configuration option:
   - Config field: `no_filename_extensions` (bool, default false)
   - Flag: `--no-filename-extensions`
   - Environment: `TIGERFS_NO_FILENAME_EXTENSIONS`
4. Write unit tests for extension mapping and stripping

**Files to Modify:**
- `internal/tigerfs/db/query.go`
- `internal/tigerfs/fuse/fs.go` (or relevant FUSE files)
- `internal/tigerfs/config/config.go`
- `internal/tigerfs/cmd/mount.go`

**Files to Create:**
- `internal/tigerfs/fuse/extensions.go`
- `internal/tigerfs/fuse/extensions_test.go`

**Verification:**
```bash
# Mount database
go run ./cmd/tigerfs mount postgres://... /tmp/testmount

# Check column files have extensions
ls /tmp/testmount/users/1/
# Should show: id  name.txt  email.txt  metadata.json  avatar.bin

# Read via extended name
cat /tmp/testmount/users/1/name.txt
# Should return the name value

# Test with flag disabled
go run ./cmd/tigerfs mount --no-filename-extensions postgres://... /tmp/testmount2
ls /tmp/testmount2/users/1/
# Should show: id  name  email  metadata  avatar
```

**Completion Criteria:**
- Column types queried from information_schema
- Extensions added to TEXT/VARCHAR/CHAR → .txt
- Extensions added to JSON/JSONB → .json
- Extensions added to XML → .xml
- Extensions added to BYTEA → .bin
- Extensions added to PostGIS types → .wkb
- Lookup strips extensions to find column
- --no-filename-extensions flag works
- Config file option works
- Environment variable works
- Tests pass

---

### Task 4.20: Implement .ddl Extended Schema File

**Objective:** Add a `.ddl` metadata file that shows complete table DDL including indexes, constraints, triggers, and comments.

**Background:** The existing `.schema` file shows only the CREATE TABLE statement, which is fast but incomplete. For understanding a table's full definition, users need to see indexes, foreign keys, check constraints, triggers, and documentation comments. The `.ddl` file provides this comprehensive view.

**Output Format:**
```sql
-- Table
CREATE TABLE "public"."users" (
  ...
);

-- Indexes
CREATE INDEX "users_email_idx" ON "public"."users" ("email");
CREATE UNIQUE INDEX "users_pkey" ON "public"."users" ("id");

-- Foreign Keys
ALTER TABLE "public"."orders" ADD CONSTRAINT "orders_user_id_fkey"
  FOREIGN KEY ("user_id") REFERENCES "public"."users" ("id");

-- Check Constraints
ALTER TABLE "public"."users" ADD CONSTRAINT "users_age_check"
  CHECK (age >= 0 AND age <= 150);

-- Triggers
CREATE TRIGGER "update_timestamp" BEFORE UPDATE ON "public"."users"
  FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- Comments
COMMENT ON TABLE "public"."users" IS 'User accounts';
COMMENT ON COLUMN "public"."users"."email" IS 'Primary contact email';
```

**Steps:**
1. Add database queries in `internal/tigerfs/db/schema.go`:
   - `GetIndexDDL(ctx, schema, table)` - Returns CREATE INDEX statements
   - `GetForeignKeyDDL(ctx, schema, table)` - Returns FK constraints
   - `GetCheckConstraintDDL(ctx, schema, table)` - Returns CHECK constraints
   - `GetTriggerDDL(ctx, schema, table)` - Returns CREATE TRIGGER statements
   - `GetTableComments(ctx, schema, table)` - Returns COMMENT statements
   - `GetFullDDL(ctx, schema, table)` - Combines all of the above
2. Update `internal/tigerfs/fuse/metadata.go`:
   - Add "ddl" as a new file type
   - Implement `fetchDDL()` that calls `GetFullDDL`
3. Update `internal/tigerfs/fuse/table.go`:
   - Add `.ddl` to Readdir output
   - Add `.ddl` to Lookup metadata file handling
4. Write unit tests for new db functions
5. Write integration test for `.ddl` file access

**Files to Modify:**
- `internal/tigerfs/db/schema.go`
- `internal/tigerfs/fuse/metadata.go`
- `internal/tigerfs/fuse/table.go`

**Files to Create:**
- None (tests go in existing `*_test.go` files)

**Verification:**
```bash
# Mount database
go run ./cmd/tigerfs mount postgres://... /tmp/testmount

# View simple schema (fast)
cat /tmp/testmount/users/.schema

# View full DDL (comprehensive)
cat /tmp/testmount/users/.ddl

# Verify sections present
cat /tmp/testmount/users/.ddl | grep -E "^-- (Table|Indexes|Foreign Keys|Triggers|Comments)"
```

**Completion Criteria:**
- `.ddl` file appears in table directory listing
- CREATE TABLE statement included
- CREATE INDEX statements included
- Foreign key constraints included
- Check constraints included
- Trigger definitions included
- Table/column comments included (if any exist)
- Sections omitted if empty (no "-- Indexes" header if no indexes)
- Tests pass

---

### Task 4.21: Implement .indexes Metadata File

**Objective:** Add a `.indexes` metadata file that lists available index navigation paths for quick discovery.

**Background:** Currently, discovering what indexes are available requires either `ls -la` and scanning for dot-prefixed directories, or reading the full `.ddl` file. A simple `.indexes` file provides instant visibility into available index paths.

**Output Format:**
```
.email/                    (unique)
.last_name.first_name/     (composite)
.created_at/
```

Format: `.<index_path>/` followed by annotations in parentheses:
- `(unique)` for unique indexes
- `(composite)` for multi-column indexes
- No annotation for regular single-column indexes

**Steps:**
1. Add `fetchIndexes()` method to `internal/tigerfs/fuse/metadata.go`
2. Update `fetchData()` switch to handle "indexes" file type
3. Update `internal/tigerfs/fuse/table.go`:
   - Add `.indexes` to Readdir output
   - Add `.indexes` to Lookup metadata file handling
   - Add `.indexes` to Unlink protection

**Files to Modify:**
- `internal/tigerfs/fuse/metadata.go`
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
# Mount database
go run ./cmd/tigerfs mount postgres://... /tmp/testmount

# List available indexes
cat /tmp/testmount/users/.indexes

# Should show something like:
# .email/                    (unique)
# .created_at/
```

**Completion Criteria:**
- `.indexes` file appears in table directory listing
- Lists all non-primary-key index directories
- Shows `(unique)` annotation for unique indexes
- Shows `(composite)` annotation for multi-column indexes
- Excludes primary key (rows already accessible by PK)
- Tests pass

---

### Task 4.22: Add Pagination to Index Navigation

**Objective:** Support `.first/N/` and `.last/N/` within index directories for ordered access.

**Background:** Indexes are often used with ORDER BY and LIMIT for efficient ordered access. Currently, `.first/N/` and `.last/N/` only work at the table level. Adding them to index navigation enables patterns like "first 10 timestamps" or "last 5 rows matching this category".

**Supported Paths:**

Single-column indexes:
```
.column/.first/N/          # First N distinct values (ordered by column)
.column/.last/N/           # Last N distinct values
.column/value/.first/N/    # First N rows matching value (ordered by PK)
.column/value/.last/N/     # Last N rows matching value
```

Composite indexes:
```
.col1.col2/.first/N/           # First N distinct col1 values
.col1.col2/.last/N/            # Last N distinct col1 values
.col1.col2/val1/.first/N/      # First N distinct col2 values where col1=val1
.col1.col2/val1/val2/.first/N/ # First N rows matching both conditions
```

**Example Usage:**
```bash
# First 10 order timestamps
ls /mnt/db/orders/.created_at/.first/10/

# Last 5 orders for user 42
ls /mnt/db/orders/.user_id/42/.last/5/

# First 10 users with last name "Smith"
ls /mnt/db/users/.last_name.first_name/Smith/.first/10/
```

**Steps:**
1. Update `IndexNode` to handle `.first` and `.last` lookups
2. Update `IndexValueNode` to handle `.first` and `.last` lookups
3. Update `CompositeIndexNode` to handle `.first` and `.last` lookups
4. Update `CompositeIndexLevelNode` to handle `.first` and `.last` lookups
5. Update db queries to support ORDER BY ASC/DESC with LIMIT

**Files to Modify:**
- `internal/tigerfs/fuse/index.go`
- `internal/tigerfs/db/indexes.go` (may need new query variants)

**Verification:**
```bash
# Mount and test
ls /mnt/db/orders/.created_at/.first/5/     # First 5 timestamps
ls /mnt/db/orders/.created_at/.last/5/      # Last 5 timestamps
ls /mnt/db/orders/.user_id/1/.first/3/      # First 3 orders for user 1
cat /mnt/db/orders/.user_id/1/.first/3/*.json  # Read them
```

**Completion Criteria:**
- `.first/N/` works in IndexNode (distinct values)
- `.last/N/` works in IndexNode (distinct values)
- `.first/N/` works in IndexValueNode (matching rows)
- `.last/N/` works in IndexValueNode (matching rows)
- Works with composite indexes at each level
- Tests pass

---

## Phase 5: DDL Operations via Filesystem

### Overview

Add ability to create, modify, and delete tables, indexes, schemas, and views via TigerFS filesystem operations using a unified staging pattern.

**Core Pattern:** All DDL operations follow the same flow:
1. **Read `.schema`** → see dynamically-generated template
2. **Write `.schema`** → stage DDL (stored in daemon memory)
3. **Touch `.test`** → validate via BEGIN/ROLLBACK (optional)
4. **Touch `.commit`** → execute, or **`.abort`** → cancel

---

### Task 5.1: Implement Core Staging Infrastructure

**Objective:** Create the foundational staging system for DDL operations

**Background:**
All DDL operations share common infrastructure: staging tracker (in-memory storage), control files (`.schema`, `.test`, `.commit`, `.abort`), and template generation. This task builds that foundation.

**Steps:**
1. Create `internal/tigerfs/fuse/staging.go`:
   - Define `StagingTracker` struct with mutex-protected map:
     ```go
     type StagingTracker struct {
         mu      sync.RWMutex
         entries map[string]*StagingEntry  // keyed by path
     }

     type StagingEntry struct {
         Content    string    // User-provided DDL
         TestResult string    // Last test result
         CreatedAt  time.Time
     }
     ```
   - Implement `GetOrCreate(path string) *StagingEntry`
   - Implement `Get(path string) *StagingEntry` (returns nil if not exists)
   - Implement `Set(path string, content string)`
   - Implement `Delete(path string)`
   - Implement `HasContent(path string) bool` (checks for non-empty, non-comment content)

2. Create `internal/tigerfs/fuse/staging_node.go`:
   - Define `StagingDirNode` for staging directories (`.create/<name>/`, `.modify/`, `.delete/`)
   - Implement `Readdir()` to list control files: `.schema`, `.test`, `.commit`, `.abort`
   - Implement `Lookup()` for control files
   - Implement `Mkdir()` for creating new staging entries (e.g., `mkdir .create/orders`)

3. Create `internal/tigerfs/fuse/control_files.go`:
   - Define `SchemaFileNode` for `.schema` files:
     - `Read()`: Return staged content if exists, else generate template
     - `Write()`: Store content in StagingTracker
     - `Getattr()`: Mode 0644 (writable)
   - Define `TestFileNode` for `.test` files:
     - `Open()` with O_WRONLY or `Setattr()` (touch): Execute BEGIN/DDL/ROLLBACK
     - `Read()`: Return last test result
     - Return error if `.schema` is empty/commented
   - Define `CommitFileNode` for `.commit` files:
     - `Open()` with O_WRONLY or `Setattr()` (touch): Execute DDL
     - Return error if `.schema` is empty/commented
     - Clear staging entry on success
   - Define `AbortFileNode` for `.abort` files:
     - `Open()` with O_WRONLY or `Setattr()` (touch): Clear staging entry

4. Implement `db.Exec()` in `internal/tigerfs/db/client.go`:
   - Remove stub implementation
   - Execute DDL using `pool.Exec(ctx, sql)`
   - Return error from PostgreSQL if DDL fails

5. Implement `db.ExecInTransaction()` for testing:
   - `BEGIN`
   - Execute DDL
   - `ROLLBACK`
   - Return success/error without persisting

6. Add helper to detect empty/commented DDL:
   ```go
   func IsEmptyOrCommented(content string) bool {
       // Strip comments (-- and /* */), trim whitespace
       // Return true if nothing remains
   }
   ```

**Files to Create:**
- `internal/tigerfs/fuse/staging.go`
- `internal/tigerfs/fuse/staging_node.go`
- `internal/tigerfs/fuse/control_files.go`
- `internal/tigerfs/fuse/staging_test.go`

**Files to Modify:**
- `internal/tigerfs/db/client.go`
- `internal/tigerfs/fuse/fs.go` (add StagingTracker to FS struct)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v -run TestStaging
go test ./internal/tigerfs/db/... -v -run TestExec
```

**Completion Criteria:**
- StagingTracker stores/retrieves content by path
- Control file nodes handle read/write correctly
- `db.Exec()` executes DDL
- `db.ExecInTransaction()` tests DDL without persisting
- Empty/commented detection works
- Unit tests pass

---

### Task 5.2: Implement Template Generation Framework

**Objective:** Generate context-aware DDL templates for each operation type

**Steps:**
1. Create `internal/tigerfs/fuse/templates.go`:
   - Define template generator interface:
     ```go
     type TemplateGenerator interface {
         Generate(ctx context.Context) (string, error)
     }
     ```

2. Implement `CreateTableTemplate`:
   ```go
   func (g *CreateTableTemplate) Generate(ctx context.Context) (string, error) {
       return fmt.Sprintf(`CREATE TABLE %s (
       -- add columns here
       -- id SERIAL PRIMARY KEY,
       -- name TEXT NOT NULL,
   );`, g.tableName), nil
   }
   ```

3. Implement `ModifyTableTemplate`:
   - Fetch current schema via `db.GetTableDDL()`
   - Format as comment block
   - Add ALTER TABLE examples and stub:
   ```
   -- Current schema:
   -- CREATE TABLE users (
   --     id serial PRIMARY KEY,
   --     name text NOT NULL
   -- );

   -- Examples:
   -- ADD COLUMN column_name type
   -- DROP COLUMN column_name
   -- ALTER COLUMN column_name TYPE new_type

   ALTER TABLE users
   ```

4. Implement `DeleteTableTemplate`:
   - Fetch table info: column count, row count estimate
   - Fetch FK references via new `db.GetReferencingForeignKeys()`
   - Format with/without CASCADE based on dependencies:

   **No dependencies:**
   ```
   -- Table: users
   -- Columns: id, name, email
   -- Rows: ~42

   -- Uncomment to delete:
   -- DROP TABLE users;
   ```

   **With dependencies:**
   ```
   -- Table: users
   -- Columns: id, name, email
   -- Rows: ~42

   -- Foreign keys referencing this table:
   --   orders.user_id -> users.id (3,847 rows)

   -- Uncomment to delete:
   -- DROP TABLE users RESTRICT;
   -- DROP TABLE users CASCADE;
   ```

5. Implement `CreateIndexTemplate`:
   ```
   CREATE INDEX idx_name ON table_name (
       -- column(s) here
   );
   ```

6. Implement `DeleteIndexTemplate`:
   - Fetch index info via `db.GetIndexInfo()`
   ```
   -- Index: email_idx
   -- Table: users
   -- Columns: email
   -- Type: btree, unique

   -- Uncomment to delete:
   -- DROP INDEX email_idx;
   ```

7. Implement `CreateSchemaTemplate`:
   ```
   CREATE SCHEMA schema_name;
   ```

8. Implement `DeleteSchemaTemplate`:
   - Fetch table count in schema
   - With/without CASCADE based on contents

9. Implement `CreateViewTemplate`:
   ```
   CREATE VIEW view_name AS
   SELECT
       -- columns
   FROM
       -- table(s)
   WHERE
       -- conditions
   ;
   ```

10. Implement `DeleteViewTemplate`:
    - Fetch view definition
    - Check for dependent views

**Files to Create:**
- `internal/tigerfs/fuse/templates.go`
- `internal/tigerfs/fuse/templates_test.go`

**Files to Modify:**
- `internal/tigerfs/db/schema.go` (add `GetReferencingForeignKeys()`, `GetIndexInfo()`)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v -run TestTemplate
```

**Completion Criteria:**
- All template types generate correct DDL
- Templates include relevant context (row counts, FKs)
- CASCADE/RESTRICT shown only when dependencies exist
- Unit tests pass

---

### Task 5.3: Implement Schema Create/Delete Operations

**Objective:** Enable `mkdir .schemas/.create/name` and `.schemas/name/.delete/`

**Steps:**
1. Update `internal/tigerfs/fuse/schemas.go` (SchemasNode):
   - Add `.create/` to `Readdir()` entries
   - Add `Lookup()` handling for `.create`
   - Return new `SchemaCreateDirNode`

2. Create `SchemaCreateDirNode` in `internal/tigerfs/fuse/schema_create.go`:
   - `Readdir()`: List pending schema creations from StagingTracker
   - `Lookup(name)`: Return `StagingDirNode` for that schema
   - `Mkdir(name)`: Create staging entry, return `StagingDirNode`

3. Update `internal/tigerfs/fuse/schema.go` (SchemaNode for existing schemas):
   - Add `.delete/` to `Readdir()` entries
   - Add `.create/` to `Readdir()` entries (for creating tables in this schema)
   - Add `Lookup()` handling for `.delete` and `.create`

4. Implement schema-specific template generators

5. Wire up commit handlers:
   - Schema create: Execute `CREATE SCHEMA name`
   - Schema delete: Execute `DROP SCHEMA name [CASCADE]`

**Files to Create:**
- `internal/tigerfs/fuse/schema_create.go`

**Files to Modify:**
- `internal/tigerfs/fuse/schemas.go`
- `internal/tigerfs/fuse/schema.go`

**Verification:**
```bash
# Mount and test
mkdir /mnt/db/.schemas/.create/test_schema
cat /mnt/db/.schemas/.create/test_schema/.sql
# Shows: CREATE SCHEMA test_schema;

touch /mnt/db/.schemas/.create/test_schema/.commit
ls /mnt/db/.schemas/
# Shows: test_schema

echo "DROP SCHEMA test_schema" > /mnt/db/.schemas/test_schema/.delete/.sql
touch /mnt/db/.schemas/test_schema/.delete/.commit
ls /mnt/db/.schemas/
# test_schema gone
```

**Completion Criteria:**
- `mkdir .schemas/.create/name` creates staging
- Template shows `CREATE SCHEMA name;`
- `.commit` executes schema creation
- `.delete/` available on existing schemas
- Delete template shows dependencies if any
- Tests pass

---

### Task 5.4: Implement Index Create/Delete Operations

**Objective:** Enable `mkdir table/.indexes/.create/idx` and `table/.indexes/idx/.delete/`

**Steps:**
1. Update `internal/tigerfs/fuse/index.go` (IndexesNode - the `.indexes` directory):
   - Add `.create/` to `Readdir()` entries
   - Add `Lookup()` handling for `.create`
   - Return new `IndexCreateDirNode`

2. Create `IndexCreateDirNode`:
   - `Readdir()`: List pending index creations from StagingTracker
   - `Lookup(name)`: Return `StagingDirNode` for that index
   - `Mkdir(name)`: Create staging entry with table context

3. Update existing index nodes to include `.delete/`:
   - Add `.delete/` to index directory `Readdir()`
   - Add `Lookup()` handling for `.delete`

4. Implement index-specific template generators (pass table name for context)

5. Wire up commit handlers:
   - Index create: Execute user-provided `CREATE INDEX` DDL
   - Index delete: Execute `DROP INDEX name`

**Files to Create:**
- `internal/tigerfs/fuse/index_create.go`

**Files to Modify:**
- `internal/tigerfs/fuse/index.go`

**Verification:**
```bash
# Create index
mkdir /mnt/db/users/.indexes/.create/email_idx
cat /mnt/db/users/.indexes/.create/email_idx/.sql
# Shows template

echo "CREATE INDEX email_idx ON users(email)" > /mnt/db/users/.indexes/.create/email_idx/.sql
touch /mnt/db/users/.indexes/.create/email_idx/.test
# Exit 0

touch /mnt/db/users/.indexes/.create/email_idx/.commit
ls /mnt/db/users/.indexes/
# Shows: email_idx

# Delete index
echo "DROP INDEX email_idx" > /mnt/db/users/.indexes/email_idx/.delete/.sql
touch /mnt/db/users/.indexes/email_idx/.delete/.commit
```

**Completion Criteria:**
- `mkdir .indexes/.create/name` creates staging
- Template shows CREATE INDEX with table name
- `.commit` executes index creation
- `.delete/` available on existing indexes
- Tests pass

---

### Task 5.5: Implement Table Create Operations

**Objective:** Enable `mkdir .create/tablename` at root and schema levels

**Steps:**
1. Update `internal/tigerfs/fuse/root.go` (RootNode):
   - Add `.create/` to `Readdir()` entries
   - Add `Lookup()` handling for `.create`
   - Return `TableCreateDirNode` (creates in default schema)

2. Create `TableCreateDirNode` in `internal/tigerfs/fuse/table_create.go`:
   - Store target schema name
   - `Readdir()`: List pending table creations from StagingTracker
   - `Lookup(name)`: Return `StagingDirNode` for that table
   - `Mkdir(name)`: Create staging entry

3. Also support direct write (no mkdir):
   - Writing to `.create/tablename/.sql` should create staging entry if not exists

4. Update SchemaNode to include `.create/` for tables in that schema

5. Wire up commit handler:
   - Execute user-provided `CREATE TABLE` DDL
   - Validate DDL starts with `CREATE TABLE`

**Files to Create:**
- `internal/tigerfs/fuse/table_create.go`

**Files to Modify:**
- `internal/tigerfs/fuse/root.go`
- `internal/tigerfs/fuse/schema.go`

**Verification:**
```bash
# Create in default schema
mkdir /mnt/db/.create/orders
cat /mnt/db/.create/orders/.sql
# Shows template

echo "CREATE TABLE orders (id serial PRIMARY KEY, name text)" > /mnt/db/.create/orders/.sql
touch /mnt/db/.create/orders/.test
touch /mnt/db/.create/orders/.commit
ls /mnt/db/
# Shows: orders

# Create in specific schema
echo "CREATE TABLE foo (id serial PRIMARY KEY)" > /mnt/db/.schemas/public/.create/foo/.sql
touch /mnt/db/.schemas/public/.create/foo/.commit
```

**Completion Criteria:**
- `mkdir .create/name` creates staging at root
- `mkdir .schemas/schema/.create/name` creates staging in schema
- Direct write to `.schema` also creates staging
- Template shows CREATE TABLE scaffold
- `.commit` executes table creation
- Tests pass

---

### Task 5.6: Implement Table Modify Operations

**Objective:** Enable `table/.modify/` for ALTER TABLE operations

**Steps:**
1. Update `internal/tigerfs/fuse/table.go` (TableNode):
   - Add `.modify/` to `Readdir()` entries
   - Add `Lookup()` handling for `.modify`
   - Return `StagingDirNode` with modify context

2. Implement modify-specific template:
   - Show current schema as comment (via `db.GetTableDDL()`)
   - Show ALTER TABLE examples
   - Provide `ALTER TABLE tablename` stub

3. Wire up commit handler:
   - Execute user-provided ALTER statement(s)
   - Support multiple statements separated by semicolons

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
cat /mnt/db/users/.modify/.sql
# Shows current schema + examples + stub

echo "ALTER TABLE users ADD COLUMN status text" > /mnt/db/users/.modify/.sql
touch /mnt/db/users/.modify/.test
touch /mnt/db/users/.modify/.commit

cat /mnt/db/users/.schema
# Shows new column
```

**Completion Criteria:**
- `.modify/` directory appears in table listings
- Template shows current schema and examples
- `.commit` executes ALTER statements
- Multiple statements work
- Tests pass

---

### Task 5.7: Implement Table Delete Operations

**Objective:** Enable `table/.delete/` for DROP TABLE operations

**Steps:**
1. Update `internal/tigerfs/fuse/table.go` (TableNode):
   - Add `.delete/` to `Readdir()` entries
   - Add `Lookup()` handling for `.delete`
   - Return `StagingDirNode` with delete context

2. Implement delete-specific template:
   - Show table info (columns, row count)
   - Query for referencing foreign keys
   - Show CASCADE/RESTRICT only if dependencies exist

3. Wire up commit handler:
   - Execute user-provided DROP TABLE statement
   - Must be explicit (user writes the DROP)

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/db/schema.go` (add `GetReferencingForeignKeys()` if not done)

**Verification:**
```bash
cat /mnt/db/users/.delete/.sql
# Shows table info, FKs if any, DROP options

echo "DROP TABLE users CASCADE" > /mnt/db/users/.delete/.sql
touch /mnt/db/users/.delete/.test
touch /mnt/db/users/.delete/.commit

ls /mnt/db/
# users gone
```

**Completion Criteria:**
- `.delete/` directory appears in table listings
- Template shows table info and dependencies
- CASCADE/RESTRICT shown only when needed
- `.commit` executes DROP TABLE
- Tests pass

---

### Task 5.8: Implement View Create/Delete Operations

**Objective:** Enable `.views/.create/name` and `.views/name/.delete/`

**Steps:**
1. Create `internal/tigerfs/fuse/views.go`:
   - Define `ViewsNode` for `.views/` directory
   - `Readdir()`: List existing views + `.create/`
   - `Lookup()`: Handle `.create` and view names

2. Create `ViewCreateDirNode`:
   - `Readdir()`: List pending view creations
   - `Mkdir(name)`: Create staging entry
   - `Lookup(name)`: Return `StagingDirNode`

3. Create `ViewNode` for existing views:
   - `Readdir()`: List `.schema` (read-only), `.delete/`, row PKs
   - `Lookup()`: Handle `.schema`, `.delete`, row access
   - Make view `.schema` read-only (mode 0444)

4. Update `internal/tigerfs/fuse/root.go`:
   - Add `.views/` to `Readdir()` entries
   - Add `Lookup()` handling for `.views`

5. Implement view templates

6. Wire up commit handlers

**Files to Create:**
- `internal/tigerfs/fuse/views.go`
- `internal/tigerfs/fuse/view_node.go`

**Files to Modify:**
- `internal/tigerfs/fuse/root.go`
- `internal/tigerfs/db/schema.go` (add `GetViews()`, `GetViewDefinition()`)

**Verification:**
```bash
ls /mnt/db/.views/
# Shows existing views + .create/

mkdir /mnt/db/.views/.create/active_users
echo "CREATE VIEW active_users AS SELECT * FROM users WHERE active" > /mnt/db/.views/.create/active_users/.sql
touch /mnt/db/.views/.create/active_users/.commit

ls /mnt/db/.views/active_users/
# Shows rows from view

echo "DROP VIEW active_users" > /mnt/db/.views/active_users/.delete/.sql
touch /mnt/db/.views/active_users/.delete/.commit
```

**Completion Criteria:**
- `.views/` directory appears at root
- View creation works via staging
- Views queryable like tables
- View deletion works
- Tests pass

---

### Task 5.9: Integration Tests for DDL Operations

**Objective:** Comprehensive integration tests for all DDL operations

**Steps:**
1. Create `test/integration/ddl_test.go`:
   - Test schema create/delete cycle
   - Test table create/modify/delete cycle
   - Test index create/delete cycle
   - Test view create/delete cycle
   - Test `.test` validation (success and failure)
   - Test `.abort` clears staging
   - Test empty `.schema` returns error
   - Test CASCADE vs RESTRICT behavior

2. Test human workflow (mkdir + edit + commit)

3. Test script workflow (direct write + commit)

4. Test error cases:
   - Invalid DDL
   - Permission denied
   - Object already exists
   - Object doesn't exist

**Files to Create:**
- `test/integration/ddl_test.go`

**Verification:**
```bash
go test ./test/integration/... -v -run TestDDL
```

**Completion Criteria:**
- All DDL operations tested end-to-end
- Both human and script workflows tested
- Error cases handled correctly
- Tests pass

---

### Task 5.10: Documentation for DDL Operations

**Objective:** Document DDL operations in README and spec

**Steps:**
1. Update `README.md`:
   - Add "Schema Management" section
   - Show create/modify/delete examples for tables
   - Show index and view examples
   - Explain staging pattern (read template, write DDL, commit)

2. Update `specs/spec.md`:
   - Add "DDL Operations" section
   - Document directory structure
   - Document control files (`.schema`, `.test`, `.commit`, `.abort`)
   - Document template formats
   - Document error handling

3. Add examples to `examples/` directory:
   - `examples/ddl/create-table.sh`
   - `examples/ddl/modify-table.sh`
   - `examples/ddl/delete-table.sh`

**Files to Modify:**
- `README.md`
- `specs/spec.md`

**Files to Create:**
- `examples/ddl/create-table.sh`
- `examples/ddl/modify-table.sh`
- `examples/ddl/delete-table.sh`

**Completion Criteria:**
- README shows DDL examples
- Spec documents full DDL behavior
- Example scripts work
- Documentation clear for both humans and scripts

---

## Phase 6: Distribution & Release

### Task 6.1: Create Unix Install Script

**Objective:** Write install.sh for Unix/Linux/macOS

**Steps:**
1. Create `scripts/install.sh`
2. Implement install script:
   - Detect platform (OS + arch): `uname -s`, `uname -m`
   - Construct download URL for appropriate binary
   - Download binary: `curl -fsSL <url>`
   - Download checksum: `curl -fsSL <url>.sha256`
   - Verify SHA256: `sha256sum -c` or `shasum -a 256 -c`
   - Install to `~/.local/bin` or `~/bin`
   - Make executable: `chmod +x`
   - Check if directory in PATH, suggest export if not
3. Add retry logic and error handling
4. Follow tiger-cli install.sh patterns

**Files to Create:**
- `scripts/install.sh`

**Verification:**
```bash
# Test locally (simulate download)
bash scripts/install.sh
# Should install tigerfs to ~/.local/bin/

# Verify
~/.local/bin/tigerfs version
# Should show version
```

**Completion Criteria:**
- Detects platform correctly
- Downloads appropriate binary
- Verifies checksum
- Installs to correct location
- Provides PATH instructions
- Error handling robust

---

### Task 6.2: Create Windows Install Script

**Objective:** Write install.ps1 for Windows

**Steps:**
1. Create `scripts/install.ps1`
2. Implement install script:
   - Detect architecture: `$env:PROCESSOR_ARCHITECTURE`
   - Construct download URL for Windows binary
   - Download binary: `Invoke-WebRequest`
   - Download checksum
   - Verify SHA256: `Get-FileHash`
   - Install to user directory: `$env:LOCALAPPDATA\tigerfs`
   - Add to PATH (if not present)
3. Follow tiger-cli install.ps1 patterns

**Files to Create:**
- `scripts/install.ps1`

**Verification:**
```powershell
# Test on Windows (if available)
irm scripts/install.ps1 | iex
# Should install tigerfs

tigerfs version
# Should show version
```

**Completion Criteria:**
- Detects architecture
- Downloads Windows binary
- Verifies checksum
- Installs correctly
- Adds to PATH
- Error handling robust

---

### Task 6.3: Finalize GoReleaser Configuration

**Objective:** Complete .goreleaser.yaml for releases

**Steps:**
1. Open `.goreleaser.yaml`
2. Verify build configuration:
   - All platforms: linux, darwin, windows
   - All architectures: amd64, arm64
   - ldflags set correctly (version, build time, commit)
3. Configure archives:
   - tar.gz for Unix
   - zip for Windows
   - Include README, spec
4. Configure checksums
5. Configure release notes
6. Test locally: `goreleaser build --snapshot --clean`

**Files to Modify:**
- `.goreleaser.yaml`

**Verification:**
```bash
# Install goreleaser
go install github.com/goreleaser/goreleaser/v2@latest

# Test build
goreleaser build --snapshot --clean
# Should create binaries in dist/

ls dist/
# Should show binaries for all platforms
```

**Completion Criteria:**
- All platforms configured
- ldflags set correctly
- Archives configured
- Test build succeeds
- All platforms have binaries

---

### Task 6.4: Test Release Workflow

**Objective:** Verify GitHub Actions release workflow

**Steps:**
1. Open `.github/workflows/release.yml`
2. Verify workflow configuration:
   - Triggers on semver tags (v*.*.*)
   - Runs GoReleaser
   - Uploads to GitHub Releases
3. Create test tag: `git tag v0.0.1-test`
4. Push tag: `git push origin v0.0.1-test`
5. Monitor GitHub Actions
6. Verify release created with binaries
7. Delete test release and tag

**Files to Verify:**
- `.github/workflows/release.yml`

**Verification:**
```bash
# Create test tag
git tag v0.0.1-test
git push origin v0.0.1-test

# Check GitHub Actions
# https://github.com/timescale/tigerfs/actions

# Verify release
# https://github.com/timescale/tigerfs/releases/tag/v0.0.1-test

# Cleanup
git tag -d v0.0.1-test
git push --delete origin v0.0.1-test
# Delete release via GitHub UI
```

**Completion Criteria:**
- Workflow triggers on tag
- GoReleaser runs successfully
- Release created with all binaries
- Checksums included
- Test cleanup complete

---

### Task 6.5: Daemon Mode Support

**Objective:** Add `--daemon` flag for proper background execution with PID file management

**Background:** Currently `tigerfs mount` runs in foreground (blocking), which is correct for containers and process managers. However, for traditional deployment, a `--daemon` flag provides cleaner backgrounding than shell `&`.

**Steps:**
1. Add flags to mount command in `internal/tigerfs/cmd/mount.go`:
   - `--daemon` - Fork to background, write PID file
   - `--foreground` / `-f` - Explicit foreground (default, for clarity in scripts)
   - `--pid-file` - Custom PID file location (default: `~/.tigerfs/<mountpoint-hash>.pid`)
2. Implement daemonization in `internal/tigerfs/daemon/daemon.go`:
   - Fork process (platform-specific)
   - Detach from terminal (setsid on Unix)
   - Write PID to file
   - Redirect stdout/stderr to log file
3. Update `unmount` command to read PID file:
   - If FUSE unmount fails, can signal process via PID
   - Clean up PID file after unmount
4. Handle platform differences:
   - Unix: fork + setsid
   - Windows: Different approach or skip daemon mode
5. Update `status` command to show daemon status from PID file

**Files to Create:**
- `internal/tigerfs/daemon/daemon.go`
- `internal/tigerfs/daemon/daemon_unix.go` (build-tagged)
- `internal/tigerfs/daemon/daemon_windows.go` (stub or alternative)
- `internal/tigerfs/daemon/pidfile.go`

**Files to Modify:**
- `internal/tigerfs/cmd/mount.go`
- `internal/tigerfs/cmd/unmount.go`
- `internal/tigerfs/cmd/status.go`

**Verification:**
```bash
# Start as daemon
tigerfs mount --daemon postgres://... /mnt/db
# Should return immediately, mount in background

# Check PID file
cat ~/.tigerfs/*.pid
# Should show PID

# Verify mount
ls /mnt/db
# Should work

# Check status
tigerfs status
# Should show mount with PID

# Stop daemon
tigerfs unmount /mnt/db
# Should unmount and clean up PID file
```

**Completion Criteria:**
- `--daemon` flag forks to background
- PID file written and cleaned up
- `--foreground` flag works (explicit default)
- `unmount` cleans up PID file
- `status` shows daemon info
- Works on Linux and macOS
- Windows handled gracefully (skip or alternative)

---

### Task 6.6: Write Documentation

**Objective:** Expand README and create guides

**Steps:**
1. Expand `README.md`:
   - Complete installation section (all methods)
   - Complete usage section (all commands)
   - Add examples for common workflows
   - Add troubleshooting section
2. Create `docs/getting-started.md`:
   - Step-by-step tutorial
   - First mount, explore data, make changes
   - Examples with output
3. Create `docs/installation.md`:
   - Platform-specific instructions
   - Prerequisites (FUSE installation)
   - Verification steps
   - Troubleshooting
4. Update `CLAUDE.md` with implementation status

**Files to Modify:**
- `README.md`
- `CLAUDE.md`

**Files to Create:**
- `docs/getting-started.md`
- `docs/installation.md`

**Verification:**
```bash
# Verify documentation accuracy by following it
# Each step in getting-started.md should work

# Check for broken links
# Check formatting (markdown lint)
```

**Completion Criteria:**
- README complete and accurate
- Getting started guide clear
- Installation guide complete
- CLAUDE.md updated
- No broken links

---

### Task 6.7: Performance Testing

**Objective:** Benchmark operations, document performance

**Steps:**
1. Create `test/benchmark/performance_test.go`
2. Benchmark operations:
   - Single row read (by PK)
   - Directory listing (100, 1000 rows)
   - Index lookup
   - Row write (INSERT, UPDATE)
   - Column write
3. Compare with direct psql queries
4. Document performance characteristics in `docs/performance.md`
5. Identify optimization opportunities

**Files to Create:**
- `test/benchmark/performance_test.go`
- `docs/performance.md`

**Verification:**
```bash
# Run benchmarks
go test -bench=. ./test/benchmark/

# Should show:
# BenchmarkRowRead-8      1000    1.2 ms/op
# BenchmarkDirList-8      500     2.5 ms/op
# BenchmarkIndexLookup-8  1200    0.9 ms/op
```

**Completion Criteria:**
- Benchmarks written
- Performance documented
- Comparison with psql included
- Optimization opportunities identified

---

### Task 6.8: Bug Fixes and Polish

**Objective:** Fix remaining issues, improve UX

**Steps:**
1. Review all TODO comments in code
2. Fix critical bugs from testing
3. Improve error messages:
   - Make errors actionable
   - Include suggestions
   - Clear formatting
4. Add helpful hints to common errors
5. Clean up debug logging (remove verbose logs)
6. Final code review

**Verification:**
```bash
# Search for TODOs
grep -r "TODO" internal/

# Test common error scenarios
# Verify error messages are clear and actionable
```

**Completion Criteria:**
- No critical TODOs remain
- All known bugs fixed
- Error messages improved
- Debug logging cleaned up
- Code review complete

---

### Task 6.9: Final Testing and v0.1 Release

**Objective:** Release v0.1

**Steps:**
1. Run full test suite:
   ```bash
   go test ./...
   go test -race ./...
   go test -coverprofile=coverage.txt ./...
   ```
2. Verify coverage >80%
3. Test on all platforms (macOS, Linux, Windows if available)
4. Test install scripts on each platform
5. Update version in code
6. Create release checklist in GitHub issue
7. Tag release: `git tag v0.1.0`
8. Push tag: `git push origin v0.1.0`
9. Monitor GitHub Actions
10. Verify release artifacts
11. Write release announcement
12. Update README badges

**Verification:**
```bash
# Run all tests
go test ./...
# All pass

# Check coverage
go tool cover -func=coverage.txt | grep total
# >80%

# Create release
git tag v0.1.0
git push origin v0.1.0

# Verify release
# https://github.com/timescale/tigerfs/releases/tag/v0.1.0
```

**Completion Criteria:**
- All tests pass
- Coverage >80%
- Tested on all platforms
- v0.1.0 tag pushed
- Release published
- Binaries available
- Documentation updated
- Release announced

---

## Phase 7: Performance & Scalability

### Task 7.1: Implement Hybrid Metadata Caching

**Objective:** Optimize metadata caching for databases with many tables (100s-1000s)

**Background:**
The current cache fetches all table metadata (row counts, permissions) at once during refresh. This is expensive for databases with many tables. A hybrid approach fetches metadata eagerly for small databases but lazily for large ones.

**Steps:**
1. Add new config option to `internal/tigerfs/config/config.go`:
   - `MetadataPreloadLimit int` with default of 100
   - Tables at or below this count: eager fetch all metadata
   - Tables above this count: lazy fetch per-table on first access

2. Update `internal/tigerfs/fuse/cache.go`:
   - Add per-table timestamp tracking for lazy-cached entries:
     ```go
     type cachedEntry[T any] struct {
         value     T
         fetchedAt time.Time
     }
     ```
   - Modify `tableRowCounts` and `tablePermissions` to use `cachedEntry`
   - Update `Refresh()` to check table count against threshold:
     - If `len(tables) <= cfg.MetadataPreloadLimit`: eager fetch (current behavior)
     - If `len(tables) > cfg.MetadataPreloadLimit`: skip, let getters fetch lazily
   - Update `GetRowCountEstimate()` to fetch on-demand if entry missing/stale
   - Update `GetTablePermissions()` to fetch on-demand if entry missing/stale

3. Add tests in `internal/tigerfs/fuse/cache_test.go`:
   - Test eager behavior when table count <= threshold
   - Test lazy behavior when table count > threshold
   - Test per-table TTL expiration

4. Document in `specs/spec.md`:
   - Add `metadata_preload_limit` to Configuration System section

**Files to Modify:**
- `internal/tigerfs/config/config.go`
- `internal/tigerfs/fuse/cache.go`
- `internal/tigerfs/fuse/cache_test.go`
- `specs/spec.md`

**Verification:**
```bash
# Run tests
go test ./internal/tigerfs/fuse/... -v -run TestCache

# Test with small database (eager)
TIGERFS_METADATA_PRELOAD_LIMIT=100 ./bin/tigerfs postgres://... /tmp/mount
# First ls should fetch all metadata

# Test with threshold=1 to force lazy behavior
TIGERFS_METADATA_PRELOAD_LIMIT=1 ./bin/tigerfs postgres://... /tmp/mount
# First ls should only fetch table list
# Accessing individual table fetches its metadata
```

**Completion Criteria:**
- Config option `metadata_preload_limit` added with default 100
- Eager caching works for small databases
- Lazy caching works for large databases
- Per-table TTL respected for lazy-cached entries
- Tests pass
- Documented in spec.md

---

### Task 7.2: Evaluate Multi-User Mount Support (allow_other)

**Objective:** Research and potentially implement `--allow-other` flag for multi-user mount access

**Background:**
FUSE's `allow_other` option allows users other than the mounting user to access the filesystem. Currently TigerFS mounts are single-user only. Cross-platform support varies:
- Linux: Full support (requires `user_allow_other` in `/etc/fuse.conf`)
- macOS: Limited support (macFUSE, may require SIP adjustments)
- Windows: Different model (WinFsp uses ACLs, not Unix permissions)

**Questions to Address:**
1. Should TigerFS support multi-user mounts at all?
2. If yes, how to handle the single PostgreSQL connection shared across Unix users?
3. Should permission bits change for multi-user access?
   - Files: 0600 (owner rw) → 0644 (world-readable) or 0444 (read-only)
   - Directories: 0700 (owner rwx) → 0755 (world-traversable) or 0555 (read-only)
4. Platform-specific implementation requirements?

**Steps:**
1. Research current macFUSE `allow_other` support on modern macOS
2. Test `allow_other` behavior with go-fuse library on Linux
3. Decide on permission model if `allow_other` is enabled
4. If proceeding: wire up the commented-out flag in `cmd/mount.go`
5. Document platform-specific behavior and limitations

**Files to Modify:**
- `internal/tigerfs/cmd/mount.go` (uncomment and wire up flag)
- `internal/tigerfs/fuse/fs.go` (pass AllowOther to mount options)
- `specs/spec.md` (document behavior)

**Completion Criteria:**
- Decision made on whether to support allow_other
- If yes: flag wired up and working on Linux
- Platform limitations documented
- Permission model documented (files: 0600 vs 0644; directories: 0700 vs 0755)

---

### Task 7.3: Row Timestamps from Database Columns (Optional)

**Objective:** Show file modification times based on table timestamp columns

**Status:** Optional - to be reconsidered based on user feedback

**Background:**
FUSE filesystems can report mtime (modification time) for files. For row files, this could come from a timestamp column in the table (e.g., `updated_at`). Currently, rows show epoch time (1970) or mount time.

**Limitations to Consider:**

1. **No standard column naming**: Unlike Rails/Django conventions (`updated_at`, `modified_at`), many databases use arbitrary names or have no timestamp column at all.

2. **Configuration complexity**: Making the column name configurable adds user burden:
   - Global config doesn't work for mixed schemas
   - Per-table config is verbose for large databases

3. **Type ambiguity**: Tables may have multiple timestamp columns (`created_at`, `updated_at`, `deleted_at`, `processed_at`). Choosing the "right" one requires domain knowledge.

4. **NULL handling**: Timestamp columns are often nullable. Fallback behavior (mount time? epoch?) may confuse users.

5. **Marginal benefit**: Most filesystem tools don't rely on mtime for database exploration. The primary use case (sorting by modification) may not apply to database rows.

**If Proceeding:**

1. Add config option `timestamp_columns` with ordered list of column names to try:
   ```yaml
   timestamp_columns: ["updated_at", "modified_at", "last_modified", "mtime"]
   ```

2. During metadata refresh, check each table for first matching column name

3. Parse timestamp from row data in `fetchData()`, store as `mtime` field

4. Set `out.Mtime` in `Getattr()`, fallback to mount time if no column or NULL

**Files to Modify:**
- `internal/tigerfs/config/config.go` (add timestamp_columns option)
- `internal/tigerfs/fuse/cache.go` (detect timestamp column per table)
- `internal/tigerfs/fuse/row.go` (parse and return mtime)

**Completion Criteria:**
- Decision made on whether to implement
- If yes: configurable column name list
- If yes: fallback behavior documented
- If yes: works with common conventions (updated_at, modified_at)

---

## Task Execution Guidelines

### Before Starting Each Task

1. **Read the task description completely**
2. **Check dependencies** - Are previous tasks complete?
3. **Understand acceptance criteria** - What defines "done"?
4. **Ask questions if unclear** - Better to clarify than guess

### While Executing Task

1. **Follow the steps sequentially**
2. **Write tests as you go** - Don't defer testing
3. **Run verification commands** - Ensure each step works
4. **Update documentation if needed**
5. **Commit working code frequently**

### After Completing Task

1. **Run all verification commands**
2. **Verify all acceptance criteria met**
3. **Run test suite**: `go test ./...`
4. **Format code**: `go fmt ./...`
5. **Check for errors**: `go vet ./...`
6. **Create git commit** with descriptive message
7. **Move to next task** or ask for guidance

### If Blocked

1. **Document the blocker** - What specifically is the issue?
2. **Attempt workarounds** - Can you proceed partially?
3. **Ask for help** - Describe what you tried and what didn't work
4. **Don't skip ahead** - Tasks build on each other

---

## Success Criteria for Implementation

### Functional Requirements
- All Phase 6 tasks completed
- All CLI commands functional
- CRUD operations working
- Index navigation operational
- Large table handling implemented
- Tiger Cloud integration working

### Quality Requirements
- >80% test coverage
- All unit tests pass
- All integration tests pass
- No race conditions (`go test -race` passes)
- No critical bugs

### Documentation Requirements
- README complete
- Getting started guide written
- Installation guide written
- Examples documented
- Performance documented

### Release Requirements
- v0.1.0 tagged and pushed
- Binaries built for all platforms
- Install scripts working
- GitHub Release published
- Release announcement written

---

## Notes for Claude Code

- **Each task is designed to be self-contained** - You can complete it with the information provided
- **Verification commands are provided** - Run them to confirm success
- **Tests are integrated throughout** - Not deferred to the end
- **Dependencies are explicit** - Tasks build on previous work
- **Commit after each task** - Keep git history clean and logical
- **Ask questions when blocked** - Don't guess or skip ahead

**Ready to begin! Start with Task 1.1: Evaluate and Select FUSE Library**
