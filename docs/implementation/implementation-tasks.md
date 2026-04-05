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

### Task 3.6: Implement Cloud Backend Integration

**Objective:** Support Tiger Cloud and Ghost backends via prefix scheme (`tiger:<id>`, `ghost:<id>`) with `create`, `fork`, and `info` commands.

**Implemented (see ADR-013 for design):**
1. Created `internal/tigerfs/backend/` package with `Backend` interface, `TigerBackend`, `GhostBackend`, `Resolve()` function
2. Updated `mount` command to accept `tiger:<id>` / `ghost:<id>` prefix on CONNECTION argument (replaced old `--tiger-service-id` flag)
3. Added `tigerfs create [BACKEND:]NAME [MOUNTPOINT]` command
4. Added `tigerfs fork SOURCE [DEST]` command
5. Added `tigerfs info [MOUNTPOINT]` command
6. Added `default_backend` config option for bare name resolution
7. Removed old `--host`, `--port`, `--user`, `--database`, `--tiger-service-id` flags

**Files Created:**
- `internal/tigerfs/backend/backend.go` (interface and types)
- `internal/tigerfs/backend/tiger.go` (Tiger Cloud backend)
- `internal/tigerfs/backend/ghost.go` (Ghost backend)
- `internal/tigerfs/backend/resolve.go` (prefix resolution)
- `internal/tigerfs/backend/errors.go` (error handling)
- `internal/tigerfs/backend/resolve_test.go`
- `internal/tigerfs/backend/errors_test.go`
- `internal/tigerfs/cmd/create.go`
- `internal/tigerfs/cmd/fork.go`
- `internal/tigerfs/cmd/info.go`
- `internal/tigerfs/cmd/create_test.go`
- `internal/tigerfs/cmd/fork_test.go`
- `docs/adr/013-backend-prefix-scheme.md`

**Files Modified:**
- `internal/tigerfs/cmd/mount.go` (prefix scheme, removed old flags)
- `internal/tigerfs/cmd/root.go` (added new commands)
- `internal/tigerfs/cmd/status.go` (shows backend/service info)
- `internal/tigerfs/mount/registry.go` (ServiceID, CLIBackend fields)
- `internal/tigerfs/config/config.go` (DefaultBackend field)
- `internal/tigerfs/db/connection.go` (uses backend.TigerBackend)

**Files Deleted:**
- `internal/tigerfs/tigercloud/` (replaced by backend/)

**Completion Criteria:**
- Prefix scheme resolves tiger:/ghost:/postgres:// correctly
- Create, fork, info commands work with both backends
- Mount accepts prefix-style connection references
- Error messages guide users to install CLI or authenticate
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

### Task 4.15: Support Database Views

**Objective:** Expose PostgreSQL views alongside tables with appropriate read/write behavior

**Background:** Views are specified in spec.md to be treated identically to tables for reading.
This includes JOIN views (e.g., `CREATE VIEW user_orders AS SELECT ... FROM users JOIN orders ...`).
Updatable views support writes (PostgreSQL handles); non-updatable views return EACCES.

**Key Design Points:**
- Simple single-table views may have a primary key and be updatable
- JOIN views typically have no primary key, so they:
  - Use `ctid` for row identification (like Task 4.16a)
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
   - No PK → falls back to ctid-based access (Task 4.16a/4.16b)
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

### Task 4.16: Support Composite Primary Keys

**Objective:** Enable full filesystem operations on tables with multi-column primary keys

**Background:** TigerFS currently rejects tables with composite PKs (`db/keys.go:60-63`). The `PrimaryKey` struct holds `Columns []string`, but all downstream code assumes a single column via `pk.Columns[0]` (~40+ sites). This task removes that limitation.

**Design Decisions:**
- **Path representation:** Comma-delimited values. For `PRIMARY KEY (customer_id, product_id)` with values `(5, 42)`, the directory name is `5,42`. Single-column PKs unchanged (backward compatible). Values containing commas are URL-encoded (`%2C`).
- **Code strategy:** Introduce `PKMatch` struct bundling column names + values. DB methods accept `*PKMatch` for row identification. `PrimaryKey` gains `Encode(values)` and `Decode(dirname)` methods.

**Steps:**
1. Create `internal/tigerfs/db/pk_match.go`:
   - `PKMatch` struct with `Columns []string` and `Values []string`
   - `WhereClause(startParam int)` generates `"col1" = $1 AND "col2" = $2`
   - `WhereArgs()` returns values as `[]interface{}`
   - `SinglePKMatch(column, value string)` convenience constructor
2. Add `Encode`/`Decode` methods to `PrimaryKey` in `internal/tigerfs/db/keys.go`:
   - `Encode(values []string) string` -- comma-delimited with URL-encoding
   - `Decode(dirname string) (*PKMatch, error)` -- returns `*PKMatch`
   - Remove composite PK rejection (lines 60-63)
3. Update `internal/tigerfs/db/interfaces.go`:
   - Row-identifying methods (`GetRow`, `UpdateRow`, `DeleteRow`, etc.) take `*PKMatch`
   - Listing/pagination methods take `pkColumns []string`
4. Update `internal/tigerfs/db/query.go`:
   - All CRUD functions use `PKMatch.WhereClause()` and `PKMatch.WhereArgs()`
   - Pagination functions handle multi-column SELECT/ORDER BY/scan
5. Update `internal/tigerfs/db/keys.go`:
   - `ListRows`/`ListAllRows`: SELECT all PK columns, encode with `pk.Encode()`
6. Update `internal/tigerfs/db/pipeline.go`:
   - `QueryParams.PKColumn string` -> `PKColumns []string`
   - Multi-column SELECT, ORDER BY in pipeline SQL
7. Update `internal/tigerfs/fs/context.go`:
   - `FSContext.PKColumn string` -> `PKColumns []string`
8. Update `internal/tigerfs/fs/operations.go` (7 sites):
   - Each `pk.Columns[0]` -> `pk.Decode(parsed.PrimaryKey)` then pass `*PKMatch`
9. Update `internal/tigerfs/fs/write.go` (6 sites):
   - Same pattern; rename decodes both old and new PK
10. Update FUSE legacy code (compile fix only):
    - Wrap existing `pk.Columns[0]` calls with `db.SinglePKMatch()` to satisfy new interfaces

**Files to Create:**
- `internal/tigerfs/db/pk_match.go`
- `internal/tigerfs/db/pk_match_test.go`

**Files to Modify:**
- `internal/tigerfs/db/keys.go`
- `internal/tigerfs/db/keys_test.go`
- `internal/tigerfs/db/interfaces.go`
- `internal/tigerfs/db/query.go`
- `internal/tigerfs/db/pipeline.go`
- `internal/tigerfs/db/export.go`
- `internal/tigerfs/fs/context.go`
- `internal/tigerfs/fs/operations.go`
- `internal/tigerfs/fs/write.go`
- `internal/tigerfs/fs/synth_ops.go`
- `internal/tigerfs/fuse/table.go` (compile fix)
- `internal/tigerfs/fuse/index.go` (compile fix)
- `internal/tigerfs/fuse/all.go` (compile fix)
- `internal/tigerfs/fuse/pagination.go` (compile fix)
- `internal/tigerfs/fuse/sample.go` (compile fix)
- `internal/tigerfs/fuse/order.go` (compile fix)

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/db/... ./internal/tigerfs/fs/...

# All tests (backward compat)
go test ./...

# Integration tests
go test -v -run TestMount_CompositePK ./test/integration/...

# Manual verification
psql -c "CREATE TABLE order_items (customer_id int, product_id int, quantity int, PRIMARY KEY (customer_id, product_id));"
psql -c "INSERT INTO order_items VALUES (1, 100, 5), (1, 200, 3), (2, 100, 1);"
ls /mount/order_items/            # Should show: 1,100  1,200  2,100
cat /mount/order_items/1,100      # Should show row data
cat /mount/order_items/1,100/quantity  # Should show: 5
```

**Completion Criteria:**
- Composite primary keys work for ReadDir, ReadFile, ReadColumn
- Writes (update, insert, delete) work with composite PKs
- Pipeline operations (.first, .last, .order, .filter, .export) work
- Mixed PK types (int+text, uuid+int) work
- Single-column PK behavior unchanged
- All existing tests pass
- New unit and integration tests pass

---

### Task 4.17a: Support Tables Without Primary Keys

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
4. Add `.info/warning` metadata file explaining the limitation

**Files to Modify:**
- `internal/tigerfs/db/keys.go`
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/row.go`
- `internal/tigerfs/fuse/info.go`

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

cat /tmp/testmount/logs/.info/warning
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

### Task 4.17b: Support Reading Views (No Primary Key)

**Objective:** Allow read-only access to views using ctid-based row identification

**Depends on:** Task 4.16a (ctid infrastructure)

Views don't have primary keys, so they need the same ctid-based access pattern
implemented in 4.16a. This task extends that support to views.

**Background:**
- Task 4.15 implemented view discovery and metadata (`.info/schema`, `.info/ddl`)
- Views currently fail when trying to list rows because `GetPrimaryKey` fails
- Simple views (single-table filters) could theoretically use the underlying PK,
  but detecting this reliably is complex
- Consistent approach: treat all views as "tables without PK" using ctid

**Steps:**
1. Update `internal/tigerfs/fuse/table.go`:
   - When `isView=true`, skip primary key lookup
   - Use ctid-based row listing (from 4.16a)
   - Views are always read-only for row operations
2. Update `internal/tigerfs/fuse/info.go`:
   - Add `.info/warning` for views explaining ctid limitations
3. Ensure capability directories work for views:
   - `.info/` - metadata (already working)
   - `.first/N/`, `.last/N/`, `.sample/N/` - pagination via ctid
   - `.by/` and `.order/` - may not be applicable (no indexes on views)

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/info.go`
- `internal/tigerfs/fuse/pagination.go`
- `internal/tigerfs/fuse/sample.go`

**Verification:**
```bash
# Assuming views exist from demo init.sql:
# - active_users (simple filter view)
# - order_summary (JOIN view)

go run ./cmd/tigerfs postgres://... /tmp/testmount

# View should be listable
ls /tmp/testmount/active_users/
# Should show: .info/  .first/  .last/  .sample/  (0,1)  (0,2)  ...

# Read row from view
cat /tmp/testmount/active_users/'(0,1)'
# Should show row data

# Check warning
cat /tmp/testmount/active_users/.info/warning
# Should explain ctid limitations and read-only nature

# Pagination should work
ls /tmp/testmount/order_summary/.first/10/
cat /tmp/testmount/order_summary/.first/10/'(0,1)'.json

# Write should fail (views are read-only for row ops)
echo '{"name":"test"}' > /tmp/testmount/active_users/'(0,1)'.json
# Should fail with EACCES or EROFS
```

**Completion Criteria:**
- Views can be listed (showing ctid-based row identifiers)
- View rows can be read via ctid
- Pagination (`.first/N/`, `.last/N/`, `.sample/N/`) works for views
- Write operations return appropriate error
- `.info/warning` explains limitations
- Tests pass

---

### Task 4.18a: Basic Hypertable Support (with Primary Keys)

**Objective:** Verify and ensure that TimescaleDB hypertables with primary keys work as regular tables through existing TigerFS filesystem operations.

**Background:** TimescaleDB hypertables are regular PostgreSQL tables with automatic partitioning. They appear in `information_schema.tables` as `BASE TABLE` and their PKs appear in `information_schema.table_constraints`. Hypertable PKs must include the time dimension column, making them composite (e.g., `PRIMARY KEY (time, product_id, user_id)`). With composite PK support (task 4.16), hypertables should work transparently through existing table discovery, PK discovery, and CRUD/pipeline operations.

**Steps:**
1. Add `product_views` hypertable to demo data (`scripts/demo/init.sql`) using modern `WITH (tsdb.hypertable, tsdb.segmentby, tsdb.orderby)` syntax
2. Add `createProductViewsTable` to test data helper (`test/integration/demo_data.go`)
3. Write integration tests (`test/integration/hypertable_test.go`) covering:
   - ReadDir (listing with timestamp composite PK entries)
   - ReadRow (reading by 3-column composite PK)
   - ReadColumn (single column from composite PK row)
   - WriteColumn (update column value)
   - DeleteRow (delete by composite PK)
   - Pipeline (.first, .filter, .order, .export/json)
   - InsertRow (write JSON to new PK path)
   - DDLCreate (create hypertable via DDL staging with multi-statement SQL)

**Files Modified:**
- `scripts/demo/init.sql` -- added product_views hypertable with columnstore
- `test/integration/demo_data.go` -- added createProductViewsTable
- `test/integration/hypertable_test.go` -- new: 8 integration tests

**Verification:**
```bash
# Run hypertable integration tests
go test -v -run "Hypertable" -timeout 120s ./test/integration/...

# Verify all existing tests still pass
go test ./... -count=1
```

**Completion Criteria:**
- Hypertables with composite PKs work as regular tables (no code changes needed)
- All CRUD operations work (read, write, update, delete)
- Pipeline operations work (.first, .filter, .order, .export)
- DDL staging creates hypertables with multi-statement SQL
- Timestamp PK values round-trip correctly through filesystem encoding
- All 8 integration tests pass

---

### Task 4.18b: Advanced Hypertable Support (.time/, .chunks/, continuous aggregates)

**Objective:** Add hypertable-specific navigation with time-based access and chunk visibility

**Steps:**
1. Create `internal/tigerfs/db/timescale.go`:
   - Detect TimescaleDB extension
   - Detect hypertables: `SELECT * FROM timescaledb_information.hypertables`
   - Get time column for hypertable
   - Get chunk information
2. Update filesystem path handling:
   - For hypertables, add `.chunks/` virtual directory
   - Add time-based access paths: `.time/2026-01-01/` to `.time/2026-01-31/`
3. Implement time-range and chunk-based navigation
4. Support continuous aggregates as virtual tables

**Files to Create:**
- `internal/tigerfs/db/timescale.go`
- `internal/tigerfs/db/timescale_test.go`

**Verification:**
```bash
go run ./cmd/tigerfs postgres://... /tmp/testmount

# Access via time range
ls /tmp/testmount/product_views/.time/
ls /tmp/testmount/product_views/.time/2026-01-01/

# Access via chunks
ls /tmp/testmount/product_views/.chunks/
```

**Completion Criteria:**
- Hypertables detected
- Time-based navigation works
- Chunk navigation works
- Continuous aggregates accessible
- Tests pass

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

### Task 4.23: Centralize Path and Format Constants

**Objective:** Create a single source of truth for all special path names, format extensions, and control file names used throughout the codebase.

**Background:** The codebase has accumulated many hard-coded strings for special directories (`.indexes`, `.schemas`, `.views`, `.create`, `.modify`, `.delete`), metadata files (`.count`, `.ddl`, `.schema`, `.columns`), control files (`.sql`, `.test`, `.commit`, `.abort`), and format extensions (`.json`, `.csv`, `.tsv`, `.yaml`). Centralizing these enables single-point changes and reduces typo risk.

**Steps:**
1. Create `internal/tigerfs/fuse/constants.go` with organized constant groups:
   ```go
   // Metadata directory and files
   const DirInfo = ".info"
   const (
       FileCount   = "count"
       FileDDL     = "ddl"
       FileSchema  = "schema"
       FileColumns = "columns"
   )

   // Navigation capabilities
   const (
       DirBy     = ".by"
       DirFirst  = ".first"
       DirLast   = ".last"
       DirSample = ".sample"
       DirAll    = ".all"
       DirOrder  = ".order"
   )

   // Bulk data capabilities
   const (
       DirExport    = ".export"
       DirImport    = ".import"
       DirOverwrite = ".overwrite"
       DirSync      = ".sync"
       DirAppend    = ".append"
   )

   // Schema-level directories
   const (
       DirSchemas = ".schemas"
       DirViews   = ".views"
   )

   // DDL capabilities
   const (
       DirIndexes = ".indexes"
       DirCreate  = ".create"
       DirModify  = ".modify"
       DirDelete  = ".delete"
   )

   // Control files (DDL staging)
   // Content files (visible): sql, test.log
   // Trigger files (hidden): .test, .commit, .abort
   const (
       FileSQL     = "sql"
       FileTest    = ".test"
       FileTestLog = "test.log"
       FileCommit  = ".commit"
       FileAbort   = ".abort"
   )

   // Format extensions (with dot)
   const (
       ExtJSON = ".json"
       ExtCSV  = ".csv"
       ExtTSV  = ".tsv"
       ExtYAML = ".yaml"
       ExtTxt  = ".txt"
       ExtBin  = ".bin"
   )

   // Format names (without dot, for directory entries)
   const (
       FmtJSON = "json"
       FmtCSV  = "csv"
       FmtTSV  = "tsv"
       FmtYAML = "yaml"
   )
   ```

2. Audit and refactor ALL existing code to use constants:
   - `internal/tigerfs/fuse/*.go` - all FUSE node implementations
   - `internal/tigerfs/fuse/*_test.go` - all FUSE tests
   - `internal/tigerfs/db/*.go` - any path references
   - `test/integration/*.go` - integration tests

3. Search for hard-coded strings to ensure complete coverage:
   ```bash
   grep -r '"\\.indexes"' internal/
   grep -r '"\\.schemas"' internal/
   grep -r '"\\.views"' internal/
   grep -r '"\\.create"' internal/
   grep -r '"\\.modify"' internal/
   grep -r '"\\.delete"' internal/
   grep -r '"\\.sql"' internal/
   grep -r '"\\.test"' internal/
   grep -r '"\\.commit"' internal/
   grep -r '"\\.abort"' internal/
   grep -r '"\\.first"' internal/
   grep -r '"\\.last"' internal/
   grep -r '"\\.sample"' internal/
   grep -r '"\\.all"' internal/
   grep -r '"\\.count"' internal/
   grep -r '"\\.ddl"' internal/
   grep -r '"\\.schema"' internal/
   grep -r '"\\.columns"' internal/
   grep -r '"\\.json"' internal/
   grep -r '"\\.csv"' internal/
   grep -r '"\\.tsv"' internal/
   grep -r '"\\.yaml"' internal/
   grep -r '"\\.txt"' internal/
   grep -r '"\\.bin"' internal/
   ```

4. Update tests to use constants

**Files to Create:**
- `internal/tigerfs/fuse/constants.go`
- `internal/tigerfs/fuse/constants_test.go` (optional, for validation)

**Files to Modify:**
- All files in `internal/tigerfs/fuse/` that reference special paths
- All test files that reference special paths
- `test/integration/*.go` as needed

**Verification:**
```bash
go build ./...
go test ./...

# Verify no remaining hard-coded special paths (should return empty)
grep -rE '"\.(indexes|schemas|views|create|modify|delete|sql|test|commit|abort|first|last|sample|all|count|ddl|schema|columns|json|csv|tsv|yaml|txt|bin)"' internal/ | grep -v constants.go
```

**Completion Criteria:**
- All special paths defined in `constants.go`
- No hard-coded special path strings remain in code (except `constants.go`)
- All tests pass
- Code compiles without errors

---

### Task 4.24: Implement `.info/` Metadata Subdirectory

**Objective:** Move metadata files (`.count`, `.ddl`, `.schema`, `.columns`) under a `.info/` subdirectory.

**Background:** See `docs/adr/005-capability-directory-taxonomy.md` for full rationale. This separates metadata (nouns describing the table) from capabilities (verbs for actions).

**New Structure:**
```
/mnt/db/users/
├── .info/
│   ├── count      # row count
│   ├── ddl        # CREATE TABLE statement
│   ├── schema     # column details
│   └── columns    # column names (one per line)
├── .first/
├── .last/
└── 1/             # row by PK
```

**Steps:**
1. Create `InfoDirNode` in `internal/tigerfs/fuse/info.go`:
   - Implement `Readdir()` to list: count, ddl, schema, columns
   - Implement `Lookup()` to return appropriate file nodes
   - Reuse existing metadata file implementations

2. Update `TableNode.Lookup()` to handle `.info` directory
3. Update `TableNode.Readdir()` to include `.info` in listing
4. Update all tests to use new paths

**Files to Create:**
- `internal/tigerfs/fuse/info.go`

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/table_test.go`
- `test/integration/*.go` as needed

**Verification:**
```bash
go test ./...
# Mount and verify
ls /mnt/db/users/.info/
cat /mnt/db/users/.info/count
cat /mnt/db/users/.info/ddl
```

**Completion Criteria:**
- `.info/` directory appears in table listing
- All four metadata files accessible under `.info/`
- Old top-level metadata paths still work (temporary, removed in 4.26)
- Tests pass

---

### Task 4.25: Implement `.by/` Index Navigation

**Objective:** Move index-based navigation under a `.by/` subdirectory.

**Background:** See `docs/adr/005-capability-directory-taxonomy.md`. This prevents column names from colliding with reserved capability names.

**New Structure:**
```
/mnt/db/users/
├── .by/
│   ├── email/           # single-column index
│   │   └── foo@example.com/
│   └── last_name.first_name/  # composite index
│       └── Smith/
│           └── John/
├── .info/
└── 1/
```

**Steps:**
1. Create `ByDirNode` in `internal/tigerfs/fuse/by.go`:
   - Implement `Readdir()` to list indexed columns (single + composite)
   - Implement `Lookup()` to return `IndexNode` or `CompositeIndexNode`

2. Update `TableNode.Lookup()` to handle `.by` directory
3. Update `TableNode.Readdir()` to include `.by` in listing
4. Update all tests to use new paths

**Files to Create:**
- `internal/tigerfs/fuse/by.go`

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/index.go`
- `internal/tigerfs/fuse/index_test.go`
- `test/integration/*.go` as needed

**Verification:**
```bash
go test ./...
# Mount and verify
ls /mnt/db/users/.by/
ls /mnt/db/users/.by/email/
cat /mnt/db/users/.by/email/foo@example.com.json
```

**Completion Criteria:**
- `.by/` directory appears in table listing
- All indexed columns accessible under `.by/`
- Old top-level index paths still work (temporary, removed in 4.26)
- Tests pass

---

### Task 4.26: Remove Legacy Metadata and Index Paths

**Objective:** Remove backward-compatible aliases for old paths. This is a breaking change.

**Background:** After 4.24 and 4.25, both old and new paths work. This task removes the old paths to clean up the codebase.

**Paths to Remove:**
- Table-level metadata: `.count`, `.ddl`, `.schema`, `.columns` (now under `.info/`)
- Table-level index navigation: `.<column>/` patterns (now under `.by/`)

**Steps:**
1. Update `TableNode.Lookup()` to remove handling for legacy paths
2. Update `TableNode.Readdir()` to remove legacy entries from listing
3. Update all tests to use only new paths
4. Update README.md and docs/spec.md to reflect new structure
5. Search for any remaining references to old paths

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/table_test.go`
- `test/integration/*.go`
- `README.md`
- `docs/spec.md`

**Verification:**
```bash
go test ./...

# Verify old paths no longer work
ls /mnt/db/users/.count        # Should fail (ENOENT)
ls /mnt/db/users/.email/       # Should fail (ENOENT)

# Verify new paths work
cat /mnt/db/users/.info/count
ls /mnt/db/users/.by/email/
```

**Completion Criteria:**
- Old metadata paths return ENOENT
- Old index paths return ENOENT
- New paths work correctly
- Documentation updated
- All tests pass

---

### Task 4.27: Implement `.order/<column>/` Capability

**Objective:** Add row ordering capability for any collection context.

**Background:** See `docs/adr/006-bulk-export-capability.md`. The `.order/<column>/` capability specifies row ordering. Direction is implicit: `.first` = ASC, `.last` = DESC.

**Supported Paths:**
```
.order/created_at/.first/100/    # First 100 by created_at ASC
.order/price/.last/50/           # Last 50 by price DESC (highest prices)
.order/name/.first/10/.export/csv  # (future: with export)
```

**Steps:**
1. Create `OrderDirNode` in `internal/tigerfs/fuse/order.go`:
   - `Readdir()`: List all table columns as ordering options
   - `Lookup()`: Return `OrderColumnNode` for valid column names

2. Create `OrderColumnNode`:
   - `Readdir()`: List `.first`, `.last` (and rows if no pagination)
   - `Lookup()`: Handle `.first`, `.last`, and row access

3. Update db queries to support ORDER BY with column parameter:
   - `GetFirstNRowsOrdered(ctx, schema, table, pkColumn, orderColumn, limit)`
   - `GetLastNRowsOrdered(ctx, schema, table, pkColumn, orderColumn, limit)`

4. Update `TableNode` to include `.order` in listing and lookup

**Files to Create:**
- `internal/tigerfs/fuse/order.go`
- `internal/tigerfs/fuse/order_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/db/rows.go` (or appropriate file for queries)

**Verification:**
```bash
go test ./...
# Mount and verify
ls /mnt/db/events/.order/
ls /mnt/db/events/.order/created_at/.first/10/
ls /mnt/db/events/.order/created_at/.last/10/
```

**Completion Criteria:**
- `.order/` directory lists all columns
- `.order/<column>/.first/N/` returns rows ordered ASC
- `.order/<column>/.last/N/` returns rows ordered DESC
- Works with any column (not just indexed)
- Tests pass

---

### Task 4.28: Implement `.export/` Bulk Read Capability

**Objective:** Add single-file materialization of table data in multiple formats.

**Background:** See `docs/adr/006-bulk-export-capability.md`. The `.export/` capability provides bulk data export as csv, tsv, json, or yaml files.

**Structure:**
```
/mnt/db/users/
├── .export/
│   ├── csv      # All rows as CSV
│   ├── tsv      # All rows as TSV
│   ├── json     # All rows as JSON array
│   └── yaml     # All rows as multi-document YAML
```

**Composition:** `.export/` appears in any collection context:
```
users/.export/csv                           # Entire table
users/.first/100/.export/json               # First 100 rows
users/.order/created_at/.last/50/.export/csv  # Last 50 by date
users/.by/status/active/.export/yaml        # All active users
```

**Steps:**
1. Create `ExportDirNode` in `internal/tigerfs/fuse/export.go`:
   - `Readdir()`: List format options (csv, tsv, json, yaml)
   - `Lookup()`: Return `ExportFileNode` for each format

2. Create `ExportFileNode`:
   - `Read()`: Fetch rows from context, serialize to requested format
   - `Getattr()`: Return file attributes (size may require full materialization or estimate)

3. Implement bulk serialization in `internal/tigerfs/format/`:
   - `RowsToCSV(columns []string, rows [][]interface{}) ([]byte, error)`
   - `RowsToTSV(columns []string, rows [][]interface{}) ([]byte, error)`
   - `RowsToJSON(columns []string, rows [][]interface{}) ([]byte, error)`
   - `RowsToYAML(columns []string, rows [][]interface{}) ([]byte, error)`

4. Add db method to fetch multiple rows:
   - `GetRows(ctx, schema, table, pkColumn string, limit int) ([][]interface{}, error)`

5. Enforce `DirListingLimit` for exports (error if exceeded, suggest pagination)

6. Add `.export/` to `TableNode`, `PaginationNode`, `OrderColumnNode`, `IndexValueNode`

**Files to Create:**
- `internal/tigerfs/fuse/export.go`
- `internal/tigerfs/fuse/export_test.go`

**Files to Modify:**
- `internal/tigerfs/format/csv.go`
- `internal/tigerfs/format/tsv.go`
- `internal/tigerfs/format/json.go`
- `internal/tigerfs/format/yaml.go`
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/pagination.go`
- `internal/tigerfs/fuse/order.go`
- `internal/tigerfs/fuse/index.go`

**Verification:**
```bash
go test ./...
# Mount and verify
cat /mnt/db/users/.export/csv
cat /mnt/db/users/.first/10/.export/json
cat /mnt/db/users/.order/name/.first/5/.export/yaml
```

**Completion Criteria:**
- All four formats work (csv, tsv, json, yaml)
- Works at table level and with pagination/ordering
- Enforces row limit with helpful error message
- Empty tables return empty file (0 bytes for csv/tsv, `[]` for json, empty for yaml)
- Tests pass

---

### Task 4.29: Implement `.import/` Bulk Write Capability

**Objective:** Add bulk data import with explicit write modes.

**Background:** See `docs/adr/006-bulk-export-capability.md`. The `.import/` capability provides three write modes with atomic transactions.

**Structure:**
```
/mnt/db/users/
├── .import/
│   ├── .overwrite/    # Truncate + insert
│   │   ├── csv
│   │   ├── tsv
│   │   ├── json
│   │   └── yaml
│   ├── .sync/         # Upsert (requires PK)
│   │   ├── csv
│   │   ├── tsv
│   │   ├── json
│   │   └── yaml
│   └── .append/       # Insert only (fail on conflict)
│       ├── csv
│       ├── tsv
│       ├── json
│       └── yaml
```

**Usage:**
```bash
cat data.csv > /mnt/db/users/.import/.overwrite/csv
cat updates.json > /mnt/db/users/.import/.sync/json
cat new_rows.csv > /mnt/db/users/.import/.append/csv
```

**Steps:**
1. Create `ImportDirNode` in `internal/tigerfs/fuse/import.go`:
   - `Readdir()`: List modes (.overwrite, .sync, .append)
   - `Lookup()`: Return `ImportModeNode`

2. Create `ImportModeNode`:
   - `Readdir()`: List formats (csv, tsv, json, yaml)
   - `Lookup()`: Return `ImportFileNode`

3. Create `ImportFileNode`:
   - `Write()`: Buffer incoming data
   - `Release()`: Parse format, execute import in transaction

4. Implement import operations in `internal/tigerfs/db/`:
   - `ImportOverwrite(ctx, schema, table, columns []string, rows [][]interface{}) error`
     - BEGIN, TRUNCATE, INSERT all rows, COMMIT (or ROLLBACK on error)
   - `ImportSync(ctx, schema, table, pkColumns []string, columns []string, rows [][]interface{}) error`
     - BEGIN, UPSERT all rows (INSERT ... ON CONFLICT UPDATE), COMMIT
     - Return error if table has no PK
   - `ImportAppend(ctx, schema, table, columns []string, rows [][]interface{}) error`
     - BEGIN, INSERT all rows (fail on conflict), COMMIT

5. Implement bulk parsing in `internal/tigerfs/format/`:
   - `ParseCSVBulk(data []byte) (columns []string, rows [][]interface{}, error)`
   - `ParseTSVBulk(data []byte) (columns []string, rows [][]interface{}, error)`
   - `ParseJSONBulk(data []byte) (columns []string, rows [][]interface{}, error)`
   - `ParseYAMLBulk(data []byte) (columns []string, rows [][]interface{}, error)`

6. Add configuration option `DirWritingLimit` (default 100,000)

7. Only allow `.import/` at table level (not in filtered views)

**Files to Create:**
- `internal/tigerfs/fuse/import.go`
- `internal/tigerfs/fuse/import_test.go`

**Files to Modify:**
- `internal/tigerfs/db/client.go` (or new file for bulk operations)
- `internal/tigerfs/format/csv.go`
- `internal/tigerfs/format/tsv.go`
- `internal/tigerfs/format/json.go`
- `internal/tigerfs/format/yaml.go`
- `internal/tigerfs/config/config.go` (add DirWritingLimit)
- `internal/tigerfs/fuse/table.go`

**Verification:**
```bash
go test ./...

# Test overwrite
echo 'id,name
1,Alice
2,Bob' > /mnt/db/users/.import/.overwrite/csv

# Test sync (updates + inserts)
echo '[{"id":1,"name":"Alice Updated"},{"id":3,"name":"Charlie"}]' > /mnt/db/users/.import/.sync/json

# Test append (insert only)
echo 'id,name
4,David' > /mnt/db/users/.import/.append/csv

# Verify .sync fails without PK
# (create table without PK, attempt sync, expect error)
```

**Completion Criteria:**
- All three modes work (.overwrite, .sync, .append)
- All four formats work (csv, tsv, json, yaml)
- Atomic transactions (all-or-nothing)
- `.sync/` returns error for tables without PK
- Enforces `DirWritingLimit`
- Only available at table level
- Tests pass

---

### Task 4.30: Require Headers for CSV/TSV Writes

**Objective:** Make CSV and TSV writes self-describing by requiring header rows.

**Background:** See `docs/adr/006-bulk-export-capability.md`. This is a breaking change that makes all formats consistently self-describing.

**Change:**
```bash
# Old (no longer supported):
echo 'Alice,alice@example.com' > /mnt/db/users/123.csv

# New (header required):
echo 'name,email
Alice,alice@example.com' > /mnt/db/users/123.csv
```

**Steps:**
1. Update `ParseCSV()` in `internal/tigerfs/format/csv.go`:
   - Require first row to be header
   - Return error if no header row
   - Map values to columns by header names

2. Update `ParseTSV()` in `internal/tigerfs/format/tsv.go`:
   - Same changes as CSV

3. Update all tests that write CSV/TSV:
   - Add header rows to test data
   - Update expected error cases

4. Update documentation:
   - README.md examples
   - docs/spec.md format documentation

**Files to Modify:**
- `internal/tigerfs/format/csv.go`
- `internal/tigerfs/format/tsv.go`
- `internal/tigerfs/format/csv_test.go`
- `internal/tigerfs/format/tsv_test.go`
- `internal/tigerfs/fuse/row_test.go`
- `internal/tigerfs/fuse/column_file_test.go`
- `test/integration/*.go`
- `README.md`
- `docs/spec.md`

**Verification:**
```bash
go test ./...

# Verify header required
echo 'Alice,alice@example.com' > /mnt/db/users/123.csv  # Should fail
echo 'name,email
Alice,alice@example.com' > /mnt/db/users/123.csv  # Should work
```

**Completion Criteria:**
- CSV writes require header row
- TSV writes require header row
- Clear error message when header missing
- Documentation updated
- All tests updated and pass

---

## Phase 5: Pipeline Query Architecture

### Overview

Enable composable capability chaining for complex queries. Users can combine filters, ordering, pagination, and export in flexible pipelines like:
- `.filter/status/active/.first/1000/.sample/50/.export/csv`
- `.by/customer_id/123/.order/created_at/.last/10/.export/json`
- `.first/100/.last/50/` (rows 51-100)

**Architecture:** See `docs/adr/007-pipeline-query-architecture.md` for full design.

**Key Concepts:**
- `PipelineContext` accumulates query state as users navigate paths
- Each capability exposes valid next capabilities based on semantic rules
- Permissive model allows all meaningful combinations, disallows only redundant operations
- `.by/` for indexed columns (guaranteed fast), `.filter/` for any column (may be slow)

---

### Task 5.1: Add Global Query Timeout Configuration

**Objective:** Protect against slow queries causing filesystem hangs

**Background:**
All database queries should have a configurable timeout. This is especially important for `.filter/` operations that may require full table scans on large tables.

**Steps:**
1. Add to `internal/tigerfs/config/config.go`:
   - `QueryTimeout time.Duration` (default: 30 seconds)
   - Support via config file, env var (`TIGERFS_QUERY_TIMEOUT`), and CLI flag
   - Validate range (1s - 10m)

2. Update `internal/tigerfs/db/client.go`:
   - Apply `statement_timeout` to query context
   - Create helper function `withQueryTimeout(ctx context.Context) context.Context`

3. Update all query functions to use timeout context:
   - `QueryRows`, `QueryRowData`, `QueryIndexValues`, etc.

4. Map timeout errors to `syscall.EIO` in FUSE operations:
   - Detect `context.DeadlineExceeded` or PostgreSQL timeout error
   - Log descriptive message with query details
   - Return EIO to caller

5. Add unit tests for timeout behavior

**Files to Modify:**
- `internal/tigerfs/config/config.go`
- `internal/tigerfs/db/client.go`
- `internal/tigerfs/cmd/mount.go` (add --query-timeout flag)

**Files to Create:**
- `internal/tigerfs/db/timeout_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/config/... -v -run TestQueryTimeout
go test ./internal/tigerfs/db/... -v -run TestTimeout

# Manual test with short timeout
TIGERFS_QUERY_TIMEOUT=1ms tigerfs mount ...
# Queries should fail with EIO
```

**Completion Criteria:**
- QueryTimeout config option works via all methods
- Queries respect statement timeout
- Timeout errors map to EIO with clear log message
- Unit tests pass

---

### Task 5.2: Add DirFilterLimit Configuration

**Objective:** Configure threshold for large table safety in `.filter/` operations

**Steps:**
1. Add to `internal/tigerfs/config/config.go`:
   - `DirFilterLimit int` (default: 100000)
   - Support via config file, env var (`TIGERFS_DIR_FILTER_LIMIT`), and CLI flag

2. Add database function `GetTableRowCountEstimate` in `internal/tigerfs/db/schema.go`:
   ```sql
   SELECT reltuples::bigint FROM pg_class
   WHERE relname = $1 AND relnamespace = (
       SELECT oid FROM pg_namespace WHERE nspname = $2
   )
   ```

3. Add unit tests

**Files to Modify:**
- `internal/tigerfs/config/config.go`
- `internal/tigerfs/db/schema.go`
- `internal/tigerfs/cmd/mount.go` (add --dir-filter-limit flag)

**Files to Create:**
- `internal/tigerfs/db/schema_estimate_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/config/... -v -run TestDirFilterLimit
go test ./internal/tigerfs/db/... -v -run TestRowCountEstimate
```

**Completion Criteria:**
- DirFilterLimit config option works
- Row count estimation function works
- Unit tests pass

---

### Task 5.3: Implement PipelineContext

**Objective:** Create the core data structure for accumulating query state

**Steps:**
1. Create `internal/tigerfs/fuse/pipeline.go`:
   ```go
   type PipelineContext struct {
       Schema    string
       TableName string
       PKColumn  string

       // Filters (from .by and .filter)
       Filters   []FilterCondition  // AND-combined

       // Ordering (from .order)
       OrderBy   string
       OrderDesc bool

       // Limit (from .first, .last, .sample)
       Limit     int
       LimitType LimitType  // None, First, Last, Sample

       // For nested limit operations
       PreviousLimit     int
       PreviousLimitType LimitType
   }

   type FilterCondition struct {
       Column  string
       Value   string
       Indexed bool
   }

   type LimitType int
   const (
       LimitNone LimitType = iota
       LimitFirst
       LimitLast
       LimitSample
   )
   ```

2. Implement methods:
   - `NewPipelineContext(schema, table, pkColumn string) *PipelineContext`
   - `Clone() *PipelineContext` (immutable pattern)
   - `WithFilter(col, val string, indexed bool) *PipelineContext`
   - `WithOrder(col string, desc bool) *PipelineContext`
   - `WithLimit(limit int, limitType LimitType) *PipelineContext`

3. Implement capability availability:
   - `CanAddFilter() bool` - true unless after .order/
   - `CanAddOrder() bool` - true unless already ordered or after sample
   - `CanAddLimit(limitType LimitType) bool` - check redundancy rules
   - `AvailableCapabilities() []string` - list valid next steps

4. Implement SQL generation helpers:
   - `NeedsSubquery() bool` - true if nested limits
   - `ToQueryParams() QueryParams` - convert to db layer params

5. Write comprehensive unit tests

**Files to Create:**
- `internal/tigerfs/fuse/pipeline.go`
- `internal/tigerfs/fuse/pipeline_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v -run TestPipeline
```

**Completion Criteria:**
- PipelineContext accumulates state correctly
- Clone creates independent copies
- Capability availability rules enforced
- All method combinations tested
- Unit tests pass

---

### Task 5.4: Implement Pipeline Query Functions

**Objective:** Add database query functions that support full pipeline semantics

**Steps:**
1. Add `QueryParams` struct to `internal/tigerfs/db/query.go`:
   ```go
   type QueryParams struct {
       Schema, Table, PKColumn string
       Filters      []FilterCondition
       OrderBy      string
       OrderDesc    bool
       Limit        int
       LimitType    LimitType
       // For nested limits (subquery)
       PreviousLimit     int
       PreviousLimitType LimitType
   }
   ```

2. Implement `QueryRowsPipeline(ctx, pool, params) ([]string, error)`:
   - Build SQL with filters, order, limit
   - Handle subquery for nested limits
   - Return primary key values

3. Implement `QueryRowsWithDataPipeline(ctx, pool, params) ([]string, [][]interface{}, error)`:
   - Same as above but returns full row data
   - Used for `.export/` operations

4. SQL generation examples:
   ```sql
   -- Simple: .filter/status/active/.order/name/.first/100/
   SELECT pk FROM t WHERE status = 'active' ORDER BY name LIMIT 100

   -- Nested: .first/100/.last/50/
   SELECT pk FROM (
       SELECT pk FROM t ORDER BY pk LIMIT 100
   ) sub ORDER BY pk DESC LIMIT 50

   -- Complex: .first/1000/.filter/status/active/.sample/50/
   SELECT pk FROM (
       SELECT * FROM (
           SELECT * FROM t ORDER BY pk LIMIT 1000
       ) sub1 WHERE status = 'active'
   ) sub2 ORDER BY RANDOM() LIMIT 50
   ```

5. Add unit tests with SQL verification

**Files to Modify:**
- `internal/tigerfs/db/query.go`

**Files to Create:**
- `internal/tigerfs/db/query_pipeline_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/db/... -v -run TestQueryPipeline
```

**Completion Criteria:**
- Simple pipelines generate correct SQL
- Nested limits use subqueries
- Complex combinations work
- SQL injection prevented
- Unit tests pass

---

### Task 5.5: Implement FilterDirNode for `.filter/`

**Objective:** Add `.filter/` capability for filtering by any column

**Steps:**
1. Create `internal/tigerfs/fuse/filter.go`:
   - `FilterDirNode` - shows available columns at `.filter/`
   - `FilterColumnNode` - shows available values at `.filter/<col>/`
   - `FilterValueNode` - applies filter, shows next capabilities

2. Implement column listing in `FilterDirNode.Readdir()`:
   - List all columns from table schema (not just indexed)

3. Implement value listing in `FilterColumnNode.Readdir()`:
   - Check if column is indexed (reuse existing index discovery)
   - If indexed: Always run `SELECT DISTINCT col` (fast)
   - If non-indexed:
     - Get table row count estimate
     - If < DirFilterLimit: Run `SELECT DISTINCT col`
     - If >= DirFilterLimit: Return `.table-too-large` indicator only
   - All DISTINCT queries respect QueryTimeout

4. Implement `FilterValueNode`:
   - Create `PipelineContext` with filter applied
   - `Readdir()` lists available capabilities
   - `Lookup()` routes to next capability or row nodes

5. Handle direct path access:
   - `.filter/<col>/<val>/` always works even if value not listed

6. Write unit tests

**Files to Create:**
- `internal/tigerfs/fuse/filter.go`
- `internal/tigerfs/fuse/filter_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v -run TestFilter

# Manual test
ls /mnt/db/users/.filter/           # Shows columns
ls /mnt/db/users/.filter/status/    # Shows values (if small table)
ls /mnt/db/large_table/.filter/col/ # Shows .table-too-large
```

**Completion Criteria:**
- `.filter/` shows all columns
- Value listing respects safety mechanisms
- Large table indicator works
- Direct path access works regardless of listing
- Unit tests pass

---

### Task 5.6: Implement PipelineNode

**Objective:** Create unified node that exposes capabilities based on context

**Steps:**
1. Create `PipelineNode` in `internal/tigerfs/fuse/pipeline_node.go`:
   ```go
   type PipelineNode struct {
       fs      *FS
       ctx     *PipelineContext
       baseDir string  // For constructing child paths
   }
   ```

2. Implement `Readdir()`:
   - List available capabilities from `ctx.AvailableCapabilities()`
   - List row files if context has data (PKs from query)

3. Implement `Lookup(name)`:
   - Route `.by/` to index capability (with new context)
   - Route `.filter/` to FilterDirNode (with new context)
   - Route `.order/` to order capability (with new context)
   - Route `.first/`, `.last/`, `.sample/` to limit capability
   - Route `.export/` to ExportDirNode (with context)
   - Route row PKs to RowNode

4. Implement `Getattr()`:
   - Standard directory attributes

5. Write unit tests

**Files to Create:**
- `internal/tigerfs/fuse/pipeline_node.go`
- `internal/tigerfs/fuse/pipeline_node_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v -run TestPipelineNode
```

**Completion Criteria:**
- PipelineNode routes to correct child nodes
- Capabilities exposed based on context
- Row listing works with pipeline context
- Unit tests pass

---

### Task 5.7: Update Existing Capability Nodes for Pipeline

**Objective:** Modify pagination, order, and index nodes to work with PipelineContext

**Steps:**
1. Update `internal/tigerfs/fuse/pagination.go`:
   - Add `PipelineContext` field to `FirstDirNode`, `LastDirNode`, `SampleDirNode`
   - Update constructors to accept optional context
   - In value nodes (e.g., `FirstValueNode`):
     - Create new context with limit applied
     - Expose capabilities via `Readdir()` based on new context
     - Route via `Lookup()` to PipelineNode or rows

2. Update `internal/tigerfs/fuse/order.go`:
   - Add `PipelineContext` field to `OrderDirNode`, `OrderColumnNode`
   - Update constructors to accept optional context
   - In `OrderColumnNode`:
     - Create new context with order applied
     - Expose capabilities via `Readdir()`
     - Route via `Lookup()` to next capability

3. Update `internal/tigerfs/fuse/index.go`:
   - Add `PipelineContext` field to index nodes
   - Update to expose pipeline capabilities after value selection
   - Add composite index parsing (`.by/col1.col2/val1.val2/`)

4. Update `internal/tigerfs/fuse/export.go`:
   - Ensure `ExportDirNode` accepts `PipelineContext`
   - Use context for query execution

5. Maintain backward compatibility:
   - Nodes without context work as before
   - Context is optional, defaults to "table root" behavior

6. Update existing tests

**Files to Modify:**
- `internal/tigerfs/fuse/pagination.go`
- `internal/tigerfs/fuse/order.go`
- `internal/tigerfs/fuse/index.go`
- `internal/tigerfs/fuse/export.go`
- `internal/tigerfs/fuse/*_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v
```

**Completion Criteria:**
- All capability nodes accept PipelineContext
- Pipeline capabilities exposed correctly
- Backward compatibility maintained
- Existing tests still pass
- New pipeline paths work

---

### Task 5.8: Update TableNode for Pipeline Integration

**Objective:** Wire up pipeline capabilities at table root

**Steps:**
1. Update `internal/tigerfs/fuse/table.go`:
   - Add `.filter/` to `Readdir()` entries
   - Update `Lookup()` to route `.filter/` to FilterDirNode
   - Create initial `PipelineContext` for table root
   - Pass context to capability nodes

2. Ensure all existing paths still work:
   - `.by/`, `.order/`, `.first/`, `.last/`, `.sample/`, `.export/`
   - Direct row access
   - Metadata files

3. Update tests

**Files to Modify:**
- `internal/tigerfs/fuse/table.go`
- `internal/tigerfs/fuse/table_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/... -v -run TestTable

# Manual verification
ls /mnt/db/users/           # Should show .filter/ now
ls /mnt/db/users/.filter/   # Should show columns
```

**Completion Criteria:**
- `.filter/` exposed at table root
- All capabilities have pipeline context
- Existing functionality preserved
- Tests pass

---

### Task 5.9: Implement Composite Index Support

**Objective:** Support `.by/col1.col2/val1.val2/` syntax for composite indexes

**Steps:**
1. Add composite index detection to `internal/tigerfs/db/schema.go`:
   - `GetCompositeIndexColumns(schema, table string) ([][]string, error)`
   - Returns list of column combinations that have indexes

2. Update `internal/tigerfs/fuse/index.go`:
   - Parse composite column syntax in `IndexByDirNode.Readdir()`:
     - Show single columns AND composite combinations
     - E.g., `status`, `customer_id`, `status.customer_id`
   - Parse composite value syntax in `IndexColumnNode.Lookup()`:
     - Split `val1.val2` into separate values
     - Match with column order

3. Handle edge cases:
   - Column names with dots (escape mechanism?)
   - Value mismatch (wrong number of values)
   - No matching composite index (decompose to separate filters)

4. Write unit tests

**Files to Modify:**
- `internal/tigerfs/db/schema.go`
- `internal/tigerfs/fuse/index.go`

**Files to Create:**
- `internal/tigerfs/db/composite_index_test.go`

**Verification:**
```bash
go test ./... -v -run TestComposite

# Manual test
ls /mnt/db/users/.by/                          # Shows columns and composites
ls /mnt/db/users/.by/last_name.first_name/     # Shows composite values
cat /mnt/db/users/.by/last_name.first_name/Smith.John/1.json
```

**Completion Criteria:**
- Composite columns shown in `.by/` listing
- Composite value lookup works
- Fallback to separate filters when no composite index
- Unit tests pass

---

### Task 5.10: Pipeline Integration Tests

**Objective:** Verify end-to-end pipeline functionality

**Steps:**
1. Create `test/integration/pipeline_test.go`:
   - Test simple pipelines:
     - `.by/status/active/.first/50/`
     - `.filter/name/Alice/.first/10/`
   - Test nested pagination:
     - `.first/100/.last/50/` (rows 51-100)
     - `.last/100/.first/50/`
     - `.first/1000/.sample/50/`
   - Test mixed filters:
     - `.by/customer_id/123/.filter/notes/urgent/`
   - Test full pipelines:
     - `.by/a/1/.order/b/.last/10/.export/json`
   - Test disallowed combinations (should return ENOENT):
     - `.first/100/.first/50/`
     - `.order/a/.order/b/`
     - `.export/csv/.first/10/`

2. Test large table safety:
   - Create table with >100k rows
   - Verify `.filter/<col>/` shows `.table-too-large`
   - Verify direct access still works

3. Test timeout behavior:
   - Configure short timeout
   - Verify slow query returns EIO

4. Test backward compatibility:
   - Existing `.by/<col>/<val>/` paths work
   - Existing `.first/N/` paths work
   - Direct `.export/csv` at table root works

**Files to Create:**
- `test/integration/pipeline_test.go`

**Verification:**
```bash
go test ./test/integration/... -v -run TestPipeline
```

**Completion Criteria:**
- All pipeline combinations work correctly
- Disallowed combinations fail gracefully
- Large table safety verified
- Timeout behavior verified
- Backward compatibility confirmed

---

### Task 5.11: Pipeline Documentation

**Objective:** Document pipeline capability for users

**Steps:**
1. Update `docs/spec.md`:
   - Add Pipeline Query Architecture section
   - Document all capabilities and combinations
   - Add SQL generation examples
   - Document safety mechanisms

2. Update `README.md`:
   - Add pipeline examples to usage section

3. Create `docs/pipeline-queries.md`:
   - Comprehensive guide with examples
   - Performance considerations
   - Limitations

**Files to Modify:**
- `docs/spec.md`
- `README.md`

**Files to Create:**
- `docs/pipeline-queries.md`

**Verification:**
```bash
# Verify documentation accuracy by following examples
```

**Completion Criteria:**
- Spec updated with full pipeline documentation
- README has pipeline examples
- Standalone guide comprehensive
- All examples tested and working

---

## Phase 6: Synthesized Apps

### Overview

Enable TigerFS to present database tables as synthesized files (markdown, plain text, task lists) rather than native row-as-directory format. Users access content as actual files with appropriate extensions and structure.

**Supported Formats:**
- **Markdown (.md)** - filename + YAML frontmatter + body
- **Plain Text (.txt)** - filename + body (no metadata)
- **Tasks (.md)** - ordered task items with `{number}-{name}-{status}.md` filenames

**Two Creation Methods:**
- `.build/` - Creates table + view (+ triggers) from scratch (primary)
- `.format/` - Creates view (+ triggers) on existing table

**Reference:** See ADR-008 for full design details.

**Phase Structure:**
- **6.1: Markdown & Plain Text** - Self-contained, fully testable without tasks
- **6.2: Tasks** - Extends 6.1 with task-specific filename format, triggers, and controls

---

## 6.1: Markdown & Plain Text

### Overview

Implement synthesized file support for markdown and plain text formats. This phase is self-contained and fully testable before implementing tasks format.

---

### Task 6.1.1: Implement Format Detection

**Objective:** Detect synthesized format from view name suffix or column patterns (markdown/plaintext only in this phase)

**Background:**
TigerFS needs to determine how to render a view based on naming conventions or column patterns. Explicit suffix takes priority over column convention detection. Tasks format is defined in this phase but implementation deferred to 6.2.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/format.go`:
   - Define format types:
     ```go
     type SynthFormat int
     const (
         FormatNative SynthFormat = iota  // Row-as-directory
         FormatMarkdown                   // .md files
         FormatPlainText                  // .txt files
         FormatTasks                      // Task list .md files (implemented in 6.2)
     )
     ```
   - Implement `DetectFormat(viewName string, columns []string) SynthFormat`:
     - Check suffix: `_md` → Markdown, `_txt` → PlainText, `_tasks`/`_todo`/`_items` → Tasks
     - If no suffix match, check columns:
       - `filename` + `body` + others → Markdown
       - `filename` + `body` only → PlainText
       - `number` + `name` + `status` + `body` → Tasks (detection only, handling in 6.2)
       - Otherwise → Native

2. Create `internal/tigerfs/fuse/synthesized/columns.go`:
   - Define column role detection:
     ```go
     type ColumnRoles struct {
         Filename    string   // Column for filename
         Body        string   // Column for body content
         Frontmatter []string // Columns for YAML frontmatter
         // Tasks-specific (populated in 6.2)
         Number      string
         Name        string
         Status      string
         Assignee    string
         Timestamps  map[string]string // todo_at, doing_at, done_at, etc.
     }
     ```
   - Implement `DetectColumnRoles(columns []string, format SynthFormat) (*ColumnRoles, error)`:
     - Filename conventions: `name`, `filename`, `title`, `slug`
     - Body conventions: `body`, `content`, `description`, `text`
     - For FormatTasks, return error "tasks format not yet implemented" (handled in 6.2)
     - Return error if required columns not found

3. Add view introspection query to `internal/tigerfs/db/query.go`:
   ```go
   func (c *Client) GetViewColumns(ctx context.Context, schema, view string) ([]string, error)
   ```

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/format.go`
- `internal/tigerfs/fuse/synthesized/columns.go`
- `internal/tigerfs/fuse/synthesized/format_test.go`

**Files to Modify:**
- `internal/tigerfs/db/query.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestFormat
```

**Completion Criteria:**
- Format detection works by suffix and column patterns (markdown/plaintext)
- Tasks format detected but returns "not implemented" error
- Suffix takes priority over column detection
- Column role detection finds filename/body/frontmatter columns
- Unit tests pass

---

### Task 6.1.2: Implement Markdown Synthesis

**Objective:** Generate markdown files from database rows with YAML frontmatter

**Background:**
When reading a markdown-format view, TigerFS synthesizes a `.md` file with YAML frontmatter from non-filename/body columns, followed by the body content.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/markdown.go`:
   - Implement `SynthesizeMarkdown(row map[string]any, roles *ColumnRoles) ([]byte, error)`:
     - Build YAML frontmatter from frontmatter columns
     - Add `---` delimiters
     - Append body content
     - Handle empty frontmatter (no delimiters)
   - Implement `GetMarkdownFilename(row map[string]any, roles *ColumnRoles, pk string) string`:
     - Return filename from column value (used as-is; no extension auto-appended)
     - Handle duplicates with PK suffix

2. Add YAML dependency to `go.mod`:
   ```
   gopkg.in/yaml.v3
   ```

3. Implement frontmatter ordering:
   - Output keys in column order (as defined in view)
   - Handle various types: strings, numbers, arrays, timestamps

4. Handle edge cases:
   - NULL filename → hide row or show as `<null>-{pk}.md`
   - Invalid filesystem characters → replace with `-`

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/markdown.go`
- `internal/tigerfs/fuse/synthesized/markdown_test.go`

**Files to Modify:**
- `go.mod` (add yaml dependency)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestMarkdown
```

**Completion Criteria:**
- Markdown synthesis produces valid YAML frontmatter + body
- Empty frontmatter omits `---` delimiters
- Filename generation handles duplicates and edge cases
- Unit tests pass

---

### Task 6.1.3: Implement Markdown Parsing (Write Support)

**Objective:** Parse edited markdown files back to column values for UPDATE/INSERT

**Background:**
When a user edits and saves a markdown file, TigerFS must parse it back into column values and execute the appropriate SQL operation.

**Steps:**
1. Add to `internal/tigerfs/fuse/synthesized/markdown.go`:
   - Implement `ParseMarkdown(content []byte) (*ParsedMarkdown, error)`:
     ```go
     type ParsedMarkdown struct {
         Frontmatter map[string]any
         Body        string
     }
     ```
     - Split on `---` delimiters
     - Parse YAML frontmatter
     - Extract body content

2. Implement `MapToColumns(parsed *ParsedMarkdown, roles *ColumnRoles) (map[string]any, error)`:
   - Map frontmatter keys to column names
   - Set body column value
   - Return error for unknown frontmatter keys

3. Handle unknown frontmatter keys:
   - Return error with list of unknown keys
   - Hint to remove keys or update view

4. Handle filename changes:
   - Parse old filename from path
   - If frontmatter includes new filename, update that column

**Files to Modify:**
- `internal/tigerfs/fuse/synthesized/markdown.go`
- `internal/tigerfs/fuse/synthesized/markdown_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestMarkdownParse
```

**Completion Criteria:**
- Parsing splits frontmatter from body correctly
- Column mapping handles all frontmatter types
- Unknown keys produce helpful error
- Round-trip (synthesize → parse) preserves data
- Unit tests pass

---

### Task 6.1.4: Implement Plain Text Format

**Objective:** Support plain text files (no frontmatter)

**Background:**
Plain text format is simpler than markdown—just filename + body, no YAML frontmatter.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/plaintext.go`:
   - Implement `SynthesizePlainText(row map[string]any, roles *ColumnRoles) ([]byte, error)`:
     - Return body content directly (no frontmatter)
   - Implement `ParsePlainText(content []byte) string`:
     - Return content as body (trivial)
   - Implement `GetPlainTextFilename(row map[string]any, roles *ColumnRoles, pk string) string`:
     - Return filename with `.txt` extension

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/plaintext.go`
- `internal/tigerfs/fuse/synthesized/plaintext_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestPlainText
```

**Completion Criteria:**
- Plain text synthesis returns body only
- Filename uses `.txt` extension
- Unit tests pass

---

### Task 6.1.5: Implement .build/ Scaffolding Handler (Markdown/PlainText)

**Objective:** Create table + view from scratch via filesystem (markdown/plaintext only)

**Background:**
`.build/` provides full scaffolding—creates the underlying table with default columns, then creates the synthesized view on top. This is the primary way to create new synthesized apps. Tasks format support added in 6.2.3.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/build_node.go`:
   - Implement `BuildDirNode` for `schema/.build/` directory:
     - `Readdir()`: Empty (write-only)
     - `Create()`: Handle new app creation

2. Implement `BuildFileNode`:
   - `Write()`: Parse format (or JSON5 with custom columns), create table + view
   - For "tasks" format, return error "tasks format not yet implemented" (handled in 6.2.3)

3. Implement default table schemas in `internal/tigerfs/db/ddl.go`:
   ```go
   func (c *Client) CreateMarkdownTable(ctx context.Context, schema, tableName string, extraColumns map[string]string) error
   func (c *Client) CreatePlainTextTable(ctx context.Context, schema, tableName string) error
   // CreateTasksTable added in 6.2.3
   ```

4. Implement naming convention:
   - View gets clean name (e.g., `posts`)
   - Table gets underscore prefix (e.g., `_posts`)

5. Create `modified_at` trigger:
   ```sql
   CREATE FUNCTION update_modified_at() RETURNS TRIGGER AS $$
   BEGIN
       NEW.modified_at := now();
       RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;
   ```

6. Handle errors:
   - View already exists → error with hint
   - Table already exists → error with hint
   - "tasks" format → "not yet implemented"

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/build_node.go`
- `internal/tigerfs/fuse/synthesized/build_node_test.go`

**Files to Modify:**
- `internal/tigerfs/db/ddl.go`
- `internal/tigerfs/fuse/schema.go` (expose `.build/` directory)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestBuildNode
```

**Completion Criteria:**
- `.build/` directory appears under schemas
- Writing "markdown" or "txt" creates table + view
- Writing "tasks" returns "not yet implemented" error
- Default columns match ADR-008 specification
- Naming convention applied correctly
- `modified_at` trigger created
- Unit tests pass

---

### Task 6.1.6: Implement .format/ Handler (Markdown/PlainText)

**Objective:** Create views via filesystem interface for existing tables (markdown/plaintext only)

**Background:**
Users can create synthesized views by writing to `.format/` directory instead of using SQL directly. TigerFS generates the appropriate CREATE VIEW statement. Use this when you already have a table with content. Tasks format support added in 6.2.3.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/format_node.go`:
   - Implement `FormatDirNode` for `table/.format/` directory:
     - `Readdir()`: List existing format files (markdown, txt)
     - `Lookup()`: Return FormatFileNode for each format
     - `Create()`: Handle new format creation

2. Implement `FormatFileNode`:
   - `Read()`: Return current config as JSON:
     ```json
     {
       "format": "markdown",
       "view": "posts_md",
       "columns": {"filename": "slug", "body": "content"},
       "frontmatter": ["author", "date", "tags"],
       "source": "convention"
     }
     ```
   - `Write()`: Parse JSON5 config, generate and execute CREATE VIEW
   - For "tasks" format, return error "tasks format not yet implemented" (handled in 6.2.3)

3. Add JSON5 dependency to `go.mod`:
   ```
   github.com/yosuke-furukawa/json5
   ```

4. Implement view generation in `internal/tigerfs/db/ddl.go`:
   ```go
   func (c *Client) CreateSynthesizedView(ctx context.Context, schema, table, viewName string, roles *ColumnRoles, format SynthFormat) error
   ```
   - Generate CREATE VIEW with column aliases
   - Add COMMENT with format marker (e.g., `tigerfs:md`)

5. Handle existing view:
   - Return warning if view exists with custom SQL
   - Support `--force` to overwrite

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/format_node.go`
- `internal/tigerfs/fuse/synthesized/format_node_test.go`

**Files to Modify:**
- `internal/tigerfs/db/ddl.go`
- `internal/tigerfs/fuse/table.go` (expose `.format/` directory)
- `go.mod` (add json5 dependency)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestFormatNode
```

**Completion Criteria:**
- `.format/` directory appears under tables
- Writing "markdown" or "txt" config creates view
- Writing "tasks" returns "not yet implemented" error
- Reading format returns current config
- View comment includes format marker
- Unit tests pass

---

### Task 6.1.7: Implement SynthesizedViewNode (Markdown/PlainText)

**Objective:** Create FUSE node that presents synthesized files for a view (markdown/plaintext only)

**Background:**
When TigerFS detects a synthesized view, it should present the view as a directory of synthesized files (e.g., `.md` files) instead of native row-as-directory format. Tasks format handling added in 6.2.4.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/view_node.go`:
   - Implement `SynthesizedViewNode`:
     ```go
     type SynthesizedViewNode struct {
         fs.Inode
         fs      *FS
         schema  string
         view    string
         format  SynthFormat
         roles   *ColumnRoles
     }
     ```
   - `Readdir()`: Query view, return list of synthesized filenames
   - `Lookup()`: Return `SynthesizedFileNode` for each file
   - For FormatTasks, return error "tasks format not yet implemented" (handled in 6.2.4)

2. Implement `SynthesizedFileNode`:
   - `Read()`: Query row, synthesize content based on format
   - `Write()`: Parse content, execute UPDATE
   - `Getattr()`: Return file size, mode 0644

3. Handle file operations:
   - Create (write new file) → INSERT
   - Delete (rm file) → DELETE
   - Rename (mv file) → UPDATE filename column

4. Integrate with table discovery:
   - In `internal/tigerfs/fuse/table.go`, detect synthesized views
   - Route to `SynthesizedViewNode` instead of `TableNode`

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/view_node.go`
- `internal/tigerfs/fuse/synthesized/file_node.go`
- `internal/tigerfs/fuse/synthesized/view_node_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/schema.go` (route synthesized views)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestViewNode
```

**Completion Criteria:**
- Synthesized views appear as directories of files (markdown/plaintext)
- Files have correct extensions (.md, .txt)
- Read returns synthesized content
- Write parses and updates database
- Create/delete/rename work
- FormatTasks returns "not yet implemented" error
- Unit tests pass

---

### Task 6.1.8: Integration Tests for Markdown/PlainText

**Objective:** End-to-end tests for markdown and plain text synthesized app functionality

**Background:**
Verify that markdown and plain text formats work correctly through the full filesystem interface. Tasks format tests are in 6.2.6.

**Steps:**
1. Create `test/integration/synthesized_markdown_test.go`:
   - Test markdown via `.format/`:
     ```bash
     # Create view on existing table
     echo "markdown" > /mnt/db/posts/.format/markdown
     ls /mnt/db/posts_md/
     cat /mnt/db/posts_md/hello-world.md
     # Edit file
     echo "new content" >> /mnt/db/posts_md/hello-world.md
     # Create new file
     echo "---\ntitle: New\n---\nContent" > /mnt/db/posts_md/new-post.md
     ```
   - Test markdown via `.build/`:
     ```bash
     echo "markdown" > /mnt/db/public/.build/notes
     ls /mnt/db/public/notes/
     cat /mnt/db/public/_notes/  # Native access
     ```
   - Test plain text:
     ```bash
     echo "txt" > /mnt/db/public/.build/snippets
     echo "Hello world" > /mnt/db/public/snippets/hello.txt
     cat /mnt/db/public/snippets/hello.txt
     ```

2. Test error cases:
   - Unknown frontmatter keys
   - Missing required columns for format detection
   - View already exists
   - "tasks" format returns "not yet implemented" error

3. Test config reading:
   ```bash
   cat /mnt/db/posts/.format/markdown  # Returns JSON config
   ```

**Files to Create:**
- `test/integration/synthesized_markdown_test.go`

**Verification:**
```bash
go test ./test/integration/... -v -run TestSynthesizedMarkdown
```

**Completion Criteria:**
- Markdown and plain text formats work via `.format/` and `.build/`
- Read, write, create, delete operations work
- Error cases handled gracefully
- "tasks" format returns appropriate "not yet implemented" error
- Integration tests pass

---

### Task 6.1.9: Add Extra Headers JSONB Column for User-Defined Frontmatter

**Objective:** Allow users to add arbitrary key-value pairs to markdown frontmatter that are stored in a `headers` JSONB column

**Background:**
The markdown synthesized app has fixed frontmatter columns (title, author). Users need the ability to add custom key-value pairs (tags, draft status, categories, etc.) without altering the database schema. A `headers JSONB` column stores these extra pairs, merged into YAML frontmatter during synthesis and collected from unknown keys during parsing.

**Steps:**
1. Add `ExtraHeaders` field to `ColumnRoles` with `extraHeadersConventions = []string{"headers"}` convention
2. Add `headers JSONB DEFAULT '{}'::jsonb` column to `GenerateMarkdownTableSQL` in `.build/`
3. Merge extra headers into YAML frontmatter during synthesis (after known columns, alphabetically sorted)
4. Collect unknown frontmatter keys into extra headers during parsing (overwrite semantics)
5. Update demo data (`scripts/demo/init.sql` and `scripts/demo/seed.sh`) with headers column and sample data
6. Unit tests for all new behavior

**Files to Modify:**
- `internal/tigerfs/fs/synth/columns.go`
- `internal/tigerfs/fs/synth/build.go`
- `internal/tigerfs/fs/synth/markdown.go`
- `internal/tigerfs/fs/synth/format_handler.go` (TODO for reuse existing schema)
- `scripts/demo/init.sql`
- `scripts/demo/seed.sh`

**Files to Modify (tests):**
- `internal/tigerfs/fs/synth/columns_test.go`
- `internal/tigerfs/fs/synth/build_test.go`
- `internal/tigerfs/fs/synth/markdown_test.go`

**Verification:**
```bash
go test -v -run "^TestSynth_" ./internal/tigerfs/fs/synth/... ./internal/tigerfs/fs/...
go test ./...
```

**Completion Criteria:**
- `headers JSONB` column created by `.build/` for markdown apps
- Extra frontmatter keys round-trip through synthesis and parsing
- Overwrite semantics: removing a key from frontmatter removes it from the DB
- Tables without `headers` column preserve original behavior (reject unknown keys)
- All new tests use `TestSynth_` prefix

---

## 6.2: Tasks

### Overview

Extend synthesized apps to support the tasks format, which adds:
- Special filename format: `{number}-{name}-{status}.md`
- PostgreSQL triggers for shift-on-insert and status timestamps
- `.renumber` file for explicit gap compaction
- `.add` file for easy task creation

**Prerequisite:** Phase 6.1 (Markdown & Plain Text) must be complete.

---

### Task 6.2.1: Implement Tasks Filename Synthesis

**Objective:** Generate task filenames in `{number}-{name}-{status}.md` format with dynamic padding

**Background:**
Tasks format uses composite filenames with number, name, and status symbol. Status is stored as word but displayed as symbol in filename. Numbers use integers only (no letters) and are zero-padded per-level for correct `ls` sorting.

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/tasks.go`:
   - Define status mapping:
     ```go
     var StatusToSymbol = map[string]string{
         "todo":  "o",
         "doing": "~",
         "done":  "x",
     }
     var SymbolToStatus = map[string]string{
         "o": "todo",
         "~": "doing",
         "x": "done",
     }
     var StatusAliases = map[string]string{
         "pending": "todo", "new": "todo",
         "active": "doing", "wip": "doing",
         "complete": "doing", "finished": "done",
     }
     ```

2. Implement dynamic per-level padding:
   - `ComputePaddingWidths(numbers []string) map[int]int`:
     - Parse each number into components (split on `.`)
     - Track max value at each level
     - Return width needed per level: `width = len(strconv.Itoa(max))`
   - Example: numbers `["1", "2", "1.15", "1.2"]` → `{0: 1, 1: 2}` (level 0 max=2, level 1 max=15)

3. Implement filename synthesis:
   - `SynthesizeTaskFilename(row map[string]any, roles *ColumnRoles, widths map[int]int) string`:
     - Pad each number component to its level's width
     - Format: `{padded_number}-{name}-{symbol}.md`
     - Example: number=`"1.2"`, widths=`{0:1, 1:2}` → `"1.02-task-o.md"`

4. Implement filename parsing with validation:
   - `ParseTaskFilename(filename string) (number, name, status string, err error)`:
     - Extract components from filename
     - Strip leading zeros from number components
     - **Validate number format** (must match `^[1-9][0-9]*(\.[1-9][0-9]*)*$`)
     - Convert symbol to canonical status word
     - Example: `"1.02-task-o.md"` → number=`"1.2"`, name=`"task"`, status=`"todo"`
   - On invalid number, log error for user feedback before returning errno:
     ```go
     if !isValidTaskNumber(number) {
         logging.Error("invalid task number",
             zap.String("number", number),
             zap.String("hint", "must be positive integers (e.g., 1, 1.2, 10.15)"))
         return "", "", "", fmt.Errorf("invalid task number: %s", number)
     }
     ```
   - User sees: `{"message":"invalid task number","number":"a","hint":"..."}`

5. Implement `SynthesizeTaskMarkdown(row map[string]any, roles *ColumnRoles) ([]byte, error)`:
   - Frontmatter: number (unpadded), name, status, assignee, timestamps
   - Body: description/body column
   - Reuses markdown synthesis from 6.1.2 for the actual content generation

6. Implement status normalization:
   - Accept symbol or word on input
   - Store canonical word (todo/doing/done)

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/tasks.go`
- `internal/tigerfs/fuse/synthesized/tasks_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestTasks
```

**Completion Criteria:**
- Task filenames use correct format with per-level padding
- Padding widths computed correctly from all numbers in view
- Status symbol/word conversion works both directions
- Aliases normalized to canonical values
- Frontmatter includes all task fields (unpadded numbers)
- Filename parsing strips leading zeros correctly
- Invalid numbers rejected with user-friendly error (logged to stderr)
- Unit tests pass

---

### Task 6.2.2: Implement Tasks Triggers and Compact Function

**Objective:** Generate PostgreSQL triggers for shift-on-insert and status timestamps, plus explicit compact function

**Background:**
Tasks format requires automatic shift-on-insert (prevent overwrites) and status timestamps. Gap closing is NOT automatic—users explicitly call compact via `.renumber` file (handled in 6.2.4).

**Steps:**
1. Add trigger generation to `internal/tigerfs/db/ddl.go`:
   - `CreateShiftOnInsertTrigger(ctx, schema, table string)`:
     ```sql
     CREATE FUNCTION {schema}.{table}_shift_numbers() RETURNS TRIGGER AS $$
     DECLARE
         r RECORD;
         new_sort INT[];
     BEGIN
         -- Compute sort array for NEW.number
         new_sort := string_to_array(NEW.number, '.')::int[];

         -- If target number is occupied, shift siblings down
         -- Process one row at a time in descending order to avoid UNIQUE conflicts
         -- IMPORTANT: Use number_sort for comparison, not TEXT (TEXT sort is wrong: '2' > '10')
         IF EXISTS (SELECT 1 FROM {schema}.{table} WHERE number = NEW.number AND id != NEW.id) THEN
             FOR r IN
                 SELECT * FROM {schema}.{table}
                 WHERE number_sort >= new_sort
                   AND parse_parent(number) = parse_parent(NEW.number)
                   AND id != NEW.id
                 ORDER BY number_sort DESC
             LOOP
                 UPDATE {schema}.{table} SET number = increment_number(r.number) WHERE id = r.id;
             END LOOP;
         END IF;
         RETURN NEW;
     END;
     $$ LANGUAGE plpgsql;

     CREATE TRIGGER {table}_shift_trigger
         BEFORE INSERT OR UPDATE OF number ON {schema}.{table}
         FOR EACH ROW EXECUTE FUNCTION {schema}.{table}_shift_numbers();
     ```
   - `CreateStatusTimestampTrigger(ctx, schema, table string)`:
     ```sql
     CREATE FUNCTION {table}_status_timestamp() RETURNS TRIGGER AS $$
     BEGIN
         IF NEW.status != OLD.status THEN
             CASE NEW.status
                 WHEN 'todo' THEN NEW.todo_at := now();
                 WHEN 'doing' THEN NEW.doing_at := now();
                 WHEN 'done' THEN NEW.done_at := now();
             END CASE;
         END IF;
         RETURN NEW;
     END;
     $$ LANGUAGE plpgsql;
     ```

2. Implement hierarchical number helper functions (integers only):
   - `parse_parent(number TEXT) RETURNS TEXT`: Extract parent (e.g., `'1.2.3'` → `'1.2'`, `'1'` → `''`)
   - `increment_number(number TEXT) RETURNS TEXT`: Increment last component (e.g., `'1.2'` → `'1.3'`, `'9'` → `'10'`)
   ```sql
   CREATE FUNCTION parse_parent(number TEXT) RETURNS TEXT AS $$
   BEGIN
       IF position('.' in number) = 0 THEN
           RETURN '';
       END IF;
       RETURN left(number, length(number) - position('.' in reverse(number)));
   END;
   $$ LANGUAGE plpgsql IMMUTABLE;

   CREATE FUNCTION increment_number(number TEXT) RETURNS TEXT AS $$
   DECLARE
       parts TEXT[];
       last_part INT;
   BEGIN
       parts := string_to_array(number, '.');
       last_part := parts[array_length(parts, 1)]::INT + 1;
       parts[array_length(parts, 1)] := last_part::TEXT;
       RETURN array_to_string(parts, '.');
   END;
   $$ LANGUAGE plpgsql IMMUTABLE;
   ```

   **Note:** `decrement_number` is not needed - compact function assigns sequential numbers directly.

3. Add CHECK constraint for valid task numbers:
   ```sql
   -- Numbers must be positive integers (starting at 1) separated by dots
   -- Valid: '1', '1.2', '10.15.3'
   -- Invalid: '0', '01', 'abc', '1.a', '-1', '1.0'
   ALTER TABLE {schema}.{table}
       ADD CONSTRAINT {table}_number_format
       CHECK (number ~ '^[1-9][0-9]*(\.[1-9][0-9]*)*$');
   ```

4. Implement compact function for explicit renumbering (called via `.renumber` file in 6.2.4):
   ```sql
   CREATE FUNCTION {schema}.{table}_compact(scope TEXT DEFAULT NULL) RETURNS void AS $$
   DECLARE
       r RECORD;
       current_parent TEXT;
       counter INT;
       parent_filter TEXT;
   BEGIN
       -- Compact numbers within each parent group
       -- If scope provided, only compact that subtree
       parent_filter := COALESCE(scope, '');

       FOR current_parent IN
           SELECT DISTINCT parse_parent(number) as parent
           FROM {schema}.{table}
           WHERE (scope IS NULL OR number LIKE scope || '%' OR number LIKE scope || '.%')
           ORDER BY parent
       LOOP
           counter := 1;
           FOR r IN
               SELECT * FROM {schema}.{table}
               WHERE parse_parent(number) = current_parent
               ORDER BY number_sort
           LOOP
               IF current_parent = '' THEN
                   UPDATE {schema}.{table} SET number = counter::TEXT WHERE id = r.id;
               ELSE
                   UPDATE {schema}.{table} SET number = current_parent || '.' || counter::TEXT WHERE id = r.id;
               END IF;
               counter := counter + 1;
           END LOOP;
       END LOOP;
   END;
   $$ LANGUAGE plpgsql;
   ```

**Files to Modify:**
- `internal/tigerfs/db/ddl.go`

**Files to Create:**
- `internal/tigerfs/db/ddl_tasks_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/db/... -v -run TestTasksTriggers
```

**Completion Criteria:**
- Shift trigger uses `number_sort` for comparison (not TEXT - TEXT sort is wrong)
- Shift trigger works with UNIQUE constraint
- Status timestamp trigger created and works
- Compact function created and closes gaps correctly
- Compact function respects scope parameter
- CHECK constraint validates number format (positive integers, no leading zeros)
- Hierarchical number functions handle integers at all depths
- Unit tests pass

**Future extension:** Support `echo "flatten" > .renumber` to collapse hierarchy (e.g., `1.1, 1.2, 2.1` → `1, 2, 3`).

---

### Task 6.2.3: Extend .build/ and .format/ for Tasks

**Objective:** Add tasks format support to `.build/` and `.format/` handlers

**Background:**
The `.build/` and `.format/` handlers from 6.1.5 and 6.1.6 return "not yet implemented" for tasks format. This task adds full support.

**Steps:**
1. Update `internal/tigerfs/fuse/synthesized/build_node.go`:
   - Handle "tasks" format in `Write()`:
     - Create tasks table with required columns
     - Create triggers and helper functions
     - Create synthesized view

2. Implement `CreateTasksTable` in `internal/tigerfs/db/ddl.go`:
   ```go
   func (c *Client) CreateTasksTable(ctx context.Context, schema, tableName string) error
   ```
   - Creates table with:
     ```sql
     CREATE TABLE {schema}.{tableName} (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       number TEXT UNIQUE NOT NULL
           CHECK (number ~ '^[1-9][0-9]*(\.[1-9][0-9]*)*$'),
       number_sort INT[] GENERATED ALWAYS AS (string_to_array(number, '.')::int[]) STORED,
       name TEXT NOT NULL,
       description TEXT,
       status TEXT DEFAULT 'todo',
       assignee TEXT,
       created_at TIMESTAMPTZ DEFAULT now(),
       modified_at TIMESTAMPTZ DEFAULT now(),
       todo_at TIMESTAMPTZ,
       doing_at TIMESTAMPTZ,
       done_at TIMESTAMPTZ
     );
     CREATE INDEX ON {schema}.{tableName} (number_sort);
     ```
   - Installs helper functions if not exists
   - Creates shift trigger
   - Creates status timestamp trigger
   - Creates compact function

3. Update `internal/tigerfs/fuse/synthesized/format_node.go`:
   - Handle "tasks" format in `Write()`:
     - Detect column roles for tasks
     - Create synthesized view with tasks format marker
     - Install triggers on source table

4. Update `internal/tigerfs/fuse/synthesized/columns.go`:
   - Implement tasks column role detection:
     - Number conventions: `number`, `sort_key`, `order`
     - Name conventions: `name`, `title`
     - Status conventions: `status`, `state`
     - Assignee conventions: `assignee`, `owner`, `assigned_to`

**Files to Modify:**
- `internal/tigerfs/fuse/synthesized/build_node.go`
- `internal/tigerfs/fuse/synthesized/format_node.go`
- `internal/tigerfs/fuse/synthesized/columns.go`
- `internal/tigerfs/db/ddl.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestBuildTasks
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestFormatTasks
```

**Completion Criteria:**
- `echo "tasks" > .build/work` creates full tasks app
- `echo "tasks" > table/.format/tasks` creates tasks view on existing table
- Helper functions, triggers, and compact function installed
- Column role detection works for tasks columns
- Unit tests pass

---

### Task 6.2.4: Extend SynthesizedViewNode for Tasks

**Objective:** Add tasks format handling to SynthesizedViewNode, including `.renumber` file

**Background:**
The SynthesizedViewNode from 6.1.7 returns "not yet implemented" for tasks format. This task adds full support including the `.renumber` control file.

**Steps:**
1. Update `internal/tigerfs/fuse/synthesized/view_node.go`:
   - Handle FormatTasks in `Readdir()`:
     - Query view and compute padding widths
     - Generate task filenames with dynamic padding
   - Handle FormatTasks in `Lookup()`:
     - Parse task filename to extract number, name, status
     - Return `SynthesizedFileNode` with tasks format

2. Update `internal/tigerfs/fuse/synthesized/file_node.go`:
   - Handle FormatTasks in `Read()`:
     - Use `SynthesizeTaskMarkdown()` for content
   - Handle FormatTasks in `Write()`:
     - Parse markdown content
     - Extract filename changes (number, name, status from new filename)
     - Execute UPDATE with all changed fields
   - Handle FormatTasks in `Rename()`:
     - Parse old and new filenames
     - Update number, name, and/or status as needed

3. Implement `.renumber` file handler:
   - Add `RenumberFileNode` that appears in tasks view directories
   - `Write()` implementation:
     - Empty content or whitespace → `{table}_compact(NULL)` (compact all)
     - Content like "2" → `{table}_compact('2')` (compact subtree)
     - Content like "2.1" → `{table}_compact('2.1')` (compact sub-subtree)
   - Log success/failure for user feedback:
     ```go
     // On success:
     logging.Warn("tasks renumbered",
         zap.String("scope", scope),
         zap.Int("count", rowsAffected))

     // On invalid scope:
     logging.Error("invalid renumber scope",
         zap.String("scope", scope),
         zap.String("hint", "must be a valid task number prefix (e.g., 2, 2.1)"))
     return syscall.EINVAL
     ```

**Files to Modify:**
- `internal/tigerfs/fuse/synthesized/view_node.go`
- `internal/tigerfs/fuse/synthesized/file_node.go`

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/renumber_node.go`

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestTasksViewNode
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestRenumber
```

**Completion Criteria:**
- Tasks views display files with correct `{number}-{name}-{status}.md` format
- Dynamic padding adjusts based on max number at each level
- Read returns task markdown with proper frontmatter
- Write/rename update number, name, and status correctly
- `.renumber` file appears in tasks directories
- `touch .renumber` compacts all numbers
- `echo "2" > .renumber` compacts only 2.* subtree
- User feedback logged on renumber success/failure
- Unit tests pass

---

### Task 6.2.5: Implement .add File Handler

**Objective:** Enable easy task creation via `.add` file

**Background:**
Writing to `.add` creates a new task with the next available number, simplifying task creation without needing to know the current numbering.

**Basic Usage:**
```bash
echo "Set up CI pipeline" > tasks/.add
# Creates: 9-set-up-ci-pipeline-o.md (if highest was 8)
# Or: 4.4-set-up-ci-pipeline-o.md (if highest was 4.3)
```

**Steps:**
1. Create `internal/tigerfs/fuse/synthesized/add_node.go`:
   - Implement `AddFileNode` that appears in tasks view directories
   - `Write()` implementation:
     - Parse content as task description (basic case)
     - Determine "next number" based on current highest:
       - If all top-level (1, 2, 3), next is 4
       - If highest is nested (4.3), next is 4.4 (same parent)
     - Generate slug from description (lowercase, hyphens)
     - Insert with status "todo"
     - Log created task info for user feedback

2. **Advanced syntax (TBD):**
   - Potential future formats:
     ```bash
     echo "[4] description" > .add        # Add under 4.*
     echo "[4][done] description" > .add  # Add under 4.* with status
     echo "[done] description" > .add     # Add at top level with status
     ```
   - Implementation details to be determined when this task is planned

**Files to Create:**
- `internal/tigerfs/fuse/synthesized/add_node.go`
- `internal/tigerfs/fuse/synthesized/add_node_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/synthesized/view_node.go` (expose `.add` in tasks directories)

**Verification:**
```bash
go test ./internal/tigerfs/fuse/synthesized/... -v -run TestAddNode
```

**Completion Criteria:**
- `.add` file appears in tasks directories
- Writing description creates task with next number
- Next number follows current highest (top-level or nested)
- Slug generated from description
- Status defaults to "todo"
- User feedback logged on creation
- Unit tests pass

---

### Task 6.2.6: Integration Tests for Tasks

**Objective:** End-to-end tests for tasks format functionality

**Background:**
Verify that tasks format works correctly through the full filesystem interface, including triggers, renumbering, and `.add` file.

**Steps:**
1. Create `test/integration/synthesized_tasks_test.go`:
   - Test tasks via `.build/`:
     ```bash
     echo "tasks" > /mnt/db/public/.build/work
     echo "First task" > /mnt/db/public/work/1-setup-o.md
     mv /mnt/db/public/work/1-setup-o.md /mnt/db/public/work/1-setup-~.md
     # Verify doing_at timestamp set
     cat /mnt/db/public/work/1-setup-~.md
     ```
   - Test shift-on-insert:
     ```bash
     echo "New task" > /mnt/db/public/work/1-new-task-o.md
     ls /mnt/db/public/work/  # Verify numbering shifted
     ```
   - Test explicit renumbering via `.renumber`:
     ```bash
     # Create tasks with gaps
     echo "Task 1" > /mnt/db/public/work/1-first-o.md
     echo "Task 3" > /mnt/db/public/work/3-third-o.md
     echo "Task 5" > /mnt/db/public/work/5-fifth-o.md
     ls /mnt/db/public/work/  # Shows 1, 3, 5 (gaps)

     # Compact all
     touch /mnt/db/public/work/.renumber
     ls /mnt/db/public/work/  # Shows 1, 2, 3 (compacted)

     # Test scoped compact
     echo "Sub 1" > /mnt/db/public/work/2.1-sub1-o.md
     echo "Sub 3" > /mnt/db/public/work/2.3-sub3-o.md
     echo "2" > /mnt/db/public/work/.renumber  # Compact only 2.*
     ls /mnt/db/public/work/  # 2.1, 2.2 (2.* compacted)
     ```
   - Test dynamic filename padding:
     ```bash
     # With max < 10 at level 0, no padding needed
     ls /mnt/db/public/work/  # 1-task-o.md, 2-task-o.md

     # Add task 10+
     echo "Task" > /mnt/db/public/work/15-task-o.md
     ls /mnt/db/public/work/  # 01-task-o.md, 02-task-o.md, 15-task-o.md (padded)
     ```
   - Test `.add` file:
     ```bash
     echo "New feature" > /mnt/db/public/work/.add
     ls /mnt/db/public/work/  # Verify new task with next number
     ```

2. Test error cases:
   - Invalid task number format
   - Invalid status symbol
   - Renumber with invalid scope

**Files to Create:**
- `test/integration/synthesized_tasks_test.go`

**Verification:**
```bash
go test ./test/integration/... -v -run TestSynthesizedTasks
```

**Completion Criteria:**
- Tasks format works via `.build/` and `.format/`
- Shift-on-insert trigger functions correctly
- Status timestamp trigger functions correctly
- `.renumber` file compacts all when touched
- `.renumber` file compacts scoped subtree when written to
- Dynamic filename padding adjusts based on max number at each level
- `.add` file creates tasks with next number
- Error cases handled gracefully
- Integration tests pass

---

## 6.3: Directory Hierarchies

### Overview

Add subdirectory support within synthesized views using filename-based hierarchy with a `filetype` column. See ADR-011 for full design rationale.

**Key idea:** Slashes in the filename column create directory structure. Every directory has an explicit row with `filetype='directory'`. A `resolveSynthHierarchy` method converts multi-segment paths from `PathColumn` to `PathRow`, so all existing synth hooks handle the rest.

---

### Task 6.3.1: Add RawSubPath to ParsedPath

**Objective:** Capture all path segments after the table name for hierarchy reconstruction.

**Steps:**
1. Add `RawSubPath []string` field to `ParsedPath` struct in `internal/tigerfs/fs/path.go`
2. In `processRowOrColumn`, set `result.RawSubPath = append([]string{}, remaining...)` before existing PK/Column logic

**Files Modified:**
- `internal/tigerfs/fs/path.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestParsePath
```

---

### Task 6.3.2: Detect filetype Column in Synth Column Roles

**Objective:** Detect the `filetype` column and flag views that support hierarchy.

**Steps:**
1. Add `Filetype string` to `ColumnRoles` in `internal/tigerfs/fs/synth/columns.go`
2. Detect `"filetype"` column in `DetectColumnRoles`
3. Add `SupportsHierarchy bool` to `ViewInfo` in `internal/tigerfs/fs/synth/cache.go`
4. Set `SupportsHierarchy = true` when `Filetype` column exists

**Files Modified:**
- `internal/tigerfs/fs/synth/columns.go`
- `internal/tigerfs/fs/synth/cache.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/synth/... -run TestColumnRoles
```

---

### Task 6.3.3: Allow Slashes in sanitizeFilename

**Objective:** Allow `/` characters in synth filenames for hierarchy paths.

**Steps:**
1. Remove `/` from the character replacement in `sanitizeFilename` in `internal/tigerfs/fs/synth/markdown.go`

**Files Modified:**
- `internal/tigerfs/fs/synth/markdown.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/synth/... -run TestSanitize
```

---

### Task 6.3.4: Update .build/ SQL Generation for filetype Column

**Objective:** Generated CREATE TABLE includes `filetype` column and compound UNIQUE constraint.

**Steps:**
1. Add `filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory'))` to generated CREATE TABLE in `internal/tigerfs/fs/build.go`
2. Change `UNIQUE(filename)` to `UNIQUE(filename, filetype)`

**Files Modified:**
- `internal/tigerfs/fs/build.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestBuild
```

---

### Task 6.3.5: Implement resolveSynthHierarchy + Core Dispatch Hooks

**Objective:** Add the core path rewriting method and minimal dispatch hooks in operations/write.

**Steps:**
1. Add `resolveSynthHierarchy` method to `internal/tigerfs/fs/synth_ops.go` — converts `PathColumn` to `PathRow` with full subpath as `PrimaryKey` for views with `SupportsHierarchy`
2. Add one-liner `resolveSynthHierarchy` call after `parsePath` in: ReadDir, Stat, ReadFile, WriteFile, Delete, Mkdir, Rename (×2) in `operations.go` and `write.go`
3. Add dispatch hook in `readDirRow` for hierarchical synth views
4. Add dispatch hook in `Mkdir` PathRow case for hierarchical synth views

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go` (hierarchy logic)
- `internal/tigerfs/fs/operations.go` (one-liner hooks)
- `internal/tigerfs/fs/write.go` (one-liner hooks)

**Verification:**
```bash
go test ./internal/tigerfs/fs/...
```

---

### Task 6.3.6: Implement readDirSynthHierarchical

**Objective:** List directory contents for hierarchical synth views.

**Steps:**
1. Add `readDirSynthHierarchical` method to `synth_ops.go` — queries rows with filename prefix, filters to immediate children, returns entries with correct types (directory vs file with synth extension)
2. Extend `readDirSynthView` to show directories at root level when hierarchy is supported

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.7: Extend statSynthFile for Directories

**Objective:** Return correct stat info for directory and file entries in hierarchical views.

**Steps:**
1. When `SupportsHierarchy`: check for directory row (`filetype='directory'`) first, then file row
2. Directory takes priority over file when same base name exists

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.8: Extend writeSynthFile with Auto-Create Parent Dirs

**Objective:** Automatically create parent directory rows when writing nested files.

**Steps:**
1. When writing a nested path (e.g., `projects/web/todo`), INSERT directory rows for all ancestors with `ON CONFLICT DO NOTHING`

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.9: Implement mkdirSynth

**Objective:** Create directory rows via `mkdir`.

**Steps:**
1. INSERT directory row with `filetype='directory'`
2. Auto-create parent directories
3. Return `ErrExists` (EEXIST) if directory already exists

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.10: Implement rmdirSynth (via deleteSynthFile)

**Objective:** Handle directory deletion with non-empty check.

**Steps:**
1. When deleting a path matching a directory row, check for children (`filename LIKE $1 || '/%'`)
2. If children exist → return `ENOTEMPTY`
3. If empty → DELETE the directory row

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.11: Extend renameSynthFile for Directory Rename

**Objective:** Atomic directory rename that updates all descendants.

**Steps:**
1. Check if old path is a directory → directory rename
2. Single atomic SQL: `UPDATE SET filename = newPrefix || substr(filename, length(oldPrefix) + 1) WHERE filename = old OR filename LIKE old || '/%'`

**Files Modified:**
- `internal/tigerfs/fs/synth_ops.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.12: Add Generic DB Helper Methods

**Objective:** Add reusable database methods for hierarchy operations.

**Steps:**
1. `RenameByPrefix(ctx, schema, table, col, oldPrefix, newPrefix)` — batch UPDATE
2. `HasChildrenWithPrefix(ctx, schema, table, col, prefix)` — EXISTS check
3. `InsertIfNotExists(ctx, schema, table, cols, vals)` — ON CONFLICT DO NOTHING

**Files Modified:**
- `internal/tigerfs/db/query.go`
- `internal/tigerfs/db/interfaces.go`

**Verification:**
```bash
go test ./internal/tigerfs/db/...
```

---

### Task 6.3.13: Unit Tests for Hierarchy

**Objective:** Unit tests for all hierarchy logic.

**Tests:**
- `TestSynth_ParsePathRawSubPath` — RawSubPath capture at 1, 2, 3, 4+ depths
- `TestSynth_ResolveSynthHierarchy` — PathColumn→PathRow transformation
- `TestSynth_ReadDirHierarchical_Root` — root listing with dirs + flat files
- `TestSynth_ReadDirHierarchical_Subdir` — subdirectory listings
- `TestSynth_StatDirectory` — directory stat returns IsDir=true
- `TestSynth_ReadFileHierarchical` — reading nested file content
- `TestSynth_NonHierarchicalViewUnchanged` — flat views unaffected

**Files Modified:**
- `internal/tigerfs/fs/path_test.go`
- `internal/tigerfs/fs/synth_ops_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/fs/... -run TestSynth
```

---

### Task 6.3.14: Integration Tests for Hierarchy

**Objective:** End-to-end tests for hierarchy operations via mounted filesystem.

**Tests:**
- `TestSynth_HierarchicalMkdir` — mkdir creates directory row
- `TestSynth_HierarchicalMkdirNested` — mkdir creates intermediate dirs
- `TestSynth_HierarchicalWriteFile` — write creates file + parent dirs
- `TestSynth_HierarchicalReadDir` — ls shows correct children at each level
- `TestSynth_HierarchicalStatFile` — stat nested file
- `TestSynth_HierarchicalDeleteFile` — rm file, parent persists
- `TestSynth_HierarchicalRmdir` — rmdir empty dir
- `TestSynth_HierarchicalRmdirNonEmpty` — rmdir non-empty fails ENOTEMPTY
- `TestSynth_HierarchicalRenameFile` — mv file between subdirs
- `TestSynth_HierarchicalRenameDir` — mv dir renames all descendants
- `TestSynth_DeeplyNested` — 4+ level deep paths
- `TestSynth_MixedFlatAndHierarchical` — root has both flat files and dirs
- `TestSynth_HierarchicalPlainText` — hierarchy works for plaintext
- `TestSynth_MkdirAlreadyExists` — mkdir on existing dir fails EEXIST

**Files Modified:**
- `test/integration/synthesized_test.go`

**Verification:**
```bash
go test -v -timeout 300s ./test/integration/... -run "TestSynth_Hierarchical|TestSynth_Deeply|TestSynth_Mixed|TestSynth_MkdirAlready"
```

---

### Task 6.3.15: Update ADR-011

**Objective:** Update ADR-011 from "Deferred" to "Accepted" with implementation details.

**Steps:**
1. Change status to "Accepted"
2. Update Decision section with Approach B + filetype column details
3. Document database model, key design points, consequences

**Files Modified:**
- `docs/adr/011-directory-hierarchies.md`

**Verification:** Review document.

**Completion Criteria:**
- ADR-011 reflects the chosen approach and implementation
- All Phase 6.3 tasks documented in implementation-tasks-checklist.md

---

## Phase 7: DDL Operations via Filesystem

### Overview

Add ability to create, modify, and delete tables, indexes, schemas, and views via TigerFS filesystem operations using a unified staging pattern.

**Core Pattern:** All DDL operations follow the same flow:
1. **Read `.schema`** → see dynamically-generated template
2. **Write `.schema`** → stage DDL (stored in daemon memory)
3. **Touch `.test`** → validate via BEGIN/ROLLBACK (optional)
4. **Touch `.commit`** → execute, or **`.abort`** → cancel

---

### Task 7.1: Implement Core Staging Infrastructure

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

### Task 7.2: Implement Template Generation Framework

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

### Task 7.3: Implement Schema Create/Delete Operations

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

### Task 7.4: Implement Index Create/Delete Operations

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

### Task 7.5: Implement Table Create Operations

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

### Task 7.6: Implement Table Modify Operations

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

### Task 7.7: Implement Table Delete Operations

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

### Task 7.8: Implement View Create/Delete Operations

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

### Task 7.9: Integration Tests for DDL Operations

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

### Task 7.10: Documentation for DDL Operations

**Objective:** Document DDL operations in README and spec

**Steps:**
1. Update `README.md`:
   - Add "Schema Management" section
   - Show create/modify/delete examples for tables
   - Show index and view examples
   - Explain staging pattern (read template, write DDL, commit)

2. Update `docs/spec.md`:
   - Add "DDL Operations" section
   - Document directory structure
   - Document control files (`.schema`, `.test`, `.commit`, `.abort`)
   - Document template formats
   - Document error handling

3. Add examples to `scripts/ddl/` directory:
   - `scripts/ddl/create-table.sh`
   - `scripts/ddl/modify-table.sh`
   - `scripts/ddl/delete-table.sh`

**Files to Modify:**
- `README.md`
- `docs/spec.md`

**Files to Create:**
- `scripts/ddl/create-table.sh`
- `scripts/ddl/modify-table.sh`
- `scripts/ddl/delete-table.sh`

**Completion Criteria:**
- README shows DDL examples
- Spec documents full DDL behavior
- Example scripts work
- Documentation clear for both humans and scripts

---

## Phase 8: Distribution & Release

### Core Tasks (v0.1 Release Path)

### Task 8.1: Finalize GoReleaser Configuration

**Objective:** Complete .goreleaser.yaml for releases

**Steps:**
1. Open `.goreleaser.yaml`
2. Verify build configuration:
   - All platforms: linux, darwin
   - All architectures: amd64, arm64
   - ldflags set correctly (version, build time, commit)
3. Configure archives:
   - tar.gz for Unix
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

### Task 8.2: Test Release Workflow

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

### Task 8.3: Create Unix Install Script

**Objective:** Write install.sh for Linux/macOS

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

### Task 8.4: Write Documentation

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
   - Prerequisites:
     - macOS: None required (NFS backend)
     - Linux: FUSE libraries
     - Windows: WinFsp
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

### Task 8.5: v0.1 Release

**Objective:** Release v0.1

**Steps:**
1. Run full test suite:
   ```bash
   go test ./...
   go test -race ./...
   go test -coverprofile=coverage.txt ./...
   ```
2. Verify coverage >80%
3. Test on macOS and Linux
4. Test install script on each platform
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
- Tested on macOS and Linux
- v0.1.0 tag pushed
- Release published
- Binaries available
- Documentation updated
- Release announced

---

### Deferred Tasks (Post-v0.1)

### Task 8.6: Create Windows Install Script

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

### Task 8.7: Daemon Mode Support

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

### Task 8.8: Performance Testing

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

### Task 8.9: Bug Fixes and Polish

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

## Phase 9: Shared Core Library for FUSE/NFS Feature Parity

This phase creates a new `internal/tigerfs/fs/` package as a shared core library that both FUSE and NFS backends use. This achieves full feature parity—NFS gets all capabilities that FUSE has, including writes and DDL operations.

**Reference Documents:**
- ADR: `docs/adr/009-shared-core-library.md`
- Detailed Plan: `docs/implementation/shared-core-plan.md`

**Architecture:**
```
internal/tigerfs/
├── fs/                 # NEW: Shared core (backend-agnostic)
│   ├── context.go      # FSContext (from fuse/pipeline.go)
│   ├── constants.go    # Capability names, extensions
│   ├── path.go         # Path parsing → FSContext
│   ├── operations.go   # ReadDir, Stat, ReadFile, WriteFile
│   ├── directory.go    # Directory listing logic
│   ├── file.go         # File read logic
│   ├── write.go        # File write logic
│   ├── ddl.go          # DDL staging operations
│   ├── errors.go       # Backend-agnostic errors
│   └── types.go        # Entry, FileContent, WriteHandle
│
├── fuse/               # FUSE adapter (thin wrapper)
│   └── adapter.go      # Bridge fs.Operations → FUSE interfaces
│
├── nfs/                # NFS adapter (REWRITTEN)
│   ├── handler.go      # Implement nfs.Handler directly (not billy)
│   ├── handles.go      # File handle tracking for writes
│   └── server.go       # Use handler.go
│
└── db/                 # Database layer (unchanged)
```

---

### Task 9.1: Create fs/ Package Foundation

**Objective:** Create the shared core package with types, errors, and move PipelineContext

**Background:**
The FUSE package contains `PipelineContext` which tracks query state as users navigate the filesystem. This core type needs to move to the shared `fs/` package so both FUSE and NFS can use it. We'll also establish the fundamental types (`Entry`, `FileContent`) and error handling.

**Steps:**
1. Create `internal/tigerfs/fs/` directory

2. Create `internal/tigerfs/fs/types.go`:
   ```go
   package fs

   import (
       "os"
       "time"
   )

   // Entry represents a filesystem entry (file or directory)
   type Entry struct {
       Name    string
       IsDir   bool
       Size    int64
       Mode    os.FileMode
       ModTime time.Time
   }

   // FileContent holds the content of a file read operation
   type FileContent struct {
       Data []byte
       Size int64
       Mode os.FileMode
   }

   // WriteHandle supports streaming writes for large files (.import/)
   type WriteHandle struct {
       Path    string
       Buffer  []byte
       OnClose func(data []byte) error
   }
   ```

3. Create `internal/tigerfs/fs/errors.go`:
   ```go
   package fs

   import "fmt"

   // ErrorCode represents filesystem error types
   type ErrorCode int

   const (
       ErrNone ErrorCode = iota
       ErrNotExist
       ErrPermission
       ErrInvalidPath
       ErrInvalidFormat
       ErrInvalidOperation
       ErrReadOnly
       ErrNotEmpty
       ErrAlreadyExists
       ErrIO
       ErrInternal
   )

   // FSError is a backend-agnostic filesystem error
   type FSError struct {
       Code    ErrorCode
       Message string
       Cause   error
       Hint    string // User-friendly guidance
   }

   func (e *FSError) Error() string {
       if e.Cause != nil {
           return fmt.Sprintf("%s: %v", e.Message, e.Cause)
       }
       return e.Message
   }

   // Error constructors
   func NewNotExistError(path string) *FSError { ... }
   func NewPermissionError(op, path string) *FSError { ... }
   func NewInvalidPathError(path, reason string) *FSError { ... }
   ```

4. Create `internal/tigerfs/fs/constants.go`:
   - Copy constants from `fuse/constants.go`
   - Include: capability directory names, format extensions, permission bits
   - Keep fuse/constants.go importing from fs/ for backwards compatibility

5. Create `internal/tigerfs/fs/context.go`:
   - Move `PipelineContext` from `fuse/pipeline.go` to `fs/context.go`
   - Rename to `FSContext` (clearer name for shared usage)
   - Keep all existing fields and methods
   - Add `PrimaryKey`, `Column`, `Format` fields for row/column level tracking

6. Update `internal/tigerfs/fuse/pipeline.go`:
   - Add type alias: `type PipelineContext = fs.FSContext`
   - Update all imports to use fs.FSContext internally
   - Keep existing method signatures for compatibility

7. Update `internal/tigerfs/fuse/constants.go`:
   - Import constants from fs/ package
   - Re-export for backwards compatibility

8. Write tests:
   - `internal/tigerfs/fs/types_test.go` - Entry and FileContent constructors
   - `internal/tigerfs/fs/errors_test.go` - Error creation, wrapping, code mapping
   - `internal/tigerfs/fs/context_test.go` - All FSContext methods (WithFilter, WithOrder, WithLimit, CanAdd*, etc.)

**Files to Create:**
- `internal/tigerfs/fs/types.go`
- `internal/tigerfs/fs/types_test.go`
- `internal/tigerfs/fs/errors.go`
- `internal/tigerfs/fs/errors_test.go`
- `internal/tigerfs/fs/constants.go`
- `internal/tigerfs/fs/context.go`
- `internal/tigerfs/fs/context_test.go`

**Files to Modify:**
- `internal/tigerfs/fuse/pipeline.go` → type alias to fs.FSContext
- `internal/tigerfs/fuse/constants.go` → import from fs/

**Verification:**
```bash
# Build succeeds
go build ./...

# All tests pass
go test ./internal/tigerfs/fs/...
go test ./internal/tigerfs/fuse/...

# Race detection
go test -race ./internal/tigerfs/fs/...

# Coverage check
go test -cover ./internal/tigerfs/fs/...
```

**Completion Criteria:**
- fs/ package created with types.go, errors.go, constants.go, context.go
- FSContext has all PipelineContext functionality
- Type alias in fuse/ maintains backwards compatibility
- All existing tests pass
- New unit tests for fs/ package pass
- >80% coverage on fs/ package

---

### Task 9.2: Implement Path Parser

**Objective:** Create path parsing logic that converts filesystem paths to FSContext

**Background:**
When NFS or FUSE receives a path like `/users/.by/email/foo@bar.com/.first/10/`, it needs to parse this into an FSContext with the table, index navigation, and pagination settings. Currently FUSE does this through node hierarchy traversal. The shared core needs a standalone path parser.

**Steps:**
1. Create `internal/tigerfs/fs/path.go`:
   ```go
   package fs

   // ParsePath converts a filesystem path to FSContext
   // Examples:
   //   "/" → root context
   //   "/users" → table context (schema=public, table=users)
   //   "/public/users" → explicit schema
   //   "/.schemas/myschema" → schema listing
   //   "/users/.by/email/foo" → index navigation
   //   "/users/.first/10" → pagination
   //   "/users/.filter/status/active" → filter
   //   "/users/123" → row by PK
   //   "/users/123/name" → column
   //   "/users/123.json" → row with format
   //   "/.create/idx/sql" → DDL staging
   func ParsePath(path string) (*FSContext, *FSError)

   // PathType indicates what kind of path was parsed
   type PathType int
   const (
       PathRoot PathType = iota
       PathSchemaList
       PathSchema
       PathTable
       PathCapability
       PathRow
       PathColumn
       PathDDL
       PathExport
       PathImport
   )
   ```

2. Implement path segment parsing:
   - Split path by "/"
   - Handle special prefixes: `.schemas/`, `.create/`, `.modify/`, `.delete/`
   - Handle capability directories: `.by/`, `.filter/`, `.order/`, `.first/`, `.last/`, `.sample/`, `.info/`, `.export/`, `.import/`
   - Handle format extensions: `.json`, `.csv`, `.tsv`, `.yaml`
   - Handle row primary keys (with escaping)

3. Implement capability chain parsing:
   - `.by/<column>/<value>` → sets index navigation
   - `.filter/<column>/<value>` → adds filter condition
   - `.order/<column>/` or `.order/<column>.desc/` → sets ordering
   - `.first/<N>/` → sets LIMIT with ascending order
   - `.last/<N>/` → sets LIMIT with descending order
   - `.sample/<N>/` → sets random sampling
   - Capabilities can chain: `/users/.filter/active/true/.order/created_at/.first/10/`

4. Implement DDL path parsing:
   - `/.create/<name>/` → DDL create staging
   - `/.modify/<name>/` → DDL modify staging
   - `/.delete/<name>/` → DDL delete staging
   - Subpaths: `sql`, `.test`, `.commit`, `.abort`, `test.log`

5. Write comprehensive tests in `internal/tigerfs/fs/path_test.go`:
   - Root path `/`
   - Schema paths `/.schemas/`, `/.schemas/public/`
   - Table paths `/users/`, `/public/users/`
   - Capability chains `/users/.by/email/foo/.first/10/`
   - Row paths `/users/123`, `/users/123.json`
   - Column paths `/users/123/name`
   - DDL paths `/.create/myindex/sql`
   - Invalid paths (proper error handling)
   - Edge cases: special characters in PKs, dots in table names, URL-encoded values

**Files to Create:**
- `internal/tigerfs/fs/path.go`
- `internal/tigerfs/fs/path_test.go`

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/fs/... -v -run TestParsePath

# Coverage
go test -cover ./internal/tigerfs/fs/...
# Target: >90% coverage on path.go
```

**Completion Criteria:**
- ParsePath handles all path types
- All capability chains parse correctly
- DDL paths parse correctly
- Format extensions detected
- Edge cases handled (special chars, escaping)
- Comprehensive tests pass
- >90% coverage on path.go

---

### Task 9.3: Implement Read Operations

**Objective:** Implement ReadDir, Stat, and ReadFile operations in the shared core

**Background:**
Read operations are the foundation of the filesystem. These need to work identically for FUSE and NFS. The Operations struct will hold the database client and delegate to specialized functions for different path types.

**Steps:**
1. Create `internal/tigerfs/fs/operations.go`:
   ```go
   package fs

   import (
       "context"
       "github.com/timescale/tigerfs/internal/tigerfs/config"
       "github.com/timescale/tigerfs/internal/tigerfs/db"
   )

   // Operations provides filesystem operations backed by PostgreSQL
   type Operations struct {
       db      *db.Client
       config  *config.Config
       staging *StagingManager // Added in Task 9.5
   }

   func NewOperations(cfg *config.Config, dbClient *db.Client) *Operations

   // ReadDir lists directory contents
   func (o *Operations) ReadDir(ctx context.Context, path string) ([]Entry, *FSError)

   // Stat returns metadata for a path
   func (o *Operations) Stat(ctx context.Context, path string) (*Entry, *FSError)

   // ReadFile returns file contents
   func (o *Operations) ReadFile(ctx context.Context, path string) (*FileContent, *FSError)

   // Context-based variants for FUSE efficiency (avoids re-parsing)
   func (o *Operations) ReadDirWithContext(ctx context.Context, fsCtx *FSContext) ([]Entry, *FSError)
   func (o *Operations) StatWithContext(ctx context.Context, fsCtx *FSContext) (*Entry, *FSError)
   func (o *Operations) ReadFileWithContext(ctx context.Context, fsCtx *FSContext) (*FileContent, *FSError)
   ```

2. Create `internal/tigerfs/fs/directory.go`:
   - Implement directory listing for each path type:
     - Root: list schemas or flattened tables
     - Schema: list tables
     - Table: list rows (with pagination) + capability dirs
     - Capability dir: list values/options
     - Row: list columns
     - Info dir: list metadata files
     - Export dir: list format files
   - Reuse existing logic from fuse/ package where possible

3. Create `internal/tigerfs/fs/file.go`:
   - Implement file reading for each file type:
     - Row file: serialize row to format (JSON, CSV, TSV)
     - Column file: return single column value
     - Info files: `.count`, `.ddl`, `.columns`, `.indexes`
     - Export files: bulk data export

4. Create `internal/tigerfs/fs/capability.go`:
   - Implement capability directory routing
   - `.info/` → info file listing
   - `.by/` → index listing, then value listing
   - `.filter/` → column listing, then value input
   - `.order/` → column listing
   - `.first/N/`, `.last/N/`, `.sample/N/` → apply to context, continue
   - `.export/` → format file listing

5. Write tests:
   - `internal/tigerfs/fs/operations_test.go` - Integration of components with mock db
   - `internal/tigerfs/fs/directory_test.go` - Directory listing for all path types
   - `internal/tigerfs/fs/file_test.go` - File content generation
   - `internal/tigerfs/fs/capability_test.go` - Capability routing and validation

**Files to Create:**
- `internal/tigerfs/fs/operations.go`
- `internal/tigerfs/fs/operations_test.go`
- `internal/tigerfs/fs/directory.go`
- `internal/tigerfs/fs/directory_test.go`
- `internal/tigerfs/fs/file.go`
- `internal/tigerfs/fs/file_test.go`
- `internal/tigerfs/fs/capability.go`
- `internal/tigerfs/fs/capability_test.go`

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/fs/... -v

# Coverage
go test -cover ./internal/tigerfs/fs/...
# Target: >80% coverage

# Integration test (requires db)
go test ./test/integration/... -v -run TestFSOperations
```

**Completion Criteria:**
- ReadDir works for all path types
- Stat returns correct metadata
- ReadFile returns correct content for all file types
- Capability routing works correctly
- All tests pass
- >80% coverage

---

### Task 9.4: Implement Write Operations

**Objective:** Implement WriteFile, Create, and Delete operations in the shared core

**Background:**
Write operations enable INSERT, UPDATE, and DELETE of database rows through filesystem operations. These must handle format parsing (JSON input), partial row tracking (incremental creates), and proper error handling.

**Steps:**
1. Create `internal/tigerfs/fs/write.go`:
   ```go
   package fs

   // WriteFile writes content to a file path
   // Handles:
   //   /users/123.json → UPDATE row 123
   //   /users/new.json → INSERT new row
   //   /users/123/name → UPDATE column
   //   /.create/idx/sql → DDL staging
   func (o *Operations) WriteFile(ctx context.Context, path string, data []byte) *FSError

   // Create creates a new file or directory
   // Used for: new row files, DDL staging directories
   func (o *Operations) Create(ctx context.Context, path string) (*WriteHandle, *FSError)

   // Delete removes a file or directory
   // Handles:
   //   /users/123.json → DELETE row
   //   /users/123/name → SET column to NULL
   func (o *Operations) Delete(ctx context.Context, path string) *FSError

   // Mkdir creates a directory (for DDL staging)
   func (o *Operations) Mkdir(ctx context.Context, path string) *FSError
   ```

2. Implement row UPDATE:
   - Parse path to get table and primary key
   - Parse content based on format (JSON, CSV, TSV)
   - Build UPDATE SQL
   - Execute with transaction

3. Implement row INSERT:
   - Detect new row (filename "new" or non-existent PK)
   - Parse content to extract columns
   - Build INSERT SQL with RETURNING for new PK
   - Handle serial vs non-serial PKs

4. Implement column UPDATE:
   - Parse path to get table, PK, and column
   - Validate column exists and is writable
   - Build UPDATE SQL for single column
   - Execute with transaction

5. Implement DELETE:
   - Row delete: DELETE FROM table WHERE pk = value
   - Column delete: UPDATE table SET column = NULL

6. Create `internal/tigerfs/fs/staging.go`:
   - Implement `StagingManager` for partial row tracking
   - Track in-progress row creates (incremental column writes)
   - Flush to database when complete
   - Timeout stale staging entries

7. Write tests in `internal/tigerfs/fs/write_test.go`:
   - WriteFile UPDATE vs INSERT detection
   - Format parsing (JSON, CSV input)
   - Column UPDATE
   - DELETE operations
   - Partial row staging
   - Error handling (permission denied, constraint violation)

**Files to Create:**
- `internal/tigerfs/fs/write.go`
- `internal/tigerfs/fs/write_test.go`
- `internal/tigerfs/fs/staging.go`
- `internal/tigerfs/fs/staging_test.go`

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/fs/... -v -run TestWrite

# Integration test
go test ./test/integration/... -v -run TestFSWrite
```

**Completion Criteria:**
- WriteFile handles UPDATE and INSERT
- Format parsing works (JSON, CSV, TSV)
- Column-level writes work
- Delete operations work
- Partial row staging works
- Constraint violations return proper errors
- >80% coverage on write.go

---

### Task 9.5: Implement DDL Operations

**Objective:** Move DDL staging from FUSE to shared core

**Background:**
DDL operations (CREATE INDEX, ALTER TABLE, etc.) use a staging model: write SQL to a `sql` file, validate with `.test`, execute with `.commit`, or cancel with `.abort`. This logic needs to move to the shared core so NFS can use it.

**Steps:**
1. Create `internal/tigerfs/fs/ddl.go`:
   ```go
   package fs

   // DDLOpType identifies the DDL operation
   type DDLOpType int
   const (
       DDLCreate DDLOpType = iota
       DDLModify
       DDLDelete
   )

   // DDLStagingEntry tracks a DDL staging session
   type DDLStagingEntry struct {
       ID        string
       Operation DDLOpType
       Target    string    // table/index/schema name
       SQL       string    // the DDL statement
       TestLog   string    // output from .test
       Validated bool      // .test succeeded
       CreatedAt time.Time
   }

   // DDLManager handles DDL staging operations
   type DDLManager struct {
       db       *db.Client
       sessions map[string]*DDLStagingEntry
       mu       sync.RWMutex
   }

   // CreateSession starts a new DDL staging session
   func (m *DDLManager) CreateSession(op DDLOpType, target string) (string, error)

   // WriteSQL updates the SQL content
   func (m *DDLManager) WriteSQL(sessionID, sql string) error

   // Test validates the SQL (parse only, no execute)
   func (m *DDLManager) Test(ctx context.Context, sessionID string) (string, error)

   // Commit executes the SQL
   func (m *DDLManager) Commit(ctx context.Context, sessionID string) error

   // Abort cancels the session
   func (m *DDLManager) Abort(sessionID string) error
   ```

2. Migrate logic from `fuse/staging.go`:
   - Move template generation functions
   - Move SQL validation logic
   - Move transaction handling

3. Integrate with Operations:
   - Add `DDLManager` to `Operations` struct
   - Handle DDL paths in ReadDir, ReadFile, WriteFile
   - `/.create/<name>/` → create staging session, list control files
   - `sql` → read/write SQL content
   - `.test` → validate SQL, return success/failure
   - `.commit` → execute SQL
   - `.abort` → cancel staging
   - `test.log` → read validation output

4. Write tests in `internal/tigerfs/fs/ddl_test.go`:
   - DDL staging lifecycle (create → write → test → commit)
   - DDL abort workflow
   - Test validation success and failure
   - Test commit execution
   - Test test.log content
   - Multiple concurrent staging sessions

**Files to Create:**
- `internal/tigerfs/fs/ddl.go`
- `internal/tigerfs/fs/ddl_test.go`

**Files to Modify:**
- `internal/tigerfs/fs/operations.go` - Add DDLManager
- `internal/tigerfs/fuse/staging.go` - Delegate to fs/ddl.go or deprecate

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/fs/... -v -run TestDDL

# Integration test
go test ./test/integration/... -v -run TestDDL
```

**Completion Criteria:**
- DDL staging moved to fs/ package
- All DDL operations work through fs.Operations
- .test validation works
- .commit execution works
- .abort cleanup works
- test.log content correct
- Concurrent sessions handled
- >80% coverage on ddl.go

---

### Task 9.6: Implement NFS Handler (Read Operations)

**Objective:** Create new NFS handler implementing go-nfs Handler interface directly

**Background:**
The current NFS implementation uses `billy.Filesystem` which limits what operations are available. By implementing `go-nfs`'s `Handler` interface directly, we get full control over all NFS operations and can use the shared fs.Operations.

**Strategy:** Keep billy.Filesystem temporarily. Add config flag to switch between handlers during development for A/B comparison testing.

**Steps:**
1. Create `internal/tigerfs/nfs/handler.go`:
   ```go
   package nfs

   import (
       "github.com/willscott/go-nfs"
       "github.com/timescale/tigerfs/internal/tigerfs/fs"
   )

   // Handler implements the go-nfs Handler interface using fs.Operations
   type Handler struct {
       ops     *fs.Operations
       handles *HandleManager
   }

   func NewHandler(ops *fs.Operations) *Handler

   // go-nfs Handler interface methods
   func (h *Handler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, nfs.FileHandle, []nfs.AuthFlavor)
   func (h *Handler) Lookup(ctx context.Context, handle nfs.FileHandle, name string) (nfs.FileHandle, *nfs.FileInfo, error)
   func (h *Handler) ReadDir(ctx context.Context, handle nfs.FileHandle) ([]nfs.DirEntry, error)
   func (h *Handler) ReadDirPlus(ctx context.Context, handle nfs.FileHandle) ([]nfs.DirEntryPlus, error)
   func (h *Handler) GetAttr(ctx context.Context, handle nfs.FileHandle) (*nfs.FileInfo, error)
   func (h *Handler) Read(ctx context.Context, handle nfs.FileHandle, offset int64, count uint32) ([]byte, error)
   ```

2. Create `internal/tigerfs/nfs/handles.go`:
   ```go
   package nfs

   // HandleManager maps NFS file handles to filesystem paths
   type HandleManager struct {
       pathToHandle map[string]nfs.FileHandle
       handleToPath map[string]string  // handle bytes as string key
       nextID       uint64
       mu           sync.RWMutex
   }

   func NewHandleManager() *HandleManager
   func (m *HandleManager) GetOrCreateHandle(path string) nfs.FileHandle
   func (m *HandleManager) GetPath(handle nfs.FileHandle) (string, bool)
   ```

3. Implement read-only NFS operations:
   - `Mount` → return root handle
   - `Lookup` → call fs.Stat, return handle and info
   - `ReadDir` → call fs.ReadDir, convert to NFS entries
   - `ReadDirPlus` → same as ReadDir but with attrs
   - `GetAttr` → call fs.Stat, convert to NFS FileInfo
   - `Read` → call fs.ReadFile, return slice at offset

4. Add handler selection flag:
   - Add `--nfs-handler=new|legacy` flag to mount command
   - Default to `legacy` during development
   - Wire both handlers in server.go

5. Update `internal/tigerfs/nfs/server.go`:
   - Support both handler types via flag
   - Keep billy.Filesystem code for comparison
   - Log which handler is active

6. Write tests in `internal/tigerfs/nfs/handler_test.go`:
   - All read operations against mock fs.Operations
   - Handle allocation and lookup
   - Error handling

**Files to Create:**
- `internal/tigerfs/nfs/handler.go`
- `internal/tigerfs/nfs/handler_test.go`
- `internal/tigerfs/nfs/handles.go`
- `internal/tigerfs/nfs/handles_test.go`

**Files to Modify:**
- `internal/tigerfs/nfs/server.go` → support both handlers via flag
- `internal/tigerfs/cmd/mount.go` → add --nfs-handler flag

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/nfs/... -v

# Manual test on macOS
# Build and run with new handler
go build -o bin/tigerfs ./cmd/tigerfs
./bin/tigerfs mount --nfs-handler=new postgres://... /tmp/db

# Verify READ capabilities
ls /tmp/db/
ls /tmp/db/users/
cat /tmp/db/users/.info/.count
ls /tmp/db/users/.by/
cat /tmp/db/users/1.json
```

**Completion Criteria:**
- NFS Handler implements all read operations
- Handle manager tracks path-to-handle mappings
- Flag allows switching between legacy and new handlers
- All read capabilities work via NFS mount
- Unit tests pass
- Manual testing on macOS succeeds

---

### Task 9.7: Implement NFS Handler (Write Operations)

**Objective:** Add write support to NFS handler

**Background:**
Write operations in NFS require buffering (writes come in chunks) and executing SQL on Close(). This task adds Create, Write, SetAttr, and Remove operations to the NFS handler.

**Steps:**
1. Extend `internal/tigerfs/nfs/handles.go`:
   ```go
   // WriteState tracks an open file being written
   type WriteState struct {
       Path      string
       Buffer    bytes.Buffer
       Offset    int64
       CreatedAt time.Time
   }

   // HandleManager additions
   func (m *HandleManager) CreateWriteState(path string) nfs.FileHandle
   func (m *HandleManager) GetWriteState(handle nfs.FileHandle) (*WriteState, bool)
   func (m *HandleManager) CloseWriteState(handle nfs.FileHandle) ([]byte, error)
   ```

2. Add write operations to `internal/tigerfs/nfs/handler.go`:
   ```go
   // Write operations
   func (h *Handler) Create(ctx context.Context, dirHandle nfs.FileHandle, name string, mode uint32) (nfs.FileHandle, *nfs.FileInfo, error)
   func (h *Handler) Write(ctx context.Context, handle nfs.FileHandle, offset int64, data []byte) (uint32, error)
   func (h *Handler) Commit(ctx context.Context, handle nfs.FileHandle, offset int64, count uint32) error
   func (h *Handler) Remove(ctx context.Context, dirHandle nfs.FileHandle, name string) error
   func (h *Handler) Mkdir(ctx context.Context, dirHandle nfs.FileHandle, name string, mode uint32) (nfs.FileHandle, *nfs.FileInfo, error)
   func (h *Handler) Rmdir(ctx context.Context, dirHandle nfs.FileHandle, name string) error
   func (h *Handler) SetAttr(ctx context.Context, handle nfs.FileHandle, attr nfs.SetAttrArgs) (*nfs.FileInfo, error)
   ```

3. Implement write buffering:
   - `Create` → create write state, return handle
   - `Write` → append to buffer at offset
   - `Commit` → flush buffer to fs.WriteFile, execute SQL
   - Close semantics: on last close, call fs.WriteFile with buffered data

4. Implement other write operations:
   - `Remove` → call fs.Delete
   - `Mkdir` → call fs.Mkdir (for DDL staging)
   - `Rmdir` → call fs.Delete
   - `SetAttr` → handle truncate (size=0 clears buffer)

5. Remove legacy billy code:
   - After validating new handler works
   - Remove `filesystem.go`
   - Remove `--nfs-handler` flag (make new handler the default)

6. Write additional tests:
   - Write state lifecycle (create → write → commit)
   - Chunked writes (multiple Write calls)
   - Remove operations
   - Mkdir/Rmdir operations

**Files to Modify:**
- `internal/tigerfs/nfs/handler.go` - Add write operations
- `internal/tigerfs/nfs/handles.go` - Add write state tracking
- `internal/tigerfs/nfs/server.go` - Remove legacy handler after validation

**Files to Delete (after validation):**
- `internal/tigerfs/nfs/filesystem.go`

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/nfs/... -v

# Manual test on macOS
./bin/tigerfs mount postgres://... /tmp/db

# Verify WRITE capabilities
echo '{"name":"test"}' > /tmp/db/users/new.json      # INSERT
echo '{"name":"updated"}' > /tmp/db/users/1.json     # UPDATE
echo 'new value' > /tmp/db/users/1/name              # Column UPDATE
rm /tmp/db/users/1.json                              # DELETE

# Verify IMPORT
cat data.csv > /tmp/db/users/.import/.sync/data.csv

# Verify DDL
echo 'CREATE INDEX...' > /tmp/db/.create/idx/sql
cat /tmp/db/.create/idx/.test
cat /tmp/db/.create/idx/.commit
```

**Completion Criteria:**
- All write operations implemented
- Write buffering works correctly
- SQL executed on commit/close
- Import works via NFS
- DDL staging works via NFS
- Legacy billy code removed
- Unit tests pass
- Manual testing on macOS succeeds

---

### Task 9.8: FUSE Integration and Migration

**Objective:** Create FUSE adapter layer that delegates to fs.Operations

**Background:**
FUSE nodes currently contain both backend-specific code (inode management) and filesystem logic. Create an adapter layer so FUSE nodes can delegate to fs.Operations, reducing code duplication.

**Steps:**
1. Create `internal/tigerfs/fuse/adapter.go`:
   ```go
   package fuse

   import (
       "github.com/timescale/tigerfs/internal/tigerfs/fs"
       "github.com/hanwen/go-fuse/v2/fuse"
   )

   // FSAdapter bridges fs.Operations to FUSE interfaces
   type FSAdapter struct {
       ops *fs.Operations
   }

   func NewFSAdapter(ops *fs.Operations) *FSAdapter

   // Convert fs.Entry to FUSE Attr
   func (a *FSAdapter) EntryToAttr(entry *fs.Entry, out *fuse.Attr)

   // Convert fs.FSError to syscall.Errno
   func (a *FSAdapter) ErrorToErrno(err *fs.FSError) syscall.Errno

   // ReadDir using fs.Operations (for GenericNode)
   func (a *FSAdapter) ReadDir(ctx context.Context, path string) ([]fuse.DirEntry, syscall.Errno)
   ```

2. Create optional GenericNode:
   ```go
   // GenericNode is a FUSE node that delegates everything to fs.Operations
   // Use for simple nodes; keep specialized nodes for performance-critical paths
   type GenericNode struct {
       path    string
       adapter *FSAdapter
   }

   func (n *GenericNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno
   func (n *GenericNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno)
   func (n *GenericNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno)
   func (n *GenericNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno)
   ```

3. Migrate one simple node to use adapter:
   - Choose a simple node (e.g., InfoDirNode or ExportDirNode)
   - Refactor to use FSAdapter for read operations
   - Verify behavior unchanged

4. Document migration strategy:
   - Which nodes should migrate to GenericNode
   - Which nodes should keep specialized implementations
   - Performance considerations

5. Write tests:
   - `internal/tigerfs/fuse/adapter_test.go` - Adapter conversions
   - Verify migrated node behaves identically

**Files to Create:**
- `internal/tigerfs/fuse/adapter.go`
- `internal/tigerfs/fuse/adapter_test.go`

**Files to Modify:**
- One simple FUSE node (as proof of concept)

**Verification:**
```bash
# Unit tests
go test ./internal/tigerfs/fuse/... -v

# Integration tests (verify no regression)
go test ./test/integration/... -v

# Manual test on Linux
./bin/tigerfs mount postgres://... /tmp/db
# Verify all operations work as before
```

**Completion Criteria:**
- FSAdapter created with conversion functions
- GenericNode implemented (optional use)
- One node migrated as proof of concept
- No behavior regression
- Migration strategy documented
- Tests pass

---

### Task 9.9: Feature Parity Verification and Integration Tests

**Objective:** Write integration tests verifying FUSE and NFS feature parity

**Background:**
After implementing the shared core, we need to verify that FUSE and NFS produce identical results for the same operations. This task creates parity tests and updates documentation.

**Steps:**
1. Create `test/integration/fs_operations_test.go`:
   - Test fs.Operations against real PostgreSQL
   - Test all path types
   - Test all capabilities
   - Test read and write operations

2. Create `test/integration/parity_test.go`:
   ```go
   // ParityTest verifies FUSE and NFS produce identical results
   func TestParity(t *testing.T) {
       // Setup: create test database
       // Mount via FUSE (Linux) or NFS (macOS)
       // Execute operations
       // Compare results
   }

   func TestParityReadDir(t *testing.T) { ... }
   func TestParityReadFile(t *testing.T) { ... }
   func TestParityWriteFile(t *testing.T) { ... }
   func TestParityCapabilities(t *testing.T) { ... }
   ```

3. Update documentation:
   - Update `docs/spec.md` with shared core architecture
   - Add section on NFS write support
   - Document platform differences (if any remain)

4. Update feature parity checklist:
   - Verify all items in ADR-009 checklist
   - Document any gaps or platform-specific limitations

5. Final verification:
   - Run all tests on Linux (FUSE)
   - Run all tests on macOS (NFS)
   - Document any failures or limitations

**Files to Create:**
- `test/integration/fs_operations_test.go`
- `test/integration/parity_test.go`

**Files to Modify:**
- `docs/spec.md` - Update architecture section
- `docs/adr/009-shared-core-library.md` - Mark as implemented, update any learnings

**Verification:**
```bash
# Run all integration tests
go test ./test/integration/... -v

# Run with coverage
go test -cover ./internal/tigerfs/fs/...
go test -cover ./internal/tigerfs/nfs/...

# Verify on Linux
GOOS=linux go test ./test/integration/... -v

# Verify on macOS
GOOS=darwin go test ./test/integration/... -v
```

**Completion Criteria:**
- Integration tests for fs.Operations pass
- Parity tests comparing FUSE vs NFS pass
- All features from ADR checklist verified
- Documentation updated
- >80% coverage on fs/ package
- >80% coverage on nfs/handler.go
- No platform-specific behavior differences (or documented if unavoidable)

---

### Task 9.10: Add cachedFile Structure and File Cache

**Objective:** Introduce the core caching data structures for persistent memFile (ADR-010)

**Background:**
The current NFS implementation creates a fresh `memFile` for each NFS operation, causing O(n²) database traffic for large file writes. ADR-010 introduces a persistent file cache with reference counting to commit once instead of per-chunk.

**Steps:**
1. Add `cachedFile` struct to `ops_filesystem.go`:
   ```go
   type cachedFile struct {
       mu           sync.RWMutex
       path         string
       data         []byte
       dirty        bool
       refCount     int
       lastActivity time.Time
       truncated    bool
       deleted      bool
       isSequential bool
       ops          *fs.Operations
       isTrigger    bool
       isDDLSQL     bool
       isRowFile    bool
   }
   ```

2. Modify `OpsFilesystem` struct:
   - Replace `inFlightFiles` and `truncatedFiles` with unified `fileCache map[string]*cachedFile`
   - Add `cacheMu sync.RWMutex`

3. Update `NewOpsFilesystem()` to initialize `fileCache`

4. Add helper methods:
   - `getOrCreateCachedFile(path string, flags int) *cachedFile`
   - `removeFromCache(path string)`

**Files to Modify:**
- `internal/tigerfs/nfs/ops_filesystem.go`

**Verification:**
```bash
go build ./...
go test ./internal/tigerfs/nfs/... -run TestMemFile
```

**Completion Criteria:**
- `cachedFile` struct defined
- `OpsFilesystem` uses `fileCache` map
- Helper methods implemented
- Existing tests still pass (data structures in place but not yet used)

---

### Task 9.11: Modify OpenFile for Cache Lookup

**Objective:** Check cache before reading from database

**Background:**
With the cache in place, `OpenFile` should check the cache first. If a file is already cached, increment the reference count and return a memFile wrapping the cached entry. This prevents redundant database reads.

**Steps:**
1. Modify `OpenFile` to:
   - Check `fileCache` first for existing entry
   - If found: increment `refCount`, return memFile wrapping cached entry
   - If not found: create new `cachedFile`, load from DB (unless O_TRUNC/O_CREATE)
   - Handle O_TRUNC: set `cached.truncated=true`, clear data

2. Modify `Create` to use new cache structure

3. Update `memFile` struct:
   - Add `cached *cachedFile` field
   - Remove direct `data []byte` field (use `cached.data` instead)

4. Ensure `lastActivity` updated on each operation

**Files to Modify:**
- `internal/tigerfs/nfs/ops_filesystem.go`

**Verification:**
```bash
go test ./internal/tigerfs/nfs/... -v
# Verify cache hit/miss in logs with debug enabled
```

**Completion Criteria:**
- `OpenFile` checks cache before DB read
- `memFile` uses shared `cachedFile`
- Existing write tests pass
- Cache hit avoids database round-trip

---

### Task 9.12: Implement Reference Counting and Sync

**Objective:** Commit on Sync() and when last handle closes

**Background:**
Editor saves trigger `fsync()` which calls `Sync()`. We should commit to DB immediately on Sync. On Close, we only commit when the reference count drops to zero (last handle closes).

**Steps:**
1. Add `memFile.Sync()` method:
   - If dirty and has data, commit to DB immediately
   - Clear dirty flag after successful commit
   - This handles editor saves (fsync)

2. Modify `memFile.Close()`:
   - Decrement `refCount`
   - Only commit to DB when `refCount == 0 && dirty`
   - Remove from cache after successful commit

3. Modify `memFile.Write()`:
   - Lock `cached.mu` during writes
   - Write to `cached.data`
   - Track `isSequential` (offset == len(data) before write)

4. Update `Stat` to return size from `cached.data` if file is in cache

**Files to Modify:**
- `internal/tigerfs/nfs/ops_filesystem.go`

**Verification:**
```bash
go test ./internal/tigerfs/nfs/... -v
go test ./test/integration/... -run TestLargeWrite
```

**Completion Criteria:**
- `Sync()` commits immediately (editor save works)
- `Close()` uses reference counting
- Large file writes commit once (not per-chunk)
- `Stat` returns cached size

---

### Task 9.13: Large File Streaming and Memory Limits

**Objective:** Handle large files without OOM

**Background:**
PostgreSQL stores column values atomically (~1GB max with TOAST). For sequential writes, we can stream: commit at 10MB threshold, clear buffer, continue. For random writes, we buffer up to 100MB then reject.

**Steps:**
1. Add configuration constants:
   - `streamingThreshold = 10 * 1024 * 1024` (10MB)
   - `maxRandomWriteSize = 100 * 1024 * 1024` (100MB)

2. Implement streaming mode in `memFile.Write()`:
   - If `isSequential && len(cached.data) > streamingThreshold`:
     - Commit current buffer to DB
     - Clear buffer, continue accepting writes
   - Track that file has been streamed

3. Implement size limit for random writes:
   - If `!isSequential && len(cached.data) > maxRandomWriteSize`:
     - Return `syscall.EFBIG`

4. Update unit tests for streaming behavior

**Files to Modify:**
- `internal/tigerfs/nfs/ops_filesystem.go`
- `internal/tigerfs/nfs/memfile_test.go`

**Verification:**
```bash
# Should complete without OOM, commits in ~10MB chunks
dd if=/dev/zero of=/mnt/db/table/1/data.txt bs=1M count=100
```

**Completion Criteria:**
- Sequential writes >10MB stream (commit and clear)
- Random writes >100MB return EFBIG
- Large file writes work without memory exhaustion

---

### Task 9.14: Cache Reaper and Graceful Shutdown

**Objective:** Prevent memory leaks, flush on shutdown

**Background:**
NFS clients can crash without closing files. A background reaper commits stale entries after 5 minutes of inactivity. On graceful shutdown, all dirty entries are flushed.

**Steps:**
1. Add background reaper goroutine:
   - `startCacheReaper()` - ticker every 30s
   - `reapStaleCacheEntries()` - force-commit entries idle > 5 min
   - `stopCacheReaper()` - stop ticker via context/channel

2. Implement `OpsFilesystem.Close()`:
   - Stop reaper goroutine
   - Iterate all cache entries, commit dirty ones
   - Clear cache

3. Handle Remove/Rename:
   - `Remove()`: mark `cached.deleted=true`, delete from cache
   - `Rename()`: update cache key from old to new path
   - `memFile.Write()`: check `deleted` flag, return EIO

4. Update `server.go`:
   - Call `billyFS.startCacheReaper()` on start
   - Call `billyFS.Close()` on stop

**Files to Modify:**
- `internal/tigerfs/nfs/ops_filesystem.go`
- `internal/tigerfs/nfs/server.go`
- `internal/tigerfs/nfs/memfile_test.go`

**Verification:**
```bash
go test ./internal/tigerfs/nfs/... -run TestReaper
go test ./test/integration/... -run TestWrite
```

**Completion Criteria:**
- Reaper commits stale entries after 5 min idle
- Graceful shutdown flushes all dirty entries
- Delete while open returns EIO on subsequent writes
- No memory leaks from unclosed handles

---

### Task 9.15: Docker-on-macOS Integration Tests

**Objective:** Enable running FUSE integration tests on macOS via Docker

**Background:**
Currently, integration tests use NFS on macOS and FUSE on Linux. This task adds a `TEST_MOUNT_METHOD=docker` option on macOS to run tests using Linux FUSE in Docker, enabling full FUSE test coverage on macOS development machines.

**Steps:**
1. Implement Docker-on-macOS integration test support:
   - Add `TEST_MOUNT_METHOD=docker` option on macOS to run tests using Linux FUSE in Docker
   - Requires refactoring FUSE layer to use an OS abstraction interface
   - DockerFS wrapper executes file operations via `docker exec` commands
   - Build TigerFS for Linux, run in privileged container with FUSE device

2. Run all tests to verify no regressions

**Files to Modify:**
- Test infrastructure files for Docker mount method
- Dockerfile or Docker Compose for FUSE-in-container setup
- OS abstraction interface for FUSE layer

**Verification:**
```bash
# Docker-based tests on macOS
TEST_MOUNT_METHOD=docker go test ./test/integration/... -v

# All existing tests still pass
go test ./...
```

**Completion Criteria:**
- `TEST_MOUNT_METHOD=docker` runs integration tests on macOS using Linux FUSE in Docker
- All existing tests continue to pass
- No behavior changes to production code

---

### Task 9.16: Cleanup and Refactoring

**Objective:** Remove backwards compatibility shims and refactor FUSE to use fs.FSContext directly

**Background:**
Task 9.1 introduced `PipelineContext` as a type alias to `fs.FSContext` for backwards compatibility. Now that the shared core is complete, we should remove the alias and update all FUSE code to use `fs.FSContext` directly. This ensures a clean codebase without legacy indirection.

**Steps:**
1. Update all FUSE files to import and use `fs.FSContext` directly:
   - Replace `*PipelineContext` with `*fs.FSContext`
   - Replace `NewPipelineContext()` with `fs.NewFSContext()`
   - Update type references in function signatures

2. Update FUSE files to use constants from `fs` directly:
   - Replace `fuse.DirBy` with `fs.DirBy` (etc.)
   - Remove redundant constant re-exports from `fuse/constants.go`

3. Simplify `internal/tigerfs/fuse/pipeline.go`:
   - Remove `PipelineContext` type alias
   - Remove `NewPipelineContext` wrapper function
   - Keep only necessary re-exports (LimitType constants for external callers)

4. Simplify `internal/tigerfs/fuse/constants.go`:
   - Remove re-exported constants that are only used internally
   - Keep only constants needed by external packages

5. Run all tests to verify no regressions

6. Update any documentation referencing `PipelineContext`

**Files to Modify:**
- `internal/tigerfs/fuse/pipeline.go` - Remove alias
- `internal/tigerfs/fuse/constants.go` - Simplify re-exports
- `internal/tigerfs/fuse/*.go` - Use fs.FSContext directly
- Any files importing fuse.PipelineContext

**Verification:**
```bash
# Ensure no references to PipelineContext remain (except comments)
grep -r "PipelineContext" internal/tigerfs/fuse/*.go | grep -v "//"

# Build succeeds
go build ./...

# All tests pass
go test ./...

# Race detection
go test -race ./...
```

**Completion Criteria:**
- No `PipelineContext` type alias in fuse/pipeline.go
- All FUSE code uses `fs.FSContext` directly
- Constants imported from `fs` package where appropriate
- All tests pass
- No behavior changes

---

### Task 9.17: Final Testing and v0.2.0 Release

**Objective:** Comprehensive testing of shared core and v0.2.0 release

**Background:**
Phase 9 adds significant new functionality (NFS write support, DDL via NFS, full feature parity). Before releasing v0.2.0, we need thorough testing on both platforms and updated documentation.

**Steps:**
1. Comprehensive manual testing on macOS (NFS):
   ```bash
   # Mount and test all capabilities
   tigerfs mount postgres://... /tmp/db

   # Read capabilities
   ls /tmp/db/users/.info/
   cat /tmp/db/users/.info/.count
   ls /tmp/db/users/.by/email/
   ls /tmp/db/users/.filter/status/
   ls /tmp/db/users/.first/10/
   cat /tmp/db/users/.export/all.csv

   # Write capabilities
   echo '{"name":"test"}' > /tmp/db/users/new.json
   echo '{"name":"updated"}' > /tmp/db/users/1.json
   rm /tmp/db/users/999.json

   # DDL capabilities
   mkdir /tmp/db/.create/test_idx
   echo 'CREATE INDEX...' > /tmp/db/.create/test_idx/sql
   cat /tmp/db/.create/test_idx/.test
   cat /tmp/db/.create/test_idx/.abort
   ```

2. Comprehensive manual testing on Linux (FUSE):
   - Same tests as macOS
   - Verify no regressions from refactoring

3. Update documentation:
   - Update README.md with NFS write support
   - Update docs/spec.md with shared core architecture
   - Add platform parity section

4. Update version to v0.2.0:
   - Update version in relevant files
   - Update CHANGELOG.md

5. Create release:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   # GoReleaser builds binaries
   ```

6. Verify release:
   - Download and test release binaries
   - Verify install script works

**Files to Modify:**
- `README.md` - Update feature list
- `docs/spec.md` - Add shared core architecture
- `CHANGELOG.md` - Add v0.2.0 changes
- Version files as needed

**Verification:**
```bash
# All tests pass
go test ./...

# Integration tests pass
go test ./test/integration/... -v

# Build release
goreleaser release --snapshot --clean

# Verify binaries
./dist/tigerfs_darwin_arm64/tigerfs --version
./dist/tigerfs_linux_amd64/tigerfs --version
```

**Completion Criteria:**
- All manual tests pass on macOS (NFS)
- All manual tests pass on Linux (FUSE)
- Documentation updated
- v0.2.0 tagged and released
- Release binaries working
- Install script updated if needed

---

## Phase 10: Performance & Scalability

### Task 10.1: Implement Hybrid Metadata Caching

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

4. Document in `docs/spec.md`:
   - Add `metadata_preload_limit` to Configuration System section

**Files to Modify:**
- `internal/tigerfs/config/config.go`
- `internal/tigerfs/fuse/cache.go`
- `internal/tigerfs/fuse/cache_test.go`
- `docs/spec.md`

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

### Task 10.2: Evaluate Multi-User Mount Support (allow_other)

**Objective:** Research and potentially implement `--allow-other` flag for multi-user mount access

**Background:**
FUSE's `allow_other` option allows users other than the mounting user to access the filesystem. Currently TigerFS mounts are single-user only. Cross-platform support varies:
- Linux: Full support (requires `user_allow_other` in `/etc/fuse.conf`)
- macOS: Uses NFS backend (different access control model)
- Windows: Different model (WinFsp uses ACLs, not Unix permissions)

**Questions to Address:**
1. Should TigerFS support multi-user mounts at all?
2. If yes, how to handle the single PostgreSQL connection shared across Unix users?
3. Should permission bits change for multi-user access?
   - Files: 0600 (owner rw) → 0644 (world-readable) or 0444 (read-only)
   - Directories: 0700 (owner rwx) → 0755 (world-traversable) or 0555 (read-only)
4. Platform-specific implementation requirements?

**Steps:**
1. Research NFS export options for multi-user access on macOS
2. Test `allow_other` behavior with go-fuse library on Linux
3. Decide on permission model if `allow_other` is enabled
4. If proceeding: wire up the commented-out flag in `cmd/mount.go`
5. Document platform-specific behavior and limitations

**Files to Modify:**
- `internal/tigerfs/cmd/mount.go` (uncomment and wire up flag)
- `internal/tigerfs/fuse/fs.go` (pass AllowOther to mount options)
- `docs/spec.md` (document behavior)

**Completion Criteria:**
- Decision made on whether to support allow_other
- If yes: flag wired up and working on Linux
- Platform limitations documented
- Permission model documented (files: 0600 vs 0644; directories: 0700 vs 0755)

---

### Task 10.3: Row Timestamps from Database Columns (Optional)

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

### Task 10.4: Patch go-nfs for UNSTABLE Writes and COMMIT Flush

**Objective:** Eliminate O(n²) write amplification by patching go-nfs to use proper NFS v3 write buffering.

**Background:** go-nfs currently calls billy.File Close() after every WRITE RPC and always returns
FILE_SYNC stability, making COMMIT a no-op. This forces our adapter to commit the full accumulated
buffer on every WRITE RPC, producing O(n²) DB write volume for multi-chunk writes.

The proper NFS v3 model: WRITE returns UNSTABLE (data buffered), COMMIT flushes to storage.
This would give O(n) writes with correct durability semantics.

**Approach:** Use `go mod replace` with a local patched copy of go-nfs (~10 lines changed):
- `nfs_onwrite.go`: Return `unstable` instead of `fileSync`
- `nfs_oncommit.go`: Call `file.Sync()` on the billy.File to trigger DB flush

**Files to Modify:**
- `go.mod` (add replace directive)
- Local copy of go-nfs `nfs_onwrite.go`
- Local copy of go-nfs `nfs_oncommit.go`
- `internal/tigerfs/nfs/ops_filesystem.go` — Sync() becomes the real commit point

**Completion Criteria:**
- go-nfs WRITE RPCs return UNSTABLE stability level
- go-nfs COMMIT RPCs trigger billy.File Sync()
- Multi-chunk writes produce O(n) DB write volume instead of O(n²)
- All existing integration tests pass
- No durability regressions (data reaches DB on COMMIT)

---

## Phase 11: Skills

### Task 11.1: Auto-install Skills on Binary Install

**Objective:** Bundle agent skills in the release archive and install them to the user's coding agent during `curl | sh` binary install.

**Background:**
TigerFS ships skills (reusable instruction packages in the open `SKILL.md` standard) that teach coding agents how to interact with TigerFS-mounted databases. Users who install via `go install` or clone the repo get skills automatically, but binary-install users don't. This task adds skills to the release archive and prompts users to install them for their agent(s).

**Steps:**
1. Add `skills/tigerfs/*` to `.goreleaser.yaml` archives files list
2. Update `scripts/install.sh` to detect installed coding agents and prompt the user to select where to install skills
3. Support 7 agents: Claude Code, Cursor, Codex CLI, Gemini CLI, Windsurf, Antigravity, Kiro
4. For non-interactive installs (piped stdin), stage skills to `~/.config/tigerfs/skills/tigerfs/` with copy instructions
5. Write `scripts/test-install.sh` to verify the full install flow using `file://` URLs

**Files to Modify:**
- `.goreleaser.yaml`
- `scripts/install.sh`
- `scripts/test-install.sh` (new)

**Verification:**
```bash
./scripts/test-install.sh
goreleaser release --snapshot --clean
```

**Completion Criteria:**
- Skills included in release archive
- Interactive install prompts for agent selection with detection
- Non-interactive install stages skills with copy instructions
- Upgrade install removes stale skill files
- All test-install.sh tests pass

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
- All Phase 8 (Distribution) tasks completed
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
