# ADR-004: Mock Interfaces for Database Testing

**Date:** 2026-01-27
**Status:** Accepted
**Author:** Mike Freedman

## Context

TigerFS FUSE nodes currently hold direct references to `*db.Client`, a concrete struct with 40+ methods. This creates testing challenges:

1. **No unit test isolation** - FUSE nodes can't be tested without a live database
2. **Slow feedback loop** - Integration tests with testcontainers take 10-30 seconds to start
3. **Hard to test edge cases** - Simulating database errors, timeouts, or specific responses requires a real database in that state
4. **Phase 5 DDL operations** - New control files (`.test`, `.commit`) call `db.Exec()` and `db.ExecInTransaction()` which have zero test coverage

Current testing approach:
- `nil` db.Client + pre-populated MetadataCache (limited, fragile)
- Integration tests with testcontainers-go (comprehensive but slow)

## Options Considered

### Option 1: Focused Interfaces (Chosen)

Introduce **focused interfaces** that abstract db.Client methods by domain. FUSE nodes will accept interfaces instead of `*db.Client`, enabling dependency injection of mock implementations for unit testing.

**Pros:**
- Faster unit tests - No database startup, tests run in milliseconds
- Better test isolation - Each test controls exactly what the "database" returns
- Edge case coverage - Easy to simulate errors, timeouts, empty results
- Clearer dependencies - Interfaces document what each node actually needs
- No external dependencies - Hand-written mocks, no mockgen or testify/mock
- Backward compatible - `*db.Client` satisfies all interfaces automatically

**Cons:**
- Limited scope - Mocks test FUSE logic, not actual SQL queries or PostgreSQL behavior
- Maintenance overhead - Must keep interfaces in sync with db.Client methods
- Potential false confidence - Unit tests pass but real SQL could be wrong
- Code churn - ~12 files modified to adopt interfaces

### Option 2: Mock at pgx level (pgxmock)

Use pgxmock library to mock the PostgreSQL driver, allowing tests to verify actual SQL generation.

**Rejected because:**
- Higher complexity (~45 function signature changes)
- SQL string matching is fragile (whitespace sensitivity)
- External dependency (pgxmock)
- Effort: 2-3 days vs 1-2 days

### Option 3: Mocks only for Phase 5 DDL

Add interfaces and mocks only for `DDLExecutor`, leave Phase 1/2 to integration tests.

**Rejected because:**
- User preferred consistent pattern across codebase
- Interfaces improve code documentation
- Marginal additional effort for full coverage

## Decision

Use **Option 1: Focused Interfaces**.

### Interface Design

```go
// DDLExecutor - DDL operations (Phase 5 control files)
type DDLExecutor interface {
    Exec(ctx context.Context, sql string, args ...interface{}) error
    ExecInTransaction(ctx context.Context, sql string, args ...interface{}) error
}

// SchemaReader - schema/table metadata (cache, root node)
type SchemaReader interface {
    GetCurrentSchema(ctx context.Context) (string, error)
    GetSchemas(ctx context.Context) ([]string, error)
    GetTables(ctx context.Context, schema string) ([]string, error)
    GetColumns(ctx context.Context, schema, table string) ([]Column, error)
    GetPrimaryKey(ctx context.Context, schema, table string) (*PrimaryKey, error)
}

// RowReader - row-level reads (row/column nodes)
type RowReader interface {
    GetRow(ctx context.Context, schema, table, pkColumn, pkValue string) (*Row, error)
    GetColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error)
    ListRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)
}

// RowWriter - row-level writes (row/column nodes)
type RowWriter interface {
    InsertRow(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error)
    UpdateRow(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error
    UpdateColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error
    DeleteRow(ctx context.Context, schema, table, pkColumn, pkValue string) error
}

// IndexReader - index operations (index nodes)
type IndexReader interface {
    GetIndexes(ctx context.Context, schema, table string) ([]Index, error)
    GetSingleColumnIndexes(ctx context.Context, schema, table string) ([]Index, error)
    GetCompositeIndexes(ctx context.Context, schema, table string) ([]Index, error)
    GetDistinctValues(ctx context.Context, schema, table, column string, limit int) ([]string, error)
    GetRowsByIndexValue(ctx context.Context, schema, table, column, value, pkColumn string, limit int) ([]string, error)
}

// CountReader - row count operations (table nodes, .count file)
type CountReader interface {
    GetRowCount(ctx context.Context, schema, table string) (int64, error)
    GetRowCountSmart(ctx context.Context, schema, table string) (int64, error)
    GetRowCountEstimates(ctx context.Context, schema string, tables []string) (map[string]int64, error)
}

// DDLReader - DDL generation (.ddl file, templates)
type DDLReader interface {
    GetTableDDL(ctx context.Context, schema, table string) (string, error)
    GetFullDDL(ctx context.Context, schema, table string) (string, error)
    GetIndexDDL(ctx context.Context, schema, table string) (string, error)
}

// PaginationReader - pagination queries (.first/N, .last/N, .sample/N)
type PaginationReader interface {
    GetFirstNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)
    GetLastNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)
    GetRandomSampleRows(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error)
}

// DBClient - composite interface for nodes needing multiple capabilities
type DBClient interface {
    DDLExecutor
    SchemaReader
    RowReader
    RowWriter
    IndexReader
    CountReader
    DDLReader
    PaginationReader
}
```

### Mock Implementation Pattern

Hand-written mocks using function fields (no external dependencies):

```go
type MockRowWriter struct {
    InsertRowFunc    func(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error)
    UpdateRowFunc    func(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error
    UpdateColumnFunc func(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error
    DeleteRowFunc    func(ctx context.Context, schema, table, pkColumn, pkValue string) error
}

func (m *MockRowWriter) DeleteRow(ctx context.Context, schema, table, pkColumn, pkValue string) error {
    if m.DeleteRowFunc != nil {
        return m.DeleteRowFunc(ctx, schema, table, pkColumn, pkValue)
    }
    return nil
}
```

### Files to Create

| File | Purpose |
|------|---------|
| `internal/tigerfs/db/interfaces.go` | Interface definitions |
| `internal/tigerfs/db/mocks.go` | Mock implementations |
| `internal/tigerfs/fuse/control_files_test.go` | DDL control file tests |
| `internal/tigerfs/fuse/column_file_test.go` | Column read/write tests |
| `internal/tigerfs/fuse/row_test.go` | Row operation tests |

### Files to Modify

| File | Change |
|------|--------|
| `internal/tigerfs/fuse/control_files.go` | Accept `DDLExecutor` |
| `internal/tigerfs/fuse/table.go` | Accept `DBClient` |
| `internal/tigerfs/fuse/row.go` | Accept interfaces |
| `internal/tigerfs/fuse/column_file.go` | Accept interfaces |
| `internal/tigerfs/fuse/cache.go` | Accept `SchemaReader` |
| `internal/tigerfs/fuse/table_test.go` | Add mock-based tests |
| `internal/tigerfs/fuse/cache_test.go` | Add mock-based tests |

### Verification

```bash
# Compile-time verification that db.Client implements DBClient
var _ db.DBClient = (*db.Client)(nil)

# Run all tests
go test ./...

# Run unit tests only (fast)
go test ./internal/tigerfs/fuse/... -v

# Run integration tests (slow, comprehensive)
go test ./test/integration/... -v
```

## Consequences

### Positive

- **Faster unit tests** - No database startup, tests run in milliseconds
- **Better test isolation** - Each test controls exactly what the "database" returns
- **Edge case coverage** - Easy to simulate errors, timeouts, empty results
- **Clearer dependencies** - Interfaces document what each node actually needs
- **No external dependencies** - Hand-written mocks, no mockgen or testify/mock
- **Backward compatible** - `*db.Client` satisfies all interfaces automatically

### Negative

- **Limited scope** - Mocks test FUSE logic, not actual SQL queries or PostgreSQL behavior
- **Maintenance overhead** - Must keep interfaces in sync with db.Client methods
- **Potential false confidence** - Unit tests pass but real SQL coulq
d be wrong
- **Code churn** - ~12 files modified to adopt interfaces

### Neutral

- **Integration tests remain essential** - They verify actual PostgreSQL behavior
- **Gradual adoption possible** - Can migrate nodes one at a time

## References

- Go interface design: https://go.dev/doc/effective_go#interfaces
- Testing with interfaces: https://go.dev/wiki/TableDrivenTests
