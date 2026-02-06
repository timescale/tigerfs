# Plan: Shared Core Library for FUSE/NFS Feature Parity

## Summary

Create a new `internal/tigerfs/fs/` package as the shared core library that both FUSE and NFS backends use. This achieves **full feature parity** - NFS gets all capabilities that FUSE has, including writes and DDL operations.

See [ADR-009](../adr/009-shared-core-library.md) for the architectural decision and rationale.

## Problem

- NFS backend is missing: `.info/`, `.by/`, `.filter/`, `.order/`, `.first/N/`, `.last/N/`, `.sample/N/`, `.export/`, `.import/`
- NFS backend lacks write support: row INSERT/UPDATE, column UPDATE, DDL operations
- FUSE has ~30 node types implementing these capabilities
- Current NFS uses `billy.Filesystem` which limits write operations

## Solution Architecture

```
internal/tigerfs/
├── fs/                 # NEW: Shared core (backend-agnostic)
│   ├── context.go      # Move PipelineContext here
│   ├── constants.go    # Move constants here
│   ├── path.go         # Path parsing → semantic components
│   ├── operations.go   # Main interface: ReadDir, Stat, ReadFile, WriteFile, etc.
│   ├── directory.go    # Directory listing logic
│   ├── file.go         # File read logic
│   ├── write.go        # File write logic (INSERT/UPDATE)
│   ├── ddl.go          # DDL staging operations
│   ├── capability.go   # Capability routing (.by/, .filter/, etc.)
│   ├── errors.go       # Backend-agnostic errors
│   └── types.go        # Entry, FileContent, WriteHandle, etc.
│
├── fuse/               # FUSE adapter (thin wrapper)
│   ├── adapter.go      # Bridge fs.Operations → FUSE interfaces
│   ├── node.go         # Generic node using fs.Operations
│   └── ...             # Existing files (simplified over time)
│
├── nfs/                # NFS adapter (REWRITTEN)
│   ├── handler.go      # NEW: Implement nfs.Handler directly (not billy)
│   ├── server.go       # Use handler.go instead of billy wrapper
│   └── mount_*.go      # Platform mount code (unchanged)
│
└── db/                 # Database layer (unchanged)
```

## Core Types

### FSContext (from PipelineContext)

```go
// fs/context.go - already exists in fuse/pipeline.go, move here
type FSContext struct {
    Schema, TableName, PKColumn string
    Filters   []FilterCondition
    OrderBy   string
    OrderDesc bool
    Limit     int
    LimitType LimitType
    // ... rest of PipelineContext fields

    // Location tracking
    PrimaryKey string  // Set when at row level
    Column     string  // Set when at column level
    Format     string  // "csv", "json", "tsv", "yaml", ""

    // DDL staging state
    DDLOperation DDLOpType  // Create, Modify, Delete
    DDLTarget    string     // Table/index/schema name
    StagingID    string     // Unique staging session ID
}
```

### Operations Interface

```go
// fs/operations.go
type Operations struct {
    db      db.DBClient
    config  *config.Config
    staging *StagingManager  // For DDL operations
}

func NewOperations(cfg *config.Config, db db.DBClient) *Operations

// Read operations
func (o *Operations) ReadDir(ctx context.Context, path string) ([]Entry, *FSError)
func (o *Operations) Stat(ctx context.Context, path string) (*Entry, *FSError)
func (o *Operations) ReadFile(ctx context.Context, path string) (*FileContent, *FSError)

// Write operations
func (o *Operations) WriteFile(ctx context.Context, path string, data []byte) *FSError
func (o *Operations) Create(ctx context.Context, path string) (*WriteHandle, *FSError)
func (o *Operations) Delete(ctx context.Context, path string) *FSError
func (o *Operations) Mkdir(ctx context.Context, path string) *FSError

// Write handle for streaming writes (needed for .import/ and large files)
type WriteHandle struct {
    Path    string
    Buffer  *bytes.Buffer
    OnClose func(data []byte) *FSError
}
func (h *WriteHandle) Write(p []byte) (int, error)
func (h *WriteHandle) Close() error

// Context-based operations (for FUSE efficiency)
func (o *Operations) ReadDirWithContext(ctx context.Context, fsCtx *FSContext) ([]Entry, *FSError)
func (o *Operations) LookupWithContext(ctx context.Context, fsCtx *FSContext, name string) (*FSContext, *Entry, *FSError)
func (o *Operations) WriteFileWithContext(ctx context.Context, fsCtx *FSContext, data []byte) *FSError
```

### Backend-Agnostic Types

```go
// fs/types.go
type Entry struct {
    Name    string
    IsDir   bool
    Size    int64
    Mode    os.FileMode
    ModTime time.Time
}

type FileContent struct {
    Data []byte
    Size int64
    Mode os.FileMode
}

// fs/errors.go
type FSError struct {
    Code    ErrorCode  // ErrNotExist, ErrPermission, etc.
    Message string
    Cause   error
    Hint    string     // User-friendly guidance
}
```

## Implementation Phases

### Phase 1: Foundation (1-2 days)
1. Create `internal/tigerfs/fs/` package
2. Move `PipelineContext` from `fuse/pipeline.go` to `fs/context.go`
3. Move `constants.go` to `fs/constants.go`
4. Add type aliases in `fuse/` for backwards compatibility
5. Add `fs/types.go` and `fs/errors.go`

**Files to create:**
- `internal/tigerfs/fs/context.go`
- `internal/tigerfs/fs/context_test.go`
- `internal/tigerfs/fs/constants.go`
- `internal/tigerfs/fs/types.go`
- `internal/tigerfs/fs/errors.go`

**Files to modify:**
- `internal/tigerfs/fuse/pipeline.go` → type aliases to fs/
- `internal/tigerfs/fuse/constants.go` → import from fs/

### Phase 2: Path Parser (2-3 days)
1. Create `fs/path.go` with path parsing logic
2. Convert paths like `/users/.by/email/foo/.first/10/` to `FSContext`
3. Handle all path types: root, schema, table, capability chain, row, column, DDL staging

**Files to create:**
- `internal/tigerfs/fs/path.go`
- `internal/tigerfs/fs/path_test.go`

### Phase 3: Read Operations (3-4 days)
1. Create `fs/operations.go` with main interface
2. Implement `ReadDir` for all path types (table listing, capability dirs, row listing)
3. Implement `Stat` for metadata queries
4. Implement `ReadFile` for row/column/export content

**Files to create:**
- `internal/tigerfs/fs/operations.go`
- `internal/tigerfs/fs/operations_test.go`
- `internal/tigerfs/fs/directory.go`
- `internal/tigerfs/fs/file.go`
- `internal/tigerfs/fs/capability.go`

### Phase 4: Write Operations (2-3 days)
1. Implement `WriteFile` for row UPDATE and column UPDATE
2. Implement `Create` for row INSERT (new row files)
3. Implement `Delete` for row DELETE
4. Implement `WriteHandle` for streaming writes (`.import/` support)
5. Move staging logic from FUSE to shared core

**Files to create:**
- `internal/tigerfs/fs/write.go`
- `internal/tigerfs/fs/write_test.go`
- `internal/tigerfs/fs/staging.go` (DDL staging manager)

### Phase 5: DDL Operations (2-3 days)
1. Move DDL staging from `fuse/staging.go` to `fs/ddl.go`
2. Support `.create/`, `.modify/`, `.delete/` directories
3. Support `.test`, `.commit`, `.abort` control files
4. Support `sql` content file and `test.log` output

**Files to create:**
- `internal/tigerfs/fs/ddl.go`
- `internal/tigerfs/fs/ddl_test.go`

### Phase 6: NFS Handler (3-4 days)

**Strategy: Incremental migration with side-by-side comparison**

Keep `billy.Filesystem` temporarily. Add config flag to switch between old and new handlers during development. This allows A/B comparison testing before removing the old code.

**Phase 6a: Read-Only Handler (2 days)**
1. Create `nfs/handler.go` implementing `nfs.Handler` interface
2. Implement read operations: `Lookup`, `ReadDir`, `GetAttr`, `Read`
3. Add `--nfs-handler=new|legacy` flag for testing
4. Compare outputs between old and new handlers
5. Verify all read capabilities work via NFS mount

**Phase 6b: Write Handler (1-2 days)**
1. Add write operations: `Write`, `Create`, `Remove`, `Mkdir`, `Rmdir`, `SetAttr`
2. Implement file handle tracking for buffered writes
3. Test write capabilities via NFS mount
4. Remove legacy billy code after validation

**Files to create:**
- `internal/tigerfs/nfs/handler.go`
- `internal/tigerfs/nfs/handler_test.go`
- `internal/tigerfs/nfs/handles.go` (file handle tracking)

**Files to modify:**
- `internal/tigerfs/nfs/server.go` → support both handlers via flag
- `internal/tigerfs/nfs/filesystem.go` → keep during migration, delete after Phase 6b

### Phase 7: FUSE Integration (2-3 days)
1. Create `fuse/adapter.go` bridging to fs.Operations
2. Create generic node type that delegates to fs.Operations
3. Gradually migrate existing nodes to use adapter
4. Keep specialized nodes where performance matters

**Files to create:**
- `internal/tigerfs/fuse/adapter.go`

**Files to modify:**
- Various node files to optionally use adapter

## Testing Strategy

### Unit Test Requirements by Phase

**Phase 1: Foundation**
- `fs/context_test.go` - All FSContext methods (WithFilter, WithOrder, WithLimit, CanAdd*, etc.)
- `fs/errors_test.go` - Error creation, wrapping, code mapping
- `fs/types_test.go` - Entry and FileContent constructors

**Phase 2: Path Parser**
- `fs/path_test.go` - Comprehensive path parsing tests:
  - Root path `/`
  - Schema paths `/.schemas/`, `/.schemas/public/`
  - Table paths `/users/`, `/public/users/`
  - Capability chains `/users/.by/email/foo/.first/10/`
  - Row paths `/users/123`, `/users/123.json`
  - Column paths `/users/123/name`
  - DDL paths `/.create/myindex/sql`
  - Invalid paths (proper error handling)
  - Edge cases (special characters in PKs, dots in names)

**Phase 3: Read Operations**
- `fs/directory_test.go` - Directory listing for all path types
- `fs/file_test.go` - File content generation (mock db)
- `fs/capability_test.go` - Capability routing and validation
- `fs/operations_test.go` - Integration of components with mock db

**Phase 4: Write Operations**
- `fs/write_test.go` - WriteFile, Create, Delete operations
- `fs/staging_test.go` - Partial row tracking (incremental creates)
- Test UPDATE vs INSERT detection
- Test format parsing (JSON, CSV input)

**Phase 5: DDL Operations**
- `fs/ddl_test.go` - DDL staging lifecycle
- Test `.test` validation (success and failure)
- Test `.commit` execution
- Test `.abort` cleanup
- Test `test.log` content

**Phase 6: NFS Handler**
- `nfs/handler_test.go` - All NFS operations against mock fs.Operations
- `nfs/handles_test.go` - File handle allocation, lookup, expiration
- Comparison tests: old handler vs new handler (same inputs → same outputs)

### Integration Test Requirements

All integration tests use `testcontainers-go` for PostgreSQL.

**New Integration Test Files:**

```
test/integration/
├── fs_path_test.go          # Path parsing with real db metadata
├── fs_read_test.go          # Read operations via fs.Operations
├── fs_write_test.go         # Write operations via fs.Operations
├── fs_ddl_test.go           # DDL operations via fs.Operations
├── nfs_handler_test.go      # NFS handler with real PostgreSQL
├── parity_test.go           # FUSE vs NFS output comparison
└── ...existing tests...
```

**Parity Tests (`parity_test.go`):**
- Mount same database via FUSE and NFS
- Execute identical operations on both
- Compare results (directory listings, file contents, write effects)
- Ensure behavior is identical

**Test Coverage Goals:**
- `fs/` package: >80% coverage
- `nfs/handler.go`: >80% coverage
- All capability paths tested end-to-end

## Key Files to Modify

| File | Change |
|------|--------|
| `internal/tigerfs/fuse/pipeline.go` | Move to fs/context.go, leave aliases |
| `internal/tigerfs/fuse/constants.go` | Move to fs/constants.go, leave imports |
| `internal/tigerfs/fuse/staging.go` | Move logic to fs/ddl.go |
| `internal/tigerfs/nfs/server.go` | Use new handler instead of billy |
| `internal/tigerfs/nfs/filesystem.go` | Remove or deprecate |
| `internal/tigerfs/fuse/*.go` | Gradually delegate to fs.Operations |

## Verification

### After Each Phase
1. Run `go test ./...` - all unit tests must pass
2. Run `go test -race ./...` - verify no race conditions
3. Check coverage: `go test -cover ./internal/tigerfs/fs/...` (target: >80%)
4. Run integration tests: `go test ./test/integration/...`

### Before Merging Phase 6
1. Run parity tests comparing FUSE vs NFS outputs
2. Manual testing on macOS (NFS backend):
   - All read capabilities
   - All write capabilities
   - DDL operations
3. Manual testing on Linux (FUSE backend):
   - Verify no regressions from refactoring

### Manual Verification Commands

After Phase 6 (NFS Handler):
```bash
# Mount via NFS on macOS
tigerfs mount postgres://... /tmp/db

# Verify READ capabilities
ls /tmp/db/users/.info/
cat /tmp/db/users/.info/.count
cat /tmp/db/users/.info/.ddl
ls /tmp/db/users/.by/
ls /tmp/db/users/.by/email/
ls /tmp/db/users/.first/10/
cat /tmp/db/users/.export/all.csv

# Verify WRITE capabilities
echo '{"name":"test"}' > /tmp/db/users/new.json      # INSERT
echo '{"name":"updated"}' > /tmp/db/users/1.json     # UPDATE
echo 'new value' > /tmp/db/users/1/name              # Column UPDATE
rm /tmp/db/users/1.json                              # DELETE

# Verify IMPORT
cat data.csv > /tmp/db/users/.import/.sync/data.csv

# Verify DDL
echo 'CREATE INDEX...' > /tmp/db/.create/idx/sql
cat /tmp/db/.create/idx/.test                        # Validate
cat /tmp/db/.create/idx/.commit                      # Execute
```

## Risk Mitigations

| Risk | Mitigation |
|------|------------|
| Performance regression | Benchmark critical paths before/after |
| Behavior differences | Integration tests comparing FUSE vs NFS |
| Breaking existing FUSE | Keep type aliases, migrate incrementally |
| Complex error mapping | Centralize in adapter layers |
| NFS write complexity | Buffer writes, execute on Close() |
| go-nfs limitations | Test thoroughly with macOS mount_nfs |

## Dependencies

- No new external dependencies
- Uses existing `db.DBClient` interface
- Uses existing `format` package for serialization
- Uses existing `go-nfs` library (already a dependency)

## Estimated Effort

| Phase | Implementation | Testing | Total |
|-------|----------------|---------|-------|
| Phase 1: Foundation | 1 day | 0.5 days | 1-2 days |
| Phase 2: Path Parser | 1-2 days | 1-2 days | 2-4 days |
| Phase 3: Read Operations | 2-3 days | 1-2 days | 3-5 days |
| Phase 4: Write Operations | 1-2 days | 1 day | 2-3 days |
| Phase 5: DDL Operations | 1-2 days | 1 day | 2-3 days |
| Phase 6: NFS Handler | 2-3 days | 1-2 days | 3-5 days |
| Phase 7: FUSE Integration | 1-2 days | 1 day | 2-3 days |
| **Integration/Parity Tests** | - | 2-3 days | 2-3 days |
| **Total** | **9-15 days** | **8-12 days** | **17-28 days**

## Out of Scope

- Performance optimizations beyond maintaining current FUSE performance
- New features not already in FUSE
