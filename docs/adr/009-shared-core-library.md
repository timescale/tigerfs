# ADR-009: Shared Core Library for FUSE/NFS Feature Parity

**Status:** Accepted
**Date:** 2026-02-01
**Author:** Mike Freedman

## Context

TigerFS supports two filesystem backends:

1. **FUSE** (Linux): Full-featured implementation with ~30 node types supporting all capabilities
2. **NFS** (macOS): Limited implementation using `billy.Filesystem` interface

The NFS backend currently lacks:
- Capability directories: `.info/`, `.by/`, `.filter/`, `.order/`, `.first/N/`, `.last/N/`, `.sample/N/`
- Bulk operations: `.export/`, `.import/`
- Write support: row INSERT/UPDATE, column UPDATE
- DDL operations: `.create/`, `.modify/`, `.delete/`

This creates a poor experience for macOS users who cannot use FUSE without third-party kernel extensions (MacFUSE/FUSE-T).

### Current Architecture

```
internal/tigerfs/
├── fuse/           # ~30 node types, full capability support
│   ├── pipeline.go # PipelineContext (query state accumulation)
│   ├── constants.go
│   ├── by.go, filter.go, order.go, pagination.go, sample.go
│   ├── export.go, import.go
│   ├── staging.go  # DDL staging
│   └── ...
│
├── nfs/            # Limited billy.Filesystem wrapper
│   ├── filesystem.go  # Basic read operations only
│   └── server.go
│
└── db/             # Database operations (shared)
```

The problem: FUSE node implementations contain both **backend-specific code** (FUSE inode management) and **core filesystem logic** (path interpretation, query building, format conversion). This logic cannot be reused by NFS.

## Decision

Create a new `internal/tigerfs/fs/` package as a **shared core library** that both FUSE and NFS backends use. This achieves full feature parity—NFS gets all capabilities that FUSE has.

### New Architecture

```
internal/tigerfs/
├── fs/                 # NEW: Shared core (backend-agnostic)
│   ├── context.go      # FSContext (moved from fuse/pipeline.go)
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
│   ├── adapter.go      # Bridge fs.Operations → FUSE interfaces
│   └── ...             # Existing files delegate to fs/
│
├── nfs/                # NFS adapter (REWRITTEN)
│   ├── handler.go      # Implement nfs.Handler directly (not billy)
│   ├── handles.go      # File handle tracking for writes
│   └── server.go       # Use handler.go
│
└── db/                 # Database layer (unchanged)
```

### Key Design: NFS Handler

**Current approach** (limited by billy.Filesystem):
```
billy.Filesystem → nfshelper.NullAuthHandler → nfs.Serve
```

**New approach** (full control):
```
fs.Operations → custom nfs.Handler → nfs.Serve
```

By implementing go-nfs's `Handler` interface directly, we gain:
- Full control over `Write()`, `Create()`, `SetAttr()` operations
- Ability to buffer writes and execute SQL on `Close()`
- Support for DDL control files (`.test`, `.commit`, `.abort`)

### Core Interface

```go
// fs/operations.go
type Operations struct {
    db      db.DBClient
    config  *config.Config
    staging *StagingManager
}

// Read operations
func (o *Operations) ReadDir(ctx context.Context, path string) ([]Entry, *FSError)
func (o *Operations) Stat(ctx context.Context, path string) (*Entry, *FSError)
func (o *Operations) ReadFile(ctx context.Context, path string) (*FileContent, *FSError)

// Write operations
func (o *Operations) WriteFile(ctx context.Context, path string, data []byte) *FSError
func (o *Operations) Create(ctx context.Context, path string) (*WriteHandle, *FSError)
func (o *Operations) Delete(ctx context.Context, path string) *FSError
```

## Consequences

### Benefits

1. **Feature Parity**: macOS users get full TigerFS functionality via NFS
2. **Code Reuse**: Single implementation of filesystem logic
3. **Testability**: Core logic testable without FUSE/NFS mocking
4. **Maintainability**: New capabilities added once, work on both backends
5. **Consistency**: Identical behavior across platforms

### Costs

1. **Migration Effort**: Significant refactoring of existing FUSE code
2. **Abstraction Layer**: Additional indirection may affect performance
3. **Testing Burden**: Must verify parity between backends

### Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Performance regression | Benchmark critical paths before/after |
| Behavior differences | Integration tests comparing FUSE vs NFS |
| Breaking existing FUSE | Keep type aliases, migrate incrementally |
| NFS write complexity | Buffer writes, execute on Close() |
| go-nfs limitations | Test thoroughly with macOS mount_nfs |

## Alternatives Considered

### Alternative 1: Duplicate Logic in NFS

Copy FUSE implementations to NFS package, adapting for billy.Filesystem.

**Rejected:** Creates maintenance burden, divergent behavior over time.

### Alternative 2: Extend billy.Filesystem

Add write support to existing billy wrapper.

**Rejected:** billy.Filesystem interface is too limited for DDL staging and buffered writes. Would require extensive workarounds.

### Alternative 3: FUSE-T on macOS

Recommend users install FUSE-T for macOS.

**Rejected:** Requires third-party software, poor user experience. NFS works out-of-box on macOS.

## Feature Parity Checklist

After implementation, both FUSE and NFS will support:

| Feature | FUSE | NFS |
|---------|:----:|:---:|
| `.info/` metadata | ✅ | ✅ |
| `.by/` index navigation | ✅ | ✅ |
| `.filter/` column filtering | ✅ | ✅ |
| `.order/` ordering | ✅ | ✅ |
| `.first/N/` pagination | ✅ | ✅ |
| `.last/N/` pagination | ✅ | ✅ |
| `.sample/N/` sampling | ✅ | ✅ |
| `.export/` bulk export | ✅ | ✅ |
| `.import/` bulk import | ✅ | ✅ |
| Row INSERT | ✅ | ✅ |
| Row UPDATE | ✅ | ✅ |
| Column UPDATE | ✅ | ✅ |
| Row DELETE | ✅ | ✅ |
| DDL `.create/` | ✅ | ✅ |
| DDL `.modify/` | ✅ | ✅ |
| DDL `.delete/` | ✅ | ✅ |

## Implementation

See `docs/implementation/shared-core-plan.md` for detailed implementation phases.

## References

- [ADR-007: Pipeline Query Architecture](007-pipeline-query-architecture.md) - PipelineContext design
- [go-nfs Handler interface](https://pkg.go.dev/github.com/willscott/go-nfs#Handler)
- [billy.Filesystem interface](https://pkg.go.dev/github.com/go-git/go-billy/v5#Filesystem)
- TigerFS spec: `docs/spec.md`
