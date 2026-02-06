# ADR-010: Persistent memFile Cache for NFS Write Performance

**Status:** Proposed
**Date:** 2026-02-05
**Author:** [Author]

## Context

The NFS backend (ADR-009) uses a `billy.Filesystem` implementation where each NFS operation creates a fresh `memFile` instance. For write operations, this causes severe performance problems:

### Current Architecture

```
NFS WRITE RPC → OpenFile() → ReadFile(from DB) → Write(to buffer) → Close() → WriteFile(to DB)
```

Each NFS WRITE operation:
1. Opens the file, reading entire content from database
2. Overlays the write at the specified offset
3. Closes the file, writing entire content back to database

### Performance Problem

For a 1MB file with 32KB NFS wsize:
- 32 WRITE RPCs (one per 32KB chunk)
- Each RPC: 1MB read + 1MB write = 2MB transferred
- **Total: 64MB of database traffic for 1MB file (O(n²) complexity)**

### Current Workarounds

Two maps track file state across NFS operations:
- `inFlightFiles map[string]*memFile` - files created but not yet in DB
- `truncatedFiles map[string]bool` - files truncated via O_TRUNC (macOS pattern)

These handle specific edge cases but don't solve the performance problem.

## Scope: Write Types Affected

TigerFS supports three types of writes:

| Write Type | Example Path | Current Behavior | This ADR Applies? |
|------------|--------------|------------------|-------------------|
| **Column file** | `/table/1/name.txt` | memFile → UPDATE single column | **Yes** |
| **Row file** | `/table/1.json`, `/table/1.csv` | memFile → INSERT/UPDATE row | **Yes** |
| **Bulk import** | `/table/.import/data.csv` | Streaming import via COPY | **No** |

### Why Bulk Import is Excluded

Bulk import (`.import/` capability directory) uses a different code path:
- Data is streamed directly to PostgreSQL COPY command
- No in-memory buffering of entire file
- Already optimized for large datasets

The persistent memFile cache applies to **column and row file writes** only, which go through `OpsFilesystem.OpenFile` → `memFile` → `Close` → `WriteFile`.

## Decision

Introduce a **persistent file cache** with reference counting. Files remain in memory until all handles close, then commit once to the database.

### New Data Structures

```go
// cachedFile persists across NFS RPCs until refCount drops to zero
type cachedFile struct {
    mu           sync.RWMutex
    path         string        // Full path (cache key)
    data         []byte        // Current file content
    dirty        bool          // Has uncommitted changes
    refCount     int           // Number of open handles
    lastActivity time.Time     // For reaper timeout
    truncated    bool          // Was truncated, start fresh
    deleted      bool          // File was deleted, fail writes
    isSequential bool          // All writes append-only (for streaming)

    // Metadata
    ops       *fs.Operations
    isTrigger bool
    isDDLSQL  bool
    isRowFile bool
}

type OpsFilesystem struct {
    ops *fs.Operations

    // Unified file cache (replaces inFlightFiles + truncatedFiles)
    cacheMu   sync.RWMutex
    fileCache map[string]*cachedFile
}

type memFile struct {
    cached   *cachedFile   // Shared cached file
    offset   int64         // Per-handle position
    writable bool          // Per-handle permission
    fs       *OpsFilesystem
}
```

### Commit Triggers

Data is committed to the database when:

| Trigger | Description |
|---------|-------------|
| `Sync()` | Editor save (fsync) - immediate commit |
| `Close()` with refCount=0 | Last handle closes |
| Idle timeout (5 min) | Background reaper for crashed clients |
| Server shutdown | Graceful flush of all dirty entries |

### Large File Handling

PostgreSQL stores column values atomically (~1GB max with TOAST, performance degrades >10-100MB). Two modes based on write pattern:

| Mode | Condition | Behavior | Max Size |
|------|-----------|----------|----------|
| **Streaming** | Sequential writes, buffer > 10MB | Commit, clear, continue | Unlimited |
| **Buffered** | Random writes, buffer < 100MB | Keep in memory | 100MB |
| **Reject** | Random writes, buffer >= 100MB | Return EFBIG | N/A |

### Memory Leak Prevention

Background reaper goroutine handles clients that crash without closing:

```go
func (f *OpsFilesystem) startCacheReaper() {
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        for range ticker.C {
            f.reapStaleCacheEntries()  // Force-commit entries idle > 5 min
        }
    }()
}
```

Stale entries are committed to DB before eviction—no data loss.

### Delete/Rename While Open

If a file is deleted while cached with open handles:
- Cache entry marked `deleted = true`
- Subsequent writes return EIO
- Uncommitted data is discarded (user deleted the file)

## Consequences

### Benefits

1. **Performance**: O(n) instead of O(n²) for large file writes
2. **Correctness**: Truncate-before-write pattern still works
3. **Compatibility**: No changes to go-nfs or billy interface
4. **Robustness**: Reaper handles client crashes, graceful shutdown flushes data

### Costs

1. **Memory Usage**: Files cached in memory until close (bounded by limits)
2. **Complexity**: Reference counting, background reaper, multiple commit paths
3. **Testing**: New edge cases (timeout eviction, delete while open)

### Performance Impact

| Scenario | Before | After |
|----------|--------|-------|
| 1MB write (32KB chunks) | 32 reads + 32 writes | 0 reads + 1 write |
| Editor save while open | N/A | Immediate commit via Sync() |
| Client crash | Data in buffer lost | Committed after 5 min timeout |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Memory leak (client doesn't close) | Background reaper force-evicts after 5 min |
| Large file OOM | 100MB limit for random writes, streaming for sequential |
| Delete while open | Mark deleted, fail writes with EIO |
| Server crash | Same as before (no durability until commit) |
| Graceful shutdown | `OpsFilesystem.Close()` flushes all dirty entries |

## Alternatives Considered

### Alternative 1: Commit on Every Write

Keep current architecture, accept O(n²) performance.

**Rejected:** 1MB file = 64MB DB traffic is unacceptable for common operations like `cp` or `dd`.

### Alternative 2: Database-Level Chunking

Store files as multiple chunks in DB, update only changed chunks.

**Rejected:** Requires schema changes, complex query logic, doesn't match TigerFS's column-per-file model.

### Alternative 3: NFS COMMIT-Based Flushing

Only commit when NFS COMMIT RPC is received.

**Rejected:** go-nfs doesn't expose COMMIT handling. Sync() achieves similar result.

## Implementation

### Files to Modify

| File | Changes |
|------|---------|
| `internal/tigerfs/nfs/ops_filesystem.go` | Add cachedFile, modify OpsFilesystem, OpenFile, Sync, Close, Stat |
| `internal/tigerfs/nfs/server.go` | Start cache reaper, add shutdown flush |
| `internal/tigerfs/nfs/memfile_test.go` | Update tests for caching behavior |

### Implementation Steps

1. Add `cachedFile` struct and `fileCache` map to OpsFilesystem
2. Modify `OpenFile` to check cache before reading from DB
3. Modify `memFile.Write` to use shared cached data
4. Add `memFile.Sync` to commit immediately (editor save)
5. Modify `memFile.Close` with reference counting
6. Update `Stat` to return size from cache
7. Add cache management (reaper, large file streaming, delete handling)
8. Handle Remove/Rename with cache invalidation
9. Add graceful shutdown flush
10. Start cache reaper on server start

### Verification

```bash
# Unit tests
go test ./internal/tigerfs/nfs/... -run TestMemFile

# Integration tests
go test ./test/integration/... -run TestNFS_LargeWrite

# Manual test
./bin/tigerfs mount postgres://... /mnt/db --nfs
dd if=/dev/zero of=/mnt/db/table/1/data.txt bs=1M count=1
# Should complete with 1 DB write, not 32
```

## References

- [ADR-009: Shared Core Library](009-shared-core-library.md) - NFS backend architecture
- [go-nfs](https://github.com/willscott/go-nfs) - NFS server library
- [billy.Filesystem](https://pkg.go.dev/github.com/go-git/go-billy/v5) - Filesystem interface
- PostgreSQL TOAST: https://www.postgresql.org/docs/current/storage-toast.html
