# ADR-001: FUSE Library Selection

**Date:** 2026-01-23
**Status:** Accepted
**Deciders:** Mike Freedman
**Decision:** Use `github.com/hanwen/go-fuse/v2`

## Summary

After evaluating both `bazil.org/fuse` and `github.com/hanwen/go-fuse/v2`, we have selected **hanwen/go-fuse/v2** as the FUSE library for TigerFS.

## Options Evaluated

### Option 1: bazil.org/fuse
- **Pros:**
  - Simpler, higher-level API
  - Pure Go implementation
  - Well-documented with clear examples
  - Lower barrier to entry

- **Cons:**
  - **Build failures on macOS** - Critical blocker
  - Last commit: January 2023 (less active maintenance)
  - Missing platform-specific implementations causing undefined symbol errors
  - Less performant for high-throughput workloads

### Option 2: github.com/hanwen/go-fuse/v2
- **Pros:**
  - **Compiles successfully on macOS and Linux**
  - Actively maintained (commits in 2024-2025)
  - Production-proven in large projects (e.g., Google's remote execution system)
  - Higher performance through direct low-level control
  - Better control over caching and consistency
  - Type-safe API with explicit error handling (syscall.Errno)

- **Cons:**
  - More verbose API requiring more boilerplate
  - Steeper learning curve
  - Lower-level abstractions require more careful implementation

## Evaluation Process

### 1. Build Compatibility Test

**Result:** bazil.org/fuse failed to compile on macOS Darwin 23.5.0

```
../../../go/pkg/mod/bazil.org/fuse@v0.0.0-20230120002735-62a210ff1fd5/error_std.go:26:20: undefined: errNoXattr
../../../go/pkg/mod/bazil.org/fuse@v0.0.0-20230120002735-62a210ff1fd5/fuse.go:157:12: undefined: mount
../../../go/pkg/mod/bazil.org/fuse@v0.0.0-20230120002735-62a210ff1fd5/fuse.go:222:24: undefined: maxWrite
```

This indicates missing platform-specific implementation files for macOS, making bazil.org/fuse unusable for development on macOS without significant fixes.

hanwen/go-fuse/v2 compiled successfully on the same system.

### 2. API Comparison

Created equivalent implementations in both libraries to compare API complexity:

**bazil.org/fuse:**
- Simpler interface methods
- Implicit error handling with Go errors
- Less boilerplate for basic operations

**hanwen/go-fuse/v2:**
- More explicit with Inode management
- Syscall errno values for precise error mapping
- Interface segregation (NodeLookuper, NodeReaddirer, etc.)
- Better control over filesystem behavior

**Example: Directory Lookup**

```go
// bazil.org/fuse
func (d dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
    return file{}, nil
}

// hanwen/go-fuse/v2
func (d *dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
    child := d.NewPersistentInode(ctx, &file{}, fs.StableAttr{Mode: syscall.S_IFREG})
    return child, 0
}
```

### 3. Maintenance and Community

- **bazil.org/fuse:** Last significant commit January 2023, fewer recent updates
- **hanwen/go-fuse/v2:** Active development in 2024-2025, responsive issue tracking

### 4. Performance Characteristics

While we couldn't run full benchmarks due to missing FUSE installation, research and production usage indicates:

- hanwen/go-fuse/v2 is significantly faster for high-throughput workloads
- Direct control over low-level operations allows optimization
- Used in Google's production systems for remote execution

## Decision Rationale

The decision to use **hanwen/go-fuse/v2** is based on:

1. **Build Compatibility (Critical):** bazil.org/fuse fails to compile on macOS, which is unacceptable for cross-platform development
2. **Active Maintenance:** hanwen/go-fuse/v2 shows recent development activity and bug fixes
3. **Performance:** Better performance characteristics for database-backed filesystems with many files
4. **Production Proven:** Used in large-scale production systems (Google, others)
5. **Error Handling:** Explicit syscall.Errno return values map perfectly to filesystem error codes needed for TigerFS
6. **Fine-grained Control:** TigerFS needs precise control over caching, consistency, and inode management for database semantics

The trade-off of a more complex API is acceptable given the critical requirement for cross-platform compatibility and the performance needs of a database-backed filesystem.

## Implementation Notes

### Key Interfaces to Implement

For TigerFS, we'll need to implement:

**Root Directory:**
- `fs.InodeEmbedder` - Base inode behavior
- `fs.NodeReaddirer` - List tables
- `fs.NodeLookuper` - Look up tables by name

**Table Directories:**
- `fs.InodeEmbedder`
- `fs.NodeReaddirer` - List rows (with pagination support)
- `fs.NodeLookuper` - Look up rows by primary key
- `fs.NodeMkdirer` - Create new rows (row-as-directory)
- `fs.NodeRmdirer` - Delete rows

**Row Files:**
- `fs.InodeEmbedder`
- `fs.NodeOpener` - Open for reading/writing
- `fs.NodeGetattrer` - Get file attributes
- `fs.NodeUnlinker` - Delete row

**File Handles:**
- `fs.FileReader` - Read row data (TSV/CSV/JSON)
- `fs.FileWriter` - Write row data
- `fs.FileGetattrer` - Get attributes during open

### Inode Management Strategy

TigerFS will use:
- **Stable inodes** for tables (persistent across mounts)
- **Dynamic inodes** for rows (generated from primary key hash)
- **Persistent inodes** for special files (.schema, .count, etc.)

### Caching Strategy

- **Entry cache:** 1 second (configurable via `entry_timeout`)
- **Attribute cache:** 1 second (configurable via `attr_timeout`)
- **Metadata refresh:** 30 seconds for schema information
- **No data caching:** Always fetch fresh data from PostgreSQL

## Next Steps

1. ✅ Add hanwen/go-fuse/v2 to go.mod
2. ✅ Remove bazil.org/fuse from go.mod
3. Remove benchmark test files
4. Begin FUSE implementation in `internal/tigerfs/fuse/` following hanwen API patterns
5. Start with basic mount and root directory listing (Task 1.4)

## References

- hanwen/go-fuse GitHub: https://github.com/hanwen/go-fuse
- hanwen/go-fuse v2 Documentation: https://pkg.go.dev/github.com/hanwen/go-fuse/v2
- bazil.org/fuse GitHub: https://github.com/bazil/fuse
- FUSE Protocol: https://www.kernel.org/doc/html/latest/filesystems/fuse.html
