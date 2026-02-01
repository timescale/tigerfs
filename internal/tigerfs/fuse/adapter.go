package fuse

// Migration Strategy for FUSE Nodes
//
// This adapter enables FUSE nodes to delegate filesystem operations to the shared
// fs.Operations core. The migration should be incremental and preserve existing
// behavior while reducing code duplication.
//
// # Nodes Recommended for Migration
//
// The following nodes have simple logic that can fully delegate to fs.Operations:
//   - InfoDirNode: Metadata directory (.info/) - simple directory listing
//   - ExportDirNode: Export formats (.export/) - file content generation
//   - MetadataFileNode: count, ddl, schema, columns files
//
// These nodes primarily perform ReadDir, Stat, and ReadFile operations that
// map directly to fs.Operations methods.
//
// # Nodes to Keep Specialized
//
// The following nodes should keep their specialized implementations:
//   - TableNode: Complex logic for row enumeration, pagination, filtering
//   - RowNode/RowDirNode: Row-specific operations with column handling
//   - PipelineNode: Stateful query building across capability navigation
//   - StagingNode: DDL operation state management
//
// These nodes have FUSE-specific optimizations, complex state management,
// or performance-critical paths that benefit from direct implementation.
//
// # Performance Considerations
//
//   - The adapter adds a thin conversion layer with minimal overhead
//   - For read-heavy paths, consider caching Entry results
//   - Large directory listings should continue using streaming patterns
//   - Batch operations (e.g., .import/) benefit from specialized handling
//
// # Migration Process
//
//  1. Create FSAdapter in the node (typically in constructor)
//  2. Replace direct db calls with adapter.ReadDir/Stat/ReadFile
//  3. Use adapter.ErrorToErrno for error conversion
//  4. Keep FUSE-specific inode management unchanged
//  5. Run integration tests to verify behavior

import (
	"context"
	"syscall"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// FSAdapter bridges fs.Operations to FUSE interfaces.
//
// This adapter allows FUSE nodes to delegate filesystem operations to the shared
// fs.Operations core, enabling code reuse between FUSE and NFS backends.
// It handles conversion between fs.Entry/FSError types and FUSE-specific types.
//
// Usage:
//
//	ops := fs.NewOperations(cfg, dbClient)
//	adapter := NewFSAdapter(ops)
//
//	// In a FUSE node:
//	entries, errno := adapter.ReadDir(ctx, path)
type FSAdapter struct {
	ops *tigerfs.Operations
}

// NewFSAdapter creates a new FUSE adapter for the given operations.
//
// Parameters:
//   - ops: the shared filesystem operations instance
//
// Returns a new FSAdapter ready for use.
func NewFSAdapter(ops *tigerfs.Operations) *FSAdapter {
	return &FSAdapter{ops: ops}
}

// Operations returns the underlying fs.Operations.
//
// This is useful for tests and advanced use cases that need direct
// access to the operations layer.
func (a *FSAdapter) Operations() *tigerfs.Operations {
	return a.ops
}

// EntryToAttr converts an fs.Entry to FUSE Attr.
//
// This method populates the FUSE Attr structure with values from an fs.Entry,
// handling the differences in representation between the two systems.
//
// Parameters:
//   - entry: the filesystem entry to convert
//   - out: the FUSE Attr to populate
func (a *FSAdapter) EntryToAttr(entry *tigerfs.Entry, out *gofuse.Attr) {
	out.Size = uint64(entry.Size)

	if entry.IsDir {
		// Directory mode: include S_IFDIR flag
		out.Mode = syscall.S_IFDIR | uint32(entry.Mode&0777)
		out.Nlink = 2 // directories have . and ..
	} else {
		// File mode: include S_IFREG flag
		out.Mode = syscall.S_IFREG | uint32(entry.Mode&0777)
		out.Nlink = 1
	}

	// Set timestamps if available
	if !entry.ModTime.IsZero() {
		out.SetTimes(nil, &entry.ModTime, nil)
	}
}

// ErrorToErrno converts an fs.FSError to syscall.Errno.
//
// This method maps backend-agnostic error codes to POSIX errno values
// that FUSE operations can return. It also logs the error with any
// hint information for user feedback.
//
// Parameters:
//   - err: the filesystem error to convert (nil returns 0)
//
// Returns the corresponding syscall.Errno value.
func (a *FSAdapter) ErrorToErrno(err *tigerfs.FSError) syscall.Errno {
	if err == nil {
		return 0
	}

	// Log the error with hint if available
	if err.Hint != "" {
		logging.Error(err.Message,
			zap.String("hint", err.Hint),
			zap.Error(err.Cause))
	} else if err.Cause != nil {
		logging.Debug(err.Message, zap.Error(err.Cause))
	}

	switch err.Code {
	case tigerfs.ErrNotExist:
		return syscall.ENOENT
	case tigerfs.ErrPermission:
		return syscall.EACCES
	case tigerfs.ErrInvalidPath, tigerfs.ErrInvalidFormat:
		return syscall.EINVAL
	case tigerfs.ErrInvalidOperation:
		return syscall.EPERM
	case tigerfs.ErrReadOnly:
		return syscall.EROFS
	case tigerfs.ErrNotEmpty:
		return syscall.ENOTEMPTY
	case tigerfs.ErrAlreadyExists:
		return syscall.EEXIST
	case tigerfs.ErrIO, tigerfs.ErrInternal:
		return syscall.EIO
	case tigerfs.ErrNotImplemented:
		return syscall.ENOSYS
	default:
		return syscall.EIO
	}
}

// EntriesToDirEntries converts a slice of fs.Entry to FUSE DirEntry slice.
//
// This method converts directory listing results from the fs package format
// to the FUSE DirEntry format used by Readdir operations.
//
// Parameters:
//   - entries: slice of fs.Entry from a ReadDir operation
//
// Returns a slice of FUSE DirEntry suitable for NewListDirStream.
func (a *FSAdapter) EntriesToDirEntries(entries []tigerfs.Entry) []gofuse.DirEntry {
	result := make([]gofuse.DirEntry, len(entries))
	for i, entry := range entries {
		if entry.IsDir {
			result[i] = gofuse.DirEntry{
				Name: entry.Name,
				Mode: syscall.S_IFDIR,
			}
		} else {
			result[i] = gofuse.DirEntry{
				Name: entry.Name,
				Mode: syscall.S_IFREG,
			}
		}
	}
	return result
}

// ReadDir reads a directory and returns FUSE DirStream.
//
// This method delegates to fs.Operations.ReadDir and converts the result
// to FUSE format. It provides a convenient way for FUSE nodes to perform
// directory listing without duplicating path parsing and query logic.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - path: filesystem path to read (e.g., "/public/users")
//
// Returns a DirStream and errno (0 on success).
func (a *FSAdapter) ReadDir(ctx context.Context, path string) (gofs.DirStream, syscall.Errno) {
	entries, fsErr := a.ops.ReadDir(ctx, path)
	if fsErr != nil {
		return nil, a.ErrorToErrno(fsErr)
	}

	return gofs.NewListDirStream(a.EntriesToDirEntries(entries)), 0
}

// Stat retrieves attributes for a path.
//
// This method delegates to fs.Operations.Stat and converts the result
// to a FUSE EntryOut structure.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - path: filesystem path to stat
//   - out: FUSE EntryOut to populate
//
// Returns errno (0 on success).
func (a *FSAdapter) Stat(ctx context.Context, path string, out *gofuse.EntryOut) syscall.Errno {
	entry, fsErr := a.ops.Stat(ctx, path)
	if fsErr != nil {
		return a.ErrorToErrno(fsErr)
	}

	a.EntryToAttr(entry, &out.Attr)
	return 0
}

// ReadFile reads file content from a path.
//
// This method delegates to fs.Operations.ReadFile and returns the raw
// content bytes. FUSE nodes can use this for Open/Read operations.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - path: filesystem path to read
//
// Returns the file content and errno (0 on success).
func (a *FSAdapter) ReadFile(ctx context.Context, path string) ([]byte, syscall.Errno) {
	content, fsErr := a.ops.ReadFile(ctx, path)
	if fsErr != nil {
		return nil, a.ErrorToErrno(fsErr)
	}

	return content.Data, 0
}

// WriteFile writes content to a path.
//
// This method delegates to fs.Operations.WriteFile for write operations.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - path: filesystem path to write
//   - data: content to write
//
// Returns errno (0 on success).
func (a *FSAdapter) WriteFile(ctx context.Context, path string, data []byte) syscall.Errno {
	fsErr := a.ops.WriteFile(ctx, path, data)
	return a.ErrorToErrno(fsErr)
}

// Delete removes a file or sets a column to NULL.
//
// This method delegates to fs.Operations.Delete.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - path: filesystem path to delete
//
// Returns errno (0 on success).
func (a *FSAdapter) Delete(ctx context.Context, path string) syscall.Errno {
	fsErr := a.ops.Delete(ctx, path)
	return a.ErrorToErrno(fsErr)
}

// Mkdir creates a directory.
//
// This method delegates to fs.Operations.Mkdir for creating schemas
// or row directories (incremental creation).
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - path: filesystem path to create
//
// Returns errno (0 on success).
func (a *FSAdapter) Mkdir(ctx context.Context, path string) syscall.Errno {
	fsErr := a.ops.Mkdir(ctx, path)
	return a.ErrorToErrno(fsErr)
}
