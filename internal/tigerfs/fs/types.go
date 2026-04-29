// Package fs provides the shared core filesystem logic for TigerFS.
//
// This package implements backend-agnostic filesystem operations that both
// FUSE and NFS adapters use. It handles path parsing, query building,
// directory listing, file content generation, and write operations.
//
// The key types are:
//   - PipelineContext: Tracks query state as users navigate capability paths
//   - Entry: Represents a filesystem entry (file or directory)
//   - FileContent: Holds file content for read operations
//   - FSError: Backend-agnostic error with user-friendly hints
package fs

import (
	"os"
	"time"
)

// Entry represents a filesystem entry (file, directory, or symlink).
// Used by ReadDir and Stat operations to describe filesystem structure.
type Entry struct {
	// Name is the entry name (filename or directory name, not full path).
	Name string

	// IsDir is true for directories, false for files and symlinks.
	IsDir bool

	// Size is the content size in bytes. For directories, this is typically
	// a nominal value (e.g., 4096). For symlinks, this is the length of
	// the target path.
	Size int64

	// Mode contains the permission bits and file type.
	// Use os.ModeDir for directories, os.ModeSymlink for symlinks.
	Mode os.FileMode

	// ModTime is the modification time. For database rows, this may be
	// derived from a timestamp column or default to mount time.
	ModTime time.Time

	// Target is the symlink target path. Only set when Mode includes
	// os.ModeSymlink. Empty for regular files and directories.
	Target string
}

// IsSymlink returns true if the entry represents a symlink.
func (e *Entry) IsSymlink() bool {
	return e.Mode&os.ModeSymlink != 0
}

// FileContent holds the content of a file read operation.
// Returned by ReadFile for row files, column files, and metadata files.
type FileContent struct {
	// Data contains the file content bytes.
	Data []byte

	// Size is the content length. Should equal len(Data).
	Size int64

	// Mode contains the permission bits for this file.
	Mode os.FileMode
}

// WriteHandle supports streaming writes for large files.
// Used for .import/ operations where data arrives in chunks.
type WriteHandle struct {
	// Path is the filesystem path being written.
	Path string

	// Buffer accumulates write data until close.
	Buffer []byte

	// OnClose is called when the handle is closed, with the accumulated data.
	// The callback executes the database operation (INSERT/UPDATE).
	OnClose func(data []byte) error
}
