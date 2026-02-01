package nfs

import (
	"context"

	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// Handler implements NFS operations using fs.Operations.
//
// This handler provides a direct implementation that uses the shared fs.Operations
// core, enabling feature parity between FUSE and NFS backends.
//
// The handler manages file handles internally, mapping NFS opaque handles to
// filesystem paths for fs.Operations calls.
type Handler struct {
	ops     *fs.Operations
	handles *HandleManager
}

// NewHandler creates a new NFS handler.
//
// Parameters:
//   - ops: the shared filesystem operations
//
// Returns a new Handler ready for use.
func NewHandler(ops *fs.Operations) *Handler {
	return &Handler{
		ops:     ops,
		handles: NewHandleManager(),
	}
}

// Operations returns the underlying fs.Operations.
// This is useful for tests and advanced use cases.
func (h *Handler) Operations() *fs.Operations {
	return h.ops
}

// Mount returns the root file handle.
//
// This is called when a client mounts the NFS filesystem.
// Returns the root handle that clients use as the starting point.
func (h *Handler) Mount(ctx context.Context) ([]byte, error) {
	return h.handles.GetOrCreateHandle("/"), nil
}

// ToNFSFileHandle converts an internal handle to the NFS wire format.
// Currently this is a pass-through since our handles are already byte slices.
func (h *Handler) ToNFSFileHandle(handle []byte) []byte {
	return handle
}

// FromNFSFileHandle converts an NFS wire format handle to internal format.
// Currently this is a pass-through since our handles are already byte slices.
func (h *Handler) FromNFSFileHandle(nfsHandle []byte) []byte {
	return nfsHandle
}

// GetHandlePath returns the filesystem path for a handle.
//
// Parameters:
//   - handle: the file handle
//
// Returns the path and true if found, or empty string and false if not found.
func (h *Handler) GetHandlePath(handle []byte) (string, bool) {
	return h.handles.GetPath(handle)
}

// GetChildHandle returns a handle for a child of the given parent.
//
// Parameters:
//   - parentHandle: handle of the parent directory
//   - childName: name of the child entry
//
// Returns the child handle, or nil if parent handle is unknown.
func (h *Handler) GetChildHandle(parentHandle []byte, childName string) []byte {
	return h.handles.HandleToChild(parentHandle, childName)
}

// Lookup looks up a name in a directory and returns its handle and attributes.
//
// Parameters:
//   - ctx: context for cancellation
//   - dirHandle: handle of the directory to search
//   - name: name to look up
//
// Returns the file handle and entry info, or an error.
func (h *Handler) Lookup(ctx context.Context, dirHandle []byte, name string) ([]byte, *fs.Entry, error) {
	// Get parent path
	dirPath, ok := h.handles.GetPath(dirHandle)
	if !ok {
		return nil, nil, fs.NewNotExistError("unknown handle")
	}

	// Get child handle and path
	childHandle := h.handles.HandleToChild(dirHandle, name)
	childPath, _ := h.handles.GetPath(childHandle)

	// Stat the child
	entry, fsErr := h.ops.Stat(ctx, childPath)
	if fsErr != nil {
		return nil, nil, fsErr
	}

	// For root lookup, use dirPath context
	_ = dirPath // suppress unused warning for now

	return childHandle, entry, nil
}

// ReadDir reads directory contents.
//
// Parameters:
//   - ctx: context for cancellation
//   - dirHandle: handle of the directory to read
//
// Returns the directory entries, or an error.
func (h *Handler) ReadDir(ctx context.Context, dirHandle []byte) ([]fs.Entry, error) {
	dirPath, ok := h.handles.GetPath(dirHandle)
	if !ok {
		return nil, fs.NewNotExistError("unknown handle")
	}

	entries, fsErr := h.ops.ReadDir(ctx, dirPath)
	if fsErr != nil {
		return nil, fsErr
	}

	return entries, nil
}

// GetAttr gets file/directory attributes.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: file handle
//
// Returns the entry info, or an error.
func (h *Handler) GetAttr(ctx context.Context, handle []byte) (*fs.Entry, error) {
	path, ok := h.handles.GetPath(handle)
	if !ok {
		return nil, fs.NewNotExistError("unknown handle")
	}

	entry, fsErr := h.ops.Stat(ctx, path)
	if fsErr != nil {
		return nil, fsErr
	}

	return entry, nil
}

// Read reads file contents.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: file handle
//   - offset: byte offset to start reading
//   - count: number of bytes to read
//
// Returns the data read, or an error.
func (h *Handler) Read(ctx context.Context, handle []byte, offset int64, count uint32) ([]byte, error) {
	path, ok := h.handles.GetPath(handle)
	if !ok {
		return nil, fs.NewNotExistError("unknown handle")
	}

	content, fsErr := h.ops.ReadFile(ctx, path)
	if fsErr != nil {
		return nil, fsErr
	}

	// Apply offset and count
	data := content.Data
	if offset >= int64(len(data)) {
		return []byte{}, nil
	}

	end := offset + int64(count)
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	return data[offset:end], nil
}
