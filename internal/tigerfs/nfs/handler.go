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

// SetAttrRequest contains the attributes to set on a file.
//
// NFS SETATTR operations can modify various file attributes. This struct
// tracks which attributes should be changed and their new values.
type SetAttrRequest struct {
	// Size is the new file size (used for truncation).
	Size uint64
	// SetSize indicates whether to change the file size.
	SetSize bool
	// Mode is the new file permissions (not currently implemented).
	Mode uint32
	// SetMode indicates whether to change permissions.
	SetMode bool
}

// Create creates a new file and returns a handle with write state.
//
// This method creates a handle for a new file and initializes a write buffer.
// The file is not actually written to the database until Commit is called.
// NFS CREATE operations use this to prepare a file for writing.
//
// Parameters:
//   - ctx: context for cancellation
//   - parentHandle: handle of the directory to create the file in
//   - name: name of the new file
//
// Returns the file handle for the new file, or an error if the parent handle
// is unknown.
func (h *Handler) Create(ctx context.Context, parentHandle []byte, name string) ([]byte, error) {
	parentPath, ok := h.handles.GetPath(parentHandle)
	if !ok {
		return nil, fs.NewNotExistError("unknown parent handle")
	}

	// Build child path
	childPath := parentPath
	if parentPath == "/" {
		childPath = "/" + name
	} else {
		childPath = parentPath + "/" + name
	}

	// Create write state for the new file
	handle := h.handles.CreateWriteState(childPath)
	return handle, nil
}

// Write writes data to a file at the specified offset.
//
// NFS writes are buffered until the file is committed or closed.
// This method appends data to the internal buffer at the given offset.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: file handle (must have an associated write state)
//   - data: bytes to write
//   - offset: byte offset to write at
//
// Returns the number of bytes written, or an error if the handle has no
// write state (e.g., it's a read-only handle).
func (h *Handler) Write(ctx context.Context, handle []byte, data []byte, offset int64) (int, error) {
	ws, ok := h.handles.GetWriteState(handle)
	if !ok {
		return 0, fs.NewNotExistError("no write state for handle")
	}

	return ws.WriteAt(data, offset)
}

// SetAttr sets file attributes.
//
// Currently supports truncation via SetSize. When SetSize is true and Size
// is 0, the file's write buffer is cleared.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: file handle
//   - req: attributes to set
//
// Returns an error if the operation fails.
func (h *Handler) SetAttr(ctx context.Context, handle []byte, req *SetAttrRequest) error {
	if req.SetSize && req.Size == 0 {
		// Truncate: clear the write buffer if it exists
		if ws, ok := h.handles.GetWriteState(handle); ok {
			ws.Truncate()
		}
	}

	return nil
}

// Commit commits buffered write data and closes the write state.
//
// This method retrieves the buffered data from the write state, closes the
// write state, and returns the data and path. The caller is responsible for
// actually writing the data to the database via fs.Operations.WriteFile.
//
// This two-step approach allows NFS COMMIT operations to handle the buffer
// extraction separately from the database write, enabling proper error handling.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: file handle with an associated write state
//
// Returns:
//   - data: the complete buffered file contents
//   - path: the filesystem path to write to
//   - err: error if the handle has no write state
func (h *Handler) Commit(ctx context.Context, handle []byte) ([]byte, string, error) {
	return h.handles.CloseWriteState(handle)
}

// Remove removes a file (row deletion).
//
// This method deletes the file at the given handle's path. For TigerFS,
// this typically translates to deleting a row from a table or setting a
// column to NULL.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: file handle for the file to remove
//
// Returns an error if the handle is unknown or the deletion fails.
func (h *Handler) Remove(ctx context.Context, handle []byte) error {
	path, ok := h.handles.GetPath(handle)
	if !ok {
		return fs.NewNotExistError("unknown handle")
	}

	return h.ops.Delete(ctx, path)
}

// Mkdir creates a new directory.
//
// For TigerFS, directories at the schema level create new database schemas,
// and directories at the table level create new rows (incremental creation).
//
// Parameters:
//   - ctx: context for cancellation
//   - parentHandle: handle of the parent directory
//   - name: name of the new directory
//
// Returns the handle for the new directory, or an error.
func (h *Handler) Mkdir(ctx context.Context, parentHandle []byte, name string) ([]byte, error) {
	parentPath, ok := h.handles.GetPath(parentHandle)
	if !ok {
		return nil, fs.NewNotExistError("unknown parent handle")
	}

	// Build child path
	childPath := parentPath
	if parentPath == "/" {
		childPath = "/" + name
	} else {
		childPath = parentPath + "/" + name
	}

	// Create via fs.Operations
	if fsErr := h.ops.Mkdir(ctx, childPath); fsErr != nil {
		return nil, fsErr
	}

	return h.handles.GetOrCreateHandle(childPath), nil
}

// Rmdir removes a directory.
//
// For TigerFS, this removes row directories (deletes the row). Schema and
// table directories cannot be removed via Rmdir - use DDL operations instead.
//
// Parameters:
//   - ctx: context for cancellation
//   - handle: handle of the directory to remove
//
// Returns an error if the handle is unknown or the removal fails.
func (h *Handler) Rmdir(ctx context.Context, handle []byte) error {
	path, ok := h.handles.GetPath(handle)
	if !ok {
		return fs.NewNotExistError("unknown handle")
	}

	// For TigerFS, rmdir on a row directory deletes the row
	return h.ops.Delete(ctx, path)
}
