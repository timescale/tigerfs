package nfs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// mockOps creates a mock Operations for testing.
func mockOps(t *testing.T) *fs.Operations {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	// Use nil db - the handler tests will mock at the fs.Operations level
	return fs.NewOperations(cfg, nil)
}

// TestNewHandler tests the Handler constructor.
func TestNewHandler(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	require.NotNil(t, h)
	assert.NotNil(t, h.ops)
	assert.NotNil(t, h.handles)
}

// TestHandler_Mount tests the Mount operation.
func TestHandler_Mount(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	rootHandle, err := h.Mount(context.Background())
	require.NoError(t, err)
	require.NotNil(t, rootHandle)

	// Root handle should map to "/"
	path, ok := h.handles.GetPath(rootHandle)
	assert.True(t, ok)
	assert.Equal(t, "/", path)
}

// TestHandler_ToNFSFileHandle tests file handle conversion.
func TestHandler_ToNFSFileHandle(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create a handle
	handle := h.handles.GetOrCreateHandle("/users")

	// Convert to NFS file handle
	nfsHandle := h.ToNFSFileHandle(handle)
	assert.NotNil(t, nfsHandle)
}

// TestHandler_FromNFSFileHandle tests file handle conversion.
func TestHandler_FromNFSFileHandle(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create a handle
	handle := h.handles.GetOrCreateHandle("/users")
	nfsHandle := h.ToNFSFileHandle(handle)

	// Convert back
	recovered := h.FromNFSFileHandle(nfsHandle)
	assert.Equal(t, handle, recovered)
}

// TestHandler_GetHandlePath tests getting path from handle.
func TestHandler_GetHandlePath(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	handle := h.handles.GetOrCreateHandle("/users/123")

	path, ok := h.GetHandlePath(handle)
	assert.True(t, ok)
	assert.Equal(t, "/users/123", path)
}

// TestHandler_GetChildHandle tests getting child handle.
func TestHandler_GetChildHandle(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	parentHandle := h.handles.GetOrCreateHandle("/users")
	childHandle := h.GetChildHandle(parentHandle, "123")

	path, ok := h.handles.GetPath(childHandle)
	assert.True(t, ok)
	assert.Equal(t, "/users/123", path)
}

// TestHandler_HandleOperations tests handle operations are connected to fs.Operations.
func TestHandler_HandleOperations(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Get operations
	assert.Equal(t, ops, h.Operations())
}

// TestHandler_Create tests creating a new file.
func TestHandler_Create(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create parent directory handle
	parentHandle := h.handles.GetOrCreateHandle("/users")

	// Create a new file
	handle, err := h.Create(context.Background(), parentHandle, "new.json")
	require.NoError(t, err)
	require.NotNil(t, handle)

	// Should have a write state
	ws, ok := h.handles.GetWriteState(handle)
	assert.True(t, ok)
	assert.Equal(t, "/users/new.json", ws.Path)
}

// TestHandler_Create_UnknownParent tests creating with unknown parent handle.
func TestHandler_Create_UnknownParent(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	unknownHandle := []byte("unknown")
	_, err := h.Create(context.Background(), unknownHandle, "new.json")
	assert.Error(t, err)
}

// TestHandler_Write tests writing data to a file.
func TestHandler_Write(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create a file first
	parentHandle := h.handles.GetOrCreateHandle("/users")
	handle, _ := h.Create(context.Background(), parentHandle, "new.json")

	// Write data
	n, err := h.Write(context.Background(), handle, []byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Write more data
	n, err = h.Write(context.Background(), handle, []byte(" world"), 5)
	require.NoError(t, err)
	assert.Equal(t, 6, n)

	// Check buffered data
	ws, _ := h.handles.GetWriteState(handle)
	assert.Equal(t, "hello world", ws.Buffer.String())
}

// TestHandler_Write_NoWriteState tests writing to a handle without write state.
func TestHandler_Write_NoWriteState(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create a read-only handle (no write state)
	handle := h.handles.GetOrCreateHandle("/users")

	_, err := h.Write(context.Background(), handle, []byte("hello"), 0)
	assert.Error(t, err)
}

// TestHandler_SetAttr_Truncate tests truncating a file.
func TestHandler_SetAttr_Truncate(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create a file with data
	parentHandle := h.handles.GetOrCreateHandle("/users")
	handle, _ := h.Create(context.Background(), parentHandle, "new.json")
	h.Write(context.Background(), handle, []byte("hello world"), 0)

	// Truncate to 0
	err := h.SetAttr(context.Background(), handle, &SetAttrRequest{Size: 0, SetSize: true})
	require.NoError(t, err)

	// Check buffer is empty
	ws, _ := h.handles.GetWriteState(handle)
	assert.Equal(t, 0, ws.Buffer.Len())
}

// TestHandler_SetAttr_NoChange tests SetAttr with no changes.
func TestHandler_SetAttr_NoChange(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	handle := h.handles.GetOrCreateHandle("/users")

	// SetAttr with no changes should succeed
	err := h.SetAttr(context.Background(), handle, &SetAttrRequest{})
	assert.NoError(t, err)
}

// TestHandler_Commit tests committing a file (closes write state).
func TestHandler_Commit(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	// Create a file with data
	parentHandle := h.handles.GetOrCreateHandle("/users")
	handle, _ := h.Create(context.Background(), parentHandle, "new.json")
	h.Write(context.Background(), handle, []byte(`{"name":"Alice"}`), 0)

	// Commit (note: without a real db, WriteFile will fail, but we test the flow)
	// The actual database write is tested in integration tests
	data, path, err := h.Commit(context.Background(), handle)
	// We expect success for the buffer extraction part
	require.NoError(t, err)
	assert.Equal(t, `{"name":"Alice"}`, string(data))
	assert.Equal(t, "/users/new.json", path)

	// Write state should be removed
	_, ok := h.handles.GetWriteState(handle)
	assert.False(t, ok)
}

// TestHandler_Commit_NoWriteState tests committing a handle without write state.
func TestHandler_Commit_NoWriteState(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	handle := h.handles.GetOrCreateHandle("/users")

	_, _, err := h.Commit(context.Background(), handle)
	assert.Error(t, err)
}

// TestHandler_Remove_UnknownHandle tests removing with unknown handle.
// Note: Full Remove testing requires a database and is covered in integration tests.
func TestHandler_Remove_UnknownHandle(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	unknownHandle := []byte("unknown")
	err := h.Remove(context.Background(), unknownHandle)
	assert.Error(t, err)
	// Verify the error is a "not exist" error for unknown handle
	assert.Contains(t, err.Error(), "unknown handle")
}

// TestHandler_Mkdir_UnknownParent tests creating directory with unknown parent.
// Note: Full Mkdir testing requires a database and is covered in integration tests.
func TestHandler_Mkdir_UnknownParent(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	unknownHandle := []byte("unknown")
	_, err := h.Mkdir(context.Background(), unknownHandle, "newdir")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown parent handle")
}

// TestHandler_Rmdir_UnknownHandle tests removing directory with unknown handle.
// Note: Full Rmdir testing requires a database and is covered in integration tests.
func TestHandler_Rmdir_UnknownHandle(t *testing.T) {
	ops := mockOps(t)
	h := NewHandler(ops)

	unknownHandle := []byte("unknown")
	err := h.Rmdir(context.Background(), unknownHandle)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown handle")
}
