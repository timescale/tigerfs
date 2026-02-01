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
