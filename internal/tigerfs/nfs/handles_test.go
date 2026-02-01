package nfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewHandleManager tests the HandleManager constructor.
func TestNewHandleManager(t *testing.T) {
	hm := NewHandleManager()
	require.NotNil(t, hm)
	assert.NotNil(t, hm.pathToHandle)
	assert.NotNil(t, hm.handleToPath)
}

// TestHandleManager_GetOrCreateHandle tests handle creation and retrieval.
func TestHandleManager_GetOrCreateHandle(t *testing.T) {
	hm := NewHandleManager()

	// Create handle for root
	handle1 := hm.GetOrCreateHandle("/")
	require.NotNil(t, handle1)
	assert.NotEmpty(t, handle1)

	// Same path should return same handle
	handle2 := hm.GetOrCreateHandle("/")
	assert.Equal(t, handle1, handle2)

	// Different path should return different handle
	handle3 := hm.GetOrCreateHandle("/users")
	assert.NotEqual(t, handle1, handle3)
}

// TestHandleManager_GetPath tests retrieving path from handle.
func TestHandleManager_GetPath(t *testing.T) {
	hm := NewHandleManager()

	// Create handle
	handle := hm.GetOrCreateHandle("/users/123")

	// Should retrieve correct path
	path, ok := hm.GetPath(handle)
	assert.True(t, ok)
	assert.Equal(t, "/users/123", path)

	// Unknown handle should return false
	unknownHandle := []byte("unknown")
	_, ok = hm.GetPath(unknownHandle)
	assert.False(t, ok)
}

// TestHandleManager_RootHandle tests root handle is consistent.
func TestHandleManager_RootHandle(t *testing.T) {
	hm := NewHandleManager()

	// Root handle should be deterministic
	root1 := hm.GetOrCreateHandle("/")
	root2 := hm.GetOrCreateHandle("/")
	assert.Equal(t, root1, root2)

	// Should be able to retrieve root path
	path, ok := hm.GetPath(root1)
	assert.True(t, ok)
	assert.Equal(t, "/", path)
}

// TestHandleManager_Concurrent tests thread safety.
func TestHandleManager_Concurrent(t *testing.T) {
	hm := NewHandleManager()

	// Create handles concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			path := "/table" + string(rune('0'+idx))
			handle := hm.GetOrCreateHandle(path)
			assert.NotNil(t, handle)

			retrieved, ok := hm.GetPath(handle)
			assert.True(t, ok)
			assert.Equal(t, path, retrieved)

			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestHandleManager_HandleToChild tests getting child path handle.
func TestHandleManager_HandleToChild(t *testing.T) {
	hm := NewHandleManager()

	// Create parent handle
	parentHandle := hm.GetOrCreateHandle("/users")

	// Get child handle
	childHandle := hm.HandleToChild(parentHandle, "123")
	require.NotNil(t, childHandle)

	// Child path should be correct
	path, ok := hm.GetPath(childHandle)
	assert.True(t, ok)
	assert.Equal(t, "/users/123", path)
}

// TestHandleManager_HandleToChild_Root tests child of root.
func TestHandleManager_HandleToChild_Root(t *testing.T) {
	hm := NewHandleManager()

	rootHandle := hm.GetOrCreateHandle("/")
	childHandle := hm.HandleToChild(rootHandle, "users")

	path, ok := hm.GetPath(childHandle)
	assert.True(t, ok)
	assert.Equal(t, "/users", path)
}

// TestHandleManager_HandleToChild_Unknown tests child of unknown handle.
func TestHandleManager_HandleToChild_Unknown(t *testing.T) {
	hm := NewHandleManager()

	unknownHandle := []byte("unknown")
	childHandle := hm.HandleToChild(unknownHandle, "child")

	// Should return nil for unknown parent
	assert.Nil(t, childHandle)
}
