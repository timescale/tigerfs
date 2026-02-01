package nfs

import (
	"encoding/binary"
	"path"
	"sync"
	"sync/atomic"
)

// HandleManager maps NFS file handles to filesystem paths.
//
// NFS uses opaque file handles to identify files and directories. This manager
// maintains a bidirectional mapping between handles and paths, allowing the
// NFS handler to translate handle-based requests to path-based fs.Operations calls.
//
// Thread-safety: All methods are safe for concurrent use.
type HandleManager struct {
	pathToHandle map[string][]byte
	handleToPath map[string]string // handle bytes as string key
	nextID       uint64
	mu           sync.RWMutex
}

// NewHandleManager creates a new handle manager.
func NewHandleManager() *HandleManager {
	return &HandleManager{
		pathToHandle: make(map[string][]byte),
		handleToPath: make(map[string]string),
	}
}

// GetOrCreateHandle returns the handle for a path, creating one if needed.
//
// Parameters:
//   - path: filesystem path (e.g., "/users/123")
//
// Returns the file handle (never nil for valid paths).
func (m *HandleManager) GetOrCreateHandle(fsPath string) []byte {
	m.mu.RLock()
	if handle, exists := m.pathToHandle[fsPath]; exists {
		m.mu.RUnlock()
		return handle
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if handle, exists := m.pathToHandle[fsPath]; exists {
		return handle
	}

	// Create new handle
	id := atomic.AddUint64(&m.nextID, 1)
	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, id)

	m.pathToHandle[fsPath] = handle
	m.handleToPath[string(handle)] = fsPath

	return handle
}

// GetPath returns the path for a handle.
//
// Parameters:
//   - handle: NFS file handle
//
// Returns the path and true if found, or empty string and false if not found.
func (m *HandleManager) GetPath(handle []byte) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path, exists := m.handleToPath[string(handle)]
	return path, exists
}

// HandleToChild returns a handle for a child of the given parent handle.
//
// Parameters:
//   - parentHandle: handle of the parent directory
//   - childName: name of the child entry
//
// Returns the child handle, or nil if the parent handle is unknown.
func (m *HandleManager) HandleToChild(parentHandle []byte, childName string) []byte {
	parentPath, ok := m.GetPath(parentHandle)
	if !ok {
		return nil
	}

	childPath := path.Join(parentPath, childName)
	// path.Join removes trailing slash, but we need "/" for root
	if parentPath == "/" && childName != "" {
		childPath = "/" + childName
	}

	return m.GetOrCreateHandle(childPath)
}
