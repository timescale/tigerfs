package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// WriteState tracks an open file being written.
//
// NFS writes come in chunks, so we buffer data until the file is closed
// or committed. The buffered data is then passed to fs.WriteFile.
type WriteState struct {
	// Path is the filesystem path being written to.
	Path string
	// Buffer accumulates write data.
	Buffer bytes.Buffer
	// CreatedAt is when this write state was created.
	CreatedAt time.Time
}

// Write appends data to the buffer.
func (ws *WriteState) Write(data []byte) (int, error) {
	return ws.Buffer.Write(data)
}

// WriteAt writes data at a specific offset.
// For simplicity, this appends if offset matches current length,
// or extends the buffer if needed.
func (ws *WriteState) WriteAt(data []byte, offset int64) (int, error) {
	current := ws.Buffer.Bytes()

	// If writing beyond current length, extend with zeros
	if offset > int64(len(current)) {
		padding := make([]byte, offset-int64(len(current)))
		ws.Buffer.Write(padding)
		current = ws.Buffer.Bytes()
	}

	// If offset is within current data, we need to replace
	if offset < int64(len(current)) {
		// Create new buffer with data at correct position
		newData := make([]byte, max(int64(len(current)), offset+int64(len(data))))
		copy(newData, current)
		copy(newData[offset:], data)
		ws.Buffer.Reset()
		ws.Buffer.Write(newData)
		return len(data), nil
	}

	// Appending at end
	return ws.Buffer.Write(data)
}

// Truncate clears the buffer.
func (ws *WriteState) Truncate() {
	ws.Buffer.Reset()
}

// HandleManager maps NFS file handles to filesystem paths.
//
// NFS uses opaque file handles to identify files and directories. This manager
// maintains a bidirectional mapping between handles and paths, allowing the
// NFS handler to translate handle-based requests to path-based fs.Operations calls.
//
// It also tracks write states for files being written, buffering data until
// the file is closed or committed.
//
// Thread-safety: All methods are safe for concurrent use.
type HandleManager struct {
	pathToHandle map[string][]byte
	handleToPath map[string]string      // handle bytes as string key
	writeStates  map[string]*WriteState // handle bytes as string key
	nextID       uint64
	mu           sync.RWMutex
}

// NewHandleManager creates a new handle manager.
//
// Returns a new HandleManager ready for use with empty mappings.
func NewHandleManager() *HandleManager {
	return &HandleManager{
		pathToHandle: make(map[string][]byte),
		handleToPath: make(map[string]string),
		writeStates:  make(map[string]*WriteState),
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

// CreateWriteState creates a new write state for buffering file writes.
//
// NFS writes arrive in chunks that need to be buffered until the file is
// closed or committed. This method creates the buffer and returns a handle
// that can be used for subsequent write operations.
//
// Parameters:
//   - fsPath: the filesystem path being written to (e.g., "/users/new.json")
//
// Returns a handle that can be used to reference this write state.
func (m *HandleManager) CreateWriteState(fsPath string) []byte {
	handle := m.GetOrCreateHandle(fsPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeStates[string(handle)] = &WriteState{
		Path:      fsPath,
		CreatedAt: time.Now(),
	}

	return handle
}

// GetWriteState retrieves the write state for a handle.
//
// Parameters:
//   - handle: the file handle returned by CreateWriteState
//
// Returns the WriteState and true if found, or nil and false if no write
// state exists for this handle (e.g., it's a read-only handle or was already closed).
func (m *HandleManager) GetWriteState(handle []byte) (*WriteState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ws, exists := m.writeStates[string(handle)]
	return ws, exists
}

// CloseWriteState closes a write state and returns the buffered data.
//
// This method retrieves all buffered data, removes the write state from
// tracking, and returns the data for persistence to the database.
//
// Parameters:
//   - handle: the file handle for the write state
//
// Returns:
//   - data: the complete buffered file contents
//   - path: the filesystem path the data should be written to
//   - err: error if the handle has no associated write state
func (m *HandleManager) CloseWriteState(handle []byte) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := string(handle)
	ws, exists := m.writeStates[key]
	if !exists {
		return nil, "", fmt.Errorf("no write state for handle")
	}

	data := ws.Buffer.Bytes()
	path := ws.Path

	delete(m.writeStates, key)

	return data, path, nil
}
