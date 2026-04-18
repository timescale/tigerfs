package main

import (
	"crypto/md5"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WorkspaceState tracks the expected state of a workspace as a map of
// relative file paths to their md5 content hashes, plus a set of directories.
type WorkspaceState struct {
	Files map[string]string // relative path -> md5 hex hash
	Dirs  map[string]bool   // relative path -> exists
}

// NewWorkspaceState creates an empty workspace state.
func NewWorkspaceState() *WorkspaceState {
	return &WorkspaceState{
		Files: make(map[string]string),
		Dirs:  make(map[string]bool),
	}
}

// DeepCopy returns an independent copy of the workspace state.
func (ws *WorkspaceState) DeepCopy() *WorkspaceState {
	clone := &WorkspaceState{
		Files: make(map[string]string, len(ws.Files)),
		Dirs:  make(map[string]bool, len(ws.Dirs)),
	}
	for k, v := range ws.Files {
		clone.Files[k] = v
	}
	for k, v := range ws.Dirs {
		clone.Dirs[k] = v
	}
	return clone
}

// SetFile records a file with the given content hash.
func (ws *WorkspaceState) SetFile(relPath, hash string) {
	ws.Files[relPath] = hash
}

// RemoveFile removes a file from the expected state.
func (ws *WorkspaceState) RemoveFile(relPath string) {
	delete(ws.Files, relPath)
}

// AddDir records a directory.
func (ws *WorkspaceState) AddDir(relPath string) {
	ws.Dirs[relPath] = true
}

// RemoveDir removes a directory and all files/subdirs under it.
func (ws *WorkspaceState) RemoveDir(relPath string) {
	delete(ws.Dirs, relPath)
	prefix := relPath + "/"
	for k := range ws.Files {
		if strings.HasPrefix(k, prefix) {
			delete(ws.Files, k)
		}
	}
	for k := range ws.Dirs {
		if strings.HasPrefix(k, prefix) {
			delete(ws.Dirs, k)
		}
	}
}

// RenameFile moves a file from oldPath to newPath in the expected state.
func (ws *WorkspaceState) RenameFile(oldPath, newPath string) {
	if hash, ok := ws.Files[oldPath]; ok {
		delete(ws.Files, oldPath)
		ws.Files[newPath] = hash
	}
}

// RenameDir moves a directory and all its contents from oldPath to newPath.
func (ws *WorkspaceState) RenameDir(oldPath, newPath string) {
	delete(ws.Dirs, oldPath)
	ws.Dirs[newPath] = true

	oldPrefix := oldPath + "/"
	newPrefix := newPath + "/"

	// Move files
	for k, v := range ws.Files {
		if strings.HasPrefix(k, oldPrefix) {
			newKey := newPrefix + strings.TrimPrefix(k, oldPrefix)
			delete(ws.Files, k)
			ws.Files[newKey] = v
		}
	}

	// Move subdirectories
	for k := range ws.Dirs {
		if strings.HasPrefix(k, oldPrefix) {
			newKey := newPrefix + strings.TrimPrefix(k, oldPrefix)
			delete(ws.Dirs, k)
			ws.Dirs[newKey] = true
		}
	}
}

// FileCount returns the number of files in the given directory (non-recursive).
func (ws *WorkspaceState) FileCount(dirPath string) int {
	count := 0
	prefix := dirPath + "/"
	if dirPath == "" {
		prefix = ""
	}
	for k := range ws.Files {
		rel := k
		if prefix != "" {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			rel = strings.TrimPrefix(k, prefix)
		}
		// Only count direct children (no further slashes)
		if !strings.Contains(rel, "/") {
			count++
		}
	}
	return count
}

// SubdirCount returns the number of direct subdirectories in the given directory.
func (ws *WorkspaceState) SubdirCount(dirPath string) int {
	count := 0
	prefix := dirPath + "/"
	if dirPath == "" {
		prefix = ""
	}
	for k := range ws.Dirs {
		rel := k
		if prefix != "" {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			rel = strings.TrimPrefix(k, prefix)
		} else {
			rel = k
		}
		if rel == "" {
			continue
		}
		// Only count direct children
		if !strings.Contains(rel, "/") {
			count++
		}
	}
	return count
}

// StackEntry is a snapshot of the workspace state at a point in time.
type StackEntry struct {
	State     *WorkspaceState
	Iteration int
}

// StateStack tracks workspace state history for undo operations.
type StateStack struct {
	entries    []StackEntry
	savepoints map[string]int // savepoint name -> stack index
}

// NewStateStack creates an empty state stack.
func NewStateStack() *StateStack {
	return &StateStack{
		entries:    nil,
		savepoints: make(map[string]int),
	}
}

// Push saves the current state before an operation.
func (s *StateStack) Push(state *WorkspaceState, iteration int) {
	s.entries = append(s.entries, StackEntry{
		State:     state.DeepCopy(),
		Iteration: iteration,
	})
}

// Pop removes and returns the most recent state (for undo_single).
// Returns nil if the stack is empty.
func (s *StateStack) Pop() *WorkspaceState {
	if len(s.entries) == 0 {
		return nil
	}
	entry := s.entries[len(s.entries)-1]
	s.entries = s.entries[:len(s.entries)-1]
	return entry.State
}

// Len returns the number of entries on the stack.
func (s *StateStack) Len() int {
	return len(s.entries)
}

// SaveSavepoint records a savepoint at the current stack position.
func (s *StateStack) SaveSavepoint(name string) {
	s.savepoints[name] = len(s.entries)
}

// RestoreToSavepoint returns the state at the given savepoint and trims
// the stack back to that point. Returns nil if savepoint not found.
func (s *StateStack) RestoreToSavepoint(name string) *WorkspaceState {
	idx, ok := s.savepoints[name]
	if !ok {
		return nil
	}
	if idx == 0 {
		// Savepoint was at the very beginning
		s.entries = s.entries[:0]
		return NewWorkspaceState()
	}
	state := s.entries[idx-1].State
	s.entries = s.entries[:idx]

	// Remove savepoints that are after this point
	for spName, spIdx := range s.savepoints {
		if spIdx > idx {
			delete(s.savepoints, spName)
		}
	}

	return state.DeepCopy()
}

// RestoreToIndex returns the state at the given stack index and trims
// the stack back to that point. Returns nil if index is out of bounds.
func (s *StateStack) RestoreToIndex(idx int) *WorkspaceState {
	if idx < 0 || idx >= len(s.entries) {
		return nil
	}
	state := s.entries[idx].State
	s.entries = s.entries[:idx+1]

	// Remove savepoints that are after this point
	for name, spIdx := range s.savepoints {
		if spIdx > idx+1 {
			delete(s.savepoints, name)
		}
	}

	return state.DeepCopy()
}

// MostRecentSavepoint returns the name of the most recently created savepoint,
// or empty string if none exist.
func (s *StateStack) MostRecentSavepoint() string {
	best := ""
	bestIdx := -1
	for name, idx := range s.savepoints {
		if idx > bestIdx {
			best = name
			bestIdx = idx
		}
	}
	return best
}

// HasSavepoints returns true if any savepoints exist.
func (s *StateStack) HasSavepoints() bool {
	return len(s.savepoints) > 0
}

// HashContent returns the md5 hex hash of the given content.
func HashContent(content []byte) string {
	h := md5.Sum(content)
	return fmt.Sprintf("%x", h)
}

// ValidateWorkspace compares the actual filesystem state at wsPath against
// the expected state. Returns nil if they match, or a descriptive error
// listing all mismatches.
func ValidateWorkspace(wsPath string, expected *WorkspaceState) error {
	actualFiles := make(map[string]string) // relpath -> md5 hash

	err := filepath.WalkDir(wsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(wsPath, path)
		if err != nil {
			return err
		}

		// Skip root
		if relPath == "." {
			return nil
		}

		// Skip dotfiles and virtual directories
		name := d.Name()
		if strings.HasPrefix(name, ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if d.IsDir() {
			return nil // we just walk into it
		}

		// Regular file: read and hash
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", relPath, err)
		}
		actualFiles[relPath] = HashContent(content)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk workspace: %w", err)
	}

	var errs []string

	// Check for missing files (expected but not on disk)
	for relPath, expectedHash := range expected.Files {
		actualHash, ok := actualFiles[relPath]
		if !ok {
			errs = append(errs, fmt.Sprintf("missing file: %s (expected hash %s)", relPath, expectedHash[:8]))
			continue
		}
		if actualHash != expectedHash {
			errs = append(errs, fmt.Sprintf("hash mismatch: %s (expected %s, got %s)", relPath, expectedHash[:8], actualHash[:8]))
		}
	}

	// Check for unexpected files (on disk but not expected)
	for relPath := range actualFiles {
		if _, ok := expected.Files[relPath]; !ok {
			errs = append(errs, fmt.Sprintf("unexpected file: %s", relPath))
		}
	}

	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("validation failed (%d issues):\n  %s", len(errs), strings.Join(errs, "\n  "))
	}

	return nil
}

// SnapshotHash computes a deterministic hash of the entire workspace.
// Files are sorted by path; each contributes "relpath:md5hash\n".
// The concatenation is then md5-hashed.
func SnapshotHash(wsPath string) (string, error) {
	var entries []string

	err := filepath.WalkDir(wsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(wsPath, path)
		if err != nil {
			return err
		}

		if relPath == "." {
			return nil
		}

		name := d.Name()
		if strings.HasPrefix(name, ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if d.IsDir() {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", relPath, err)
		}
		entries = append(entries, fmt.Sprintf("%s:%s", relPath, HashContent(content)))
		return nil
	})
	if err != nil {
		return "", err
	}

	sort.Strings(entries)
	combined := strings.Join(entries, "\n") + "\n"
	h := md5.Sum([]byte(combined))
	return fmt.Sprintf("%x", h), nil
}
