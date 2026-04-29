package main

import (
	"crypto/md5"
	"fmt"
	"io/fs"
	"math/rand"
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
//
// Mutations are collected before the loops finish: adding keys to a map
// while ranging over it is undefined per the Go spec, and freshly added
// keys may or may not be visited.
func (ws *WorkspaceState) RenameDir(oldPath, newPath string) {
	delete(ws.Dirs, oldPath)
	ws.Dirs[newPath] = true

	oldPrefix := oldPath + "/"
	newPrefix := newPath + "/"

	// Collect file moves first, then apply.
	type fileMove struct{ oldKey, newKey, hash string }
	var fileMoves []fileMove
	for k, v := range ws.Files {
		if strings.HasPrefix(k, oldPrefix) {
			fileMoves = append(fileMoves, fileMove{
				oldKey: k,
				newKey: newPrefix + strings.TrimPrefix(k, oldPrefix),
				hash:   v,
			})
		}
	}
	for _, m := range fileMoves {
		delete(ws.Files, m.oldKey)
		ws.Files[m.newKey] = m.hash
	}

	// Collect subdirectory moves first, then apply.
	type dirMove struct{ oldKey, newKey string }
	var dirMoves []dirMove
	for k := range ws.Dirs {
		if strings.HasPrefix(k, oldPrefix) {
			dirMoves = append(dirMoves, dirMove{
				oldKey: k,
				newKey: newPrefix + strings.TrimPrefix(k, oldPrefix),
			})
		}
	}
	for _, m := range dirMoves {
		delete(ws.Dirs, m.oldKey)
		ws.Dirs[m.newKey] = true
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
//
// LogID holds the log_id of the operation that ran AFTER this state was
// captured (empty for non-logged ops like create_savepoint). LogCount holds
// how many log entries that operation produced -- usually 1, but a large
// create_file or edit_file fans out into multiple log entries because go-nfs
// fabricates Open/Write/Close per WRITE RPC, so a multi-chunk write commits
// once per chunk. undo_single only undoes the LAST of those entries; the
// intermediate states (file with N chunks of content) are not represented in
// WorkspaceState (which tracks md5 hashes), so undo_single is unsafe for
// multi-log entries -- use undo_to_id or undo_to_savepoint instead.
type StackEntry struct {
	State     *WorkspaceState
	Iteration int
	LogID     string // log_id of the most recent log entry produced by this op
	LogCount  int    // number of log entries produced by this op (0 if non-logged)
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

// SetLastLogID sets the LogID on the most recent stack entry and marks it
// as a single-log-entry operation. Used by ops that push one stack entry per
// log entry (OpDeleteDir walks the deletion tree and pushes per row).
func (s *StateStack) SetLastLogID(logID string) {
	if len(s.entries) > 0 {
		s.entries[len(s.entries)-1].LogID = logID
		s.entries[len(s.entries)-1].LogCount = 1
	}
}

// SetLogIDsForLastEntry records the log_ids produced by the most recent op.
// LogID is set to the newest id (the one undo_single would target). LogCount
// captures the total -- if > 1, the most recent op fanned out into multiple
// log entries (typically multi-chunk NFS writes on large files), and
// undo_single cannot reach a workspace-trackable state by undoing just one
// of them.
func (s *StateStack) SetLogIDsForLastEntry(ids []string) {
	if len(s.entries) == 0 || len(ids) == 0 {
		return
	}
	s.entries[len(s.entries)-1].LogID = ids[len(ids)-1]
	s.entries[len(s.entries)-1].LogCount = len(ids)
}

// MostRecentLogIsAtomic returns true if the most recent logged stack entry
// produced exactly one log entry. Returns false if no logged entries exist
// or if the most recent logged op fanned out into multiple log entries.
// Gates undo_single, which can only safely undo single-log-entry operations
// (intermediate states from chunked writes aren't representable in
// WorkspaceState).
func (s *StateStack) MostRecentLogIsAtomic() bool {
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].LogID != "" {
			return s.entries[i].LogCount == 1
		}
	}
	return false
}

// LoggedCount returns the number of entries with non-empty LogIDs.
func (s *StateStack) LoggedCount() int {
	count := 0
	for _, e := range s.entries {
		if e.LogID != "" {
			count++
		}
	}
	return count
}

// PopToLogID finds the stack entry matching the given LogID (searching from top),
// returns its State (the state before the operation was applied), and trims the
// stack to exclude that entry and everything above it.
func (s *StateStack) PopToLogID(logID string) *WorkspaceState {
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].LogID == logID {
			state := s.entries[i].State
			s.entries = s.entries[:i]
			for name, spIdx := range s.savepoints {
				if spIdx > i {
					delete(s.savepoints, name)
				}
			}
			return state.DeepCopy()
		}
	}
	return nil
}

// RestoreAfterLogID finds the entry with the given LogID, then returns the state
// AFTER that operation (entries[idx+1].State) and trims the stack to [0..idx].
// This is used for undo_to_id where TigerFS keeps the target operation.
func (s *StateStack) RestoreAfterLogID(logID string) *WorkspaceState {
	targetIdx := -1
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].LogID == logID {
			targetIdx = i
			break
		}
	}
	if targetIdx < 0 || targetIdx+1 >= len(s.entries) {
		return nil
	}

	afterState := s.entries[targetIdx+1].State.DeepCopy()
	s.entries = s.entries[:targetIdx+1]
	for name, spIdx := range s.savepoints {
		if spIdx > targetIdx+1 {
			delete(s.savepoints, name)
		}
	}
	return afterState
}

// RandomLoggedTarget picks a random logged stack entry that isn't the most recent
// logged entry (so there's at least one logged operation to undo after it).
// Returns the LogID and true, or empty string and false if not enough logged entries.
func (s *StateStack) RandomLoggedTarget(rng *rand.Rand) (string, bool) {
	var logged []int
	for i, e := range s.entries {
		if e.LogID != "" {
			logged = append(logged, i)
		}
	}
	if len(logged) < 2 {
		return "", false
	}
	// Pick any logged entry except the last one
	idx := rng.Intn(len(logged) - 1)
	return s.entries[logged[idx]].LogID, true
}

// MostRecentLogID returns the LogID of the most recent logged stack entry,
// or empty string if none have LogIDs.
func (s *StateStack) MostRecentLogID() string {
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].LogID != "" {
			return s.entries[i].LogID
		}
	}
	return ""
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

// snapshotWorkspace walks the live filesystem at wsPath and returns
// (files: relpath -> md5 hash, dirs: relpath -> true). Dotfiles and
// dot-prefixed directories (.log, .savepoint, .undo, .history) are TigerFS
// virtuals, not real children of the workspace, and are skipped.
func snapshotWorkspace(wsPath string) (map[string]string, map[string]bool, error) {
	files := make(map[string]string)
	dirs := make(map[string]bool)

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
			dirs[relPath] = true
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", relPath, err)
		}
		files[relPath] = HashContent(content)
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("walk workspace: %w", err)
	}
	return files, dirs, nil
}

// ValidationIssueKind tags one of four divergence types between the
// stress-test expectation and the live workspace. Used both for the
// human-readable summary and for the failure-dump diff.
type ValidationIssueKind string

const (
	IssueMissingFile    ValidationIssueKind = "missing_file"
	IssueUnexpectedFile ValidationIssueKind = "unexpected_file"
	IssueHashMismatch   ValidationIssueKind = "hash_mismatch"
	IssueMissingDir     ValidationIssueKind = "missing_dir"
	IssueUnexpectedDir  ValidationIssueKind = "unexpected_dir"
)

// ValidationIssue describes a single state divergence. ExpectedHash and
// ActualHash are populated only for file issues (empty for dir issues).
type ValidationIssue struct {
	Kind         ValidationIssueKind `json:"kind"`
	Path         string              `json:"path"`
	ExpectedHash string              `json:"expected_hash,omitempty"`
	ActualHash   string              `json:"actual_hash,omitempty"`
}

// diffWorkspace compares expected vs snapshot maps and returns a sorted
// list of issues. Sort order: kind first (so all "missing" group together),
// then path within each kind. Stable across runs for diffability.
func diffWorkspace(expected *WorkspaceState, actualFiles map[string]string, actualDirs map[string]bool) []ValidationIssue {
	var issues []ValidationIssue

	for relPath, expectedHash := range expected.Files {
		actualHash, ok := actualFiles[relPath]
		if !ok {
			issues = append(issues, ValidationIssue{
				Kind:         IssueMissingFile,
				Path:         relPath,
				ExpectedHash: expectedHash,
			})
			continue
		}
		if actualHash != expectedHash {
			issues = append(issues, ValidationIssue{
				Kind:         IssueHashMismatch,
				Path:         relPath,
				ExpectedHash: expectedHash,
				ActualHash:   actualHash,
			})
		}
	}
	for relPath, actualHash := range actualFiles {
		if _, ok := expected.Files[relPath]; !ok {
			issues = append(issues, ValidationIssue{
				Kind:       IssueUnexpectedFile,
				Path:       relPath,
				ActualHash: actualHash,
			})
		}
	}
	for relPath := range expected.Dirs {
		if !actualDirs[relPath] {
			issues = append(issues, ValidationIssue{Kind: IssueMissingDir, Path: relPath})
		}
	}
	for relPath := range actualDirs {
		if _, ok := expected.Dirs[relPath]; !ok {
			issues = append(issues, ValidationIssue{Kind: IssueUnexpectedDir, Path: relPath})
		}
	}

	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Kind != issues[j].Kind {
			return issues[i].Kind < issues[j].Kind
		}
		return issues[i].Path < issues[j].Path
	})
	return issues
}

// ValidateWorkspace compares the actual filesystem state at wsPath against
// the expected state. Returns nil if they match, or an error whose message
// lists all mismatches.
//
// Validates four invariants:
//   - every file in expected.Files exists on disk and hashes correctly,
//   - no extra files are on disk,
//   - every dir in expected.Dirs exists on disk,
//   - no extra dirs are on disk.
func ValidateWorkspace(wsPath string, expected *WorkspaceState) error {
	actualFiles, actualDirs, err := snapshotWorkspace(wsPath)
	if err != nil {
		return err
	}
	issues := diffWorkspace(expected, actualFiles, actualDirs)
	if len(issues) == 0 {
		return nil
	}
	lines := make([]string, len(issues))
	for i, iss := range issues {
		lines[i] = formatIssue(iss)
	}
	return fmt.Errorf("validation failed (%d issues):\n  %s", len(issues), strings.Join(lines, "\n  "))
}

// formatIssue produces the one-line human-readable summary of an issue,
// matching the historical text format so existing log greps still work.
func formatIssue(iss ValidationIssue) string {
	switch iss.Kind {
	case IssueMissingFile:
		return fmt.Sprintf("missing file: %s (expected hash %s)", iss.Path, shortHash(iss.ExpectedHash))
	case IssueUnexpectedFile:
		return fmt.Sprintf("unexpected file: %s", iss.Path)
	case IssueHashMismatch:
		return fmt.Sprintf("hash mismatch: %s (expected %s, got %s)", iss.Path, shortHash(iss.ExpectedHash), shortHash(iss.ActualHash))
	case IssueMissingDir:
		return fmt.Sprintf("missing dir: %s", iss.Path)
	case IssueUnexpectedDir:
		return fmt.Sprintf("unexpected dir: %s", iss.Path)
	}
	return fmt.Sprintf("unknown issue: %+v", iss)
}

func shortHash(h string) string {
	if len(h) >= 8 {
		return h[:8]
	}
	return h
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
