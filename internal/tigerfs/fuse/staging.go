package fuse

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// StagingEntry represents a staged DDL operation.
// Stores user-provided DDL content and validation results.
type StagingEntry struct {
	Content    string            // User-provided DDL content
	TestResult string            // Last test result (success message or error)
	CreatedAt  time.Time         // When the entry was created
	ExtraFiles map[string][]byte // Editor temp files (backups, swap files, etc.)
}

// StagingTracker manages in-memory staging state for DDL operations.
// Thread-safe storage for DDL content that will be executed via .commit files.
//
// Usage pattern:
//  1. User writes DDL to .schema file -> stored in StagingTracker
//  2. User touches .test file -> DDL validated via BEGIN/ROLLBACK
//  3. User touches .commit file -> DDL executed
//  4. User touches .abort file -> staging entry cleared
type StagingTracker struct {
	mu      sync.RWMutex
	entries map[string]*StagingEntry // keyed by staging path (e.g., ".create/orders", "users/.modify")
}

// NewStagingTracker creates a new staging tracker.
func NewStagingTracker() *StagingTracker {
	return &StagingTracker{
		entries: make(map[string]*StagingEntry),
	}
}

// GetOrCreate gets an existing staging entry or creates a new one.
// Returns the entry (never nil).
func (t *StagingTracker) GetOrCreate(path string) *StagingEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	if entry, exists := t.entries[path]; exists {
		return entry
	}

	entry := &StagingEntry{
		CreatedAt: time.Now(),
	}
	t.entries[path] = entry

	logging.Debug("Created staging entry",
		zap.String("path", path))

	return entry
}

// Get retrieves a staging entry if it exists.
// Returns nil if no entry exists at the given path.
func (t *StagingTracker) Get(path string) *StagingEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.entries[path]
}

// Set stores content in a staging entry, creating the entry if needed.
func (t *StagingTracker) Set(path string, content string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, exists := t.entries[path]
	if !exists {
		entry = &StagingEntry{
			CreatedAt: time.Now(),
		}
		t.entries[path] = entry
	}

	entry.Content = content

	logging.Debug("Set staging content",
		zap.String("path", path),
		zap.Int("content_length", len(content)))
}

// SetTestResult stores the test result for a staging entry.
func (t *StagingTracker) SetTestResult(path string, result string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if entry, exists := t.entries[path]; exists {
		entry.TestResult = result
	}
}

// Delete removes a staging entry.
func (t *StagingTracker) Delete(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.entries, path)

	logging.Debug("Deleted staging entry",
		zap.String("path", path))
}

// HasContent checks if a staging entry exists and has non-empty, non-comment content.
// Returns false if the entry doesn't exist or contains only comments/whitespace.
func (t *StagingTracker) HasContent(path string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry, exists := t.entries[path]
	if !exists {
		return false
	}

	return !IsEmptyOrCommented(entry.Content)
}

// GetContent returns the content of a staging entry, or empty string if not found.
func (t *StagingTracker) GetContent(path string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if entry, exists := t.entries[path]; exists {
		return entry.Content
	}
	return ""
}

// GetTestResult returns the test result of a staging entry, or empty string if not found.
func (t *StagingTracker) GetTestResult(path string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if entry, exists := t.entries[path]; exists {
		return entry.TestResult
	}
	return ""
}

// ListPending returns all pending staging paths with a given prefix.
// Used by .create directories to list pending creations.
func (t *StagingTracker) ListPending(prefix string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var paths []string
	for path := range t.entries {
		if strings.HasPrefix(path, prefix) {
			// Extract the name after the prefix
			rest := strings.TrimPrefix(path, prefix)
			rest = strings.TrimPrefix(rest, "/")
			// Only include direct children (no further slashes)
			if rest != "" && !strings.Contains(rest, "/") {
				paths = append(paths, rest)
			}
		}
	}
	return paths
}

// SetExtraFile stores an extra file (editor temp file) in a staging entry.
// Creates the entry if it doesn't exist.
func (t *StagingTracker) SetExtraFile(path string, filename string, content []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, exists := t.entries[path]
	if !exists {
		entry = &StagingEntry{
			CreatedAt:  time.Now(),
			ExtraFiles: make(map[string][]byte),
		}
		t.entries[path] = entry
	}

	if entry.ExtraFiles == nil {
		entry.ExtraFiles = make(map[string][]byte)
	}

	entry.ExtraFiles[filename] = content

	logging.Debug("Set extra file",
		zap.String("path", path),
		zap.String("filename", filename),
		zap.Int("size", len(content)))
}

// GetExtraFile retrieves an extra file from a staging entry.
// Returns nil if not found.
func (t *StagingTracker) GetExtraFile(path string, filename string) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry, exists := t.entries[path]
	if !exists || entry.ExtraFiles == nil {
		return nil
	}

	return entry.ExtraFiles[filename]
}

// DeleteExtraFile removes an extra file from a staging entry.
func (t *StagingTracker) DeleteExtraFile(path string, filename string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, exists := t.entries[path]
	if !exists || entry.ExtraFiles == nil {
		return
	}

	delete(entry.ExtraFiles, filename)

	logging.Debug("Deleted extra file",
		zap.String("path", path),
		zap.String("filename", filename))
}

// ListExtraFiles returns the names of all extra files in a staging entry.
func (t *StagingTracker) ListExtraFiles(path string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry, exists := t.entries[path]
	if !exists || entry.ExtraFiles == nil {
		return nil
	}

	names := make([]string, 0, len(entry.ExtraFiles))
	for name := range entry.ExtraFiles {
		names = append(names, name)
	}
	return names
}

// HasExtraFile checks if an extra file exists in a staging entry.
func (t *StagingTracker) HasExtraFile(path string, filename string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry, exists := t.entries[path]
	if !exists || entry.ExtraFiles == nil {
		return false
	}

	_, exists = entry.ExtraFiles[filename]
	return exists
}

// IsEmptyOrCommented checks if content is empty or contains only SQL comments.
// Returns true if the content has no executable SQL statements.
//
// Handles:
//   - Empty strings
//   - Whitespace-only strings
//   - Single-line comments (-- comment)
//   - Block comments (/* comment */)
func IsEmptyOrCommented(content string) bool {
	if content == "" {
		return true
	}

	// Remove block comments /* ... */
	blockCommentRe := regexp.MustCompile(`/\*[\s\S]*?\*/`)
	cleaned := blockCommentRe.ReplaceAllString(content, "")

	// Remove single-line comments -- ...
	lineCommentRe := regexp.MustCompile(`--.*$`)
	lines := strings.Split(cleaned, "\n")
	var nonCommentLines []string
	for _, line := range lines {
		// Remove line comment from this line
		line = lineCommentRe.ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line != "" {
			nonCommentLines = append(nonCommentLines, line)
		}
	}

	return len(nonCommentLines) == 0
}

// ExtractSQL extracts executable SQL from content, removing comments.
// Returns the SQL with comments stripped.
func ExtractSQL(content string) string {
	// Remove block comments /* ... */
	blockCommentRe := regexp.MustCompile(`/\*[\s\S]*?\*/`)
	cleaned := blockCommentRe.ReplaceAllString(content, "")

	// Remove single-line comments -- ... but preserve newlines
	lineCommentRe := regexp.MustCompile(`--.*$`)
	lines := strings.Split(cleaned, "\n")
	var resultLines []string
	for _, line := range lines {
		// Remove line comment from this line
		line = lineCommentRe.ReplaceAllString(line, "")
		line = strings.TrimRight(line, " \t")
		resultLines = append(resultLines, line)
	}

	result := strings.Join(resultLines, "\n")
	return strings.TrimSpace(result)
}
