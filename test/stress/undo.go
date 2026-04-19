package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

// LogEntry represents a single entry from .log/.last/N/.export/json.
type LogEntry struct {
	LogID     string `json:"log_id"`
	FileID    string `json:"file_id"`
	Type      string `json:"type"`
	Filename  string `json:"filename"`
	UserID    string `json:"user_id"`
	VersionID string `json:"version_id"`
}

// logReadSeq is an incrementing counter used to bust NFS attribute caching.
// Each call to readLogEntries uses a unique .last/N path, preventing the
// macOS NFS client from serving stale cached data for virtual log files.
var logReadSeq int

// readLogEntries reads the N most recent log entries from the workspace.
func readLogEntries(wsPath string, n int) ([]LogEntry, error) {
	logPath := filepath.Join(wsPath, ".log", fmt.Sprintf(".last/%d/.export/json", n))
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("read log: %w", err)
	}

	var entries []LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse log JSON: %w", err)
	}
	return entries, nil
}

// readLatestLogEntry reads the most recent log entry, using an incrementing
// path counter to bypass NFS attribute caching.
func readLatestLogEntry(wsPath string) (*LogEntry, error) {
	logReadSeq++
	entries, err := readLogEntries(wsPath, logReadSeq)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("log is empty")
	}
	return &entries[0], nil
}

// readLatestLogID reads the most recent log entry's ID (cache-busting).
func readLatestLogID(wsPath string) string {
	entry, err := readLatestLogEntry(wsPath)
	if err != nil {
		return ""
	}
	return entry.LogID
}

// OpUndoSingle undoes the most recent logged operation.
// Uses the stack (not the TigerFS log) to identify the target, avoiding issues
// with NFS caching and undo log entries that TigerFS adds after undo operations.
// Returns the expected state after undo (state before the undone operation).
func OpUndoSingle(wsPath string, stack *StateStack) (string, *WorkspaceState, error) {
	logID := stack.MostRecentLogID()
	if logID == "" {
		return "", nil, fmt.Errorf("no logged operations to undo")
	}

	// Apply undo
	applyPath := filepath.Join(wsPath, ".undo", "id", logID, ".apply")
	if err := os.WriteFile(applyPath, []byte("apply"), 0644); err != nil {
		return "", nil, fmt.Errorf("apply undo id/%s: %w", logID, err)
	}

	// Pop the stack entry and any non-logged entries above it.
	// The returned state is the state before the undone operation.
	restored := stack.PopToLogID(logID)
	if restored == nil {
		return "", nil, fmt.Errorf("log_id %s not found in stack", logID)
	}

	return fmt.Sprintf("undo_single %s", logID), restored, nil
}

// OpUndoToID undoes all logged operations after a random logged stack entry.
// Returns the expected state after undo (state after the target operation).
func OpUndoToID(wsPath string, rng *rand.Rand, stack *StateStack) (string, *WorkspaceState, error) {
	// Pick a random logged entry (not the most recent logged one)
	logID, ok := stack.RandomLoggedTarget(rng)
	if !ok {
		return "", nil, fmt.Errorf("need at least 2 logged operations for undo_to_id")
	}

	// Apply undo -- TigerFS undoes everything AFTER this log_id
	applyPath := filepath.Join(wsPath, ".undo", "to-id", logID, ".apply")
	if err := os.WriteFile(applyPath, []byte("apply"), 0644); err != nil {
		return "", nil, fmt.Errorf("apply undo to-id/%s: %w", logID, err)
	}

	// Restore state: the entry AFTER the target has the state we want
	// (state after the target operation = state before the next operation).
	restored := stack.RestoreAfterLogID(logID)
	if restored == nil {
		return "", nil, fmt.Errorf("could not restore state for undo to-id/%s", logID)
	}

	return fmt.Sprintf("undo_to_id %s", logID), restored, nil
}

// OpUndoToSavepoint undoes all operations after the most recent savepoint.
// Returns the expected state after undo (state at the savepoint).
func OpUndoToSavepoint(wsPath string, stack *StateStack) (string, *WorkspaceState, error) {
	name := stack.MostRecentSavepoint()
	if name == "" {
		return "", nil, fmt.Errorf("no savepoints to undo to")
	}

	// Apply undo
	applyPath := filepath.Join(wsPath, ".undo", "to-savepoint", name, ".apply")
	if err := os.WriteFile(applyPath, []byte("apply"), 0644); err != nil {
		return "", nil, fmt.Errorf("apply undo to-savepoint/%s: %w", name, err)
	}

	// Restore state from savepoint
	restored := stack.RestoreToSavepoint(name)
	if restored == nil {
		return "", nil, fmt.Errorf("savepoint %q not found in stack", name)
	}

	return fmt.Sprintf("undo_to_savepoint %s", name), restored, nil
}
