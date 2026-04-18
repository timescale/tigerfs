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

// OpUndoSingle undoes the most recent operation by applying .undo/id/<log_id>/.apply.
func OpUndoSingle(wsPath string, stack *StateStack) (string, error) {
	if stack.Len() == 0 {
		return "", fmt.Errorf("no operations to undo")
	}

	entries, err := readLogEntries(wsPath, 1)
	if err != nil {
		return "", fmt.Errorf("read log for undo_single: %w", err)
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("log is empty")
	}

	logID := entries[0].LogID

	// Apply undo
	applyPath := filepath.Join(wsPath, ".undo", "id", logID, ".apply")
	if err := os.WriteFile(applyPath, []byte("apply"), 0644); err != nil {
		return "", fmt.Errorf("apply undo id/%s: %w", logID, err)
	}

	// Pop state stack
	restored := stack.Pop()
	if restored == nil {
		return "", fmt.Errorf("stack underflow during undo_single")
	}

	return fmt.Sprintf("undo_single %s (type=%s file=%s)", logID, entries[0].Type, entries[0].Filename), nil
}

// OpUndoToID undoes all operations after a random log entry.
func OpUndoToID(wsPath string, rng *rand.Rand, stack *StateStack) (string, error) {
	if stack.Len() < 2 {
		return "", fmt.Errorf("need at least 2 operations for undo_to_id")
	}

	// How far back to undo (at least 2, at most stack size)
	maxBack := stack.Len()
	if maxBack > 20 {
		maxBack = 20 // cap to avoid huge undos
	}
	nBack := 2 + rng.Intn(maxBack-1)
	if nBack > stack.Len() {
		nBack = stack.Len()
	}

	entries, err := readLogEntries(wsPath, nBack)
	if err != nil {
		return "", fmt.Errorf("read log for undo_to_id: %w", err)
	}
	if len(entries) < nBack {
		nBack = len(entries)
	}
	if nBack < 2 {
		return "", fmt.Errorf("not enough log entries for undo_to_id")
	}

	// Pick the oldest entry in the batch (last in the list since .last/N is newest-first)
	targetEntry := entries[len(entries)-1]
	logID := targetEntry.LogID

	// Apply undo
	applyPath := filepath.Join(wsPath, ".undo", "to-id", logID, ".apply")
	if err := os.WriteFile(applyPath, []byte("apply"), 0644); err != nil {
		return "", fmt.Errorf("apply undo to-id/%s: %w", logID, err)
	}

	// Restore state: go back nBack steps
	targetIdx := stack.Len() - nBack
	if targetIdx < 0 {
		targetIdx = 0
	}
	restored := stack.RestoreToIndex(targetIdx)
	if restored == nil {
		return "", fmt.Errorf("stack restore failed for undo_to_id at index %d", targetIdx)
	}

	return fmt.Sprintf("undo_to_id %s (back %d ops)", logID, nBack), nil
}

// OpUndoToSavepoint undoes all operations after the most recent savepoint.
func OpUndoToSavepoint(wsPath string, stack *StateStack) (string, error) {
	name := stack.MostRecentSavepoint()
	if name == "" {
		return "", fmt.Errorf("no savepoints to undo to")
	}

	// Apply undo
	applyPath := filepath.Join(wsPath, ".undo", "to-savepoint", name, ".apply")
	if err := os.WriteFile(applyPath, []byte("apply"), 0644); err != nil {
		return "", fmt.Errorf("apply undo to-savepoint/%s: %w", name, err)
	}

	// Restore state from savepoint
	restored := stack.RestoreToSavepoint(name)
	if restored == nil {
		return "", fmt.Errorf("savepoint %q not found in stack", name)
	}

	return fmt.Sprintf("undo_to_savepoint %s", name), nil
}

// GetRestoredState returns the current expected state after an undo operation
// has modified the stack. The caller should use this to update their state pointer.
func GetRestoredState(stack *StateStack) *WorkspaceState {
	if stack.Len() == 0 {
		return NewWorkspaceState()
	}
	return stack.entries[stack.Len()-1].State.DeepCopy()
}
