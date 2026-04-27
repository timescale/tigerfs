package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
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

// monotonicRetryCount caps how many times readLatestLogIDMonotonic will
// re-poll before giving up. Each retry waits monotonicRetryDelay; the
// total wait budget is roughly count*delay. Sized for the empirically
// observed lag distribution: regressions recover anywhere from 150ms
// to ~1.55s, with the runtime distribution biased toward the short end.
// 2.5s leaves ~60% headroom over the observed max while staying under
// any reasonable per-iteration budget.
const (
	monotonicRetryCount = 25
	monotonicRetryDelay = 100 * time.Millisecond
)

// readLatestLogIDMonotonic wraps readLatestLogID with a safety net for
// the empirically observed staleness path:
//
// After a heavy undo_to_savepoint, an immediate read of
// `.log/.last/N/.export/json` over NFS sometimes returns a snapshot
// from before the undo's log rows were visible -- so the "newest"
// log_id reported is older than ones we've already seen. The post-undo
// log_id can only ever be greater than every prior log_id (UUIDv7 is
// time-ordered), so a smaller value is provably stale.
//
// The fix: re-poll briefly. If recovery happens within the retry
// budget, return the larger value. If the read keeps regressing past
// the budget, log a warning and return the prior known good (we don't
// regress the runner's lastLogID; downstream ops would over-attribute
// previously-seen log entries to themselves).
func readLatestLogIDMonotonic(wsPath, priorLastLogID string, iter int, desc string) string {
	got := readLatestLogID(wsPath)
	if got == "" {
		// Read failed entirely; keep prior. (Also empty/never-set.)
		return priorLastLogID
	}
	if priorLastLogID == "" || got >= priorLastLogID {
		return got
	}
	// Regression. Retry with brief sleeps.
	for attempt := 1; attempt <= monotonicRetryCount; attempt++ {
		time.Sleep(monotonicRetryDelay)
		got = readLatestLogID(wsPath)
		if got >= priorLastLogID {
			fmt.Fprintf(os.Stderr,
				"  [warn iter %d] readLatestLogID regressed after %q; recovered after %d retries (~%v)\n",
				iter, desc, attempt, time.Duration(attempt)*monotonicRetryDelay)
			return got
		}
	}
	fmt.Fprintf(os.Stderr,
		"  [warn iter %d] readLatestLogID regressed after %q and did NOT recover within %d retries; keeping prior %s (got stuck at %s)\n",
		iter, desc, monotonicRetryCount, priorLastLogID, got)
	return priorLastLogID
}

// logScanDepth is the minimum number of log entries to scan when looking for
// new entries since the last known log_id. Sized to comfortably cover any
// single user-level op -- the largest fan-out we see is ~10 log entries from
// a 1MB create_file (one entry per 128KB NFS chunk).
const logScanDepth = 50

// readLogIDsSince returns log_ids of every entry strictly newer than
// sinceLogID, in chronological (oldest-first) order. Used after non-undo ops
// to discover whether a single user-level op produced multiple log entries
// (NFS multi-chunk writes fan out into N entries), so undo_single can be
// gated when it can't reach a workspace-trackable state.
//
// The N passed to readLogEntries doubles as a cache-buster: the path
// `.last/N/.export/json` must be unique on every call or the macOS NFS
// client serves stale attribute/data cache. Use logReadSeq + logScanDepth
// so N is both unique (logReadSeq increments per call) and large enough to
// cover any single op's fan-out.
func readLogIDsSince(wsPath, sinceLogID string) []string {
	logReadSeq++
	entries, err := readLogEntries(wsPath, logReadSeq+logScanDepth)
	if err != nil {
		return nil
	}
	// readLogEntries returns newest-first; iterate reverse for oldest-first.
	var ids []string
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].LogID > sinceLogID {
			ids = append(ids, entries[i].LogID)
		}
	}
	return ids
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
