package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// --- Single operation undo ---

func TestUndo_SingleCreate(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undo1")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undo1", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create a file
	ops.WriteFile(ctx, "/undo1/hello.md", []byte("---\ntitle: Hello\n---\nOriginal\n"))
	require.Nil(t, fsErr)

	// Verify it exists
	_, fsErr = ops.Stat(ctx, "/undo1/hello.md")
	require.Nil(t, fsErr, "file should exist after creation")

	// Find the log entry for the create
	entries, fsErr := ops.ReadDir(ctx, "/undo1/.log/.by/type/create")
	require.Nil(t, fsErr)
	var logIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			logIDs = append(logIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(logIDs), 1, "should have at least one create log entry")

	// Undo the create -- should delete the file
	t.Logf("Undoing log entry: %s", logIDs[len(logIDs)-1])
	undoResult, err := ops.ExecuteUndoSingle(ctx, "public", "undo1", logIDs[len(logIDs)-1])
	require.NoError(t, err)
	t.Logf("Undo result: deleted=%d, restored=%d, skipped=%d",
		undoResult.FilesDeleted, undoResult.FilesRestored, undoResult.FilesSkipped)
	assert.Equal(t, 1, undoResult.FilesDeleted)

	// Verify file is gone via ReadDir (more reliable than Stat for synth views)
	entries, fsErr = ops.ReadDir(ctx, "/undo1")
	require.Nil(t, fsErr)
	var fileNames []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") && strings.HasSuffix(e.Name, ".md") {
			fileNames = append(fileNames, e.Name)
		}
	}
	assert.Empty(t, fileNames, "file should be gone after undoing create, but found: %v", fileNames)
}

func TestUndo_SingleEdit(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undo2")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undo2", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create and then edit
	ops.WriteFile(ctx, "/undo2/hello.md", []byte("---\ntitle: Hello\n---\nVersion 1\n"))
	ops.WriteFile(ctx, "/undo2/hello.md", []byte("---\ntitle: Hello\n---\nVersion 2\n"))

	// Verify current content
	fc, fsErr := ops.ReadFile(ctx, "/undo2/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Version 2")

	// Find the edit log entry
	entries, fsErr := ops.ReadDir(ctx, "/undo2/.log/.by/type/edit")
	require.Nil(t, fsErr)
	var logIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			logIDs = append(logIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(logIDs), 1)

	// Undo the edit -- should restore Version 1
	undoResult, err := ops.ExecuteUndoSingle(ctx, "public", "undo2", logIDs[len(logIDs)-1])
	require.NoError(t, err)
	assert.Equal(t, 1, undoResult.FilesRestored)

	fc, fsErr = ops.ReadFile(ctx, "/undo2/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Version 1", "should be restored to pre-edit state")
}

func TestUndo_SingleDelete(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undo3")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undo3", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create then delete
	ops.WriteFile(ctx, "/undo3/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	ops.Delete(ctx, "/undo3/hello.md")

	// Verify deleted
	_, fsErr = ops.Stat(ctx, "/undo3/hello.md")
	require.NotNil(t, fsErr, "file should be deleted")

	// Find the delete log entry
	entries, fsErr := ops.ReadDir(ctx, "/undo3/.log/.by/type/delete")
	require.Nil(t, fsErr)
	var logIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			logIDs = append(logIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(logIDs), 1)

	// Undo the delete -- should restore the file
	undoResult, err := ops.ExecuteUndoSingle(ctx, "public", "undo3", logIDs[len(logIDs)-1])
	require.NoError(t, err)
	assert.Equal(t, 1, undoResult.FilesRestored)

	fc, fsErr := ops.ReadFile(ctx, "/undo3/hello.md")
	require.Nil(t, fsErr, "file should be restored after undoing delete")
	assert.Contains(t, string(fc.Data), "Content")
}

// --- Multi-file undo to savepoint ---

func TestUndo_ToSavepoint_MultiFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undomulti")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undomulti", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create initial files
	ops.WriteFile(ctx, "/undomulti/existing.md", []byte("---\ntitle: Existing\n---\nOriginal content\n"))

	// Create savepoint
	ops.WriteFile(ctx, "/undomulti/.savepoint/checkpoint.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)

	// After savepoint: create A, edit existing, delete existing
	ops.WriteFile(ctx, "/undomulti/new-file.md", []byte("---\ntitle: New\n---\nNew content\n"))
	ops.WriteFile(ctx, "/undomulti/existing.md", []byte("---\ntitle: Existing\n---\nModified content\n"))

	// Verify state before undo
	_, fsErr = ops.Stat(ctx, "/undomulti/new-file.md")
	require.Nil(t, fsErr, "new-file should exist")
	fc, fsErr := ops.ReadFile(ctx, "/undomulti/existing.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Modified content")

	// Undo to savepoint
	undoResult, err := ops.ExecuteUndoToSavepoint(ctx, "public", "undomulti", "checkpoint", nil)
	require.NoError(t, err)
	t.Logf("Undo result: deleted=%d, restored=%d, skipped=%d",
		undoResult.FilesDeleted, undoResult.FilesRestored, undoResult.FilesSkipped)

	// new-file should be gone (created after savepoint)
	// Use ReadDir instead of Stat -- synth path cache may be stale
	dirEntries, fsErr := ops.ReadDir(ctx, "/undomulti")
	require.Nil(t, fsErr)
	var mdFiles []string
	for _, e := range dirEntries {
		if strings.HasSuffix(e.Name, ".md") {
			mdFiles = append(mdFiles, e.Name)
		}
	}
	assert.NotContains(t, mdFiles, "new-file.md", "new-file should be deleted after undo")

	// existing should be restored to original content
	fc, fsErr = ops.ReadFile(ctx, "/undomulti/existing.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Original content", "existing should be restored")
}

// --- Undo by user ---

func TestUndo_ByUser(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undouser")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undouser", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create initial files
	ops.WriteFile(ctx, "/undouser/shared.md", []byte("---\ntitle: Shared\n---\nOriginal\n"))

	// Create savepoint
	ops.WriteFile(ctx, "/undouser/.savepoint/before-edits.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)

	// agent-7 edits shared.md
	ops.SetUserID("agent-7")
	ops.WriteFile(ctx, "/undouser/shared.md", []byte("---\ntitle: Shared\n---\nEdited by agent-7\n"))

	// agent-9 creates a new file
	ops.SetUserID("agent-9")
	ops.WriteFile(ctx, "/undouser/agent9-file.md", []byte("---\ntitle: Agent9\n---\nBy agent-9\n"))

	// Undo only agent-7's changes
	ops.SetUserID("agent-7")
	filters := []db.UndoFilter{{Column: "user_id", Value: "agent-7"}}
	undoResult, err := ops.ExecuteUndoToSavepoint(ctx, "public", "undouser", "before-edits", filters)
	require.NoError(t, err)
	t.Logf("Undo by user result: deleted=%d, restored=%d, skipped=%d",
		undoResult.FilesDeleted, undoResult.FilesRestored, undoResult.FilesSkipped)

	// shared.md should be restored to original (agent-7's edit undone)
	fc, fsErr := ops.ReadFile(ctx, "/undouser/shared.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Original", "agent-7's edit should be undone")

	// agent-9's file should still exist (not affected by agent-7 undo)
	_, fsErr = ops.Stat(ctx, "/undouser/agent9-file.md")
	assert.Nil(t, fsErr, "agent-9's file should be preserved")
}

// --- Undo of undo (ADR Section 3.4) ---

func TestUndo_UndoOfUndo(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoundo")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoundo", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Step 1: Create with v1
	ops.WriteFile(ctx, "/undoundo/hello.md", []byte("---\ntitle: Hello\n---\nVersion 1\n"))

	// Step 2: Edit to v2
	ops.WriteFile(ctx, "/undoundo/hello.md", []byte("---\ntitle: Hello\n---\nVersion 2\n"))

	// Find the edit log entry (L1)
	entries, fsErr := ops.ReadDir(ctx, "/undoundo/.log/.by/type/edit")
	require.Nil(t, fsErr)
	var editLogIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			editLogIDs = append(editLogIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(editLogIDs), 1)
	editLogID := editLogIDs[len(editLogIDs)-1]

	// Step 3: Undo the edit (restores v1)
	_, err := ops.ExecuteUndoSingle(ctx, "public", "undoundo", editLogID)
	require.NoError(t, err)

	fc, fsErr := ops.ReadFile(ctx, "/undoundo/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Version 1", "after first undo, should be v1")

	// Find the undo log entry (L2)
	entries, fsErr = ops.ReadDir(ctx, "/undoundo/.log/.by/type/undo")
	require.Nil(t, fsErr)
	var undoLogIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			undoLogIDs = append(undoLogIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(undoLogIDs), 1)
	undoLogID := undoLogIDs[len(undoLogIDs)-1]

	// Step 4: Undo the undo (restores v2)
	_, err = ops.ExecuteUndoSingle(ctx, "public", "undoundo", undoLogID)
	require.NoError(t, err)

	fc, fsErr = ops.ReadFile(ctx, "/undoundo/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Version 2", "after undo-of-undo, should be v2")
}

// --- Idempotent undo to savepoint (ADR Section 3.5) ---

func TestUndo_Idempotent(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoidem")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoidem", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create initial state
	ops.WriteFile(ctx, "/undoidem/hello.md", []byte("---\ntitle: Hello\n---\nOriginal\n"))
	ops.WriteFile(ctx, "/undoidem/.savepoint/checkpoint.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)

	// Modify after savepoint
	ops.WriteFile(ctx, "/undoidem/hello.md", []byte("---\ntitle: Hello\n---\nModified\n"))

	// Undo to savepoint -- first time
	_, err := ops.ExecuteUndoToSavepoint(ctx, "public", "undoidem", "checkpoint", nil)
	require.NoError(t, err)

	fc, fsErr := ops.ReadFile(ctx, "/undoidem/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Original")

	// Undo to same savepoint again -- should produce same data state
	_, err = ops.ExecuteUndoToSavepoint(ctx, "public", "undoidem", "checkpoint", nil)
	require.NoError(t, err)

	fc, fsErr = ops.ReadFile(ctx, "/undoidem/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Original", "idempotent: same result on second undo")
}

// --- Undo with no operations after target ---

func TestUndo_NoOpsAfterSavepoint(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undonoop")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undonoop", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undonoop/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	ops.WriteFile(ctx, "/undonoop/.savepoint/latest.json", []byte("{}"))

	// No operations after savepoint -- undo should be a no-op
	undoResult, err := ops.ExecuteUndoToSavepoint(ctx, "public", "undonoop", "latest", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, undoResult.FilesDeleted)
	assert.Equal(t, 0, undoResult.FilesRestored)
	assert.Equal(t, 0, undoResult.FilesSkipped)

	// File should still be there unchanged
	fc, fsErr := ops.ReadFile(ctx, "/undonoop/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Content")
}

// --- Undo log entries verification ---

func TestUndo_CreatesUndoLogEntries(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undolog")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undolog", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undolog/hello.md", []byte("---\ntitle: Hello\n---\nV1\n"))
	ops.WriteFile(ctx, "/undolog/.savepoint/sp1.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)
	ops.WriteFile(ctx, "/undolog/hello.md", []byte("---\ntitle: Hello\n---\nV2\n"))

	// Count undo log entries before
	entries, fsErr := ops.ReadDir(ctx, "/undolog/.log/.by/type/undo")
	undoBefore := 0
	if fsErr == nil {
		for _, e := range entries {
			if !strings.HasPrefix(e.Name, ".") {
				undoBefore++
			}
		}
	}

	// Undo to savepoint
	_, err := ops.ExecuteUndoToSavepoint(ctx, "public", "undolog", "sp1", nil)
	require.NoError(t, err)

	// Count undo log entries after
	entries, fsErr = ops.ReadDir(ctx, "/undolog/.log/.by/type/undo")
	require.Nil(t, fsErr)
	undoAfter := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			undoAfter++
		}
	}
	assert.Greater(t, undoAfter, undoBefore, "undo should create new log entries with type='undo'")
}

// --- Filtered undo ---

func TestUndo_FilterByType(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undofilt")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undofilt", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create two files, savepoint, then delete one and edit the other
	ops.WriteFile(ctx, "/undofilt/keep.md", []byte("---\ntitle: Keep\n---\nKeep this\n"))
	ops.WriteFile(ctx, "/undofilt/restore-me.md", []byte("---\ntitle: Restore\n---\nRestore this\n"))
	ops.WriteFile(ctx, "/undofilt/.savepoint/sp1.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)

	ops.WriteFile(ctx, "/undofilt/keep.md", []byte("---\ntitle: Keep\n---\nEdited\n"))
	ops.Delete(ctx, "/undofilt/restore-me.md")

	// Undo only deletes (not edits)
	filters := []db.UndoFilter{{Column: "type", Value: "delete"}}
	undoResult, err := ops.ExecuteUndoToSavepoint(ctx, "public", "undofilt", "sp1", filters)
	require.NoError(t, err)
	t.Logf("Filtered undo: deleted=%d, restored=%d, skipped=%d",
		undoResult.FilesDeleted, undoResult.FilesRestored, undoResult.FilesSkipped)

	// restore-me.md should be back
	_, fsErr = ops.Stat(ctx, "/undofilt/restore-me.md")
	assert.Nil(t, fsErr, "deleted file should be restored")

	// keep.md should still have the edited content (edit was not undone)
	fc, fsErr := ops.ReadFile(ctx, "/undofilt/keep.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Edited", "edit should NOT be undone by type=delete filter")
}

// Note: Rename undo (parent_id restoration) requires the NFS adapter's Rename method,
// which is not exposed on fs.Operations. Tested via mount-based integration tests.

// --- .undo/ filesystem interface tests ---

func TestUndo_Interface_ReadDirRoot(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// ReadDir .undo/ should list id/, to-id/, to-savepoint/
	entries, fsErr := ops.ReadDir(ctx, "/undoui/.undo")
	require.Nil(t, fsErr)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	assert.Contains(t, names, "id")
	assert.Contains(t, names, "to-id")
	assert.Contains(t, names, "to-savepoint")
}

func TestUndo_Interface_ReadDirSavepoints(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui2")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui2", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui2/.savepoint/sp1.json", []byte("{}"))
	ops.WriteFile(ctx, "/undoui2/.savepoint/sp2.json", []byte("{}"))

	// ReadDir .undo/to-savepoint/ should list savepoints
	entries, fsErr := ops.ReadDir(ctx, "/undoui2/.undo/to-savepoint")
	require.Nil(t, fsErr)
	var names []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			names = append(names, e.Name)
		}
	}
	assert.Contains(t, names, "sp1")
	assert.Contains(t, names, "sp2")
}

func TestUndo_Interface_Summary(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui3")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui3", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui3/existing.md", []byte("---\ntitle: Existing\n---\nOriginal\n"))
	ops.WriteFile(ctx, "/undoui3/.savepoint/checkpoint.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui3/new-file.md", []byte("---\ntitle: New\n---\nNew content\n"))
	ops.WriteFile(ctx, "/undoui3/existing.md", []byte("---\ntitle: Existing\n---\nModified\n"))

	// Read .info/summary (TSV)
	fc, fsErr := ops.ReadFile(ctx, "/undoui3/.undo/to-savepoint/checkpoint/.info/summary")
	require.Nil(t, fsErr, "ReadFile .info/summary should succeed")
	summary := string(fc.Data)
	t.Logf("Summary TSV:\n%s", summary)

	// Metadata headers
	assert.Contains(t, summary, "# savepoint: checkpoint")
	assert.Contains(t, summary, "# affected: 2 files")

	// Column header comment
	assert.Contains(t, summary, "# type\tfilename\tuser\ttimestamp")

	// Data rows with operation type (not "restore"/"delete")
	assert.Contains(t, summary, "create\tnew-file.md")
	assert.Contains(t, summary, "edit\texisting.md")

	// Timestamps should be present (RFC3339 format)
	assert.Contains(t, summary, "202") // year prefix in timestamp
}

func TestUndo_Interface_SummaryJSON(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undojs")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undojs", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undojs/hello.md", []byte("---\ntitle: Hello\n---\nV1\n"))
	ops.WriteFile(ctx, "/undojs/.savepoint/sp1.tsv", []byte("description\nTest savepoint\n"))
	time.Sleep(50 * time.Millisecond)
	ops.WriteFile(ctx, "/undojs/hello.md", []byte("---\ntitle: Hello\n---\nV2\n"))

	fc, fsErr := ops.ReadFile(ctx, "/undojs/.undo/to-savepoint/sp1/.info/summary.json")
	require.Nil(t, fsErr, "ReadFile summary.json should succeed")
	summary := string(fc.Data)
	t.Logf("Summary JSON:\n%s", summary)

	// Structured fields
	assert.Contains(t, summary, `"savepoint"`)
	assert.Contains(t, summary, `"sp1"`)
	assert.Contains(t, summary, `"description"`)
	assert.Contains(t, summary, `"Test savepoint"`)
	assert.Contains(t, summary, `"affected"`)
	assert.Contains(t, summary, `"files"`)
	assert.Contains(t, summary, `"type"`)
	assert.Contains(t, summary, `"edit"`)
	assert.Contains(t, summary, `"hello.md"`)
}

func TestUndo_Interface_SummaryCSV(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undocsv")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undocsv", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undocsv/hello.md", []byte("---\ntitle: Hello\n---\nV1\n"))
	ops.WriteFile(ctx, "/undocsv/.savepoint/sp1.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)
	ops.WriteFile(ctx, "/undocsv/hello.md", []byte("---\ntitle: Hello\n---\nV2\n"))

	fc, fsErr := ops.ReadFile(ctx, "/undocsv/.undo/to-savepoint/sp1/.info/summary.csv")
	require.Nil(t, fsErr, "ReadFile summary.csv should succeed")
	summary := string(fc.Data)
	t.Logf("Summary CSV:\n%s", summary)

	// Header row (no # comments in CSV)
	assert.True(t, strings.HasPrefix(summary, "type,filename,user,timestamp\n"),
		"CSV should start with header row")
	// No metadata comments
	assert.NotContains(t, summary, "#")
	// Data row
	assert.Contains(t, summary, "edit,hello.md,")
}

func TestUndo_Interface_SummaryToID(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undotid")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undotid", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undotid/hello.md", []byte("---\ntitle: Hello\n---\nV1\n"))
	ops.WriteFile(ctx, "/undotid/hello.md", []byte("---\ntitle: Hello\n---\nV2\n"))

	// Get a log entry to use as to-id target
	entries, fsErr := ops.ReadDir(ctx, "/undotid/.log/.first/1")
	require.Nil(t, fsErr)
	var logIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			logIDs = append(logIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(logIDs), 1)

	fc, fsErr := ops.ReadFile(ctx, "/undotid/.undo/to-id/"+logIDs[0]+"/.info/summary")
	require.Nil(t, fsErr, "ReadFile to-id summary should succeed")
	summary := string(fc.Data)
	t.Logf("Summary to-id:\n%s", summary)

	// to-id mode should show # target: instead of # savepoint:
	assert.Contains(t, summary, "# target:")
	assert.NotContains(t, summary, "# savepoint:")
	assert.Contains(t, summary, "# affected:")
}

func TestUndo_Interface_SummaryWithUserID(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undouid")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undouid", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.SetUserID("agent-7")
	ops.WriteFile(ctx, "/undouid/hello.md", []byte("---\ntitle: Hello\n---\nV1\n"))
	ops.WriteFile(ctx, "/undouid/.savepoint/sp1.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)
	ops.WriteFile(ctx, "/undouid/hello.md", []byte("---\ntitle: Hello\n---\nV2\n"))

	fc, fsErr := ops.ReadFile(ctx, "/undouid/.undo/to-savepoint/sp1/.info/summary")
	require.Nil(t, fsErr)
	summary := string(fc.Data)
	t.Logf("Summary with user:\n%s", summary)

	// Per-file user column should show agent-7
	assert.Contains(t, summary, "agent-7")
}

func TestUndo_Interface_PreviewContent(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui4")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui4", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui4/hello.md", []byte("---\ntitle: Hello\n---\nVersion 1\n"))
	ops.WriteFile(ctx, "/undoui4/.savepoint/checkpoint.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)
	ops.WriteFile(ctx, "/undoui4/hello.md", []byte("---\ntitle: Hello\n---\nVersion 2\n"))

	// Preview should show Version 1 (the before-state)
	fc, fsErr := ops.ReadFile(ctx, "/undoui4/.undo/to-savepoint/checkpoint/hello.md")
	require.Nil(t, fsErr, "ReadFile preview should succeed")
	assert.Contains(t, string(fc.Data), "Version 1", "preview should show before-state")
	assert.NotContains(t, string(fc.Data), "Version 2")
}

func TestUndo_Interface_ApplyViaSavepoint(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui5")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui5", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui5/hello.md", []byte("---\ntitle: Hello\n---\nOriginal\n"))
	ops.WriteFile(ctx, "/undoui5/.savepoint/checkpoint.json", []byte("{}"))
	time.Sleep(50 * time.Millisecond)
	ops.WriteFile(ctx, "/undoui5/hello.md", []byte("---\ntitle: Hello\n---\nModified\n"))

	// Apply undo via .apply
	fsErr = ops.WriteFile(ctx, "/undoui5/.undo/to-savepoint/checkpoint/.apply", []byte(""))
	require.Nil(t, fsErr, "WriteFile .apply should succeed")

	// Verify content restored
	fc, fsErr := ops.ReadFile(ctx, "/undoui5/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "Original", "content should be restored after .apply")
}

func TestUndo_Interface_ApplyViaID(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui6")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui6", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui6/hello.md", []byte("---\ntitle: Hello\n---\nV1\n"))
	ops.WriteFile(ctx, "/undoui6/hello.md", []byte("---\ntitle: Hello\n---\nV2\n"))

	// Find the edit log entry
	entries, fsErr := ops.ReadDir(ctx, "/undoui6/.log/.by/type/edit")
	require.Nil(t, fsErr)
	var logIDs []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			logIDs = append(logIDs, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(logIDs), 1)

	// Apply single undo via .undo/id/<log_id>/.apply
	fsErr = ops.WriteFile(ctx, "/undoui6/.undo/id/"+logIDs[len(logIDs)-1]+"/.apply", []byte(""))
	require.Nil(t, fsErr, "WriteFile .apply via id should succeed")

	fc, fsErr := ops.ReadFile(ctx, "/undoui6/hello.md")
	require.Nil(t, fsErr)
	assert.Contains(t, string(fc.Data), "V1", "should be restored to V1")
}

func TestUndo_Interface_ApplyNoOp(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui7")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui7", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoui7/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	ops.WriteFile(ctx, "/undoui7/.savepoint/latest.json", []byte("{}"))

	// .apply on empty set should succeed (no-op)
	fsErr = ops.WriteFile(ctx, "/undoui7/.undo/to-savepoint/latest/.apply", []byte(""))
	require.Nil(t, fsErr, ".apply on empty set should be a no-op")
}

func TestUndo_Interface_InvalidTarget(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoui8")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoui8", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// .apply with invalid savepoint should fail
	fsErr = ops.WriteFile(ctx, "/undoui8/.undo/to-savepoint/nonexistent/.apply", []byte(""))
	require.NotNil(t, fsErr, ".apply with invalid savepoint should fail")
}

// --- Target validation tests (cd into nonexistent targets should fail) ---

func TestUndo_Interface_StatInvalidSavepoint(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoval1")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoval1", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Stat on nonexistent savepoint should return ENOENT
	_, fsErr = ops.Stat(ctx, "/undoval1/.undo/to-savepoint/nonexistent")
	require.NotNil(t, fsErr, "Stat on nonexistent savepoint should fail")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)
}

func TestUndo_Interface_StatInvalidLogID(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoval2")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoval2", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Stat on nonexistent log_id in id/ should return ENOENT
	_, fsErr = ops.Stat(ctx, "/undoval2/.undo/id/nonexistent")
	require.NotNil(t, fsErr, "Stat on nonexistent log_id should fail")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)
}

func TestUndo_Interface_StatInvalidToID(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoval3")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoval3", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Stat on nonexistent log_id in to-id/ should return ENOENT
	_, fsErr = ops.Stat(ctx, "/undoval3/.undo/to-id/nonexistent")
	require.NotNil(t, fsErr, "Stat on nonexistent to-id target should fail")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)
}

func TestUndo_Interface_StatValidSavepoint(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undoval4")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undoval4", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.WriteFile(ctx, "/undoval4/.savepoint/valid-sp.json", []byte("{}"))

	// Stat on existing savepoint should succeed
	entry, fsErr := ops.Stat(ctx, "/undoval4/.undo/to-savepoint/valid-sp")
	require.Nil(t, fsErr, "Stat on existing savepoint should succeed")
	assert.True(t, entry.IsDir)
	assert.Equal(t, "valid-sp", entry.Name)
}

// TestUndo_ToID_RestoresDeletedDirWithFile exercises the deferred-constraint
// path documented in ADR-017. Without SET CONSTRAINTS ALL DEFERRED in
// ExecuteUndoTransaction, restoring a child file row whose parent_id points
// to a directory row that hasn't been UPSERTed yet trips parent_id_fkey
// (SQLSTATE 23503) and aborts the undo with EIO. This was the failure
// reported by the stress test seed 1777065886566418000.
//
// To force the FK-ordering case, we create the file at root *before* the
// directory. Restore order in ExecuteUndoTransaction is by file_id ASC, so
// the older file_id (the file) is upserted first, with parent_id pointing to
// the not-yet-restored dir row. We use the production .apply path so cache
// invalidation matches the FUSE/NFS layer behavior. The undo target is read
// from .log/.last/.../.export/json so it's a raw UUID -- the format
// ExecuteUndo expects when called from writeUndoApply.
func TestUndo_ToID_RestoresDeletedDirWithFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "undofk")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/undofk", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// 1. Create x.md at root -- its file_id (UUIDv7) is the oldest.
	fsErr = ops.WriteFile(ctx, "/undofk/x.md", []byte("---\ntitle: X\n---\nbody x\n"))
	require.Nil(t, fsErr)

	// 2. Create dir A -- newer file_id.
	fsErr = ops.Mkdir(ctx, "/undofk/A")
	require.Nil(t, fsErr)

	// 3. Move x.md into A. After this, x.md.parent_id = A.id but its
	//    file_id stays unchanged (older than A's). When the undo restores
	//    rows in file_id ASC order, x.md is upserted before A -- forcing
	//    the FK to violate immediately without deferral.
	fsErr = ops.Rename(ctx, "/undofk/x.md", "/undofk/A/x.md")
	require.Nil(t, fsErr)

	// Target: the most recent log entry, which is the rename. After undoing
	// back to this point, both x.md (under A) and A should be restored.
	targetLogID := mostRecentLogIDRaw(t, ops, ctx, "/undofk")

	// 4. Delete file then dir (rmdir requires empty). We deliberately avoid
	//    calling Stat between delete and undo: writeUndoApply invalidates
	//    the stat cache under synth.TigerFSSchema while statSynthFile caches
	//    under the user's schema, so a sanity Stat here would leave a stale
	//    negative entry that survives the undo. (Unrelated to the FK fix.)
	require.Nil(t, ops.Delete(ctx, "/undofk/A/x.md"))
	require.Nil(t, ops.Delete(ctx, "/undofk/A"))

	// 5. Apply undo via the production .apply path. The path segment is
	//    the raw UUID (matches what writeUndoApply passes to ExecuteUndo).
	applyPath := "/undofk/.undo/to-id/" + targetLogID + "/.apply"
	fsErr = ops.WriteFile(ctx, applyPath, []byte(""))
	require.Nil(t, fsErr, "undo .apply must succeed with deferred constraints")

	// 6. Both the dir and the file inside should be back.
	fc, fsErr := ops.ReadFile(ctx, "/undofk/A/x.md")
	require.Nil(t, fsErr, "file x.md should be restored under A")
	assert.Contains(t, string(fc.Data), "body x")

	entries, fsErr := ops.ReadDir(ctx, "/undofk/A")
	require.Nil(t, fsErr, "directory A should be readable after undo")
	var names []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			names = append(names, e.Name)
		}
	}
	assert.Contains(t, names, "x.md", "A should contain x.md after undo")
}

// mostRecentLogIDRaw returns the raw UUID of the most recent log entry for
// the given app, read via .log/.last/1/.export/json. ReadDir on .log/ returns
// display names (timestamp-encoded), but the undo apply path expects a raw
// UUID, so we go through the JSON export to get the underlying value.
func mostRecentLogIDRaw(t *testing.T, ops *fs.Operations, ctx context.Context, appPath string) string {
	t.Helper()
	fc, fsErr := ops.ReadFile(ctx, appPath+"/.log/.last/1/.export/json")
	require.Nil(t, fsErr, "read latest log JSON")
	var entries []struct {
		LogID string `json:"log_id"`
	}
	require.NoError(t, json.Unmarshal(fc.Data, &entries))
	require.NotEmpty(t, entries, "log should have at least one entry")
	require.NotEmpty(t, entries[0].LogID, "log_id must not be empty")
	return entries[0].LogID
}
