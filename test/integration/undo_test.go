package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
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
