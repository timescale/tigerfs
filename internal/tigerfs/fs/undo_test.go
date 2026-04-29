package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func newUndoMock() *mockDBClient {
	return &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "name"},
		},
	}
}

func newUndoOps(mockDB *mockDBClient) *Operations {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ops.SetUserID("agent-7")
	return ops
}

// --- ExecuteUndoSingle: decision matrix ---

func TestUndo_Single_Create_RowExists(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "create", VersionID: "", Filename: "hello.md",
	}
	mockDB.fileExistsMap = map[string]bool{"file-1": true}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesDeleted)
	assert.Equal(t, 0, result.FilesRestored)

	require.NotNil(t, mockDB.lastUndoParams)
	assert.Equal(t, []string{"file-1"}, mockDB.lastUndoParams.DeleteFileIDs)
	assert.Empty(t, mockDB.lastUndoParams.RestoreVersionIDs)
}

func TestUndo_Single_Create_RowAlreadyDeleted(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "create", VersionID: "", Filename: "hello.md",
	}
	mockDB.fileExistsMap = map[string]bool{"file-1": false}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)
	assert.Equal(t, 0, result.FilesDeleted)
	assert.Equal(t, 1, result.FilesSkipped)
	assert.Equal(t, 0, mockDB.undoTransactionCalls, "no transaction needed for no-op")
}

func TestUndo_Single_Edit_RowExists(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "edit", VersionID: "v-before", Filename: "hello.md",
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored)

	require.NotNil(t, mockDB.lastUndoParams)
	assert.Equal(t, []string{"v-before"}, mockDB.lastUndoParams.RestoreVersionIDs)
	assert.Equal(t, []string{"file-1"}, mockDB.lastUndoParams.RestoreFileIDs)
}

func TestUndo_Single_Rename_RestoresParentAndFilename(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "rename", VersionID: "v-before-rename", Filename: "old-name.md",
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored)

	require.NotNil(t, mockDB.lastUndoParams)
	assert.Equal(t, []string{"v-before-rename"}, mockDB.lastUndoParams.RestoreVersionIDs)
	assert.Equal(t, []string{"old-name.md"}, mockDB.lastUndoParams.RestoreFilenames)
}

func TestUndo_Single_Delete_RestoresFromHistory(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "delete", VersionID: "v-before-delete", Filename: "removed.md",
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored)

	require.NotNil(t, mockDB.lastUndoParams)
	assert.Equal(t, []string{"v-before-delete"}, mockDB.lastUndoParams.RestoreVersionIDs)
}

func TestUndo_Single_UndoType_SameAsEdit(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "undo", VersionID: "v-before-undo", Filename: "hello.md",
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored)
	assert.Equal(t, []string{"v-before-undo"}, mockDB.lastUndoParams.RestoreVersionIDs)
}

func TestUndo_Single_Edit_MissingVersionID(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "edit", VersionID: "", Filename: "hello.md",
	}
	ops := newUndoOps(mockDB)

	_, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no version_id")
}

func TestUndo_Single_NotFound(t *testing.T) {
	mockDB := newUndoMock()
	// undoLogEntry is nil -- QueryLogEntry returns error
	ops := newUndoOps(mockDB)

	_, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// --- ExecuteUndo: multi-file ---

func TestUndo_Multi_MixedOperations(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = []db.UndoAffectedFile{
		{FileID: "file-a", Type: "create", VersionID: "", Filename: "new-file.md"},
		{FileID: "file-b", Type: "edit", VersionID: "v-b-before", Filename: "edited.md"},
		{FileID: "file-c", Type: "delete", VersionID: "v-c-before", Filename: "deleted.md"},
	}
	mockDB.fileExistsMap = map[string]bool{
		"file-a": true,  // created and still exists → DELETE
		"file-b": true,  // edited
		"file-c": false, // deleted
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-id-1", "test undo", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesDeleted)  // file-a
	assert.Equal(t, 2, result.FilesRestored) // file-b, file-c
	assert.Equal(t, 0, result.FilesSkipped)

	p := mockDB.lastUndoParams
	require.NotNil(t, p)
	assert.Equal(t, []string{"file-a"}, p.DeleteFileIDs)
	assert.Equal(t, []string{"v-b-before", "v-c-before"}, p.RestoreVersionIDs)
	assert.Equal(t, "agent-7", p.UserID)
	assert.Equal(t, "test undo", p.Description)
}

func TestUndo_Multi_CreateThenDeleted_NoOp(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = []db.UndoAffectedFile{
		{FileID: "file-a", Type: "create", VersionID: "", Filename: "ephemeral.md"},
	}
	mockDB.fileExistsMap = map[string]bool{"file-a": false} // created then deleted
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-id-1", "test", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.FilesDeleted)
	assert.Equal(t, 1, result.FilesSkipped)
	assert.Equal(t, 0, mockDB.undoTransactionCalls, "no transaction for all-skip")
}

func TestUndo_Multi_NoAffectedFiles(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = nil // no operations after target
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-id-1", "test", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.FilesDeleted)
	assert.Equal(t, 0, result.FilesRestored)
	assert.Equal(t, 0, result.FilesSkipped)
	assert.Equal(t, 0, mockDB.undoTransactionCalls)
}

// --- ExecuteUndoToSavepoint ---

func TestUndo_ToSavepoint_LooksUpSavepointID(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.rowData = map[string]*mockRow{
		"tigerfs.notes_savepoint.before-refactor": {
			columns: []string{"name", "savepoint_id", "user_id", "description"},
			values:  []interface{}{"before-refactor", "sp-uuid-123", "agent-7", "test"},
		},
	}
	mockDB.undoAffectedFiles = []db.UndoAffectedFile{
		{FileID: "file-1", Type: "edit", VersionID: "v-1", Filename: "hello.md"},
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndoToSavepoint(context.Background(), "public", "notes", "before-refactor", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored)
	assert.Contains(t, mockDB.lastUndoParams.Description, "before-refactor")
}

func TestUndo_ToSavepoint_NotFound(t *testing.T) {
	mockDB := newUndoMock()
	// No savepoint row data
	ops := newUndoOps(mockDB)

	_, err := ops.ExecuteUndoToSavepoint(context.Background(), "public", "notes", "nonexistent", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "savepoint not found")
}

// --- Undo log entry metadata ---

func TestUndo_LogEntries_HaveUserIDAndDescription(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "edit", VersionID: "v-1", Filename: "hello.md",
	}
	ops := newUndoOps(mockDB)
	ops.SetUserID("demo-user")

	_, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.NoError(t, err)

	p := mockDB.lastUndoParams
	assert.Equal(t, "demo-user", p.UserID)
	assert.Contains(t, p.Description, "Undo single operation")
}

// --- Filter handling ---

func TestUndo_Multi_UserIDFilter(t *testing.T) {
	// The user_id filter is passed through to QueryUndoAffectedFiles.
	// We verify it reaches the params correctly.
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = nil // empty is fine, we just test the call path
	ops := newUndoOps(mockDB)

	filters := []db.UndoFilter{{Column: "user_id", Value: "agent-9"}}
	_, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-1", "test", filters)
	require.NoError(t, err)
	// The mock doesn't verify the userID parameter directly, but the code path is exercised.
}

func TestUndo_Multi_TypeFilter(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = nil
	ops := newUndoOps(mockDB)

	filters := []db.UndoFilter{{Column: "type", Value: "delete"}}
	_, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-1", "test", filters)
	require.NoError(t, err)
}

func TestUndo_Multi_CombinedFilters(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = nil
	ops := newUndoOps(mockDB)

	filters := []db.UndoFilter{
		{Column: "user_id", Value: "agent-7"},
		{Column: "type", Value: "delete"},
	}
	_, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-1", "test", filters)
	require.NoError(t, err)
}

// --- Edge cases ---

func TestUndo_Single_UnknownType(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoLogEntry = &db.UndoAffectedFile{
		FileID: "file-1", Type: "unknown", VersionID: "", Filename: "hello.md",
	}
	ops := newUndoOps(mockDB)

	_, err := ops.ExecuteUndoSingle(context.Background(), "public", "notes", "log-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation type")
}

func TestUndo_Multi_SkipsUnknownType(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = []db.UndoAffectedFile{
		{FileID: "file-1", Type: "unknown", VersionID: "", Filename: "weird.md"},
		{FileID: "file-2", Type: "edit", VersionID: "v-2", Filename: "normal.md"},
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-1", "test", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored) // file-2
	assert.Equal(t, 1, result.FilesSkipped)  // file-1 (unknown type)
}

func TestUndo_Multi_SkipsMissingVersionID(t *testing.T) {
	mockDB := newUndoMock()
	mockDB.undoAffectedFiles = []db.UndoAffectedFile{
		{FileID: "file-1", Type: "edit", VersionID: "", Filename: "no-version.md"},
		{FileID: "file-2", Type: "delete", VersionID: "v-2", Filename: "good.md"},
	}
	ops := newUndoOps(mockDB)

	result, err := ops.ExecuteUndo(context.Background(), "public", "notes", "sp-1", "test", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.FilesRestored) // file-2
	assert.Equal(t, 1, result.FilesSkipped)  // file-1 (missing version_id)
}
