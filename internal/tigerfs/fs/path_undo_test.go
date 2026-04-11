package fs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- .log/ path parsing ---

func TestParsePath_Log_Listing(t *testing.T) {
	result, err := ParsePath("/notes/.log")
	require.Nil(t, err)
	assert.Equal(t, PathLog, result.Type)
	assert.Equal(t, "tigerfs", result.Context.Schema)
	assert.Equal(t, "notes_log", result.Context.TableName)
	assert.Equal(t, "notes", result.OrigTableName)
}

func TestParsePath_Log_Pipeline(t *testing.T) {
	result, err := ParsePath("/notes/.log/.last/10")
	require.Nil(t, err)
	assert.Equal(t, "notes_log", result.Context.TableName)
	assert.Equal(t, LimitLast, result.Context.LimitType)
	assert.Equal(t, 10, result.Context.Limit)
}

func TestParsePath_Log_ByUserID(t *testing.T) {
	result, err := ParsePath("/notes/.log/.by/user_id/agent-7")
	require.Nil(t, err)
	assert.Equal(t, "notes_log", result.Context.TableName)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "user_id", result.Context.Filters[0].Column)
	assert.Equal(t, "agent-7", result.Context.Filters[0].Value)
}

func TestParsePath_Log_Entry(t *testing.T) {
	result, err := ParsePath("/notes/.log/2026-04-08T143012.001Z-g7h8i9j0k1l2a")
	require.Nil(t, err)
	assert.Equal(t, PathRow, result.Type)
	assert.Equal(t, "notes_log", result.Context.TableName)
	assert.Equal(t, "2026-04-08T143012.001Z-g7h8i9j0k1l2a", result.PrimaryKey)
}

func TestParsePath_Log_EntryColumn(t *testing.T) {
	result, err := ParsePath("/notes/.log/2026-04-08T143012.001Z-g7h8i9j0k1l2a/type")
	require.Nil(t, err)
	assert.Equal(t, PathColumn, result.Type)
	assert.Equal(t, "notes_log", result.Context.TableName)
	assert.Equal(t, "type", result.Column)
}

func TestParsePath_Log_Export(t *testing.T) {
	result, err := ParsePath("/notes/.log/.export/json")
	require.Nil(t, err)
	assert.Equal(t, "notes_log", result.Context.TableName)
	assert.Equal(t, PathExport, result.Type)
}

func TestParsePath_Log_ByUserExport(t *testing.T) {
	result, err := ParsePath("/notes/.log/.by/user_id/agent-7/.export/json")
	require.Nil(t, err)
	assert.Equal(t, "notes_log", result.Context.TableName)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "agent-7", result.Context.Filters[0].Value)
	assert.Equal(t, PathExport, result.Type)
}

// --- .savepoint/ path parsing ---

func TestParsePath_Savepoint_Listing(t *testing.T) {
	result, err := ParsePath("/notes/.savepoint")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, result.Type)
	assert.Equal(t, "tigerfs", result.Context.Schema)
	assert.Equal(t, "notes_savepoint", result.Context.TableName)
	assert.Equal(t, "notes", result.OrigTableName)
}

func TestParsePath_Savepoint_Entry(t *testing.T) {
	result, err := ParsePath("/notes/.savepoint/before-exploration")
	require.Nil(t, err)
	assert.Equal(t, PathRow, result.Type)
	assert.Equal(t, "notes_savepoint", result.Context.TableName)
	assert.Equal(t, "before-exploration", result.PrimaryKey)
}

func TestParsePath_Savepoint_EntryColumn(t *testing.T) {
	result, err := ParsePath("/notes/.savepoint/before-exploration/description")
	require.Nil(t, err)
	assert.Equal(t, PathColumn, result.Type)
	assert.Equal(t, "notes_savepoint", result.Context.TableName)
	assert.Equal(t, "description", result.Column)
}

func TestParsePath_Savepoint_Pipeline(t *testing.T) {
	result, err := ParsePath("/notes/.savepoint/.by/user_id/agent-7/.last/5")
	require.Nil(t, err)
	assert.Equal(t, "notes_savepoint", result.Context.TableName)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "agent-7", result.Context.Filters[0].Value)
	assert.Equal(t, LimitLast, result.Context.LimitType)
	assert.Equal(t, 5, result.Context.Limit)
}

// --- .undo/ path parsing ---

func TestParsePath_Undo_Root(t *testing.T) {
	result, err := ParsePath("/notes/.undo")
	require.Nil(t, err)
	assert.Equal(t, PathUndo, result.Type)
	assert.Empty(t, result.UndoMode)
	assert.Equal(t, "notes", result.OrigTableName)
}

func TestParsePath_Undo_ModeID(t *testing.T) {
	result, err := ParsePath("/notes/.undo/id")
	require.Nil(t, err)
	assert.Equal(t, PathUndo, result.Type)
	assert.Equal(t, "id", result.UndoMode)
	assert.Empty(t, result.UndoTarget)
}

func TestParsePath_Undo_ModeToID(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-id")
	require.Nil(t, err)
	assert.Equal(t, "to-id", result.UndoMode)
}

func TestParsePath_Undo_ModeToSavepoint(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
}

func TestParsePath_Undo_InvalidMode(t *testing.T) {
	_, err := ParsePath("/notes/.undo/badmode")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
}

func TestParsePath_Undo_IDTarget(t *testing.T) {
	result, err := ParsePath("/notes/.undo/id/2026-04-08T143015.234Z-i9j0k1l2m3n4b")
	require.Nil(t, err)
	assert.Equal(t, "id", result.UndoMode)
	assert.Equal(t, "2026-04-08T143015.234Z-i9j0k1l2m3n4b", result.UndoTarget)
	assert.False(t, result.UndoApply)
}

func TestParsePath_Undo_IDApply(t *testing.T) {
	result, err := ParsePath("/notes/.undo/id/2026-04-08T143015.234Z-i9j0k1l2m3n4b/.apply")
	require.Nil(t, err)
	assert.Equal(t, "id", result.UndoMode)
	assert.Equal(t, "2026-04-08T143015.234Z-i9j0k1l2m3n4b", result.UndoTarget)
	assert.True(t, result.UndoApply)
}

func TestParsePath_Undo_IDInfoSummary(t *testing.T) {
	result, err := ParsePath("/notes/.undo/id/2026-04-08T143015.234Z-i9j0k1l2m3n4b/.info/summary")
	require.Nil(t, err)
	assert.Equal(t, "id", result.UndoMode)
	assert.Equal(t, "2026-04-08T143015.234Z-i9j0k1l2m3n4b", result.UndoTarget)
	assert.Equal(t, "summary", result.InfoFile)
	assert.False(t, result.UndoApply)
}

func TestParsePath_Undo_ToSavepointTarget(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-exploration")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-exploration", result.UndoTarget)
}

func TestParsePath_Undo_ToSavepointApply(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-exploration/.apply")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-exploration", result.UndoTarget)
	assert.True(t, result.UndoApply)
}

func TestParsePath_Undo_ToSavepointByUserApply(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-exploration/.by/user_id/agent-7/.apply")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-exploration", result.UndoTarget)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "user_id", result.Context.Filters[0].Column)
	assert.Equal(t, "agent-7", result.Context.Filters[0].Value)
	assert.True(t, result.UndoApply)
}

func TestParsePath_Undo_ToSavepointByUserInfoSummary(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-exploration/.by/user_id/agent-7/.info/summary")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-exploration", result.UndoTarget)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "agent-7", result.Context.Filters[0].Value)
	assert.Equal(t, "summary", result.InfoFile)
	assert.False(t, result.UndoApply)
}

func TestParsePath_Undo_FilterTypeDelete(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-refactor/.filter/type/delete/.apply")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-refactor", result.UndoTarget)
	assert.True(t, result.UndoApply)
}

func TestParsePath_Undo_Last5InfoSummary(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-refactor/.last/5/.info/summary")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-refactor", result.UndoTarget)
	assert.Equal(t, "summary", result.InfoFile)
}

func TestParsePath_Undo_PreviewFile(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-savepoint/before-exploration/docs/hello.md")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-exploration", result.UndoTarget)
	assert.Equal(t, "docs/hello.md", result.UndoFile)
}

func TestParsePath_Undo_ToIDTarget(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-id/2026-04-08T143015.234Z-abc123")
	require.Nil(t, err)
	assert.Equal(t, "to-id", result.UndoMode)
	assert.Equal(t, "2026-04-08T143015.234Z-abc123", result.UndoTarget)
}

func TestParsePath_Undo_ToIDApply(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-id/2026-04-08T143015.234Z-abc123/.apply")
	require.Nil(t, err)
	assert.Equal(t, "to-id", result.UndoMode)
	assert.True(t, result.UndoApply)
}

func TestParsePath_Undo_ToIDByUserApply(t *testing.T) {
	result, err := ParsePath("/notes/.undo/to-id/2026-04-08T143015.234Z-abc123/.by/user_id/agent-7/.apply")
	require.Nil(t, err)
	assert.Equal(t, "to-id", result.UndoMode)
	assert.Equal(t, "2026-04-08T143015.234Z-abc123", result.UndoTarget)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "agent-7", result.Context.Filters[0].Value)
	assert.True(t, result.UndoApply)
}

func TestParsePath_Undo_SampleParses(t *testing.T) {
	// .sample/ should parse correctly (pipeline works) but .apply should NOT
	// be available under .sample/ -- that's a handler concern, not parsing.
	result, err := ParsePath("/notes/.undo/to-savepoint/before-refactor/.sample/5")
	require.Nil(t, err)
	assert.Equal(t, "to-savepoint", result.UndoMode)
	assert.Equal(t, "before-refactor", result.UndoTarget)
	assert.Equal(t, LimitSample, result.Context.LimitType)
	assert.Equal(t, 5, result.Context.Limit)
	assert.False(t, result.UndoApply)
}

// --- .log/ diff symlink column parsing ---

func TestParsePath_Log_EntryBefore(t *testing.T) {
	result, err := ParsePath("/notes/.log/2026-04-08T143012.001Z-g7h8i9j0k1l2a/before")
	require.Nil(t, err)
	assert.Equal(t, PathColumn, result.Type)
	assert.Equal(t, "notes_log", result.Context.TableName)
	assert.Equal(t, "2026-04-08T143012.001Z-g7h8i9j0k1l2a", result.PrimaryKey)
	assert.Equal(t, "before", result.Column)
}

func TestParsePath_Log_EntryAfter(t *testing.T) {
	result, err := ParsePath("/notes/.log/2026-04-08T143012.001Z-g7h8i9j0k1l2a/after")
	require.Nil(t, err)
	assert.Equal(t, PathColumn, result.Type)
	assert.Equal(t, "after", result.Column)
}

func TestParsePath_Log_EntryCurrent(t *testing.T) {
	result, err := ParsePath("/notes/.log/2026-04-08T143012.001Z-g7h8i9j0k1l2a/current")
	require.Nil(t, err)
	assert.Equal(t, PathColumn, result.Type)
	assert.Equal(t, "current", result.Column)
}
