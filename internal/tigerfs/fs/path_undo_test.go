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

// --- .by/.filter/ filename block on the log table ---
//
// The log's `filename` column stores denormalized full paths (ADR-017 § log
// schema); a single FUSE/NFS path segment cannot contain `/`, so
// .{by,filter}/filename/<val>/ would silently match only root-level files.
// Path parsing rejects both forms with a hint pointing at .by/file_id/.

func TestParsePath_Log_ByFilename_Rejected(t *testing.T) {
	// Listing form: .log/.by/filename
	_, err := ParsePath("/notes/.log/.by/filename")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Hint, "file_id")
}

func TestParsePath_Log_ByFilenameValue_Rejected(t *testing.T) {
	// Filter form: .log/.by/filename/<val>
	_, err := ParsePath("/notes/.log/.by/filename/hello.md")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Hint, "file_id")
}

func TestParsePath_Log_FilterFilename_Rejected(t *testing.T) {
	// Listing form: .log/.filter/filename
	_, err := ParsePath("/notes/.log/.filter/filename")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Hint, "file_id")
}

func TestParsePath_Log_FilterFilenameValue_Rejected(t *testing.T) {
	// Filter form: .log/.filter/filename/<val>
	_, err := ParsePath("/notes/.log/.filter/filename/hello.md")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Hint, "file_id")
}

func TestParsePath_Log_ByFileID_StillWorks(t *testing.T) {
	// .by/file_id/<uuid>/ is the documented alternative; must continue to work.
	result, err := ParsePath("/notes/.log/.by/file_id/019e6ab6-ed60-7a46-90bf-ce17fa441cf8/.last/5")
	require.Nil(t, err)
	assert.Equal(t, "notes_log", result.Context.TableName)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "file_id", result.Context.Filters[0].Column)
	assert.Equal(t, "019e6ab6-ed60-7a46-90bf-ce17fa441cf8", result.Context.Filters[0].Value)
	assert.Equal(t, LimitLast, result.Context.LimitType)
	assert.Equal(t, 5, result.Context.Limit)
}

func TestParsePath_NonLog_ByFilename_NotAffected(t *testing.T) {
	// On a non-log table (e.g., the workspace backing table), .by/filename/
	// must still be accepted by the path parser. The backing table's
	// `filename` is leaf-only (ADR-017:43); no slash hazard.
	result, err := ParsePath("/notes/.by/filename/hello.md")
	require.Nil(t, err)
	require.Len(t, result.Context.Filters, 1)
	assert.Equal(t, "filename", result.Context.Filters[0].Column)
	assert.Equal(t, "hello.md", result.Context.Filters[0].Value)
}

func TestParsePath_History_ByUUID_NotAffected(t *testing.T) {
	// .history/.by/<uuid>/ is special-cased in processHistory and takes a UUID,
	// not a column name. Confirm it isn't accidentally caught by the log block.
	result, err := ParsePath("/notes/.history/.by/019e6ab6-ed60-7a46-90bf-ce17fa441cf8")
	require.Nil(t, err)
	assert.Equal(t, PathHistory, result.Type)
	assert.True(t, result.HistoryByID)
	assert.Equal(t, "019e6ab6-ed60-7a46-90bf-ce17fa441cf8", result.HistoryRowID)
}

// --- .history/.id and bare-version rejection (Bug C) ---
//
// Without a preceding filename, .id and version IDs aren't meaningful. The
// parser used to silently set HistoryVersionID with HistoryFile = "", which
// the readdir dispatcher then ignored, making the paths act as undocumented
// aliases for .history/. Reject them explicitly with a hint pointing at the
// supported forms.

func TestParsePath_History_BareDotID_Rejected(t *testing.T) {
	_, err := ParsePath("/notes/.history/.id")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Message, ".id")
	assert.Contains(t, err.Hint, "<filename>")
	assert.Contains(t, err.Hint, ".by/")
}

func TestParsePath_History_BareVersionID_Rejected(t *testing.T) {
	// A version-shaped string with no filename prefix is the same parser
	// quirk: the greedy filename loop ate it as a HistoryVersionID and left
	// HistoryFile empty. Reject for the same reason as .history/.id.
	_, err := ParsePath("/notes/.history/2026-04-08T143012.001Z-g7h8i9j0k1l2a")
	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Hint, "<filename>")
}

func TestParsePath_History_FilenameDotID_StillWorks(t *testing.T) {
	// .history/<filename>/.id is the legitimate UUID lookup -- must still parse.
	result, err := ParsePath("/notes/.history/foo.md/.id")
	require.Nil(t, err)
	assert.Equal(t, PathHistory, result.Type)
	assert.Equal(t, "foo.md", result.HistoryFile)
	assert.Equal(t, FileID, result.HistoryVersionID)
}

func TestParsePath_History_FilenameVersion_StillWorks(t *testing.T) {
	// .history/<filename>/<version> is the legitimate version read -- must still parse.
	result, err := ParsePath("/notes/.history/foo.md/2026-04-08T143012.001Z-g7h8i9j0k1l2a")
	require.Nil(t, err)
	assert.Equal(t, PathHistory, result.Type)
	assert.Equal(t, "foo.md", result.HistoryFile)
	assert.Equal(t, "2026-04-08T143012.001Z-g7h8i9j0k1l2a", result.HistoryVersionID)
}

func TestBlockLogFilenameQuery_PredicateMatrix(t *testing.T) {
	cases := []struct {
		name      string
		ctx       *FSContext
		column    string
		wantBlock bool
	}{
		{"log table + filename", &FSContext{Schema: "tigerfs", TableName: "notes_log"}, "filename", true},
		{"log table + other col", &FSContext{Schema: "tigerfs", TableName: "notes_log"}, "user_id", false},
		{"history table + filename", &FSContext{Schema: "tigerfs", TableName: "notes_history"}, "filename", false},
		{"savepoint + filename", &FSContext{Schema: "tigerfs", TableName: "notes_savepoint"}, "filename", false},
		{"public schema + filename", &FSContext{Schema: "public", TableName: "notes_log"}, "filename", false},
		{"workspace backing + filename", &FSContext{Schema: "public", TableName: "notes"}, "filename", false},
		{"nil context", nil, "filename", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := blockLogFilenameQuery(tc.ctx, tc.column)
			if tc.wantBlock {
				require.NotNil(t, err, "expected block")
				assert.Equal(t, ErrInvalidPath, err.Code)
				assert.Contains(t, err.Hint, "file_id")
			} else {
				require.Nil(t, err, "expected pass-through")
			}
		})
	}
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
