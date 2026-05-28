package fs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

// --- synthUndoDirs tests ---

func TestSynthUndoDirs_WithHistory(t *testing.T) {
	info := &synth.ViewInfo{HasHistory: true, CachedMountTime: time.Now()}
	dirs := synthUndoDirs(info)
	require.Len(t, dirs, 4)
	assert.Equal(t, ".history", dirs[0].Name)
	assert.Equal(t, ".log", dirs[1].Name)
	assert.Equal(t, ".savepoint", dirs[2].Name)
	assert.Equal(t, ".undo", dirs[3].Name)
	for _, d := range dirs {
		assert.True(t, d.IsDir)
	}
}

func TestSynthUndoDirs_WithoutHistory(t *testing.T) {
	info := &synth.ViewInfo{HasHistory: false}
	dirs := synthUndoDirs(info)
	assert.Nil(t, dirs)
}

// --- uuidToDisplayName tests ---

func TestUuidToDisplayName_ValidV7Hex(t *testing.T) {
	cfg := &config.Config{}
	ops := NewOperations(cfg, &mockDBClient{})
	// Use a known UUIDv7 hex -- we just need to verify it converts
	// The format package handles the actual conversion
	result := ops.uuidToDisplayName("019d7db2-237b-77ab-949a-afe464991e0e")
	assert.Contains(t, result, "2026-", "should convert to display name with timestamp")
	assert.Contains(t, result, "Z-", "should have Z- separator between timestamp and entropy")
}

func TestUuidToDisplayName_AlreadyDisplayName(t *testing.T) {
	cfg := &config.Config{}
	ops := NewOperations(cfg, &mockDBClient{})
	result := ops.uuidToDisplayName("2026-04-07T143000.123Z-zzz0063hd8e5r42")
	assert.Equal(t, "2026-04-07T143000.123Z-zzz0063hd8e5r42", result, "should pass through display names")
}

func TestUuidToDisplayName_NonUUID(t *testing.T) {
	cfg := &config.Config{}
	ops := NewOperations(cfg, &mockDBClient{})
	result := ops.uuidToDisplayName("not-a-uuid")
	assert.Equal(t, "not-a-uuid", result, "should pass through non-UUIDs")
}

// --- parseUUIDBytes tests ---

func TestParseUUIDBytes_Valid(t *testing.T) {
	b, err := parseUUIDBytes("019d7db2-237b-77ab-949a-afe464991e0e")
	require.NoError(t, err)
	assert.Len(t, b, 16)
}

func TestParseUUIDBytes_InvalidLength(t *testing.T) {
	_, err := parseUUIDBytes("too-short")
	assert.Error(t, err)
}

func TestParseUUIDBytes_NoDashes(t *testing.T) {
	b, err := parseUUIDBytes("019d7db2237b77ab949aafe464991e0e")
	require.NoError(t, err)
	assert.Len(t, b, 16)
}

// --- resolveLogDiffSymlink full state matrix (mock-based) ---

// Helper to create a mock with a log entry row and configurable next/exists results.
func setupLogDiffMock(versionID, fileID, filename, logID string) *mockDBClient {
	mock := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.test_log": {column: "log_id"},
		},
		rowData: map[string]*mockRow{
			"tigerfs.test_log." + logID: {
				columns: []string{"log_id", "file_id", "type", "user_id", "filename", "version_id", "description"},
				values:  []interface{}{logID, fileID, "edit", nil, filename, nilIfEmpty(versionID), nil},
			},
		},
	}
	return mock
}

func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func TestResolveLogDiffSymlink_Before_NullVersionID(t *testing.T) {
	mock := setupLogDiffMock("", "file-1", "hello.md", "log-1")
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "before",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "/dev/null", target)
}

func TestResolveLogDiffSymlink_Before_WithVersionID(t *testing.T) {
	mock := setupLogDiffMock("019d7db2-237b-77ab-949a-afe464991e0e", "file-1", "hello.md", "log-1")
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "before",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	// file_id-keyed history path: rename-invariant, doesn't depend on the
	// log row's filename column.
	assert.Contains(t, target, "../../.history/.by/file-1/")
	assert.NotEqual(t, "/dev/null", target)
}

func TestResolveLogDiffSymlink_After_NextEntryWithVersionID(t *testing.T) {
	mock := setupLogDiffMock("v-1", "file-1", "hello.md", "log-1")
	mock.nextLogVersionID = "019d7db2-337b-77ab-949a-afe464991e0e"
	mock.nextLogFilename = "hello.md"
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "after",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Contains(t, target, "../../.history/.by/file-1/")
}

func TestResolveLogDiffSymlink_After_NextEntryNullVersionID(t *testing.T) {
	mock := setupLogDiffMock("v-1", "file-1", "hello.md", "log-1")
	mock.nextLogVersionID = "" // NULL -- next op was a create
	mock.nextLogFilename = "hello.md"
	mock.currentPath = "hello.md" // next-create's file lives at this path now
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "after",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "../../hello.md", target)
}

func TestResolveLogDiffSymlink_After_NoNextEntry_FileExists(t *testing.T) {
	mock := setupLogDiffMock("v-1", "file-1", "hello.md", "log-1")
	// No next entry (defaults empty); file lives at its current path.
	mock.currentPath = "hello.md"
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "after",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "../../hello.md", target)
}

func TestResolveLogDiffSymlink_After_NoNextEntry_FileDeleted(t *testing.T) {
	mock := setupLogDiffMock("v-1", "file-1", "hello.md", "log-1")
	// currentPath defaults to "" -> file no longer exists.
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "after",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "/dev/null", target)
}

func TestResolveLogDiffSymlink_Current_FileExists(t *testing.T) {
	mock := setupLogDiffMock("v-1", "file-1", "hello.md", "log-1")
	mock.currentPath = "hello.md"
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "current",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "../../hello.md", target)
}

func TestResolveLogDiffSymlink_Current_FileDeleted(t *testing.T) {
	mock := setupLogDiffMock("v-1", "file-1", "hello.md", "log-1")
	// currentPath defaults to "" -> file no longer exists.
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "current",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "/dev/null", target)
}

func TestResolveLogDiffSymlink_Current_RenameInvariant(t *testing.T) {
	// Log entry recorded "old-name.md" at log-write time; file has since been
	// renamed/moved. The symlink should point at the file's current path,
	// not the stale stored filename.
	mock := setupLogDiffMock("v-1", "file-1", "old-name.md", "log-1")
	mock.currentPath = "archive/new-name.md"
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-1",
		Column:        "current",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.Equal(t, "../../archive/new-name.md", target)
}

func TestResolveLogDiffSymlink_Before_RenameLogEntry_UsesFileID(t *testing.T) {
	// A rename log entry stores the NEW filename; the history row's filename
	// is the OLD name. The file_id-keyed history path sidesteps the namespace
	// mismatch -- doesn't depend on either filename.
	mock := setupLogDiffMock("019d7db2-237b-77ab-949a-afe464991e0e", "file-1", "new-name.md", "log-rename")
	cfg := &config.Config{}
	ops := NewOperations(cfg, mock)

	parsed := &ParsedPath{
		Type:          PathColumn,
		Context:       &FSContext{Schema: "tigerfs", TableName: "test_log"},
		PrimaryKey:    "log-rename",
		Column:        "before",
		OrigTableName: "test",
	}

	target, fsErr := ops.resolveLogDiffSymlink(context.Background(), parsed)
	require.Nil(t, fsErr)
	// Must use file_id (file-1), not the stored filename (new-name.md or old).
	assert.Contains(t, target, "../../.history/.by/file-1/")
	assert.NotContains(t, target, "new-name.md")
	assert.NotContains(t, target, "old-name.md")
}
