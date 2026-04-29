package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// --- Path parsing tests ---

func TestParsePath_RootInfo(t *testing.T) {
	result, err := ParsePath("/.info")
	require.Nil(t, err)
	assert.Equal(t, PathRootInfo, result.Type)
	assert.Empty(t, result.InfoFile)
}

func TestParsePath_RootInfoUser(t *testing.T) {
	result, err := ParsePath("/.info/user")
	require.Nil(t, err)
	assert.Equal(t, PathRootInfo, result.Type)
	assert.Equal(t, "user", result.InfoFile)
}

func TestParsePath_RootInfoUnknown(t *testing.T) {
	result, err := ParsePath("/.info/nonexistent")
	require.Nil(t, err)
	assert.Equal(t, PathRootInfo, result.Type)
	assert.Equal(t, "nonexistent", result.InfoFile)
}

// --- Operations user identity tests ---

func TestOperations_UserID_DefaultEmpty(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	assert.Empty(t, ops.GetUserID())
}

func TestOperations_UserID_SetGet(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	ops.SetUserID("agent-7")
	assert.Equal(t, "agent-7", ops.GetUserID())
}

func TestOperations_UserID_FromConfig(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000, UserID: "config-user"}
	ops := NewOperations(cfg, &mockDBClient{})
	ops.SetUserID(cfg.UserID)
	assert.Equal(t, "config-user", ops.GetUserID())
}

// --- .info/user ReadDir/Stat/ReadFile/WriteFile tests ---

func TestRootInfo_ReadDir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	ops.SetUserID("test-user")

	entries, fsErr := ops.ReadDir(context.Background(), "/.info")
	require.Nil(t, fsErr)
	require.Len(t, entries, 1)
	assert.Equal(t, "user", entries[0].Name)
	assert.False(t, entries[0].IsDir)
}

func TestRootInfo_StatDir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	entry, fsErr := ops.Stat(context.Background(), "/.info")
	require.Nil(t, fsErr)
	assert.Equal(t, ".info", entry.Name)
	assert.True(t, entry.IsDir)
}

func TestRootInfo_StatUser(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	ops.SetUserID("agent-7")

	entry, fsErr := ops.Stat(context.Background(), "/.info/user")
	require.Nil(t, fsErr)
	assert.Equal(t, "user", entry.Name)
	assert.False(t, entry.IsDir)
	assert.Equal(t, int64(len("agent-7\n")), entry.Size)
}

func TestRootInfo_StatUnknown(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	_, fsErr := ops.Stat(context.Background(), "/.info/nonexistent")
	require.NotNil(t, fsErr)
	assert.Equal(t, ErrNotExist, fsErr.Code)
}

func TestRootInfo_ReadUser(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	ops.SetUserID("agent-7")

	content, fsErr := ops.ReadFile(context.Background(), "/.info/user")
	require.Nil(t, fsErr)
	assert.Equal(t, "agent-7\n", string(content.Data))
}

func TestRootInfo_ReadUserEmpty(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	content, fsErr := ops.ReadFile(context.Background(), "/.info/user")
	require.Nil(t, fsErr)
	assert.Equal(t, "\n", string(content.Data))
}

func TestRootInfo_WriteUser(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	fsErr := ops.WriteFile(context.Background(), "/.info/user", []byte("agent-9\n"))
	require.Nil(t, fsErr)
	assert.Equal(t, "agent-9", ops.GetUserID())
}

func TestRootInfo_WriteUserTrimsWhitespace(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	fsErr := ops.WriteFile(context.Background(), "/.info/user", []byte("  agent-9  \n"))
	require.Nil(t, fsErr)
	assert.Equal(t, "agent-9", ops.GetUserID())
}

func TestRootInfo_WriteUserClear(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	ops.SetUserID("agent-7")

	fsErr := ops.WriteFile(context.Background(), "/.info/user", []byte("\n"))
	require.Nil(t, fsErr)
	assert.Empty(t, ops.GetUserID(), "writing empty/newline should clear user ID")
}

// --- Log entry user_id wiring tests ---

func TestSynth_LogEntry_IncludesUserID(t *testing.T) {
	mockDB := newParentPointerMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ops.SetUserID("agent-7")

	content := "---\ntitle: Hello\n---\n# Hello\n"
	fsErr := ops.WriteFile(context.Background(), "/notes/hello.md", []byte(content))
	require.Nil(t, fsErr)

	require.Len(t, mockDB.logEntries, 1)
	assert.Equal(t, "create", mockDB.logEntries[0].opType)
	assert.Equal(t, "agent-7", mockDB.logEntries[0].userID,
		"log entry should include the mount-level user ID")
}

func TestSynth_LogEntry_AnonymousWhenNoUserID(t *testing.T) {
	mockDB := newParentPointerMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	// No SetUserID -- anonymous

	content := "---\ntitle: Hello\n---\n# Hello\n"
	fsErr := ops.WriteFile(context.Background(), "/notes/hello.md", []byte(content))
	require.Nil(t, fsErr)

	require.Len(t, mockDB.logEntries, 1)
	assert.Empty(t, mockDB.logEntries[0].userID,
		"log entry should have empty user ID when anonymous")
}

func TestSynth_LogEntry_UserIDChangesAfterWrite(t *testing.T) {
	mockDB := newParentPointerMockDB()
	mockDB.lastInsertReturnPK = "uuid-new-1"
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	// First write as agent-7
	ops.SetUserID("agent-7")
	fsErr := ops.WriteFile(context.Background(), "/notes/hello.md", []byte("---\ntitle: Hello\n---\n# Hello\n"))
	require.Nil(t, fsErr)

	// Change identity via .info/user
	fsErr = ops.WriteFile(context.Background(), "/.info/user", []byte("agent-9\n"))
	require.Nil(t, fsErr)

	// Second write -- simulate edit by making resolve find the file
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-new-1", Name: "hello.md"},
	}
	mockDB.latestVersionIDs = map[string]string{"uuid-new-1": "v-1"}
	fsErr = ops.WriteFile(context.Background(), "/notes/hello.md", []byte("---\ntitle: Updated\n---\n# Updated\n"))
	require.Nil(t, fsErr)

	require.Len(t, mockDB.logEntries, 2)
	assert.Equal(t, "agent-7", mockDB.logEntries[0].userID, "first write should use agent-7")
	assert.Equal(t, "agent-9", mockDB.logEntries[1].userID, "second write should use agent-9")
}
