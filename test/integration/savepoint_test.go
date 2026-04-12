package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// TestSynth_Savepoint_CRUD verifies create, read, update, and delete of savepoints.
func TestSynth_Savepoint_CRUD(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "sptest")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/sptest", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create savepoint with description
	fsErr = ops.WriteFile(ctx, "/sptest/.savepoint/before-exploration", []byte("Starting exploration\n"))
	require.Nil(t, fsErr, "create savepoint with description")

	// Create savepoint via touch (empty data = NULL description)
	fsErr = ops.WriteFile(ctx, "/sptest/.savepoint/quick-mark", []byte(""))
	require.Nil(t, fsErr, "create savepoint via touch")

	// List savepoints
	entries, fsErr := ops.ReadDir(ctx, "/sptest/.savepoint")
	require.Nil(t, fsErr, "ReadDir .savepoint/ should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "before-exploration")
	assert.Contains(t, names, "quick-mark")

	// Read column: description
	desc, fsErr := ops.ReadFile(ctx, "/sptest/.savepoint/before-exploration/description")
	require.Nil(t, fsErr, "ReadFile description should succeed")
	assert.Equal(t, "Starting exploration", strings.TrimSpace(string(desc.Data)))

	// Read column: savepoint_id (should be a UUID)
	spID, fsErr := ops.ReadFile(ctx, "/sptest/.savepoint/before-exploration/savepoint_id")
	require.Nil(t, fsErr, "ReadFile savepoint_id should succeed")
	assert.GreaterOrEqual(t, len(strings.TrimSpace(string(spID.Data))), 36, "savepoint_id should be a UUID")

	// Update description
	fsErr = ops.WriteFile(ctx, "/sptest/.savepoint/before-exploration/description", []byte("Updated description\n"))
	require.Nil(t, fsErr, "update description should succeed")

	desc, fsErr = ops.ReadFile(ctx, "/sptest/.savepoint/before-exploration/description")
	require.Nil(t, fsErr)
	assert.Equal(t, "Updated description", strings.TrimSpace(string(desc.Data)))

	// Delete savepoint
	fsErr = ops.Delete(ctx, "/sptest/.savepoint/quick-mark")
	require.Nil(t, fsErr, "delete savepoint should succeed")

	entries, fsErr = ops.ReadDir(ctx, "/sptest/.savepoint")
	require.Nil(t, fsErr)
	names = fsEntryNames(entries)
	assert.Contains(t, names, "before-exploration")
	assert.NotContains(t, names, "quick-mark", "deleted savepoint should not appear")
}

// TestSynth_Savepoint_ChronologicalOrder verifies that .last/N and .first/N
// return savepoints in chronological order (by savepoint_id), not alphabetical.
func TestSynth_Savepoint_ChronologicalOrder(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "sporder")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/sporder", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create savepoints with names that sort DIFFERENTLY alphabetically vs chronologically.
	// Alphabetical: aaa < bbb < zzz
	// Chronological (creation order): zzz, aaa, bbb

	fsErr = ops.WriteFile(ctx, "/sporder/.savepoint/zzz-first", []byte(""))
	require.Nil(t, fsErr)
	time.Sleep(50 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/sporder/.savepoint/aaa-second", []byte(""))
	require.Nil(t, fsErr)
	time.Sleep(50 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/sporder/.savepoint/bbb-third", []byte(""))
	require.Nil(t, fsErr)

	// .last/2 should return the 2 most RECENT (chronologically)
	entries, fsErr := ops.ReadDir(ctx, "/sporder/.savepoint/.last/2")
	require.Nil(t, fsErr, "ReadDir .last/2 should succeed")
	names := fsEntryNames(entries)
	require.Len(t, names, 2, ".last/2 should return exactly 2")

	// Most recent first: bbb-third, aaa-second (NOT alphabetical: aaa, bbb)
	assert.Equal(t, "bbb-third", names[0], "most recent savepoint should be first")
	assert.Equal(t, "aaa-second", names[1], "second most recent should be second")

	// .last/1 should return only the most recent
	entries, fsErr = ops.ReadDir(ctx, "/sporder/.savepoint/.last/1")
	require.Nil(t, fsErr)
	names = fsEntryNames(entries)
	require.Len(t, names, 1)
	assert.Equal(t, "bbb-third", names[0], ".last/1 should return the most recent")

	// .first/2 should return the 2 OLDEST (chronologically)
	entries, fsErr = ops.ReadDir(ctx, "/sporder/.savepoint/.first/2")
	require.Nil(t, fsErr, "ReadDir .first/2 should succeed")
	names = fsEntryNames(entries)
	require.Len(t, names, 2, ".first/2 should return exactly 2")
	assert.Equal(t, "zzz-first", names[0], "oldest savepoint should be first")
	assert.Equal(t, "aaa-second", names[1], "second oldest should be second")

	// Full listing (no limit) should also be chronological (most recent first)
	entries, fsErr = ops.ReadDir(ctx, "/sporder/.savepoint")
	require.Nil(t, fsErr)
	names = fsEntryNames(entries)
	require.Len(t, names, 3)
	assert.Equal(t, "bbb-third", names[0])
	assert.Equal(t, "aaa-second", names[1])
	assert.Equal(t, "zzz-first", names[2])
}

// TestSynth_Savepoint_FilterByUser verifies that .by/user_id/<user>/.last/N
// filters by user AND returns chronological order.
func TestSynth_Savepoint_FilterByUser(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spuser")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spuser", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create savepoints as agent-7
	ops.SetUserID("agent-7")
	fsErr = ops.WriteFile(ctx, "/spuser/.savepoint/agent7-first", []byte(""))
	require.Nil(t, fsErr)
	time.Sleep(50 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/spuser/.savepoint/agent7-second", []byte(""))
	require.Nil(t, fsErr)

	// Create savepoints as agent-9
	ops.SetUserID("agent-9")
	fsErr = ops.WriteFile(ctx, "/spuser/.savepoint/agent9-only", []byte(""))
	require.Nil(t, fsErr)

	// Filter by agent-7: should see only agent-7's savepoints
	entries, fsErr := ops.ReadDir(ctx, "/spuser/.savepoint/.by/user_id/agent-7")
	require.Nil(t, fsErr, "ReadDir .by/user_id/agent-7 should succeed")
	names := fsEntryNames(entries)
	assert.Len(t, names, 2, "agent-7 should have 2 savepoints")
	assert.Contains(t, names, "agent7-first")
	assert.Contains(t, names, "agent7-second")
	assert.NotContains(t, names, "agent9-only")

	// Filter by agent-9: should see only agent-9's savepoint
	entries, fsErr = ops.ReadDir(ctx, "/spuser/.savepoint/.by/user_id/agent-9")
	require.Nil(t, fsErr)
	names = fsEntryNames(entries)
	assert.Len(t, names, 1)
	assert.Equal(t, "agent9-only", names[0])

	// Combined: .by/user_id/agent-7/.last/1 should return most recent agent-7 savepoint
	entries, fsErr = ops.ReadDir(ctx, "/spuser/.savepoint/.by/user_id/agent-7/.last/1")
	require.Nil(t, fsErr, "ReadDir .by/user_id/agent-7/.last/1 should succeed")
	names = fsEntryNames(entries)
	require.Len(t, names, 1)
	assert.Equal(t, "agent7-second", names[0], "should return agent-7's most recent savepoint")
}

// TestSynth_Savepoint_UserIDPopulated verifies that creating a savepoint
// populates user_id from the mount-level identity.
func TestSynth_Savepoint_UserIDPopulated(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spuid")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spuid", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	ops.SetUserID("demo-user")
	fsErr = ops.WriteFile(ctx, "/spuid/.savepoint/my-save", []byte("checkpoint"))
	require.Nil(t, fsErr)

	uid, fsErr := ops.ReadFile(ctx, "/spuid/.savepoint/my-save/user_id")
	require.Nil(t, fsErr)
	assert.Equal(t, "demo-user", strings.TrimSpace(string(uid.Data)),
		"savepoint should have the mount-level user_id")
}

// TestSynth_Savepoint_StatNotFound verifies that stat on nonexistent savepoint returns ENOENT.
func TestSynth_Savepoint_StatNotFound(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spnf")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spnf", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	_, fsErr = ops.Stat(ctx, "/spnf/.savepoint/nonexistent")
	require.NotNil(t, fsErr)
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should return ErrNotExist for nonexistent savepoint")
}
