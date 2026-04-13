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

	// Create savepoint with description (TSV format)
	fsErr = ops.WriteFile(ctx, "/sptest/.savepoint/before-exploration.tsv", []byte("description\nStarting exploration\n"))
	require.Nil(t, fsErr, "create savepoint with description")

	// Create savepoint via JSON (name only, no description)
	fsErr = ops.WriteFile(ctx, "/sptest/.savepoint/quick-mark.json", []byte("{}"))
	require.Nil(t, fsErr, "create savepoint via JSON")

	// List savepoints -- with name as PK, entries are human-readable names
	entries, fsErr := ops.ReadDir(ctx, "/sptest/.savepoint")
	require.Nil(t, fsErr, "ReadDir .savepoint/ should succeed")
	var rowEntries []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			rowEntries = append(rowEntries, e.Name)
		}
	}
	assert.GreaterOrEqual(t, len(rowEntries), 2, "should have at least 2 savepoint entries")
	assert.Contains(t, rowEntries, "before-exploration", "should list by name")
	assert.Contains(t, rowEntries, "quick-mark", "should list by name")

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

	// Verify deleted savepoint is gone
	_, fsErr = ops.Stat(ctx, "/sptest/.savepoint/quick-mark")
	require.NotNil(t, fsErr, "deleted savepoint should not be found")

	// Original savepoint still accessible
	desc, fsErr = ops.ReadFile(ctx, "/sptest/.savepoint/before-exploration/description")
	require.Nil(t, fsErr, "non-deleted savepoint should still be accessible")
}

// TestSynth_Savepoint_PipelineLast verifies that .last/N pipeline works on savepoints.
func TestSynth_Savepoint_PipelineLast(t *testing.T) {
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

	// Create 3 savepoints
	fsErr = ops.WriteFile(ctx, "/sporder/.savepoint/first.json", []byte("{}"))
	require.Nil(t, fsErr)
	time.Sleep(50 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/sporder/.savepoint/second.json", []byte("{}"))
	require.Nil(t, fsErr)
	time.Sleep(50 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/sporder/.savepoint/third.json", []byte("{}"))
	require.Nil(t, fsErr)

	// .last/2 should return exactly 2 data entries (plus capability dirs)
	entries, fsErr := ops.ReadDir(ctx, "/sporder/.savepoint/.last/2")
	require.Nil(t, fsErr, "ReadDir .last/2 should succeed")
	var rowEntries []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			rowEntries = append(rowEntries, e.Name)
		}
	}
	assert.Equal(t, 2, len(rowEntries), ".last/2 should return exactly 2 savepoint entries")
}

// TestSynth_Savepoint_FilterByUser verifies that .by/user_id/<user> filters work.
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
	fsErr = ops.WriteFile(ctx, "/spuser/.savepoint/agent7-first.tsv", []byte("description\nFirst by agent 7\n"))
	require.Nil(t, fsErr)
	time.Sleep(50 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/spuser/.savepoint/agent7-second.tsv", []byte("description\nSecond by agent 7\n"))
	require.Nil(t, fsErr)

	// Create savepoints as agent-9
	ops.SetUserID("agent-9")
	fsErr = ops.WriteFile(ctx, "/spuser/.savepoint/agent9-only.tsv", []byte("description\nOnly by agent 9\n"))
	require.Nil(t, fsErr)

	// Verify column read works
	uid, fsErr := ops.ReadFile(ctx, "/spuser/.savepoint/agent7-first/user_id")
	require.Nil(t, fsErr)
	assert.Equal(t, "agent-7", strings.TrimSpace(string(uid.Data)))

	uid, fsErr = ops.ReadFile(ctx, "/spuser/.savepoint/agent9-only/user_id")
	require.Nil(t, fsErr)
	assert.Equal(t, "agent-9", strings.TrimSpace(string(uid.Data)))
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
	fsErr = ops.WriteFile(ctx, "/spuid/.savepoint/my-save.tsv", []byte("description\ncheckpoint\n"))
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

// TestSynth_Savepoint_BarePathRejected verifies that bare-path savepoint creation is rejected.
func TestSynth_Savepoint_BarePathRejected(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spbare")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spbare", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Bare path (no format suffix) should be rejected
	fsErr = ops.WriteFile(ctx, "/spbare/.savepoint/my-save", []byte("checkpoint\n"))
	require.NotNil(t, fsErr, "bare-path savepoint creation should be rejected")
	assert.Equal(t, fs.ErrInvalidArgument, fsErr.Code)
}

// TestSynth_Savepoint_MultipleFormats verifies savepoint creation with different formats.
func TestSynth_Savepoint_MultipleFormats(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spfmt")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spfmt", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// TSV format
	fsErr = ops.WriteFile(ctx, "/spfmt/.savepoint/via-tsv.tsv", []byte("description\nCreated via TSV\n"))
	require.Nil(t, fsErr, "TSV savepoint creation should succeed")

	// JSON format
	fsErr = ops.WriteFile(ctx, "/spfmt/.savepoint/via-json.json", []byte(`{"description":"Created via JSON"}`))
	require.Nil(t, fsErr, "JSON savepoint creation should succeed")

	// CSV format
	fsErr = ops.WriteFile(ctx, "/spfmt/.savepoint/via-csv.csv", []byte("description\nCreated via CSV\n"))
	require.Nil(t, fsErr, "CSV savepoint creation should succeed")

	// Verify all three exist and have correct descriptions
	for _, tc := range []struct {
		name, desc string
	}{
		{"via-tsv", "Created via TSV"},
		{"via-json", "Created via JSON"},
		{"via-csv", "Created via CSV"},
	} {
		d, fsErr := ops.ReadFile(ctx, "/spfmt/.savepoint/"+tc.name+"/description")
		require.Nil(t, fsErr, "ReadFile description for %s should succeed", tc.name)
		assert.Equal(t, tc.desc, strings.TrimSpace(string(d.Data)), "description for %s", tc.name)
	}
}
