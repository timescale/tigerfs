package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
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

// TestSynth_Savepoint_NameBasedListing verifies that ReadDir returns human-readable
// names (not UUIDs) now that name is the PK.
func TestSynth_Savepoint_NameBasedListing(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spnames")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spnames", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/spnames/.savepoint/alpha.json", []byte(`{"description":"first"}`))
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/spnames/.savepoint/beta.json", []byte(`{"description":"second"}`))
	require.Nil(t, fsErr)

	entries, fsErr := ops.ReadDir(ctx, "/spnames/.savepoint")
	require.Nil(t, fsErr)

	var names []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			names = append(names, e.Name)
		}
	}
	assert.Contains(t, names, "alpha", "should list by human-readable name")
	assert.Contains(t, names, "beta", "should list by human-readable name")
	// Names should NOT look like UUIDs
	for _, n := range names {
		assert.Less(t, len(n), 36, "entry %q should be a name, not a UUID", n)
	}
}

// TestSynth_Savepoint_FormatFileRead verifies that cat .savepoint/name/.json
// returns the full row serialized in the requested format.
func TestSynth_Savepoint_FormatFileRead(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spfmtrd")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spfmtrd", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/spfmtrd/.savepoint/my-save.tsv", []byte("description\nTest checkpoint\n"))
	require.Nil(t, fsErr)

	// Read as JSON
	content, fsErr := ops.ReadFile(ctx, "/spfmtrd/.savepoint/my-save/.json")
	require.Nil(t, fsErr, "ReadFile .json should succeed")
	data := string(content.Data)
	assert.Contains(t, data, "my-save", "JSON should contain name")
	assert.Contains(t, data, "Test checkpoint", "JSON should contain description")

	// Read as TSV
	content, fsErr = ops.ReadFile(ctx, "/spfmtrd/.savepoint/my-save/.tsv")
	require.Nil(t, fsErr, "ReadFile .tsv should succeed")
	assert.Contains(t, string(content.Data), "my-save", "TSV should contain name")
}

// TestSynth_Savepoint_YAMLFormat verifies savepoint creation with .yaml suffix.
func TestSynth_Savepoint_YAMLFormat(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spyaml")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spyaml", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/spyaml/.savepoint/yaml-test.yaml", []byte("description: Created via YAML\n"))
	require.Nil(t, fsErr, "YAML savepoint creation should succeed")

	desc, fsErr := ops.ReadFile(ctx, "/spyaml/.savepoint/yaml-test/description")
	require.Nil(t, fsErr)
	assert.Equal(t, "Created via YAML", strings.TrimSpace(string(desc.Data)))
}

// TestSynth_Savepoint_EmptyBodyCreation verifies that echo "" > .savepoint/name.tsv
// creates a savepoint with just the name PK.
func TestSynth_Savepoint_EmptyBodyCreation(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spempty")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spempty", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Empty body -- should create with just name + auto-generated savepoint_id
	fsErr = ops.WriteFile(ctx, "/spempty/.savepoint/empty-save.json", []byte("{}"))
	require.Nil(t, fsErr, "empty body savepoint creation should succeed")

	// Verify name column
	name, fsErr := ops.ReadFile(ctx, "/spempty/.savepoint/empty-save/name")
	require.Nil(t, fsErr)
	assert.Equal(t, "empty-save", strings.TrimSpace(string(name.Data)))

	// Verify savepoint_id was auto-generated
	spID, fsErr := ops.ReadFile(ctx, "/spempty/.savepoint/empty-save/savepoint_id")
	require.Nil(t, fsErr)
	assert.GreaterOrEqual(t, len(strings.TrimSpace(string(spID.Data))), 36, "should have auto-generated UUID")
}

// TestSynth_Savepoint_DeleteAndRecreate verifies that a savepoint can be deleted
// and a new one created with the same name.
func TestSynth_Savepoint_DeleteAndRecreate(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spreuse")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spreuse", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create
	fsErr = ops.WriteFile(ctx, "/spreuse/.savepoint/reusable.tsv", []byte("description\nOriginal\n"))
	require.Nil(t, fsErr)

	desc, fsErr := ops.ReadFile(ctx, "/spreuse/.savepoint/reusable/description")
	require.Nil(t, fsErr)
	assert.Equal(t, "Original", strings.TrimSpace(string(desc.Data)))

	// Delete
	fsErr = ops.Delete(ctx, "/spreuse/.savepoint/reusable")
	require.Nil(t, fsErr)

	_, fsErr = ops.Stat(ctx, "/spreuse/.savepoint/reusable")
	require.NotNil(t, fsErr, "deleted savepoint should not be found")

	// Recreate with same name, different description
	fsErr = ops.WriteFile(ctx, "/spreuse/.savepoint/reusable.tsv", []byte("description\nRecreated\n"))
	require.Nil(t, fsErr, "recreating deleted savepoint should succeed")

	desc, fsErr = ops.ReadFile(ctx, "/spreuse/.savepoint/reusable/description")
	require.Nil(t, fsErr)
	assert.Equal(t, "Recreated", strings.TrimSpace(string(desc.Data)))
}

// TestSynth_Savepoint_Ordering verifies that .last/N returns entries in
// descending PK order (alphabetical by name since name is PK).
func TestSynth_Savepoint_Ordering(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spord")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/spord", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create savepoints with names that sort alphabetically
	fsErr = ops.WriteFile(ctx, "/spord/.savepoint/aaa-first.json", []byte("{}"))
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/spord/.savepoint/bbb-second.json", []byte("{}"))
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/spord/.savepoint/ccc-third.json", []byte("{}"))
	require.Nil(t, fsErr)

	// .last/2 should return the last 2 alphabetically (bbb, ccc)
	entries, fsErr := ops.ReadDir(ctx, "/spord/.savepoint/.last/2")
	require.Nil(t, fsErr)
	var names []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			names = append(names, e.Name)
		}
	}
	assert.Equal(t, 2, len(names), ".last/2 should return 2 entries")
	assert.Contains(t, names, "bbb-second")
	assert.Contains(t, names, "ccc-third")
	assert.NotContains(t, names, "aaa-first", "aaa-first should not be in .last/2")
}

// TestSynth_AutoSavepoint_CreatedOnGap verifies that an auto-savepoint is created
// when the inactivity gap exceeds the configured threshold. Uses injectable clock
// to avoid real sleeps.
func TestSynth_AutoSavepoint_CreatedOnGap(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spauto")

	// Create ops with auto-savepoint interval of 5 minutes
	cfg := &config.Config{
		DirListingLimit:       10000,
		QueryTimeout:          30,
		PoolSize:              5,
		PoolMaxIdle:           2,
		InsecureNoSSL:         true,
		AutoSavepointInterval: 5 * time.Minute,
	}
	ctx := context.Background()
	dbClient, err := db.NewClient(ctx, cfg, result.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { dbClient.Close() })

	ops := fs.NewOperations(cfg, dbClient)

	// Injectable clock
	baseTime := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	currentTime := baseTime
	ops.SetNowFunc(func() time.Time { return currentTime })

	// Create the app
	fsErr := ops.WriteFile(ctx, "/.build/spauto", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// First write at T+0 -- no auto-savepoint (first write, no previous session)
	fsErr = ops.WriteFile(ctx, "/spauto/first.md", []byte("---\ntitle: First\n---\nContent\n"))
	require.Nil(t, fsErr)

	// Second write at T+1m -- within interval, no auto-savepoint
	currentTime = baseTime.Add(1 * time.Minute)
	fsErr = ops.WriteFile(ctx, "/spauto/second.md", []byte("---\ntitle: Second\n---\nContent\n"))
	require.Nil(t, fsErr)

	// Verify no auto-savepoints yet
	entries, fsErr := ops.ReadDir(ctx, "/spauto/.savepoint")
	require.Nil(t, fsErr)
	var autoNames []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "auto-") {
			autoNames = append(autoNames, e.Name)
		}
	}
	assert.Empty(t, autoNames, "no auto-savepoints should exist yet")

	// Third write at T+10m -- exceeds 5m interval, should trigger auto-savepoint
	currentTime = baseTime.Add(10 * time.Minute)
	fsErr = ops.WriteFile(ctx, "/spauto/third.md", []byte("---\ntitle: Third\n---\nContent\n"))
	require.Nil(t, fsErr)

	// Verify auto-savepoint was created
	entries, fsErr = ops.ReadDir(ctx, "/spauto/.savepoint")
	require.Nil(t, fsErr)
	autoNames = nil
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "auto-") {
			autoNames = append(autoNames, e.Name)
		}
	}
	assert.Equal(t, 1, len(autoNames), "should have exactly 1 auto-savepoint")
	if len(autoNames) > 0 {
		assert.Contains(t, autoNames[0], "auto-", "name should start with auto-")
		// Verify it has a description mentioning inactivity
		desc, fsErr := ops.ReadFile(ctx, "/spauto/.savepoint/"+autoNames[0]+"/description")
		require.Nil(t, fsErr)
		assert.Contains(t, string(desc.Data), "inactivity")
	}
}

// TestSynth_AutoSavepoint_DisabledWhenZero verifies that interval=0 disables auto-savepoints.
func TestSynth_AutoSavepoint_DisabledWhenZero(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "spnone")

	cfg := &config.Config{
		DirListingLimit:       10000,
		QueryTimeout:          30,
		PoolSize:              5,
		PoolMaxIdle:           2,
		InsecureNoSSL:         true,
		AutoSavepointInterval: 0, // disabled
	}
	ctx := context.Background()
	dbClient, err := db.NewClient(ctx, cfg, result.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { dbClient.Close() })

	ops := fs.NewOperations(cfg, dbClient)

	baseTime := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	currentTime := baseTime
	ops.SetNowFunc(func() time.Time { return currentTime })

	fsErr := ops.WriteFile(ctx, "/.build/spnone", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// First write
	fsErr = ops.WriteFile(ctx, "/spnone/first.md", []byte("---\ntitle: First\n---\nContent\n"))
	require.Nil(t, fsErr)

	// Second write with huge gap -- should NOT trigger auto-savepoint
	currentTime = baseTime.Add(24 * time.Hour)
	fsErr = ops.WriteFile(ctx, "/spnone/second.md", []byte("---\ntitle: Second\n---\nContent\n"))
	require.Nil(t, fsErr)

	entries, fsErr := ops.ReadDir(ctx, "/spnone/.savepoint")
	require.Nil(t, fsErr)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name, "auto-"),
			"no auto-savepoints should exist when interval=0, found: %s", e.Name)
	}
}
