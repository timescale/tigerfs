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

// TestSynth_HistoryBuildWithHistory tests creating an app with "markdown,history".
// echo "markdown,history" > /.build/hist_build → creates tigerfs.hist_build table + view + history table + trigger
func TestSynth_HistoryBuildWithHistory(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_build")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build the markdown app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_build", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "WriteFile to .build/hist_build should succeed: %v", fsErr)

	// Root should list hist_build (synth view)
	entries, fsErr := ops.ReadDir(ctx, "/")
	require.Nil(t, fsErr, "ReadDir root should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "hist_build", "root should contain view hist_build")

	// The hist_build synth view should show .history/ in directory listing (even when empty)
	entries, fsErr = ops.ReadDir(ctx, "/hist_build")
	require.Nil(t, fsErr, "ReadDir /hist_build should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, ".history", "hist_build view should contain .history")

	// Stat .history/ should succeed
	entry, fsErr := ops.Stat(ctx, "/hist_build/.history")
	require.Nil(t, fsErr, "Stat /hist_build/.history should succeed")
	assert.True(t, entry.IsDir, ".history should be a directory")
}

// TestSynth_HistoryAddLater tests adding history to an existing app.
// First create a markdown app, then echo "history" > /.build/hist_add
func TestSynth_HistoryAddLater(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_add")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// First, create a markdown app without history
	fsErr := ops.WriteFile(ctx, "/.build/hist_add", []byte("markdown\n"))
	require.Nil(t, fsErr, "WriteFile markdown should succeed: %v", fsErr)

	// Verify .history/ does NOT appear
	entries, fsErr := ops.ReadDir(ctx, "/hist_add")
	require.Nil(t, fsErr, "ReadDir /hist_add should succeed")
	names := fsEntryNames(entries)
	assert.NotContains(t, names, ".history", "should NOT have .history before enabling")

	// Now add history to the existing app
	fsErr = ops.WriteFile(ctx, "/.build/hist_add", []byte("history\n"))
	require.Nil(t, fsErr, "WriteFile history should succeed: %v", fsErr)

	// Verify .history/ now appears
	entries, fsErr = ops.ReadDir(ctx, "/hist_add")
	require.Nil(t, fsErr, "ReadDir /hist_add should succeed after adding history")
	names = fsEntryNames(entries)
	assert.Contains(t, names, ".history", "should have .history after enabling")
}

// TestSynth_HistoryCaptureUpdate tests that updating a file captures the old version in .history/.
func TestSynth_HistoryCaptureUpdate(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_update")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_update", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed: %v", fsErr)

	// Create a file
	v1Content := "---\ntitle: Hello\nauthor: alice\n---\n\nFirst version.\n"
	fsErr = ops.WriteFile(ctx, "/hist_update/hello.md", []byte(v1Content))
	require.Nil(t, fsErr, "WriteFile v1 should succeed")

	// Brief pause to ensure distinct UUIDv7 timestamps
	time.Sleep(1100 * time.Millisecond)

	// Update the file — this should trigger the history capture
	v2Content := "---\ntitle: Hello\nauthor: alice\n---\n\nSecond version.\n"
	fsErr = ops.WriteFile(ctx, "/hist_update/hello.md", []byte(v2Content))
	require.Nil(t, fsErr, "WriteFile v2 should succeed")

	// Current file should show v2
	content, fsErr := ops.ReadFile(ctx, "/hist_update/hello.md")
	require.Nil(t, fsErr, "ReadFile current should succeed")
	assert.Contains(t, string(content.Data), "Second version.")

	// .history/hello.md/ should list at least one past version
	entries, fsErr := ops.ReadDir(ctx, "/hist_update/.history/hello.md")
	require.Nil(t, fsErr, "ReadDir .history/hello.md should succeed")

	// Should have .id file + at least 1 version
	names := fsEntryNames(entries)
	assert.Contains(t, names, ".id", "should have .id file")
	assert.GreaterOrEqual(t, len(entries), 2, "should have .id + at least 1 version entry")

	// Find the version entry (not .id)
	var versionID string
	for _, e := range entries {
		if e.Name != ".id" {
			versionID = e.Name
			break
		}
	}
	require.NotEmpty(t, versionID, "should find a version entry")

	// Read the past version — should contain v1 content
	histContent, fsErr := ops.ReadFile(ctx, "/hist_update/.history/hello.md/"+versionID)
	require.Nil(t, fsErr, "ReadFile history version should succeed")
	assert.Contains(t, string(histContent.Data), "First version.",
		"history version should contain the old content")
	assert.NotContains(t, string(histContent.Data), "Second version.",
		"history version should NOT contain the new content")
}

// TestSynth_HistoryCaptureDelete tests that deleting a file captures it in .history/.
func TestSynth_HistoryCaptureDelete(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_del")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_del", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a file
	content := "---\ntitle: Doomed\nauthor: bob\n---\n\nThis will be deleted.\n"
	fsErr = ops.WriteFile(ctx, "/hist_del/doomed.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile should succeed")

	// Brief pause
	time.Sleep(1100 * time.Millisecond)

	// Delete the file
	fsErr = ops.Delete(ctx, "/hist_del/doomed.md")
	require.Nil(t, fsErr, "Delete should succeed")

	// Current file should be gone
	_, fsErr = ops.ReadFile(ctx, "/hist_del/doomed.md")
	require.NotNil(t, fsErr, "ReadFile should fail after delete")

	// .history/ should still show the deleted file's history
	entries, fsErr := ops.ReadDir(ctx, "/hist_del/.history")
	require.Nil(t, fsErr, "ReadDir .history should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, ".by", "should have .by directory")
	assert.Contains(t, names, "doomed.md", "should have doomed.md in history")

	// Read the historical version
	versions, fsErr := ops.ReadDir(ctx, "/hist_del/.history/doomed.md")
	require.Nil(t, fsErr, "ReadDir .history/doomed.md should succeed")

	var versionID string
	for _, e := range versions {
		if e.Name != ".id" {
			versionID = e.Name
			break
		}
	}
	require.NotEmpty(t, versionID, "should find a version")

	histContent, fsErr := ops.ReadFile(ctx, "/hist_del/.history/doomed.md/"+versionID)
	require.Nil(t, fsErr, "ReadFile history of deleted file should succeed")
	assert.Contains(t, string(histContent.Data), "This will be deleted.")
}

// TestSynth_HistoryMultipleVersions tests that multiple updates create multiple history entries.
func TestSynth_HistoryMultipleVersions(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_list")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_list", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create initial file
	fsErr = ops.WriteFile(ctx, "/hist_list/evolving.md",
		[]byte("---\ntitle: Evolving\n---\n\nVersion 1.\n"))
	require.Nil(t, fsErr, "WriteFile v1 should succeed")

	// Update 3 times with delays for distinct timestamps
	for i := 2; i <= 4; i++ {
		time.Sleep(1100 * time.Millisecond)
		body := strings.Replace("---\ntitle: Evolving\n---\n\nVersion N.\n", "N", strings.Repeat("I", i), 1)
		fsErr = ops.WriteFile(ctx, "/hist_list/evolving.md", []byte(body))
		require.Nil(t, fsErr, "WriteFile v%d should succeed", i)
	}

	// Should have 3 history entries (v1, v2, v3 — v4 is current)
	entries, fsErr := ops.ReadDir(ctx, "/hist_list/.history/evolving.md")
	require.Nil(t, fsErr, "ReadDir .history/evolving.md should succeed")

	// Count version entries (exclude .id)
	versionCount := 0
	for _, e := range entries {
		if e.Name != ".id" {
			versionCount++
		}
	}
	assert.Equal(t, 3, versionCount, "should have 3 history versions (v1, v2, v3)")
}

// TestSynth_HistoryReadOnly tests that writes to .history/ are rejected.
func TestSynth_HistoryReadOnly(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_ro")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_ro", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Attempt to write to .history/ should fail with permission error
	fsErr = ops.WriteFile(ctx, "/hist_ro/.history/fake.md", []byte("should fail"))
	require.NotNil(t, fsErr, "WriteFile to .history/ should fail")
	assert.Equal(t, fs.ErrPermission, fsErr.Code, "should be permission error")

	// Attempt to mkdir in .history/ should also fail
	fsErr = ops.Mkdir(ctx, "/hist_ro/.history/fake-dir")
	require.NotNil(t, fsErr, "Mkdir in .history/ should fail")
	assert.Equal(t, fs.ErrPermission, fsErr.Code, "should be permission error")
}

// TestSynth_HistoryNoHistoryReturnsNotExist tests that .history/ returns ENOENT
// when the app doesn't have history enabled.
func TestSynth_HistoryNoHistoryReturnsNotExist(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "posts")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build a markdown app WITHOUT history
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Stat .history/ should return not-exist
	_, fsErr = ops.Stat(ctx, "/posts/.history")
	require.NotNil(t, fsErr, "Stat .history/ should fail without history")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should be not-exist error")

	// ReadDir .history/ should also fail
	_, fsErr = ops.ReadDir(ctx, "/posts/.history")
	require.NotNil(t, fsErr, "ReadDir .history/ should fail without history")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should be not-exist error")
}

// TestSynth_HistoryIDFile tests that .history/foo.md/.id returns the row UUID.
func TestSynth_HistoryIDFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_idf")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_idf", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a file and update it (so there's a history entry)
	fsErr = ops.WriteFile(ctx, "/hist_idf/test.md",
		[]byte("---\ntitle: Test\n---\n\nOriginal.\n"))
	require.Nil(t, fsErr, "WriteFile v1 should succeed")

	time.Sleep(1100 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/hist_idf/test.md",
		[]byte("---\ntitle: Test\n---\n\nUpdated.\n"))
	require.Nil(t, fsErr, "WriteFile v2 should succeed")

	// Read .id file
	content, fsErr := ops.ReadFile(ctx, "/hist_idf/.history/test.md/.id")
	require.Nil(t, fsErr, "ReadFile .id should succeed")

	idStr := strings.TrimSpace(string(content.Data))
	t.Logf(".id content: %q", idStr)

	// Should be a UUID (36 chars with hyphens)
	assert.Equal(t, 36, len(idStr), ".id should return a 36-char UUID, got %q", idStr)
	assert.Contains(t, idStr, "-", "UUID should contain hyphens")
}

// TestSynth_HistoryByUUID tests the .history/.by/<uuid>/ navigation path.
func TestSynth_HistoryByUUID(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_byid")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_byid", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a file and update it
	fsErr = ops.WriteFile(ctx, "/hist_byid/tracked.md",
		[]byte("---\ntitle: Tracked\n---\n\nOriginal content.\n"))
	require.Nil(t, fsErr, "WriteFile v1 should succeed")

	time.Sleep(1100 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/hist_byid/tracked.md",
		[]byte("---\ntitle: Tracked\n---\n\nUpdated content.\n"))
	require.Nil(t, fsErr, "WriteFile v2 should succeed")

	// Get the row UUID via .id file
	idContent, fsErr := ops.ReadFile(ctx, "/hist_byid/.history/tracked.md/.id")
	require.Nil(t, fsErr, "ReadFile .id should succeed")
	rowUUID := strings.TrimSpace(string(idContent.Data))

	// List .by/ should contain this UUID
	entries, fsErr := ops.ReadDir(ctx, "/hist_byid/.history/.by")
	require.Nil(t, fsErr, "ReadDir .by should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, rowUUID, ".by/ should list the row UUID")

	// List .by/<uuid>/ should show versions
	entries, fsErr = ops.ReadDir(ctx, "/hist_byid/.history/.by/"+rowUUID)
	require.Nil(t, fsErr, "ReadDir .by/<uuid> should succeed")
	assert.GreaterOrEqual(t, len(entries), 1, "should have at least 1 version under .by/<uuid>")

	// Read a version via .by/<uuid>/<version>
	versionID := entries[0].Name
	content, fsErr := ops.ReadFile(ctx, "/hist_byid/.history/.by/"+rowUUID+"/"+versionID)
	require.Nil(t, fsErr, "ReadFile via .by/<uuid>/<version> should succeed")
	assert.Contains(t, string(content.Data), "title: Tracked")
}

// TestSynth_HistoryDiff tests reading both current and historical versions for comparison.
func TestSynth_HistoryDiff(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_diff")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_diff", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create initial version
	fsErr = ops.WriteFile(ctx, "/hist_diff/doc.md",
		[]byte("---\ntitle: Document\nauthor: alice\n---\n\nFirst draft.\n"))
	require.Nil(t, fsErr, "WriteFile v1 should succeed")

	time.Sleep(1100 * time.Millisecond)

	// Update
	fsErr = ops.WriteFile(ctx, "/hist_diff/doc.md",
		[]byte("---\ntitle: Document\nauthor: alice\n---\n\nRevised draft.\n"))
	require.Nil(t, fsErr, "WriteFile v2 should succeed")

	// Read current version
	current, fsErr := ops.ReadFile(ctx, "/hist_diff/doc.md")
	require.Nil(t, fsErr, "ReadFile current should succeed")
	assert.Contains(t, string(current.Data), "Revised draft.")

	// Read historical version
	entries, fsErr := ops.ReadDir(ctx, "/hist_diff/.history/doc.md")
	require.Nil(t, fsErr, "ReadDir history should succeed")

	var versionID string
	for _, e := range entries {
		if e.Name != ".id" {
			versionID = e.Name
			break
		}
	}
	require.NotEmpty(t, versionID, "should find a version")

	historical, fsErr := ops.ReadFile(ctx, "/hist_diff/.history/doc.md/"+versionID)
	require.Nil(t, fsErr, "ReadFile historical should succeed")

	// They should differ
	assert.Contains(t, string(historical.Data), "First draft.")
	assert.NotContains(t, string(historical.Data), "Revised draft.")
	assert.Contains(t, string(current.Data), "Revised draft.")
	assert.NotContains(t, string(current.Data), "First draft.")
}

// TestSynth_HistoryPerDirectory tests that .history/ appears in subdirectory listings
// and shows only files at that directory level (not all filenames).
func TestSynth_HistoryPerDirectory(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_pdir")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build a hierarchical markdown app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_pdir", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed: %v", fsErr)

	// Create directory structure and files
	fsErr = ops.Mkdir(ctx, "/hist_pdir/getting-started")
	require.Nil(t, fsErr, "Mkdir getting-started should succeed")

	fsErr = ops.WriteFile(ctx, "/hist_pdir/getting-started/installation.md",
		[]byte("---\ntitle: Installation\n---\n\nInstall v1.\n"))
	require.Nil(t, fsErr, "WriteFile installation v1 should succeed")

	fsErr = ops.WriteFile(ctx, "/hist_pdir/getting-started/quickstart.md",
		[]byte("---\ntitle: Quickstart\n---\n\nQuickstart v1.\n"))
	require.Nil(t, fsErr, "WriteFile quickstart v1 should succeed")

	// Create a root-level file
	fsErr = ops.WriteFile(ctx, "/hist_pdir/readme.md",
		[]byte("---\ntitle: README\n---\n\nReadme v1.\n"))
	require.Nil(t, fsErr, "WriteFile readme v1 should succeed")

	// Update files to create history entries
	time.Sleep(1100 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/hist_pdir/getting-started/installation.md",
		[]byte("---\ntitle: Installation\n---\n\nInstall v2.\n"))
	require.Nil(t, fsErr, "WriteFile installation v2 should succeed")

	fsErr = ops.WriteFile(ctx, "/hist_pdir/getting-started/quickstart.md",
		[]byte("---\ntitle: Quickstart\n---\n\nQuickstart v2.\n"))
	require.Nil(t, fsErr, "WriteFile quickstart v2 should succeed")

	fsErr = ops.WriteFile(ctx, "/hist_pdir/readme.md",
		[]byte("---\ntitle: README\n---\n\nReadme v2.\n"))
	require.Nil(t, fsErr, "WriteFile readme v2 should succeed")

	// 1. Subdirectory listing should include .history/
	entries, fsErr := ops.ReadDir(ctx, "/hist_pdir/getting-started")
	require.Nil(t, fsErr, "ReadDir getting-started should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, ".history", "subdirectory should contain .history")

	// 2. Subdirectory .history/ should show only local files (not root files)
	entries, fsErr = ops.ReadDir(ctx, "/hist_pdir/getting-started/.history")
	require.Nil(t, fsErr, "ReadDir getting-started/.history should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, "installation.md", "should list installation.md")
	assert.Contains(t, names, "quickstart.md", "should list quickstart.md")
	assert.NotContains(t, names, "readme.md", "should NOT list root-level readme.md")
	assert.NotContains(t, names, ".by", "subdirectory .history should NOT have .by")

	// 3. Root .history/ should show only root-level files + .by/
	entries, fsErr = ops.ReadDir(ctx, "/hist_pdir/.history")
	require.Nil(t, fsErr, "ReadDir root .history should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, ".by", "root .history should have .by")
	assert.Contains(t, names, "readme.md", "root .history should list readme.md")
	assert.NotContains(t, names, "installation.md", "root .history should NOT list subdirectory files")
	assert.NotContains(t, names, "quickstart.md", "root .history should NOT list subdirectory files")

	// 4. Can stat subdirectory .history/
	entry, fsErr := ops.Stat(ctx, "/hist_pdir/getting-started/.history")
	require.Nil(t, fsErr, "Stat getting-started/.history should succeed")
	assert.True(t, entry.IsDir, ".history should be a directory")

	// 5. Can list versions of a file in subdirectory .history/
	entries, fsErr = ops.ReadDir(ctx, "/hist_pdir/getting-started/.history/installation.md")
	require.Nil(t, fsErr, "ReadDir getting-started/.history/installation.md should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, ".id", "should have .id file")
	assert.GreaterOrEqual(t, len(entries), 2, "should have .id + at least 1 version")

	// 6. Can read a historical version from subdirectory .history/
	var versionID string
	for _, e := range entries {
		if e.Name != ".id" {
			versionID = e.Name
			break
		}
	}
	require.NotEmpty(t, versionID, "should find a version entry")

	histContent, fsErr := ops.ReadFile(ctx, "/hist_pdir/getting-started/.history/installation.md/"+versionID)
	require.Nil(t, fsErr, "ReadFile subdirectory history version should succeed")
	assert.Contains(t, string(histContent.Data), "Install v1.",
		"historical version should contain v1 content")

	// 7. Can read .id from subdirectory .history/
	idContent, fsErr := ops.ReadFile(ctx, "/hist_pdir/getting-started/.history/installation.md/.id")
	require.Nil(t, fsErr, "ReadFile .id from subdirectory should succeed")
	idStr := strings.TrimSpace(string(idContent.Data))
	assert.Equal(t, 36, len(idStr), ".id should be a 36-char UUID, got %q", idStr)
}

// TestSynth_HistoryPerDirectoryStatVersion tests that stat works for older versions
// in subdirectory .history/ (regression for limit 1 bug).
func TestSynth_HistoryPerDirectoryStatVersion(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "hist_statv")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build hierarchical markdown app with history
	fsErr := ops.WriteFile(ctx, "/.build/hist_statv", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build should succeed")

	fsErr = ops.Mkdir(ctx, "/hist_statv/guide")
	require.Nil(t, fsErr, "Mkdir should succeed")

	// Create and update file 3 times to get multiple history entries
	fsErr = ops.WriteFile(ctx, "/hist_statv/guide/intro.md",
		[]byte("---\ntitle: Intro\n---\n\nVersion 1.\n"))
	require.Nil(t, fsErr, "WriteFile v1 should succeed")

	for i := 2; i <= 3; i++ {
		time.Sleep(1100 * time.Millisecond)
		body := strings.Replace("---\ntitle: Intro\n---\n\nVersion N.\n", "N",
			strings.Repeat("I", i), 1)
		fsErr = ops.WriteFile(ctx, "/hist_statv/guide/intro.md", []byte(body))
		require.Nil(t, fsErr, "WriteFile v%d should succeed", i)
	}

	// List all versions
	entries, fsErr := ops.ReadDir(ctx, "/hist_statv/guide/.history/intro.md")
	require.Nil(t, fsErr, "ReadDir should succeed")

	// Stat every version — all should succeed (tests fix for limit 1 bug)
	for _, e := range entries {
		if e.Name == ".id" {
			continue
		}
		entry, fsErr := ops.Stat(ctx, "/hist_statv/guide/.history/intro.md/"+e.Name)
		require.Nil(t, fsErr, "Stat version %s should succeed", e.Name)
		assert.False(t, entry.IsDir, "version file should not be a directory")
		assert.Greater(t, entry.Size, int64(0), "version file should have content")
	}
}
