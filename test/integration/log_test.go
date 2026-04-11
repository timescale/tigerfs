package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSynth_LogEntries_Integration verifies that write operations on a
// history-enabled synth app create log entries in the _log hypertable.
// This is a full end-to-end test against a real PostgreSQL database.
func TestSynth_LogEntries_Integration(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logtest")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Create a synth app with history
	fsErr := ops.WriteFile(ctx, "/.build/logtest", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build logtest app: %v", fsErr)

	// Wait briefly for DDL to complete
	time.Sleep(100 * time.Millisecond)

	// INSERT: create a file
	content1 := "---\ntitle: First Post\n---\n# First Post\n\nHello world.\n"
	fsErr = ops.WriteFile(ctx, "/logtest/first-post.md", []byte(content1))
	require.Nil(t, fsErr, "create first-post.md: %v", fsErr)

	// UPDATE: edit the file
	content2 := "---\ntitle: First Post Updated\n---\n# First Post\n\nUpdated content.\n"
	fsErr = ops.WriteFile(ctx, "/logtest/first-post.md", []byte(content2))
	require.Nil(t, fsErr, "update first-post.md: %v", fsErr)

	// INSERT: create a second file
	content3 := "---\ntitle: Second Post\n---\n# Second Post\n"
	fsErr = ops.WriteFile(ctx, "/logtest/second-post.md", []byte(content3))
	require.Nil(t, fsErr, "create second-post.md: %v", fsErr)

	// DELETE: delete the second file
	fsErr = ops.Delete(ctx, "/logtest/second-post.md")
	require.Nil(t, fsErr, "delete second-post.md: %v", fsErr)

	// RENAME: rename the first file
	fsErr = ops.Rename(ctx, "/logtest/first-post.md", "/logtest/hello.md")
	require.Nil(t, fsErr, "rename first-post.md to hello.md: %v", fsErr)

	// Query the log table directly to verify entries
	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	rows, err := pool.Query(ctx, `SELECT type, filename,
		file_id IS NOT NULL AS has_file_id,
		version_id IS NOT NULL AS has_version_id
		FROM tigerfs.logtest_log ORDER BY log_id ASC`)
	require.NoError(t, err)
	defer rows.Close()

	type logRow struct {
		opType       string
		filename     string
		hasFileID    bool
		hasVersionID bool
	}
	var entries []logRow
	for rows.Next() {
		var r logRow
		err := rows.Scan(&r.opType, &r.filename, &r.hasFileID, &r.hasVersionID)
		require.NoError(t, err)
		entries = append(entries, r)
	}

	// Should have 5 log entries: create, edit, create, delete, rename
	require.Len(t, entries, 5, "expected 5 log entries, got %d: %+v", len(entries), entries)

	// Entry 0: CREATE first-post.md
	assert.Equal(t, "create", entries[0].opType)
	assert.Equal(t, "first-post.md", entries[0].filename)
	assert.True(t, entries[0].hasFileID, "create should have file_id")
	assert.False(t, entries[0].hasVersionID, "create should NOT have version_id")

	// Entry 1: EDIT first-post.md
	assert.Equal(t, "edit", entries[1].opType)
	assert.Equal(t, "first-post.md", entries[1].filename)
	assert.True(t, entries[1].hasFileID)
	assert.True(t, entries[1].hasVersionID, "edit should have version_id")

	// Entry 2: CREATE second-post.md
	assert.Equal(t, "create", entries[2].opType)
	assert.Equal(t, "second-post.md", entries[2].filename)

	// Entry 3: DELETE second-post.md
	assert.Equal(t, "delete", entries[3].opType)
	assert.Equal(t, "second-post.md", entries[3].filename)
	assert.True(t, entries[3].hasVersionID, "delete should have version_id")

	// Entry 4: RENAME with NEW filename
	assert.Equal(t, "rename", entries[4].opType)
	assert.Equal(t, "hello.md", entries[4].filename, "rename should log the new filename")
	assert.True(t, entries[4].hasVersionID, "rename should have version_id")

	// Verify the log table is a hypertable (has chunks)
	var isHypertable bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'logtest_log')`,
	).Scan(&isHypertable)
	require.NoError(t, err)
	assert.True(t, isHypertable, "logtest_log should be a hypertable")

	// Verify the savepoint table exists
	var savepointExists bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'tigerfs' AND table_name = 'logtest_savepoint')`,
	).Scan(&savepointExists)
	require.NoError(t, err)
	assert.True(t, savepointExists, "logtest_savepoint table should exist")

	// Verify user_id is NULL (identity not yet wired)
	var userIDNull bool
	err = pool.QueryRow(ctx,
		`SELECT user_id IS NULL FROM tigerfs.logtest_log LIMIT 1`,
	).Scan(&userIDNull)
	require.NoError(t, err)
	assert.True(t, userIDNull, "user_id should be NULL until identity is wired")

	t.Logf("Log entries verified: %d entries for create/edit/create/delete/rename", len(entries))
	for i, e := range entries {
		t.Logf("  [%d] %s %s (file_id=%v, version_id=%v)", i, e.opType, e.filename, e.hasFileID, e.hasVersionID)
	}
}

// TestSynth_LogEntries_NestedFiles verifies that log entries for files in
// subdirectories store the denormalized full path (ADR-017 Section 13.8).
// The log's filename column has different semantics from the source table's
// filename (leaf only) -- the log stores the full path for human-readable display.
func TestSynth_LogEntries_NestedFiles(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logdir")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/logdir", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build logdir app: %v", fsErr)
	time.Sleep(100 * time.Millisecond)

	// CREATE: nested file (auto-creates parent directories)
	content := "---\ntitle: Todo\n---\n# Todo\n"
	fsErr = ops.WriteFile(ctx, "/logdir/projects/web/todo.md", []byte(content))
	require.Nil(t, fsErr, "create nested file")

	// EDIT: update the nested file
	content2 := "---\ntitle: Todo Updated\n---\n# Todo Updated\n"
	fsErr = ops.WriteFile(ctx, "/logdir/projects/web/todo.md", []byte(content2))
	require.Nil(t, fsErr, "edit nested file")

	// RENAME: rename within same directory
	fsErr = ops.Rename(ctx, "/logdir/projects/web/todo.md", "/logdir/projects/web/done.md")
	require.Nil(t, fsErr, "rename nested file")

	// MOVE: move to different directory
	fsErr = ops.Mkdir(ctx, "/logdir/archive")
	require.Nil(t, fsErr, "mkdir archive")
	fsErr = ops.Rename(ctx, "/logdir/projects/web/done.md", "/logdir/archive/done.md")
	require.Nil(t, fsErr, "move file to archive")

	// DELETE: delete the moved file
	fsErr = ops.Delete(ctx, "/logdir/archive/done.md")
	require.Nil(t, fsErr, "delete file")

	// Query log entries
	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	rows, err := pool.Query(ctx, `SELECT type, filename FROM tigerfs.logdir_log ORDER BY log_id ASC`)
	require.NoError(t, err)
	defer rows.Close()

	type logRow struct {
		opType   string
		filename string
	}
	var entries []logRow
	for rows.Next() {
		var r logRow
		require.NoError(t, rows.Scan(&r.opType, &r.filename))
		entries = append(entries, r)
	}

	// Should have 5 log entries: create, edit, rename, rename(move), delete
	require.Len(t, entries, 5, "expected 5 log entries, got %d: %+v", len(entries), entries)

	// CREATE: full path of nested file
	assert.Equal(t, "create", entries[0].opType)
	assert.Equal(t, "projects/web/todo.md", entries[0].filename,
		"create log should store denormalized full path")

	// EDIT: same full path
	assert.Equal(t, "edit", entries[1].opType)
	assert.Equal(t, "projects/web/todo.md", entries[1].filename,
		"edit log should store denormalized full path")

	// RENAME: new filename in same directory
	assert.Equal(t, "rename", entries[2].opType)
	assert.Equal(t, "projects/web/done.md", entries[2].filename,
		"rename log should store new full path")

	// MOVE: new full path in different directory
	assert.Equal(t, "rename", entries[3].opType)
	assert.Equal(t, "archive/done.md", entries[3].filename,
		"move log should store new full path in target directory")

	// DELETE: full path at time of deletion
	assert.Equal(t, "delete", entries[4].opType)
	assert.Equal(t, "archive/done.md", entries[4].filename,
		"delete log should store full path at time of deletion")

	t.Logf("Nested log entries verified:")
	for i, e := range entries {
		t.Logf("  [%d] %s %s", i, e.opType, e.filename)
	}
}

// TestSynth_LogEntries_DirRenameOneEntry verifies that renaming a directory
// with children produces exactly ONE log entry (ADR-017 verification #44).
// This is the key improvement over the old prefix-based model where directory
// rename was an N-row UPDATE producing N log entries.
func TestSynth_LogEntries_DirRenameOneEntry(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logdirren")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/logdirren", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create directory with multiple files
	fsErr = ops.WriteFile(ctx, "/logdirren/mydir/a.md", []byte("---\ntitle: A\n---\nA\n"))
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/logdirren/mydir/b.md", []byte("---\ntitle: B\n---\nB\n"))
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/logdirren/mydir/c.md", []byte("---\ntitle: C\n---\nC\n"))
	require.Nil(t, fsErr)

	// Count log entries before rename
	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	var countBefore int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM tigerfs.logdirren_log`).Scan(&countBefore)
	require.NoError(t, err)

	// Rename the directory (contains 3 files)
	fsErr = ops.Rename(ctx, "/logdirren/mydir", "/logdirren/renamed")
	require.Nil(t, fsErr, "rename directory should succeed")

	// Count log entries after rename
	var countAfter int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM tigerfs.logdirren_log`).Scan(&countAfter)
	require.NoError(t, err)

	// Should produce exactly ONE new log entry (single-row rename, not N entries)
	assert.Equal(t, 1, countAfter-countBefore,
		"directory rename should produce exactly 1 log entry (ADR-017: single-row operation)")

	// Verify the entry is a rename with the new directory path
	var opType, filename string
	err = pool.QueryRow(ctx,
		`SELECT type, filename FROM tigerfs.logdirren_log ORDER BY log_id DESC LIMIT 1`,
	).Scan(&opType, &filename)
	require.NoError(t, err)
	assert.Equal(t, "rename", opType)
	assert.Equal(t, "renamed", filename, "should log new directory name")
}

// TestSynth_LogInterface_ReadDir verifies that .log/ appears in synth app
// ReadDir listings and that the .log/ pipeline works end-to-end.
func TestSynth_LogInterface_ReadDir(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logui")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Create app with history
	fsErr := ops.WriteFile(ctx, "/.build/logui", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// .log/ should appear in the app ReadDir
	entries, fsErr := ops.ReadDir(ctx, "/logui")
	require.Nil(t, fsErr)
	names := fsEntryNames(entries)
	assert.Contains(t, names, ".log", "synth app should show .log/")
	assert.Contains(t, names, ".savepoint", "synth app should show .savepoint/")
	assert.Contains(t, names, ".undo", "synth app should show .undo/")
	assert.Contains(t, names, ".history", "synth app should show .history/")

	// Create some files to generate log entries
	fsErr = ops.WriteFile(ctx, "/logui/hello.md", []byte("---\ntitle: Hello\n---\n# Hello\n"))
	require.Nil(t, fsErr)

	time.Sleep(50 * time.Millisecond)

	fsErr = ops.WriteFile(ctx, "/logui/hello.md", []byte("---\ntitle: Updated\n---\n# Updated\n"))
	require.Nil(t, fsErr)

	// .log/ listing should have entries
	logEntries, fsErr := ops.ReadDir(ctx, "/logui/.log")
	require.Nil(t, fsErr, "ReadDir .log/ should succeed")
	assert.GreaterOrEqual(t, len(logEntries), 2, "should have at least 2 log entries (create + edit)")

	// Log entries should be directories (rows-as-directories)
	for _, e := range logEntries {
		assert.True(t, e.IsDir, "log entries should be directories, got: %s", e.Name)
	}

	// Find an actual log entry (skip capability directories like .all, .by, etc.)
	var entryName string
	for _, e := range logEntries {
		if !strings.HasPrefix(e.Name, ".") {
			entryName = e.Name
			break
		}
	}
	require.NotEmpty(t, entryName, "should find a non-capability log entry")

	entryEntries, fsErr := ops.ReadDir(ctx, "/logui/.log/"+entryName)
	require.Nil(t, fsErr, "ReadDir .log/<entry> should succeed")

	entryNames := fsEntryNames(entryEntries)
	// Should have column files + diff symlinks
	assert.Contains(t, entryNames, "before", "log entry should have 'before' symlink")
	assert.Contains(t, entryNames, "after", "log entry should have 'after' symlink")
	assert.Contains(t, entryNames, "current", "log entry should have 'current' symlink")

	// Read a column value
	typeContent, fsErr := ops.ReadFile(ctx, "/logui/.log/"+entryName+"/type")
	require.Nil(t, fsErr, "ReadFile .log/<entry>/type should succeed")
	typeStr := strings.TrimSpace(string(typeContent.Data))
	assert.Contains(t, []string{"create", "edit", "rename", "delete", "undo"}, typeStr,
		"type column should be a valid operation type")
}

// TestSynth_LogInterface_DiffSymlinks verifies the before/after/current
// diff symlinks on log entries resolve correctly.
func TestSynth_LogInterface_DiffSymlinks(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logdiff")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/logdiff", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create a file (log: type=create, version_id=NULL)
	fsErr = ops.WriteFile(ctx, "/logdiff/hello.md", []byte("---\ntitle: V1\n---\n# V1\n"))
	require.Nil(t, fsErr)

	time.Sleep(1100 * time.Millisecond) // distinct UUIDv7

	// Edit the file (log: type=edit, version_id=non-NULL)
	fsErr = ops.WriteFile(ctx, "/logdiff/hello.md", []byte("---\ntitle: V2\n---\n# V2\n"))
	require.Nil(t, fsErr)

	// Get log entries
	logEntries, fsErr := ops.ReadDir(ctx, "/logdiff/.log")
	require.Nil(t, fsErr)
	require.GreaterOrEqual(t, len(logEntries), 2)

	// Find create and edit entries (oldest first since ReadDir returns by PK order)
	// Sort to find create vs edit
	var createEntry, editEntry string
	for _, e := range logEntries {
		typeContent, err := ops.ReadFile(ctx, "/logdiff/.log/"+e.Name+"/type")
		if err != nil {
			continue
		}
		tp := strings.TrimSpace(string(typeContent.Data))
		switch tp {
		case "create":
			createEntry = e.Name
		case "edit":
			editEntry = e.Name
		}
	}
	require.NotEmpty(t, createEntry, "should find a create log entry")
	require.NotEmpty(t, editEntry, "should find an edit log entry")

	// Create entry: before should be /dev/null (no before-state)
	beforeTarget, fsErr := ops.Readlink(ctx, "/logdiff/.log/"+createEntry+"/before")
	require.Nil(t, fsErr, "Readlink before on create should succeed")
	assert.Equal(t, "/dev/null", beforeTarget, "create's before should be /dev/null")

	// Edit entry: before should point to .history/ (has version_id)
	beforeTarget, fsErr = ops.Readlink(ctx, "/logdiff/.log/"+editEntry+"/before")
	require.Nil(t, fsErr, "Readlink before on edit should succeed")
	assert.Contains(t, beforeTarget, ".history/", "edit's before should point to .history/")
	assert.Contains(t, beforeTarget, "hello.md", "edit's before should reference hello.md")

	// Current on edit: file still exists
	currentTarget, fsErr := ops.Readlink(ctx, "/logdiff/.log/"+editEntry+"/current")
	require.Nil(t, fsErr, "Readlink current should succeed")
	assert.Contains(t, currentTarget, "hello.md", "current should point to live file")
	assert.NotEqual(t, "/dev/null", currentTarget, "current should not be /dev/null for existing file")

	// Delete the file to test /dev/null current
	fsErr = ops.Delete(ctx, "/logdiff/hello.md")
	require.Nil(t, fsErr)

	// Find the delete log entry
	logEntries, fsErr = ops.ReadDir(ctx, "/logdiff/.log")
	require.Nil(t, fsErr)
	var deleteEntry string
	for _, e := range logEntries {
		typeContent, err := ops.ReadFile(ctx, "/logdiff/.log/"+e.Name+"/type")
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(typeContent.Data)) == "delete" {
			deleteEntry = e.Name
		}
	}
	require.NotEmpty(t, deleteEntry, "should find a delete log entry")

	// Delete entry: current should be /dev/null (file deleted)
	currentTarget, fsErr = ops.Readlink(ctx, "/logdiff/.log/"+deleteEntry+"/current")
	require.Nil(t, fsErr, "Readlink current on delete should succeed")
	assert.Equal(t, "/dev/null", currentTarget, "current should be /dev/null for deleted file")

	// Delete entry: after should be /dev/null (nothing after delete)
	afterTarget, fsErr := ops.Readlink(ctx, "/logdiff/.log/"+deleteEntry+"/after")
	require.Nil(t, fsErr, "Readlink after on delete should succeed")
	assert.Equal(t, "/dev/null", afterTarget, "delete's after should be /dev/null")
}

// TestSynth_LogInterface_AfterChain verifies the "after" symlink points to
// .history/ (not current file) when there's a subsequent edit entry.
func TestSynth_LogInterface_AfterChain(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logchain")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/logchain", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create → Edit1 → Edit2 (three log entries)
	fsErr = ops.WriteFile(ctx, "/logchain/hello.md", []byte("---\ntitle: V1\n---\nV1\n"))
	require.Nil(t, fsErr)
	time.Sleep(1100 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/logchain/hello.md", []byte("---\ntitle: V2\n---\nV2\n"))
	require.Nil(t, fsErr)
	time.Sleep(1100 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/logchain/hello.md", []byte("---\ntitle: V3\n---\nV3\n"))
	require.Nil(t, fsErr)

	// Find the first edit entry (middle of the chain)
	logEntries, fsErr := ops.ReadDir(ctx, "/logchain/.log")
	require.Nil(t, fsErr)

	var editEntries []string
	for _, e := range logEntries {
		if strings.HasPrefix(e.Name, ".") {
			continue
		}
		typeContent, err := ops.ReadFile(ctx, "/logchain/.log/"+e.Name+"/type")
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(typeContent.Data)) == "edit" {
			editEntries = append(editEntries, e.Name)
		}
	}
	require.GreaterOrEqual(t, len(editEntries), 2, "should have at least 2 edit entries")

	// First edit's "after" should point to .history/ (because there's a subsequent edit)
	afterTarget, fsErr := ops.Readlink(ctx, "/logchain/.log/"+editEntries[0]+"/after")
	require.Nil(t, fsErr, "Readlink after on first edit should succeed")
	assert.Contains(t, afterTarget, ".history/", "first edit's after should point to .history/ (next edit's before-state)")
}

// TestSynth_LogInterface_Pipeline verifies that pipeline operations work on .log/.
func TestSynth_LogInterface_Pipeline(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logpipe")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/logpipe", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create 5 files to generate 5 log entries
	for i := 1; i <= 5; i++ {
		content := fmt.Sprintf("---\ntitle: File %d\n---\nContent %d\n", i, i)
		fsErr = ops.WriteFile(ctx, fmt.Sprintf("/logpipe/file%d.md", i), []byte(content))
		require.Nil(t, fsErr)
	}

	// .last/2 should return exactly 2 entries (plus capability dirs)
	entries, fsErr := ops.ReadDir(ctx, "/logpipe/.log/.last/2")
	require.Nil(t, fsErr, "ReadDir .log/.last/2 should succeed")

	var rowEntries []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name, ".") {
			rowEntries = append(rowEntries, e.Name)
		}
	}
	assert.Equal(t, 2, len(rowEntries), ".last/2 should return exactly 2 log entries")
}

// TestSynth_LogInterface_NestedFilename verifies that log entries for nested files
// store the denormalized full path in the filename column.
func TestSynth_LogInterface_NestedFilename(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "logpath")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/logpath", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create nested file
	fsErr = ops.WriteFile(ctx, "/logpath/docs/guide.md", []byte("---\ntitle: Guide\n---\n# Guide\n"))
	require.Nil(t, fsErr)

	// Find the log entry
	logEntries, fsErr := ops.ReadDir(ctx, "/logpath/.log")
	require.Nil(t, fsErr)
	var entryName string
	for _, e := range logEntries {
		if !strings.HasPrefix(e.Name, ".") {
			entryName = e.Name
			break
		}
	}
	require.NotEmpty(t, entryName)

	// Read the filename column -- should be full denormalized path
	fnContent, fsErr := ops.ReadFile(ctx, "/logpath/.log/"+entryName+"/filename")
	require.Nil(t, fsErr, "ReadFile .log/<id>/filename should succeed")
	assert.Equal(t, "docs/guide.md", strings.TrimSpace(string(fnContent.Data)),
		"log filename should be denormalized full path")
}

// TestSynth_LogInterface_UserIdentity verifies that log entries include the user_id
// when set via .info/user.
func TestSynth_LogInterface_UserIdentity(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "loguser")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/loguser", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Set user identity
	fsErr = ops.WriteFile(ctx, "/.info/user", []byte("agent-7\n"))
	require.Nil(t, fsErr)

	// Create a file
	fsErr = ops.WriteFile(ctx, "/loguser/hello.md", []byte("---\ntitle: Hello\n---\n# Hello\n"))
	require.Nil(t, fsErr)

	// Find the log entry
	logEntries, fsErr := ops.ReadDir(ctx, "/loguser/.log")
	require.Nil(t, fsErr)
	var entryName string
	for _, e := range logEntries {
		if !strings.HasPrefix(e.Name, ".") {
			entryName = e.Name
			break
		}
	}
	require.NotEmpty(t, entryName)

	// Read user_id column
	userContent, fsErr := ops.ReadFile(ctx, "/loguser/.log/"+entryName+"/user_id")
	require.Nil(t, fsErr, "ReadFile .log/<id>/user_id should succeed")
	assert.Equal(t, "agent-7", strings.TrimSpace(string(userContent.Data)),
		"log entry should include the user identity")
}

// cleanupTigerFSTablesWithLog extends cleanup to also drop _log and _savepoint tables.
func cleanupLogTables(t *testing.T, connStr string, tableNames ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			return
		}
		defer pool.Close()
		for _, name := range tableNames {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS tigerfs.%s_log CASCADE", name))
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS tigerfs.%s_savepoint CASCADE", name))
		}
	})
}
