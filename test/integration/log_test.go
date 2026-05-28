package integration

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
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

	// MKDIRS: each gets its own 'create' log entry now that mkdirSynth
	// logs (mirrors per-segment kernel mkdir(2) calls in production).
	require.Nil(t, ops.Mkdir(ctx, "/logdir/projects"), "mkdir projects")
	require.Nil(t, ops.Mkdir(ctx, "/logdir/projects/web"), "mkdir projects/web")

	// CREATE: nested file (parents already exist).
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

	// 8 log entries total: 3 mkdirs (projects, projects/web, archive)
	// + 5 file ops (create, edit, rename, move-rename, delete).
	require.Len(t, entries, 8, "expected 8 log entries, got %d: %+v", len(entries), entries)

	// MKDIR projects (dir, denormalized full path).
	assert.Equal(t, "create", entries[0].opType)
	assert.Equal(t, "projects", entries[0].filename, "mkdir log full path")

	// MKDIR projects/web.
	assert.Equal(t, "create", entries[1].opType)
	assert.Equal(t, "projects/web", entries[1].filename, "mkdir log full path")

	// CREATE file: full path of nested file.
	assert.Equal(t, "create", entries[2].opType)
	assert.Equal(t, "projects/web/todo.md", entries[2].filename,
		"create log should store denormalized full path")

	// EDIT: same full path.
	assert.Equal(t, "edit", entries[3].opType)
	assert.Equal(t, "projects/web/todo.md", entries[3].filename,
		"edit log should store denormalized full path")

	// RENAME: new filename in same directory.
	assert.Equal(t, "rename", entries[4].opType)
	assert.Equal(t, "projects/web/done.md", entries[4].filename,
		"rename log should store new full path")

	// MKDIR archive (between rename and move).
	assert.Equal(t, "create", entries[5].opType)
	assert.Equal(t, "archive", entries[5].filename, "mkdir archive log full path")

	// MOVE: new full path in different directory.
	assert.Equal(t, "rename", entries[6].opType)
	assert.Equal(t, "archive/done.md", entries[6].filename,
		"move log should store new full path in target directory")

	// DELETE: full path at time of deletion.
	assert.Equal(t, "delete", entries[7].opType)
	assert.Equal(t, "archive/done.md", entries[7].filename,
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

	// Create directory with multiple files (mkdir-p the parent on first
	// write, then siblings just need WriteFile).
	fsErr = ops.WriteFileEnsureDirs(ctx, "/logdirren/mydir/a.md", []byte("---\ntitle: A\n---\nA\n"))
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

	// Edit entry: before should point into .history/.by/<file_id>/<version>.
	// We use the file_id-keyed path so the symlink stays valid across any
	// future rename of the file or its ancestor directories.
	beforeTarget, fsErr = ops.Readlink(ctx, "/logdiff/.log/"+editEntry+"/before")
	require.Nil(t, fsErr, "Readlink before on edit should succeed")
	assert.Contains(t, beforeTarget, ".history/.by/", "edit's before should use the file_id-keyed history path")
	assert.NotEqual(t, "/dev/null", beforeTarget)
	// Verify the symlink actually resolves by reading through it.
	beforeContent := readThroughSymlink(t, ops, "/logdiff/.log/"+editEntry+"/before")
	assert.Contains(t, string(beforeContent), "V1", "edit's before should hold the pre-edit content")

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

// readThroughSymlink reads a symlink target and follows it to read the
// underlying file content. Mirrors what the kernel does for cat <symlink>.
// Symlink targets are relative to the symlink's parent directory.
func readThroughSymlink(t *testing.T, ops *fs.Operations, symlinkPath string) []byte {
	t.Helper()
	ctx := context.Background()
	target, fsErr := ops.Readlink(ctx, symlinkPath)
	require.Nil(t, fsErr, "Readlink %s should succeed", symlinkPath)
	resolved := path.Join(path.Dir(symlinkPath), target)
	content, fsErr := ops.ReadFile(ctx, resolved)
	require.Nil(t, fsErr, "ReadFile %s (resolved from %s -> %s) should succeed", resolved, symlinkPath, target)
	return content.Data
}

// findLogEntryByType scans the log directory and returns the most recent
// entry of the given type. Returns "" if no such entry exists.
func findLogEntryByType(t *testing.T, ops *fs.Operations, logDir, opType string) string {
	t.Helper()
	ctx := context.Background()
	entries, fsErr := ops.ReadDir(ctx, logDir)
	require.Nil(t, fsErr)
	var found string
	for _, e := range entries {
		content, err := ops.ReadFile(ctx, logDir+"/"+e.Name+"/type")
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(content.Data)) == opType {
			// Keep the latest (entries returned in ascending log_id order).
			found = e.Name
		}
	}
	return found
}

// TestSynth_LogSymlinks_RenameInvariant covers the rename-aware behavior of
// the before/after/current diff symlinks. The symlinks must remain valid
// across renames and moves of the file or any ancestor directory; see
// keen-tumbling-dewdrop.md Bug A.
func TestSynth_LogSymlinks_RenameInvariant(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "ren")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/ren", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	t.Run("A.1_before_on_rename_entry", func(t *testing.T) {
		// create a.md, edit, rename a.md -> b.md.
		// Reading .log/<rename-id>/before must resolve to a real history
		// entry whose contents match the pre-rename body.
		require.Nil(t, ops.WriteFile(ctx, "/ren/a1.md", []byte("---\ntitle: a1v1\n---\n# a1v1\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.WriteFile(ctx, "/ren/a1.md", []byte("---\ntitle: a1v2\n---\n# a1v2\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.Rename(ctx, "/ren/a1.md", "/ren/b1.md"))

		renameEntry := findLogEntryByType(t, ops, "/ren/.log", "rename")
		require.NotEmpty(t, renameEntry, "must find a rename log entry")

		target, fsErr := ops.Readlink(ctx, "/ren/.log/"+renameEntry+"/before")
		require.Nil(t, fsErr)
		assert.Contains(t, target, ".history/.by/", "rename's before must use file_id path, not the new filename")
		// Must resolve through the symlink to actual content.
		content := readThroughSymlink(t, ops, "/ren/.log/"+renameEntry+"/before")
		assert.Contains(t, string(content), "a1v2", "rename's before should hold the pre-rename body")
	})

	t.Run("A.2_after_when_next_is_rename", func(t *testing.T) {
		// create a2.md, edit, rename a2.md -> b2.md.
		// Reading .log/<edit-id>/after must resolve to a real history entry
		// matching the post-edit/pre-rename body.
		require.Nil(t, ops.WriteFile(ctx, "/ren/a2.md", []byte("---\ntitle: a2v1\n---\n# a2v1\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.WriteFile(ctx, "/ren/a2.md", []byte("---\ntitle: a2v2\n---\n# a2v2\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.Rename(ctx, "/ren/a2.md", "/ren/b2.md"))

		// Find the edit for a2 specifically (multiple edits in the workspace).
		// Read the log and pick the one whose file_id matches the surviving file.
		var editEntry string
		entries, fsErr := ops.ReadDir(ctx, "/ren/.log")
		require.Nil(t, fsErr)
		for _, e := range entries {
			tp, err := ops.ReadFile(ctx, "/ren/.log/"+e.Name+"/type")
			if err != nil || strings.TrimSpace(string(tp.Data)) != "edit" {
				continue
			}
			fn, err := ops.ReadFile(ctx, "/ren/.log/"+e.Name+"/filename")
			if err != nil {
				continue
			}
			if strings.TrimSpace(string(fn.Data)) == "a2.md" {
				editEntry = e.Name
			}
		}
		require.NotEmpty(t, editEntry, "must find an edit log entry for a2.md")

		target, fsErr := ops.Readlink(ctx, "/ren/.log/"+editEntry+"/after")
		require.Nil(t, fsErr)
		assert.Contains(t, target, ".history/.by/", "after-with-next-rename must use file_id path")
		content := readThroughSymlink(t, ops, "/ren/.log/"+editEntry+"/after")
		assert.Contains(t, string(content), "a2v2", "edit's after should hold the post-edit body")
	})

	t.Run("A.4_current_after_a_rename", func(t *testing.T) {
		// create a4.md, then rename a4.md -> b4.md.
		// Reading .log/<create-id>/current must resolve to the live file at
		// its new name (b4.md), not the stale stored name (a4.md).
		require.Nil(t, ops.WriteFile(ctx, "/ren/a4.md", []byte("# a4\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.Rename(ctx, "/ren/a4.md", "/ren/b4.md"))

		// Find the create log entry for a4.md (filename column matches at log-write time).
		var createEntry string
		entries, fsErr := ops.ReadDir(ctx, "/ren/.log")
		require.Nil(t, fsErr)
		for _, e := range entries {
			tp, err := ops.ReadFile(ctx, "/ren/.log/"+e.Name+"/type")
			if err != nil || strings.TrimSpace(string(tp.Data)) != "create" {
				continue
			}
			fn, err := ops.ReadFile(ctx, "/ren/.log/"+e.Name+"/filename")
			if err != nil {
				continue
			}
			if strings.TrimSpace(string(fn.Data)) == "a4.md" {
				createEntry = e.Name
			}
		}
		require.NotEmpty(t, createEntry, "must find a create log entry for a4.md")

		target, fsErr := ops.Readlink(ctx, "/ren/.log/"+createEntry+"/current")
		require.Nil(t, fsErr)
		assert.Equal(t, "../../b4.md", target, "current must reflect the file's current path, not the log row's stored filename")
	})

	t.Run("A.5_directory_rename_invalidates_ancestor_path", func(t *testing.T) {
		// create tutorials/a5.md, edit, then rename directory tutorials -> archive.
		// Reading .log/<edit-id>/current must resolve to archive/a5.md (the
		// file's current path) and the before symlink must still resolve.
		require.Nil(t, ops.WriteFileEnsureDirs(ctx, "/ren/tutorials/a5.md", []byte("---\ntitle: a5v1\n---\n# a5v1\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.WriteFile(ctx, "/ren/tutorials/a5.md", []byte("---\ntitle: a5v2\n---\n# a5v2\n")))
		time.Sleep(1100 * time.Millisecond)
		require.Nil(t, ops.Rename(ctx, "/ren/tutorials", "/ren/archive"))

		// Find the most recent edit of tutorials/a5.md.
		var editEntry string
		entries, fsErr := ops.ReadDir(ctx, "/ren/.log")
		require.Nil(t, fsErr)
		for _, e := range entries {
			tp, err := ops.ReadFile(ctx, "/ren/.log/"+e.Name+"/type")
			if err != nil || strings.TrimSpace(string(tp.Data)) != "edit" {
				continue
			}
			fn, err := ops.ReadFile(ctx, "/ren/.log/"+e.Name+"/filename")
			if err != nil {
				continue
			}
			if strings.TrimSpace(string(fn.Data)) == "tutorials/a5.md" {
				editEntry = e.Name
			}
		}
		require.NotEmpty(t, editEntry, "must find an edit log entry under tutorials/a5.md")

		currentTarget, fsErr := ops.Readlink(ctx, "/ren/.log/"+editEntry+"/current")
		require.Nil(t, fsErr)
		assert.Equal(t, "../../archive/a5.md", currentTarget, "current must follow the directory rename")

		// before must still resolve through the file_id-keyed history path.
		content := readThroughSymlink(t, ops, "/ren/.log/"+editEntry+"/before")
		assert.Contains(t, string(content), "a5v1", "before should hold the pre-edit body")
	})
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

	// Create the parent dir then the nested file. Two log entries result
	// (one for each Mkdir, one for the file create); we want to assert
	// against the FILE create's denormalized filename specifically.
	require.Nil(t, ops.Mkdir(ctx, "/logpath/docs"), "mkdir docs")
	fsErr = ops.WriteFile(ctx, "/logpath/docs/guide.md", []byte("---\ntitle: Guide\n---\n# Guide\n"))
	require.Nil(t, fsErr)

	// Find the most recent log entry (the file create).
	logEntries, fsErr := ops.ReadDir(ctx, "/logpath/.log/.last/1")
	require.Nil(t, fsErr)
	var entryName string
	for _, e := range logEntries {
		if !strings.HasPrefix(e.Name, ".") {
			entryName = e.Name
			break
		}
	}
	require.NotEmpty(t, entryName)

	// Read the filename column -- should be full denormalized path.
	fnContent, fsErr := ops.ReadFile(ctx, "/logpath/.log/.last/1/"+entryName+"/filename")
	require.Nil(t, fsErr, "ReadFile .log/.last/1/<id>/filename should succeed")
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
