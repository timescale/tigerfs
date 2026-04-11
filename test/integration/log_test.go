package integration

import (
	"context"
	"fmt"
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
