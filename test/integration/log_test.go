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
