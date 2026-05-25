package integration

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/cmd"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

// TestSynth_MigrateDetectAndExecute tests the full tigerfs migrate command flow:
// 1. Create old-convention tables (_app + app view with tigerfs comment)
// 2. Run migrate --describe, verify detection
// 3. Run migrate --dry-run, verify SQL output
// 4. Run migrate, verify execution
// 5. Verify DB state post-migration
// 6. Run migrate again, verify idempotency
func TestSynth_MigrateDetectAndExecute(t *testing.T) {
	require.NoError(t, config.Init(), "config.Init should succeed")

	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	var schema string
	err = pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
	require.NoError(t, err)

	// --- Setup: Create old-convention synth app ---
	oldSQL := []string{
		fmt.Sprintf(`CREATE TABLE %q."_mig_test" (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			filename TEXT NOT NULL,
			filetype TEXT NOT NULL DEFAULT 'file',
			title TEXT,
			body TEXT,
			encoding TEXT NOT NULL DEFAULT 'utf8',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE(filename, filetype)
		)`, schema),
		fmt.Sprintf(`CREATE VIEW %q."mig_test" AS SELECT * FROM %q."_mig_test"`, schema, schema),
		fmt.Sprintf(`COMMENT ON VIEW %q."mig_test" IS 'tigerfs:md'`, schema),
		fmt.Sprintf(`INSERT INTO %q."_mig_test" (filename, title, body) VALUES ('hello.md', 'Hello', 'World')`, schema),
	}
	for _, sql := range oldSQL {
		_, err := pool.Exec(ctx, sql)
		require.NoError(t, err, "setup SQL failed: %s", sql)
	}

	// --- Step 1: migrate --describe ---
	describeCmd := cmd.BuildMigrateCmd()
	var describeBuf bytes.Buffer
	describeCmd.SetOut(&describeBuf)
	describeCmd.SetErr(&describeBuf)
	describeCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = describeCmd.Execute()
	require.NoError(t, err, "migrate --describe should succeed")

	describeOutput := describeBuf.String()
	assert.Contains(t, describeOutput, "move-backing-tables", "should list the migration name")
	assert.Contains(t, describeOutput, "_mig_test", "should list the pending table")

	// --- Step 2: migrate --dry-run ---
	dryRunCmd := cmd.BuildMigrateCmd()
	var dryRunBuf bytes.Buffer
	dryRunCmd.SetOut(&dryRunBuf)
	dryRunCmd.SetErr(&dryRunBuf)
	dryRunCmd.SetArgs([]string{result.ConnStr, "--dry-run", "--insecure-no-ssl"})
	err = dryRunCmd.Execute()
	require.NoError(t, err, "migrate --dry-run should succeed")

	dryRunOutput := dryRunBuf.String()
	assert.Contains(t, dryRunOutput, "ALTER TABLE", "dry-run should show ALTER TABLE")
	assert.Contains(t, dryRunOutput, "CREATE SCHEMA", "dry-run should show CREATE SCHEMA")
	assert.Contains(t, dryRunOutput, "CREATE VIEW", "dry-run should show CREATE VIEW")

	// Verify nothing actually changed (dry-run)
	var stillExists bool
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '_mig_test'
	)`, schema)).Scan(&stillExists)
	require.NoError(t, err)
	assert.True(t, stillExists, "dry-run should not have moved the table")

	// --- Step 3: migrate (execute) ---
	execCmd := cmd.BuildMigrateCmd()
	var execBuf bytes.Buffer
	execCmd.SetOut(&execBuf)
	execCmd.SetErr(&execBuf)
	execCmd.SetArgs([]string{result.ConnStr, "--insecure-no-ssl"})
	err = execCmd.Execute()
	require.NoError(t, err, "migrate should succeed")

	execOutput := execBuf.String()
	assert.Contains(t, execOutput, "Running migration", "should show progress")
	assert.Contains(t, execOutput, "Migrated", "should confirm completion")

	// --- Step 4: Verify DB state ---

	// Backing table should be in tigerfs schema
	var inTigerFS bool
	err = pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = 'tigerfs' AND tablename = 'mig_test'
	)`).Scan(&inTigerFS)
	require.NoError(t, err)
	assert.True(t, inTigerFS, "table should be in tigerfs schema")

	// Old table should be gone
	var oldExists bool
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '_mig_test'
	)`, schema)).Scan(&oldExists)
	require.NoError(t, err)
	assert.False(t, oldExists, "old table should be gone from user schema")

	// View should still exist and point to tigerfs schema
	var viewDef string
	err = pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT pg_get_viewdef('%s.mig_test'::regclass)`, schema)).Scan(&viewDef)
	require.NoError(t, err)
	assert.True(t, strings.Contains(viewDef, "tigerfs"), "view should reference tigerfs schema, got: %s", viewDef)

	// Data should still be accessible through the view
	var title string
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT title FROM %q."mig_test" WHERE filename = 'hello.md'`, schema)).Scan(&title)
	require.NoError(t, err)
	assert.Equal(t, "Hello", title)

	// --- Step 5: Idempotency ---
	idempCmd := cmd.BuildMigrateCmd()
	var idempBuf bytes.Buffer
	idempCmd.SetOut(&idempBuf)
	idempCmd.SetErr(&idempBuf)
	idempCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = idempCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, idempBuf.String(), "No pending migrations", "second run should find nothing to do")

	// Cleanup tigerfs tables
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cp, err := pgxpool.New(cleanupCtx, result.ConnStr)
		if err != nil {
			return
		}
		defer cp.Close()
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_test" CASCADE`)
	})
}

// TestSynth_MigrateWithHistory tests migration of a synth app that includes a history table.
func TestSynth_MigrateWithHistory(t *testing.T) {
	require.NoError(t, config.Init(), "config.Init should succeed")

	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	var schema string
	err = pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
	require.NoError(t, err)

	// --- Setup: Old-convention app with history table ---
	oldSQL := []string{
		fmt.Sprintf(`CREATE TABLE %q."_mig_hist" (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			filename TEXT NOT NULL,
			title TEXT,
			body TEXT,
			encoding TEXT NOT NULL DEFAULT 'utf8',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			modified_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, schema),
		fmt.Sprintf(`CREATE TABLE %q."_mig_hist_history" (
			id UUID,
			filename TEXT,
			title TEXT,
			body TEXT,
			encoding TEXT,
			created_at TIMESTAMPTZ,
			modified_at TIMESTAMPTZ,
			_history_id UUID NOT NULL PRIMARY KEY,
			_operation TEXT NOT NULL
		)`, schema),
		fmt.Sprintf(`CREATE VIEW %q."mig_hist" AS SELECT * FROM %q."_mig_hist"`, schema, schema),
		fmt.Sprintf(`COMMENT ON VIEW %q."mig_hist" IS 'tigerfs:md,history'`, schema),
		fmt.Sprintf(`INSERT INTO %q."_mig_hist" (filename, title, body) VALUES ('test.md', 'Test', 'Content')`, schema),
	}
	for _, sql := range oldSQL {
		_, err := pool.Exec(ctx, sql)
		require.NoError(t, err, "setup SQL failed: %s", sql)
	}

	// --- Describe should find it ---
	describeCmd := cmd.BuildMigrateCmd()
	var describeBuf bytes.Buffer
	describeCmd.SetOut(&describeBuf)
	describeCmd.SetErr(&describeBuf)
	describeCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = describeCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, describeBuf.String(), "_mig_hist")

	// --- Dry-run should include history table migration ---
	dryRunCmd := cmd.BuildMigrateCmd()
	var dryRunBuf bytes.Buffer
	dryRunCmd.SetOut(&dryRunBuf)
	dryRunCmd.SetErr(&dryRunBuf)
	dryRunCmd.SetArgs([]string{result.ConnStr, "--dry-run", "--insecure-no-ssl"})
	err = dryRunCmd.Execute()
	require.NoError(t, err)
	dryRunOutput := dryRunBuf.String()
	assert.Contains(t, dryRunOutput, "_mig_hist_history", "dry-run should include history table migration")

	// --- Execute ---
	execCmd := cmd.BuildMigrateCmd()
	var execBuf bytes.Buffer
	execCmd.SetOut(&execBuf)
	execCmd.SetErr(&execBuf)
	execCmd.SetArgs([]string{result.ConnStr, "--insecure-no-ssl"})
	err = execCmd.Execute()
	require.NoError(t, err, "migrate should succeed")

	// --- Verify both tables migrated ---
	var tableExists, historyExists bool
	err = pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = 'tigerfs' AND tablename = 'mig_hist'
	)`).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "backing table should be in tigerfs schema")

	err = pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = 'tigerfs' AND tablename = 'mig_hist_history'
	)`).Scan(&historyExists)
	require.NoError(t, err)
	assert.True(t, historyExists, "history table should be in tigerfs schema")

	// Old tables should be gone
	var oldTable, oldHistory bool
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '_mig_hist'
	)`, schema)).Scan(&oldTable)
	require.NoError(t, err)
	assert.False(t, oldTable, "old backing table should be gone")

	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(
		SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '_mig_hist_history'
	)`, schema)).Scan(&oldHistory)
	require.NoError(t, err)
	assert.False(t, oldHistory, "old history table should be gone")

	// Data accessible through view
	var title string
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT title FROM %q."mig_hist" WHERE filename = 'test.md'`, schema)).Scan(&title)
	require.NoError(t, err)
	assert.Equal(t, "Test", title)

	// Idempotent
	idempCmd := cmd.BuildMigrateCmd()
	var idempBuf bytes.Buffer
	idempCmd.SetOut(&idempBuf)
	idempCmd.SetErr(&idempBuf)
	idempCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = idempCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, idempBuf.String(), "No pending migrations")

	// Cleanup
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cp, err := pgxpool.New(cleanupCtx, result.ConnStr)
		if err != nil {
			return
		}
		defer cp.Close()
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_hist" CASCADE`)
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_hist_history" CASCADE`)
	})
}

// TestSynth_MigrateAddParentPointer tests the relational-directories migration end-to-end:
// 1. Creates old-schema tables (no parent_id) in tigerfs schema with hierarchical data
// 2. Creates old-schema history and log tables with old column names
// 3. Runs migration, verifies detection/dry-run/execution
// 4. Verifies DB state: parent_id chain, leaf filenames, column renames, type renames
// 5. Verifies TigerFS operations work on migrated data (ReadDir, Stat, ReadFile, Write)
// 6. Verifies idempotency
func TestSynth_MigrateAddParentPointer(t *testing.T) {
	require.NoError(t, config.Init(), "config.Init should succeed")

	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	var schema string
	err = pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
	require.NoError(t, err)

	// Enable TimescaleDB
	_, _ = pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb")

	// --- Setup: Create OLD-schema synth app in tigerfs schema ---
	// This simulates a database created before ADR-017 that has already been
	// through the move-backing-tables migration (tables in tigerfs schema).
	oldSQL := []string{
		`CREATE SCHEMA IF NOT EXISTS tigerfs`,

		// Source table: old schema (no parent_id, UNIQUE(filename, filetype))
		`CREATE TABLE tigerfs."mig_pp" (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			filename TEXT NOT NULL,
			filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
			title TEXT,
			author TEXT,
			headers JSONB DEFAULT '{}'::jsonb,
			body TEXT,
			encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE(filename, filetype)
		)`,

		// View in user schema
		fmt.Sprintf(`CREATE VIEW %q."mig_pp" AS SELECT * FROM tigerfs."mig_pp"`, schema),
		fmt.Sprintf(`COMMENT ON VIEW %q."mig_pp" IS 'tigerfs:md,history'`, schema),

		// Insert hierarchical data with old full-path filenames.
		// File rows include extension (TigerFS stores "readme.md" not "readme").
		`INSERT INTO tigerfs."mig_pp" (filename, filetype) VALUES ('docs', 'directory')`,
		`INSERT INTO tigerfs."mig_pp" (filename, filetype) VALUES ('docs/guides', 'directory')`,
		`INSERT INTO tigerfs."mig_pp" (filename, filetype, title, body) VALUES ('readme.md', 'file', 'Root Readme', '# Root')`,
		`INSERT INTO tigerfs."mig_pp" (filename, filetype, title, body) VALUES ('docs/install.md', 'file', 'Install Guide', '# Install')`,
		`INSERT INTO tigerfs."mig_pp" (filename, filetype, title, body) VALUES ('docs/guides/quickstart.md', 'file', 'Quickstart', '# Quick')`,

		// History table: old column names (id, _history_id, _operation).
		// _history_id defaults to uuidv7() to match real 0.6 (see commit
		// d3d77be / Task 12.3 -- the log/history hypertable schema has used
		// uuidv7() defaults from day one). encoding column carries 'utf8'
		// to match what the trigger would have captured from the source.
		`CREATE TABLE tigerfs."mig_pp_history" (
			id UUID,
			filename TEXT NOT NULL,
			filetype TEXT,
			title TEXT,
			author TEXT,
			headers JSONB,
			body TEXT,
			encoding TEXT,
			created_at TIMESTAMPTZ,
			modified_at TIMESTAMPTZ,
			_history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			_operation TEXT NOT NULL
		)`,

		// Insert some history entries (simulating past edits)
		`INSERT INTO tigerfs."mig_pp_history" (id, filename, filetype, title, body, encoding, _operation)
		 SELECT id, filename, filetype, 'Old Title', '# Old', 'utf8', 'UPDATE'
		 FROM tigerfs."mig_pp" WHERE filename = 'readme.md' AND filetype = 'file'`,
		`INSERT INTO tigerfs."mig_pp_history" (id, filename, filetype, title, body, encoding, _operation)
		 SELECT id, filename, filetype, 'Old Install', '# Old Install', 'utf8', 'UPDATE'
		 FROM tigerfs."mig_pp" WHERE filename = 'docs/install.md' AND filetype = 'file'`,

		// Log table: old column names (history_id) and old type values
		// (insert, update). log_id defaults to uuidv7() to match real 0.6.
		fmt.Sprintf(`CREATE TABLE tigerfs."mig_pp_log" (
			log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			user_id TEXT,
			type TEXT NOT NULL CHECK (type IN ('insert', 'update', 'delete', 'undo')),
			file_id UUID NOT NULL,
			filename TEXT NOT NULL,
			history_id UUID,
			description TEXT
		)`),

		// Savepoint table sits alongside log and history in a
		// history-enabled workspace.
		`CREATE TABLE tigerfs."mig_pp_savepoint" (
			name TEXT NOT NULL PRIMARY KEY,
			savepoint_id UUID NOT NULL DEFAULT uuidv7() UNIQUE,
			user_id TEXT,
			description TEXT
		)`,

		// Insert log entries with old type names
		`INSERT INTO tigerfs."mig_pp_log" (file_id, type, filename)
		 SELECT id, 'insert', filename FROM tigerfs."mig_pp" WHERE filetype = 'file'`,
	}
	for _, sql := range oldSQL {
		_, err := pool.Exec(ctx, sql)
		require.NoError(t, err, "setup SQL failed: %s", sql)
	}

	// --- Step 1: Detect ---
	describeCmd := cmd.BuildMigrateCmd()
	var describeBuf bytes.Buffer
	describeCmd.SetOut(&describeBuf)
	describeCmd.SetErr(&describeBuf)
	describeCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = describeCmd.Execute()
	require.NoError(t, err, "migrate --describe should succeed")
	assert.Contains(t, describeBuf.String(), "relational-directories")
	assert.Contains(t, describeBuf.String(), "mig_pp")

	// --- Step 2: Dry-run ---
	dryRunCmd := cmd.BuildMigrateCmd()
	var dryRunBuf bytes.Buffer
	dryRunCmd.SetOut(&dryRunBuf)
	dryRunCmd.SetErr(&dryRunBuf)
	dryRunCmd.SetArgs([]string{result.ConnStr, "--dry-run", "--insecure-no-ssl"})
	err = dryRunCmd.Execute()
	require.NoError(t, err, "migrate --dry-run should succeed")
	dryRunOutput := dryRunBuf.String()
	assert.Contains(t, dryRunOutput, "parent_id", "dry-run should mention parent_id")
	assert.Contains(t, dryRunOutput, "resolve_path", "dry-run should create resolve_path")

	// --- Step 3: Execute ---
	execCmd := cmd.BuildMigrateCmd()
	var execBuf bytes.Buffer
	execCmd.SetOut(&execBuf)
	execCmd.SetErr(&execBuf)
	execCmd.SetArgs([]string{result.ConnStr, "--insecure-no-ssl"})
	err = execCmd.Execute()
	require.NoError(t, err, "migrate should succeed: %s", execBuf.String())

	// --- Step 4: Verify source table ---

	// parent_id column should exist
	var hasParentID bool
	err = pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='tigerfs' AND table_name='mig_pp' AND column_name='parent_id'
	)`).Scan(&hasParentID)
	require.NoError(t, err)
	assert.True(t, hasParentID, "source table should have parent_id column")

	// Filenames should be leaf names only
	var filenames []string
	rows, err := pool.Query(ctx, `SELECT filename FROM tigerfs."mig_pp" ORDER BY filename`)
	require.NoError(t, err)
	for rows.Next() {
		var fn string
		require.NoError(t, rows.Scan(&fn))
		filenames = append(filenames, fn)
	}
	rows.Close()
	// Should be leaf names: docs, guides, install.md, quickstart.md, readme.md (alphabetical)
	assert.Equal(t, []string{"docs", "guides", "install.md", "quickstart.md", "readme.md"}, filenames,
		"filenames should be leaf names only (no slashes)")

	// Verify parent_id chain: docs is root, guides is child of docs
	var docsID, guidesParentID string
	err = pool.QueryRow(ctx, `SELECT id::text FROM tigerfs."mig_pp" WHERE filename='docs' AND filetype='directory'`).Scan(&docsID)
	require.NoError(t, err)
	err = pool.QueryRow(ctx, `SELECT parent_id::text FROM tigerfs."mig_pp" WHERE filename='guides' AND filetype='directory'`).Scan(&guidesParentID)
	require.NoError(t, err)
	assert.Equal(t, docsID, guidesParentID, "guides should be child of docs")

	// install.md should be child of docs
	var installParentID string
	err = pool.QueryRow(ctx, `SELECT parent_id::text FROM tigerfs."mig_pp" WHERE filename='install.md' AND filetype='file'`).Scan(&installParentID)
	require.NoError(t, err)
	assert.Equal(t, docsID, installParentID, "install.md should be child of docs")

	// readme.md should be at root (parent_id IS NULL)
	var readmeParentNull bool
	err = pool.QueryRow(ctx, `SELECT parent_id IS NULL FROM tigerfs."mig_pp" WHERE filename='readme.md' AND filetype='file'`).Scan(&readmeParentNull)
	require.NoError(t, err)
	assert.True(t, readmeParentNull, "readme should have NULL parent_id (root level)")

	// --- Step 5: Verify history table ---
	var hasFileID, hasVersionID, hasOperation bool
	err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name='mig_pp_history' AND column_name='file_id')`).Scan(&hasFileID)
	require.NoError(t, err)
	assert.True(t, hasFileID, "history should have file_id (renamed from id)")

	err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name='mig_pp_history' AND column_name='version_id')`).Scan(&hasVersionID)
	require.NoError(t, err)
	assert.True(t, hasVersionID, "history should have version_id (renamed from _history_id)")

	err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name='mig_pp_history' AND column_name='operation')`).Scan(&hasOperation)
	require.NoError(t, err)
	assert.True(t, hasOperation, "history should have operation (renamed from _operation)")

	// History filenames should be leaf names
	var histFilename string
	err = pool.QueryRow(ctx, `SELECT filename FROM tigerfs."mig_pp_history" LIMIT 1`).Scan(&histFilename)
	require.NoError(t, err)
	assert.False(t, strings.Contains(histFilename, "/"), "history filenames should be leaf names, got: %s", histFilename)

	// --- Step 6: Verify log table ---
	var hasLogVersionID bool
	err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name='mig_pp_log' AND column_name='version_id')`).Scan(&hasLogVersionID)
	require.NoError(t, err)
	assert.True(t, hasLogVersionID, "log should have version_id (renamed from history_id)")

	// Log type values should be updated
	var logTypes []string
	logRows, err := pool.Query(ctx, `SELECT DISTINCT type FROM tigerfs."mig_pp_log" ORDER BY type`)
	require.NoError(t, err)
	for logRows.Next() {
		var tp string
		require.NoError(t, logRows.Scan(&tp))
		logTypes = append(logTypes, tp)
	}
	logRows.Close()
	assert.Equal(t, []string{"create"}, logTypes, "old 'insert' types should be renamed to 'create'")

	// --- Step 7: Verify TigerFS operations work on migrated data ---
	ops := setupFSOperations(t, result.ConnStr)

	// ReadDir root should show readme.md and docs/
	entries, fsErr := ops.ReadDir(ctx, "/mig_pp")
	require.Nil(t, fsErr, "ReadDir root should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "readme.md", "root should show readme.md")
	assert.Contains(t, names, "docs", "root should show docs/")
	assert.NotContains(t, names, "install.md", "root should NOT show nested files")

	// ReadDir docs/ should show install.md and guides/
	entries, fsErr = ops.ReadDir(ctx, "/mig_pp/docs")
	require.Nil(t, fsErr, "ReadDir docs should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, "install.md")
	assert.Contains(t, names, "guides")

	// ReadFile should return content
	content, fsErr := ops.ReadFile(ctx, "/mig_pp/docs/guides/quickstart.md")
	require.Nil(t, fsErr, "ReadFile nested file should succeed")
	assert.Contains(t, string(content.Data), "# Quick")

	// Write a new file in migrated app
	newContent := "---\ntitle: New File\n---\n# New\n"
	fsErr = ops.WriteFile(ctx, "/mig_pp/docs/new-file.md", []byte(newContent))
	require.Nil(t, fsErr, "WriteFile in migrated app should succeed")

	// Verify new file is accessible
	entries, fsErr = ops.ReadDir(ctx, "/mig_pp/docs")
	require.Nil(t, fsErr)
	assert.Contains(t, fsEntryNames(entries), "new-file.md")

	// --- Step 7.5: Undo boundary (metadata row + block/allow semantics) ---
	verifyMigrationUndoBoundary(t, ctx, pool, result.ConnStr, schema, "mig_pp")

	// --- Step 8: Idempotency ---
	idempCmd := cmd.BuildMigrateCmd()
	var idempBuf bytes.Buffer
	idempCmd.SetOut(&idempBuf)
	idempCmd.SetErr(&idempBuf)
	idempCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = idempCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, idempBuf.String(), "No pending migrations", "should be idempotent")

	// Re-running the migrate command must not duplicate the metadata row.
	reRunCmd := cmd.BuildMigrateCmd()
	var reRunBuf bytes.Buffer
	reRunCmd.SetOut(&reRunBuf)
	reRunCmd.SetErr(&reRunBuf)
	reRunCmd.SetArgs([]string{result.ConnStr, "--insecure-no-ssl"})
	_ = reRunCmd.Execute()
	var markerCount int
	err = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM tigerfs."mig_pp_metadata" WHERE subject = $1`,
		synth.SubjectHistoryFormatMigration).Scan(&markerCount)
	require.NoError(t, err)
	assert.Equal(t, 1, markerCount, "re-running migration must not duplicate the boundary marker")

	// Cleanup
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cp, err := pgxpool.New(cleanupCtx, result.ConnStr)
		if err != nil {
			return
		}
		defer cp.Close()
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_pp" CASCADE`)
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_pp_history" CASCADE`)
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_pp_log" CASCADE`)
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_pp_savepoint" CASCADE`)
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_pp_metadata" CASCADE`)
		cp.Exec(cleanupCtx, `DROP VIEW IF EXISTS "mig_pp" CASCADE`)
	})
}

// verifyMigrationUndoBoundary covers both block and allow paths:
//
//   - One metadata row with subject SubjectHistoryFormatMigration exists.
//   - Single-entry undo of a pre-migration log_id is refused with EPERM
//     and the boundary marker's description as the FSError Hint.
//   - Range undo (to-log-id) targeting pre-migration is refused at the
//     entry point (no DB transaction begun).
//   - A post-migration edit is undoable normally.
//   - Re-applying the migration does not duplicate the boundary marker.
func verifyMigrationUndoBoundary(t *testing.T, ctx context.Context, pool *pgxpool.Pool, connStr, schema, appName string) {
	t.Helper()

	metadataTable := appName + synth.MetadataTableSuffix
	logTable := appName + "_log"

	// 1. Marker row present (exactly one).
	var markerRows int
	err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM tigerfs.%q WHERE subject = $1`, metadataTable),
		synth.SubjectHistoryFormatMigration).Scan(&markerRows)
	require.NoError(t, err)
	assert.Equal(t, 1, markerRows, "expected exactly one history-format-migration marker")

	var markerDescription string
	var markerEntryID string
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT entry_id::text, description FROM tigerfs.%q WHERE subject = $1`, metadataTable),
		synth.SubjectHistoryFormatMigration).Scan(&markerEntryID, &markerDescription)
	require.NoError(t, err)
	require.NotEmpty(t, markerDescription, "boundary marker must have non-empty description")
	require.NotEmpty(t, markerEntryID, "boundary marker must have an entry_id")

	// 2. Pre-migration log_id (one of the entries inserted in test setup).
	// log_ids are UUIDv7 in real 0.6 (and in our corrected fixture), so
	// lexical compare against the marker entry_id is meaningful.
	var preMigrationLogID string
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT log_id::text FROM tigerfs.%q ORDER BY log_id ASC LIMIT 1`, logTable),
	).Scan(&preMigrationLogID)
	require.NoError(t, err, "expected at least one pre-migration log entry from test setup")

	require.True(t, preMigrationLogID < markerEntryID,
		"pre-migration log_id (%s) should sort before marker entry_id (%s)",
		preMigrationLogID, markerEntryID)

	// 3. Block: ExecuteUndoSingle on the pre-migration log_id.
	ops := setupFSOperations(t, connStr)
	_, undoErr := ops.ExecuteUndoSingle(ctx, schema, appName, preMigrationLogID)
	require.Error(t, undoErr, "undo of pre-migration log entry must be refused")
	fsErr, ok := undoErr.(*fs.FSError)
	require.True(t, ok, "expected *fs.FSError, got %T: %v", undoErr, undoErr)
	assert.Equal(t, fs.ErrPermission, fsErr.Code)
	assert.Equal(t, markerDescription, fsErr.Hint,
		"FSError.Hint must surface the boundary entry's description verbatim")

	// 4. Block: ExecuteUndoToLogID targeting a log_id older than the marker.
	_, rangeErr := ops.ExecuteUndoToLogID(ctx, schema, appName, preMigrationLogID, nil)
	require.Error(t, rangeErr, "undo-to-log-id crossing the boundary must be refused")
	rangeFsErr, ok := rangeErr.(*fs.FSError)
	require.True(t, ok, "expected *fs.FSError, got %T: %v", rangeErr, rangeErr)
	assert.Equal(t, fs.ErrPermission, rangeFsErr.Code)

	// 5. Allow: post-migration edit + undo of the edit. Exercises the
	// full restore-from-history path through the boundary check.
	editContent := "---\ntitle: Edited Post Migration\n---\n# Edited\n"
	wErr := ops.WriteFile(ctx, "/"+appName+"/readme.md", []byte(editContent))
	require.Nil(t, wErr, "WriteFile (edit) should succeed post-migration")

	var postLogID string
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT log_id::text FROM tigerfs.%q ORDER BY log_id DESC LIMIT 1`, logTable),
	).Scan(&postLogID)
	require.NoError(t, err)
	require.True(t, postLogID > markerEntryID,
		"post-migration log_id (%s) should sort after marker entry_id (%s)",
		postLogID, markerEntryID)

	undoResult, undoErr := ops.ExecuteUndoSingle(ctx, schema, appName, postLogID)
	require.Nil(t, undoErr, "post-migration undo must succeed (boundary did not block)")
	require.NotNil(t, undoResult)
	assert.GreaterOrEqual(t, undoResult.FilesRestored+undoResult.FilesDeleted, 1,
		"post-migration undo should restore or delete at least one row")

	// 6. Allow: undo-to-savepoint created post-migration. Savepoint sits
	// strictly after the marker, so the boundary check passes and the
	// multi-file restore runs through ExecuteUndo.
	spName := "post-mig-checkpoint"
	wErr = ops.WriteFile(ctx, "/"+appName+"/.savepoint/"+spName+".json",
		[]byte(`{"description":"post-migration checkpoint"}`))
	require.Nil(t, wErr, "savepoint creation should succeed")

	wErr = ops.WriteFile(ctx, "/"+appName+"/readme.md",
		[]byte("---\ntitle: Edited A\n---\n# A\n"))
	require.Nil(t, wErr)
	wErr = ops.WriteFile(ctx, "/"+appName+"/docs/install.md",
		[]byte("---\ntitle: Edited B\n---\n# B\n"))
	require.Nil(t, wErr)

	spResult, spErr := ops.ExecuteUndoToSavepoint(ctx, schema, appName, spName, nil)
	require.Nil(t, spErr, "undo-to-savepoint past-marker target must succeed")
	require.NotNil(t, spResult)
	assert.GreaterOrEqual(t, spResult.FilesRestored, 2,
		"undo-to-savepoint should restore both edited files")

	// 7. Allow: undo-to-log-id between marker and current. Edit A, edit B,
	// undo to A's log_id; B's edit should be reverted, A's preserved.
	contentA := "---\ntitle: First Edit\n---\n# First\n"
	wErr = ops.WriteFile(ctx, "/"+appName+"/readme.md", []byte(contentA))
	require.Nil(t, wErr)

	var firstEditLogID string
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT log_id::text FROM tigerfs.%q ORDER BY log_id DESC LIMIT 1`, logTable),
	).Scan(&firstEditLogID)
	require.NoError(t, err)
	require.True(t, firstEditLogID > markerEntryID,
		"first edit's log_id must be post-marker")

	contentB := "---\ntitle: Second Edit\n---\n# Second\n"
	wErr = ops.WriteFile(ctx, "/"+appName+"/docs/install.md", []byte(contentB))
	require.Nil(t, wErr)

	idResult, idErr := ops.ExecuteUndoToLogID(ctx, schema, appName, firstEditLogID, nil)
	require.Nil(t, idErr, "undo-to-log-id past-marker target must succeed")
	require.NotNil(t, idResult)
	assert.GreaterOrEqual(t, idResult.FilesRestored+idResult.FilesDeleted, 1,
		"undo-to-log-id should restore the second edit")

	// readme.md is the file we edited *at* firstEditLogID -- it should
	// remain at contentA (undo target is exclusive of the target itself).
	readmeBody, rfErr := ops.ReadFile(ctx, "/"+appName+"/readme.md")
	require.Nil(t, rfErr)
	assert.Contains(t, string(readmeBody.Data), "First",
		"readme.md should still hold the first-edit content (target is exclusive)")
}

// TestSynth_FreshInstall_MetadataFastPath verifies that a fresh 0.7
// workspace (one that was never migrated from 0.6) has the metadata table
// present and empty, and that undo operations work normally without ever
// hitting a boundary block. This is the fast-path scenario: most users
// will never have a history-format-migration marker because they started
// on 0.7 or later.
func TestSynth_FreshInstall_MetadataFastPath(t *testing.T) {
	require.NoError(t, config.Init(), "config.Init should succeed")

	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "freshapp")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb")

	ops := setupFSOperations(t, result.ConnStr)

	// Build a fresh history-enabled markdown app.
	wErr := ops.WriteFile(ctx, "/.build/freshapp", []byte("markdown,history\n"))
	require.Nil(t, wErr, "build should succeed")

	// Verify the metadata table exists (created by GenerateHistorySQL)
	// and is empty: a fresh install has no migrations to mark.
	var tableExists bool
	err = pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM pg_tables
		WHERE schemaname = 'tigerfs' AND tablename = 'freshapp' || $1
	)`, synth.MetadataTableSuffix).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "fresh-install metadata table should exist after build")

	var rowCount int
	err = pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM tigerfs.%q`,
			"freshapp"+synth.MetadataTableSuffix),
	).Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 0, rowCount, "fresh-install metadata table should be empty (no boundary markers)")

	// Create + edit a file so we have something to undo.
	wErr = ops.WriteFile(ctx, "/freshapp/hello.md",
		[]byte("---\ntitle: Hello\n---\n# Hello\n"))
	require.Nil(t, wErr)

	wErr = ops.WriteFile(ctx, "/freshapp/hello.md",
		[]byte("---\ntitle: Hello\n---\n# Hello edited\n"))
	require.Nil(t, wErr)

	var editLogID string
	err = pool.QueryRow(ctx,
		`SELECT log_id::text FROM tigerfs."freshapp_log" WHERE type = 'edit' ORDER BY log_id DESC LIMIT 1`,
	).Scan(&editLogID)
	require.NoError(t, err, "expected an edit log entry")

	// Undo on a fresh-install workspace must succeed: no boundary entries
	// exist, so checkBoundary returns nil immediately.
	undoResult, undoErr := ops.ExecuteUndoSingle(ctx, "public", "freshapp", editLogID)
	require.Nil(t, undoErr, "fresh-install undo should not be blocked by boundary check")
	require.NotNil(t, undoResult)
	assert.GreaterOrEqual(t, undoResult.FilesRestored, 1,
		"undo of edit should restore one row")
}

// TestSynth_MigrateAddParentDirMtimeTrigger tests migration for workspaces that have
// parent_id but lack the parent directory mtime trigger.
func TestSynth_MigrateAddParentDirMtimeTrigger(t *testing.T) {
	require.NoError(t, config.Init(), "config.Init should succeed")

	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	var schema string
	err = pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
	require.NoError(t, err)

	// Setup: Create a workspace with parent_id but WITHOUT the parent mtime trigger.
	// This simulates an existing workspace created before this feature.
	setupSQL := []string{
		`CREATE SCHEMA IF NOT EXISTS tigerfs`,
		`CREATE TABLE tigerfs."mig_mtime" (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			parent_id UUID REFERENCES tigerfs."mig_mtime"(id),
			filename TEXT NOT NULL,
			filetype TEXT NOT NULL DEFAULT 'file',
			title TEXT,
			body TEXT,
			encoding TEXT NOT NULL DEFAULT 'utf8',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype)
		)`,
		fmt.Sprintf(`CREATE VIEW %q."mig_mtime" AS SELECT * FROM tigerfs."mig_mtime"`, schema),
		fmt.Sprintf(`COMMENT ON VIEW %q."mig_mtime" IS 'tigerfs:md'`, schema),
		// Add the modified_at BEFORE trigger (existing workspaces have this)
		`CREATE OR REPLACE FUNCTION tigerfs."set_mig_mtime_modified_at"()
		RETURNS TRIGGER AS $$
		BEGIN NEW.modified_at = now(); RETURN NEW; END;
		$$ LANGUAGE plpgsql`,
		`CREATE TRIGGER "trg_mig_mtime_modified_at"
		BEFORE UPDATE ON tigerfs."mig_mtime"
		FOR EACH ROW EXECUTE FUNCTION tigerfs."set_mig_mtime_modified_at"()`,
	}
	for _, sql := range setupSQL {
		_, err := pool.Exec(ctx, sql)
		require.NoError(t, err, "setup SQL failed: %s", sql)
	}

	// Step 1: Detect should find this workspace
	descCmd := cmd.BuildMigrateCmd()
	var descBuf bytes.Buffer
	descCmd.SetOut(&descBuf)
	descCmd.SetErr(&descBuf)
	descCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = descCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, descBuf.String(), "parent-dir-mtime-trigger")
	assert.Contains(t, descBuf.String(), "mig_mtime")

	// Step 2: Dry-run should show trigger SQL
	dryCmd := cmd.BuildMigrateCmd()
	var dryBuf bytes.Buffer
	dryCmd.SetOut(&dryBuf)
	dryCmd.SetErr(&dryBuf)
	dryCmd.SetArgs([]string{result.ConnStr, "--dry-run", "--insecure-no-ssl"})
	err = dryCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, dryBuf.String(), "bump_mig_mtime_parent_mtime")
	assert.Contains(t, dryBuf.String(), "AFTER INSERT OR DELETE OR UPDATE")

	// Step 3: Execute migration
	execCmd := cmd.BuildMigrateCmd()
	var execBuf bytes.Buffer
	execCmd.SetOut(&execBuf)
	execCmd.SetErr(&execBuf)
	execCmd.SetArgs([]string{result.ConnStr, "--insecure-no-ssl"})
	err = execCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, execBuf.String(), "parent-dir-mtime-trigger")

	// Step 4: Verify trigger exists
	var hasTrigger bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM pg_trigger t
			JOIN pg_class c ON c.oid = t.tgrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE t.tgname = 'trg_mig_mtime_parent_mtime'
			  AND c.relname = 'mig_mtime' AND n.nspname = 'tigerfs'
		)`).Scan(&hasTrigger)
	require.NoError(t, err)
	assert.True(t, hasTrigger, "trigger should exist after migration")

	// Step 5: Verify trigger works -- create a dir, then insert a file, check parent mtime
	_, err = pool.Exec(ctx, `INSERT INTO tigerfs."mig_mtime" (id, filename, filetype) VALUES (gen_random_uuid(), 'testdir', 'directory')`)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	var dirMtimeBefore time.Time
	err = pool.QueryRow(ctx, `SELECT modified_at FROM tigerfs."mig_mtime" WHERE filename = 'testdir' AND filetype = 'directory'`).Scan(&dirMtimeBefore)
	require.NoError(t, err)

	var dirID string
	err = pool.QueryRow(ctx, `SELECT id FROM tigerfs."mig_mtime" WHERE filename = 'testdir' AND filetype = 'directory'`).Scan(&dirID)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	_, err = pool.Exec(ctx, `INSERT INTO tigerfs."mig_mtime" (id, parent_id, filename, filetype, body) VALUES (gen_random_uuid(), $1, 'test.md', 'file', 'hello')`, dirID)
	require.NoError(t, err)

	var dirMtimeAfter time.Time
	err = pool.QueryRow(ctx, `SELECT modified_at FROM tigerfs."mig_mtime" WHERE filename = 'testdir' AND filetype = 'directory'`).Scan(&dirMtimeAfter)
	require.NoError(t, err)
	assert.True(t, dirMtimeAfter.After(dirMtimeBefore), "dir mtime should increase after child insert")

	// Step 6: Idempotency -- second describe should find nothing
	idemp := cmd.BuildMigrateCmd()
	var idempBuf bytes.Buffer
	idemp.SetOut(&idempBuf)
	idemp.SetErr(&idempBuf)
	idemp.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err = idemp.Execute()
	require.NoError(t, err)
	assert.Contains(t, idempBuf.String(), "No pending migrations")

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cp, err := pgxpool.New(cleanupCtx, result.ConnStr)
		if err != nil {
			return
		}
		defer cp.Close()
		cp.Exec(cleanupCtx, `DROP TABLE IF EXISTS tigerfs."mig_mtime" CASCADE`)
		cp.Exec(cleanupCtx, `DROP VIEW IF EXISTS "mig_mtime" CASCADE`)
	})
}

// TestSynth_MigrateParentDirMtimeTrigger_NotNeeded tests that workspaces
// created with the trigger already present don't show up in migration detect.
func TestSynth_MigrateParentDirMtimeTrigger_NotNeeded(t *testing.T) {
	require.NoError(t, config.Init(), "config.Init should succeed")

	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "mig_mtime_new")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Create a workspace with the full build path (includes trigger)
	fsErr := ops.WriteFile(ctx, "/.build/mig_mtime_new", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Describe should find no pending migrations
	descCmd := cmd.BuildMigrateCmd()
	var descBuf bytes.Buffer
	descCmd.SetOut(&descBuf)
	descCmd.SetErr(&descBuf)
	descCmd.SetArgs([]string{result.ConnStr, "--describe", "--insecure-no-ssl"})
	err := descCmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, descBuf.String(), "No pending migrations")
}
