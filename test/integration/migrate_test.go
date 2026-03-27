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
