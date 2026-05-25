// Package cmd provides CLI commands for TigerFS.
//
// This file implements the migrate command, a general-purpose migration framework.
// Migrations are named actions that detect whether they're needed, generate SQL,
// and optionally execute it. Each migration has a Detect function (returns items
// needing migration) and a Plan function (generates SQL for those items).
package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// migration defines a named, self-detecting migration action.
type migration struct {
	// Name is a short identifier (e.g., "move-backing-tables").
	Name string
	// Summary is a human-readable description of what this migration does.
	Summary string
	// Detect returns the items (e.g., table names) that need migrating, or nil if nothing to do.
	Detect func(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error)
	// Plan returns SQL statements needed to migrate the detected items.
	Plan func(ctx context.Context, pool *pgxpool.Pool, schema string, items []string) ([]string, error)
}

// migrations is the ordered list of all registered migrations.
// Order matters: earlier migrations run first.
var migrations = []migration{
	moveBackingTablesMigration(),
	addParentPointerMigration(),
	addParentDirMtimeTriggerMigration(),
}

// moveBackingTablesMigration returns the migration that moves synth backing tables
// from _name in the user schema to name in the tigerfs schema, then creates a
// view in the user schema pointing to the new location.
func moveBackingTablesMigration() migration {
	return migration{
		Name:    "move-backing-tables",
		Summary: "Move backing tables from user schema to tigerfs schema",
		Detect: func(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error) {
			// Get all tables in user schema
			rows, err := pool.Query(ctx,
				"SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename", schema)
			if err != nil {
				return nil, fmt.Errorf("failed to list tables: %w", err)
			}
			defer rows.Close()

			var tables []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					return nil, fmt.Errorf("failed to scan table name: %w", err)
				}
				tables = append(tables, name)
			}
			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate tables: %w", err)
			}

			// Get all view comments in user schema
			commentRows, err := pool.Query(ctx,
				`SELECT c.relname, d.description
				 FROM pg_class c
				 JOIN pg_namespace n ON n.oid = c.relnamespace
				 LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
				 WHERE n.nspname = $1 AND c.relkind = 'v'`, schema)
			if err != nil {
				return nil, fmt.Errorf("failed to list view comments: %w", err)
			}
			defer commentRows.Close()

			comments := make(map[string]string)
			for commentRows.Next() {
				var name string
				var desc *string
				if err := commentRows.Scan(&name, &desc); err != nil {
					return nil, fmt.Errorf("failed to scan view comment: %w", err)
				}
				if desc != nil {
					comments[name] = *desc
				}
			}
			if err := commentRows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate view comments: %w", err)
			}

			var items []string
			for _, tbl := range tables {
				if !strings.HasPrefix(tbl, "_") {
					continue
				}
				viewName := tbl[1:] // strip leading underscore
				if viewName == "" {
					continue
				}

				// Check if the matching view has a tigerfs comment
				comment, ok := comments[viewName]
				if !ok || comment == "" {
					continue
				}
				features := synth.DetectFeaturesFromComment(comment)
				if features.Format == synth.FormatNative && !features.History {
					continue
				}

				// Check if already migrated (table exists in tigerfs schema)
				var exists bool
				err := pool.QueryRow(ctx,
					`SELECT EXISTS(
						SELECT 1 FROM pg_tables
						WHERE schemaname = $1 AND tablename = $2
					)`, synth.TigerFSSchema, viewName).Scan(&exists)
				if err != nil {
					return nil, fmt.Errorf("failed to check migration status for %s: %w", tbl, err)
				}
				if exists {
					continue // already migrated
				}

				items = append(items, tbl)
			}
			return items, nil
		},
		Plan: func(ctx context.Context, pool *pgxpool.Pool, schema string, items []string) ([]string, error) {
			var stmts []string

			// Create tigerfs schema (idempotent)
			stmts = append(stmts, fmt.Sprintf(
				`CREATE SCHEMA IF NOT EXISTS %s`, db.QuoteIdent(synth.TigerFSSchema)))

			for _, oldName := range items {
				newName := oldName[1:] // strip leading underscore

				// Move table to tigerfs schema
				stmts = append(stmts, fmt.Sprintf(
					`ALTER TABLE %s SET SCHEMA %s`,
					db.QuoteTable(schema, oldName),
					db.QuoteIdent(synth.TigerFSSchema)))

				// Rename table from _name to name
				stmts = append(stmts, fmt.Sprintf(
					`ALTER TABLE %s RENAME TO %s`,
					db.QuoteTable(synth.TigerFSSchema, oldName),
					db.QuoteIdent(newName)))

				// Drop existing view (it references the old table location)
				stmts = append(stmts, fmt.Sprintf(
					`DROP VIEW IF EXISTS %s`,
					db.QuoteTable(schema, newName)))

				// Create view pointing to new location
				stmts = append(stmts, synth.GenerateViewSQL(schema, newName, synth.TigerFSSchema, newName))

				// Check if history table exists
				historyOldName := oldName + "_history"
				var historyExists bool
				err := pool.QueryRow(ctx,
					`SELECT EXISTS(
						SELECT 1 FROM pg_tables
						WHERE schemaname = $1 AND tablename = $2
					)`, schema, historyOldName).Scan(&historyExists)
				if err != nil {
					return nil, fmt.Errorf("failed to check history table for %s: %w", oldName, err)
				}

				if historyExists {
					historyNewName := newName + "_history"

					// Move history table to tigerfs schema
					stmts = append(stmts, fmt.Sprintf(
						`ALTER TABLE %s SET SCHEMA %s`,
						db.QuoteTable(schema, historyOldName),
						db.QuoteIdent(synth.TigerFSSchema)))

					// Rename history table
					stmts = append(stmts, fmt.Sprintf(
						`ALTER TABLE %s RENAME TO %s`,
						db.QuoteTable(synth.TigerFSSchema, historyOldName),
						db.QuoteIdent(historyNewName)))
				}
			}
			return stmts, nil
		},
	}
}

// addParentPointerMigration returns the migration that converts synth apps from
// path-encoded filenames (ADR-011) to the parent-pointer directory model (ADR-017).
// For each app, it adds parent_id, populates it from existing path hierarchy,
// strips filenames to leaf names, updates constraints/indexes, migrates history
// and log table column names, and recreates the BEFORE trigger.
func addParentPointerMigration() migration {
	return migration{
		Name:    "relational-directories",
		Summary: "Upgrade directory structure for improved performance and undo support",
		Detect: func(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error) {
			// Find synth apps in tigerfs schema that DON'T have parent_id yet
			rows, err := pool.Query(ctx,
				`SELECT c.relname, d.description
				 FROM pg_class c
				 JOIN pg_namespace n ON n.oid = c.relnamespace
				 LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
				 WHERE n.nspname = $1 AND c.relkind = 'v'
				   AND d.description LIKE 'tigerfs:%'`, schema)
			if err != nil {
				return nil, fmt.Errorf("failed to list synth views: %w", err)
			}
			defer rows.Close()

			var items []string
			for rows.Next() {
				var viewName string
				var comment *string
				if err := rows.Scan(&viewName, &comment); err != nil {
					return nil, fmt.Errorf("failed to scan view: %w", err)
				}

				// Check if backing table in tigerfs schema has parent_id
				var hasParentID bool
				err := pool.QueryRow(ctx,
					`SELECT EXISTS(
						SELECT 1 FROM information_schema.columns
						WHERE table_schema = 'tigerfs' AND table_name = $1
						  AND column_name = 'parent_id'
					)`, viewName).Scan(&hasParentID)
				if err != nil {
					return nil, fmt.Errorf("failed to check parent_id for %s: %w", viewName, err)
				}

				// Also check the table actually exists in tigerfs schema
				var tableExists bool
				err = pool.QueryRow(ctx,
					`SELECT EXISTS(
						SELECT 1 FROM pg_tables
						WHERE schemaname = 'tigerfs' AND tablename = $1
					)`, viewName).Scan(&tableExists)
				if err != nil {
					return nil, fmt.Errorf("failed to check table existence for %s: %w", viewName, err)
				}

				if tableExists && !hasParentID {
					items = append(items, viewName)
				}
			}
			return items, nil
		},
		Plan: func(ctx context.Context, pool *pgxpool.Pool, schema string, items []string) ([]string, error) {
			var stmts []string

			// Create resolve_path function (idempotent)
			stmts = append(stmts, synth.GenerateResolvePathSQL())

			for _, appName := range items {
				qt := fmt.Sprintf("%s.%s", db.QuoteIdent(synth.TigerFSSchema), db.QuoteIdent(appName))

				// --- Source table ---

				// Switch id column from UUIDv4 to UUIDv7 (time-ordered)
				stmts = append(stmts, fmt.Sprintf(
					`ALTER TABLE %s ALTER COLUMN id SET DEFAULT uuidv7()`, qt))

				// Add parent_id column
				stmts = append(stmts, fmt.Sprintf(
					`ALTER TABLE %s ADD COLUMN parent_id UUID`, qt))

				// Populate parent_id from path hierarchy (PL/pgSQL DO block).
				// Processes rows shallowest first; looks up parent by old full-path filename.
				stmts = append(stmts, fmt.Sprintf(`DO $migrate$
DECLARE
    r RECORD;
    parts TEXT[];
    parent_path TEXT;
    found_parent_id UUID;
BEGIN
    FOR r IN SELECT id, filename FROM %s
             WHERE filename LIKE '%%/%%'
             ORDER BY length(filename) - length(replace(filename, '/', ''))
    LOOP
        parts := string_to_array(r.filename, '/');
        parent_path := array_to_string(parts[1:array_length(parts,1)-1], '/');
        IF parent_path = '' OR parent_path IS NULL THEN
            SELECT id INTO found_parent_id FROM %s
            WHERE filename = parts[1] AND filetype = 'directory' LIMIT 1;
        ELSE
            SELECT id INTO found_parent_id FROM %s
            WHERE filename = parent_path AND filetype = 'directory' LIMIT 1;
        END IF;
        UPDATE %s SET parent_id = found_parent_id WHERE id = r.id;
    END LOOP;
END $migrate$`, qt, qt, qt, qt))

				// Strip filenames to leaf names
				stmts = append(stmts, fmt.Sprintf(
					`UPDATE %s SET filename = split_part(filename, '/', array_length(string_to_array(filename, '/'), 1)) WHERE filename LIKE '%%/%%'`, qt))

				// Add FK constraint
				stmts = append(stmts, fmt.Sprintf(
					`ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (parent_id) REFERENCES %s(id) DEFERRABLE INITIALLY IMMEDIATE`,
					qt, db.QuoteIdent("fk_"+appName+"_parent"), qt))

				// Find and drop old UNIQUE constraint, add new one
				var oldConstraint *string
				_ = pool.QueryRow(ctx,
					`SELECT conname FROM pg_constraint
					 WHERE conrelid = $1::regclass AND contype = 'u'
					   AND array_length(conkey, 1) = 2 LIMIT 1`,
					fmt.Sprintf("tigerfs.%s", appName)).Scan(&oldConstraint)
				if oldConstraint != nil {
					stmts = append(stmts, fmt.Sprintf(
						`ALTER TABLE %s DROP CONSTRAINT %s`, qt, db.QuoteIdent(*oldConstraint)))
				}
				stmts = append(stmts, fmt.Sprintf(
					`ALTER TABLE %s ADD CONSTRAINT %s UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE`,
					qt, db.QuoteIdent("uq_"+appName+"_parent_filename")))

				// Parent index
				stmts = append(stmts, fmt.Sprintf(
					`CREATE INDEX IF NOT EXISTS %s ON %s (parent_id, filename)`,
					db.QuoteIdent("idx_"+appName+"_parent"), qt))

				// Recreate view to pick up the new parent_id column.
				// PostgreSQL views with SELECT * snapshot columns at creation time;
				// ALTER TABLE ADD COLUMN does NOT update existing views.
				stmts = append(stmts, fmt.Sprintf(
					`DROP VIEW IF EXISTS %s`, db.QuoteTable(schema, appName)))
				stmts = append(stmts, synth.GenerateViewSQL(schema, appName, synth.TigerFSSchema, appName))
				// Preserve the view comment
				var viewComment string
				_ = pool.QueryRow(ctx,
					`SELECT obj_description(c.oid, 'pg_class')
					 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
					 WHERE n.nspname = $1 AND c.relname = $2`, schema, appName).Scan(&viewComment)
				if viewComment != "" {
					stmts = append(stmts, fmt.Sprintf(
						`COMMENT ON VIEW %s IS '%s'`,
						db.QuoteTable(schema, appName), viewComment))
				}

				// --- History table (if exists) ---
				var hasHistory bool
				_ = pool.QueryRow(ctx,
					`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'tigerfs' AND tablename = $1)`,
					appName+"_history").Scan(&hasHistory)

				if hasHistory {
					htQt := fmt.Sprintf("%s.%s", db.QuoteIdent(synth.TigerFSSchema), db.QuoteIdent(appName+"_history"))

					stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS parent_id UUID`, htQt))

					// Rename columns (check existence first in Plan since we can query)
					var hasOldID, hasOldHistID, hasOldOp bool
					_ = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name=$1 AND column_name='id')`, appName+"_history").Scan(&hasOldID)
					_ = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name=$1 AND column_name='_history_id')`, appName+"_history").Scan(&hasOldHistID)
					_ = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name=$1 AND column_name='_operation')`, appName+"_history").Scan(&hasOldOp)

					if hasOldID {
						stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN id TO file_id`, htQt))
					}
					if hasOldHistID {
						stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN _history_id TO version_id`, htQt))
					}
					if hasOldOp {
						stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN _operation TO operation`, htQt))
					}

					// Populate parent_id from source table
					stmts = append(stmts, fmt.Sprintf(
						`UPDATE %s h SET parent_id = (SELECT parent_id FROM %s s WHERE s.id = h.file_id)`,
						htQt, qt))

					// Strip history filenames to leaf names
					stmts = append(stmts, fmt.Sprintf(
						`UPDATE %s SET filename = split_part(filename, '/', array_length(string_to_array(filename, '/'), 1)) WHERE filename LIKE '%%/%%'`, htQt))

					// Migrate operation values: UPDATE -> edit, DELETE -> delete
					stmts = append(stmts, fmt.Sprintf(
						`UPDATE %s SET operation = CASE operation WHEN 'UPDATE' THEN 'edit' WHEN 'DELETE' THEN 'delete' ELSE operation END WHERE operation IN ('UPDATE', 'DELETE')`, htQt))

					// Recreate trigger with new column names and operation logic
					stmts = append(stmts, fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`,
						db.QuoteIdent("trg_"+appName+"_history_archive"), qt))
					stmts = append(stmts, fmt.Sprintf(`DROP FUNCTION IF EXISTS %s.%s()`,
						db.QuoteIdent(synth.TigerFSSchema), db.QuoteIdent("archive_"+appName+"_history")))

					// Determine format from view comment
					var comment string
					_ = pool.QueryRow(ctx,
						`SELECT obj_description(c.oid, 'pg_class')
						 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
						 WHERE n.nspname = $1 AND c.relname = $2`, schema, appName).Scan(&comment)

					features := synth.DetectFeaturesFromComment(comment)
					historyStmts := synth.GenerateHistorySQL(schema, appName, features.Format)
					// Only take the trigger function and trigger (indices 3 and 4)
					if len(historyStmts) >= 5 {
						stmts = append(stmts, historyStmts[3]) // archive function
						stmts = append(stmts, historyStmts[4]) // trigger
					}
				}

				// --- Log table (if exists) ---
				var hasLog bool
				_ = pool.QueryRow(ctx,
					`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'tigerfs' AND tablename = $1)`,
					appName+"_log").Scan(&hasLog)

				if hasLog {
					logQt := fmt.Sprintf("%s.%s", db.QuoteIdent(synth.TigerFSSchema), db.QuoteIdent(appName+"_log"))

					var hasOldHistoryID bool
					_ = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='tigerfs' AND table_name=$1 AND column_name='history_id')`, appName+"_log").Scan(&hasOldHistoryID)
					if hasOldHistoryID {
						stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN history_id TO version_id`, logQt))
					}

					// Drop old CHECK, rename values, THEN add new CHECK (order matters:
					// can't add new constraint while old values still exist)
					stmts = append(stmts, fmt.Sprintf(
						`ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s`,
						logQt, db.QuoteIdent(appName+"_log_type_check")))
					stmts = append(stmts, fmt.Sprintf(
						`UPDATE %s SET type = CASE type WHEN 'insert' THEN 'create' WHEN 'update' THEN 'edit' ELSE type END`,
						logQt))
					stmts = append(stmts, fmt.Sprintf(
						`ALTER TABLE %s ADD CONSTRAINT %s CHECK (type IN ('create', 'edit', 'rename', 'delete', 'undo'))`,
						logQt, db.QuoteIdent(appName+"_log_type_check")))
				}

				// --- Metadata table + boundary marker ---
				// 0.6 workspaces don't have a metadata table; create it on
				// migration. Then insert the history-format-migration row at
				// the *end* of the migration (its UUIDv7 entry_id is therefore
				// strictly newer than every pre-migration log_id, which is
				// what makes "target < entry_id" the boundary check). The
				// INSERT is guarded by WHERE NOT EXISTS so re-running the
				// migration leaves exactly one marker row.
				metadataTableName := appName + synth.MetadataTableSuffix
				metadataQt := fmt.Sprintf("%s.%s",
					db.QuoteIdent(synth.TigerFSSchema), db.QuoteIdent(metadataTableName))
				metadataIndexName := db.QuoteIdent("idx_" + metadataTableName + "_subject")

				stmts = append(stmts, fmt.Sprintf(
					`CREATE TABLE IF NOT EXISTS %s (
    entry_id    UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    subject     TEXT NOT NULL,
    user_id     TEXT,
    description TEXT,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb
)`, metadataQt))

				stmts = append(stmts, fmt.Sprintf(
					`CREATE INDEX IF NOT EXISTS %s ON %s (subject, entry_id)`,
					metadataIndexName, metadataQt))

				const boundaryDescription = "Pre-0.7 history is read-only after the v0.7 format upgrade. " +
					"Use .log/<id>/before to view content or .history/ to browse old versions."
				const boundaryPayload = `{"from":"0.6","to":"0.7","reason":"parent-pointer model"}`

				stmts = append(stmts, fmt.Sprintf(
					`INSERT INTO %s (subject, description, payload)
SELECT '%s', '%s', '%s'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM %s WHERE subject = '%s')`,
					metadataQt,
					synth.SubjectHistoryFormatMigration, boundaryDescription, boundaryPayload,
					metadataQt, synth.SubjectHistoryFormatMigration))
			}
			return stmts, nil
		},
	}
}

// addParentDirMtimeTriggerMigration returns the migration that adds a trigger to
// update the parent directory's modified_at when children are added, removed, or
// moved. This gives directories POSIX-correct mtime semantics and ensures the NFS
// client re-fetches directory listings after changes.
func addParentDirMtimeTriggerMigration() migration {
	return migration{
		Name:    "parent-dir-mtime-trigger",
		Summary: "Add trigger to update parent directory mtime when children change",
		Detect: func(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error) {
			// Find synth apps with parent_id (parent-pointer model) but missing
			// the parent mtime trigger.
			rows, err := pool.Query(ctx,
				`SELECT c.relname
				 FROM pg_class c
				 JOIN pg_namespace n ON n.oid = c.relnamespace
				 LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
				 WHERE n.nspname = $1 AND c.relkind = 'v'
				   AND d.description LIKE 'tigerfs:%'`, schema)
			if err != nil {
				return nil, fmt.Errorf("failed to list synth views: %w", err)
			}
			defer rows.Close()

			var items []string
			for rows.Next() {
				var viewName string
				if err := rows.Scan(&viewName); err != nil {
					return nil, fmt.Errorf("failed to scan view: %w", err)
				}

				// Check prerequisites: backing table exists in tigerfs schema with parent_id
				var hasParentID bool
				err := pool.QueryRow(ctx,
					`SELECT EXISTS(
						SELECT 1 FROM information_schema.columns
						WHERE table_schema = 'tigerfs' AND table_name = $1
						  AND column_name = 'parent_id'
					)`, viewName).Scan(&hasParentID)
				if err != nil {
					return nil, fmt.Errorf("failed to check parent_id for %s: %w", viewName, err)
				}
				if !hasParentID {
					continue
				}

				// Check if trigger already exists
				triggerName := "trg_" + viewName + "_parent_mtime"
				var hasTrigger bool
				err = pool.QueryRow(ctx,
					`SELECT EXISTS(
						SELECT 1 FROM pg_trigger t
						JOIN pg_class c ON c.oid = t.tgrelid
						JOIN pg_namespace n ON n.oid = c.relnamespace
						WHERE t.tgname = $1 AND c.relname = $2 AND n.nspname = 'tigerfs'
					)`, triggerName, viewName).Scan(&hasTrigger)
				if err != nil {
					return nil, fmt.Errorf("failed to check trigger for %s: %w", viewName, err)
				}

				if !hasTrigger {
					items = append(items, viewName)
				}
			}
			return items, nil
		},
		Plan: func(ctx context.Context, pool *pgxpool.Pool, schema string, items []string) ([]string, error) {
			var stmts []string
			for _, appName := range items {
				triggerStmts := synth.GenerateParentDirMtimeTriggerSQL(schema, appName)
				stmts = append(stmts, triggerStmts...)
			}
			return stmts, nil
		},
	}
}

// BuildMigrateCmd creates the migrate command. Exported for integration testing.
//
// The migrate command detects and runs pending database migrations. It supports
// three modes: --describe (list pending), --dry-run (show SQL), or execute.
func BuildMigrateCmd() *cobra.Command {
	var describe bool
	var dryRun bool
	var insecureNoSSL bool
	var schemaFlag string

	cmd := &cobra.Command{
		Use:   "migrate [CONNECTION]",
		Short: "Run pending database migrations",
		Long: `Detect and run pending database migrations.

Migrations are named actions that update database structures for compatibility
with newer TigerFS versions. Each migration detects whether it's needed and
generates the appropriate SQL.

CONNECTION uses a prefix to select the backend:
  tiger:ID    Tiger Cloud service by ID
  ghost:ID    Ghost database by ID
  postgres:// Direct connection string

Examples:
  # List pending migrations
  tigerfs migrate tiger:abcde12345 --describe

  # Preview SQL without executing
  tigerfs migrate postgres://user@host/db --dry-run

  # Run all pending migrations
  tigerfs migrate tiger:abcde12345`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}
			if insecureNoSSL {
				cfg.InsecureNoSSL = true
			}

			var explicitConnStr string
			if len(args) > 0 {
				explicitConnStr = args[0]
			}

			connStr, err := db.ResolveConnectionString(ctx, cfg, explicitConnStr)
			if err != nil {
				return err
			}

			client, err := db.NewClient(ctx, cfg, connStr)
			if err != nil {
				return fmt.Errorf("connection failed: %w", err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					logging.Warn("Failed to close client", zap.Error(err))
				}
			}()

			// Resolve schema
			schema := schemaFlag
			if schema == "" {
				err = client.Pool().QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
				if err != nil {
					return fmt.Errorf("failed to get current schema: %w", err)
				}
			}

			w := cmd.OutOrStdout()
			pool := client.Pool()
			anyPending := false

			for _, m := range migrations {
				items, err := m.Detect(ctx, pool, schema)
				if err != nil {
					return fmt.Errorf("migration %s: detection failed: %w", m.Name, err)
				}
				if len(items) == 0 {
					continue
				}
				anyPending = true

				if describe {
					fmt.Fprintf(w, "%s: %s\n", m.Name, m.Summary)
					for _, item := range items {
						fmt.Fprintf(w, "  - %s\n", item)
					}
					continue
				}

				stmts, err := m.Plan(ctx, pool, schema, items)
				if err != nil {
					return fmt.Errorf("migration %s: planning failed: %w", m.Name, err)
				}

				if dryRun {
					fmt.Fprintf(w, "-- Migration: %s\n", m.Name)
					for _, stmt := range stmts {
						fmt.Fprintf(w, "%s;\n", stmt)
					}
					continue
				}

				// Execute in transaction
				fmt.Fprintf(w, "Running migration: %s\n", m.Name)
				tx, err := pool.Begin(ctx)
				if err != nil {
					return fmt.Errorf("migration %s: failed to begin transaction: %w", m.Name, err)
				}
				for _, stmt := range stmts {
					if _, err := tx.Exec(ctx, stmt); err != nil {
						tx.Rollback(ctx)
						return fmt.Errorf("migration %s: failed to execute SQL: %w\nSQL: %s", m.Name, err, stmt)
					}
				}
				if err := tx.Commit(ctx); err != nil {
					return fmt.Errorf("migration %s: failed to commit: %w", m.Name, err)
				}
				fmt.Fprintf(w, "  Migrated %d views\n", len(items))
			}

			if !anyPending {
				fmt.Fprintln(w, "No pending migrations.")
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&describe, "describe", false, "List pending migrations without executing")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show SQL that would be executed")
	cmd.Flags().StringVar(&schemaFlag, "schema", "", "Schema to migrate (default: database search_path)")
	cmd.Flags().BoolVar(&insecureNoSSL, "insecure-no-ssl", false, "Allow non-TLS connections to remote databases (insecure)")
	return cmd
}
