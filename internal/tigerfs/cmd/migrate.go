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
var migrations = []migration{
	moveBackingTablesMigration(),
}

// moveBackingTablesMigration returns the migration that moves synth backing tables
// from _name in the user schema to name in the tigerfs schema, then creates a
// view in the user schema pointing to the new location.
func moveBackingTablesMigration() migration {
	return migration{
		Name:    "move-backing-tables",
		Summary: "Move synth backing tables from _name in user schema to name in tigerfs schema",
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

// BuildMigrateCmd creates the migrate command. Exported for integration testing.
//
// The migrate command detects and runs pending database migrations. It supports
// three modes: --describe (list pending), --dry-run (show SQL), or execute.
func BuildMigrateCmd() *cobra.Command {
	var describe bool
	var dryRun bool
	var schemaFlag string

	cmd := &cobra.Command{
		Use:   "migrate [CONNECTION]",
		Short: "Run pending database migrations",
		Long: `Detect and run pending database migrations.

Migrations are named actions that update database structures for compatibility
with newer TigerFS versions. Each migration detects whether it's needed and
generates the appropriate SQL.

Examples:
  # List pending migrations
  tigerfs migrate postgres://localhost/mydb --describe

  # Preview SQL without executing
  tigerfs migrate postgres://localhost/mydb --dry-run

  # Run all pending migrations
  tigerfs migrate postgres://localhost/mydb`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
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
				fmt.Fprintf(w, "  Migrated %d items\n", len(items))
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
	return cmd
}
