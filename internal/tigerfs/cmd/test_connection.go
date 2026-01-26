// Package cmd provides CLI commands for TigerFS.
//
// This file implements the test-connection command which verifies database
// connectivity and displays information about the database environment.
package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// buildTestConnectionCmd creates the test-connection command.
//
// The test-connection command verifies database connectivity without mounting
// a filesystem. It displays:
//   - PostgreSQL version
//   - Current database and user
//   - Number of accessible schemas and tables
//
// This is useful for troubleshooting connection issues before attempting a mount.
func buildTestConnectionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "test-connection [CONNECTION]",
		Short: "Test database connectivity",
		Long: `Test database connectivity and display environment information.

Verifies credentials and shows PostgreSQL version and accessible tables.
Useful for troubleshooting before mounting.

Connection sources (in order of precedence):
  1. Explicit connection string argument
  2. Tiger Cloud service (TIGER_SERVICE_ID)
  3. PostgreSQL config (PGHOST, PGUSER, etc.)

Examples:
  # Test connection using connection string
  tigerfs test-connection postgres://localhost/mydb

  # Test connection using Tiger Cloud service
  # export TIGER_SERVICE_ID=<your-service-id>
  tigerfs test-connection

  # Test connection using PostgreSQL env vars
  export PGHOST=localhost PGDATABASE=mydb
  tigerfs test-connection`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// Create context with timeout for all operations
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Load config
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Resolve connection string from args, Tiger Cloud, or PG config
			var explicitConnStr string
			if len(args) > 0 {
				explicitConnStr = args[0]
			}

			connStr, err := db.ResolveConnectionString(ctx, cfg, explicitConnStr)
			if err != nil {
				return err
			}

			logging.Debug("Testing database connection", zap.String("connection", connStr))

			// Attempt connection (this also pings the database)
			client, err := db.NewClient(ctx, cfg, connStr)
			if err != nil {
				return fmt.Errorf("connection failed: %w", err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					logging.Warn("Failed to close client", zap.Error(err))
				}
			}()

			// Display connection information
			return displayConnectionInfo(ctx, cmd.OutOrStdout(), client)
		},
	}
}

// displayConnectionInfo queries and displays database environment details.
//
// Parameters:
//   - ctx: Context for query timeout
//   - w: Writer for output (typically stdout)
//   - client: Connected database client
//
// Queries executed:
//   - SELECT version() - PostgreSQL version string
//   - current_database(), current_user - Connection identity
//   - information_schema.schemata - Accessible schema count
//   - information_schema.tables - Accessible table count
func displayConnectionInfo(ctx context.Context, w io.Writer, client *db.Client) error {
	pool := client.Pool()

	// Query PostgreSQL version
	var version string
	err := pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to query version: %w", err)
	}
	fmt.Fprintf(w, "Connected to %s\n", extractPgVersion(version))

	// Query current database and user
	var database, user string
	err = pool.QueryRow(ctx, "SELECT current_database(), current_user").Scan(&database, &user)
	if err != nil {
		return fmt.Errorf("failed to query database info: %w", err)
	}
	fmt.Fprintf(w, "Database:   %s\n", database)
	fmt.Fprintf(w, "User:       %s\n", user)

	// Query accessible schemas count.
	// Excludes: pg_* (PostgreSQL system), information_schema, and _* (internal schemas
	// like _timescaledb_internal, _timescaledb_catalog, etc.)
	var schemaCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.schemata
		WHERE schema_name NOT LIKE 'pg_%'
		  AND schema_name NOT LIKE '\_%' ESCAPE '\'
		  AND schema_name != 'information_schema'
	`).Scan(&schemaCount)
	if err != nil {
		return fmt.Errorf("failed to query schema count: %w", err)
	}
	fmt.Fprintf(w, "Schemas:    %d\n", schemaCount)

	// Query accessible tables count (user tables only).
	// Same exclusion logic as schemas, plus filtering to BASE TABLE only.
	var tableCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema NOT LIKE 'pg_%'
		  AND table_schema NOT LIKE '\_%' ESCAPE '\'
		  AND table_schema != 'information_schema'
		  AND table_type = 'BASE TABLE'
	`).Scan(&tableCount)
	if err != nil {
		return fmt.Errorf("failed to query table count: %w", err)
	}
	fmt.Fprintf(w, "Tables:     %d\n", tableCount)

	return nil
}

// extractPgVersion extracts the PostgreSQL version from the full version string.
//
// Input example: "PostgreSQL 17.2 on x86_64-pc-linux-gnu, compiled by gcc..."
// Output example: "PostgreSQL 17.2"
//
// Returns the original string if parsing fails.
func extractPgVersion(fullVersion string) string {
	// Find the first comma or parenthesis to truncate
	for i, c := range fullVersion {
		if c == ',' || c == '(' {
			if i > 0 {
				return fullVersion[:i]
			}
			break
		}
	}
	// Return first 50 chars max if no delimiter found
	if len(fullVersion) > 50 {
		return fullVersion[:50] + "..."
	}
	return fullVersion
}
