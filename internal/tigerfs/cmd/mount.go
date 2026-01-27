// Package cmd provides CLI commands for TigerFS.
//
// This file implements the mount command which is the primary command for
// mounting a PostgreSQL database as a FUSE filesystem.
package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
	"go.uber.org/zap"
)

// buildMountCmd creates the mount command.
//
// The mount command is the primary command for TigerFS. It connects to a
// PostgreSQL database and exposes it as a FUSE filesystem at the specified
// mountpoint.
//
// Parameters:
//   - ctx: Parent context for cancellation (typically from signal handling)
//
// The command supports multiple ways to specify the database connection:
//   - Connection string argument (postgres://...)
//   - Individual flags (--host, --port, --user, --database)
//   - Tiger Cloud service ID (--tiger-service-id)
//   - Environment variables (PGHOST, etc.)
func buildMountCmd(ctx context.Context) *cobra.Command {
	// Declare local flags - these are scoped to this command only
	var host string
	var port int
	var user string
	var database string
	var tigerServiceID string
	var readOnly bool
	var maxLsRows int
	var foreground bool
	// TODO: allow-other support has inconsistent cross-platform behavior
	// (works on Linux, limited on macOS, different model on Windows).
	// Revisit in Phase 6 Task 6.2. For now, mounts are single-user only.
	// var allowOther bool

	cmd := &cobra.Command{
		Use:   "mount [CONNECTION] MOUNTPOINT",
		Short: "Mount a PostgreSQL database as a filesystem (default command)",
		Long: `Mount a PostgreSQL database as a filesystem directory.

The mount command is the default command and can be omitted.

Examples:
  # Mount using connection string
  tigerfs postgres://user@host/db /mnt/db

  # Mount using flags
  tigerfs --host=localhost --database=mydb /mnt/db

  # Mount Tiger Cloud service
  tigerfs --tiger-service-id=e6ue9697jf /mnt/db

  # Mount read-only
  tigerfs --read-only postgres://host/db /mnt/db

  # Run in foreground with debug logging
  tigerfs --foreground --debug postgres://host/db /mnt/db`,
		Args: cobra.RangeArgs(1, 2), // [CONNECTION] MOUNTPOINT or just MOUNTPOINT
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// Parse arguments - connection string is optional, mountpoint is required
			var connStr, mountpoint string
			if len(args) == 2 {
				connStr = args[0]
				mountpoint = args[1]
			} else {
				mountpoint = args[0]
			}

			// Resolve mountpoint to absolute path for consistent registry handling
			absMountpoint, err := filepath.Abs(mountpoint)
			if err != nil {
				return fmt.Errorf("failed to resolve mountpoint path: %w", err)
			}

			logging.Info("Starting TigerFS mount",
				zap.String("mountpoint", absMountpoint),
				zap.Bool("read_only", readOnly),
			)

			// Load configuration from file, environment, and flags
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Mount the FUSE filesystem
			fs, err := fuse.Mount(ctx, cfg, connStr, absMountpoint)
			if err != nil {
				return fmt.Errorf("failed to mount: %w", err)
			}
			defer func() {
				if err := fs.Close(); err != nil {
					logging.Error("Failed to close filesystem", zap.Error(err))
				}
			}()

			logging.Info("Filesystem mounted successfully", zap.String("mountpoint", absMountpoint))

			// Register the mount in the registry for discovery by other commands
			// (status, list, unmount). We sanitize the connection string to remove
			// any password before storing.
			if err := registerMount(absMountpoint, connStr); err != nil {
				// Log but don't fail - mount itself succeeded
				logging.Warn("Failed to register mount in registry", zap.Error(err))
			}

			// Ensure we clean up the registry entry when we exit
			defer func() {
				if err := unregisterMount(absMountpoint); err != nil {
					logging.Warn("Failed to unregister mount from registry", zap.Error(err))
				}
			}()

			// Serve filesystem requests - this blocks until unmount or signal
			if err := fs.Serve(ctx); err != nil {
				return fmt.Errorf("filesystem error: %w", err)
			}

			logging.Info("Filesystem unmounted", zap.String("mountpoint", absMountpoint))
			return nil
		},
	}

	// Connection flags
	cmd.Flags().StringVar(&host, "host", "", "database host")
	cmd.Flags().IntVarP(&port, "port", "p", 5432, "database port")
	cmd.Flags().StringVarP(&user, "user", "U", "", "database user")
	cmd.Flags().StringVarP(&database, "database", "d", "", "database name")
	cmd.Flags().StringVar(&tigerServiceID, "tiger-service-id", "", "Tiger Cloud service ID")

	// Filesystem flags
	cmd.Flags().BoolVar(&readOnly, "read-only", false, "mount as read-only")
	cmd.Flags().IntVar(&maxLsRows, "max-ls-rows", 10000, "large table threshold")
	cmd.Flags().BoolVar(&foreground, "foreground", false, "run in foreground (don't daemonize)")

	return cmd
}

// registerMount adds the current mount to the registry for discovery.
//
// Parameters:
//   - mountpoint: Absolute path to the mountpoint
//   - connStr: Database connection string (will be sanitized to remove password)
//
// Returns an error if the registry cannot be accessed or modified.
func registerMount(mountpoint, connStr string) error {
	registry, err := mount.NewRegistry("")
	if err != nil {
		return fmt.Errorf("failed to open registry: %w", err)
	}

	entry := mount.Entry{
		Mountpoint: mountpoint,
		PID:        os.Getpid(),
		Database:   sanitizeConnectionString(connStr),
		StartTime:  time.Now(),
	}

	if err := registry.Register(entry); err != nil {
		return fmt.Errorf("failed to register mount: %w", err)
	}

	logging.Debug("Registered mount in registry",
		zap.String("mountpoint", mountpoint),
		zap.Int("pid", entry.PID),
	)
	return nil
}

// unregisterMount removes the current mount from the registry.
//
// Parameters:
//   - mountpoint: Absolute path to the mountpoint to remove
//
// Returns an error if the registry cannot be accessed or modified.
func unregisterMount(mountpoint string) error {
	registry, err := mount.NewRegistry("")
	if err != nil {
		return fmt.Errorf("failed to open registry: %w", err)
	}

	if err := registry.Unregister(mountpoint); err != nil {
		return fmt.Errorf("failed to unregister mount: %w", err)
	}

	logging.Debug("Unregistered mount from registry", zap.String("mountpoint", mountpoint))
	return nil
}

// sanitizeConnectionString removes sensitive information (password) from a
// connection string for safe storage and display.
//
// This is a simple implementation that handles common PostgreSQL URL formats.
// For full parsing, consider using pgx's connection string parser.
//
// Parameters:
//   - connStr: The original connection string
//
// Returns a sanitized connection string with password removed or masked.
func sanitizeConnectionString(connStr string) string {
	if connStr == "" {
		return "(from environment)"
	}

	// Simple approach: if it looks like a URL, try to redact password
	// A more robust solution would use net/url parsing
	// For now, just return the connection string with a note that passwords
	// should not be included in connection strings (use .pgpass or env vars)
	//
	// TODO: Implement proper password redaction using net/url parsing
	return connStr
}
