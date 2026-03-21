// Package cmd provides CLI commands for TigerFS.
//
// This file implements the mount command which is the primary command for
// mounting a PostgreSQL database as a filesystem. On Linux, FUSE is used;
// on macOS, NFS is used to avoid requiring third-party kernel extensions.
package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/backend"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
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
// CONNECTION uses the prefix scheme (see ADR-013):
//   - "tiger:ID" — Tiger Cloud service
//   - "ghost:ID" — Ghost database
//   - "postgres://..." — direct connection string
//   - Environment variables (PGHOST, etc.) when no CONNECTION argument
func buildMountCmd(ctx context.Context) *cobra.Command {
	var schema string
	var readOnly bool
	var maxLsRows int
	var foreground bool
	var noFilenameExtensions bool
	var queryTimeout time.Duration
	var dirFilterLimit int
	var legacyFuse bool
	var insecureNoSSL bool

	cmd := &cobra.Command{
		Use:   "mount [CONNECTION] [MOUNTPOINT]",
		Short: "Mount a PostgreSQL database as a filesystem (default command)",
		Long: `Mount a PostgreSQL database as a filesystem directory.

The mount command is the default command and can be omitted.

CONNECTION uses a prefix to select the backend:
  tiger:ID    Tiger Cloud service by ID
  ghost:ID    Ghost database by ID
  postgres:// Direct connection string

MOUNTPOINT is optional when CONNECTION has a backend prefix (tiger: or ghost:).
When omitted, the mountpoint defaults to <default_mount_dir>/<ID>.

Examples:
  # Mount Tiger Cloud service (auto-derived mountpoint)
  tigerfs mount tiger:e6ue9697jf

  # Mount Ghost database with explicit mountpoint
  tigerfs mount ghost:a2x6xoj0oz /mnt/db

  # Mount using connection string (mountpoint required)
  tigerfs mount postgres://user@host/db /mnt/db

  # Mount read-only
  tigerfs mount --read-only tiger:e6ue9697jf /mnt/db

  # Run in foreground with debug logging
  tigerfs mount --foreground --log-level debug postgres://host/db /mnt/db`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// Load configuration first — needed for DefaultMountDir during arg resolution.
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Parse arguments — mountpoint is optional when a backend prefix is used.
			connStr, mountpoint, err := resolveMountArgs(args, cfg.DefaultMountDir)
			if err != nil {
				return err
			}

			// Resolve backend prefix (tiger:ID, ghost:ID, or direct connection).
			var serviceID, cliBackend string
			if connStr != "" {
				b, id, err := backend.Resolve(connStr)
				if err != nil {
					return err
				}
				if b != nil {
					serviceID = id
					cliBackend = b.CLIName()
					logging.Info("Fetching connection string from backend",
						zap.String("backend", b.Name()),
						zap.String("service_id", serviceID))

					connStr, err = b.GetConnectionString(ctx, serviceID)
					if err != nil {
						return fmt.Errorf("failed to get connection string from %s: %w", b.Name(), err)
					}
					logging.Debug("Got connection string from backend")
				}
			}

			// Resolve mountpoint to absolute path for consistent registry handling.
			absMountpoint, err := filepath.Abs(mountpoint)
			if err != nil {
				return fmt.Errorf("failed to resolve mountpoint path: %w", err)
			}

			// Check if the mountpoint directory already exists before we (or the
			// platform mount layer) create it. If TigerFS creates the directory,
			// we clean it up on unmount. Pre-existing directories are left alone.
			_, statErr := os.Stat(absMountpoint)
			autoCreated := os.IsNotExist(statErr)

			// Ensure mountpoint directory exists (the NFS mount layer also calls
			// MkdirAll, but we do it here too for the FUSE path and to keep the
			// auto-created check above accurate).
			if autoCreated {
				if mkErr := os.MkdirAll(absMountpoint, 0755); mkErr != nil {
					return fmt.Errorf("failed to create mountpoint directory: %w", mkErr)
				}
			}

			logging.Info("Starting TigerFS mount",
				zap.String("mountpoint", absMountpoint),
				zap.Bool("read_only", readOnly),
			)

			if schema != "" {
				cfg.DefaultSchema = schema
			}
			if noFilenameExtensions {
				cfg.NoFilenameExtensions = true
			}
			if queryTimeout > 0 {
				cfg.QueryTimeout = queryTimeout
			}
			if dirFilterLimit > 0 {
				cfg.DirFilterLimit = dirFilterLimit
			}
			if legacyFuse {
				cfg.LegacyFuse = true
			}
			if insecureNoSSL {
				cfg.InsecureNoSSL = true
			}

			// Mount the filesystem using platform-specific method
			// (NFS on macOS, FUSE on Linux).
			fs, err := mountFilesystem(ctx, cfg, connStr, absMountpoint)
			if err != nil {
				return fmt.Errorf("failed to mount: %w", err)
			}
			defer func() {
				if err := fs.Close(); err != nil {
					logging.Error("Failed to close filesystem", zap.Error(err))
				}
				// Clean up auto-created mountpoint directory after unmount.
				// Must run after fs.Close() which actually unmounts the filesystem;
				// os.Remove would fail on a mounted directory.
				if autoCreated {
					if err := os.Remove(absMountpoint); err != nil {
						logging.Debug("Could not remove auto-created mountpoint directory",
							zap.String("mountpoint", absMountpoint), zap.Error(err))
					} else {
						logging.Debug("Removed auto-created mountpoint directory",
							zap.String("mountpoint", absMountpoint))
					}
				}
			}()

			logging.Info("Filesystem mounted successfully", zap.String("mountpoint", absMountpoint))

			// Register the mount in the registry for discovery by other commands.
			if err := registerMount(absMountpoint, connStr, serviceID, cliBackend, autoCreated); err != nil {
				logging.Warn("Failed to register mount in registry", zap.Error(err))
			}

			defer func() {
				if err := unregisterMount(absMountpoint); err != nil {
					logging.Warn("Failed to unregister mount from registry", zap.Error(err))
				}
			}()

			// Serve filesystem requests — blocks until unmount or signal.
			if err := fs.Serve(ctx); err != nil {
				return fmt.Errorf("filesystem error: %w", err)
			}

			logging.Info("Filesystem unmounted", zap.String("mountpoint", absMountpoint))
			return nil
		},
	}

	cmd.Flags().StringVar(&schema, "schema", "", "default schema (inherits from PostgreSQL if not set)")
	cmd.Flags().BoolVar(&readOnly, "read-only", false, "mount as read-only")
	cmd.Flags().IntVar(&maxLsRows, "max-ls-rows", 10000, "large table threshold")
	cmd.Flags().BoolVar(&foreground, "foreground", false, "run in foreground (don't daemonize)")
	cmd.Flags().BoolVar(&noFilenameExtensions, "no-filename-extensions", false, "disable automatic file extensions based on column type")
	cmd.Flags().DurationVar(&queryTimeout, "query-timeout", 0, "global query timeout (e.g., 30s, 1m); 0 uses config default")
	cmd.Flags().IntVar(&dirFilterLimit, "dir-filter-limit", 0, "row count threshold for .filter/ value listing; 0 uses config default")
	cmd.Flags().BoolVar(&legacyFuse, "legacy-fuse", false, "use legacy FUSE node tree (Linux only)")
	cmd.Flags().BoolVar(&insecureNoSSL, "insecure-no-ssl", false, "allow non-TLS connections to remote databases (insecure)")

	return cmd
}

// resolveMountArgs resolves the connection string and mountpoint from
// command-line arguments.
//
// With 2 args: first is connection, second is mountpoint.
// With 1 arg and a backend prefix (tiger:, ghost:): connection ref with
// mountpoint auto-derived as baseDir/<id>.
// With 1 arg and no backend prefix: treated as mountpoint (existing behavior).
//
// Parameters:
//   - args: Command-line arguments (1 or 2 elements)
//   - baseDir: Base directory for auto-derived mountpoints (e.g., /tmp)
//
// Returns (connStr, mountpoint, error).
func resolveMountArgs(args []string, baseDir string) (string, string, error) {
	if len(args) == 2 {
		return args[0], args[1], nil
	}

	ref := args[0]
	b, id, err := backend.Resolve(ref)
	if err != nil {
		return "", "", err
	}
	if b != nil {
		// Backend prefix → connection ref, derive mountpoint.
		return ref, filepath.Join(baseDir, id), nil
	}

	// No backend prefix → treat as mountpoint (existing behavior).
	return "", ref, nil
}

// registerMount adds the current mount to the registry for discovery.
//
// Parameters:
//   - mountpoint: Absolute path to the mountpoint
//   - connStr: Database connection string (will be sanitized to remove password)
//   - serviceID: Cloud service ID (empty for direct connections)
//   - cliBackend: Backend name — "tiger", "ghost", or "" (empty for direct connections)
//   - autoCreated: true if the mountpoint directory was auto-created (should be cleaned up on unmount)
//
// Returns an error if the registry cannot be accessed or modified.
func registerMount(mountpoint, connStr, serviceID, cliBackend string, autoCreated bool) error {
	registry, err := mount.NewRegistry("")
	if err != nil {
		return fmt.Errorf("failed to open registry: %w", err)
	}

	entry := mount.Entry{
		Mountpoint:  mountpoint,
		PID:         os.Getpid(),
		Database:    db.SanitizeConnectionString(connStr),
		StartTime:   time.Now(),
		ServiceID:   serviceID,
		CLIBackend:  cliBackend,
		AutoCreated: autoCreated,
	}

	if err := registry.Register(entry); err != nil {
		return fmt.Errorf("failed to register mount: %w", err)
	}

	logging.Debug("Registered mount in registry",
		zap.String("mountpoint", mountpoint),
		zap.Int("pid", entry.PID),
		zap.String("service_id", serviceID),
		zap.String("backend", cliBackend),
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
