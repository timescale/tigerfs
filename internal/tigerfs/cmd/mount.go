package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

func buildMountCmd(ctx context.Context) *cobra.Command {
	// Declare local flags
	var host string
	var port int
	var user string
	var database string
	var tigerServiceID string
	var readOnly bool
	var maxLsRows int
	var foreground bool
	var allowOther bool
	var allowRoot bool

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

			// Parse arguments
			var connStr, mountpoint string
			if len(args) == 2 {
				connStr = args[0]
				mountpoint = args[1]
			} else {
				mountpoint = args[0]
			}

			logging.Info("Starting TigerFS mount",
				zap.String("mountpoint", mountpoint),
				zap.Bool("read_only", readOnly),
			)

			// Load config
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// TODO: Mount filesystem
			fs, err := fuse.Mount(ctx, cfg, connStr, mountpoint)
			if err != nil {
				return fmt.Errorf("failed to mount: %w", err)
			}
			defer fs.Close()

			logging.Info("Filesystem mounted successfully", zap.String("mountpoint", mountpoint))

			// Serve filesystem (blocks until unmount or signal)
			if err := fs.Serve(ctx); err != nil {
				return fmt.Errorf("filesystem error: %w", err)
			}

			logging.Info("Filesystem unmounted", zap.String("mountpoint", mountpoint))
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
	cmd.Flags().BoolVar(&allowOther, "allow-other", false, "allow other users to access mount")
	cmd.Flags().BoolVar(&allowRoot, "allow-root", false, "allow root to access mount")

	return cmd
}
