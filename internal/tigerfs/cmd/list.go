// Package cmd provides CLI commands for TigerFS.
//
// This file implements the list command which outputs mountpoints of active
// TigerFS instances, one per line, for easy scripting and piping.
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
	"go.uber.org/zap"
)

// buildListCmd creates the list command.
//
// The list command outputs mountpoints of active TigerFS instances, one per line.
// This is designed for scripting - use `status` for human-readable output with
// additional details like PID, database, and uptime.
//
// Output format:
//   - One mountpoint per line
//   - No headers or decorations
//   - Empty output (no error) if no mounts exist
//
// Example usage in scripts:
//
//	for mp in $(tigerfs list); do
//	    echo "Processing $mp"
//	done
func buildListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all mounted TigerFS instances",
		Long: `List all mounted TigerFS instances (simple output for scripting).

Outputs one mountpoint per line with no headers or formatting.
For detailed information, use 'tigerfs status' instead.

Examples:
  # List all mounts
  tigerfs list

  # Count active mounts
  tigerfs list | wc -l

  # Unmount all TigerFS instances
  tigerfs list | xargs -I {} tigerfs unmount {}`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			registry, err := mount.NewRegistry("")
			if err != nil {
				return fmt.Errorf("failed to open registry: %w", err)
			}

			// Clean up stale entries before listing
			removed, err := registry.Cleanup()
			if err != nil {
				logging.Warn("Failed to clean up stale entries", zap.Error(err))
			} else if removed > 0 {
				logging.Debug("Cleaned up stale registry entries", zap.Int("count", removed))
			}

			entries, err := registry.ListActive()
			if err != nil {
				return fmt.Errorf("failed to list mounts: %w", err)
			}

			// Output mountpoints only, one per line
			for _, entry := range entries {
				fmt.Fprintln(cmd.OutOrStdout(), entry.Mountpoint)
			}

			return nil
		},
	}
}
