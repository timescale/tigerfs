package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func buildStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status [MOUNTPOINT]",
		Short: "Show status of mounted filesystems",
		Long: `Show status of mounted TigerFS instances.

Without MOUNTPOINT argument, lists all mounted instances.
With MOUNTPOINT, shows detailed status for that specific mount.

Examples:
  # List all mounts
  tigerfs status

  # Show detailed status for specific mount
  tigerfs status /mnt/db`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// TODO: Implement status logic
			if len(args) == 0 {
				return fmt.Errorf("status listing not yet implemented")
			}

			mountpoint := args[0]
			return fmt.Errorf("status for %s not yet implemented", mountpoint)
		},
	}
}
