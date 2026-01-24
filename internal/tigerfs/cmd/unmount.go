package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func buildUnmountCmd() *cobra.Command {
	var force bool
	var timeout int

	cmd := &cobra.Command{
		Use:   "unmount MOUNTPOINT",
		Short: "Unmount a TigerFS filesystem",
		Long: `Gracefully unmount a TigerFS instance.

Examples:
  # Unmount filesystem
  tigerfs unmount /mnt/db

  # Force unmount
  tigerfs unmount --force /mnt/db

  # Unmount with custom timeout
  tigerfs unmount --timeout=60 /mnt/db`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			mountpoint := args[0]

			// TODO: Implement unmount logic
			return fmt.Errorf("unmount not yet implemented: %s", mountpoint)
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "force unmount even if busy")
	cmd.Flags().IntVarP(&timeout, "timeout", "t", 30, "wait timeout in seconds")

	return cmd
}
