package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func buildListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all mounted TigerFS instances",
		Long: `List all mounted TigerFS instances (simple output for scripting).

Examples:
  # List all mounts
  tigerfs list`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// TODO: Implement list logic
			return fmt.Errorf("list not yet implemented")
		},
	}
}
