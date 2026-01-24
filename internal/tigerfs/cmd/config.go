package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func buildConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configuration management",
		Long:  `Manage TigerFS configuration.`,
	}

	cmd.AddCommand(buildConfigShowCmd())
	cmd.AddCommand(buildConfigValidateCmd())
	cmd.AddCommand(buildConfigPathCmd())

	return cmd
}

func buildConfigShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Show current configuration",
		Long:  `Display merged configuration from all sources (defaults, config file, environment variables, flags).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// TODO: Implement config show
			return fmt.Errorf("config show not yet implemented")
		},
	}
}

func buildConfigValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate configuration file",
		Long:  `Validate configuration file syntax and values.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// TODO: Implement config validate
			return fmt.Errorf("config validate not yet implemented")
		},
	}
}

func buildConfigPathCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "path",
		Short: "Show configuration file path",
		Long:  `Display the path to the configuration file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// TODO: Implement config path
			return fmt.Errorf("config path not yet implemented")
		},
	}
}
