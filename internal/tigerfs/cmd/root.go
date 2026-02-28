package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
)

func Execute(ctx context.Context) error {
	rootCmd := buildRootCmd(ctx)
	return rootCmd.ExecuteContext(ctx)
}

func buildRootCmd(ctx context.Context) *cobra.Command {
	// Declare ALL persistent flag variables locally
	var configDir string
	var logLevel string
	var logFile string
	var logFormat string
	var logSQLParams bool

	cmd := &cobra.Command{
		Use:   "tigerfs",
		Short: "TigerFS - Mount PostgreSQL databases as filesystems",
		Long: `TigerFS exposes PostgreSQL database contents as mountable directories.

Use standard Unix tools (ls, cat, grep, rm) to interact with database data
without writing SQL queries. Perfect for exploration, scripting, and AI assistants.`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Bind persistent flags to viper at execution time
			if err := errors.Join(
				viper.BindPFlag("config_dir", cmd.Flags().Lookup("config-dir")),
				viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level")),
				viper.BindPFlag("log_file", cmd.Flags().Lookup("log-file")),
				viper.BindPFlag("log_format", cmd.Flags().Lookup("log-format")),
				viper.BindPFlag("log_sql_params", cmd.Flags().Lookup("log-sql-params")),
			); err != nil {
				return fmt.Errorf("failed to bind flags: %w", err)
			}

			// Initialize logging first
			if err := logging.Init(logLevel); err != nil {
				return fmt.Errorf("failed to initialize logging: %w", err)
			}

			// Load configuration
			if err := config.Init(); err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			return nil
		},
	}

	// Add persistent flags
	cmd.PersistentFlags().StringVar(&configDir, "config-dir", config.GetDefaultConfigDir(), "config directory")
	cmd.PersistentFlags().StringVar(&logLevel, "log-level", "warn", "log level (debug, info, warn, error)")
	cmd.PersistentFlags().StringVar(&logFile, "log-file", "", "log file path (default: stderr)")
	cmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "log format (text, json)")
	cmd.PersistentFlags().BoolVar(&logSQLParams, "log-sql-params", false, "log SQL query parameters (may contain sensitive data)")

	// Add all subcommands (complete tree building)
	cmd.AddCommand(buildMountCmd(ctx))
	cmd.AddCommand(buildUnmountCmd())
	cmd.AddCommand(buildStopCmd())
	cmd.AddCommand(buildStatusCmd())
	cmd.AddCommand(buildInfoCmd())
	cmd.AddCommand(buildCreateCmd())
	cmd.AddCommand(buildForkCmd())
	cmd.AddCommand(buildListCmd())
	cmd.AddCommand(buildTestConnectionCmd())
	cmd.AddCommand(buildVersionCmd())
	cmd.AddCommand(buildConfigCmd())

	return cmd
}
