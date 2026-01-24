package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

func buildTestConnectionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "test-connection [CONNECTION]",
		Short: "Test database connectivity",
		Long: `Test database connectivity and verify credentials.

Examples:
  # Test connection using connection string
  tigerfs test-connection postgres://localhost/mydb

  # Test connection using config file / env vars
  tigerfs test-connection`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// Get connection string from args or environment
			var connStr string
			if len(args) > 0 {
				connStr = args[0]
			} else {
				// Build connection string from environment/config
				cfg, err := config.Load()
				if err != nil {
					return fmt.Errorf("failed to load config: %w", err)
				}

				if cfg.Host == "" {
					return fmt.Errorf("no connection string provided and PGHOST not set")
				}

				// Build basic connection string from config
				connStr = fmt.Sprintf("postgres://%s@%s:%d/%s",
					cfg.User, cfg.Host, cfg.Port, cfg.Database)
			}

			logging.Info("Testing database connection", zap.String("connection", connStr))

			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Load config
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Attempt connection
			client, err := db.NewClient(ctx, cfg, connStr)
			if err != nil {
				logging.Error("Connection failed", zap.Error(err))
				return fmt.Errorf("connection failed: %w", err)
			}
			defer client.Close()

			logging.Info("Connection successful!")
			fmt.Println("✓ Database connection successful")

			return nil
		},
	}
}
