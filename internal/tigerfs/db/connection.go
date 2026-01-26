package db

import (
	"context"
	"fmt"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/tigercloud"
	"go.uber.org/zap"
)

// ResolveConnectionString determines the database connection string from
// multiple sources in the following precedence order:
//
//  1. Explicit connection string (if provided)
//  2. Tiger Cloud service (if TigerCloudServiceID is configured)
//  3. PostgreSQL environment/config (PGHOST, etc.)
//
// For Tiger Cloud, this requires the Tiger CLI to be installed and authenticated.
// Authentication can be done via:
//   - Desktop: Run `tiger auth login` (opens browser for OAuth)
//   - Headless/Docker: Set TIGER_PUBLIC_KEY, TIGER_SECRET_KEY, TIGER_PROJECT_ID
//     environment variables, then run `tiger auth login`
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - cfg: TigerFS configuration
//   - explicitConnStr: Explicit connection string (from CLI argument), empty if none
//
// Returns the resolved connection string or an error if no valid source is found.
func ResolveConnectionString(ctx context.Context, cfg *config.Config, explicitConnStr string) (string, error) {
	// 1. Explicit connection string takes precedence
	if explicitConnStr != "" {
		logging.Debug("Using explicit connection string")
		return explicitConnStr, nil
	}

	// 2. Tiger Cloud service ID
	if cfg.TigerCloudServiceID != "" {
		logging.Debug("Resolving connection string from Tiger Cloud",
			zap.String("service_id", cfg.TigerCloudServiceID))

		connStr, err := tigercloud.GetConnectionString(ctx, cfg.TigerCloudServiceID)
		if err != nil {
			return "", fmt.Errorf("failed to get Tiger Cloud connection string: %w", err)
		}

		logging.Debug("Successfully resolved Tiger Cloud connection string")
		return connStr, nil
	}

	// 3. PostgreSQL environment/config
	if cfg.Host != "" {
		logging.Debug("Building connection string from config",
			zap.String("host", cfg.Host),
			zap.Int("port", cfg.Port),
			zap.String("database", cfg.Database))

		// Build basic connection string from config
		connStr := fmt.Sprintf("postgres://%s@%s:%d/%s",
			cfg.User, cfg.Host, cfg.Port, cfg.Database)

		return connStr, nil
	}

	// No valid connection source found
	return "", fmt.Errorf("no connection configuration found: provide a connection string, " +
		"set TIGER_SERVICE_ID for Tiger Cloud, or set PGHOST for direct connection")
}
