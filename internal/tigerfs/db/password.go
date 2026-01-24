package db

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// resolvePassword resolves the database password from multiple sources
// in the following precedence order:
// 1. Password in connection string (if present)
// 2. PGPASSWORD or TIGERFS_PASSWORD environment variable
// 3. password_command execution (if configured)
// 4. .pgpass file (handled automatically by pgx, no code needed)
//
// Returns the resolved password or empty string if none found.
func resolvePassword(ctx context.Context, cfg *config.Config, connStr string) (string, error) {
	// Note: We don't modify the connection string if it already has a password
	// pgx will use it as-is

	// Check environment variables
	if password := os.Getenv("PGPASSWORD"); password != "" {
		logging.Debug("Using password from PGPASSWORD environment variable")
		return password, nil
	}

	if password := os.Getenv("TIGERFS_PASSWORD"); password != "" {
		logging.Debug("Using password from TIGERFS_PASSWORD environment variable")
		return password, nil
	}

	// Execute password_command if configured
	if cfg.PasswordCommand != "" {
		logging.Debug("Executing password_command", zap.String("command", cfg.PasswordCommand))

		password, err := executePasswordCommand(ctx, cfg.PasswordCommand)
		if err != nil {
			return "", fmt.Errorf("password_command failed: %w", err)
		}

		logging.Debug("Successfully retrieved password from password_command")
		return password, nil
	}

	// No password found from explicit sources
	// pgx will automatically check ~/.pgpass file if no password is provided
	logging.Debug("No explicit password source found, pgx will check .pgpass file")
	return "", nil
}

// executePasswordCommand executes the configured password command
// and returns the password from stdout.
func executePasswordCommand(ctx context.Context, command string) (string, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Split command into program and args
	// Simple split by spaces - doesn't handle quoted args, but sufficient for most use cases
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty password_command")
	}

	// Execute command
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("command exited with code %d: %s", exitErr.ExitCode(), string(exitErr.Stderr))
		}
		return "", err
	}

	// Trim whitespace from output
	password := strings.TrimSpace(string(output))

	if password == "" {
		return "", fmt.Errorf("password_command returned empty output")
	}

	return password, nil
}

// injectPasswordIntoConnStr injects a password into a PostgreSQL connection string
// Returns the modified connection string with the password included.
// If the connection string already has a password, it is replaced.
func injectPasswordIntoConnStr(connStr, password string) (string, error) {
	if password == "" {
		return connStr, nil
	}

	// Parse the connection string to inject the password
	// For postgres:// URLs, we need to add/replace the password component
	// Format: postgres://[user[:password]@]host[:port][/dbname][?param=value]

	// Simple approach: check if URL format
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		// Find the @ symbol to locate user:password section
		atIdx := strings.Index(connStr, "@")
		if atIdx == -1 {
			return "", fmt.Errorf("cannot inject password: connection string has no user@host format")
		}

		// Find the :// prefix
		prefixIdx := strings.Index(connStr, "://")
		if prefixIdx == -1 {
			return "", fmt.Errorf("invalid connection string format")
		}

		// Extract user part
		userPart := connStr[prefixIdx+3 : atIdx]

		// Check if password already exists
		colonIdx := strings.Index(userPart, ":")
		var user string
		if colonIdx != -1 {
			// Password exists, replace it
			user = userPart[:colonIdx]
		} else {
			// No password, use entire userPart as username
			user = userPart
		}

		// Reconstruct connection string with password
		prefix := connStr[:prefixIdx+3]
		suffix := connStr[atIdx:]
		return fmt.Sprintf("%s%s:%s%s", prefix, user, password, suffix), nil
	}

	// For key=value format (host=... user=... dbname=...), add password=...
	if !strings.Contains(connStr, "password=") {
		return connStr + " password=" + password, nil
	}

	// Password already exists in key=value format, don't modify
	return connStr, nil
}
