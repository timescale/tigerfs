// Package tigercloud provides integration with Tiger Cloud services via Tiger CLI.
//
// This package wraps the Tiger CLI to retrieve database connection strings.
// It supports two authentication methods:
//
//  1. Desktop (Browser-based OAuth):
//     Run `tiger auth login` which opens a browser for authentication.
//     Credentials are stored in the system keychain or ~/.config/tiger/credentials.
//
//  2. Docker/Headless (Client Credentials):
//     Set environment variables before running `tiger auth login`:
//     - TIGER_PUBLIC_KEY: Public key from Tiger Cloud Console
//     - TIGER_SECRET_KEY: Secret key from Tiger Cloud Console
//     - TIGER_PROJECT_ID: Your Tiger Cloud project ID
//     Then run `tiger auth login` which uses the env vars (no browser needed).
//
// Create client credentials at: Console → Settings → Create credentials
package tigercloud

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// ErrCLINotFound indicates the Tiger CLI is not installed.
var ErrCLINotFound = errors.New("tiger CLI not found: install from https://github.com/timescale/tiger-cli")

// ErrNotAuthenticated indicates the user needs to run `tiger auth login`.
var ErrNotAuthenticated = errors.New("not authenticated: run 'tiger auth login' first")

// GetConnectionString retrieves a PostgreSQL connection string for a Tiger Cloud service.
//
// This function calls `tiger db connection-string --with-password --service-id=<id>`.
//
// Prerequisites:
//   - Tiger CLI installed and in PATH
//   - User authenticated via `tiger auth login` (browser or env vars)
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - serviceID: The Tiger Cloud service identifier (e.g., "svc-12345")
//
// Returns the connection string in PostgreSQL URI format:
//
//	postgres://user:password@host:port/database?sslmode=require
func GetConnectionString(ctx context.Context, serviceID string) (string, error) {
	if err := checkCLIInstalled(); err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, "tiger", "db", "connection-string",
		"--with-password",
		"--service-id", serviceID)

	output, err := cmd.Output()
	if err != nil {
		return "", formatCLIError(err)
	}

	connStr := strings.TrimSpace(string(output))
	if connStr == "" {
		return "", errors.New("tiger CLI returned empty connection string")
	}

	return connStr, nil
}

// IsAuthenticated checks if the Tiger CLI has valid authentication.
//
// This runs `tiger auth status` to verify credentials are valid.
// Returns true if authenticated, false otherwise.
func IsAuthenticated(ctx context.Context) bool {
	if err := checkCLIInstalled(); err != nil {
		return false
	}

	cmd := exec.CommandContext(ctx, "tiger", "auth", "status")
	err := cmd.Run()
	return err == nil
}

// IsCLIInstalled checks if the Tiger CLI is available in PATH.
func IsCLIInstalled() bool {
	return checkCLIInstalled() == nil
}

// checkCLIInstalled verifies the tiger binary is in PATH.
func checkCLIInstalled() error {
	if _, err := exec.LookPath("tiger"); err != nil {
		return ErrCLINotFound
	}
	return nil
}

// formatCLIError converts exec errors into user-friendly messages.
func formatCLIError(err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		stderr := string(exitErr.Stderr)

		// Check for common authentication errors
		if strings.Contains(stderr, "not authenticated") ||
			strings.Contains(stderr, "login") ||
			strings.Contains(stderr, "unauthorized") ||
			strings.Contains(stderr, "401") {
			return ErrNotAuthenticated
		}

		// Check for service not found
		if strings.Contains(stderr, "not found") ||
			strings.Contains(stderr, "404") {
			return fmt.Errorf("service not found: %s", strings.TrimSpace(stderr))
		}

		// Return the stderr content as the error
		if stderr != "" {
			return fmt.Errorf("tiger CLI error: %s", strings.TrimSpace(stderr))
		}
	}

	return fmt.Errorf("failed to run tiger CLI: %w", err)
}
