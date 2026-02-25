package backend

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// ErrCLINotFound indicates the backend CLI is not installed or not in PATH.
var ErrCLINotFound = errors.New("CLI not found")

// ErrNotAuthenticated indicates the user needs to authenticate with the backend CLI.
var ErrNotAuthenticated = errors.New("not authenticated")

// ErrNoBackend indicates no backend was specified and no default is configured.
var ErrNoBackend = errors.New("no backend specified")

// CLINotFoundError returns a formatted error with install instructions for the given CLI.
func CLINotFoundError(cli string) error {
	switch cli {
	case "tiger":
		return fmt.Errorf("%w: install tiger CLI from https://github.com/timescale/tiger-cli", ErrCLINotFound)
	case "ghost":
		return fmt.Errorf("%w: install ghost CLI from https://ghost.dev", ErrCLINotFound)
	default:
		return fmt.Errorf("%w: %s", ErrCLINotFound, cli)
	}
}

// NotAuthenticatedError returns a formatted error with login instructions for the given CLI.
func NotAuthenticatedError(cli string) error {
	return fmt.Errorf("%w: run '%s auth login' first", ErrNotAuthenticated, cli)
}

// checkCLIInstalled verifies that a CLI binary is available in PATH.
func checkCLIInstalled(cli string) error {
	if _, err := exec.LookPath(cli); err != nil {
		return CLINotFoundError(cli)
	}
	return nil
}

// formatCLIError converts exec errors from a CLI into user-friendly messages.
//
// It classifies common error patterns (authentication failures, not-found responses)
// into sentinel errors. Unrecognized errors are returned with stderr content.
func formatCLIError(cli string, err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		stderr := string(exitErr.Stderr)

		// Check for authentication errors.
		if strings.Contains(stderr, "not authenticated") ||
			strings.Contains(stderr, "login") ||
			strings.Contains(stderr, "unauthorized") ||
			strings.Contains(stderr, "401") {
			return NotAuthenticatedError(cli)
		}

		// Check for service not found.
		if strings.Contains(stderr, "not found") ||
			strings.Contains(stderr, "404") {
			return fmt.Errorf("service not found: %s", strings.TrimSpace(stderr))
		}

		// Return the stderr content as the error.
		if stderr != "" {
			return fmt.Errorf("%s CLI error: %s", cli, strings.TrimSpace(stderr))
		}
	}

	return fmt.Errorf("failed to run %s CLI: %w", cli, err)
}
