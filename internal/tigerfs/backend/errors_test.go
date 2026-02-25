package backend

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestCLINotFoundError(t *testing.T) {
	tests := []struct {
		cli      string
		contains string
	}{
		{"tiger", "tiger CLI from https://github.com/timescale/tiger-cli"},
		{"ghost", "ghost CLI from https://ghost.dev"},
		{"unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.cli, func(t *testing.T) {
			err := CLINotFoundError(tt.cli)
			if !errors.Is(err, ErrCLINotFound) {
				t.Errorf("CLINotFoundError(%q) should wrap ErrCLINotFound", tt.cli)
			}
			if msg := err.Error(); !strings.Contains(msg, tt.contains) {
				t.Errorf("CLINotFoundError(%q) = %q, want to contain %q", tt.cli, msg, tt.contains)
			}
		})
	}
}

func TestNotAuthenticatedError(t *testing.T) {
	tests := []struct {
		cli      string
		contains string
	}{
		{"tiger", "tiger auth login"},
		{"ghost", "ghost auth login"},
	}

	for _, tt := range tests {
		t.Run(tt.cli, func(t *testing.T) {
			err := NotAuthenticatedError(tt.cli)
			if !errors.Is(err, ErrNotAuthenticated) {
				t.Errorf("NotAuthenticatedError(%q) should wrap ErrNotAuthenticated", tt.cli)
			}
			if msg := err.Error(); !strings.Contains(msg, tt.contains) {
				t.Errorf("NotAuthenticatedError(%q) = %q, want to contain %q", tt.cli, msg, tt.contains)
			}
		})
	}
}

func TestFormatCLIError(t *testing.T) {
	tests := []struct {
		name         string
		cli          string
		err          error
		wantIs       error // nil if no sentinel expected
		wantContains string
	}{
		{
			name:         "auth error - not authenticated",
			cli:          "tiger",
			err:          makeExitError("not authenticated with Tiger Cloud"),
			wantIs:       ErrNotAuthenticated,
			wantContains: "tiger auth login",
		},
		{
			name:         "auth error - unauthorized",
			cli:          "ghost",
			err:          makeExitError("401 unauthorized"),
			wantIs:       ErrNotAuthenticated,
			wantContains: "ghost auth login",
		},
		{
			name:         "auth error - login prompt",
			cli:          "tiger",
			err:          makeExitError("please login first"),
			wantIs:       ErrNotAuthenticated,
			wantContains: "tiger auth login",
		},
		{
			name:         "service not found - 404",
			cli:          "tiger",
			err:          makeExitError("404 service not found"),
			wantContains: "service not found",
		},
		{
			name:         "service not found - text",
			cli:          "ghost",
			err:          makeExitError("database not found"),
			wantContains: "service not found",
		},
		{
			name:         "generic stderr",
			cli:          "tiger",
			err:          makeExitError("some unexpected error"),
			wantContains: "tiger CLI error: some unexpected error",
		},
		{
			name:         "empty stderr",
			cli:          "tiger",
			err:          makeExitError(""),
			wantContains: "failed to run tiger CLI",
		},
		{
			name:         "non-exit error",
			cli:          "ghost",
			err:          errors.New("exec: not started"),
			wantContains: "failed to run ghost CLI",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatCLIError(tt.cli, tt.err)
			if tt.wantIs != nil && !errors.Is(got, tt.wantIs) {
				t.Errorf("formatCLIError() should wrap %v, got: %v", tt.wantIs, got)
			}
			if !strings.Contains(got.Error(), tt.wantContains) {
				t.Errorf("formatCLIError() = %q, want to contain %q", got.Error(), tt.wantContains)
			}
		})
	}
}

// makeExitError creates an *exec.ExitError with the given stderr content.
// cmd.Output() populates ExitError.Stderr when the command fails.
func makeExitError(stderr string) *exec.ExitError {
	cmd := exec.Command("sh", "-c", "printf '%s' '"+stderr+"' >&2; exit 1")
	_, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr
	}
	// Fallback: should not happen in tests.
	return &exec.ExitError{}
}
