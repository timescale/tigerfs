package tigercloud

import (
	"errors"
	"os/exec"
	"testing"
)

// TestFormatCLIError verifies error message formatting from CLI output.
func TestFormatCLIError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantErr    error
		wantSubstr string
	}{
		{
			name:    "not authenticated error",
			err:     &exec.ExitError{Stderr: []byte("not authenticated")},
			wantErr: ErrNotAuthenticated,
		},
		{
			name:    "login required error",
			err:     &exec.ExitError{Stderr: []byte("please login first")},
			wantErr: ErrNotAuthenticated,
		},
		{
			name:    "unauthorized 401 error",
			err:     &exec.ExitError{Stderr: []byte("401 unauthorized")},
			wantErr: ErrNotAuthenticated,
		},
		{
			name:       "service not found error",
			err:        &exec.ExitError{Stderr: []byte("service svc-123 not found")},
			wantSubstr: "service not found",
		},
		{
			name:       "404 error",
			err:        &exec.ExitError{Stderr: []byte("404: resource does not exist")},
			wantSubstr: "service not found",
		},
		{
			name:       "generic CLI error with stderr",
			err:        &exec.ExitError{Stderr: []byte("some error message")},
			wantSubstr: "tiger CLI error: some error message",
		},
		{
			name:       "non-exit error",
			err:        errors.New("command not found"),
			wantSubstr: "failed to run tiger CLI",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatCLIError(tt.err)

			if tt.wantErr != nil {
				if !errors.Is(got, tt.wantErr) {
					t.Errorf("formatCLIError() = %v, want %v", got, tt.wantErr)
				}
				return
			}

			if tt.wantSubstr != "" && !containsString(got.Error(), tt.wantSubstr) {
				t.Errorf("formatCLIError() = %q, want substring %q", got.Error(), tt.wantSubstr)
			}
		})
	}
}

// TestCheckCLIInstalled verifies CLI detection logic.
func TestCheckCLIInstalled(t *testing.T) {
	// This test just verifies the function doesn't panic
	// Actual result depends on whether tiger CLI is installed
	err := checkCLIInstalled()
	if err != nil && !errors.Is(err, ErrCLINotFound) {
		t.Errorf("checkCLIInstalled() returned unexpected error: %v", err)
	}
}

// TestIsCLIInstalled verifies the public function wrapper.
func TestIsCLIInstalled(t *testing.T) {
	// This test just verifies the function doesn't panic
	// and returns a boolean without error
	_ = IsCLIInstalled()
}

// containsString checks if s contains substr.
func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
