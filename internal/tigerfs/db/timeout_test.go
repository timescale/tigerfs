package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestIsTimeoutError_ContextDeadlineExceeded tests detection of context deadline exceeded
func TestIsTimeoutError_ContextDeadlineExceeded(t *testing.T) {
	err := context.DeadlineExceeded
	if !IsTimeoutError(err) {
		t.Error("Expected IsTimeoutError to return true for context.DeadlineExceeded")
	}
}

// TestIsTimeoutError_WrappedDeadlineExceeded tests detection of wrapped deadline exceeded
func TestIsTimeoutError_WrappedDeadlineExceeded(t *testing.T) {
	err := errors.Join(errors.New("query failed"), context.DeadlineExceeded)
	if !IsTimeoutError(err) {
		t.Error("Expected IsTimeoutError to return true for wrapped context.DeadlineExceeded")
	}
}

// TestIsTimeoutError_PostgreSQLStatementTimeout tests detection of PostgreSQL statement_timeout
func TestIsTimeoutError_PostgreSQLStatementTimeout(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "statement timeout message",
			err:  errors.New("ERROR: canceling statement due to statement timeout (SQLSTATE 57014)"),
			want: true,
		},
		{
			name: "just sqlstate",
			err:  errors.New("error with code 57014"),
			want: true,
		},
		{
			name: "canceling statement",
			err:  errors.New("canceling statement due to user request"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsTimeoutError(tc.err)
			if got != tc.want {
				t.Errorf("IsTimeoutError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestClient_WithQueryTimeout tests the WithQueryTimeout method
func TestClient_WithQueryTimeout(t *testing.T) {
	tests := []struct {
		name           string
		cfg            *config.Config
		expectTimeout  bool
		expectedMargin time.Duration
	}{
		{
			name:           "with 100ms timeout",
			cfg:            &config.Config{QueryTimeout: 100 * time.Millisecond},
			expectTimeout:  true,
			expectedMargin: 20 * time.Millisecond,
		},
		{
			name:          "with zero timeout",
			cfg:           &config.Config{QueryTimeout: 0},
			expectTimeout: false,
		},
		{
			name:          "with nil config",
			cfg:           nil,
			expectTimeout: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{cfg: tc.cfg}
			ctx := context.Background()

			newCtx, cancel := client.WithQueryTimeout(ctx)
			defer cancel()

			if tc.expectTimeout {
				deadline, ok := newCtx.Deadline()
				if !ok {
					t.Error("Expected context to have deadline")
					return
				}

				remaining := time.Until(deadline)
				expected := tc.cfg.QueryTimeout

				// Check that remaining time is close to expected (within margin)
				if remaining < expected-tc.expectedMargin || remaining > expected+tc.expectedMargin {
					t.Errorf("Expected remaining time ~%v, got %v", expected, remaining)
				}
			} else {
				_, ok := newCtx.Deadline()
				if ok {
					t.Error("Expected context to NOT have deadline")
				}
			}
		})
	}
}

// TestClient_QueryTimeout tests the QueryTimeout method
func TestClient_QueryTimeout(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		expected time.Duration
	}{
		{
			name:     "with 30s timeout",
			cfg:      &config.Config{QueryTimeout: 30 * time.Second},
			expected: 30 * time.Second,
		},
		{
			name:     "with zero timeout",
			cfg:      &config.Config{QueryTimeout: 0},
			expected: 0,
		},
		{
			name:     "with nil config",
			cfg:      nil,
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{cfg: tc.cfg}
			got := client.QueryTimeout()
			if got != tc.expected {
				t.Errorf("QueryTimeout() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestWithQueryTimeout_CancelFunction tests that the cancel function works correctly
func TestWithQueryTimeout_CancelFunction(t *testing.T) {
	client := &Client{cfg: &config.Config{QueryTimeout: 1 * time.Hour}}
	ctx := context.Background()

	newCtx, cancel := client.WithQueryTimeout(ctx)

	// Context should not be done yet
	select {
	case <-newCtx.Done():
		t.Error("Context should not be done before cancel")
	default:
		// Expected
	}

	// Cancel the context
	cancel()

	// Context should be done now
	select {
	case <-newCtx.Done():
		// Expected
	default:
		t.Error("Context should be done after cancel")
	}
}
