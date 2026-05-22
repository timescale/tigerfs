// Package fs tests for errors.go
package fs

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestErrorCode verifies error code constants.
func TestErrorCode(t *testing.T) {
	// Verify codes are distinct
	codes := []ErrorCode{
		ErrNone, ErrNotExist, ErrPermission, ErrInvalidPath,
		ErrInvalidFormat, ErrInvalidOperation, ErrReadOnly,
		ErrNotEmpty, ErrAlreadyExists, ErrIO, ErrInternal,
	}
	seen := make(map[ErrorCode]bool)
	for _, code := range codes {
		if seen[code] {
			t.Errorf("duplicate error code: %d", code)
		}
		seen[code] = true
	}
}

// TestFSErrorWithoutCause verifies error message without cause.
func TestFSErrorWithoutCause(t *testing.T) {
	err := &FSError{
		Code:    ErrNotExist,
		Message: "path not found",
		Hint:    "check the table name",
	}

	if err.Error() != "path not found" {
		t.Errorf("Error() = %q, want %q", err.Error(), "path not found")
	}
	if err.Code != ErrNotExist {
		t.Errorf("Code = %d, want %d", err.Code, ErrNotExist)
	}
	if err.Hint != "check the table name" {
		t.Errorf("Hint = %q, want %q", err.Hint, "check the table name")
	}
}

// TestFSErrorWithCause verifies error message includes cause.
func TestFSErrorWithCause(t *testing.T) {
	cause := errors.New("connection refused")
	err := &FSError{
		Code:    ErrIO,
		Message: "database error",
		Cause:   cause,
	}

	want := "database error: connection refused"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

// TestNewNotExistError verifies the constructor.
func TestNewNotExistError(t *testing.T) {
	err := NewNotExistError("/users/999")

	if err.Code != ErrNotExist {
		t.Errorf("Code = %d, want %d", err.Code, ErrNotExist)
	}
	if err.Message == "" {
		t.Error("Message should not be empty")
	}
}

// TestNewPermissionError verifies the constructor.
func TestNewPermissionError(t *testing.T) {
	err := NewPermissionError("write", "/users/1")

	if err.Code != ErrPermission {
		t.Errorf("Code = %d, want %d", err.Code, ErrPermission)
	}
	if err.Message == "" {
		t.Error("Message should not be empty")
	}
}

// TestNewInvalidPathError verifies the constructor.
func TestNewInvalidPathError(t *testing.T) {
	err := NewInvalidPathError("/users/../etc", "path traversal not allowed")

	if err.Code != ErrInvalidPath {
		t.Errorf("Code = %d, want %d", err.Code, ErrInvalidPath)
	}
	if err.Message == "" {
		t.Error("Message should not be empty")
	}
	if err.Hint == "" {
		t.Error("Hint should not be empty")
	}
}

// TestFSErrorUnwrap verifies error unwrapping for errors.Is/As.
func TestFSErrorUnwrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := &FSError{
		Code:    ErrIO,
		Message: "wrapper",
		Cause:   cause,
	}

	if !errors.Is(err, cause) {
		t.Error("errors.Is should find the cause")
	}
}

// TestIsCancellationError covers the defensive helper used as a guard
// before statCache.setNegative. The helper must report "looks like
// cancellation" when either fsErr.Cause traces back to context.Canceled
// / context.DeadlineExceeded, OR the ctx itself is cancelled. Both
// signals are sufficient because intermediate layers (resolveSynthPath,
// today) can drop the cause chain.
func TestIsCancellationError(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name   string
		ctx    context.Context
		fsErr  *FSError
		expect bool
	}{
		{
			name:   "nil error, live ctx",
			ctx:    context.Background(),
			fsErr:  nil,
			expect: false,
		},
		{
			name:   "ErrNotExist without cause, live ctx (real not-found)",
			ctx:    context.Background(),
			fsErr:  &FSError{Code: ErrNotExist, Message: "x"},
			expect: false,
		},
		{
			name:   "ErrNotExist caused by context.Canceled, live ctx",
			ctx:    context.Background(),
			fsErr:  &FSError{Code: ErrNotExist, Message: "x", Cause: context.Canceled},
			expect: true,
		},
		{
			name:   "ErrNotExist caused by context.DeadlineExceeded",
			ctx:    context.Background(),
			fsErr:  &FSError{Code: ErrNotExist, Message: "x", Cause: context.DeadlineExceeded},
			expect: true,
		},
		{
			name:   "ErrNotExist with wrapped context.Canceled (errors.Is unwraps)",
			ctx:    context.Background(),
			fsErr:  &FSError{Code: ErrNotExist, Message: "x", Cause: fmt.Errorf("query failed: %w", context.Canceled)},
			expect: true,
		},
		{
			name:   "nil error, cancelled ctx (cause was lost upstream)",
			ctx:    canceledCtx,
			fsErr:  nil,
			expect: true,
		},
		{
			name:   "ErrNotExist without cause, cancelled ctx",
			ctx:    canceledCtx,
			fsErr:  &FSError{Code: ErrNotExist, Message: "x"},
			expect: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCancellationError(tc.ctx, tc.fsErr)
			if got != tc.expect {
				t.Errorf("isCancellationError = %v, want %v", got, tc.expect)
			}
		})
	}
}
