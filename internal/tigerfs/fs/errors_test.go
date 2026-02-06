// Package fs tests for errors.go
package fs

import (
	"errors"
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
