package fs

import "fmt"

// ErrorCode represents filesystem error types.
// These are backend-agnostic and map to appropriate errno values
// in FUSE and NFS adapters.
type ErrorCode int

// Error codes for filesystem operations.
const (
	// ErrNone indicates no error.
	ErrNone ErrorCode = iota

	// ErrNotExist indicates the path does not exist.
	// Maps to ENOENT in POSIX.
	ErrNotExist

	// ErrPermission indicates insufficient permissions.
	// Maps to EACCES in POSIX.
	ErrPermission

	// ErrInvalidPath indicates a malformed or invalid path.
	// Maps to EINVAL in POSIX.
	ErrInvalidPath

	// ErrInvalidFormat indicates invalid data format (e.g., malformed JSON).
	// Maps to EINVAL in POSIX.
	ErrInvalidFormat

	// ErrInvalidOperation indicates an operation that isn't allowed
	// (e.g., writing to a read-only mount, deleting a non-empty directory).
	// Maps to EPERM in POSIX.
	ErrInvalidOperation

	// ErrReadOnly indicates a write operation on a read-only filesystem.
	// Maps to EROFS in POSIX.
	ErrReadOnly

	// ErrNotEmpty indicates a directory is not empty (for rmdir).
	// Maps to ENOTEMPTY in POSIX.
	ErrNotEmpty

	// ErrAlreadyExists indicates the path already exists.
	// Maps to EEXIST in POSIX.
	ErrAlreadyExists

	// ErrIO indicates a general I/O error (database connection, etc.).
	// Maps to EIO in POSIX.
	ErrIO

	// ErrInternal indicates an unexpected internal error.
	// Maps to EIO in POSIX.
	ErrInternal

	// ErrNotImplemented indicates a feature is not yet implemented.
	// Maps to ENOSYS in POSIX.
	ErrNotImplemented

	// ErrInvalidArgument indicates an invalid argument value.
	// Maps to EINVAL in POSIX.
	ErrInvalidArgument
)

// Alias for backwards compatibility
const ErrExists = ErrAlreadyExists

// FSError is a backend-agnostic filesystem error.
// It provides structured error information that adapters can convert
// to appropriate backend-specific errors (errno for FUSE, NFS status codes).
type FSError struct {
	// Code identifies the error type for programmatic handling.
	Code ErrorCode

	// Message is a human-readable error description.
	Message string

	// Cause is the underlying error, if any.
	Cause error

	// Hint provides user-friendly guidance for resolving the error.
	// Displayed via logging before returning errno to user.
	Hint string
}

// Error implements the error interface.
func (e *FSError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// Unwrap returns the underlying cause for errors.Is/As support.
func (e *FSError) Unwrap() error {
	return e.Cause
}

// NewNotExistError creates an error for a non-existent path.
func NewNotExistError(path string) *FSError {
	return &FSError{
		Code:    ErrNotExist,
		Message: fmt.Sprintf("path not found: %s", path),
	}
}

// NewPermissionError creates an error for permission denied.
func NewPermissionError(op, path string) *FSError {
	return &FSError{
		Code:    ErrPermission,
		Message: fmt.Sprintf("permission denied: %s on %s", op, path),
	}
}

// NewInvalidPathError creates an error for an invalid path.
func NewInvalidPathError(path, reason string) *FSError {
	return &FSError{
		Code:    ErrInvalidPath,
		Message: fmt.Sprintf("invalid path %s: %s", path, reason),
		Hint:    reason,
	}
}
