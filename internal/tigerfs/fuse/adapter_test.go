package fuse

import (
	"os"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// TestNewFSAdapter tests creating a new adapter.
func TestNewFSAdapter(t *testing.T) {
	cfg := &config.Config{}
	ops := tigerfs.NewOperations(cfg, nil)
	adapter := NewFSAdapter(ops)

	require.NotNil(t, adapter)
	assert.Equal(t, ops, adapter.ops)
}

// TestFSAdapter_EntryToAttr_Directory tests converting a directory entry.
func TestFSAdapter_EntryToAttr_Directory(t *testing.T) {
	cfg := &config.Config{}
	ops := tigerfs.NewOperations(cfg, nil)
	adapter := NewFSAdapter(ops)

	entry := &tigerfs.Entry{
		Name:    "users",
		IsDir:   true,
		Size:    4096,
		Mode:    os.ModeDir | 0755,
		ModTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	var out gofuse.Attr
	adapter.EntryToAttr(entry, &out)

	assert.Equal(t, uint32(syscall.S_IFDIR|0755), out.Mode)
	assert.Equal(t, uint64(4096), out.Size)
	assert.Equal(t, uint32(2), out.Nlink) // directories have nlink=2
}

// TestFSAdapter_EntryToAttr_File tests converting a file entry.
func TestFSAdapter_EntryToAttr_File(t *testing.T) {
	cfg := &config.Config{}
	ops := tigerfs.NewOperations(cfg, nil)
	adapter := NewFSAdapter(ops)

	entry := &tigerfs.Entry{
		Name:    "1.json",
		IsDir:   false,
		Size:    256,
		Mode:    0644,
		ModTime: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	var out gofuse.Attr
	adapter.EntryToAttr(entry, &out)

	assert.Equal(t, uint32(syscall.S_IFREG|0644), out.Mode)
	assert.Equal(t, uint64(256), out.Size)
	assert.Equal(t, uint32(1), out.Nlink) // regular files have nlink=1
}

// TestFSAdapter_ErrorToErrno tests error code conversion.
func TestFSAdapter_ErrorToErrno(t *testing.T) {
	cfg := &config.Config{}
	ops := tigerfs.NewOperations(cfg, nil)
	adapter := NewFSAdapter(ops)

	tests := []struct {
		name     string
		err      *tigerfs.FSError
		expected syscall.Errno
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: 0,
		},
		{
			name:     "not exist",
			err:      &tigerfs.FSError{Code: tigerfs.ErrNotExist, Message: "not found"},
			expected: syscall.ENOENT,
		},
		{
			name:     "permission denied",
			err:      &tigerfs.FSError{Code: tigerfs.ErrPermission, Message: "denied"},
			expected: syscall.EACCES,
		},
		{
			name:     "invalid path",
			err:      &tigerfs.FSError{Code: tigerfs.ErrInvalidPath, Message: "invalid"},
			expected: syscall.EINVAL,
		},
		{
			name:     "invalid format",
			err:      &tigerfs.FSError{Code: tigerfs.ErrInvalidFormat, Message: "bad format"},
			expected: syscall.EINVAL,
		},
		{
			name:     "invalid operation",
			err:      &tigerfs.FSError{Code: tigerfs.ErrInvalidOperation, Message: "not allowed"},
			expected: syscall.EPERM,
		},
		{
			name:     "read only",
			err:      &tigerfs.FSError{Code: tigerfs.ErrReadOnly, Message: "read only"},
			expected: syscall.EROFS,
		},
		{
			name:     "not empty",
			err:      &tigerfs.FSError{Code: tigerfs.ErrNotEmpty, Message: "not empty"},
			expected: syscall.ENOTEMPTY,
		},
		{
			name:     "already exists",
			err:      &tigerfs.FSError{Code: tigerfs.ErrAlreadyExists, Message: "exists"},
			expected: syscall.EEXIST,
		},
		{
			name:     "io error",
			err:      &tigerfs.FSError{Code: tigerfs.ErrIO, Message: "io error"},
			expected: syscall.EIO,
		},
		{
			name:     "internal error",
			err:      &tigerfs.FSError{Code: tigerfs.ErrInternal, Message: "internal"},
			expected: syscall.EIO,
		},
		{
			name:     "not implemented",
			err:      &tigerfs.FSError{Code: tigerfs.ErrNotImplemented, Message: "not impl"},
			expected: syscall.ENOSYS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := adapter.ErrorToErrno(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFSAdapter_EntriesToDirEntries tests converting fs.Entry slice to FUSE DirEntry slice.
func TestFSAdapter_EntriesToDirEntries(t *testing.T) {
	cfg := &config.Config{}
	ops := tigerfs.NewOperations(cfg, nil)
	adapter := NewFSAdapter(ops)

	entries := []tigerfs.Entry{
		{Name: "public", IsDir: true, Mode: os.ModeDir | 0755},
		{Name: "users", IsDir: true, Mode: os.ModeDir | 0755},
		{Name: ".info", IsDir: true, Mode: os.ModeDir | 0500},
		{Name: "1.json", IsDir: false, Mode: 0644},
	}

	dirEntries := adapter.EntriesToDirEntries(entries)

	require.Len(t, dirEntries, 4)
	assert.Equal(t, "public", dirEntries[0].Name)
	assert.Equal(t, uint32(syscall.S_IFDIR), dirEntries[0].Mode)
	assert.Equal(t, "users", dirEntries[1].Name)
	assert.Equal(t, uint32(syscall.S_IFDIR), dirEntries[1].Mode)
	assert.Equal(t, ".info", dirEntries[2].Name)
	assert.Equal(t, uint32(syscall.S_IFDIR), dirEntries[2].Mode)
	assert.Equal(t, "1.json", dirEntries[3].Name)
	assert.Equal(t, uint32(syscall.S_IFREG), dirEntries[3].Mode)
}

// TestFSAdapter_Operations tests getting underlying operations.
func TestFSAdapter_Operations(t *testing.T) {
	cfg := &config.Config{}
	ops := tigerfs.NewOperations(cfg, nil)
	adapter := NewFSAdapter(ops)

	assert.Equal(t, ops, adapter.Operations())
}

// Note: ReadDir, Stat, ReadFile, WriteFile, Delete, and Mkdir tests require
// a database connection and are covered in integration tests.
