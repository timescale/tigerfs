package nfs

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	nfsfile "github.com/willscott/go-nfs/file"
	"go.uber.org/zap"
)

// OpsFilesystem implements billy.Filesystem by delegating to fs.Operations.
// This provides feature parity between FUSE and NFS by using the shared core.
type OpsFilesystem struct {
	ops *fs.Operations
}

// NewOpsFilesystem creates a new OpsFilesystem that wraps fs.Operations.
func NewOpsFilesystem(ops *fs.Operations) *OpsFilesystem {
	return &OpsFilesystem{ops: ops}
}

// normalizePath ensures the path starts with "/" as required by fs.Operations.
// NFS/billy may pass paths without leading slash.
func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		return "/" + p
	}
	return p
}

// Create creates a new file for writing.
func (f *OpsFilesystem) Create(filename string) (billy.File, error) {
	filename = normalizePath(filename)
	logging.Debug("OpsFilesystem.Create", zap.String("filename", filename))
	return &memFile{
		name:     path.Base(filename),
		data:     []byte{},
		writable: true,
		dirty:    false,
		path:     filename,
		ops:      f.ops,
	}, nil
}

// Open opens a file for reading.
func (f *OpsFilesystem) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile opens a file with the specified flags and mode.
func (f *OpsFilesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = normalizePath(filename)
	logging.Debug("OpsFilesystem.OpenFile", zap.String("filename", filename), zap.Int("flag", flag))

	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0

	// For write with truncate or create, start with empty data
	if flag&os.O_TRUNC != 0 || flag&os.O_CREATE != 0 {
		return &memFile{
			name:     path.Base(filename),
			data:     []byte{},
			writable: isWrite,
			dirty:    false,
			path:     filename,
			ops:      f.ops,
		}, nil
	}

	// Try to read existing content
	content, fsErr := f.ops.ReadFile(ctx(), filename)
	if fsErr != nil {
		if fsErr.Code == fs.ErrNotExist {
			// If creating, return empty writable file
			if flag&os.O_CREATE != 0 {
				return &memFile{
					name:     path.Base(filename),
					data:     []byte{},
					writable: isWrite,
					dirty:    false,
					path:     filename,
					ops:      f.ops,
				}, nil
			}
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}

	return &memFile{
		name:     path.Base(filename),
		data:     content.Data,
		writable: isWrite,
		dirty:    false,
		path:     filename,
		ops:      f.ops,
	}, nil
}

// Stat returns file info for the given path.
func (f *OpsFilesystem) Stat(filename string) (os.FileInfo, error) {
	filename = normalizePath(filename)
	logging.Debug("OpsFilesystem.Stat", zap.String("filename", filename))

	entry, fsErr := f.ops.Stat(ctx(), filename)
	if fsErr != nil {
		if fsErr.Code == fs.ErrNotExist {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}

	return &opsFileInfo{entry: entry, path: filename}, nil
}

// Rename is not supported.
func (f *OpsFilesystem) Rename(oldpath, newpath string) error {
	return fmt.Errorf("rename not supported")
}

// Remove removes a file.
func (f *OpsFilesystem) Remove(filename string) error {
	filename = normalizePath(filename)
	logging.Debug("OpsFilesystem.Remove", zap.String("filename", filename))

	fsErr := f.ops.Delete(ctx(), filename)
	if fsErr != nil {
		if fsErr.Code == fs.ErrNotExist {
			return os.ErrNotExist
		}
		return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}
	return nil
}

// Join joins path elements.
func (f *OpsFilesystem) Join(elem ...string) string {
	return path.Join(elem...)
}

// TempFile creates a temporary file. Not supported.
func (f *OpsFilesystem) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("temp files not supported")
}

// ReadDir returns directory entries for the given path.
func (f *OpsFilesystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	dirname = normalizePath(dirname)
	logging.Debug("OpsFilesystem.ReadDir", zap.String("dirname", dirname))

	entries, fsErr := f.ops.ReadDir(ctx(), dirname)
	if fsErr != nil {
		if fsErr.Code == fs.ErrNotExist {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}

	result := make([]os.FileInfo, len(entries))
	for i := range entries {
		// Construct full path for unique file ID generation
		entryPath := path.Join(dirname, entries[i].Name)
		result[i] = &opsFileInfo{entry: &entries[i], path: entryPath}
	}
	return result, nil
}

// MkdirAll creates directories.
func (f *OpsFilesystem) MkdirAll(filename string, perm os.FileMode) error {
	filename = normalizePath(filename)
	logging.Debug("OpsFilesystem.MkdirAll", zap.String("filename", filename))

	fsErr := f.ops.Mkdir(ctx(), filename)
	if fsErr != nil {
		if fsErr.Code == fs.ErrAlreadyExists {
			return nil // MkdirAll doesn't fail if directory exists
		}
		return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}
	return nil
}

// Lstat is the same as Stat (no symlinks).
func (f *OpsFilesystem) Lstat(filename string) (os.FileInfo, error) {
	return f.Stat(filename)
}

// Symlink is not supported.
func (f *OpsFilesystem) Symlink(target, link string) error {
	return fmt.Errorf("symlinks not supported")
}

// Readlink is not supported.
func (f *OpsFilesystem) Readlink(link string) (string, error) {
	return "", fmt.Errorf("symlinks not supported")
}

// Chroot returns a filesystem rooted at the given path.
func (f *OpsFilesystem) Chroot(p string) (billy.Filesystem, error) {
	return &opsChrootFilesystem{fs: f, root: p}, nil
}

// Root returns the root path of the filesystem.
func (f *OpsFilesystem) Root() string {
	return "/"
}

// opsFileInfo implements os.FileInfo using fs.Entry.
type opsFileInfo struct {
	entry *fs.Entry
	path  string // Full path for generating unique file IDs
}

func (fi *opsFileInfo) Name() string       { return fi.entry.Name }
func (fi *opsFileInfo) Size() int64        { return fi.entry.Size }
func (fi *opsFileInfo) Mode() os.FileMode  { return fi.entry.Mode }
func (fi *opsFileInfo) ModTime() time.Time { return fi.entry.ModTime }
func (fi *opsFileInfo) IsDir() bool        { return fi.entry.IsDir }

// Sys returns NFS file info with the current user's UID/GID and a unique Fileid.
// This ensures files appear owned by the user who mounted the filesystem,
// rather than root (which would cause permission denied errors).
// The Fileid is generated by hashing the full path to ensure uniqueness.
func (fi *opsFileInfo) Sys() interface{} {
	// Generate file ID by hashing the path (same approach as go-nfs uses)
	hasher := fnv.New64()
	hasher.Write([]byte(fi.path))
	fileid := hasher.Sum64()

	return &nfsfile.FileInfo{
		Fileid: fileid,
		UID:    uint32(os.Getuid()),
		GID:    uint32(os.Getgid()),
		Nlink:  1,
	}
}

// opsChrootFilesystem wraps an OpsFilesystem with a root prefix.
type opsChrootFilesystem struct {
	fs   *OpsFilesystem
	root string
}

func (c *opsChrootFilesystem) Create(filename string) (billy.File, error) {
	return c.fs.Create(path.Join(c.root, filename))
}

func (c *opsChrootFilesystem) Open(filename string) (billy.File, error) {
	return c.fs.Open(path.Join(c.root, filename))
}

func (c *opsChrootFilesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	return c.fs.OpenFile(path.Join(c.root, filename), flag, perm)
}

func (c *opsChrootFilesystem) Stat(filename string) (os.FileInfo, error) {
	return c.fs.Stat(path.Join(c.root, filename))
}

func (c *opsChrootFilesystem) Rename(oldpath, newpath string) error {
	return c.fs.Rename(path.Join(c.root, oldpath), path.Join(c.root, newpath))
}

func (c *opsChrootFilesystem) Remove(filename string) error {
	return c.fs.Remove(path.Join(c.root, filename))
}

func (c *opsChrootFilesystem) Join(elem ...string) string {
	return c.fs.Join(elem...)
}

func (c *opsChrootFilesystem) TempFile(dir, prefix string) (billy.File, error) {
	return c.fs.TempFile(path.Join(c.root, dir), prefix)
}

func (c *opsChrootFilesystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.fs.ReadDir(path.Join(c.root, dirname))
}

func (c *opsChrootFilesystem) MkdirAll(filename string, perm os.FileMode) error {
	return c.fs.MkdirAll(path.Join(c.root, filename), perm)
}

func (c *opsChrootFilesystem) Lstat(filename string) (os.FileInfo, error) {
	return c.fs.Lstat(path.Join(c.root, filename))
}

func (c *opsChrootFilesystem) Symlink(target, link string) error {
	return c.fs.Symlink(target, path.Join(c.root, link))
}

func (c *opsChrootFilesystem) Readlink(link string) (string, error) {
	return c.fs.Readlink(path.Join(c.root, link))
}

func (c *opsChrootFilesystem) Chroot(p string) (billy.Filesystem, error) {
	return &opsChrootFilesystem{fs: c.fs, root: path.Join(c.root, p)}, nil
}

func (c *opsChrootFilesystem) Root() string {
	return c.root
}

// ctx returns a background context for operations.
// In a real implementation, this should be passed through from the NFS request.
func ctx() context.Context {
	return context.Background()
}

// memFile is an in-memory file for reading and writing row data.
type memFile struct {
	name     string
	data     []byte
	offset   int64
	writable bool
	dirty    bool
	path     string         // full path for write-back
	ops      *fs.Operations // for write-back on Close
}

func (f *memFile) Name() string {
	return f.name
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.offset:])
	f.offset += int64(n)
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(len(f.data)) + offset
	}
	return f.offset, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	if !f.writable {
		return 0, fmt.Errorf("file not opened for writing")
	}

	// Expand buffer if needed
	newLen := f.offset + int64(len(p))
	if newLen > int64(len(f.data)) {
		newData := make([]byte, newLen)
		copy(newData, f.data)
		f.data = newData
	}

	// Write data at current offset
	copy(f.data[f.offset:], p)
	f.offset += int64(len(p))
	f.dirty = true
	return len(p), nil
}

func (f *memFile) Close() error {
	if f.dirty && f.ops != nil && f.path != "" {
		logging.Debug("memFile.Close writing back", zap.String("path", f.path), zap.Int("size", len(f.data)))
		fsErr := f.ops.WriteFile(ctx(), f.path, f.data)
		if fsErr != nil {
			return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
		}
	}
	return nil
}

func (f *memFile) Lock() error {
	return nil
}

func (f *memFile) Unlock() error {
	return nil
}

func (f *memFile) Truncate(size int64) error {
	if !f.writable {
		return fmt.Errorf("file not opened for writing")
	}
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	} else if size > int64(len(f.data)) {
		newData := make([]byte, size)
		copy(newData, f.data)
		f.data = newData
	}
	f.dirty = true
	return nil
}
