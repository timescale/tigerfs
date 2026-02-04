package nfs

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"strings"
	"sync"
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

	// inFlightMu protects inFlightFiles and truncatedFiles for concurrent access.
	inFlightMu sync.RWMutex
	// inFlightFiles tracks files being created but not yet written to the database.
	// This is needed because NFS calls Stat after Create but before Close, and the
	// row doesn't exist in the database yet. We return synthetic stat info for these.
	inFlightFiles map[string]*memFile

	// truncatedFiles tracks files that have been truncated via O_TRUNC but not yet
	// written. This handles the NFS truncate-before-write pattern for existing rows:
	//   1. SETATTR(size=0) → OpenFile with O_TRUNC → marks as truncated
	//   2. WRITE → OpenFile for write → checks truncated, returns empty memFile
	// Without this, step 2 would read existing row data from DB, causing corruption.
	truncatedFiles map[string]bool
}

// NewOpsFilesystem creates a new OpsFilesystem that wraps fs.Operations.
func NewOpsFilesystem(ops *fs.Operations) *OpsFilesystem {
	return &OpsFilesystem{
		ops:            ops,
		inFlightFiles:  make(map[string]*memFile),
		truncatedFiles: make(map[string]bool),
	}
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

	// DDL trigger files (.test, .commit, .abort) are marked with isTrigger=true.
	// The actual trigger fires on Close via memFile.Close() calling WriteFile.
	baseName := path.Base(filename)
	isTrigger := baseName == ".test" || baseName == ".commit" || baseName == ".abort"

	// DDL sql files need special handling: empty writes must be persisted to clear
	// the session content. This is needed for the truncate-before-write pattern.
	isDDLSQL := baseName == "sql" && isDDLPath(filename)

	// Row files (JSON, CSV, etc.) need replacement semantics on write.
	rowFile := isRowFile(baseName)

	mf := &memFile{
		name:      baseName,
		data:      []byte{},
		writable:  true,
		dirty:     false,
		path:      filename,
		ops:       f.ops,
		isTrigger: isTrigger,
		isDDLSQL:  isDDLSQL,
		isRowFile: rowFile,
		fs:        f,
	}

	// Track this file as in-flight so Stat returns success before Close writes to DB.
	// This is needed because NFS calls SETATTR (which calls Stat) after CREATE but
	// before the data is written to the database.
	f.inFlightMu.Lock()
	f.inFlightFiles[filename] = mf
	numAfter := len(f.inFlightFiles)
	f.inFlightMu.Unlock()
	logging.Debug("OpsFilesystem.Create: registered in-flight file", zap.String("path", filename), zap.Int("numInFlight", numAfter))

	return mf, nil
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

	// Check for in-flight files first.
	// NFS write workflow: CREATE → SETATTR → WRITE (OpenFile with O_RDWR, no O_CREATE)
	// The file is registered as in-flight on CREATE but doesn't exist in DB yet.
	// If we try to read from DB, it fails. So for in-flight files opened for writing,
	// return a writable memFile that will write to DB on Close.
	if isWrite {
		f.inFlightMu.RLock()
		inFlightMf, inFlight := f.inFlightFiles[filename]
		f.inFlightMu.RUnlock()

		if inFlight {
			logging.Debug("OpsFilesystem.OpenFile: returning in-flight file for write",
				zap.String("filename", filename), zap.Int("flag", flag))

			// Return a new memFile that shares the same path but allows writing.
			// The original in-flight memFile was created by CREATE and immediately closed.
			// This new memFile will receive the actual data and write to DB on Close.
			baseName := path.Base(filename)
			return &memFile{
				name:      baseName,
				data:      inFlightMf.data, // Start with any existing data
				writable:  true,
				dirty:     false,
				path:      filename,
				ops:       f.ops,
				isTrigger: inFlightMf.isTrigger,
				isDDLSQL:  inFlightMf.isDDLSQL,
				isRowFile: inFlightMf.isRowFile,
				fs:        f,
			}, nil
		}
	}

	// DDL trigger files (.test, .commit, .abort) are handled specially:
	// - They always appear to exist (via stat) for NFS protocol compliance
	// - The actual trigger fires on Close via memFile.Close() calling WriteFile
	// - We mark them with isTrigger=true so Close knows to bypass the empty-write check
	baseName := path.Base(filename)
	isTrigger := baseName == ".test" || baseName == ".commit" || baseName == ".abort"

	// DDL sql files need special handling for NFS truncate-before-write pattern.
	isDDLSQL := baseName == "sql" && isDDLPath(filename)

	// Row files (JSON, CSV, etc.) need replacement semantics on write.
	rowFile := isRowFile(baseName)

	// For write with truncate or create, start with empty data
	if flag&os.O_TRUNC != 0 || flag&os.O_CREATE != 0 {
		mf := &memFile{
			name:      baseName,
			data:      []byte{},
			writable:  isWrite,
			dirty:     false,
			path:      filename,
			ops:       f.ops,
			isTrigger: isTrigger,
			isDDLSQL:  isDDLSQL,
			isRowFile: rowFile,
			fs:        f,
		}
		f.inFlightMu.Lock()
		// Track as in-flight for O_CREATE (new file being created).
		// This allows Stat to succeed before Close writes to DB.
		if flag&os.O_CREATE != 0 {
			f.inFlightFiles[filename] = mf
			logging.Debug("OpsFilesystem.OpenFile: registered in-flight file (O_CREATE)",
				zap.String("path", filename), zap.Int("flag", flag))
		}
		// Track as truncated for O_TRUNC (existing file being overwritten).
		// This allows subsequent WRITE operations to start with empty data
		// instead of reading stale data from the database.
		if flag&os.O_TRUNC != 0 {
			f.truncatedFiles[filename] = true
			logging.Debug("OpsFilesystem.OpenFile: marked file as truncated (O_TRUNC)",
				zap.String("path", filename), zap.Int("flag", flag))
		}
		f.inFlightMu.Unlock()
		return mf, nil
	}

	// Check if file was recently truncated (truncate-before-write pattern).
	// For existing files: SETATTR(size=0) → O_TRUNC → marks truncated → WRITE → here
	// We need to return empty data instead of reading stale data from DB.
	if isWrite {
		f.inFlightMu.RLock()
		truncated := f.truncatedFiles[filename]
		f.inFlightMu.RUnlock()

		if truncated {
			logging.Debug("OpsFilesystem.OpenFile: returning empty memFile for truncated file",
				zap.String("filename", filename), zap.Int("flag", flag))
			return &memFile{
				name:      baseName,
				data:      []byte{},
				writable:  true,
				dirty:     false,
				path:      filename,
				ops:       f.ops,
				isTrigger: isTrigger,
				isDDLSQL:  isDDLSQL,
				isRowFile: rowFile,
				fs:        f,
			}, nil
		}
	}

	// For DDL sql files opened for writing (without O_TRUNC), start with empty data.
	// This handles the NFS truncate-before-write pattern:
	//   1. SETATTR(size=0) → opens with O_TRUNC, truncates, closes (marks dirty but skip write)
	//   2. WRITE → opens with O_WRONLY (no O_TRUNC), writes content, closes
	// Without this, step 2 would read the template from DDLManager (since GetSQL
	// returns template when SQL is empty), causing partial overwrites.
	// By starting with empty data for write operations, we ensure clean writes.
	if isDDLSQL && isWrite {
		logging.Debug("OpsFilesystem.OpenFile: DDL sql file opened for write, starting empty",
			zap.String("filename", filename), zap.Int("flag", flag))
		return &memFile{
			name:      baseName,
			data:      []byte{},
			writable:  isWrite,
			dirty:     false,
			path:      filename,
			ops:       f.ops,
			isTrigger: isTrigger,
			isDDLSQL:  isDDLSQL,
			isRowFile: rowFile,
			fs:        f,
		}, nil
	}

	// Try to read existing content
	content, fsErr := f.ops.ReadFile(ctx(), filename)
	if fsErr != nil {
		if fsErr.Code == fs.ErrNotExist {
			// If creating, return empty writable file and track as in-flight
			if flag&os.O_CREATE != 0 {
				mf := &memFile{
					name:      baseName,
					data:      []byte{},
					writable:  isWrite,
					dirty:     false,
					path:      filename,
					ops:       f.ops,
					isTrigger: isTrigger,
					isDDLSQL:  isDDLSQL,
					isRowFile: rowFile,
					fs:        f,
				}
				f.inFlightMu.Lock()
				f.inFlightFiles[filename] = mf
				f.inFlightMu.Unlock()
				logging.Debug("OpsFilesystem.OpenFile: registered in-flight file (O_CREATE, not exist)",
					zap.String("path", filename), zap.Int("flag", flag))
				return mf, nil
			}
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}

	return &memFile{
		name:      baseName,
		data:      content.Data,
		writable:  isWrite,
		dirty:     false,
		path:      filename,
		ops:       f.ops,
		isTrigger: isTrigger,
		isDDLSQL:  isDDLSQL,
		isRowFile: rowFile,
		fs:        f,
	}, nil
}

// Stat returns file info for the given path.
func (f *OpsFilesystem) Stat(filename string) (os.FileInfo, error) {
	filename = normalizePath(filename)
	logging.Debug("OpsFilesystem.Stat", zap.String("filename", filename))

	// Check for in-flight files first (files being created but not yet written to DB).
	// NFS calls Stat after Create but before Close, and the row doesn't exist in the
	// database yet. Return synthetic info for these files.
	f.inFlightMu.RLock()
	mf, inFlight := f.inFlightFiles[filename]
	f.inFlightMu.RUnlock()

	if inFlight {
		logging.Debug("OpsFilesystem.Stat: returning in-flight file info",
			zap.String("filename", filename),
			zap.Int("size", len(mf.data)))
		return &inFlightFileInfo{
			name:    mf.name,
			size:    int64(len(mf.data)),
			mode:    0644,
			modTime: time.Now(),
			path:    filename,
		}, nil
	}

	entry, fsErr := f.ops.Stat(ctx(), filename)
	if fsErr != nil {
		logging.Debug("OpsFilesystem.Stat error",
			zap.String("filename", filename),
			zap.Int("code", int(fsErr.Code)),
			zap.String("message", fsErr.Message),
			zap.Error(fsErr.Cause))
		// Map FSError codes to appropriate OS errors
		switch fsErr.Code {
		case fs.ErrNotExist:
			return nil, os.ErrNotExist
		case fs.ErrPermission:
			return nil, os.ErrPermission
		case fs.ErrInvalidPath:
			return nil, os.ErrInvalid
		default:
			// For other errors, return not exist to avoid false permission denied
			return nil, os.ErrNotExist
		}
	}

	fi := &opsFileInfo{entry: entry, path: filename}
	logging.Debug("OpsFilesystem.Stat success",
		zap.String("filename", filename),
		zap.String("name", entry.Name),
		zap.Bool("isDir", entry.IsDir),
		zap.Int64("size", entry.Size),
		zap.Uint32("mode", uint32(fi.Mode())))
	return fi, nil
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
		logging.Debug("OpsFilesystem.ReadDir error",
			zap.String("dirname", dirname),
			zap.Int("code", int(fsErr.Code)),
			zap.String("message", fsErr.Message),
			zap.Error(fsErr.Cause))
		// Map FSError codes to appropriate OS errors
		// Use errors that NFS will interpret correctly
		switch fsErr.Code {
		case fs.ErrNotExist:
			return nil, os.ErrNotExist
		case fs.ErrPermission:
			return nil, os.ErrPermission
		case fs.ErrInvalidPath:
			return nil, os.ErrInvalid
		default:
			// For other errors (IO, internal), return a generic error
			// that won't be mistaken for permission denied.
			// Use os.ErrNotExist as it's the safest fallback for directories.
			return nil, os.ErrNotExist
		}
	}

	logging.Debug("OpsFilesystem.ReadDir success",
		zap.String("dirname", dirname),
		zap.Int("count", len(entries)))
	result := make([]os.FileInfo, len(entries))
	for i := range entries {
		// Construct full path for unique file ID generation
		entryPath := path.Join(dirname, entries[i].Name)
		fi := &opsFileInfo{entry: &entries[i], path: entryPath}
		result[i] = fi
		// Log detailed attributes for debugging NFS permission issues
		logging.Debug("OpsFilesystem.ReadDir entry",
			zap.String("name", fi.Name()),
			zap.String("path", entryPath),
			zap.Bool("isDir", fi.IsDir()),
			zap.Uint32("mode", uint32(fi.Mode())),
			zap.Int64("size", fi.Size()))
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

// billy.Change interface implementation - required for go-nfs to allow writes.
// These are no-ops since database-backed filesystems don't support Unix permissions.

// Chmod is a no-op for database-backed filesystems.
func (f *OpsFilesystem) Chmod(name string, mode os.FileMode) error {
	logging.Debug("OpsFilesystem.Chmod (no-op)", zap.String("name", name), zap.Uint32("mode", uint32(mode)))
	return nil
}

// Lchown is a no-op for database-backed filesystems.
func (f *OpsFilesystem) Lchown(name string, uid, gid int) error {
	logging.Debug("OpsFilesystem.Lchown (no-op)", zap.String("name", name), zap.Int("uid", uid), zap.Int("gid", gid))
	return nil
}

// Chown is a no-op for database-backed filesystems.
func (f *OpsFilesystem) Chown(name string, uid, gid int) error {
	logging.Debug("OpsFilesystem.Chown (no-op)", zap.String("name", name), zap.Int("uid", uid), zap.Int("gid", gid))
	return nil
}

// Chtimes is a no-op for database-backed filesystems.
func (f *OpsFilesystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	name = normalizePath(name)
	logging.Debug("OpsFilesystem.Chtimes", zap.String("name", name), zap.Time("atime", atime), zap.Time("mtime", mtime))

	// DDL trigger files (.test, .commit, .abort) are triggered on OpenFile, not Chtimes.
	// go-nfs may call Chtimes after file close to update timestamps, but by then the
	// session might already be removed (for .abort/.commit). We just return success.
	// Note: We used to trigger here for `touch` which uses Chtimes, but now OpenFile
	// triggers on ANY open of trigger files, so Chtimes triggering is redundant.

	return nil
}

// opsFileInfo implements os.FileInfo using fs.Entry.
type opsFileInfo struct {
	entry *fs.Entry
	path  string // Full path for generating unique file IDs
}

func (fi *opsFileInfo) Name() string { return fi.entry.Name }
func (fi *opsFileInfo) Size() int64  { return fi.entry.Size }

// Mode returns the file mode.
// IMPORTANT: go-nfs has a bug where it sends the full os.FileMode to the wire
// as the NFS mode field. NFS mode should only contain permission bits (0-07777),
// not file type bits. go-nfs correctly uses IsDir() to set the NFS type field,
// but the mode field ends up with os.ModeDir (0x80000000) included.
// We return the permission bits only to work around this.
// Note: We still need IsDir() to return true for go-nfs to set the correct NFS type.
func (fi *opsFileInfo) Mode() os.FileMode {
	// Return only permission bits (masking off file type)
	// go-nfs will still correctly identify this as a directory via IsDir()
	return fi.entry.Mode.Perm()
}
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

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())

	logging.Debug("opsFileInfo.Sys",
		zap.String("path", fi.path),
		zap.String("name", fi.entry.Name),
		zap.Uint64("fileid", fileid),
		zap.Uint32("uid", uid),
		zap.Uint32("gid", gid),
		zap.Bool("isDir", fi.IsDir()),
		zap.Uint32("mode", uint32(fi.Mode())))

	return &nfsfile.FileInfo{
		Fileid: fileid,
		UID:    uid,
		GID:    gid,
		Nlink:  1,
	}
}

// inFlightFileInfo implements os.FileInfo for files being created but not yet in DB.
// This is used to return synthetic stat info for in-flight creates.
type inFlightFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	path    string
}

func (fi *inFlightFileInfo) Name() string       { return fi.name }
func (fi *inFlightFileInfo) Size() int64        { return fi.size }
func (fi *inFlightFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *inFlightFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *inFlightFileInfo) IsDir() bool        { return false }

// Sys returns NFS file info with the current user's UID/GID.
func (fi *inFlightFileInfo) Sys() interface{} {
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

// billy.Change interface implementation for chroot filesystem.

func (c *opsChrootFilesystem) Chmod(name string, mode os.FileMode) error {
	return c.fs.Chmod(path.Join(c.root, name), mode)
}

func (c *opsChrootFilesystem) Lchown(name string, uid, gid int) error {
	return c.fs.Lchown(path.Join(c.root, name), uid, gid)
}

func (c *opsChrootFilesystem) Chown(name string, uid, gid int) error {
	return c.fs.Chown(path.Join(c.root, name), uid, gid)
}

func (c *opsChrootFilesystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return c.fs.Chtimes(path.Join(c.root, name), atime, mtime)
}

// ctx returns a background context for operations.
// In a real implementation, this should be passed through from the NFS request.
func ctx() context.Context {
	return context.Background()
}

// isDDLPath checks if a path is within a DDL staging directory (.create, .modify, .delete).
func isDDLPath(p string) bool {
	return strings.Contains(p, "/.create/") || strings.Contains(p, "/.modify/") || strings.Contains(p, "/.delete/")
}

// isRowFile checks if a filename is a row format file (JSON, CSV, TSV, YAML).
// Row files represent entire database rows and writes should replace the full content.
func isRowFile(filename string) bool {
	return strings.HasSuffix(filename, ".json") ||
		strings.HasSuffix(filename, ".csv") ||
		strings.HasSuffix(filename, ".tsv") ||
		strings.HasSuffix(filename, ".yaml")
}

// memFile is an in-memory file for reading and writing row data.
type memFile struct {
	name      string
	data      []byte
	offset    int64
	writable  bool
	dirty     bool
	path      string         // full path for write-back
	ops       *fs.Operations // for write-back on Close
	isTrigger bool           // true for DDL trigger files (.test, .commit, .abort)
	isDDLSQL  bool           // true for DDL sql files (need empty writes to clear session)
	isRowFile bool           // true for row files (JSON, CSV, etc.) - writes replace entire content
	fs        *OpsFilesystem // for removing from inFlightFiles on Close
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

	// For row files (JSON, CSV, etc.), writes replace the entire content.
	// This handles the case where NFS reads existing data, then writes new data
	// at offset 0. Without replacement semantics, the buffer would retain any
	// old data beyond the new content length, causing JSON parsing errors.
	if f.isRowFile && f.offset == 0 {
		f.data = make([]byte, len(p))
		copy(f.data, p)
		f.offset = int64(len(p))
		f.dirty = true
		return len(p), nil
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
	logging.Debug("memFile.Close", zap.String("path", f.path), zap.Bool("dirty", f.dirty), zap.Int("size", len(f.data)), zap.Bool("isTrigger", f.isTrigger), zap.Bool("isDDLSQL", f.isDDLSQL))

	// Remove from in-flight tracking only when content was actually written.
	// go-nfs calls Create then immediately Close (with dirty=false, size=0) to get
	// an NFS file handle. We need to keep tracking these "created but not written"
	// files so Stat returns success. Only remove when:
	// - File has content and was dirty (actual write completed), OR
	// - File is a trigger file (these don't need tracking)
	shouldRemoveFromInFlight := f.dirty && len(f.data) > 0
	defer func() {
		if f.fs != nil && f.path != "" && shouldRemoveFromInFlight {
			f.fs.inFlightMu.Lock()
			delete(f.fs.inFlightFiles, f.path)
			delete(f.fs.truncatedFiles, f.path) // Also clear truncated flag
			f.fs.inFlightMu.Unlock()
			logging.Debug("memFile.Close: removed from in-flight/truncated tracking", zap.String("path", f.path))
		}
	}()

	// For DDL trigger files, ALWAYS write to trigger the operation.
	// The trigger happens regardless of whether the file was dirtied or has content.
	if f.isTrigger && f.ops != nil && f.path != "" {
		logging.Debug("memFile.Close: DDL trigger file, triggering operation", zap.String("path", f.path))
		fsErr := f.ops.WriteFile(ctx(), f.path, []byte{})
		if fsErr != nil {
			logging.Error("memFile.Close: DDL trigger failed", zap.String("path", f.path), zap.Error(fsErr.Cause))
			// Don't return error - let NFS operations complete normally.
			// The DDL operation failure is logged but shouldn't cause NFS protocol errors.
		}
		return nil
	}

	if f.dirty && f.ops != nil && f.path != "" {
		// WORKAROUND: Skip writing empty content to database columns.
		//
		// Background: The macOS NFS client uses a truncate-before-write pattern:
		//   1. SETATTR with size=0 (truncates file to empty)
		//   2. WRITE operations with actual content
		//   3. COMMIT to finalize
		//
		// The go-nfs library handles SETATTR by opening the file, calling Truncate(0),
		// and then Close(). Our memFile.Close() writes data back to the database.
		// This means truncating a file immediately writes empty content to the DB,
		// BEFORE the actual content arrives in step 2.
		//
		// Problem: Writing empty content to a database column can fail:
		//   - NOT NULL constraints: empty string converts to NULL, violating constraint
		//   - Data integrity: user intended to write content, not clear it
		//
		// This workaround skips the database write when content is empty. The actual
		// content will be written when the NFS WRITE operations arrive with data.
		//
		// EXCEPTION: DDL sql files MUST write empty content to clear the session.
		// Without this, the truncate-before-write pattern leaves stale template
		// content in the DDL session, causing partial overwrites.
		//
		// Limitation: This workaround means users cannot intentionally clear a column
		// to empty by truncating. They should use Delete (rm) to set columns to NULL.
		if len(f.data) == 0 && !f.isDDLSQL {
			logging.Debug("memFile.Close: skipping write of empty content (truncate-before-write pattern)",
				zap.String("path", f.path))
			return nil
		}
		logging.Debug("memFile.Close writing back", zap.String("path", f.path), zap.Int("size", len(f.data)))
		fsErr := f.ops.WriteFile(ctx(), f.path, f.data)
		if fsErr != nil {
			logging.Error("memFile.Close WriteFile failed", zap.String("path", f.path), zap.Error(fsErr.Cause))
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
	logging.Debug("memFile.Truncate", zap.String("path", f.path), zap.Int64("newSize", size), zap.Int("currentSize", len(f.data)))
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
