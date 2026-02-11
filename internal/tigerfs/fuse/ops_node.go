package fuse

// OpsNode and OpsFileHandle provide a single generic FUSE node type that
// delegates all filesystem operations to FSAdapter (which wraps fs.Operations).
// This mirrors how the NFS backend uses a single OpsFilesystem type.
//
// Each OpsNode stores its full path from the root. Lookup("users") on a node
// at "/" creates a child node at "/users". All operations are dispatched to
// FSAdapter using this reconstructed path.
//
// OpsFileHandle buffers writes in memory and commits on Flush using
// FUSE-appropriate semantics: write opens start with an empty buffer, and
// writes at offset 0 replace the entire buffer.

import (
	"context"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Compile-time interface checks
var _ fs.InodeEmbedder = (*OpsNode)(nil)
var _ fs.NodeLookuper = (*OpsNode)(nil)
var _ fs.NodeReaddirer = (*OpsNode)(nil)
var _ fs.NodeGetattrer = (*OpsNode)(nil)
var _ fs.NodeSetattrer = (*OpsNode)(nil)
var _ fs.NodeOpener = (*OpsNode)(nil)
var _ fs.NodeCreater = (*OpsNode)(nil)
var _ fs.NodeMkdirer = (*OpsNode)(nil)
var _ fs.NodeUnlinker = (*OpsNode)(nil)
var _ fs.NodeRmdirer = (*OpsNode)(nil)

// OpsNode is a single generic FUSE node type that delegates all operations
// to FSAdapter. Every directory and file in the tree is represented by an
// OpsNode carrying its full path (e.g., "/public/users/1/name.txt").
type OpsNode struct {
	fs.Inode

	// adapter is the bridge to fs.Operations
	adapter *FSAdapter

	// path is the full filesystem path from root (e.g., "/", "/public/users")
	path string
}

// newOpsNode creates a new OpsNode with the given adapter and path.
func newOpsNode(adapter *FSAdapter, nodePath string) *OpsNode {
	return &OpsNode{
		adapter: adapter,
		path:    nodePath,
	}
}

// childPath returns the full path for a child with the given name.
func (n *OpsNode) childPath(name string) string {
	if n.path == "/" {
		return "/" + name
	}
	return n.path + "/" + name
}

// Getattr returns file/directory attributes by delegating to FSAdapter.Stat.
func (n *OpsNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("OpsNode.Getattr", zap.String("path", n.path))

	entry, fsErr := n.adapter.ops.Stat(ctx, n.path)
	if fsErr != nil {
		return n.adapter.ErrorToErrno(fsErr)
	}

	n.adapter.EntryToAttr(entry, &out.Attr)
	return 0
}

// Setattr handles attribute changes (truncation, timestamp updates).
// For truncation to zero, delegates to the file handle.
// For DDL trigger files (.test, .commit, .abort), a timestamp update (touch)
// fires the DDL operation — FUSE `touch` uses utimensat → SETATTR, not Open.
func (n *OpsNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("OpsNode.Setattr", zap.String("path", n.path))

	// If this is a truncation via file handle, delegate to the handle
	if fh != nil {
		if ofh, ok := fh.(*OpsFileHandle); ok {
			if sz, hasSz := in.GetSize(); hasSz {
				ofh.mu.Lock()
				if sz == 0 {
					ofh.buf = ofh.buf[:0]
					ofh.dirty = true
				} else if int64(len(ofh.buf)) > int64(sz) {
					ofh.buf = ofh.buf[:sz]
					ofh.dirty = true
				}
				ofh.mu.Unlock()
			}
		}
	}

	// DDL trigger files: fire on touch (mtime update without open file handle).
	// FUSE `touch` uses utimensat which sends SETATTR with ATIME/MTIME, not OPEN.
	if fh == nil {
		if _, ok := in.GetMTime(); ok {
			baseName := path.Base(n.path)
			isTrigger := (baseName == ".test" || baseName == ".commit" || baseName == ".abort") && isDDLOpsPath(n.path)
			if isTrigger {
				logging.Debug("OpsNode.Setattr: triggering DDL operation via touch",
					zap.String("path", n.path))
				if errno := n.adapter.WriteFile(ctx, n.path, []byte{}); errno != 0 {
					return errno
				}
			}
		}
	}

	// Return current attributes
	return n.Getattr(ctx, fh, out)
}

// Lookup finds a child entry by name.
func (n *OpsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := n.childPath(name)
	logging.Debug("OpsNode.Lookup", zap.String("path", childPath))

	entry, fsErr := n.adapter.ops.Stat(ctx, childPath)
	if fsErr != nil {
		return nil, n.adapter.ErrorToErrno(fsErr)
	}

	n.adapter.EntryToAttr(entry, &out.Attr)

	// Determine stable attributes for inode dedup
	mode := uint32(syscall.S_IFREG)
	if entry.IsDir {
		mode = uint32(syscall.S_IFDIR)
	}

	child := n.NewPersistentInode(ctx, newOpsNode(n.adapter, childPath), fs.StableAttr{Mode: mode})
	return child, 0
}

// Readdir lists directory contents.
func (n *OpsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OpsNode.Readdir", zap.String("path", n.path))
	return n.adapter.ReadDir(ctx, n.path)
}

// Create creates a new file and returns a file handle for writing.
func (n *OpsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath := n.childPath(name)
	logging.Debug("OpsNode.Create", zap.String("path", childPath))

	// Set up the entry out for the new file
	out.Mode = syscall.S_IFREG | 0644
	out.Nlink = 1

	child := n.NewPersistentInode(ctx, newOpsNode(n.adapter, childPath), fs.StableAttr{Mode: syscall.S_IFREG})

	// Detect trigger and DDL sql files (same logic as Open)
	isTrigger := name == ".test" || name == ".commit" || name == ".abort"
	isDDLSQL := name == "sql" && isDDLOpsPath(childPath)

	handle := &OpsFileHandle{
		adapter:   n.adapter,
		path:      childPath,
		buf:       []byte{},
		dirty:     false,
		isTrigger: isTrigger,
		isDDLSQL:  isDDLSQL,
	}

	return child, handle, fuse.FOPEN_DIRECT_IO, 0
}

// Mkdir creates a new directory.
func (n *OpsNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := n.childPath(name)
	logging.Debug("OpsNode.Mkdir", zap.String("path", childPath))

	errno := n.adapter.Mkdir(ctx, childPath)
	if errno != 0 {
		return nil, errno
	}

	// Populate entry attributes
	out.Mode = syscall.S_IFDIR | 0755
	out.Nlink = 2

	child := n.NewPersistentInode(ctx, newOpsNode(n.adapter, childPath), fs.StableAttr{Mode: syscall.S_IFDIR})
	return child, 0
}

// Unlink removes a file.
func (n *OpsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := n.childPath(name)
	logging.Debug("OpsNode.Unlink", zap.String("path", childPath))
	return n.adapter.Delete(ctx, childPath)
}

// Rmdir removes a directory.
func (n *OpsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := n.childPath(name)
	logging.Debug("OpsNode.Rmdir", zap.String("path", childPath))
	return n.adapter.Delete(ctx, childPath)
}

// Open opens a file and returns a file handle.
func (n *OpsNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("OpsNode.Open", zap.String("path", n.path), zap.Uint32("flags", flags))

	accessMode := flags & syscall.O_ACCMODE
	isWrite := accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR

	// DDL trigger files (.test, .commit, .abort) should fire on close
	baseName := path.Base(n.path)
	isTrigger := baseName == ".test" || baseName == ".commit" || baseName == ".abort"
	isDDLSQL := baseName == "sql" && isDDLOpsPath(n.path)

	if isWrite || isTrigger {
		// Write mode or trigger: create a handle with an empty buffer.
		// FUSE truncates before writing (Setattr→Write→Flush), so old
		// content is never needed — starting empty avoids stale data.
		handle := &OpsFileHandle{
			adapter:   n.adapter,
			path:      n.path,
			buf:       []byte{},
			dirty:     false,
			isTrigger: isTrigger,
			isDDLSQL:  isDDLSQL,
		}

		return handle, fuse.FOPEN_DIRECT_IO, 0
	}

	// Read-only: load content eagerly
	content, errno := n.adapter.ReadFile(ctx, n.path)
	if errno != 0 {
		return nil, 0, errno
	}

	handle := &OpsFileHandle{
		adapter:  n.adapter,
		path:     n.path,
		buf:      content,
		dirty:    false,
		readOnly: true,
	}

	return handle, fuse.FOPEN_DIRECT_IO, 0
}

// isDDLOpsPath checks if a path is within a DDL staging directory.
func isDDLOpsPath(p string) bool {
	return strings.Contains(p, "/.create/") || strings.Contains(p, "/.modify/") || strings.Contains(p, "/.delete/")
}

// OpsFileHandle is a FUSE file handle that buffers writes and commits on Flush.
// Write opens start with an empty buffer, and writes at offset 0 replace the
// entire buffer — preventing stale tail bytes from shorter overwrites.
type OpsFileHandle struct {
	adapter *FSAdapter
	path    string

	mu       sync.Mutex
	buf      []byte
	dirty    bool
	readOnly bool

	// File type flags (same semantics as NFS cachedFile)
	isTrigger bool // DDL trigger files (.test, .commit, .abort)
	isDDLSQL  bool // DDL sql files
}

// Compile-time interface checks for OpsFileHandle
var _ fs.FileHandle = (*OpsFileHandle)(nil)
var _ fs.FileReader = (*OpsFileHandle)(nil)
var _ fs.FileWriter = (*OpsFileHandle)(nil)
var _ fs.FileFlusher = (*OpsFileHandle)(nil)
var _ fs.FileFsyncer = (*OpsFileHandle)(nil)
var _ fs.FileSetattrer = (*OpsFileHandle)(nil)

// Read reads data from the buffered content.
func (fh *OpsFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if off >= int64(len(fh.buf)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > int64(len(fh.buf)) {
		end = int64(len(fh.buf))
	}

	return fuse.ReadResultData(fh.buf[off:end]), 0
}

// Write buffers data in memory. Actual database write happens on Flush.
func (fh *OpsFileHandle) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	if fh.readOnly {
		return 0, syscall.EACCES
	}

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if off == 0 {
		// Replace entire buffer (common case: echo "value" > file).
		// This prevents stale tail bytes when the new content is shorter
		// than the old content.
		fh.buf = make([]byte, len(data))
		copy(fh.buf, data)
	} else {
		// Extend and overlay for non-zero offsets
		newLen := off + int64(len(data))
		if newLen > int64(len(fh.buf)) {
			newBuf := make([]byte, newLen)
			copy(newBuf, fh.buf)
			fh.buf = newBuf
		}
		copy(fh.buf[off:], data)
	}
	fh.dirty = true

	return uint32(len(data)), 0
}

// Setattr handles attribute changes on an open file handle (e.g., ftruncate).
func (fh *OpsFileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		fh.mu.Lock()
		if sz == 0 {
			fh.buf = fh.buf[:0]
			fh.dirty = true
		} else if int64(len(fh.buf)) > int64(sz) {
			fh.buf = fh.buf[:sz]
			fh.dirty = true
		}
		fh.mu.Unlock()
	}

	// Populate attributes
	out.Size = uint64(len(fh.buf))
	out.Mode = syscall.S_IFREG | 0644
	out.Nlink = 1

	return 0
}

// Flush commits buffered data to the database via FSAdapter.WriteFile.
func (fh *OpsFileHandle) Flush(ctx context.Context) syscall.Errno {
	if fh.readOnly {
		return 0
	}

	fh.mu.Lock()
	dirty := fh.dirty
	data := make([]byte, len(fh.buf))
	copy(data, fh.buf)
	isTrigger := fh.isTrigger
	isDDLSQL := fh.isDDLSQL
	fh.mu.Unlock()

	logging.Debug("OpsFileHandle.Flush",
		zap.String("path", fh.path),
		zap.Bool("dirty", dirty),
		zap.Int("size", len(data)),
		zap.Bool("isTrigger", isTrigger))

	// DDL trigger files always fire, regardless of dirty state
	if isTrigger {
		return fh.adapter.WriteFile(ctx, fh.path, []byte{})
	}

	if !dirty {
		return 0
	}

	// Skip empty non-DDL writes (truncate-before-write pattern)
	if len(data) == 0 && !isDDLSQL {
		return 0
	}

	errno := fh.adapter.WriteFile(ctx, fh.path, data)
	if errno != 0 {
		return errno
	}

	fh.mu.Lock()
	fh.dirty = false
	fh.mu.Unlock()

	return 0
}

// Fsync is a no-op since data is persisted on Flush.
func (fh *OpsFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}
