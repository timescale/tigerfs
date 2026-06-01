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
	fsops "github.com/timescale/tigerfs/internal/tigerfs/fs"
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
var _ fs.NodeRenamer = (*OpsNode)(nil)
var _ fs.NodeReadlinker = (*OpsNode)(nil)

// OpsNode is a single generic FUSE node type that delegates all operations
// to FSAdapter. Every directory and file in the tree is represented by an
// OpsNode. Its path is computed on-demand from the inode's position in the
// FUSE tree (see currentPath), so that go-fuse's MvChild after a successful
// rename automatically moves the node's path with it.
type OpsNode struct {
	fs.Inode

	// adapter is the bridge to fs.Operations
	adapter *FSAdapter
}

// newOpsNode creates a new OpsNode bound to the given adapter. The inode's
// path is derived from its eventual position in the FUSE inode tree.
func newOpsNode(adapter *FSAdapter) *OpsNode {
	return &OpsNode{
		adapter: adapter,
	}
}

// currentPath returns the inode's path in the FUSE tree, computed on-demand.
//
// Returns "/" for the root inode, "/<segments>" for a normal inode, and the
// empty string for an orphan -- a non-root inode that has been detached from
// the FUSE tree (e.g., parent was removed but a kernel reference still holds
// the inode alive). Callers MUST treat the empty string as ENOENT; see
// requirePath.
//
// We do not cache the path on OpsNode: go-fuse's bridge calls MvChild after
// a successful NodeRenamer.Rename, which rewires the inode under the new
// parent/name without touching the embedder. A cached path would go stale,
// causing subsequent Lookup/Getattr on the renamed inode to resolve against
// the old database row -- the classic "rename succeeded, but reads of the
// new path return ENOENT" symptom.
func (n *OpsNode) currentPath() string {
	embed := n.EmbeddedInode()
	p := embed.Path(nil)
	if p == "" {
		if embed.IsRoot() {
			return "/"
		}
		// Orphan: non-root inode with no parents.
		return ""
	}
	return "/" + p
}

// requirePath returns the inode's current path along with errno 0 on
// success. If the inode has been detached from the FUSE tree (currentPath
// is empty), logs and returns ("", ENOENT). Operations must not dispatch
// against an empty path -- the underlying fs.Operations would either error
// confusingly or silently misroute to the root.
func (n *OpsNode) requirePath(op string) (string, syscall.Errno) {
	p := n.currentPath()
	if p == "" {
		logging.Warn("OpsNode operation on orphan inode",
			zap.String("op", op),
			zap.String("hint", "inode is detached from the FUSE tree; returning ENOENT"))
		return "", syscall.ENOENT
	}
	return p, 0
}

// childPath returns the full path for a child with the given name. Returns
// the empty string if the parent inode is orphan; callers must check.
func (n *OpsNode) childPath(name string) string {
	cur := n.currentPath()
	if cur == "" {
		return ""
	}
	if cur == "/" {
		return "/" + name
	}
	return cur + "/" + name
}

// requireChildPath builds the child path under this inode, returning errno
// ENOENT (after logging) if the parent is orphan.
func (n *OpsNode) requireChildPath(op, name string) (string, syscall.Errno) {
	cp := n.childPath(name)
	if cp == "" {
		logging.Warn("OpsNode child operation on orphan parent inode",
			zap.String("op", op),
			zap.String("name", name),
			zap.String("hint", "parent inode is detached from the FUSE tree; returning ENOENT"))
		return "", syscall.ENOENT
	}
	return cp, 0
}

// Getattr returns file/directory attributes by delegating to Operations.Stat.
//
// Bypasses FSAdapter.Stat and calls ops.Stat directly, so we apply the same
// request-context decoupling FSAdapter.Stat does: FUSE_INTERRUPT from
// SIGURG-driven Go goroutine preemption otherwise propagates into pgx as
// context.Canceled, which surfaces as ENOENT via resolveSynthPath ->
// setNegative (caching a transient cancellation as a stale negative entry
// in statCache, blocking reads for the full 2s TTL).
func (n *OpsNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	p, errno := n.requirePath("Getattr")
	if errno != 0 {
		return errno
	}
	logging.Debug("OpsNode.Getattr", zap.String("path", p))

	ctx = decoupleFromRequestCancel(ctx)
	entry, fsErr := n.adapter.ops.Stat(ctx, p)
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
	p, errno := n.requirePath("Setattr")
	if errno != 0 {
		return errno
	}
	logging.Debug("OpsNode.Setattr", zap.String("path", p))

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

	// Trigger files: fire on touch (mtime update without open file handle).
	// FUSE `touch` uses utimensat which sends SETATTR with ATIME/MTIME, not OPEN.
	if fh == nil {
		if _, ok := in.GetMTime(); ok {
			baseName := path.Base(p)
			isTrigger := (baseName == fsops.FileTest || baseName == fsops.FileCommit || baseName == fsops.FileAbort) && isDDLOpsPath(p)
			isUndoApply := baseName == fsops.FileApply && strings.Contains(p, "/"+fsops.DirUndo+"/")
			if isTrigger || isUndoApply {
				logging.Debug("OpsNode.Setattr: triggering operation via touch",
					zap.String("path", p))
				if errno := n.adapter.WriteFile(ctx, p, []byte{}); errno != 0 {
					return errno
				}
			}
		}
	}

	// Return current attributes
	return n.Getattr(ctx, fh, out)
}

// Lookup finds a child entry by name.
//
// Bypasses FSAdapter.Stat and calls ops.Stat directly, so we apply the same
// request-context decoupling -- see Getattr for the FUSE_INTERRUPT story.
func (n *OpsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath, errno := n.requireChildPath("Lookup", name)
	if errno != 0 {
		return nil, errno
	}
	logging.Debug("OpsNode.Lookup", zap.String("path", childPath))

	ctx = decoupleFromRequestCancel(ctx)
	entry, fsErr := n.adapter.ops.Stat(ctx, childPath)
	if fsErr != nil {
		errno := n.adapter.ErrorToErrno(fsErr)
		// Logging the result of every failing Lookup -- particularly which
		// errno -- lets a post-failure correlation join up with the cache
		// hit/miss trace. ENOENT here matched against a statCache.isNegative
		// hit upstream is the smoking gun for the stale-negative-cache bug.
		logging.Debug("OpsNode.Lookup result",
			zap.String("path", childPath),
			zap.String("result", "error"),
			zap.Int("errno", int(errno)),
			zap.Int("err_code", int(fsErr.Code)),
			zap.String("err_message", fsErr.Message))
		return nil, errno
	}

	n.adapter.EntryToAttr(entry, &out.Attr)

	// Determine stable attributes for inode dedup
	mode := uint32(syscall.S_IFREG)
	if entry.IsDir {
		mode = uint32(syscall.S_IFDIR)
	} else if entry.IsSymlink() {
		mode = uint32(syscall.S_IFLNK)
	}

	child := n.NewPersistentInode(ctx, newOpsNode(n.adapter), fs.StableAttr{Mode: mode})
	return child, 0
}

// Readlink returns the target of a symlink.
//
// Bypasses FSAdapter and calls ops.Readlink directly, so we apply the same
// request-context decoupling -- see Getattr for the FUSE_INTERRUPT story.
func (n *OpsNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	p, errno := n.requirePath("Readlink")
	if errno != 0 {
		return nil, errno
	}
	logging.Debug("OpsNode.Readlink", zap.String("path", p))
	ctx = decoupleFromRequestCancel(ctx)
	target, fsErr := n.adapter.ops.Readlink(ctx, p)
	if fsErr != nil {
		return nil, n.adapter.ErrorToErrno(fsErr)
	}
	return []byte(target), 0
}

// Readdir lists directory contents.
func (n *OpsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	p, errno := n.requirePath("Readdir")
	if errno != 0 {
		return nil, errno
	}
	logging.Debug("OpsNode.Readdir", zap.String("path", p))
	return n.adapter.ReadDir(ctx, p)
}

// Create creates a new file and returns a file handle for writing.
func (n *OpsNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath, e := n.requireChildPath("Create", name)
	if e != 0 {
		return nil, nil, 0, e
	}
	logging.Debug("OpsNode.Create", zap.String("path", childPath))

	// Set up the entry out for the new file
	out.Mode = syscall.S_IFREG | 0644
	out.Nlink = 1

	child := n.NewPersistentInode(ctx, newOpsNode(n.adapter), fs.StableAttr{Mode: syscall.S_IFREG})

	// Detect trigger and DDL sql files (same logic as Open)
	isTrigger := name == fsops.FileTest || name == fsops.FileCommit || name == fsops.FileAbort
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
	childPath, errno := n.requireChildPath("Mkdir", name)
	if errno != 0 {
		return nil, errno
	}
	logging.Debug("OpsNode.Mkdir", zap.String("path", childPath))

	errno = n.adapter.Mkdir(ctx, childPath)
	if errno != 0 {
		return nil, errno
	}

	// Populate entry attributes
	out.Mode = syscall.S_IFDIR | 0755
	out.Nlink = 2

	child := n.NewPersistentInode(ctx, newOpsNode(n.adapter), fs.StableAttr{Mode: syscall.S_IFDIR})
	return child, 0
}

// Unlink removes a file.
func (n *OpsNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath, errno := n.requireChildPath("Unlink", name)
	if errno != 0 {
		return errno
	}
	logging.Debug("OpsNode.Unlink", zap.String("path", childPath))
	return n.adapter.Delete(ctx, childPath)
}

// Rmdir removes a directory.
func (n *OpsNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath, errno := n.requireChildPath("Rmdir", name)
	if errno != 0 {
		return errno
	}
	logging.Debug("OpsNode.Rmdir", zap.String("path", childPath))
	return n.adapter.Delete(ctx, childPath)
}

// Rename moves or renames a child entry to a new parent directory.
func (n *OpsNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDir, ok := newParent.(*OpsNode)
	if !ok {
		return syscall.EINVAL
	}

	oldPath, errno := n.requireChildPath("Rename:source", name)
	if errno != 0 {
		return errno
	}
	newPath, errno := newDir.requireChildPath("Rename:target", newName)
	if errno != 0 {
		return errno
	}

	logging.Debug("OpsNode.Rename", zap.String("oldPath", oldPath), zap.String("newPath", newPath))
	return n.adapter.Rename(ctx, oldPath, newPath)
}

// Open opens a file and returns a file handle.
func (n *OpsNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	p, errno := n.requirePath("Open")
	if errno != 0 {
		return nil, 0, errno
	}
	logging.Debug("OpsNode.Open", zap.String("path", p), zap.Uint32("flags", flags))

	accessMode := flags & syscall.O_ACCMODE
	isWrite := accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR

	// DDL trigger files (.test, .commit, .abort) should fire on close
	baseName := path.Base(p)
	isTrigger := baseName == fsops.FileTest || baseName == fsops.FileCommit || baseName == fsops.FileAbort
	isDDLSQL := baseName == "sql" && isDDLOpsPath(p)

	if isWrite || isTrigger {
		// Write mode or trigger: create a handle with an empty buffer.
		// FUSE truncates before writing (Setattr→Write→Flush), so old
		// content is never needed — starting empty avoids stale data.
		handle := &OpsFileHandle{
			adapter:   n.adapter,
			path:      p,
			buf:       []byte{},
			dirty:     false,
			isTrigger: isTrigger,
			isDDLSQL:  isDDLSQL,
		}

		return handle, fuse.FOPEN_DIRECT_IO, 0
	}

	// Read-only: load content eagerly
	content, errno := n.adapter.ReadFile(ctx, p)
	if errno != 0 {
		return nil, 0, errno
	}

	handle := &OpsFileHandle{
		adapter:  n.adapter,
		path:     p,
		buf:      content,
		dirty:    false,
		readOnly: true,
	}

	return handle, fuse.FOPEN_DIRECT_IO, 0
}

// isDDLOpsPath checks if a path is within a DDL staging directory.
func isDDLOpsPath(p string) bool {
	return strings.Contains(p, "/"+fsops.DirCreate+"/") || strings.Contains(p, "/"+fsops.DirModify+"/") || strings.Contains(p, "/"+fsops.DirDelete+"/")
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
	triggered bool // True after a DDL trigger has fired — prevents re-firing from dup2 close
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
	alreadyTriggered := fh.triggered
	if isTrigger && !alreadyTriggered {
		fh.triggered = true
	}
	fh.mu.Unlock()

	logging.Debug("OpsFileHandle.Flush",
		zap.String("path", fh.path),
		zap.Bool("dirty", dirty),
		zap.Int("size", len(data)),
		zap.Bool("isTrigger", isTrigger))

	// DDL trigger files always fire, regardless of dirty state.
	// The triggered flag prevents re-firing from multiple Flush calls
	// (GNU coreutils touch uses dup2, causing close on both fds → 2 Flushes).
	if isTrigger && !alreadyTriggered {
		return fh.adapter.WriteFile(ctx, fh.path, []byte{})
	}
	if isTrigger {
		return 0
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
