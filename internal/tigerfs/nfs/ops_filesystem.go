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
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	nfsfile "github.com/willscott/go-nfs/file"
	"go.uber.org/zap"
)

// OpsFilesystem implements billy.Filesystem by delegating to fs.Operations.
// This provides feature parity between FUSE and NFS by using the shared core.
//
// # File Caching (ADR-010)
//
// The filesystem uses a persistent file cache to reduce per-RPC overhead.
// go-nfs fabricates Open/Write/Close per WRITE RPC; the cache ensures that
// Stat returns the current buffer size (avoiding a DB read) and that subsequent
// writes reuse the cached buffer. Data is committed to DB on every Close().
//
// See cachedFile for cache entry lifecycle details and docs/spec.md § NFS Write
// Limitations for the O(n²) write amplification discussion.
type OpsFilesystem struct {
	ops *fs.Operations
	cfg *config.Config

	// cacheMu protects fileCache for concurrent access.
	// Lock ordering: always acquire cacheMu before any cachedFile.mu.
	cacheMu sync.RWMutex

	// fileCache maps normalized paths to cached file entries.
	// Entries persist until: (1) last handle closes and commits, or (2) reaper evicts.
	fileCache map[string]*cachedFile

	// reaperStop signals the cache reaper goroutine to stop.
	// Closed by Close() to trigger graceful shutdown.
	reaperStop chan struct{}
}

// cachedFile persists across NFS RPCs to reduce per-RPC overhead.
//
// # Problem Solved
//
// Without caching, each NFS WRITE RPC triggers: Open → Read from DB → Overlay → Write to DB → Close,
// plus schema resolution queries and a post-op Stat reading the full value from DB.
// With caching, per-RPC overhead is reduced to ~1 DB write (the commit in Close).
//
// # Lifecycle
//
//  1. First OpenFile: creates cachedFile with refCount=1, loads data from DB (or empty if O_TRUNC)
//  2. Subsequent OpenFile (write): increments refCount, returns memFile wrapping same cachedFile
//  3. Subsequent OpenFile (read): bypasses cache, reads from DB for proper formatting
//  4. Write: modifies cached data buffer, sets dirty=true
//  5. Sync: commits to DB immediately (handles editor saves)
//  6. Close: decrements refCount; if zero and dirty, commits to DB, marks clean, keeps in cache
//  7. Reaper: force-commits and removes entries idle for >5 minutes (handles client crashes)
//
// # Thread Safety
//
// The mu mutex protects all mutable fields. The parent OpsFilesystem.cacheMu protects
// the fileCache map itself. Lock ordering: always acquire cacheMu before mu.
//
// See ADR-010 for full architecture details.
type cachedFile struct {
	mu sync.RWMutex // Protects all fields below

	// Identity and content
	path string // Full normalized path (e.g., "/users/1/name.txt") - also the cache key
	data []byte // Current file content buffer

	// State tracking
	dirty        bool      // True if data has been modified since last commit
	refCount     int       // Number of open memFile handles pointing to this cache entry
	lastActivity time.Time // Last read/write/open time - used by reaper for idle detection
	truncated    bool      // True if O_TRUNC was set - next open should not read from DB
	deleted      bool      // True if file was rm'd while open - subsequent writes return EIO
	isSequential bool      // True if all writes have been append-only (offset == len before write)
	streamed     bool      // True if file has been partially committed via streaming (append mode)

	// Database connection for commit operations
	ops *fs.Operations

	// File type flags (determine write behavior)
	isTrigger bool // DDL trigger files (.test, .commit, .abort) - always fire on close
	isDDLSQL  bool // DDL sql files - empty writes must be persisted to clear session
	isRowFile bool // Row files (JSON, CSV, TSV, YAML) - writes at offset 0 replace entire buffer
	triggered bool // True after DDL trigger has fired — prevents re-firing from Chtimes
}

// NewOpsFilesystem creates a new OpsFilesystem that wraps fs.Operations.
//
// The returned filesystem has an initialized file cache. Callers should call
// Close() on shutdown to flush any dirty cached files.
//
// The cfg parameter provides NFS cache tuning settings (NFSStreamingThreshold,
// NFSMaxRandomWriteSize, NFSCacheReaperInterval, NFSCacheIdleTimeout).
func NewOpsFilesystem(ops *fs.Operations, cfg *config.Config) *OpsFilesystem {
	return &OpsFilesystem{
		ops:       ops,
		cfg:       cfg,
		fileCache: make(map[string]*cachedFile),
	}
}

// Default values for NFS cache settings (used when config is nil or zero)
const (
	defaultStreamingThreshold  = 10 * 1024 * 1024  // 10MB
	defaultMaxRandomWriteSize  = 100 * 1024 * 1024 // 100MB
	defaultCacheReaperInterval = 30 * time.Second
	defaultCacheIdleTimeout    = 5 * time.Minute
)

// streamingThreshold returns the configured streaming threshold or the default.
func (f *OpsFilesystem) streamingThreshold() int64 {
	if f.cfg != nil && f.cfg.NFSStreamingThreshold > 0 {
		return f.cfg.NFSStreamingThreshold
	}
	return defaultStreamingThreshold
}

// maxRandomWriteSize returns the configured max random write size or the default.
func (f *OpsFilesystem) maxRandomWriteSize() int64 {
	if f.cfg != nil && f.cfg.NFSMaxRandomWriteSize > 0 {
		return f.cfg.NFSMaxRandomWriteSize
	}
	return defaultMaxRandomWriteSize
}

// cacheReaperInterval returns the configured reaper interval or the default.
func (f *OpsFilesystem) cacheReaperInterval() time.Duration {
	if f.cfg != nil && f.cfg.NFSCacheReaperInterval > 0 {
		return f.cfg.NFSCacheReaperInterval
	}
	return defaultCacheReaperInterval
}

// cacheIdleTimeout returns the configured idle timeout or the default.
func (f *OpsFilesystem) cacheIdleTimeout() time.Duration {
	if f.cfg != nil && f.cfg.NFSCacheIdleTimeout > 0 {
		return f.cfg.NFSCacheIdleTimeout
	}
	return defaultCacheIdleTimeout
}

// =============================================================================
// File Cache Helpers (ADR-010)
//
// These methods manage the persistent file cache that enables efficient large
// file writes. The cache is keyed by normalized path and stores cachedFile
// entries that persist across NFS RPC boundaries.
//
// Lock ordering: Always acquire cacheMu before any cachedFile.mu to prevent deadlocks.
// =============================================================================

// getOrCreateCachedFile returns an existing cached file or creates a new one.
//
// If the file is already in the cache, this increments its refCount and updates
// lastActivity. If not, it creates a new entry with refCount=1.
//
// Parameters:
//   - filePath: Normalized path (must start with "/")
//   - flags: os.O_* flags from OpenFile - O_TRUNC sets cached.truncated=true
//
// Returns:
//   - The cached file entry with refCount already incremented
//
// Thread safety:
//   - Caller must hold no locks
//   - This method acquires cacheMu and briefly the cachedFile.mu
func (f *OpsFilesystem) getOrCreateCachedFile(filePath string, flags int) *cachedFile {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	if cached, exists := f.fileCache[filePath]; exists {
		cached.mu.Lock()
		cached.refCount++
		cached.lastActivity = time.Now()
		cached.mu.Unlock()
		logging.Debug("getOrCreateCachedFile: cache hit",
			zap.String("path", filePath),
			zap.Int("refCount", cached.refCount))
		return cached
	}

	// Determine file type from path
	baseName := path.Base(filePath)
	isTrigger := baseName == ".test" || baseName == ".commit" || baseName == ".abort"
	isDDLSQL := baseName == "sql" && isDDLPath(filePath)
	rowFile := isRowFile(baseName)

	// Create new cached file
	cached := &cachedFile{
		path:         filePath,
		data:         []byte{},
		dirty:        false,
		refCount:     1,
		lastActivity: time.Now(),
		truncated:    flags&os.O_TRUNC != 0,
		deleted:      false,
		isSequential: true, // Assume sequential until proven otherwise
		ops:          f.ops,
		isTrigger:    isTrigger,
		isDDLSQL:     isDDLSQL,
		isRowFile:    rowFile,
	}

	f.fileCache[filePath] = cached
	logging.Debug("getOrCreateCachedFile: cache miss, created new entry",
		zap.String("path", filePath),
		zap.Bool("truncated", cached.truncated))
	return cached
}

// removeFromCache removes a file from the cache if its refCount is zero.
//
// This is called after Close() commits data to the database. It only removes
// the entry if no other handles are still open (refCount == 0). If handles
// remain open, the entry stays in cache for their continued use.
//
// Parameters:
//   - filePath: Normalized path to remove
//
// Thread safety:
//   - Caller must hold no locks
//   - This method acquires cacheMu and briefly the cachedFile.mu
func (f *OpsFilesystem) removeFromCache(filePath string) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	cached, exists := f.fileCache[filePath]
	if !exists {
		return
	}

	cached.mu.RLock()
	refCount := cached.refCount
	cached.mu.RUnlock()

	if refCount == 0 {
		delete(f.fileCache, filePath)
		logging.Debug("removeFromCache: removed entry",
			zap.String("path", filePath))
	}
}

// getCachedFile returns an existing cached file or nil if not found.
//
// This is a read-only lookup that does NOT increment refCount. Use this for
// checking cache state (e.g., in Stat) without affecting the lifecycle.
// For opening files, use getOrCreateCachedFile instead.
//
// Parameters:
//   - filePath: Normalized path to look up
//
// Returns:
//   - The cached file entry, or nil if not in cache
//
// Thread safety:
//   - Caller must hold no locks
//   - This method acquires cacheMu for reading only
func (f *OpsFilesystem) getCachedFile(filePath string) *cachedFile {
	f.cacheMu.RLock()
	defer f.cacheMu.RUnlock()
	return f.fileCache[filePath]
}

// =============================================================================
// Cache Reaper and Shutdown (ADR-010)
//
// The reaper handles clients that crash without closing files. It periodically
// checks for idle cache entries and force-commits them to prevent memory leaks.
// On graceful shutdown, Close() flushes all dirty entries before returning.
// =============================================================================

// StartCacheReaper starts the background cache reaper goroutine.
//
// The reaper runs every 30 seconds and force-commits cache entries that have
// been idle for more than 5 minutes. This handles NFS clients that crash or
// disconnect without properly closing files.
//
// Call Close() to stop the reaper and flush all remaining dirty entries.
func (f *OpsFilesystem) StartCacheReaper() {
	f.reaperStop = make(chan struct{})

	go func() {
		ticker := time.NewTicker(f.cacheReaperInterval())
		defer ticker.Stop()

		logging.Debug("Cache reaper started",
			zap.Duration("interval", f.cacheReaperInterval()),
			zap.Duration("idleTimeout", f.cacheIdleTimeout()))

		for {
			select {
			case <-ticker.C:
				f.reapStaleCacheEntries()
			case <-f.reaperStop:
				logging.Debug("Cache reaper stopped")
				return
			}
		}
	}()
}

// reapStaleCacheEntries finds and commits cache entries that have been idle
// longer than NFSCacheIdleTimeout.
//
// For each stale entry:
//  1. If dirty, commit data to database
//  2. Remove from cache regardless of refCount
//
// This prevents memory leaks from crashed clients that never close their files.
func (f *OpsFilesystem) reapStaleCacheEntries() {
	now := time.Now()
	var staleEntries []*cachedFile
	var stalePaths []string

	// Find stale entries (hold cacheMu briefly)
	f.cacheMu.RLock()
	cacheSize := len(f.fileCache)
	for path, cached := range f.fileCache {
		cached.mu.RLock()
		idle := now.Sub(cached.lastActivity)
		isStale := idle > f.cacheIdleTimeout()
		cached.mu.RUnlock()

		if isStale {
			staleEntries = append(staleEntries, cached)
			stalePaths = append(stalePaths, path)
		}
	}
	f.cacheMu.RUnlock()

	logging.Debug("Cache reaper tick",
		zap.Int("cacheSize", cacheSize),
		zap.Int("staleCount", len(staleEntries)))

	if len(staleEntries) == 0 {
		return
	}

	logging.Debug("Reaper found stale entries",
		zap.Int("count", len(staleEntries)))

	// Commit and remove stale entries
	for i, cached := range staleEntries {
		path := stalePaths[i]

		cached.mu.Lock()
		dirty := cached.dirty
		data := cached.data
		ops := cached.ops
		isDDLSQL := cached.isDDLSQL
		cached.mu.Unlock()

		// Commit if dirty (same logic as Close)
		if dirty && ops != nil && len(data) > 0 {
			logging.Debug("Reaper committing stale entry",
				zap.String("path", path),
				zap.Int("size", len(data)))

			wctx, wcancel := ctx()
			fsErr := ops.WriteFile(wctx, path, data)
			wcancel()
			if fsErr != nil {
				logging.Error("Reaper commit failed",
					zap.String("path", path),
					zap.Error(fsErr.Cause))
			}
		} else if dirty && ops != nil && len(data) == 0 && isDDLSQL {
			// DDL sql files need empty write committed
			logging.Debug("Reaper committing empty DDL sql file",
				zap.String("path", path))
			wctx, wcancel := ctx()
			ops.WriteFile(wctx, path, data)
			wcancel()
		}

		// Remove from cache
		f.cacheMu.Lock()
		delete(f.fileCache, path)
		f.cacheMu.Unlock()

		logging.Debug("Reaper evicted stale entry",
			zap.String("path", path))
	}
}

// Close stops the cache reaper and flushes all dirty cache entries.
//
// This should be called on graceful shutdown to ensure no data is lost.
// After Close returns, no more writes will be accepted and the cache is empty.
//
// Close is safe to call multiple times; subsequent calls are no-ops.
func (f *OpsFilesystem) Close() error {
	// Stop reaper if running
	if f.reaperStop != nil {
		close(f.reaperStop)
		f.reaperStop = nil
	}

	// Flush all dirty entries
	f.cacheMu.Lock()
	entries := make([]*cachedFile, 0, len(f.fileCache))
	paths := make([]string, 0, len(f.fileCache))
	for path, cached := range f.fileCache {
		entries = append(entries, cached)
		paths = append(paths, path)
	}
	f.cacheMu.Unlock()

	if len(entries) == 0 {
		logging.Debug("OpsFilesystem.Close: no cached entries to flush")
		return nil
	}

	logging.Debug("OpsFilesystem.Close: flushing cached entries",
		zap.Int("count", len(entries)))

	var lastErr error
	for i, cached := range entries {
		path := paths[i]

		cached.mu.Lock()
		dirty := cached.dirty
		data := cached.data
		ops := cached.ops
		isDDLSQL := cached.isDDLSQL
		isTrigger := cached.isTrigger
		cached.mu.Unlock()

		// Skip non-dirty entries (unless trigger)
		if !dirty && !isTrigger {
			continue
		}

		// Skip empty non-DDL entries
		if len(data) == 0 && !isDDLSQL && !isTrigger {
			continue
		}

		if ops != nil {
			logging.Debug("OpsFilesystem.Close: flushing entry",
				zap.String("path", path),
				zap.Int("size", len(data)))

			wctx, wcancel := ctx()
			fsErr := ops.WriteFile(wctx, path, data)
			wcancel()
			if fsErr != nil {
				logging.Error("OpsFilesystem.Close: flush failed",
					zap.String("path", path),
					zap.Error(fsErr.Cause))
				lastErr = fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
			}
		}
	}

	// Clear cache
	f.cacheMu.Lock()
	f.fileCache = make(map[string]*cachedFile)
	f.cacheMu.Unlock()

	logging.Debug("OpsFilesystem.Close: cache cleared")
	return lastErr
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
//
// This creates or retrieves a cached file entry (ADR-010) and returns a memFile
// handle for writing. The file is cached in memory until all handles close,
// then committed once to the database.
func (f *OpsFilesystem) Create(filename string) (billy.File, error) {
	filename = normalizePath(filename)
	logging.Info("OpsFilesystem.Create", zap.String("filename", filename))

	// Get or create cached file entry with O_CREATE|O_TRUNC semantics
	cached := f.getOrCreateCachedFile(filename, os.O_CREATE|os.O_TRUNC)

	// If this is a fresh create (truncated flag set), clear any existing data
	cached.mu.Lock()
	if cached.truncated {
		cached.data = []byte{}
	}
	cached.mu.Unlock()

	mf := &memFile{
		name:     filename,
		offset:   0,
		writable: true,
		fs:       f,
		cached:   cached,
	}

	logging.Debug("OpsFilesystem.Create: created memFile with cache",
		zap.String("path", filename),
		zap.Int("refCount", cached.refCount))

	return mf, nil
}

// Open opens a file for reading.
func (f *OpsFilesystem) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile opens a file with the specified flags and mode.
//
// This uses the persistent file cache (ADR-010) to enable efficient large file
// writes. Multiple opens of the same file share a cachedFile entry via reference
// counting. Data is committed to the database only when the last handle closes.
//
// # Flag Handling
//
//   - O_TRUNC: Clears cached data, sets truncated=true to prevent DB read
//   - O_CREATE: Creates new cached entry if file doesn't exist
//   - O_WRONLY/O_RDWR: Enables writing to the cached buffer
//
// # Special Cases
//
//   - DDL trigger files (.test, .commit, .abort): Always trigger on close
//   - DDL sql files: Empty writes must persist to clear session state
//   - Row files (JSON, CSV, etc.): Writes at offset 0 replace entire buffer
func (f *OpsFilesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = normalizePath(filename)
	logging.Info("OpsFilesystem.OpenFile", zap.String("filename", filename), zap.Int("flag", flag))

	isWrite := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0

	// Check if file is already in cache (includes in-flight and truncated files)
	cached := f.getCachedFile(filename)
	if cached != nil {
		if !isWrite {
			// Read-only: bypass cache, read from DB for proper formatting
			// (e.g., trailing newline for column files). Cache entry stays
			// for Stat/write use. Fall through to DB read below.
			logging.Debug("OpsFilesystem.OpenFile: read-only, bypassing cache for DB read",
				zap.String("filename", filename))
		} else {
			// Write: use cache (increment refCount, existing behavior)
			cached.mu.Lock()
			cached.refCount++
			cached.lastActivity = time.Now()

			// Handle O_TRUNC on already-cached file
			if flag&os.O_TRUNC != 0 {
				cached.data = []byte{}
				cached.truncated = true
				logging.Debug("OpsFilesystem.OpenFile: truncated cached file",
					zap.String("filename", filename))
			}

			refCount := cached.refCount
			cached.mu.Unlock()

			logging.Debug("OpsFilesystem.OpenFile: cache hit (write)",
				zap.String("filename", filename),
				zap.Int("refCount", refCount),
				zap.Int("flag", flag))

			return &memFile{
				name:     filename,
				offset:   0,
				writable: isWrite,
				fs:       f,
				cached:   cached,
			}, nil
		}
	}

	// File not in cache, or read-only open bypassing cache

	// For O_TRUNC or O_CREATE, start with empty data (don't read from DB)
	if flag&os.O_TRUNC != 0 || flag&os.O_CREATE != 0 {
		cached = f.getOrCreateCachedFile(filename, flag)

		logging.Debug("OpsFilesystem.OpenFile: created new cached entry (O_TRUNC or O_CREATE)",
			zap.String("filename", filename),
			zap.Int("flag", flag))

		return &memFile{
			name:     filename,
			offset:   0,
			writable: isWrite,
			fs:       f,
			cached:   cached,
		}, nil
	}

	// For DDL sql files opened for writing, start with empty data.
	// This handles the NFS truncate-before-write pattern where we don't want
	// to read the template from DDLManager.
	baseName := path.Base(filename)
	isDDLSQL := baseName == "sql" && isDDLPath(filename)
	if isDDLSQL && isWrite {
		cached = f.getOrCreateCachedFile(filename, flag)

		logging.Debug("OpsFilesystem.OpenFile: DDL sql file opened for write, starting empty",
			zap.String("filename", filename))

		return &memFile{
			name:     filename,
			offset:   0,
			writable: isWrite,
			fs:       f,
			cached:   cached,
		}, nil
	}

	// Try to read existing content from database
	rctx, rcancel := ctx()
	defer rcancel()
	content, fsErr := f.ops.ReadFile(rctx, filename)
	if fsErr != nil {
		if fsErr.Code == fs.ErrNotExist {
			// If O_CREATE was set, create empty file
			if flag&os.O_CREATE != 0 {
				cached = f.getOrCreateCachedFile(filename, flag)

				logging.Debug("OpsFilesystem.OpenFile: created new cached entry (file not exist, O_CREATE)",
					zap.String("filename", filename))

				return &memFile{
					name:     filename,
					offset:   0,
					writable: isWrite,
					fs:       f,
					cached:   cached,
				}, nil
			}
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
	}

	// File exists in DB
	if isWrite {
		// Write mode: use cache for sharing and commit-on-close
		cached = f.getOrCreateCachedFile(filename, flag)
		cached.mu.Lock()
		// Only load data if this is the first open (data is still empty)
		// If another handle already loaded data, use that
		if len(cached.data) == 0 && !cached.truncated {
			cached.data = content.Data
		}
		cached.mu.Unlock()

		logging.Debug("OpsFilesystem.OpenFile: loaded from DB into cache (write mode)",
			zap.String("filename", filename),
			zap.Int("size", len(content.Data)))

		return &memFile{
			name:     filename,
			offset:   0,
			writable: true,
			fs:       f,
			cached:   cached,
		}, nil
	}

	// Read-only mode: use private data buffer, no cache
	logging.Debug("OpsFilesystem.OpenFile: read-only, using private buffer",
		zap.String("filename", filename),
		zap.Int("size", len(content.Data)))

	return &memFile{
		name:     filename,
		offset:   0,
		writable: false,
		fs:       f,
		data:     content.Data,
	}, nil
}

// Stat returns file info for the given path.
//
// For cached files (ADR-010), returns the current buffer size rather than
// querying the database. This ensures NFS sees correct sizes for files
// being written but not yet committed.
func (f *OpsFilesystem) Stat(filename string) (os.FileInfo, error) {
	filename = normalizePath(filename)

	// Check for cached files with dirty or newly-created (truncated) data.
	// NFS calls Stat after Write but before Close, and the data may not be in
	// the database yet. Return info from cache for these in-flight files.
	// Also return cache info for truncated (newly-created) entries so that
	// go-nfs's Apply() in the CREATE handler can Lstat the file after Close.
	// Clean (committed, non-truncated) cache entries are skipped — Stat falls
	// through to DB to get the properly formatted size.
	cached := f.getCachedFile(filename)
	if cached != nil {
		cached.mu.RLock()
		dirty := cached.dirty
		truncated := cached.truncated
		size := int64(len(cached.data))
		lastAct := cached.lastActivity
		cached.mu.RUnlock()

		if dirty || truncated {
			logging.Debug("OpsFilesystem.Stat: returning cached file info",
				zap.String("filename", filename),
				zap.Int64("size", size),
				zap.Bool("dirty", dirty),
				zap.Bool("truncated", truncated))
			return &inFlightFileInfo{
				name:    path.Base(filename),
				size:    size,
				mode:    0644,
				modTime: lastAct,
				path:    filename,
			}, nil
		}
		// Clean cache entry — fall through to DB for accurate size
		logging.Debug("OpsFilesystem.Stat: clean cache entry, falling through to DB",
			zap.String("filename", filename))
	}

	sctx, scancel := ctx()
	defer scancel()
	entry, fsErr := f.ops.Stat(sctx, filename)
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
	return fi, nil
}

// Rename moves a file from oldpath to newpath.
//
// For synth views, this updates the filename column. For native tables,
// this updates the primary key value. If the old path has a cache entry,
// it is removed (the renamed file will have a different path).
func (f *OpsFilesystem) Rename(oldpath, newpath string) error {
	oldpath = normalizePath(oldpath)
	newpath = normalizePath(newpath)
	logging.Info("OpsFilesystem.Rename", zap.String("oldpath", oldpath), zap.String("newpath", newpath))

	rctx, rcancel := ctx()
	defer rcancel()
	fsErr := f.ops.Rename(rctx, oldpath, newpath)
	if fsErr != nil {
		logging.Debug("OpsFilesystem.Rename error",
			zap.String("oldpath", oldpath),
			zap.String("newpath", newpath),
			zap.Int("code", int(fsErr.Code)),
			zap.String("message", fsErr.Message))
		switch fsErr.Code {
		case fs.ErrNotExist:
			return os.ErrNotExist
		case fs.ErrPermission:
			return os.ErrPermission
		case fs.ErrInvalidPath:
			return os.ErrInvalid
		default:
			return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
		}
	}

	// Invalidate cache for the old path (the file now lives at newpath)
	f.cacheMu.Lock()
	delete(f.fileCache, oldpath)
	f.cacheMu.Unlock()

	return nil
}

// Remove removes a file.
//
// If the file is currently cached (ADR-010), it is marked as deleted.
// Open handles will receive EIO on subsequent writes. The cached entry
// is removed when the last handle closes.
func (f *OpsFilesystem) Remove(filename string) error {
	filename = normalizePath(filename)
	logging.Info("OpsFilesystem.Remove", zap.String("filename", filename))

	// Mark cached file as deleted if it exists
	cached := f.getCachedFile(filename)
	if cached != nil {
		cached.mu.Lock()
		cached.deleted = true
		cached.mu.Unlock()
		logging.Debug("OpsFilesystem.Remove: marked cached file as deleted",
			zap.String("filename", filename))
	}

	dctx, dcancel := ctx()
	defer dcancel()
	fsErr := f.ops.Delete(dctx, filename)
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
	logging.Info("OpsFilesystem.ReadDir", zap.String("dirname", dirname))

	rdctx, rdcancel := ctx()
	defer rdcancel()
	entries, fsErr := f.ops.ReadDir(rdctx, dirname)
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

	logging.Info("OpsFilesystem.ReadDir success",
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

	mctx, mcancel := ctx()
	defer mcancel()
	fsErr := f.ops.Mkdir(mctx, filename)
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

// Chtimes handles timestamp changes.
// For DDL trigger files (.test, .commit, .abort), this fires the DDL operation.
// NFS `touch` uses SETATTR to set timestamps, which maps to Chtimes — not OpenFile.
// Without this, `touch .test` would be a no-op and DDL validation would never trigger.
func (f *OpsFilesystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	name = normalizePath(name)
	logging.Debug("OpsFilesystem.Chtimes", zap.String("name", name), zap.Time("atime", atime), zap.Time("mtime", mtime))

	// Check if this is a DDL trigger file
	baseName := path.Base(name)
	isTrigger := (baseName == ".test" || baseName == ".commit" || baseName == ".abort") && isDDLPath(name)

	if isTrigger {
		// Check if the trigger was already fired by memFile.Close() (NFS touch
		// causes both OpenFile+Close and Chtimes, both of which would fire).
		cached := f.getCachedFile(name)
		if cached != nil {
			cached.mu.RLock()
			alreadyTriggered := cached.triggered
			cached.mu.RUnlock()
			if alreadyTriggered {
				logging.Debug("OpsFilesystem.Chtimes: skipping duplicate DDL trigger",
					zap.String("name", name))
				f.removeFromCache(name)
				return nil
			}
		}

		logging.Debug("OpsFilesystem.Chtimes: triggering DDL operation",
			zap.String("name", name))
		wctx, wcancel := ctx()
		defer wcancel()
		fsErr := f.ops.WriteFile(wctx, name, []byte{})
		if fsErr != nil {
			logging.Error("OpsFilesystem.Chtimes: DDL trigger failed",
				zap.String("name", name),
				zap.Error(fsErr.Cause))
			return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
		}
	}

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

// ctx returns a context with a 30-second timeout for NFS operations.
// go-nfs doesn't provide request-scoped contexts (unlike go-fuse which passes
// kernel contexts through node methods), so we add our own deadline to prevent
// operations from blocking indefinitely when the database is slow or unreachable.
func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
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

// memFile is an in-memory file handle for reading and writing row data.
//
// For write operations, memFile wraps a shared cachedFile entry. Multiple
// memFile handles can point to the same cachedFile (reference counted).
//
// For read-only operations, memFile holds a private data buffer that is NOT
// added to the global fileCache. This prevents cache bloat when reading many
// files (e.g., `cat /mnt/db/table/*/col.txt`).
//
// # Lifecycle
//
//   - Write mode: cached != nil, data operations go through cached.data
//   - Read-only mode: cached == nil, data operations use private data buffer
//   - Close in write mode: decrements cached.refCount, commits when refCount=0
//   - Close in read-only mode: no-op (just discards private buffer)
type memFile struct {
	name     string         // full normalized path for Name() method (required by go-nfs)
	offset   int64          // per-handle read/write position
	writable bool           // true if opened for writing
	fs       *OpsFilesystem // parent filesystem for cache management

	// Shared cached file entry (ADR-010) - used for write operations only.
	// When cached is nil, this is a read-only handle with private data.
	cached *cachedFile

	// Private data buffer for read-only opens (when cached is nil).
	// This avoids adding read-only files to the global fileCache.
	data []byte
}

func (f *memFile) Name() string {
	return f.name
}

func (f *memFile) Read(p []byte) (int, error) {
	// Read-only mode: use private data buffer
	if f.cached == nil {
		if f.offset >= int64(len(f.data)) {
			return 0, io.EOF
		}
		n := copy(p, f.data[f.offset:])
		f.offset += int64(n)
		return n, nil
	}

	// Write mode: use shared cached data
	f.cached.mu.RLock()
	defer f.cached.mu.RUnlock()

	if f.offset >= int64(len(f.cached.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.cached.data[f.offset:])
	f.offset += int64(n)
	f.cached.lastActivity = time.Now()
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	// Read-only mode: use private data buffer
	if f.cached == nil {
		if off >= int64(len(f.data)) {
			return 0, io.EOF
		}
		n := copy(p, f.data[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	// Write mode: use shared cached data
	f.cached.mu.RLock()
	defer f.cached.mu.RUnlock()

	if off >= int64(len(f.cached.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.cached.data[off:])
	f.cached.lastActivity = time.Now()
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	var dataLen int64
	if f.cached == nil {
		// Read-only mode: use private data buffer
		dataLen = int64(len(f.data))
	} else {
		// Write mode: use shared cached data
		f.cached.mu.RLock()
		dataLen = int64(len(f.cached.data))
		f.cached.mu.RUnlock()
	}

	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = dataLen + offset
	}
	return f.offset, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	if !f.writable || f.cached == nil {
		return 0, fmt.Errorf("file not opened for writing")
	}

	f.cached.mu.Lock()

	// Check if file was deleted while we had it open
	if f.cached.deleted {
		f.cached.mu.Unlock()
		return 0, fmt.Errorf("file was deleted")
	}

	// For row files (JSON, CSV, etc.), writes replace the entire content.
	// This handles the case where NFS reads existing data, then writes new data
	// at offset 0. Without replacement semantics, the buffer would retain any
	// old data beyond the new content length, causing JSON parsing errors.
	if f.cached.isRowFile && f.offset == 0 {
		f.cached.data = make([]byte, len(p))
		copy(f.cached.data, p)
		f.offset = int64(len(p))
		f.cached.dirty = true
		f.cached.lastActivity = time.Now()
		f.cached.mu.Unlock()
		return len(p), nil
	}

	// Track whether writes are sequential (for large file streaming optimization)
	if f.offset != int64(len(f.cached.data)) {
		f.cached.isSequential = false
	}

	// Check size limits for random (non-sequential) writes
	newLen := f.offset + int64(len(p))
	if !f.cached.isSequential && newLen > f.fs.maxRandomWriteSize() {
		f.cached.mu.Unlock()
		logging.Warn("memFile.Write: random write exceeds size limit",
			zap.String("path", f.cached.path),
			zap.Int64("newLen", newLen),
			zap.Int64("limit", f.fs.maxRandomWriteSize()))
		return 0, syscall.EFBIG
	}

	// Expand buffer if needed
	if newLen > int64(len(f.cached.data)) {
		newData := make([]byte, newLen)
		copy(newData, f.cached.data)
		f.cached.data = newData
	}

	// Write data at current offset
	copy(f.cached.data[f.offset:], p)
	f.offset += int64(len(p))
	f.cached.dirty = true
	f.cached.lastActivity = time.Now()

	// Check if we should stream (commit and clear) for large sequential writes
	// This prevents memory exhaustion for very large files like dd output
	if f.cached.isSequential && int64(len(f.cached.data)) > f.fs.streamingThreshold() {
		// Need to commit outside the lock to avoid holding it during I/O
		data := f.cached.data
		path := f.cached.path
		ops := f.cached.ops
		f.cached.mu.Unlock()

		if ops != nil && path != "" {
			logging.Debug("memFile.Write: streaming commit",
				zap.String("path", path),
				zap.Int("size", len(data)))

			wctx, wcancel := ctx()
			fsErr := ops.WriteFile(wctx, path, data)
			wcancel()
			if fsErr != nil {
				logging.Error("memFile.Write: streaming commit failed",
					zap.String("path", path),
					zap.Error(fsErr.Cause))
				return 0, fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
			}

			// Clear buffer after successful commit
			f.cached.mu.Lock()
			f.cached.data = []byte{}
			f.cached.dirty = false
			f.cached.streamed = true
			f.cached.lastActivity = time.Now()
			f.offset = 0 // Reset offset since buffer is cleared
			f.cached.mu.Unlock()

			logging.Debug("memFile.Write: streaming commit complete, buffer cleared",
				zap.String("path", path))
		} else {
			// No ops, just clear buffer (for testing)
			f.cached.mu.Lock()
			f.cached.data = []byte{}
			f.cached.dirty = false
			f.cached.streamed = true
			f.offset = 0
			f.cached.mu.Unlock()
		}

		return len(p), nil
	}

	f.cached.mu.Unlock()
	return len(p), nil
}

func (f *memFile) Close() error {
	// Read-only mode: no cache to manage, just discard private buffer
	if f.cached == nil {
		f.data = nil
		return nil
	}

	// Write mode: manage cache reference counting and commits
	f.cached.mu.Lock()
	filePath := f.cached.path
	dirty := f.cached.dirty
	dataLen := len(f.cached.data)
	isTrigger := f.cached.isTrigger
	isDDLSQL := f.cached.isDDLSQL
	ops := f.cached.ops

	logging.Debug("memFile.Close",
		zap.String("path", filePath),
		zap.Bool("dirty", dirty),
		zap.Int("size", dataLen),
		zap.Bool("isTrigger", isTrigger),
		zap.Bool("isDDLSQL", isDDLSQL),
		zap.Int("refCount", f.cached.refCount))

	// Decrement reference count
	f.cached.refCount--
	refCount := f.cached.refCount
	f.cached.lastActivity = time.Now()
	f.cached.mu.Unlock()

	// If there are still other handles open, don't commit yet
	if refCount > 0 {
		logging.Debug("memFile.Close: other handles still open, deferring commit",
			zap.String("path", filePath),
			zap.Int("refCount", refCount))
		return nil
	}

	// Last handle closed - commit if dirty
	// Re-acquire lock to read final state and clear dirty flag
	f.cached.mu.Lock()
	dirty = f.cached.dirty
	data := f.cached.data
	deleted := f.cached.deleted
	f.cached.mu.Unlock()

	// If file was deleted while open, discard changes
	if deleted {
		logging.Debug("memFile.Close: file was deleted, discarding changes",
			zap.String("path", filePath))
		f.fs.removeFromCache(filePath)
		return nil
	}

	// For DDL trigger files, ALWAYS write to trigger the operation.
	// The trigger happens regardless of whether the file was dirtied or has content.
	// Mark as triggered so Chtimes can skip the duplicate fire (NFS touch causes
	// both Close and Chtimes, and both would otherwise fire the trigger).
	if isTrigger && ops != nil && filePath != "" {
		logging.Debug("memFile.Close: DDL trigger file, triggering operation",
			zap.String("path", filePath))

		f.cached.mu.Lock()
		f.cached.triggered = true
		f.cached.mu.Unlock()

		wctx, wcancel := ctx()
		fsErr := ops.WriteFile(wctx, filePath, []byte{})
		wcancel()
		if fsErr != nil {
			logging.Error("memFile.Close: DDL trigger failed",
				zap.String("path", filePath),
				zap.Error(fsErr.Cause))
			// Don't return error - let NFS operations complete normally.
			// The DDL operation failure is logged but shouldn't cause NFS protocol errors.
		}
		// Keep cache entry briefly so Chtimes can see the triggered flag.
		// Reaper will clean it up.
		return nil
	}

	if dirty && ops != nil && filePath != "" {
		// NFS SETATTR truncation: When NFS truncates a file before writing (the
		// SETATTR→WRITE pattern), Close is called with empty data between the two
		// RPCs. Committing empty data would either fail (row files need valid
		// JSON/CSV) or corrupt data (column files set to empty string). Keep the
		// cache entry alive and dirty so the subsequent WRITE RPC can fill it with
		// actual data and commit successfully.
		// DDL sql files are excluded because empty writes to DDL sql are meaningful
		// (they clear the session state).
		if len(data) == 0 && !isDDLSQL {
			logging.Debug("memFile.Close: skipping empty commit (SETATTR truncation pattern)",
				zap.String("path", filePath))
			return nil
		}

		// WORKAROUND(go-nfs O(n²) writes): go-nfs calls Close() after every WRITE
		// RPC, so we commit the full accumulated buffer each time. This produces
		// O(n²) total DB write volume for multi-chunk writes. The proper fix is to
		// patch go-nfs to return UNSTABLE from WRITE and flush on COMMIT (see Task
		// 10.4 in docs/implementation/implementation-tasks.md). For now this is
		// acceptable because wsize=128KB reduces RPC count by 4x vs macOS default,
		// and per-RPC overhead is reduced to ~1 DB write via the schema cache and
		// Stat cache. See docs/spec.md § NFS Write Limitations.

		logging.Info("memFile.Close: committing dirty data to DB",
			zap.String("path", filePath),
			zap.Int("size", len(data)),
			zap.Bool("isDDLSQL", isDDLSQL))

		wctx, wcancel := ctx()
		fsErr := ops.WriteFile(wctx, filePath, data)
		wcancel()
		if fsErr != nil {
			logging.Error("memFile.Close WriteFile failed",
				zap.String("path", filePath),
				zap.Error(fsErr.Cause))
			f.fs.removeFromCache(filePath)
			return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
		}

		// Mark cache entry clean but keep it alive for Stat to use.
		// The reaper will evict it after idle timeout.
		f.cached.mu.Lock()
		f.cached.dirty = false
		f.cached.truncated = false
		f.cached.mu.Unlock()
		// Do NOT call removeFromCache — let reaper handle eviction.
		return nil
	}

	// File wasn't dirty - keep in cache for Stat to use (reaper handles eviction).
	// This covers the truncate-before-write pattern and clean cache entries.
	return nil
}

func (f *memFile) Lock() error {
	return nil
}

func (f *memFile) Unlock() error {
	return nil
}

// Sync commits the current buffer to the database immediately.
//
// This is called when an editor saves (fsync) while the file is still open.
// Unlike Close(), Sync does not decrement the reference count or remove
// the file from cache - it just ensures current changes are persisted.
//
// # Behavior
//
//   - If dirty and has data: commit to DB, clear dirty flag
//   - If not dirty or empty: no-op
//   - File remains in cache for continued editing
//
// # Thread Safety
//
// Acquires cached.mu to read/update state. The actual DB write happens
// outside the lock to avoid holding it during I/O.
func (f *memFile) Sync() error {
	// Read-only mode: nothing to sync
	if f.cached == nil {
		return nil
	}

	f.cached.mu.Lock()
	filePath := f.cached.path
	dirty := f.cached.dirty
	data := f.cached.data
	ops := f.cached.ops
	isDDLSQL := f.cached.isDDLSQL
	deleted := f.cached.deleted
	f.cached.mu.Unlock()

	logging.Debug("memFile.Sync",
		zap.String("path", filePath),
		zap.Bool("dirty", dirty),
		zap.Int("size", len(data)))

	// If file was deleted, nothing to sync
	if deleted {
		logging.Debug("memFile.Sync: file was deleted, skipping",
			zap.String("path", filePath))
		return nil
	}

	// If not dirty, nothing to sync
	if !dirty {
		logging.Debug("memFile.Sync: not dirty, skipping",
			zap.String("path", filePath))
		return nil
	}

	// Skip empty content unless it's a DDL sql file
	// (same logic as Close for truncate-before-write pattern)
	if len(data) == 0 && !isDDLSQL {
		logging.Debug("memFile.Sync: empty content, skipping",
			zap.String("path", filePath))
		return nil
	}

	// Commit to database
	if ops != nil && filePath != "" {
		logging.Debug("memFile.Sync: committing to database",
			zap.String("path", filePath),
			zap.Int("size", len(data)))

		wctx, wcancel := ctx()
		fsErr := ops.WriteFile(wctx, filePath, data)
		wcancel()
		if fsErr != nil {
			logging.Error("memFile.Sync WriteFile failed",
				zap.String("path", filePath),
				zap.Error(fsErr.Cause))
			return fmt.Errorf("%s: %w", fsErr.Message, fsErr.Cause)
		}

		// Clear dirty flag after successful commit
		f.cached.mu.Lock()
		f.cached.dirty = false
		f.cached.lastActivity = time.Now()
		f.cached.mu.Unlock()

		logging.Debug("memFile.Sync: commit successful",
			zap.String("path", filePath))
	}

	return nil
}

func (f *memFile) Truncate(size int64) error {
	if !f.writable || f.cached == nil {
		return fmt.Errorf("file not opened for writing")
	}

	f.cached.mu.Lock()
	defer f.cached.mu.Unlock()

	logging.Debug("memFile.Truncate",
		zap.String("path", f.cached.path),
		zap.Int64("newSize", size),
		zap.Int("currentSize", len(f.cached.data)))

	if size < int64(len(f.cached.data)) {
		f.cached.data = f.cached.data[:size]
	} else if size > int64(len(f.cached.data)) {
		newData := make([]byte, size)
		copy(newData, f.cached.data)
		f.cached.data = newData
	}
	f.cached.dirty = true
	f.cached.lastActivity = time.Now()
	return nil
}
