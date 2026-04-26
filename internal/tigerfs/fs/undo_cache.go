package fs

import (
	"sync"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// undoCacheTTL matches the stat cache TTL (2 seconds).
const undoCacheTTL = 2 * time.Second

// undoCache provides short-lived caching for undo preview queries.
// Each NFS/FUSE RPC independently queries the same data; caching eliminates
// redundant DB queries within a single user operation (ls, cat, touch).
type undoCache struct {
	mu sync.RWMutex

	// affectedFiles caches queryUndoAffected results.
	// Key: "mode\x00target\x00filters" (e.g., "to-savepoint\x00before-edits\x00")
	affectedFiles map[string]*affectedCacheEntry

	// savepointRows caches savepoint GetRow results (name → full row).
	// Key: "schema\x00table\x00name"
	savepointRows map[string]*savepointCacheEntry

	// logEntries caches QueryLogEntry results.
	// Key: "schema\x00table\x00logID"
	logEntries map[string]*logEntryCacheEntry
}

type affectedCacheEntry struct {
	files   []db.UndoAffectedFile
	created time.Time
}

type savepointCacheEntry struct {
	row     *db.Row
	created time.Time
}

type logEntryCacheEntry struct {
	entry   *db.UndoAffectedFile
	created time.Time
}

// lookupAffected returns cached affected files if present and not expired.
func (c *undoCache) lookupAffected(mode, target, filterKey string) ([]db.UndoAffectedFile, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.affectedFiles == nil {
		return nil, false
	}
	key := mode + "\x00" + target + "\x00" + filterKey
	entry := c.affectedFiles[key]
	if entry == nil || time.Since(entry.created) > undoCacheTTL {
		return nil, false
	}
	return entry.files, true
}

// storeAffected caches affected files for a given undo target.
func (c *undoCache) storeAffected(mode, target, filterKey string, files []db.UndoAffectedFile) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.affectedFiles == nil {
		c.affectedFiles = make(map[string]*affectedCacheEntry)
	}
	key := mode + "\x00" + target + "\x00" + filterKey
	c.affectedFiles[key] = &affectedCacheEntry{files: files, created: time.Now()}
}

// lookupSavepoint returns a cached savepoint row if present and not expired.
func (c *undoCache) lookupSavepoint(schema, table, name string) (*db.Row, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.savepointRows == nil {
		return nil, false
	}
	key := schema + "\x00" + table + "\x00" + name
	entry := c.savepointRows[key]
	if entry == nil || time.Since(entry.created) > undoCacheTTL {
		return nil, false
	}
	return entry.row, true
}

// storeSavepoint caches a savepoint row.
func (c *undoCache) storeSavepoint(schema, table, name string, row *db.Row) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.savepointRows == nil {
		c.savepointRows = make(map[string]*savepointCacheEntry)
	}
	key := schema + "\x00" + table + "\x00" + name
	c.savepointRows[key] = &savepointCacheEntry{row: row, created: time.Now()}
}

// lookupLogEntry returns a cached log entry if present and not expired.
func (c *undoCache) lookupLogEntry(schema, table, logID string) (*db.UndoAffectedFile, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.logEntries == nil {
		return nil, false
	}
	key := schema + "\x00" + table + "\x00" + logID
	entry := c.logEntries[key]
	if entry == nil || time.Since(entry.created) > undoCacheTTL {
		return nil, false
	}
	return entry.entry, true
}

// storeLogEntry caches a log entry.
func (c *undoCache) storeLogEntry(schema, table, logID string, entry *db.UndoAffectedFile) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.logEntries == nil {
		c.logEntries = make(map[string]*logEntryCacheEntry)
	}
	key := schema + "\x00" + table + "\x00" + logID
	c.logEntries[key] = &logEntryCacheEntry{entry: entry, created: time.Now()}
}

// lookupHistoryRow returns a cached history row if present and not expired.
func (c *undoCache) lookupHistoryRow(schema, table, versionID string) (*db.Row, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.savepointRows == nil {
		return nil, false
	}
	// Reuse savepointRows map for history rows (same structure: key→*db.Row)
	key := "history\x00" + schema + "\x00" + table + "\x00" + versionID
	entry := c.savepointRows[key]
	if entry == nil || time.Since(entry.created) > undoCacheTTL {
		return nil, false
	}
	return entry.row, true
}

// storeHistoryRow caches a history row.
func (c *undoCache) storeHistoryRow(schema, table, versionID string, row *db.Row) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.savepointRows == nil {
		c.savepointRows = make(map[string]*savepointCacheEntry)
	}
	key := "history\x00" + schema + "\x00" + table + "\x00" + versionID
	c.savepointRows[key] = &savepointCacheEntry{row: row, created: time.Now()}
}

// invalidate clears all undo caches. Called after undo execution.
//
// Concurrency note: there is a narrow window where a concurrent reader
// can re-populate a cache entry with pre-undo data. If a reader's
// QueryUndoAffectedFiles starts before the undo transaction COMMITs (so
// PG's READ COMMITTED snapshot reflects pre-undo state), then the writer
// commits and runs invalidate(), then the reader's query finally returns,
// the reader calls storeAffected() with stale rows. Subsequent lookups
// return that stale snapshot until the 2-second TTL expires.
//
// This is acceptable: the staleness only affects undo *preview* surfaces
// (.info/summary, .undo/<target>/<preview>), not the apply path itself,
// which re-queries fresh inside ExecuteUndoTransaction. The underlying
// log/savepoint/history rows are append-only or immutable, so stale
// log/savepoint/history cache entries reflect data that's still valid;
// only affectedFiles can become semantically stale.
//
// See docs/spec.md "Undo Preview Cache" for user-facing documentation
// of the same tradeoff.
func (c *undoCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.affectedFiles = nil
	c.savepointRows = nil
	c.logEntries = nil
}
