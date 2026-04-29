package fs

import (
	"sync"
	"time"
)

// pathCacheTTL is the maximum age of cached path entries before they expire.
// Matches the stat cache TTL -- directory structure changes are visible
// to other mounts within this window.
const pathCacheTTL = 2 * time.Second

// pathCache maps (parentID, filename) -> row ID for the parent-pointer directory
// model (ADR-017). Used to avoid calling resolve_path for repeated access to the
// same directory subtree. Each table has its own cache namespace.
type pathCache struct {
	mu     sync.RWMutex
	tables map[string]*pathTableCache // key: "schema\x00table"
}

// pathTableCache holds cached path entries for a single table.
type pathTableCache struct {
	entries map[pathCacheKey]pathCacheEntry
	created time.Time
}

// pathCacheKey identifies a single path segment within a parent directory.
// Empty ParentID represents root level (NULL parent_id in the database).
type pathCacheKey struct {
	ParentID string // UUID of parent directory, empty for root
	Filename string // leaf filename
}

// pathCacheEntry holds a cached row ID with its expiration.
type pathCacheEntry struct {
	ID string // UUID of the resolved row
}

// lookup returns the cached row ID for a (parentID, filename) pair.
// Returns empty string and false if not found or expired.
func (c *pathCache) lookup(schema, table, parentID, filename string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.tables == nil {
		return "", false
	}

	tc := c.tables[schema+"\x00"+table]
	if tc == nil {
		return "", false
	}

	if time.Since(tc.created) > pathCacheTTL {
		return "", false
	}

	entry, ok := tc.entries[pathCacheKey{ParentID: parentID, Filename: filename}]
	if !ok {
		return "", false
	}

	return entry.ID, true
}

// put stores a (parentID, filename) -> id mapping in the cache.
func (c *pathCache) put(schema, table, parentID, filename, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tables == nil {
		c.tables = make(map[string]*pathTableCache)
	}

	key := schema + "\x00" + table
	tc := c.tables[key]
	if tc == nil || time.Since(tc.created) > pathCacheTTL {
		tc = &pathTableCache{
			entries: make(map[pathCacheKey]pathCacheEntry),
			created: time.Now(),
		}
		c.tables[key] = tc
	}

	tc.entries[pathCacheKey{ParentID: parentID, Filename: filename}] = pathCacheEntry{ID: id}
}

// invalidate removes all cached entries for a table. Called on directory
// renames/moves/deletes that may change the path structure.
func (c *pathCache) invalidate(schema, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tables != nil {
		delete(c.tables, schema+"\x00"+table)
	}
}
