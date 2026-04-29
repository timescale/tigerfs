package fs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPathCache_LookupMiss(t *testing.T) {
	var c pathCache
	id, ok := c.lookup("s", "t", "", "foo")
	assert.False(t, ok)
	assert.Empty(t, id)
}

func TestPathCache_PutAndLookup(t *testing.T) {
	var c pathCache
	c.put("s", "t", "", "projects", "uuid-1")
	c.put("s", "t", "uuid-1", "web", "uuid-2")
	c.put("s", "t", "uuid-2", "todo.md", "uuid-3")

	// All three levels should be cached
	id, ok := c.lookup("s", "t", "", "projects")
	assert.True(t, ok)
	assert.Equal(t, "uuid-1", id)

	id, ok = c.lookup("s", "t", "uuid-1", "web")
	assert.True(t, ok)
	assert.Equal(t, "uuid-2", id)

	id, ok = c.lookup("s", "t", "uuid-2", "todo.md")
	assert.True(t, ok)
	assert.Equal(t, "uuid-3", id)
}

func TestPathCache_SeparateTableNamespaces(t *testing.T) {
	var c pathCache
	c.put("s", "notes", "", "hello", "uuid-a")
	c.put("s", "blog", "", "hello", "uuid-b")

	id, ok := c.lookup("s", "notes", "", "hello")
	assert.True(t, ok)
	assert.Equal(t, "uuid-a", id)

	id, ok = c.lookup("s", "blog", "", "hello")
	assert.True(t, ok)
	assert.Equal(t, "uuid-b", id)
}

func TestPathCache_LookupMissWrongFilename(t *testing.T) {
	var c pathCache
	c.put("s", "t", "", "foo", "uuid-1")

	_, ok := c.lookup("s", "t", "", "bar")
	assert.False(t, ok)
}

func TestPathCache_LookupMissWrongParent(t *testing.T) {
	var c pathCache
	c.put("s", "t", "uuid-1", "foo", "uuid-2")

	_, ok := c.lookup("s", "t", "uuid-99", "foo")
	assert.False(t, ok)
}

func TestPathCache_Invalidate(t *testing.T) {
	var c pathCache
	c.put("s", "t", "", "foo", "uuid-1")

	// Confirm cached
	_, ok := c.lookup("s", "t", "", "foo")
	assert.True(t, ok)

	// Invalidate
	c.invalidate("s", "t")

	_, ok = c.lookup("s", "t", "", "foo")
	assert.False(t, ok)
}

func TestPathCache_InvalidateOnlyAffectsTargetTable(t *testing.T) {
	var c pathCache
	c.put("s", "notes", "", "foo", "uuid-1")
	c.put("s", "blog", "", "foo", "uuid-2")

	c.invalidate("s", "notes")

	_, ok := c.lookup("s", "notes", "", "foo")
	assert.False(t, ok)

	id, ok := c.lookup("s", "blog", "", "foo")
	assert.True(t, ok)
	assert.Equal(t, "uuid-2", id)
}

func TestPathCache_TTLExpiry(t *testing.T) {
	// Temporarily reduce TTL for this test by manipulating the cache internals.
	// We test the TTL check by creating a table cache with an old timestamp.
	var c pathCache
	c.mu.Lock()
	c.tables = map[string]*pathTableCache{
		"s\x00t": {
			entries: map[pathCacheKey]pathCacheEntry{
				{ParentID: "", Filename: "foo"}: {ID: "uuid-1"},
			},
			created: time.Now().Add(-3 * time.Second), // older than 2s TTL
		},
	}
	c.mu.Unlock()

	_, ok := c.lookup("s", "t", "", "foo")
	assert.False(t, ok, "entries older than TTL should expire")
}

func TestPathCache_PutResetsExpiredTable(t *testing.T) {
	var c pathCache

	// Create an expired table cache
	c.mu.Lock()
	c.tables = map[string]*pathTableCache{
		"s\x00t": {
			entries: map[pathCacheKey]pathCacheEntry{
				{ParentID: "", Filename: "old"}: {ID: "uuid-old"},
			},
			created: time.Now().Add(-3 * time.Second),
		},
	}
	c.mu.Unlock()

	// Put into expired table should reset it
	c.put("s", "t", "", "new", "uuid-new")

	// Old entry should be gone (table was reset)
	_, ok := c.lookup("s", "t", "", "old")
	assert.False(t, ok)

	// New entry should exist
	id, ok := c.lookup("s", "t", "", "new")
	assert.True(t, ok)
	assert.Equal(t, "uuid-new", id)
}

func TestPathCache_RootParentID(t *testing.T) {
	var c pathCache

	// Root-level entries use empty string for parent_id
	c.put("s", "t", "", "root-file.md", "uuid-root")
	id, ok := c.lookup("s", "t", "", "root-file.md")
	assert.True(t, ok)
	assert.Equal(t, "uuid-root", id)

	// Nested entries use parent UUID
	c.put("s", "t", "uuid-root", "child.md", "uuid-child")
	id, ok = c.lookup("s", "t", "uuid-root", "child.md")
	assert.True(t, ok)
	assert.Equal(t, "uuid-child", id)
}
