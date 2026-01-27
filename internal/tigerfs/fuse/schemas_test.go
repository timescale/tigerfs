package fuse

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// setupTestSchemasNode creates a SchemasNode with a pre-populated cache for testing
func setupTestSchemasNode(schemas []string) *SchemasNode {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := &MetadataCache{
		cfg:               cfg,
		db:                nil,
		defaultSchema:     "public",
		schemas:           schemas,
		tables:            []string{},
		schemaTables:      make(map[string][]string),
		schemaRowCounts:   make(map[string]map[string]int64),
		schemaPermissions: make(map[string]map[string]*db.TablePermissions),
		schemaLastFetch:   make(map[string]time.Time),
		lastFetch:         time.Now(),
	}

	return &SchemasNode{
		cfg:         cfg,
		db:          nil,
		cache:       cache,
		partialRows: NewPartialRowTracker(nil),
	}
}

// TestNewSchemasNode tests SchemasNode creation
func TestNewSchemasNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)
	partialRows := NewPartialRowTracker(nil)

	node := NewSchemasNode(cfg, nil, cache, partialRows)

	if node == nil {
		t.Fatal("NewSchemasNode returned nil")
	}

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}

	if node.cache != cache {
		t.Error("Expected cache to be set")
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}
}

// TestSchemasNode_Getattr tests that Getattr returns correct directory attributes
func TestSchemasNode_Getattr(t *testing.T) {
	node := setupTestSchemasNode([]string{"public", "analytics"})
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check mode is directory with 700 permissions
	expectedMode := uint32(0700 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	// Check nlink is 2 (standard for directories)
	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}

	// Check size
	if out.Size != 4096 {
		t.Errorf("Expected Size=4096, got %d", out.Size)
	}
}

// TestSchemasNode_Readdir tests listing schemas
func TestSchemasNode_Readdir(t *testing.T) {
	schemas := []string{"public", "analytics", "staging"}
	node := setupTestSchemasNode(schemas)
	ctx := context.Background()

	dirStream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	if dirStream == nil {
		t.Fatal("Expected non-nil DirStream")
	}

	// Read all entries
	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	if len(entries) != len(schemas) {
		t.Errorf("Expected %d entries, got %d", len(schemas), len(entries))
	}

	// Verify each schema is present
	schemaSet := make(map[string]bool)
	for _, schema := range schemas {
		schemaSet[schema] = true
	}

	for _, entry := range entries {
		if !schemaSet[entry.Name] {
			t.Errorf("Unexpected entry: %q", entry.Name)
		}
		// Verify entries are directories
		if entry.Mode != syscall.S_IFDIR {
			t.Errorf("Expected entry %q to be directory, got mode 0x%x", entry.Name, entry.Mode)
		}
	}
}

// TestSchemasNode_Readdir_Empty tests listing with no schemas
func TestSchemasNode_Readdir_Empty(t *testing.T) {
	node := setupTestSchemasNode([]string{})
	ctx := context.Background()

	dirStream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	if dirStream == nil {
		t.Fatal("Expected non-nil DirStream")
	}

	// Should have no entries
	if dirStream.HasNext() {
		entry, _ := dirStream.Next()
		t.Errorf("Expected no entries, got %q", entry.Name)
	}
}

// TestSchemasNode_Lookup_ValidSchema tests that lookup finds existing schemas via cache
func TestSchemasNode_Lookup_ValidSchema(t *testing.T) {
	schemas := []string{"public", "analytics", "staging"}
	node := setupTestSchemasNode(schemas)
	ctx := context.Background()

	// Test that cache correctly reports schema existence
	exists, err := node.cache.HasSchema(ctx, "analytics")
	if err != nil {
		t.Fatalf("HasSchema failed: %v", err)
	}
	if !exists {
		t.Error("Expected 'analytics' to exist in cache")
	}
}

// TestSchemasNode_Lookup_InvalidSchema tests that lookup correctly identifies missing schemas
func TestSchemasNode_Lookup_InvalidSchema(t *testing.T) {
	schemas := []string{"public", "analytics"}
	node := setupTestSchemasNode(schemas)
	ctx := context.Background()

	// Test that cache correctly reports schema non-existence
	exists, err := node.cache.HasSchema(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("HasSchema failed: %v", err)
	}
	if exists {
		t.Error("Expected 'nonexistent' to not exist in cache")
	}
}

// TestSchemasNode_Interfaces verifies SchemasNode implements required interfaces
func TestSchemasNode_Interfaces(t *testing.T) {
	node := setupTestSchemasNode([]string{})

	// Verify interface implementations
	var _ fs.InodeEmbedder = node
	var _ fs.NodeReaddirer = node
	var _ fs.NodeLookuper = node
	var _ fs.NodeGetattrer = node
}
