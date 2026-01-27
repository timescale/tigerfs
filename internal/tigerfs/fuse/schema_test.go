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

// setupTestSchemaNode creates a SchemaNode with a pre-populated cache for testing
func setupTestSchemaNode(schema string, tables []string) *SchemaNode {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := &MetadataCache{
		cfg:               cfg,
		db:                nil,
		defaultSchema:     "public",
		schemas:           []string{"public", schema},
		tables:            []string{}, // Default schema tables (not used in this test)
		schemaTables:      make(map[string][]string),
		schemaRowCounts:   make(map[string]map[string]int64),
		schemaPermissions: make(map[string]map[string]*db.TablePermissions),
		schemaLastFetch:   make(map[string]time.Time),
		lastFetch:         time.Now(),
	}

	// Pre-populate cache for this schema
	cache.schemaTables[schema] = tables
	cache.schemaLastFetch[schema] = time.Now()

	return &SchemaNode{
		cfg:         cfg,
		db:          nil,
		cache:       cache,
		schema:      schema,
		partialRows: NewPartialRowTracker(nil),
	}
}

// TestNewSchemaNode tests SchemaNode creation
func TestNewSchemaNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)
	partialRows := NewPartialRowTracker(nil)

	node := NewSchemaNode(cfg, nil, cache, "analytics", partialRows)

	if node == nil {
		t.Fatal("NewSchemaNode returned nil")
	}

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}

	if node.cache != cache {
		t.Error("Expected cache to be set")
	}

	if node.schema != "analytics" {
		t.Errorf("Expected schema='analytics', got '%s'", node.schema)
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}
}

// TestSchemaNode_Getattr tests that Getattr returns correct directory attributes
func TestSchemaNode_Getattr(t *testing.T) {
	node := setupTestSchemaNode("analytics", []string{"reports", "metrics"})
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

// TestSchemaNode_Readdir tests listing tables in a schema
func TestSchemaNode_Readdir(t *testing.T) {
	tables := []string{"reports", "metrics", "dashboards"}
	node := setupTestSchemaNode("analytics", tables)
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

	if len(entries) != len(tables) {
		t.Errorf("Expected %d entries, got %d", len(tables), len(entries))
	}

	// Verify each table is present
	tableSet := make(map[string]bool)
	for _, table := range tables {
		tableSet[table] = true
	}

	for _, entry := range entries {
		if !tableSet[entry.Name] {
			t.Errorf("Unexpected entry: %q", entry.Name)
		}
		// Verify entries are directories
		if entry.Mode != syscall.S_IFDIR {
			t.Errorf("Expected entry %q to be directory, got mode 0x%x", entry.Name, entry.Mode)
		}
	}
}

// TestSchemaNode_Readdir_Empty tests listing with no tables
func TestSchemaNode_Readdir_Empty(t *testing.T) {
	node := setupTestSchemaNode("empty_schema", []string{})
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

// TestSchemaNode_DifferentSchemas tests SchemaNodes for different schemas
func TestSchemaNode_DifferentSchemas(t *testing.T) {
	testCases := []struct {
		schema string
		tables []string
	}{
		// Note: "public" is the default schema, so we use a non-default schema
		// for consistent testing. The default schema uses cache.tables directly.
		{"analytics", []string{"reports", "metrics"}},
		{"staging", []string{"temp_data"}},
		{"myschema", []string{"users", "orders"}},
	}

	for _, tc := range testCases {
		t.Run(tc.schema, func(t *testing.T) {
			node := setupTestSchemaNode(tc.schema, tc.tables)
			ctx := context.Background()

			if node.schema != tc.schema {
				t.Errorf("Expected schema='%s', got '%s'", tc.schema, node.schema)
			}

			dirStream, errno := node.Readdir(ctx)
			if errno != 0 {
				t.Fatalf("Expected errno=0, got %d", errno)
			}

			var count int
			for dirStream.HasNext() {
				_, _ = dirStream.Next()
				count++
			}

			if count != len(tc.tables) {
				t.Errorf("Expected %d tables, got %d", len(tc.tables), count)
			}
		})
	}
}

// TestSchemaNode_Interfaces verifies SchemaNode implements required interfaces
func TestSchemaNode_Interfaces(t *testing.T) {
	node := setupTestSchemaNode("test", []string{})

	// Verify interface implementations
	var _ fs.InodeEmbedder = node
	var _ fs.NodeReaddirer = node
	var _ fs.NodeLookuper = node
	var _ fs.NodeGetattrer = node
}
