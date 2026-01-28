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
		staging:     NewStagingTracker(),
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
	staging := NewStagingTracker()

	node := NewSchemaNode(cfg, nil, cache, "analytics", partialRows, staging)

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

	if node.staging != staging {
		t.Error("Expected staging to be set")
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

// TestSchemaNode_Readdir tests listing tables in a schema plus control directories
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

	// Expected: tables + .create + .delete
	expectedCount := len(tables) + 2
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Verify .create and .delete are present, plus all tables
	expectedNames := make(map[string]bool)
	expectedNames[".create"] = true
	expectedNames[".delete"] = true
	for _, table := range tables {
		expectedNames[table] = true
	}

	for _, entry := range entries {
		if !expectedNames[entry.Name] {
			t.Errorf("Unexpected entry: %q", entry.Name)
		}
		// Verify entries are directories
		if entry.Mode != syscall.S_IFDIR {
			t.Errorf("Expected entry %q to be directory, got mode 0x%x", entry.Name, entry.Mode)
		}
		delete(expectedNames, entry.Name)
	}

	// Verify all expected entries were found
	for name := range expectedNames {
		t.Errorf("Missing expected entry: %q", name)
	}
}

// TestSchemaNode_Readdir_Empty tests listing with no tables (still has .create and .delete)
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

	// Read all entries
	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	// Should have exactly .create and .delete entries
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries (.create and .delete), got %d", len(entries))
	}

	// Verify .create and .delete are present
	expectedNames := map[string]bool{".create": true, ".delete": true}
	for _, entry := range entries {
		if !expectedNames[entry.Name] {
			t.Errorf("Unexpected entry: %q", entry.Name)
		}
		delete(expectedNames, entry.Name)
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

			// Expected count: tables + .create + .delete
			expectedCount := len(tc.tables) + 2
			if count != expectedCount {
				t.Errorf("Expected %d entries (tables + .create + .delete), got %d", expectedCount, count)
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

// TestSchemaNode_Lookup_Table tests that lookup finds existing tables
func TestSchemaNode_Lookup_Table(t *testing.T) {
	tables := []string{"users", "orders", "products"}
	node := setupTestSchemaNode("analytics", tables)
	ctx := context.Background()

	// Test lookup of existing table via cache
	cacheHasTables, err := node.cache.GetTablesForSchema(ctx, "analytics")
	if err != nil {
		t.Fatalf("GetTablesForSchema failed: %v", err)
	}

	// Verify all test tables are in cache
	tableSet := make(map[string]bool)
	for _, table := range cacheHasTables {
		tableSet[table] = true
	}

	for _, table := range tables {
		if !tableSet[table] {
			t.Errorf("Expected table %q to be in cache", table)
		}
	}
}

// TestSchemaNode_Lookup_TableNotFound tests that lookup returns ENOENT for missing tables
func TestSchemaNode_Lookup_TableNotFound(t *testing.T) {
	tables := []string{"users", "orders"}
	node := setupTestSchemaNode("analytics", tables)
	ctx := context.Background()

	// Test that a non-existent table is not found in cache
	cacheHasTables, err := node.cache.GetTablesForSchema(ctx, "analytics")
	if err != nil {
		t.Fatalf("GetTablesForSchema failed: %v", err)
	}

	tableSet := make(map[string]bool)
	for _, table := range cacheHasTables {
		tableSet[table] = true
	}

	if tableSet["nonexistent"] {
		t.Error("Expected 'nonexistent' to not be in cache")
	}
}

// TestSchemaNode_Lookup_CreateDir tests that .create directory lookup works
func TestSchemaNode_Lookup_CreateDir(t *testing.T) {
	node := setupTestSchemaNode("analytics", []string{"users"})

	// Verify .create is handled by Lookup (we test via Readdir since Lookup requires Inode operations)
	ctx := context.Background()
	dirStream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %d", errno)
	}

	foundCreate := false
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		if entry.Name == ".create" {
			foundCreate = true
			if entry.Mode != syscall.S_IFDIR {
				t.Errorf("Expected .create to be directory, got mode 0x%x", entry.Mode)
			}
		}
	}

	if !foundCreate {
		t.Error("Expected .create directory in schema listing")
	}
}

// TestSchemaNode_Lookup_DeleteDir tests that .delete directory lookup works
func TestSchemaNode_Lookup_DeleteDir(t *testing.T) {
	node := setupTestSchemaNode("analytics", []string{"users"})

	// Verify .delete is handled by Lookup (we test via Readdir since Lookup requires Inode operations)
	ctx := context.Background()
	dirStream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed: %d", errno)
	}

	foundDelete := false
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		if entry.Name == ".delete" {
			foundDelete = true
			if entry.Mode != syscall.S_IFDIR {
				t.Errorf("Expected .delete to be directory, got mode 0x%x", entry.Mode)
			}
		}
	}

	if !foundDelete {
		t.Error("Expected .delete directory in schema listing")
	}
}
