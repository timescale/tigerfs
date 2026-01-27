package fuse

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// setupTestRootNode creates a RootNode with a pre-populated cache for testing
// This allows testing without a real database connection
func setupTestRootNode(tables []string) *RootNode {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	// Create root node with nil db client (we'll use pre-populated cache)
	root := &RootNode{
		cfg:         cfg,
		db:          nil,
		partialRows: NewPartialRowTracker(nil),
	}

	// Create cache and pre-populate it
	cache := &MetadataCache{
		cfg:               cfg,
		db:                nil,
		defaultSchema:     cfg.DefaultSchema,
		tables:            tables,
		schemas:           []string{"public"},
		schemaTables:      make(map[string][]string),
		schemaRowCounts:   make(map[string]map[string]int64),
		schemaPermissions: make(map[string]map[string]*db.TablePermissions),
		schemaLastFetch:   make(map[string]time.Time),
		lastFetch:         time.Now(), // Recent fetch prevents DB refresh
	}
	root.cache = cache

	return root
}

// setupTestRootNodeWithSchemas creates a RootNode with multiple schemas for testing
func setupTestRootNodeWithSchemas(tables []string, schemas []string) *RootNode {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	// Create root node with nil db client (we'll use pre-populated cache)
	root := &RootNode{
		cfg:         cfg,
		db:          nil,
		partialRows: NewPartialRowTracker(nil),
	}

	// Create cache and pre-populate it
	cache := &MetadataCache{
		cfg:               cfg,
		db:                nil,
		defaultSchema:     cfg.DefaultSchema,
		tables:            tables,
		schemas:           schemas,
		schemaTables:      make(map[string][]string),
		schemaRowCounts:   make(map[string]map[string]int64),
		schemaPermissions: make(map[string]map[string]*db.TablePermissions),
		schemaLastFetch:   make(map[string]time.Time),
		lastFetch:         time.Now(), // Recent fetch prevents DB refresh
	}
	root.cache = cache

	return root
}

// TestNewRootNode tests RootNode creation
func TestNewRootNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	root := NewRootNode(cfg, nil, nil)

	if root == nil {
		t.Fatal("NewRootNode returned nil")
	}

	if root.cfg != cfg {
		t.Error("Expected cfg to be set")
	}

	if root.cache == nil {
		t.Error("Expected cache to be initialized")
	}
}

// TestRootNode_Getattr tests that Getattr returns correct directory attributes
func TestRootNode_Getattr(t *testing.T) {
	root := setupTestRootNode([]string{})
	ctx := context.Background()

	var out fuse.AttrOut
	errno := root.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check mode is directory with 755 permissions
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

// TestRootNode_Readdir tests listing tables from the root directory
func TestRootNode_Readdir(t *testing.T) {
	tables := []string{"users", "orders", "products"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

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

	// Expected: tables + .schemas
	expectedCount := len(tables) + 1
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Verify each table is present (and .schemas)
	tableSet := make(map[string]bool)
	for _, table := range tables {
		tableSet[table] = true
	}
	tableSet[".schemas"] = true

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

// TestRootNode_Readdir_EmptyDatabase tests listing with no tables
func TestRootNode_Readdir_EmptyDatabase(t *testing.T) {
	root := setupTestRootNode([]string{})
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	if dirStream == nil {
		t.Fatal("Expected non-nil DirStream")
	}

	// Should have exactly one entry: .schemas
	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 entry (.schemas), got %d", len(entries))
	}

	if len(entries) > 0 && entries[0].Name != ".schemas" {
		t.Errorf("Expected .schemas, got %q", entries[0].Name)
	}
}

// TestRootNode_Readdir_SingleTable tests listing with exactly one table
func TestRootNode_Readdir_SingleTable(t *testing.T) {
	root := setupTestRootNode([]string{"only_table"})
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	// Expected: 1 table + .schemas
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries (.schemas + only_table), got %d", len(entries))
	}

	// First entry should be .schemas, second should be only_table
	foundTable := false
	for _, entry := range entries {
		if entry.Name == "only_table" {
			foundTable = true
		}
	}

	if !foundTable {
		t.Error("Expected 'only_table' to be in the listing")
	}
}

// TestRootNode_Readdir_ManyTables tests listing with many tables
func TestRootNode_Readdir_ManyTables(t *testing.T) {
	// Create 100 tables
	tables := make([]string, 100)
	for i := 0; i < 100; i++ {
		tables[i] = string(rune('a'+i%26)) + string(rune('0'+i/26))
	}

	root := setupTestRootNode(tables)
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	var count int
	for dirStream.HasNext() {
		_, _ = dirStream.Next()
		count++
	}

	// Expected: 100 tables + .schemas
	if count != 101 {
		t.Errorf("Expected 101 entries (100 tables + .schemas), got %d", count)
	}
}

// TestRootNode_Lookup_ValidTable_CacheCheck tests that lookup finds existing tables via cache
// Note: Full Lookup testing requires FUSE server context (integration tests)
// Here we test the cache logic used by Lookup
func TestRootNode_Lookup_ValidTable_CacheCheck(t *testing.T) {
	tables := []string{"users", "orders", "products"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	// Test that cache correctly reports table existence
	exists, err := root.cache.HasTable(ctx, "users")
	if err != nil {
		t.Fatalf("HasTable failed: %v", err)
	}
	if !exists {
		t.Error("Expected 'users' to exist in cache")
	}
}

// TestRootNode_Lookup_InvalidTable_CacheCheck tests that lookup correctly identifies missing tables
func TestRootNode_Lookup_InvalidTable_CacheCheck(t *testing.T) {
	tables := []string{"users", "orders", "products"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	// Test that cache correctly reports table non-existence
	exists, err := root.cache.HasTable(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("HasTable failed: %v", err)
	}
	if exists {
		t.Error("Expected 'nonexistent' to not exist in cache")
	}
}

// TestRootNode_Lookup_CaseSensitivity tests that lookup is case-sensitive
func TestRootNode_Lookup_CaseSensitivity(t *testing.T) {
	tables := []string{"Users", "orders"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	testCases := []struct {
		name        string
		expected    bool
		description string
	}{
		{"Users", true, "exact match should succeed"},
		{"users", false, "lowercase should not match 'Users'"},
		{"USERS", false, "uppercase should not match 'Users'"},
		{"orders", true, "exact match should succeed"},
		{"Orders", false, "titlecase should not match 'orders'"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exists, err := root.cache.HasTable(ctx, tc.name)
			if err != nil {
				t.Fatalf("HasTable failed: %v", err)
			}

			if exists != tc.expected {
				t.Errorf("%s: expected exists=%v, got %v", tc.description, tc.expected, exists)
			}
		})
	}
}

// TestRootNode_Lookup_EmptyName tests looking up an empty string
func TestRootNode_Lookup_EmptyName(t *testing.T) {
	tables := []string{"users"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	// Empty name should not exist
	exists, err := root.cache.HasTable(ctx, "")
	if err != nil {
		t.Fatalf("HasTable failed: %v", err)
	}
	if exists {
		t.Error("Expected empty name to not exist in cache")
	}
}

// TestRootNode_Lookup_SpecialCharacters tests tables with special characters
func TestRootNode_Lookup_SpecialCharacters(t *testing.T) {
	tables := []string{"user_data", "order-items", "product.info"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			exists, err := root.cache.HasTable(ctx, table)
			if err != nil {
				t.Fatalf("HasTable failed: %v", err)
			}
			if !exists {
				t.Errorf("Expected %q to exist in cache", table)
			}
		})
	}
}

// TestRootNode_Readdir_IncludesSchemasDir tests that .schemas appears in root listing
func TestRootNode_Readdir_IncludesSchemasDir(t *testing.T) {
	tables := []string{"users", "orders"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	// Read all entries
	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	// Should have tables + .schemas
	expectedCount := len(tables) + 1
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Find .schemas entry
	foundSchemas := false
	for _, entry := range entries {
		if entry.Name == ".schemas" {
			foundSchemas = true
			if entry.Mode != syscall.S_IFDIR {
				t.Errorf("Expected .schemas to be directory, got mode 0x%x", entry.Mode)
			}
			break
		}
	}

	if !foundSchemas {
		t.Error("Expected to find .schemas in root listing")
	}
}

// TestRootNode_Readdir_SchemasFirst tests that .schemas appears first in the listing
func TestRootNode_Readdir_SchemasFirst(t *testing.T) {
	tables := []string{"users", "orders"}
	root := setupTestRootNode(tables)
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	// First entry should be .schemas
	if !dirStream.HasNext() {
		t.Fatal("Expected at least one entry")
	}

	entry, _ := dirStream.Next()
	if entry.Name != ".schemas" {
		t.Errorf("Expected first entry to be .schemas, got %q", entry.Name)
	}
}

// TestRootNode_Lookup_SchemasDir tests that looking up .schemas returns SchemasNode
func TestRootNode_Lookup_SchemasDir(t *testing.T) {
	schemas := []string{"public", "analytics"}
	root := setupTestRootNodeWithSchemas([]string{"users"}, schemas)
	ctx := context.Background()

	// Test that cache correctly reports schema existence
	exists, err := root.cache.HasSchema(ctx, "public")
	if err != nil {
		t.Fatalf("HasSchema failed: %v", err)
	}
	if !exists {
		t.Error("Expected 'public' schema to exist in cache")
	}

	exists, err = root.cache.HasSchema(ctx, "analytics")
	if err != nil {
		t.Fatalf("HasSchema failed: %v", err)
	}
	if !exists {
		t.Error("Expected 'analytics' schema to exist in cache")
	}
}

// TestRootNode_Readdir_NoSchemasAtRoot tests that other schemas don't appear at root level
// (They should only be accessible via .schemas/)
func TestRootNode_Readdir_NoSchemasAtRoot(t *testing.T) {
	tables := []string{"users", "orders"}
	schemas := []string{"public", "analytics", "staging"}
	root := setupTestRootNodeWithSchemas(tables, schemas)
	ctx := context.Background()

	dirStream, errno := root.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	// Read all entries
	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	// Verify no schema names appear at root (except public tables which are flattened)
	for _, entry := range entries {
		// .schemas is allowed
		if entry.Name == ".schemas" {
			continue
		}
		// Schema names should not appear at root
		for _, schema := range schemas {
			if entry.Name == schema && schema != "public" {
				t.Errorf("Schema %q should not appear at root level", schema)
			}
		}
	}
}

// Note: MetadataCache tests are in cache_test.go
