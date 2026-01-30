package fuse

import (
	"context"
	"strings"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestFilterDirNode_Readdir tests that FilterDirNode lists all columns.
func TestFilterDirNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}

	mockDB := db.NewMockDBClient()
	mockDB.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "status", DataType: "text"},
			{Name: "created_at", DataType: "timestamp"},
		}, nil
	}

	node := NewFilterDirNode(cfg, mockDB, nil, "public", "users", nil, nil)

	ctx := context.Background()
	stream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed with errno %d", errno)
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have all 4 columns
	if len(entries) != 4 {
		t.Errorf("Expected 4 columns, got %d: %v", len(entries), entries)
	}

	// Verify all expected columns are present
	expected := map[string]bool{"id": true, "name": true, "status": true, "created_at": true}
	for _, e := range entries {
		if !expected[e] {
			t.Errorf("Unexpected column in listing: %s", e)
		}
		delete(expected, e)
	}
	if len(expected) > 0 {
		t.Errorf("Missing columns: %v", expected)
	}
}

// TestFilterColumnNode_Readdir_IndexedColumn tests value listing for indexed columns.
func TestFilterColumnNode_Readdir_IndexedColumn(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}

	mockDB := db.NewMockDBClient()
	mockDB.MockIndexReader.GetIndexByColumnFunc = func(ctx context.Context, schema, table, column string) (*db.Index, error) {
		// Column is indexed
		return &db.Index{Name: "idx_status", Columns: []string{"status"}}, nil
	}
	mockDB.MockIndexReader.GetDistinctValuesFunc = func(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
		return []string{"active", "inactive", "pending"}, nil
	}

	node := NewFilterColumnNode(cfg, mockDB, nil, "public", "users", "status", nil, nil)

	ctx := context.Background()
	stream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed with errno %d", errno)
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have 3 distinct values
	if len(entries) != 3 {
		t.Errorf("Expected 3 values, got %d: %v", len(entries), entries)
	}
}

// TestFilterColumnNode_Readdir_SmallTable tests value listing for non-indexed columns on small tables.
func TestFilterColumnNode_Readdir_SmallTable(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}

	mockDB := db.NewMockDBClient()
	mockDB.MockIndexReader.GetIndexByColumnFunc = func(ctx context.Context, schema, table, column string) (*db.Index, error) {
		// Column is NOT indexed
		return nil, nil
	}
	mockDB.MockIndexReader.GetDistinctValuesFunc = func(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
		return []string{"value1", "value2"}, nil
	}
	mockDB.MockCountReader.GetTableRowCountEstimateFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 1000, nil // Small table
	}

	node := NewFilterColumnNode(cfg, mockDB, nil, "public", "users", "name", nil, nil)

	ctx := context.Background()
	stream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed with errno %d", errno)
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have 2 distinct values (small table, so values are listed)
	if len(entries) != 2 {
		t.Errorf("Expected 2 values, got %d: %v", len(entries), entries)
	}
}

// TestFilterColumnNode_Readdir_LargeTable tests that large tables show .table-too-large.
func TestFilterColumnNode_Readdir_LargeTable(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}

	mockDB := db.NewMockDBClient()
	mockDB.MockIndexReader.GetIndexByColumnFunc = func(ctx context.Context, schema, table, column string) (*db.Index, error) {
		// Column is NOT indexed
		return nil, nil
	}
	mockDB.MockCountReader.GetTableRowCountEstimateFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 1000000, nil // Large table (exceeds DirFilterLimit)
	}

	node := NewFilterColumnNode(cfg, mockDB, nil, "public", "users", "name", nil, nil)

	ctx := context.Background()
	stream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed with errno %d", errno)
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should only have the .table-too-large indicator
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry (.table-too-large), got %d: %v", len(entries), entries)
	}
	if len(entries) > 0 && entries[0] != FileTableTooLarge {
		t.Errorf("Expected %q, got %q", FileTableTooLarge, entries[0])
	}
}

// TestFilterColumnNode_DirectAccess documents that direct value access always works.
// Note: Full Lookup tests require FUSE infrastructure and are covered by integration tests.
// This test verifies the node creation logic.
func TestFilterColumnNode_DirectAccess(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}

	mockDB := db.NewMockDBClient()
	mockDB.MockIndexReader.GetIndexByColumnFunc = func(ctx context.Context, schema, table, column string) (*db.Index, error) {
		return nil, nil // Not indexed
	}
	mockDB.MockCountReader.GetTableRowCountEstimateFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 1000000, nil // Large table
	}

	// Verify node can be created and has correct properties
	node := NewFilterColumnNode(cfg, mockDB, nil, "public", "users", "status", nil, nil)
	if node == nil {
		t.Fatal("Expected node, got nil")
	}
	if node.column != "status" {
		t.Errorf("Expected column 'status', got %q", node.column)
	}
	if node.table != "users" {
		t.Errorf("Expected table 'users', got %q", node.table)
	}

	// Note: The actual Lookup behavior (direct path access) is tested via integration tests
	// because it requires full FUSE bridge setup with NewPersistentInode
}

// TestFilterValueNode_Readdir tests that FilterValueNode lists available capabilities.
func TestFilterValueNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}
	mockDB := db.NewMockDBClient()

	node := NewFilterValueNode(cfg, mockDB, nil, "public", "users", "status", "active", nil, nil)

	ctx := context.Background()
	stream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir failed with errno %d", errno)
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have capabilities available (filter not after order, so all capabilities)
	// Expected: .by, .filter, .order, .first, .last, .sample, .export
	expectedCaps := map[string]bool{
		".by": true, ".filter": true, ".order": true,
		".first": true, ".last": true, ".sample": true, ".export": true,
	}

	for _, e := range entries {
		if !expectedCaps[e] {
			t.Errorf("Unexpected capability: %s", e)
		}
	}

	// Verify we have a reasonable number of capabilities
	if len(entries) < 5 {
		t.Errorf("Expected at least 5 capabilities, got %d: %v", len(entries), entries)
	}
}

// TestFilterValueNode_Pipeline tests that the pipeline context is properly updated.
func TestFilterValueNode_Pipeline(t *testing.T) {
	cfg := &config.Config{DirFilterLimit: 100000}
	mockDB := db.NewMockDBClient()

	// Create a base pipeline with an existing filter
	basePipeline := NewPipelineContext("public", "users", "id")
	basePipeline = basePipeline.WithFilter("type", "premium", true) // .by/ filter

	// Create FilterValueNode with the base pipeline
	node := NewFilterValueNode(cfg, mockDB, nil, "public", "users", "status", "active", basePipeline, nil)

	// The internal pipeline should have both filters
	if len(node.pipeline.Filters) != 2 {
		t.Errorf("Expected 2 filters, got %d", len(node.pipeline.Filters))
	}

	// First filter should be from base pipeline
	if node.pipeline.Filters[0].Column != "type" || node.pipeline.Filters[0].Value != "premium" {
		t.Errorf("First filter incorrect: %+v", node.pipeline.Filters[0])
	}

	// Second filter should be the new one
	if node.pipeline.Filters[1].Column != "status" || node.pipeline.Filters[1].Value != "active" {
		t.Errorf("Second filter incorrect: %+v", node.pipeline.Filters[1])
	}

	// The new filter should be marked as non-indexed (.filter/)
	if node.pipeline.Filters[1].Indexed {
		t.Error("Filter from .filter/ should not be marked as indexed")
	}
}

// TestTableTooLargeNode_Content tests the .table-too-large indicator content.
func TestTableTooLargeNode_Content(t *testing.T) {
	node := NewTableTooLargeNode("users", "name")

	content := node.content()
	if len(content) == 0 {
		t.Error("Expected non-empty content")
	}

	// Should mention the table and column
	contentStr := string(content)
	if !strings.Contains(contentStr, "users") {
		t.Error("Content should mention table name")
	}
	if !strings.Contains(contentStr, "name") {
		t.Error("Content should mention column name")
	}
}
