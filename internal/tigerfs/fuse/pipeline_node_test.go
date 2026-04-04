package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestPipelineNode_Readdir tests that PipelineNode lists capabilities based on context.
func TestPipelineNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}

	mockDB := db.NewMockDBClient()
	mockDB.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	// Return empty rows for simplicity
	mockDB.MockPaginationReader.GetFirstNRowsFunc = func(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
		return []string{}, nil
	}

	// Create pipeline context
	pipeline := NewPipelineContext("public", "users", "id")

	node := NewPipelineNode(cfg, mockDB, nil, "public", "users", pipeline, nil)

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

	// Should have all capabilities for fresh pipeline
	// Expected: .by, .filter, .order, .first, .last, .sample, .export
	expectedCaps := map[string]bool{
		".by": true, ".filter": true, ".order": true,
		".first": true, ".last": true, ".sample": true, ".export": true,
	}

	for _, e := range entries {
		if expectedCaps[e] {
			delete(expectedCaps, e)
		}
	}

	if len(expectedCaps) > 0 {
		t.Errorf("Missing capabilities: %v", expectedCaps)
	}
}

// TestPipelineNode_Readdir_AfterOrder tests capabilities after .order/ is applied.
func TestPipelineNode_Readdir_AfterOrder(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}

	mockDB := db.NewMockDBClient()
	mockDB.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mockDB.MockPaginationReader.GetFirstNRowsFunc = func(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
		return []string{}, nil
	}

	// Create pipeline context with order applied
	pipeline := NewPipelineContext("public", "users", "id")
	pipeline = pipeline.WithOrder("name", false)

	node := NewPipelineNode(cfg, mockDB, nil, "public", "users", pipeline, nil)

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

	// After .order/, should NOT have .by, .filter, .order (filters/order disallowed)
	// Should have: .first, .last, .export (sample disallowed after order per rules)
	disallowedCaps := []string{".by", ".filter", ".order"}
	for _, cap := range disallowedCaps {
		for _, e := range entries {
			if e == cap {
				t.Errorf("Should not have %s capability after order", cap)
			}
		}
	}
}

// TestPipelineNode_Readdir_AfterLimit tests capabilities after .first/N is applied.
func TestPipelineNode_Readdir_AfterLimit(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}

	mockDB := db.NewMockDBClient()
	mockDB.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mockDB.MockPaginationReader.GetFirstNRowsFunc = func(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
		return []string{}, nil
	}

	// Create pipeline context with first limit applied
	pipeline := NewPipelineContext("public", "users", "id")
	pipeline = pipeline.WithLimit(100, LimitFirst)

	node := NewPipelineNode(cfg, mockDB, nil, "public", "users", pipeline, nil)

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

	// After .first/N, should NOT have another .first (double-first disallowed)
	// Should have: .last, .export (for nested pagination)
	for _, e := range entries {
		if e == ".first" {
			t.Error("Should not have .first capability after .first")
		}
	}

	// Should have .last for nested pagination
	hasLast := false
	for _, e := range entries {
		if e == ".last" {
			hasLast = true
		}
	}
	if !hasLast {
		t.Error("Should have .last capability for nested pagination")
	}
}

// TestPipelineLimitNode_Lookup tests that limit node parses N correctly.
func TestPipelineLimitNode_Lookup(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}
	mockDB := db.NewMockDBClient()

	pipeline := NewPipelineContext("public", "users", "id")
	node := NewPipelineLimitNode(cfg, mockDB, nil, "public", "users", pipeline, LimitFirst, nil)

	ctx := context.Background()

	// Invalid values should return ENOENT
	invalidValues := []string{"abc", "-1", "0", ""}
	for _, val := range invalidValues {
		var out fuse.EntryOut
		_, errno := node.Lookup(ctx, val, &out)
		if errno != syscall.ENOENT {
			t.Errorf("Expected ENOENT for invalid value %q, got %d", val, errno)
		}
	}

	// Note: Valid lookups require NewPersistentInode which needs FUSE infrastructure
	// Full lookup tests are covered by integration tests
}

// TestPipelineByDirNode_Readdir tests that .by/ shows indexed columns.
func TestPipelineByDirNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}

	mockDB := db.NewMockDBClient()
	mockDB.MockIndexReader.GetSingleColumnIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{
			{Name: "idx_email", Columns: []string{"email"}, IsPrimary: false},
			{Name: "idx_status", Columns: []string{"status"}, IsPrimary: false},
			{Name: "pk_id", Columns: []string{"id"}, IsPrimary: true}, // Should be excluded
		}, nil
	}

	pipeline := NewPipelineContext("public", "users", "id")
	node := NewPipelineByDirNode(cfg, mockDB, nil, "public", "users", pipeline, nil)

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

	// Should have email and status, but NOT id (primary key excluded)
	expected := map[string]bool{"email": true, "status": true}
	for _, e := range entries {
		if e == "id" {
			t.Error("Primary key column should not be listed")
		}
		if expected[e] {
			delete(expected, e)
		}
	}

	if len(expected) > 0 {
		t.Errorf("Missing indexed columns: %v", expected)
	}
}

// TestPipelineOrderDirNode_Readdir tests that .order/ shows all columns.
func TestPipelineOrderDirNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}

	mockDB := db.NewMockDBClient()
	mockDB.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "created_at", DataType: "timestamp"},
		}, nil
	}

	pipeline := NewPipelineContext("public", "users", "id")
	node := NewPipelineOrderDirNode(cfg, mockDB, nil, "public", "users", pipeline, nil)

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

	// Should have all 3 columns
	if len(entries) != 3 {
		t.Errorf("Expected 3 columns, got %d: %v", len(entries), entries)
	}
}

// TestPipelineOrderColumnNode_Readdir tests that .order/<column>/ shows directions.
func TestPipelineOrderColumnNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}
	mockDB := db.NewMockDBClient()

	pipeline := NewPipelineContext("public", "users", "id")
	node := NewPipelineOrderColumnNode(cfg, mockDB, nil, "public", "users", "name", pipeline, nil)

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

	// Should have "first" and "last"
	expected := map[string]bool{"first": true, "last": true}
	for _, e := range entries {
		delete(expected, e)
	}

	if len(expected) > 0 {
		t.Errorf("Missing direction entries: %v", expected)
	}
}

// TestPipelineExportDirNode_Readdir tests that .export/ shows formats.
func TestPipelineExportDirNode_Readdir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}
	mockDB := db.NewMockDBClient()

	pipeline := NewPipelineContext("public", "users", "id")
	node := NewPipelineExportDirNode(cfg, mockDB, nil, "public", "users", pipeline)

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

	// Should have formats and .with-headers
	expected := map[string]bool{
		".with-headers": true,
		"csv":           true,
		"json":          true,
		"tsv":           true,
		"yaml":          true,
	}

	for _, e := range entries {
		delete(expected, e)
	}

	if len(expected) > 0 {
		t.Errorf("Missing export entries: %v", expected)
	}
}

// TestPipelineByColumnNode_Pipeline tests that filter is added with indexed=true.
func TestPipelineByColumnNode_Pipeline(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 10000}
	mockDB := db.NewMockDBClient()

	// Create base pipeline
	basePipeline := NewPipelineContext("public", "users", "id")

	// Create column node
	node := NewPipelineByColumnNode(cfg, mockDB, nil, "public", "users", "status", basePipeline, nil)

	// The node should have the base pipeline stored
	if node.pipeline != basePipeline {
		t.Error("Expected node to have base pipeline")
	}

	// When Lookup is called, it creates a new pipeline with filter
	// We can't test full Lookup without FUSE infrastructure, but we can verify
	// the pipeline would have the filter added correctly by checking WithFilter
	newPipeline := basePipeline.WithFilter("status", "active", true)

	if len(newPipeline.Filters) != 1 {
		t.Fatalf("Expected 1 filter, got %d", len(newPipeline.Filters))
	}

	filter := newPipeline.Filters[0]
	if filter.Column != "status" {
		t.Errorf("Expected column 'status', got %q", filter.Column)
	}
	if filter.Value != "active" {
		t.Errorf("Expected value 'active', got %q", filter.Value)
	}
	if !filter.Indexed {
		t.Error("Filter from .by/ should be marked as indexed")
	}
}
