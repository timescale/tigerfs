package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestNewSampleNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewSampleNode(cfg, nil, nil, "myschema", "users", partialRows)

	if node.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", node.tableName)
	}

	if node.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", node.schema)
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}

	if node.db != nil {
		t.Error("Expected db to be nil")
	}

	if node.cache != nil {
		t.Error("Expected cache to be nil")
	}
}

func TestSampleNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewSampleNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expectedMode := uint32(0700 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}
}

func TestSampleNode_Readdir_Empty(t *testing.T) {
	cfg := &config.Config{}
	node := NewSampleNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	stream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Error("Expected non-nil stream")
	}

	// Stream should be empty (user must specify N)
	if stream.HasNext() {
		t.Error("Expected empty directory stream")
	}
}

func TestNewSampleLimitNode(t *testing.T) {
	cfg := &config.Config{}
	partialRows := NewPartialRowTracker(nil)

	node := NewSampleLimitNode(cfg, nil, nil, "myschema", "users", 100, partialRows)

	if node.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", node.tableName)
	}

	if node.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", node.schema)
	}

	if node.sampleSize != 100 {
		t.Errorf("Expected sampleSize=100, got %d", node.sampleSize)
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}
}

func TestSampleLimitNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewSampleLimitNode(cfg, nil, nil, "public", "users", 50, nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expectedMode := uint32(0700 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}
}

func TestSampleNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	sampleNode := NewSampleNode(cfg, nil, nil, "public", "users", nil)
	limitNode := NewSampleLimitNode(cfg, nil, nil, "public", "users", 100, nil)

	// Verify interface assertions compile (checked at compile time via var _ declarations)
	_ = sampleNode
	_ = limitNode
}

// =============================================================================
// Mock-based tests
// =============================================================================

// TestSampleLimitNode_Readdir_WithMock tests random sampling with mock
func TestSampleLimitNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockPaginationReader.GetRandomSampleRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
		// Return approximately the requested number of rows
		return []string{"42", "17", "99", "5", "73"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewSampleLimitNode(cfg, mock, nil, "public", "users", 5, partialRows)

	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Fatal("Expected non-nil stream")
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}

	// Verify all expected rows are present
	expected := map[string]bool{"42": true, "17": true, "99": true, "5": true, "73": true}
	for _, name := range entries {
		if !expected[name] {
			t.Errorf("Unexpected entry: %s", name)
		}
	}
}

// TestSampleLimitNode_Readdir_WithMock_WithCache tests sampling using cached row estimate
func TestSampleLimitNode_Readdir_WithMock_WithCache(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}

	var capturedEstimate int64
	mock.MockPaginationReader.GetRandomSampleRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
		capturedEstimate = estimatedRows
		return []string{"1", "2", "3"}, nil
	}

	// Set up cache with row count estimate
	mock.MockSchemaReader.GetCurrentSchemaFunc = func(ctx context.Context) (string, error) {
		return "public", nil
	}
	mock.MockSchemaReader.GetSchemasFunc = func(ctx context.Context) ([]string, error) {
		return []string{"public"}, nil
	}
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"users"}, nil
	}
	mock.MockCountReader.GetRowCountEstimatesFunc = func(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
		return map[string]int64{"users": 1000000}, nil
	}
	mock.MockSchemaReader.GetTablePermissionsFunc = func(ctx context.Context, schema, table string) (*db.TablePermissions, error) {
		return &db.TablePermissions{CanSelect: true}, nil
	}

	cache := NewMetadataCache(cfg, mock)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("Failed to refresh cache: %v", err)
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewSampleLimitNode(cfg, mock, cache, "public", "users", 10, partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Verify the estimated row count was passed to GetRandomSampleRows
	if capturedEstimate != 1000000 {
		t.Errorf("Expected estimatedRows=1000000, got %d", capturedEstimate)
	}
}

// TestSampleLimitNode_Readdir_WithMock_PKError tests sampling when GetPrimaryKey fails
func TestSampleLimitNode_Readdir_WithMock_PKError(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewSampleLimitNode(cfg, mock, nil, "public", "users", 10, partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestSampleLimitNode_Readdir_WithMock_SampleError tests sampling when GetRandomSampleRows fails
func TestSampleLimitNode_Readdir_WithMock_SampleError(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockPaginationReader.GetRandomSampleRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewSampleLimitNode(cfg, mock, nil, "public", "users", 10, partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestSampleNode_Lookup_WithMock tests are skipped because Lookup requires
// FUSE bridge infrastructure (NewPersistentInode). See test/integration/.
