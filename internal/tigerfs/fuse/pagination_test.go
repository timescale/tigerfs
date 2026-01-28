package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestNewPaginationNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	partialRows := NewPartialRowTracker(nil)

	tests := []struct {
		name           string
		paginationType PaginationType
	}{
		{"first", PaginationFirst},
		{"last", PaginationLast},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewPaginationNode(cfg, nil, nil, "myschema", "users", tt.paginationType, partialRows)

			if node.cfg != cfg {
				t.Error("Expected config to be set")
			}

			if node.tableName != "users" {
				t.Errorf("Expected tableName='users', got '%s'", node.tableName)
			}

			if node.schema != "myschema" {
				t.Errorf("Expected schema='myschema', got '%s'", node.schema)
			}

			if node.paginationType != tt.paginationType {
				t.Errorf("Expected paginationType=%s, got %s", tt.paginationType, node.paginationType)
			}

			if node.partialRows != partialRows {
				t.Error("Expected partialRows to be set")
			}
		})
	}
}

func TestPaginationNode_Getattr(t *testing.T) {
	cfg := &config.Config{}

	tests := []struct {
		name           string
		paginationType PaginationType
	}{
		{"first", PaginationFirst},
		{"last", PaginationLast},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewPaginationNode(cfg, nil, nil, "public", "users", tt.paginationType, nil)
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
		})
	}
}

func TestPaginationNode_Readdir_Empty(t *testing.T) {
	cfg := &config.Config{}
	node := NewPaginationNode(cfg, nil, nil, "public", "users", PaginationFirst, nil)
	ctx := context.Background()

	stream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Error("Expected non-nil stream")
	}

	// Stream should be empty (user must specify N)
	// HasNext returns false for empty streams
	if stream.HasNext() {
		t.Error("Expected empty directory stream")
	}
}

func TestNewPaginationLimitNode(t *testing.T) {
	cfg := &config.Config{}
	partialRows := NewPartialRowTracker(nil)

	node := NewPaginationLimitNode(cfg, nil, nil, "myschema", "users", PaginationFirst, 50, partialRows)

	if node.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", node.tableName)
	}

	if node.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", node.schema)
	}

	if node.paginationType != PaginationFirst {
		t.Errorf("Expected paginationType=first, got %s", node.paginationType)
	}

	if node.limit != 50 {
		t.Errorf("Expected limit=50, got %d", node.limit)
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}
}

func TestPaginationLimitNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewPaginationLimitNode(cfg, nil, nil, "public", "users", PaginationLast, 100, nil)
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

func TestPaginationType_Constants(t *testing.T) {
	if PaginationFirst != "first" {
		t.Errorf("Expected PaginationFirst='first', got '%s'", PaginationFirst)
	}

	if PaginationLast != "last" {
		t.Errorf("Expected PaginationLast='last', got '%s'", PaginationLast)
	}
}

// =============================================================================
// Mock-based tests
// =============================================================================

// TestPaginationLimitNode_Readdir_WithMock_First tests .first/N listing
func TestPaginationLimitNode_Readdir_WithMock_First(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockPaginationReader.GetFirstNRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
		return []string{"1", "2", "3", "4", "5"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewPaginationLimitNode(cfg, mock, nil, "public", "users", PaginationFirst, 5, partialRows)

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

	// Verify order is preserved
	expected := []string{"1", "2", "3", "4", "5"}
	for i, name := range entries {
		if name != expected[i] {
			t.Errorf("Entry %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// TestPaginationLimitNode_Readdir_WithMock_Last tests .last/N listing
func TestPaginationLimitNode_Readdir_WithMock_Last(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockPaginationReader.GetLastNRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
		return []string{"96", "97", "98", "99", "100"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewPaginationLimitNode(cfg, mock, nil, "public", "users", PaginationLast, 5, partialRows)

	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
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

	// Verify we got the last rows
	expected := []string{"96", "97", "98", "99", "100"}
	for i, name := range entries {
		if name != expected[i] {
			t.Errorf("Entry %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// TestPaginationLimitNode_Readdir_WithMock_PKError tests error handling
func TestPaginationLimitNode_Readdir_WithMock_PKError(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewPaginationLimitNode(cfg, mock, nil, "public", "users", PaginationFirst, 10, partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestPaginationLimitNode_Readdir_WithMock_QueryError tests error when query fails
func TestPaginationLimitNode_Readdir_WithMock_QueryError(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockPaginationReader.GetFirstNRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewPaginationLimitNode(cfg, mock, nil, "public", "users", PaginationFirst, 10, partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestPaginationLimitNode_Readdir_WithMock_Empty tests empty result
func TestPaginationLimitNode_Readdir_WithMock_Empty(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockPaginationReader.GetFirstNRowsFunc = func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
		return []string{}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewPaginationLimitNode(cfg, mock, nil, "public", "users", PaginationFirst, 10, partialRows)

	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should be empty
	if stream.HasNext() {
		t.Error("Expected empty stream")
	}
}

// TestPaginationLimitNode_Lookup_WithMock tests are skipped because Lookup requires
// FUSE bridge infrastructure (NewPersistentInode). See test/integration/.
