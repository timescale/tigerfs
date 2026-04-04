package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestNewTableNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	partialRows := NewPartialRowTracker(nil)
	tableNode := NewTableNode(cfg, nil, nil, "myschema", "users", partialRows, nil)

	if tableNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if tableNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", tableNode.tableName)
	}

	if tableNode.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", tableNode.schema)
	}

	if tableNode.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}

	if tableNode.db != nil {
		t.Error("Expected db to be nil")
	}
}

// TestTableNode_Interfaces verifies that TableNode implements required interfaces
func TestTableNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	tableNode := NewTableNode(cfg, nil, nil, "public", "users", nil, nil)

	// Verify interface assertions compile (checked at compile time via var _ declarations)
	_ = tableNode
}

// TestTableNode_Getattr tests that Getattr returns correct directory attributes
func TestTableNode_Getattr(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	tableNode := NewTableNode(cfg, nil, nil, "public", "users", nil, nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := tableNode.Getattr(ctx, nil, &out)

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

// TestTableNode_Getattr_DifferentTables tests Getattr for various table names
func TestTableNode_Getattr_DifferentTables(t *testing.T) {
	cfg := &config.Config{}
	ctx := context.Background()

	tables := []string{"users", "orders", "products", "inventory_items", "test_table_123"}

	for _, tableName := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableNode := NewTableNode(cfg, nil, nil, "public", tableName, nil, nil)

			var out fuse.AttrOut
			errno := tableNode.Getattr(ctx, nil, &out)

			if errno != 0 {
				t.Errorf("Expected errno=0 for table %s, got %d", tableName, errno)
			}

			// All tables should return same attributes
			if out.Mode != uint32(0700|syscall.S_IFDIR) {
				t.Errorf("Expected directory mode for table %s", tableName)
			}
		})
	}
}

// TestTableNode_MultipleSchemas tests TableNode with different schemas
func TestTableNode_MultipleSchemas(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		schema    string
		tableName string
	}{
		{"public", "users"},
		{"custom_schema", "orders"},
		{"analytics", "events"},
		{"", "test"}, // empty schema
	}

	for _, tc := range testCases {
		t.Run(tc.schema+"_"+tc.tableName, func(t *testing.T) {
			node := NewTableNode(cfg, nil, nil, tc.schema, tc.tableName, nil, nil)

			if node.schema != tc.schema {
				t.Errorf("Expected schema=%q, got %q", tc.schema, node.schema)
			}
			if node.tableName != tc.tableName {
				t.Errorf("Expected tableName=%q, got %q", tc.tableName, node.tableName)
			}
		})
	}
}

// ============================================================================
// Mock-based tests for database interactions (ADR-004)
// ============================================================================

// TestTableNode_Readdir_WithMock tests Readdir using MockDBClient
func TestTableNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowReader.ListRowsFunc = func(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
		return []string{"1", "2", "3"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewTableNode(cfg, mock, nil, "public", "users", partialRows, nil)

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

	// Should have special dirs plus 3 rows
	// Special dirs: .first, .last, .sample, .all, .index, .count, .ddl, .ddl/
	// The exact count depends on implementation, but should include our 3 rows
	found := 0
	for _, e := range entries {
		if e == "1" || e == "2" || e == "3" {
			found++
		}
	}

	if found != 3 {
		t.Errorf("Expected to find 3 row entries, found %d in %v", found, entries)
	}
}

// TestTableNode_Readdir_WithMock_Empty tests Readdir for an empty table
func TestTableNode_Readdir_WithMock_Empty(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowReader.ListRowsFunc = func(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
		return []string{}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewTableNode(cfg, mock, nil, "public", "users", partialRows, nil)

	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Fatal("Expected non-nil stream even for empty table")
	}

	// Should still have special directories
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have special directories even with empty table
	if len(entries) == 0 {
		t.Error("Expected special directories even for empty table")
	}
}

// TestTableNode_Readdir_WithMock_Error tests Readdir when database returns error
func TestTableNode_Readdir_WithMock_Error(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewTableNode(cfg, mock, nil, "public", "users", partialRows, nil)

	_, errno := node.Readdir(context.Background())

	// Should return EIO on database error
	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestTableNode_Lookup_WithMock_Row tests Lookup for a row
// NOTE: This test requires FUSE bridge infrastructure (NewPersistentInode).
// Lookup tests that create child inodes must be done via integration tests.
func TestTableNode_Lookup_WithMock_Row(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestTableNode_Lookup_WithMock_RowNotFound tests Lookup for a nonexistent row
// NOTE: This test requires FUSE bridge infrastructure (NewPersistentInode).
func TestTableNode_Lookup_WithMock_RowNotFound(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestTableNode_Lookup_WithMock_SpecialDirs tests Lookup for special directories
// NOTE: This test requires FUSE bridge infrastructure (NewPersistentInode).
func TestTableNode_Lookup_WithMock_SpecialDirs(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestTableNode_Lookup_WithMock_RowWithFormat tests Lookup for row with format extension
// NOTE: This test requires FUSE bridge infrastructure (NewPersistentInode).
func TestTableNode_Lookup_WithMock_RowWithFormat(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestTableNode_Unlink_WithMock tests Unlink (row deletion)
func TestTableNode_Unlink_WithMock(t *testing.T) {
	cfg := &config.Config{}

	var deleteCalled bool
	var deletedPK string

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowWriter.DeleteRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) error {
		deleteCalled = true
		deletedPK = pk.Values[0]
		return nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewTableNode(cfg, mock, nil, "public", "users", partialRows, nil)

	errno := node.Unlink(context.Background(), "1")

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if !deleteCalled {
		t.Error("Expected DeleteRow to be called")
	}

	if deletedPK != "1" {
		t.Errorf("Expected deleted PK='1', got %q", deletedPK)
	}
}

// TestTableNode_Unlink_WithMock_Error tests Unlink when database returns error
func TestTableNode_Unlink_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowWriter.DeleteRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) error {
		return context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewTableNode(cfg, mock, nil, "public", "users", partialRows, nil)

	errno := node.Unlink(context.Background(), "1")

	// Should return EIO on database error
	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestTableNode_Rmdir_WithMock tests Rmdir (row deletion via rm -r)
func TestTableNode_Rmdir_WithMock(t *testing.T) {
	cfg := &config.Config{}

	var deleteCalled bool

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowWriter.DeleteRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) error {
		deleteCalled = true
		return nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewTableNode(cfg, mock, nil, "public", "users", partialRows, nil)

	errno := node.Rmdir(context.Background(), "1")

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if !deleteCalled {
		t.Error("Expected DeleteRow to be called")
	}
}

// TestTableNode_Mkdir_WithMock tests Mkdir (create row directory)
// NOTE: This test requires FUSE bridge infrastructure (NewPersistentInode).
func TestTableNode_Mkdir_WithMock(t *testing.T) {
	t.Skip("Mkdir tests require FUSE bridge infrastructure - see test/integration/")
}

// Note: Full CRUD operation integration tests are in test/integration/
