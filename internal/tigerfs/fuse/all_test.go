package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestNewAllRowsNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	partialRows := NewPartialRowTracker(nil)
	allNode := NewAllRowsNode(cfg, nil, nil, "myschema", "users", partialRows)

	if allNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if allNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", allNode.tableName)
	}

	if allNode.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", allNode.schema)
	}

	if allNode.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}

	if allNode.db != nil {
		t.Error("Expected db to be nil")
	}

	if allNode.cache != nil {
		t.Error("Expected cache to be nil")
	}
}

func TestAllRowsNode_Getattr(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	allNode := NewAllRowsNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := allNode.Getattr(ctx, nil, &out)

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

func TestAllRowsNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	allNode := NewAllRowsNode(cfg, nil, nil, "public", "users", nil)

	// Verify interface assertions compile (checked at compile time via var _ declarations)
	_ = allNode
}

// =============================================================================
// Mock-based tests
// =============================================================================

// TestAllRowsNode_Readdir_WithMock tests Readdir listing all rows
func TestAllRowsNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 10000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowReader.ListAllRowsFunc = func(ctx context.Context, schema, table, pkColumn string) ([]string, error) {
		return []string{"1", "2", "3", "4", "5"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewAllRowsNode(cfg, mock, nil, "public", "users", partialRows)

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

	// Should have 3 metadata files + 5 rows = 8 entries
	if len(entries) != 8 {
		t.Errorf("Expected 8 entries, got %d", len(entries))
	}

	// Check metadata files are present
	hasColumns := false
	hasSchema := false
	hasCount := false
	for _, name := range entries {
		switch name {
		case ".columns":
			hasColumns = true
		case ".schema":
			hasSchema = true
		case ".count":
			hasCount = true
		}
	}

	if !hasColumns || !hasSchema || !hasCount {
		t.Error("Expected metadata files .columns, .schema, .count")
	}
}

// TestAllRowsNode_Readdir_WithMock_Empty tests Readdir with no rows
func TestAllRowsNode_Readdir_WithMock_Empty(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 10000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowReader.ListAllRowsFunc = func(ctx context.Context, schema, table, pkColumn string) ([]string, error) {
		return []string{}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewAllRowsNode(cfg, mock, nil, "public", "users", partialRows)

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

	// Should have only 3 metadata files
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries (metadata only), got %d", len(entries))
	}
}

// TestAllRowsNode_Readdir_WithMock_PKError tests Readdir when GetPrimaryKey fails
func TestAllRowsNode_Readdir_WithMock_PKError(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 10000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewAllRowsNode(cfg, mock, nil, "public", "users", partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestAllRowsNode_Readdir_WithMock_ListError tests Readdir when ListAllRows fails
func TestAllRowsNode_Readdir_WithMock_ListError(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 10000,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockRowReader.ListAllRowsFunc = func(ctx context.Context, schema, table, pkColumn string) ([]string, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewAllRowsNode(cfg, mock, nil, "public", "users", partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestAllRowsNode_Lookup_WithMock tests are skipped because Lookup requires
// FUSE bridge infrastructure (NewPersistentInode). See test/integration/.
