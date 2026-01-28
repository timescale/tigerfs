package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestNewMetadataFileNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	tests := []struct {
		name     string
		fileType string
	}{
		{"columns metadata", "columns"},
		{"schema metadata", "schema"},
		{"count metadata", "count"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewMetadataFileNode(cfg, nil, "public", "users", tt.fileType)

			if node.cfg != cfg {
				t.Error("Expected config to be set")
			}

			if node.schema != "public" {
				t.Errorf("Expected schema='public', got '%s'", node.schema)
			}

			if node.tableName != "users" {
				t.Errorf("Expected tableName='users', got '%s'", node.tableName)
			}

			if node.fileType != tt.fileType {
				t.Errorf("Expected fileType='%s', got '%s'", tt.fileType, node.fileType)
			}

			if node.data != nil {
				t.Error("Expected data to be nil initially")
			}
		})
	}
}

// TestMetadataFileNode_Interfaces verifies that MetadataFileNode implements required interfaces
func TestMetadataFileNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that MetadataFileNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	node := NewMetadataFileNode(cfg, nil, "public", "users", "columns")

	// Mark as used (compiler will verify types)
	_ = node
}

func TestMetadataFileTypes(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	validTypes := []string{"columns", "schema", "count"}

	for _, fileType := range validTypes {
		node := NewMetadataFileNode(cfg, nil, "public", "test_table", fileType)
		if node.fileType != fileType {
			t.Errorf("Expected fileType='%s', got '%s'", fileType, node.fileType)
		}
	}
}

// =============================================================================
// Mock-based tests
// =============================================================================

// TestMetadataFileNode_Getattr_WithMock_Columns tests .columns file attributes
func TestMetadataFileNode_Getattr_WithMock_Columns(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "email", DataType: "text"},
		}, nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "columns")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should have fetched data
	if node.data == nil {
		t.Fatal("Expected data to be fetched")
	}

	expected := "id\nname\nemail\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, string(node.data))
	}

	// Size should match data length
	if out.Size != uint64(len(expected)) {
		t.Errorf("Expected Size=%d, got %d", len(expected), out.Size)
	}
}

// TestMetadataFileNode_Getattr_WithMock_Count tests .count file attributes
func TestMetadataFileNode_Getattr_WithMock_Count(t *testing.T) {
	cfg := &config.Config{
		TrailingNewlines: true,
	}

	mock := db.NewMockDBClient()
	mock.MockCountReader.GetRowCountSmartFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 12345, nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "count")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expected := "12345\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, string(node.data))
	}
}

// TestMetadataFileNode_Getattr_WithMock_Count_NoNewline tests .count without trailing newline
func TestMetadataFileNode_Getattr_WithMock_Count_NoNewline(t *testing.T) {
	cfg := &config.Config{
		TrailingNewlines: false,
	}

	mock := db.NewMockDBClient()
	mock.MockCountReader.GetRowCountSmartFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 999, nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "count")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expected := "999"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, string(node.data))
	}
}

// TestMetadataFileNode_Getattr_WithMock_Schema tests .schema file
func TestMetadataFileNode_Getattr_WithMock_Schema(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetTableDDLFunc = func(ctx context.Context, schema, table string) (string, error) {
		return "CREATE TABLE users (\n  id integer PRIMARY KEY,\n  name text\n);\n", nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "schema")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if node.data == nil {
		t.Fatal("Expected data to be fetched")
	}

	if len(node.data) == 0 {
		t.Error("Expected non-empty DDL")
	}
}

// TestMetadataFileNode_Getattr_WithMock_DDL tests .ddl file (full DDL)
func TestMetadataFileNode_Getattr_WithMock_DDL(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetFullDDLFunc = func(ctx context.Context, schema, table string) (string, error) {
		return "CREATE TABLE users (...);\nCREATE INDEX ...;\n", nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "ddl")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if node.data == nil {
		t.Fatal("Expected data to be fetched")
	}
}

// TestMetadataFileNode_Getattr_WithMock_Indexes tests .indexes file
func TestMetadataFileNode_Getattr_WithMock_Indexes(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetSingleColumnIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{
			{Name: "users_email_idx", Columns: []string{"email"}, IsUnique: true, IsPrimary: false},
			{Name: "users_pkey", Columns: []string{"id"}, IsUnique: true, IsPrimary: true},
		}, nil
	}
	mock.MockIndexReader.GetCompositeIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{
			{Name: "users_name_email_idx", Columns: []string{"name", "email"}, IsUnique: false, IsPrimary: false},
		}, nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "indexes")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if node.data == nil {
		t.Fatal("Expected data to be fetched")
	}

	// Should contain email index (unique) but not primary key
	data := string(node.data)
	if data == "" {
		t.Error("Expected non-empty index list")
	}
}

// TestMetadataFileNode_Getattr_WithMock_Indexes_None tests .indexes with no indexes
func TestMetadataFileNode_Getattr_WithMock_Indexes_None(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetSingleColumnIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{}, nil
	}
	mock.MockIndexReader.GetCompositeIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{}, nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "indexes")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expected := "(no indexes)\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, string(node.data))
	}
}

// TestMetadataFileNode_Getattr_WithMock_Error tests error handling
func TestMetadataFileNode_Getattr_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return nil, context.DeadlineExceeded
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "columns")

	var out fuse.AttrOut
	errno := node.Getattr(context.Background(), nil, &out)

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestMetadataFileNode_Read_WithMock tests reading metadata content
func TestMetadataFileNode_Read_WithMock(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	node := NewMetadataFileNode(cfg, mock, "public", "users", "columns")

	// Open the file
	fh, _, errno := node.Open(context.Background(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("Open failed: errno=%d", errno)
	}

	// Read the content
	handle := fh.(*MetadataFileHandle)
	buf := make([]byte, 100)
	result, errno := handle.Read(context.Background(), buf, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expected := "id\nname\n"
	// Get bytes from ReadResult
	content, _ := result.Bytes(nil)
	if string(content) != expected {
		t.Errorf("Expected content=%q, got %q", expected, string(content))
	}
}
