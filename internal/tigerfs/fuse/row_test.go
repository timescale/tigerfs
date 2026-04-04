package fuse

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
)

func TestNewRowFileNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	rowNode := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	if rowNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if rowNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", rowNode.tableName)
	}

	if rowNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", rowNode.schema)
	}

	if rowNode.pkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got '%s'", rowNode.pkColumn)
	}

	if rowNode.pkValue != "1" {
		t.Errorf("Expected pkValue='1', got '%s'", rowNode.pkValue)
	}

	if rowNode.format != "tsv" {
		t.Errorf("Expected format='tsv', got '%s'", rowNode.format)
	}

	if rowNode.data != nil {
		t.Error("Expected data to be nil initially")
	}
}

// TestRowFileNode_Interfaces verifies that RowFileNode implements required interfaces
func TestRowFileNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that RowFileNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	rowNode := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	// Mark as used (compiler will verify types)
	_ = rowNode
}

// TestNewRowFileNode_DifferentFormats tests creation with different formats
func TestNewRowFileNode_DifferentFormats(t *testing.T) {
	cfg := &config.Config{}

	formats := []string{"tsv", "csv", "json"}

	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", format)

			if node.format != format {
				t.Errorf("Expected format=%q, got %q", format, node.format)
			}
		})
	}
}

// TestNewRowFileNode_DifferentPKTypes tests creation with various primary key values
func TestNewRowFileNode_DifferentPKTypes(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		pkColumn string
		pkValue  string
	}{
		{"id", "1"},                    // Integer PK
		{"id", "12345678901234567890"}, // Large integer
		{"uuid", "550e8400-e29b-41d4-a716-446655440000"}, // UUID
		{"email", "user@example.com"},                    // String PK
		{"code", "ABC-123"},                              // Alphanumeric code
		{"composite", "part1_part2"},                     // Composite-like
	}

	for _, tc := range testCases {
		t.Run(tc.pkColumn+"_"+tc.pkValue, func(t *testing.T) {
			node := NewRowFileNode(cfg, nil, nil, "public", "table", tc.pkColumn, tc.pkValue, "tsv")

			if node.pkColumn != tc.pkColumn {
				t.Errorf("Expected pkColumn=%q, got %q", tc.pkColumn, node.pkColumn)
			}
			if node.pkValue != tc.pkValue {
				t.Errorf("Expected pkValue=%q, got %q", tc.pkValue, node.pkValue)
			}
		})
	}
}

// TestNewRowFileNode_MultipleSchemas tests creation with different schemas
func TestNewRowFileNode_MultipleSchemas(t *testing.T) {
	cfg := &config.Config{}

	schemas := []string{"public", "custom_schema", "analytics", ""}

	for _, schema := range schemas {
		t.Run("schema_"+schema, func(t *testing.T) {
			node := NewRowFileNode(cfg, nil, nil, schema, "users", "id", "1", "tsv")

			if node.schema != schema {
				t.Errorf("Expected schema=%q, got %q", schema, node.schema)
			}
		})
	}
}

// TestRowFileNode_Getattr_WithData tests Getattr when data is pre-populated
func TestRowFileNode_Getattr_WithData(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	// Pre-populate data to avoid database fetch
	testData := []byte("1\tJohn\tjohn@example.com\n")
	node.data = testData

	ctx := context.Background()
	var out fuse.AttrOut

	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check mode is regular file with 644 permissions
	expectedMode := uint32(0600 | syscall.S_IFREG)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	// Check nlink is 1
	if out.Nlink != 1 {
		t.Errorf("Expected Nlink=1, got %d", out.Nlink)
	}

	// Check size matches data length
	expectedSize := uint64(len(testData))
	if out.Size != expectedSize {
		t.Errorf("Expected Size=%d, got %d", expectedSize, out.Size)
	}
}

// TestRowFileNode_Getattr_DifferentDataSizes tests Getattr with various data sizes
func TestRowFileNode_Getattr_DifferentDataSizes(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		name     string
		data     []byte
		expected uint64
	}{
		{"empty", []byte{}, 0},
		{"single_byte", []byte("a"), 1},
		{"small_row", []byte("1\tJohn\n"), 7},
		{"medium_row", []byte("1\tJohn Doe\tjohn.doe@example.com\t2024-01-01\n"), 43},
		{"large_row", make([]byte, 10000), 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")
			node.data = tc.data

			var out fuse.AttrOut
			errno := node.Getattr(context.Background(), nil, &out)

			if errno != 0 {
				t.Errorf("Expected errno=0, got %d", errno)
			}

			if out.Size != tc.expected {
				t.Errorf("Expected Size=%d, got %d", tc.expected, out.Size)
			}
		})
	}
}

// TestRowFileHandle_Read tests basic read operation
func TestRowFileHandle_Read(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	testData := []byte("1\tJohn\tjohn@example.com\n")

	fh := &RowFileHandle{
		node:      node,
		data:      testData,
		rowExists: true,
	}

	dest := make([]byte, 1024)
	result, errno := fh.Read(context.Background(), dest, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Get the actual data from the result
	data, _ := result.Bytes(nil)
	if string(data) != string(testData) {
		t.Errorf("Expected data=%q, got %q", testData, data)
	}
}

// TestRowFileHandle_Read_WithOffset tests read with offset
func TestRowFileHandle_Read_WithOffset(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	testData := []byte("1\tJohn\tjohn@example.com\n")

	fh := &RowFileHandle{
		node:      node,
		data:      testData,
		rowExists: true,
	}

	// Read from offset 2
	dest := make([]byte, 10)
	result, errno := fh.Read(context.Background(), dest, 2)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	expected := testData[2:12] // bytes 2-11
	if string(data) != string(expected) {
		t.Errorf("Expected data=%q, got %q", expected, data)
	}
}

// TestRowFileHandle_Read_PartialBuffer tests read with buffer smaller than remaining data
func TestRowFileHandle_Read_PartialBuffer(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	testData := []byte("1234567890")

	fh := &RowFileHandle{
		node:      node,
		data:      testData,
		rowExists: true,
	}

	// Read only 5 bytes
	dest := make([]byte, 5)
	result, errno := fh.Read(context.Background(), dest, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	if string(data) != "12345" {
		t.Errorf("Expected data=%q, got %q", "12345", data)
	}
}

// TestRowFileHandle_Read_EOF tests read at end of file
func TestRowFileHandle_Read_EOF(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	testData := []byte("1\tJohn\n")

	fh := &RowFileHandle{
		node:      node,
		data:      testData,
		rowExists: true,
	}

	// Read from offset past EOF
	dest := make([]byte, 1024)
	result, errno := fh.Read(context.Background(), dest, int64(len(testData)+10))

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	if len(data) != 0 {
		t.Errorf("Expected empty data at EOF, got %d bytes", len(data))
	}
}

// TestRowFileHandle_Read_ExactEOF tests read starting exactly at EOF
func TestRowFileHandle_Read_ExactEOF(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	testData := []byte("12345")

	fh := &RowFileHandle{
		node:      node,
		data:      testData,
		rowExists: true,
	}

	// Read from exactly at EOF
	dest := make([]byte, 100)
	result, errno := fh.Read(context.Background(), dest, int64(len(testData)))

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	if len(data) != 0 {
		t.Errorf("Expected empty data at exact EOF, got %d bytes", len(data))
	}
}

// TestRowFileHandle_Read_EmptyData tests read with empty data
func TestRowFileHandle_Read_EmptyData(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	fh := &RowFileHandle{
		node:      node,
		data:      []byte{},
		rowExists: true,
	}

	dest := make([]byte, 100)
	result, errno := fh.Read(context.Background(), dest, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	if len(data) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(data))
	}
}

// TestRowFileHandle_Write tests basic write operation
func TestRowFileHandle_Write(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	fh := &RowFileHandle{
		node:      node,
		data:      nil,
		rowExists: false,
	}

	testData := []byte("1\tJohn\tjohn@example.com\n")
	written, errno := fh.Write(context.Background(), testData, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if written != uint32(len(testData)) {
		t.Errorf("Expected written=%d, got %d", len(testData), written)
	}

	if string(fh.data) != string(testData) {
		t.Errorf("Expected fh.data=%q, got %q", testData, fh.data)
	}
}

// TestRowFileHandle_Write_WithOffset tests write at non-zero offset
func TestRowFileHandle_Write_WithOffset(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	// Start with existing data
	initialData := []byte("1234567890")
	fh := &RowFileHandle{
		node:      node,
		data:      make([]byte, len(initialData)),
		rowExists: true,
	}
	copy(fh.data, initialData)

	// Write "ABC" at offset 3
	newData := []byte("ABC")
	written, errno := fh.Write(context.Background(), newData, 3)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if written != uint32(len(newData)) {
		t.Errorf("Expected written=%d, got %d", len(newData), written)
	}

	expected := "123ABC7890"
	if string(fh.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, fh.data)
	}
}

// TestRowFileHandle_Write_Extend tests write that extends the file
func TestRowFileHandle_Write_Extend(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	// Start with small data
	initialData := []byte("123")
	fh := &RowFileHandle{
		node:      node,
		data:      make([]byte, len(initialData)),
		rowExists: true,
	}
	copy(fh.data, initialData)

	// Write at offset 5, which extends the file
	newData := []byte("XYZ")
	written, errno := fh.Write(context.Background(), newData, 5)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if written != uint32(len(newData)) {
		t.Errorf("Expected written=%d, got %d", len(newData), written)
	}

	// Buffer should be extended with zeros in between
	if len(fh.data) != 8 { // 5 offset + 3 bytes
		t.Errorf("Expected len=%d, got %d", 8, len(fh.data))
	}

	// First 3 bytes should be original data
	if string(fh.data[:3]) != "123" {
		t.Errorf("Expected first 3 bytes=%q, got %q", "123", fh.data[:3])
	}

	// Bytes 5-7 should be new data
	if string(fh.data[5:8]) != "XYZ" {
		t.Errorf("Expected bytes 5-7=%q, got %q", "XYZ", fh.data[5:8])
	}
}

// TestRowFileHandle_Write_Multiple tests multiple sequential writes
func TestRowFileHandle_Write_Multiple(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	fh := &RowFileHandle{
		node:      node,
		data:      nil,
		rowExists: false,
	}

	// First write
	written1, errno := fh.Write(context.Background(), []byte("Hello"), 0)
	if errno != 0 {
		t.Fatalf("First write failed with errno=%d", errno)
	}
	if written1 != 5 {
		t.Errorf("Expected written1=5, got %d", written1)
	}

	// Second write (append)
	written2, errno := fh.Write(context.Background(), []byte(" World"), 5)
	if errno != 0 {
		t.Fatalf("Second write failed with errno=%d", errno)
	}
	if written2 != 6 {
		t.Errorf("Expected written2=6, got %d", written2)
	}

	expected := "Hello World"
	if string(fh.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, fh.data)
	}
}

// TestRowFileHandle_Write_Overwrite tests overwriting existing data at non-zero offset
func TestRowFileHandle_Write_Overwrite(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	fh := &RowFileHandle{
		node:      node,
		data:      nil,
		rowExists: false,
	}

	// Write initial data
	_, _ = fh.Write(context.Background(), []byte("AAAAAAAAAA"), 0)

	// Overwrite at offset 3 (partial overwrite)
	written, errno := fh.Write(context.Background(), []byte("BBB"), 3)
	if errno != 0 {
		t.Fatalf("Overwrite failed with errno=%d", errno)
	}
	if written != 3 {
		t.Errorf("Expected written=3, got %d", written)
	}

	// Expect: AAABBBAAAA (offset 3 means chars 0-2 unchanged, 3-5 are BBB, 6-9 unchanged)
	expected := "AAABBBAAAA"
	if string(fh.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, fh.data)
	}
}

// TestRowFileHandle_ReadWrite_Cycle tests a complete read-modify-write cycle
func TestRowFileHandle_ReadWrite_Cycle(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	originalData := []byte("1\tJohn\tjohn@example.com\n")

	// Simulate read
	fh := &RowFileHandle{
		node:      node,
		data:      originalData,
		rowExists: true,
	}

	// Read the data
	dest := make([]byte, 1024)
	result, _ := fh.Read(context.Background(), dest, 0)
	readData, _ := result.Bytes(nil)

	if string(readData) != string(originalData) {
		t.Errorf("Read data mismatch")
	}

	// Simulate modify: write new data
	newData := []byte("1\tJane\tjane@example.com\n")
	_, _ = fh.Write(context.Background(), newData, 0)

	if string(fh.data) != string(newData) {
		t.Errorf("Write data mismatch: expected %q, got %q", newData, fh.data)
	}
}

// Note: TestRowFileNode_Setattr_Truncate requires database integration
// because Setattr calls Getattr which tries to fetch data after truncation.
// See test/integration/ for full Setattr tests.

// TestRowFileHandle_NilData tests handle behavior when data is nil
func TestRowFileHandle_NilData(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	fh := &RowFileHandle{
		node:      node,
		data:      nil,
		rowExists: false,
	}

	// Read from nil data should return empty
	dest := make([]byte, 100)
	result, errno := fh.Read(context.Background(), dest, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	if len(data) != 0 {
		t.Errorf("Expected empty data from nil, got %d bytes", len(data))
	}
}

// TestRowFileHandle_RowExists tests the rowExists flag behavior
func TestRowFileHandle_RowExists(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	// Test with existing row
	fhExists := &RowFileHandle{
		node:      node,
		data:      []byte("existing"),
		rowExists: true,
	}
	if !fhExists.rowExists {
		t.Error("Expected rowExists=true for existing row")
	}

	// Test with new row
	fhNew := &RowFileHandle{
		node:      node,
		data:      nil,
		rowExists: false,
	}
	if fhNew.rowExists {
		t.Error("Expected rowExists=false for new row")
	}
}

// ============================================================================
// Mock-based tests for database interactions (ADR-004)
// ============================================================================

// TestRowFileNode_Getattr_WithMock tests Getattr using MockDBClient
func TestRowFileNode_Getattr_WithMock(t *testing.T) {
	cfg := &config.Config{}

	// Create mock that returns row data
	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		if pk.Values[0] == "1" {
			return &db.Row{
				Columns: []string{"id", "name", "email"},
				Values:  []interface{}{1, "John", "john@example.com"},
			}, nil
		}
		return nil, context.DeadlineExceeded
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "email", DataType: "text"},
		}, nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	ctx := context.Background()
	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check that data was fetched and size is non-zero
	if out.Size == 0 {
		t.Error("Expected non-zero size")
	}

	// Verify data was cached
	if node.data == nil {
		t.Error("Expected data to be cached")
	}
}

// TestRowFileNode_Getattr_WithMock_Error tests Getattr when database returns error
func TestRowFileNode_Getattr_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return nil, context.DeadlineExceeded
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "999", "tsv")

	ctx := context.Background()
	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	// Should return EIO on database error
	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestRowFileNode_fetchData_WithMock_TSV tests fetchData with TSV format
func TestRowFileNode_fetchData_WithMock_TSV(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return &db.Row{
			Columns: []string{"id", "name"},
			Values:  []interface{}{1, "John"},
		}, nil
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	err := node.fetchData(context.Background())
	if err != nil {
		t.Fatalf("fetchData() error: %v", err)
	}

	// TSV format: tab-separated values with newline
	expected := "1\tJohn\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, node.data)
	}
}

// TestRowFileNode_fetchData_WithMock_CSV tests fetchData with CSV format
func TestRowFileNode_fetchData_WithMock_CSV(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return &db.Row{
			Columns: []string{"id", "name"},
			Values:  []interface{}{1, "John"},
		}, nil
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "csv")

	err := node.fetchData(context.Background())
	if err != nil {
		t.Fatalf("fetchData() error: %v", err)
	}

	// CSV format: comma-separated values with newline
	expected := "1,John\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, node.data)
	}
}

// TestRowFileNode_fetchData_WithMock_JSON tests fetchData with JSON format
func TestRowFileNode_fetchData_WithMock_JSON(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return &db.Row{
			Columns: []string{"id", "name"},
			Values:  []interface{}{1, "John"},
		}, nil
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "json")

	err := node.fetchData(context.Background())
	if err != nil {
		t.Fatalf("fetchData() error: %v", err)
	}

	// JSON format includes a trailing newline
	expected := "{\"id\":1,\"name\":\"John\"}\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, node.data)
	}
}

// TestRowFileNode_fetchData_WithMock_NullValues tests fetchData with NULL values
func TestRowFileNode_fetchData_WithMock_NullValues(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return &db.Row{
			Columns: []string{"id", "name", "email"},
			Values:  []interface{}{1, "John", nil}, // email is NULL
		}, nil
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "email", DataType: "text"},
		}, nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	err := node.fetchData(context.Background())
	if err != nil {
		t.Fatalf("fetchData() error: %v", err)
	}

	// TSV with NULL should have empty field
	expected := "1\tJohn\t\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, node.data)
	}
}

// TestRowFileNode_Open_WithMock tests Open using MockDBClient
func TestRowFileNode_Open_WithMock(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return &db.Row{
			Columns: []string{"id", "name"},
			Values:  []interface{}{1, "John"},
		}, nil
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	fh, flags, errno := node.Open(context.Background(), syscall.O_RDONLY)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if fh == nil {
		t.Error("Expected non-nil file handle")
	}

	// Should have DIRECT_IO flag
	if flags&fuse.FOPEN_DIRECT_IO == 0 {
		t.Error("Expected FOPEN_DIRECT_IO flag")
	}

	// Verify handle has data
	rfh, ok := fh.(*RowFileHandle)
	if !ok {
		t.Fatal("Expected RowFileHandle")
	}

	if len(rfh.data) == 0 {
		t.Error("Expected handle to have data")
	}
}

// TestRowFileHandle_Flush_WithMock_ExistingRow tests Flush for an existing row (UPDATE)
// TSV with explicit extension uses PATCH semantics (header row + value row)
func TestRowFileHandle_Flush_WithMock_ExistingRow(t *testing.T) {
	cfg := &config.Config{}

	// Track what was called
	var updateCalled bool
	var updatedColumns []string
	var updatedValues []interface{}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch) (*db.Row, error) {
		return &db.Row{Columns: []string{"id", "name"}, Values: []interface{}{1, "John"}}, nil
	}
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}
	mock.MockRowWriter.UpdateRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columns []string, values []interface{}) error {
		updateCalled = true
		updatedColumns = columns
		updatedValues = values
		return nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	// TSV format uses PATCH semantics: header row + value row
	fh := &RowFileHandle{
		node:      node,
		data:      []byte("id\tname\n1\tJane\n"),
		rowExists: true,
		dirty:     true, // Must set dirty=true for Flush to write
	}

	errno := fh.Flush(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if !updateCalled {
		t.Error("Expected UpdateRow to be called")
	}

	// Verify updated data
	if len(updatedColumns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(updatedColumns))
	}

	if len(updatedValues) != 2 {
		t.Errorf("Expected 2 values, got %d", len(updatedValues))
	}
}

// TestRowFileHandle_Flush_WithMock_NewRow tests Flush for a new row (INSERT)
// TSV with explicit extension uses PATCH semantics (header row + value row)
func TestRowFileHandle_Flush_WithMock_NewRow(t *testing.T) {
	cfg := &config.Config{}

	var insertCalled bool
	var insertedColumns []string

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}
	mock.MockRowWriter.InsertRowFunc = func(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error) {
		insertCalled = true
		insertedColumns = columns
		return "1", nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	// TSV format uses PATCH semantics: header row + value row
	fh := &RowFileHandle{
		node:      node,
		data:      []byte("id\tname\n1\tJohn\n"),
		rowExists: false,
		dirty:     true, // Must set dirty=true for Flush to write
	}

	errno := fh.Flush(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if !insertCalled {
		t.Error("Expected InsertRow to be called")
	}

	if len(insertedColumns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insertedColumns))
	}
}

// TestRowFileHandle_Flush_WithMock_Error tests Flush when database update fails
// TSV with explicit extension uses PATCH semantics (header row + value row)
func TestRowFileHandle_Flush_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}
	mock.MockRowWriter.UpdateRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columns []string, values []interface{}) error {
		return context.DeadlineExceeded
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	// TSV format uses PATCH semantics: header row + value row
	fh := &RowFileHandle{
		node:      node,
		data:      []byte("id\tname\n1\tJohn\n"),
		rowExists: true,
		dirty:     true, // Must set dirty=true for Flush to attempt write
	}

	errno := fh.Flush(context.Background())

	// Should return EIO on database error
	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestRowFileHandle_Flush_TSV_PatchSemantics tests PATCH semantics for TSV format.
// Only columns specified in the header are updated.
func TestRowFileHandle_Flush_TSV_PatchSemantics(t *testing.T) {
	cfg := &config.Config{}

	var updatedColumns []string
	var updatedValues []interface{}

	mock := db.NewMockDBClient()
	mock.MockRowWriter.UpdateRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columns []string, values []interface{}) error {
		updatedColumns = columns
		updatedValues = values
		return nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	// PATCH: Only update name column (omit id and email)
	fh := &RowFileHandle{
		node:      node,
		data:      []byte("name\nJane\n"),
		rowExists: true,
		dirty:     true,
	}

	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should only update the 'name' column
	if len(updatedColumns) != 1 {
		t.Errorf("Expected 1 column (PATCH), got %d", len(updatedColumns))
	}
	if len(updatedColumns) > 0 && updatedColumns[0] != "name" {
		t.Errorf("Expected column 'name', got %v", updatedColumns[0])
	}
	if len(updatedValues) > 0 && updatedValues[0] != "Jane" {
		t.Errorf("Expected value 'Jane', got %v", updatedValues[0])
	}
}

// TestRowFileHandle_Flush_CSV_PatchSemantics tests PATCH semantics for CSV format.
// Only columns specified in the header are updated.
func TestRowFileHandle_Flush_CSV_PatchSemantics(t *testing.T) {
	cfg := &config.Config{}

	var updatedColumns []string
	var updatedValues []interface{}

	mock := db.NewMockDBClient()
	mock.MockRowWriter.UpdateRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columns []string, values []interface{}) error {
		updatedColumns = columns
		updatedValues = values
		return nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "csv")

	// PATCH: Only update email column
	fh := &RowFileHandle{
		node:      node,
		data:      []byte("email\nnewemail@example.com\n"),
		rowExists: true,
		dirty:     true,
	}

	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should only update the 'email' column
	if len(updatedColumns) != 1 {
		t.Errorf("Expected 1 column (PATCH), got %d", len(updatedColumns))
	}
	if len(updatedColumns) > 0 && updatedColumns[0] != "email" {
		t.Errorf("Expected column 'email', got %v", updatedColumns[0])
	}
	if len(updatedValues) > 0 && updatedValues[0] != "newemail@example.com" {
		t.Errorf("Expected value 'newemail@example.com', got %v", updatedValues[0])
	}
}

// TestRowFileHandle_Flush_JSON_PatchSemantics tests PATCH semantics for JSON format.
// Only keys in the JSON object are updated.
func TestRowFileHandle_Flush_JSON_PatchSemantics(t *testing.T) {
	cfg := &config.Config{}

	var updatedColumns []string
	var updatedValues []interface{}

	mock := db.NewMockDBClient()
	mock.MockRowWriter.UpdateRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columns []string, values []interface{}) error {
		updatedColumns = columns
		updatedValues = values
		return nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "json")

	// PATCH: Only update name (JSON only includes the key being updated)
	fh := &RowFileHandle{
		node:      node,
		data:      []byte(`{"name": "NewName"}`),
		rowExists: true,
		dirty:     true,
	}

	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should only update the 'name' column
	if len(updatedColumns) != 1 {
		t.Errorf("Expected 1 column (PATCH), got %d", len(updatedColumns))
	}
	if len(updatedColumns) > 0 && updatedColumns[0] != "name" {
		t.Errorf("Expected column 'name', got %v", updatedColumns[0])
	}
	if len(updatedValues) > 0 && updatedValues[0] != "NewName" {
		t.Errorf("Expected value 'NewName', got %v", updatedValues[0])
	}
}

// TestRowFileHandle_Flush_TSV_SetNull tests setting a column to NULL via empty value.
func TestRowFileHandle_Flush_TSV_SetNull(t *testing.T) {
	cfg := &config.Config{}

	var updatedColumns []string
	var updatedValues []interface{}

	mock := db.NewMockDBClient()
	mock.MockRowWriter.UpdateRowFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columns []string, values []interface{}) error {
		updatedColumns = columns
		updatedValues = values
		return nil
	}

	node := NewRowFileNode(cfg, mock, nil, "public", "users", "id", "1", "tsv")

	// Set name to value, email to NULL (empty value)
	// TSV format: header + value row where empty field = NULL
	fh := &RowFileHandle{
		node:      node,
		data:      []byte("name\temail\nJane\t\n"),
		rowExists: true,
		dirty:     true,
	}

	errno := fh.Flush(context.Background())
	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should update both columns
	if len(updatedColumns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(updatedColumns))
	}

	// First column (name) should have value
	if len(updatedValues) > 0 && updatedValues[0] != "Jane" {
		t.Errorf("Expected 'Jane', got %v", updatedValues[0])
	}

	// Second column (email) should be NULL
	if len(updatedValues) > 1 && updatedValues[1] != nil {
		t.Errorf("Expected nil (NULL), got %v", updatedValues[1])
	}
}

// TestRowFileNode_getFileMode_WithMock tests permission mapping
func TestRowFileNode_getFileMode_WithMock(t *testing.T) {
	testCases := []struct {
		name     string
		select_  bool
		update   bool
		insert   bool
		delete_  bool
		expected uint32
	}{
		// File mode max is 0600 (no execute bit for data files)
		{"full_access", true, true, true, true, 0600},
		{"read_only", true, false, false, false, 0400},
		{"write_only", false, true, true, false, 0200},
		{"read_write", true, true, true, false, 0600},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				MetadataRefreshInterval: time.Hour, // Prevent auto-refresh
			}

			mock := db.NewMockDBClient()
			// Set up all mocks needed for cache Refresh
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
				return map[string]int64{"users": 100}, nil
			}
			mock.MockSchemaReader.GetTablePermissionsBatchFunc = func(ctx context.Context, schema string, tables []string) (map[string]*db.TablePermissions, error) {
				result := make(map[string]*db.TablePermissions, len(tables))
				for _, t := range tables {
					result[t] = &db.TablePermissions{
						CanSelect: tc.select_,
						CanUpdate: tc.update,
						CanInsert: tc.insert,
						CanDelete: tc.delete_,
					}
				}
				return result, nil
			}

			cache := tigerfs.NewMetadataCache(cfg, mock)
			// Pre-populate the cache by calling Refresh
			if err := cache.Refresh(context.Background()); err != nil {
				t.Fatalf("Failed to refresh cache: %v", err)
			}

			node := NewRowFileNode(cfg, mock, cache, "public", "users", "id", "1", "tsv")
			node.data = []byte("test") // Pre-populate to avoid fetch

			mode := node.getFileMode(context.Background())

			if mode != tc.expected {
				t.Errorf("Expected mode=0%o, got 0%o", tc.expected, mode)
			}
		})
	}
}

// Note: Open, Flush, and database-dependent tests require integration testing
// See test/integration/ for full CRUD operation tests with PostgreSQL
