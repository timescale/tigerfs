package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
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

// Note: Open, Flush, and database-dependent tests require integration testing
// See test/integration/ for full CRUD operation tests with PostgreSQL
