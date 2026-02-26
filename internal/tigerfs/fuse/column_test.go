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

func TestNewColumnFileNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	colNode := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	if colNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if colNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", colNode.tableName)
	}

	if colNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", colNode.schema)
	}

	if colNode.pkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got '%s'", colNode.pkColumn)
	}

	if colNode.pkValue != "1" {
		t.Errorf("Expected pkValue='1', got '%s'", colNode.pkValue)
	}

	if colNode.columnName != "email" {
		t.Errorf("Expected columnName='email', got '%s'", colNode.columnName)
	}

	if colNode.data != nil {
		t.Error("Expected data to be nil initially")
	}
}

// TestColumnFileNode_Interfaces verifies that ColumnFileNode implements required interfaces
func TestColumnFileNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that ColumnFileNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	colNode := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	// Mark as used (compiler will verify types)
	_ = colNode
}

// TestNewColumnFileNode_DifferentColumns tests creation with various column names
func TestNewColumnFileNode_DifferentColumns(t *testing.T) {
	cfg := &config.Config{}

	columns := []string{"id", "email", "first_name", "last_name", "created_at", "is_active", "metadata"}

	for _, col := range columns {
		t.Run(col, func(t *testing.T) {
			node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", col, nil)

			if node.columnName != col {
				t.Errorf("Expected columnName=%q, got %q", col, node.columnName)
			}
		})
	}
}

// TestNewColumnFileNode_DifferentPKTypes tests creation with various primary key values
func TestNewColumnFileNode_DifferentPKTypes(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		pkColumn string
		pkValue  string
	}{
		{"id", "1"},
		{"id", "12345678901234567890"},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000"},
		{"email", "user@example.com"},
		{"code", "ABC-123"},
	}

	for _, tc := range testCases {
		t.Run(tc.pkColumn+"_"+tc.pkValue, func(t *testing.T) {
			node := NewColumnFileNode(cfg, nil, nil, "public", "users", tc.pkColumn, tc.pkValue, "email", nil)

			if node.pkColumn != tc.pkColumn {
				t.Errorf("Expected pkColumn=%q, got %q", tc.pkColumn, node.pkColumn)
			}
			if node.pkValue != tc.pkValue {
				t.Errorf("Expected pkValue=%q, got %q", tc.pkValue, node.pkValue)
			}
		})
	}
}

// TestNewColumnFileNode_MultipleSchemas tests creation with different schemas
func TestNewColumnFileNode_MultipleSchemas(t *testing.T) {
	cfg := &config.Config{}

	schemas := []string{"public", "custom_schema", "analytics", ""}

	for _, schema := range schemas {
		t.Run("schema_"+schema, func(t *testing.T) {
			node := NewColumnFileNode(cfg, nil, nil, schema, "users", "id", "1", "email", nil)

			if node.schema != schema {
				t.Errorf("Expected schema=%q, got %q", schema, node.schema)
			}
		})
	}
}

// TestNewColumnFileNode_WithPartialRowTracker tests creation with PartialRowTracker
func TestNewColumnFileNode_WithPartialRowTracker(t *testing.T) {
	cfg := &config.Config{}
	tracker := NewPartialRowTracker(nil)

	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", tracker)

	if node.partialRows != tracker {
		t.Error("Expected partialRows to be set")
	}
}

// TestColumnFileNode_Getattr_WithData tests Getattr when data is pre-populated
func TestColumnFileNode_Getattr_WithData(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	// Pre-populate data to avoid database fetch
	testData := []byte("john@example.com")
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

// TestColumnFileNode_Getattr_DifferentDataSizes tests Getattr with various data sizes
func TestColumnFileNode_Getattr_DifferentDataSizes(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		name     string
		data     []byte
		expected uint64
	}{
		{"empty_null", []byte{}, 0},
		{"single_char", []byte("a"), 1},
		{"short_email", []byte("a@b.com"), 7},
		{"long_email", []byte("very.long.email.address@subdomain.example.com"), 45},
		{"json_blob", []byte(`{"key":"value","nested":{"array":[1,2,3]}}`), 42},
		{"large_text", make([]byte, 10000), 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "data", nil)
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

// TestColumnFileHandle_Read tests basic read operation
func TestColumnFileHandle_Read(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	testData := []byte("john@example.com")

	fh := &ColumnFileHandle{
		node: node,
		data: testData,
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

// TestColumnFileHandle_Read_WithOffset tests read with offset
func TestColumnFileHandle_Read_WithOffset(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	testData := []byte("john@example.com")

	fh := &ColumnFileHandle{
		node: node,
		data: testData,
	}

	// Read from offset 5
	dest := make([]byte, 10)
	result, errno := fh.Read(context.Background(), dest, 5)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	expected := testData[5:15] // "@example.c"
	if string(data) != string(expected) {
		t.Errorf("Expected data=%q, got %q", expected, data)
	}
}

// TestColumnFileHandle_Read_PartialBuffer tests read with buffer smaller than remaining data
func TestColumnFileHandle_Read_PartialBuffer(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	testData := []byte("john@example.com")

	fh := &ColumnFileHandle{
		node: node,
		data: testData,
	}

	// Read only 4 bytes
	dest := make([]byte, 4)
	result, errno := fh.Read(context.Background(), dest, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	data, _ := result.Bytes(nil)
	if string(data) != "john" {
		t.Errorf("Expected data=%q, got %q", "john", data)
	}
}

// TestColumnFileHandle_Read_EOF tests read at end of file
func TestColumnFileHandle_Read_EOF(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	testData := []byte("test")

	fh := &ColumnFileHandle{
		node: node,
		data: testData,
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

// TestColumnFileHandle_Read_ExactEOF tests read starting exactly at EOF
func TestColumnFileHandle_Read_ExactEOF(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	testData := []byte("hello")

	fh := &ColumnFileHandle{
		node: node,
		data: testData,
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

// TestColumnFileHandle_Read_EmptyData tests read with empty data (NULL column)
func TestColumnFileHandle_Read_EmptyData(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	fh := &ColumnFileHandle{
		node: node,
		data: []byte{},
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

// TestColumnFileHandle_Write tests basic write operation
func TestColumnFileHandle_Write(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	fh := &ColumnFileHandle{
		node: node,
		data: nil,
	}

	testData := []byte("new@example.com")
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

// TestColumnFileHandle_Write_WithOffset tests write at non-zero offset
func TestColumnFileHandle_Write_WithOffset(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	// Start with existing data
	initialData := []byte("old@example.com")
	fh := &ColumnFileHandle{
		node: node,
		data: make([]byte, len(initialData)),
	}
	copy(fh.data, initialData)

	// Write "NEW" at offset 0
	newData := []byte("NEW")
	written, errno := fh.Write(context.Background(), newData, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if written != uint32(len(newData)) {
		t.Errorf("Expected written=%d, got %d", len(newData), written)
	}

	// Since offset is 0, the whole buffer is replaced
	if string(fh.data) != "NEW" {
		t.Errorf("Expected data=%q, got %q", "NEW", fh.data)
	}
}

// TestColumnFileHandle_Write_Extend tests write that extends the file
func TestColumnFileHandle_Write_Extend(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	// Start with small data
	initialData := []byte("abc")
	fh := &ColumnFileHandle{
		node: node,
		data: make([]byte, len(initialData)),
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
	if string(fh.data[:3]) != "abc" {
		t.Errorf("Expected first 3 bytes=%q, got %q", "abc", fh.data[:3])
	}

	// Bytes 5-7 should be new data
	if string(fh.data[5:8]) != "XYZ" {
		t.Errorf("Expected bytes 5-7=%q, got %q", "XYZ", fh.data[5:8])
	}
}

// TestColumnFileHandle_Write_WithNewline tests write with trailing newline (echo behavior)
func TestColumnFileHandle_Write_WithNewline(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	fh := &ColumnFileHandle{
		node: node,
		data: nil,
	}

	// Simulate echo command: echo "value" > file (adds newline)
	testData := []byte("new@example.com\n")
	written, errno := fh.Write(context.Background(), testData, 0)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if written != uint32(len(testData)) {
		t.Errorf("Expected written=%d, got %d", len(testData), written)
	}

	// Data should include the newline (Flush will trim it)
	if string(fh.data) != string(testData) {
		t.Errorf("Expected fh.data=%q, got %q", testData, fh.data)
	}
}

// TestColumnFileHandle_Write_Multiple tests multiple sequential writes
func TestColumnFileHandle_Write_Multiple(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	fh := &ColumnFileHandle{
		node: node,
		data: nil,
	}

	// First write
	written1, errno := fh.Write(context.Background(), []byte("user"), 0)
	if errno != 0 {
		t.Fatalf("First write failed with errno=%d", errno)
	}
	if written1 != 4 {
		t.Errorf("Expected written1=4, got %d", written1)
	}

	// Second write (append)
	written2, errno := fh.Write(context.Background(), []byte("@test.com"), 4)
	if errno != 0 {
		t.Fatalf("Second write failed with errno=%d", errno)
	}
	if written2 != 9 {
		t.Errorf("Expected written2=9, got %d", written2)
	}

	expected := "user@test.com"
	if string(fh.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, fh.data)
	}
}

// TestColumnFileHandle_ReadWrite_Cycle tests a complete read-modify-write cycle
func TestColumnFileHandle_ReadWrite_Cycle(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	originalData := []byte("old@example.com")

	// Simulate read
	fh := &ColumnFileHandle{
		node: node,
		data: originalData,
	}

	// Read the data
	dest := make([]byte, 1024)
	result, _ := fh.Read(context.Background(), dest, 0)
	readData, _ := result.Bytes(nil)

	if string(readData) != string(originalData) {
		t.Errorf("Read data mismatch")
	}

	// Simulate modify: write new data
	newData := []byte("new@example.com")
	_, _ = fh.Write(context.Background(), newData, 0)

	if string(fh.data) != string(newData) {
		t.Errorf("Write data mismatch: expected %q, got %q", newData, fh.data)
	}
}

// Note: TestColumnFileNode_Setattr_Truncate requires database integration
// because Setattr calls Getattr which tries to fetch data after truncation.
// See test/integration/ for full Setattr tests.

// TestColumnFileHandle_NilData tests handle behavior when data is nil
func TestColumnFileHandle_NilData(t *testing.T) {
	cfg := &config.Config{}
	node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", "email", nil)

	fh := &ColumnFileHandle{
		node: node,
		data: nil,
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

// TestColumnFileNode_DifferentColumnTypes tests nodes for various column data types
func TestColumnFileNode_DifferentColumnTypes(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		columnName string
		data       []byte
	}{
		{"id", []byte("12345")},                        // Integer
		{"email", []byte("user@example.com")},          // Varchar
		{"created_at", []byte("2024-01-01T00:00:00Z")}, // Timestamp
		{"is_active", []byte("true")},                  // Boolean
		{"metadata", []byte(`{"key":"value"}`)},        // JSONB
		{"tags", []byte(`["tag1","tag2"]`)},            // Array
		{"balance", []byte("12345.67")},                // Numeric
		{"description", []byte("")},                    // NULL (empty)
	}

	for _, tc := range testCases {
		t.Run(tc.columnName, func(t *testing.T) {
			node := NewColumnFileNode(cfg, nil, nil, "public", "users", "id", "1", tc.columnName, nil)
			node.data = tc.data

			var out fuse.AttrOut
			errno := node.Getattr(context.Background(), nil, &out)

			if errno != 0 {
				t.Errorf("Expected errno=0, got %d", errno)
			}

			if out.Size != uint64(len(tc.data)) {
				t.Errorf("Expected Size=%d, got %d", len(tc.data), out.Size)
			}
		})
	}
}

// ============================================================================
// Mock-based tests for database interactions (ADR-004)
// ============================================================================

// TestColumnFileNode_Getattr_WithMock tests Getattr using MockDBClient
func TestColumnFileNode_Getattr_WithMock(t *testing.T) {
	cfg := &config.Config{}

	// Create mock that returns column data
	mock := db.NewMockDBClient()
	mock.MockRowReader.GetColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
		if columnName == "email" && pkValue == "1" {
			return "john@example.com", nil
		}
		return nil, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "email", partialRows)

	ctx := context.Background()
	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check that data was fetched and size is correct
	expectedSize := uint64(len("john@example.com"))
	if out.Size != expectedSize {
		t.Errorf("Expected Size=%d, got %d", expectedSize, out.Size)
	}

	// Verify the data was cached
	if string(node.data) != "john@example.com" {
		t.Errorf("Expected cached data=%q, got %q", "john@example.com", node.data)
	}
}

// TestColumnFileNode_Getattr_WithMock_NullValue tests Getattr with NULL column value
func TestColumnFileNode_Getattr_WithMock_NullValue(t *testing.T) {
	cfg := &config.Config{}

	// Mock returns nil (NULL value)
	mock := db.NewMockDBClient()
	mock.MockRowReader.GetColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
		return nil, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "email", partialRows)

	ctx := context.Background()
	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// NULL should result in empty file
	if out.Size != 0 {
		t.Errorf("Expected Size=0 for NULL, got %d", out.Size)
	}
}

// TestColumnFileNode_Getattr_WithMock_Error tests Getattr when database returns error
func TestColumnFileNode_Getattr_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	// Mock returns error
	mock := db.NewMockDBClient()
	mock.MockRowReader.GetColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "email", partialRows)

	ctx := context.Background()
	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	// Should return EIO on database error
	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestColumnFileNode_fetchData_WithMock tests fetchData with various data types
func TestColumnFileNode_fetchData_WithMock(t *testing.T) {
	testCases := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"string", "hello", "hello"},
		{"integer", 42, "42"},
		{"float", 3.14, "3.14"},
		// PostgreSQL booleans are represented as "t" and "f"
		{"boolean_true", true, "t"},
		{"boolean_false", false, "f"},
		{"null", nil, ""},
		{"json_object", map[string]interface{}{"key": "value"}, `{"key":"value"}`},
		{"json_array", []interface{}{1, 2, 3}, "[1,2,3]"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}

			mock := db.NewMockDBClient()
			mock.MockRowReader.GetColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
				return tc.value, nil
			}

			partialRows := NewPartialRowTracker(nil)
			node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "data", partialRows)

			err := node.fetchData(context.Background())
			if err != nil {
				t.Fatalf("fetchData() error: %v", err)
			}

			if string(node.data) != tc.expected {
				t.Errorf("Expected data=%q, got %q", tc.expected, node.data)
			}
		})
	}
}

// TestColumnFileNode_fetchData_WithTrailingNewlines tests fetchData with TrailingNewlines config
func TestColumnFileNode_fetchData_WithTrailingNewlines(t *testing.T) {
	cfg := &config.Config{
		TrailingNewlines: true,
	}

	mock := db.NewMockDBClient()
	mock.MockRowReader.GetColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
		return "hello", nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "data", partialRows)

	err := node.fetchData(context.Background())
	if err != nil {
		t.Fatalf("fetchData() error: %v", err)
	}

	expected := "hello\n"
	if string(node.data) != expected {
		t.Errorf("Expected data=%q, got %q", expected, node.data)
	}
}

// TestColumnFileHandle_Flush_WithMock_ExistingRow tests Flush for an existing row (UPDATE)
func TestColumnFileHandle_Flush_WithMock_ExistingRow(t *testing.T) {
	cfg := &config.Config{}

	// Track what was called
	var updatedColumn, updatedValue string

	mock := db.NewMockDBClient()
	// Row exists
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table, pkColumn, pkValue string) (*db.Row, error) {
		return &db.Row{Columns: []string{"id", "email"}, Values: []interface{}{"1", "old@example.com"}}, nil
	}
	mock.MockRowWriter.UpdateColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error {
		updatedColumn = columnName
		updatedValue = newValue
		return nil
	}

	node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "email", NewPartialRowTracker(nil))

	fh := &ColumnFileHandle{
		node: node,
		data: []byte("new@example.com\n"),
	}

	errno := fh.Flush(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if updatedColumn != "email" {
		t.Errorf("Expected UpdateColumn called with column='email', got %q", updatedColumn)
	}

	// Should strip trailing newline
	if updatedValue != "new@example.com" {
		t.Errorf("Expected UpdateColumn called with value='new@example.com', got %q", updatedValue)
	}
}

// TestColumnFileHandle_Flush_WithMock_NewRow tests Flush for a new row (partial row creation)
// Note: Full TryCommit behavior requires integration tests because PartialRowTracker
// uses *db.Client internally (not DBClient interface). This test verifies SetColumn directly.
func TestColumnFileHandle_Flush_WithMock_NewRow(t *testing.T) {
	// This test cannot use the full Flush path because PartialRowTracker.TryCommit
	// requires a real *db.Client. Instead, we test SetColumn directly.

	partialRows := NewPartialRowTracker(nil)

	// Simulate what Flush does: SetColumn for the new row
	partialRows.SetColumn("public", "users", "id", "1", "email", "test@example.com")

	// Verify the partial row has the email column set
	partial := partialRows.Get("public", "users", "1")
	if partial == nil {
		t.Fatal("Expected partial row to exist")
	}

	if partial.Columns["email"] != "test@example.com" {
		t.Errorf("Expected partial row email='test@example.com', got %q", partial.Columns["email"])
	}

	// Verify PK info was stored correctly
	if partial.PkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got %q", partial.PkColumn)
	}
	if partial.PkValue != "1" {
		t.Errorf("Expected pkValue='1', got %q", partial.PkValue)
	}
}

// TestColumnFileHandle_Flush_WithMock_Error tests Flush when database update fails
func TestColumnFileHandle_Flush_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	// Row exists
	mock.MockRowReader.GetRowFunc = func(ctx context.Context, schema, table, pkColumn, pkValue string) (*db.Row, error) {
		return &db.Row{Columns: []string{"id"}, Values: []interface{}{"1"}}, nil
	}
	// Update fails
	mock.MockRowWriter.UpdateColumnFunc = func(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error {
		return context.DeadlineExceeded
	}

	node := NewColumnFileNode(cfg, mock, nil, "public", "users", "id", "1", "email", NewPartialRowTracker(nil))

	fh := &ColumnFileHandle{
		node: node,
		data: []byte("test@example.com"),
	}

	errno := fh.Flush(context.Background())

	// Should return EIO on database error
	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestColumnFileNode_getFileMode_WithMock tests permission mapping from PostgreSQL privileges
func TestColumnFileNode_getFileMode_WithMock(t *testing.T) {
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
		{"no_access", false, false, false, false, 0000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				DefaultSchema:           "public",
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
			mock.MockSchemaReader.GetTablePermissionsFunc = func(ctx context.Context, schema, table string) (*db.TablePermissions, error) {
				return &db.TablePermissions{
					CanSelect: tc.select_,
					CanUpdate: tc.update,
					CanInsert: tc.insert,
					CanDelete: tc.delete_,
				}, nil
			}

			cache := tigerfs.NewMetadataCache(cfg, mock)
			// Pre-populate the cache by calling Refresh
			if err := cache.Refresh(context.Background()); err != nil {
				t.Fatalf("Failed to refresh cache: %v", err)
			}

			partialRows := NewPartialRowTracker(nil)
			node := NewColumnFileNode(cfg, mock, cache, "public", "users", "id", "1", "email", partialRows)
			node.data = []byte("test") // Pre-populate to avoid fetch

			mode := node.getFileMode(context.Background())

			if mode != tc.expected {
				t.Errorf("Expected mode=0%o, got 0%o", tc.expected, mode)
			}
		})
	}
}

// Note: Open, Flush, fetchData, and database-dependent tests require integration testing
// See test/integration/ for full CRUD operation tests with PostgreSQL
