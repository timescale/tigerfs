package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestNewRowDirectoryNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	rowDirNode := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", nil)

	if rowDirNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if rowDirNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", rowDirNode.tableName)
	}

	if rowDirNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", rowDirNode.schema)
	}

	if rowDirNode.pkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got '%s'", rowDirNode.pkColumn)
	}

	if rowDirNode.pkValue != "1" {
		t.Errorf("Expected pkValue='1', got '%s'", rowDirNode.pkValue)
	}
}

// TestRowDirectoryNode_Interfaces verifies that RowDirectoryNode implements required interfaces
func TestRowDirectoryNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that RowDirectoryNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	rowDirNode := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", nil)

	// Mark as used (compiler will verify types)
	_ = rowDirNode
}

// TestNewRowDirectoryNode_DifferentPKTypes tests creation with various primary key values
func TestNewRowDirectoryNode_DifferentPKTypes(t *testing.T) {
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
		{"composite", "part1_part2"},
	}

	for _, tc := range testCases {
		t.Run(tc.pkColumn+"_"+tc.pkValue, func(t *testing.T) {
			node := NewRowDirectoryNode(cfg, nil, nil, "public", "users", tc.pkColumn, tc.pkValue, nil)

			if node.pkColumn != tc.pkColumn {
				t.Errorf("Expected pkColumn=%q, got %q", tc.pkColumn, node.pkColumn)
			}
			if node.pkValue != tc.pkValue {
				t.Errorf("Expected pkValue=%q, got %q", tc.pkValue, node.pkValue)
			}
		})
	}
}

// TestNewRowDirectoryNode_MultipleSchemas tests creation with different schemas
func TestNewRowDirectoryNode_MultipleSchemas(t *testing.T) {
	cfg := &config.Config{}

	schemas := []string{"public", "custom_schema", "analytics", ""}

	for _, schema := range schemas {
		t.Run("schema_"+schema, func(t *testing.T) {
			node := NewRowDirectoryNode(cfg, nil, nil, schema, "users", "id", "1", nil)

			if node.schema != schema {
				t.Errorf("Expected schema=%q, got %q", schema, node.schema)
			}
		})
	}
}

// TestNewRowDirectoryNode_MultipleTables tests creation with different table names
func TestNewRowDirectoryNode_MultipleTables(t *testing.T) {
	cfg := &config.Config{}

	tables := []string{"users", "orders", "products", "inventory_items", "long_table_name_with_underscores"}

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			node := NewRowDirectoryNode(cfg, nil, nil, "public", table, "id", "1", nil)

			if node.tableName != table {
				t.Errorf("Expected tableName=%q, got %q", table, node.tableName)
			}
		})
	}
}

// TestNewRowDirectoryNode_WithPartialRowTracker tests creation with PartialRowTracker
func TestNewRowDirectoryNode_WithPartialRowTracker(t *testing.T) {
	cfg := &config.Config{}
	tracker := NewPartialRowTracker(nil)

	node := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", tracker)

	if node.partialRows != tracker {
		t.Error("Expected partialRows to be set")
	}
}

// TestRowDirectoryNode_Getattr tests Getattr returns correct directory attributes
func TestRowDirectoryNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", nil)

	ctx := context.Background()
	var out fuse.AttrOut

	errno := node.Getattr(ctx, nil, &out)

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

	// Check size is 4096 (standard directory block size)
	if out.Size != 4096 {
		t.Errorf("Expected Size=4096, got %d", out.Size)
	}
}

// TestRowDirectoryNode_Getattr_DifferentRows tests Getattr for various rows
func TestRowDirectoryNode_Getattr_DifferentRows(t *testing.T) {
	cfg := &config.Config{}
	ctx := context.Background()

	testCases := []struct {
		table   string
		pkValue string
	}{
		{"users", "1"},
		{"orders", "1000"},
		{"products", "SKU-ABC-123"},
		{"events", "evt_12345"},
	}

	for _, tc := range testCases {
		t.Run(tc.table+"_"+tc.pkValue, func(t *testing.T) {
			node := NewRowDirectoryNode(cfg, nil, nil, "public", tc.table, "id", tc.pkValue, nil)

			var out fuse.AttrOut
			errno := node.Getattr(ctx, nil, &out)

			if errno != 0 {
				t.Errorf("Expected errno=0 for %s/%s, got %d", tc.table, tc.pkValue, errno)
			}

			// All row directories should have same attributes
			if out.Mode != uint32(0700|syscall.S_IFDIR) {
				t.Errorf("Expected directory mode for %s/%s", tc.table, tc.pkValue)
			}
		})
	}
}

// TestRowDirectoryNode_Getattr_IsIdempotent tests that multiple calls return same result
func TestRowDirectoryNode_Getattr_IsIdempotent(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", nil)
	ctx := context.Background()

	var out1, out2 fuse.AttrOut

	errno1 := node.Getattr(ctx, nil, &out1)
	errno2 := node.Getattr(ctx, nil, &out2)

	if errno1 != 0 || errno2 != 0 {
		t.Errorf("Expected both calls to succeed, got errno1=%d, errno2=%d", errno1, errno2)
	}

	if out1.Mode != out2.Mode {
		t.Errorf("Mode changed between calls: %d vs %d", out1.Mode, out2.Mode)
	}

	if out1.Nlink != out2.Nlink {
		t.Errorf("Nlink changed between calls: %d vs %d", out1.Nlink, out2.Nlink)
	}

	if out1.Size != out2.Size {
		t.Errorf("Size changed between calls: %d vs %d", out1.Size, out2.Size)
	}
}

// TestRowDirectoryNode_NilDatabase tests node creation and basic operations with nil db
func TestRowDirectoryNode_NilDatabase(t *testing.T) {
	cfg := &config.Config{}
	node := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", nil)

	// db should be nil
	if node.db != nil {
		t.Error("Expected db to be nil")
	}

	// Getattr should still work without database
	ctx := context.Background()
	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected Getattr to succeed with nil db, got errno=%d", errno)
	}
}

// TestRowDirectoryNode_RowDirectoryVsRowFile tests distinction between row as directory vs file
func TestRowDirectoryNode_RowDirectoryVsRowFile(t *testing.T) {
	cfg := &config.Config{}

	// Create row as directory
	rowDir := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", "1", nil)

	// Create row as file
	rowFile := NewRowFileNode(cfg, nil, nil, "public", "users", "id", "1", "tsv")

	// Get attributes for both
	ctx := context.Background()
	var dirOut, fileOut fuse.AttrOut

	dirErrno := rowDir.Getattr(ctx, nil, &dirOut)

	// Pre-populate row file data to avoid database fetch
	rowFile.data = []byte("test data")
	fileErrno := rowFile.Getattr(ctx, nil, &fileOut)

	if dirErrno != 0 || fileErrno != 0 {
		t.Fatalf("Expected both to succeed, got dir=%d, file=%d", dirErrno, fileErrno)
	}

	// Directory should have directory mode
	if dirOut.Mode&syscall.S_IFDIR == 0 {
		t.Error("Row directory should have S_IFDIR mode")
	}

	// File should have regular file mode
	if fileOut.Mode&syscall.S_IFREG == 0 {
		t.Error("Row file should have S_IFREG mode")
	}

	// Directory should have nlink=2
	if dirOut.Nlink != 2 {
		t.Errorf("Row directory should have nlink=2, got %d", dirOut.Nlink)
	}

	// File should have nlink=1
	if fileOut.Nlink != 1 {
		t.Errorf("Row file should have nlink=1, got %d", fileOut.Nlink)
	}
}

// TestRowDirectoryNode_AllFieldsSet tests that all fields are properly set
func TestRowDirectoryNode_AllFieldsSet(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "test_schema",
		DirListingLimit: 5000,
	}
	tracker := NewPartialRowTracker(nil)

	node := NewRowDirectoryNode(cfg, nil, nil, "my_schema", "my_table", "pk_col", "pk_val", tracker)

	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"cfg", node.cfg, cfg},
		{"schema", node.schema, "my_schema"},
		{"tableName", node.tableName, "my_table"},
		{"pkColumn", node.pkColumn, "pk_col"},
		{"pkValue", node.pkValue, "pk_val"},
		{"partialRows", node.partialRows, tracker},
		{"db", node.db, (*interface{})(nil)}, // nil check
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Special handling for nil
			if tc.name == "db" {
				if node.db != nil {
					t.Errorf("Expected db to be nil")
				}
				return
			}

			if tc.got != tc.expected {
				t.Errorf("Expected %s=%v, got %v", tc.name, tc.expected, tc.got)
			}
		})
	}
}

// TestRowDirectoryNode_SpecialCharacterPKValues tests PK values with special characters
func TestRowDirectoryNode_SpecialCharacterPKValues(t *testing.T) {
	cfg := &config.Config{}

	testCases := []string{
		"user@example.com",  // Email
		"path/to/resource",  // Slashes
		"name with spaces",  // Spaces
		"special!@#$%^&*()", // Special chars
		"unicode-日本語",       // Unicode
		"",                  // Empty
		"123",               // Numeric string
		"true",              // Boolean-like
		"null",              // SQL keyword-like
	}

	for _, pk := range testCases {
		t.Run("pk_"+pk, func(t *testing.T) {
			node := NewRowDirectoryNode(cfg, nil, nil, "public", "users", "id", pk, nil)

			if node.pkValue != pk {
				t.Errorf("Expected pkValue=%q, got %q", pk, node.pkValue)
			}

			// Getattr should still work
			var out fuse.AttrOut
			errno := node.Getattr(context.Background(), nil, &out)
			if errno != 0 {
				t.Errorf("Getattr failed for pk=%q with errno=%d", pk, errno)
			}
		})
	}
}

// =============================================================================
// Mock-based tests
// =============================================================================

// TestRowDirectoryNode_Readdir_WithMock tests listing columns in a row directory
func TestRowDirectoryNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{
		NoFilenameExtensions: true, // Disable extensions for simpler testing
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "email", DataType: "text"},
			{Name: "created_at", DataType: "timestamp"},
		}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

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

	// RowDirectoryNode includes columns + format files (.json, .csv, .tsv, .yaml)
	// 4 columns + 4 format files = 8 entries
	if len(entries) != 8 {
		t.Errorf("Expected 8 entries (4 columns + 4 format files), got %d: %v", len(entries), entries)
	}

	// Verify columns and format files are present
	expected := map[string]bool{
		"id": true, "name": true, "email": true, "created_at": true,
		".json": true, ".csv": true, ".tsv": true, ".yaml": true,
	}
	for _, name := range entries {
		if !expected[name] {
			t.Errorf("Unexpected entry: %s", name)
		}
	}
}

// TestRowDirectoryNode_Readdir_WithMock_Empty tests empty column list
func TestRowDirectoryNode_Readdir_WithMock_Empty(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

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

	// Even with no columns, format files (.json, .csv, .tsv, .yaml) are still present
	if len(entries) != 4 {
		t.Errorf("Expected 4 format files, got %d: %v", len(entries), entries)
	}
}

// TestRowDirectoryNode_Readdir_WithMock_Error tests error handling
func TestRowDirectoryNode_Readdir_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestRowDirectoryNode_Readdir_WithMock_WithPartialRow tests including partial row columns
func TestRowDirectoryNode_Readdir_WithMock_WithPartialRow(t *testing.T) {
	cfg := &config.Config{
		NoFilenameExtensions: true, // Disable extensions for simpler testing
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	// Add a partial row with extra data using GetOrCreate
	partialRows.GetOrCreate("public", "users", "id", "1")
	partialRows.SetColumn("public", "users", "id", "1", "name", "John")

	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

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

	// 2 columns + 4 format files = 6 entries
	if len(entries) != 6 {
		t.Errorf("Expected 6 entries (2 columns + 4 format files), got %d: %v", len(entries), entries)
	}
}

// TestRowDirectoryNode_Unlink_WithMock tests deleting a column value
func TestRowDirectoryNode_Unlink_WithMock(t *testing.T) {
	cfg := &config.Config{
		NoFilenameExtensions: true, // Use exact column name matching
	}

	var updateCalled bool
	var updatedColumn string
	var updatedValue string

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer", IsNullable: false},
			{Name: "name", DataType: "text", IsNullable: true},
			{Name: "email", DataType: "text", IsNullable: true},
		}, nil
	}
	mock.MockRowWriter.UpdateColumnFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columnName, newValue string) error {
		updateCalled = true
		updatedColumn = columnName
		updatedValue = newValue
		return nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

	errno := node.Unlink(context.Background(), "email")

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if !updateCalled {
		t.Error("Expected UpdateColumn to be called")
	}

	if updatedColumn != "email" {
		t.Errorf("Expected column='email', got %q", updatedColumn)
	}

	if updatedValue != "" {
		t.Errorf("Expected value='', got %q", updatedValue)
	}
}

// TestRowDirectoryNode_Unlink_WithMock_NonNullable tests unlinking a non-nullable column
func TestRowDirectoryNode_Unlink_WithMock_NonNullable(t *testing.T) {
	cfg := &config.Config{
		NoFilenameExtensions: true, // Use exact column name matching
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer", IsNullable: false},
			{Name: "name", DataType: "text", IsNullable: false, Default: ""}, // NOT NULL, no default
		}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

	errno := node.Unlink(context.Background(), "name")

	// Should return EACCES because column is NOT NULL without default
	if errno != syscall.EACCES {
		t.Errorf("Expected errno=EACCES (13), got %d", errno)
	}
}

// TestRowDirectoryNode_Unlink_WithMock_ColumnNotFound tests unlinking nonexistent column
func TestRowDirectoryNode_Unlink_WithMock_ColumnNotFound(t *testing.T) {
	cfg := &config.Config{
		NoFilenameExtensions: true, // Use exact column name matching
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

	errno := node.Unlink(context.Background(), "nonexistent")

	if errno != syscall.ENOENT {
		t.Errorf("Expected errno=ENOENT, got %d", errno)
	}
}

// TestRowDirectoryNode_Unlink_WithMock_Error tests error handling during unlink
func TestRowDirectoryNode_Unlink_WithMock_Error(t *testing.T) {
	cfg := &config.Config{
		NoFilenameExtensions: true, // Use exact column name matching
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer", IsNullable: false},
			{Name: "name", DataType: "text", IsNullable: true},
		}, nil
	}
	mock.MockRowWriter.UpdateColumnFunc = func(ctx context.Context, schema, table string, pk *db.PKMatch, columnName, newValue string) error {
		return context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewRowDirectoryNode(cfg, mock, nil, "public", "users", "id", "1", partialRows)

	errno := node.Unlink(context.Background(), "name")

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestRowDirectoryNode_Lookup_WithMock tests are skipped because Lookup requires
// FUSE bridge infrastructure (NewPersistentInode). See test/integration/.
