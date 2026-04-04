package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestNewIndexNode verifies IndexNode creation.
func TestNewIndexNode(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{
		Name:      "idx_email",
		Columns:   []string{"email"},
		IsUnique:  true,
		IsPrimary: false,
	}

	node := NewIndexNode(cfg, nil, nil, "public", "users", "email", idx, nil)

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName 'users', got '%s'", node.tableName)
	}
	if node.column != "email" {
		t.Errorf("Expected column 'email', got '%s'", node.column)
	}
	if node.index != idx {
		t.Error("Expected index to be set")
	}
}

// TestNewIndexValueNode verifies IndexValueNode creation.
func TestNewIndexValueNode(t *testing.T) {
	cfg := &config.Config{}
	matchingPKs := []string{"1", "5", "10"}

	node := NewIndexValueNode(cfg, nil, nil, "public", "users", "email", "foo@example.com", "id", matchingPKs, nil)

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName 'users', got '%s'", node.tableName)
	}
	if node.column != "email" {
		t.Errorf("Expected column 'email', got '%s'", node.column)
	}
	if node.value != "foo@example.com" {
		t.Errorf("Expected value 'foo@example.com', got '%s'", node.value)
	}
	if node.pkColumn != "id" {
		t.Errorf("Expected pkColumn 'id', got '%s'", node.pkColumn)
	}
	if len(node.matchingPKs) != 3 {
		t.Errorf("Expected 3 matching PKs, got %d", len(node.matchingPKs))
	}
}

// TestIndexNode_Interfaces verifies IndexNode implements required interfaces.
func TestIndexNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{Name: "test_idx", Columns: []string{"col"}}
	node := NewIndexNode(cfg, nil, nil, "public", "test", "col", idx, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestIndexValueNode_Interfaces verifies IndexValueNode implements required interfaces.
func TestIndexValueNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	node := NewIndexValueNode(cfg, nil, nil, "public", "test", "col", "val", "id", []string{"1"}, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestNewCompositeIndexNode verifies CompositeIndexNode creation.
func TestNewCompositeIndexNode(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"last_name", "first_name"}
	idx := &db.Index{
		Name:      "idx_name",
		Columns:   columns,
		IsUnique:  false,
		IsPrimary: false,
	}

	node := NewCompositeIndexNode(cfg, nil, nil, "public", "users", columns, idx, nil)

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName 'users', got '%s'", node.tableName)
	}
	if len(node.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(node.columns))
	}
	if node.columns[0] != "last_name" {
		t.Errorf("Expected first column 'last_name', got '%s'", node.columns[0])
	}
	if node.columns[1] != "first_name" {
		t.Errorf("Expected second column 'first_name', got '%s'", node.columns[1])
	}
	if node.index != idx {
		t.Error("Expected index to be set")
	}
}

// TestNewCompositeIndexLevelNode verifies CompositeIndexLevelNode creation.
func TestNewCompositeIndexLevelNode(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"last_name", "first_name"}
	values := []string{"Smith"}
	idx := &db.Index{Name: "idx_name", Columns: columns}

	node := NewCompositeIndexLevelNode(cfg, nil, nil, "public", "users", columns, values, idx, nil)

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName 'users', got '%s'", node.tableName)
	}
	if len(node.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(node.columns))
	}
	if len(node.values) != 1 {
		t.Errorf("Expected 1 value, got %d", len(node.values))
	}
	if node.values[0] != "Smith" {
		t.Errorf("Expected value 'Smith', got '%s'", node.values[0])
	}
}

// TestCompositeIndexNode_Interfaces verifies CompositeIndexNode implements required interfaces.
func TestCompositeIndexNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"col1", "col2"}
	idx := &db.Index{Name: "test_idx", Columns: columns}
	node := NewCompositeIndexNode(cfg, nil, nil, "public", "test", columns, idx, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestCompositeIndexLevelNode_Interfaces verifies CompositeIndexLevelNode implements required interfaces.
func TestCompositeIndexLevelNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"col1", "col2"}
	values := []string{"val1"}
	idx := &db.Index{Name: "test_idx", Columns: columns}
	node := NewCompositeIndexLevelNode(cfg, nil, nil, "public", "test", columns, values, idx, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestFormatCompositeIndexName verifies directory name generation for composite indexes.
func TestFormatCompositeIndexName(t *testing.T) {
	tests := []struct {
		columns  []string
		expected string
	}{
		{[]string{"last_name", "first_name"}, ".last_name.first_name"},
		{[]string{"a", "b", "c"}, ".a.b.c"},
		{[]string{"single"}, ".single"},
	}

	for _, tc := range tests {
		result := FormatCompositeIndexName(tc.columns)
		if result != tc.expected {
			t.Errorf("FormatCompositeIndexName(%v) = %q, want %q", tc.columns, result, tc.expected)
		}
	}
}

// TestParseCompositeIndexName verifies parsing of composite index directory names.
func TestParseCompositeIndexName(t *testing.T) {
	tests := []struct {
		name     string
		expected []string
	}{
		{".last_name.first_name", []string{"last_name", "first_name"}},
		{".a.b.c", []string{"a", "b", "c"}},
		{".email", nil}, // Single column, not composite
		{"email", nil},  // No leading dot
		{".", nil},      // Just a dot
		{"", nil},       // Empty
	}

	for _, tc := range tests {
		result := ParseCompositeIndexName(tc.name)
		if tc.expected == nil {
			if result != nil {
				t.Errorf("ParseCompositeIndexName(%q) = %v, want nil", tc.name, result)
			}
		} else {
			if len(result) != len(tc.expected) {
				t.Errorf("ParseCompositeIndexName(%q) = %v, want %v", tc.name, result, tc.expected)
				continue
			}
			for i := range result {
				if result[i] != tc.expected[i] {
					t.Errorf("ParseCompositeIndexName(%q)[%d] = %q, want %q", tc.name, i, result[i], tc.expected[i])
				}
			}
		}
	}
}

// TestCompositeIndexLevelNode_NavigationDepth verifies correct depth tracking.
func TestCompositeIndexLevelNode_NavigationDepth(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"country", "state", "city"}
	idx := &db.Index{Name: "idx_location", Columns: columns}

	// Level 1: One value specified
	level1 := NewCompositeIndexLevelNode(cfg, nil, nil, "public", "locations", columns, []string{"USA"}, idx, nil)
	if len(level1.values) != 1 {
		t.Errorf("Level 1 should have 1 value, got %d", len(level1.values))
	}

	// Level 2: Two values specified
	level2 := NewCompositeIndexLevelNode(cfg, nil, nil, "public", "locations", columns, []string{"USA", "California"}, idx, nil)
	if len(level2.values) != 2 {
		t.Errorf("Level 2 should have 2 values, got %d", len(level2.values))
	}

	// Level 3: All values specified (final level)
	level3 := NewCompositeIndexLevelNode(cfg, nil, nil, "public", "locations", columns, []string{"USA", "California", "Los Angeles"}, idx, nil)
	if len(level3.values) != 3 {
		t.Errorf("Level 3 should have 3 values, got %d", len(level3.values))
	}
}

// =============================================================================
// Mock-based tests
// =============================================================================

// TestIndexNode_Readdir_WithMock tests listing distinct values for an indexed column
func TestIndexNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{Name: "idx_status", Columns: []string{"status"}, IsUnique: false}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetDistinctValuesFunc = func(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
		return []string{"active", "pending", "inactive", "deleted"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewIndexNode(cfg, mock, nil, "public", "users", "status", idx, partialRows)

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

	// IndexNode includes distinct values plus .first and .last pagination directories
	// So we expect 4 values + 2 pagination = 6 entries
	if len(entries) != 6 {
		t.Errorf("Expected 6 entries (4 values + .first + .last), got %d: %v", len(entries), entries)
	}

	// Verify the distinct values are present
	valueSet := make(map[string]bool)
	for _, e := range entries {
		valueSet[e] = true
	}
	for _, expected := range []string{"active", "pending", "inactive", "deleted", DirFirst, DirLast} {
		if !valueSet[expected] {
			t.Errorf("Expected entry %q not found", expected)
		}
	}
}

// TestIndexNode_Readdir_WithMock_Empty tests empty distinct values
func TestIndexNode_Readdir_WithMock_Empty(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{Name: "idx_status", Columns: []string{"status"}}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetDistinctValuesFunc = func(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
		return []string{}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewIndexNode(cfg, mock, nil, "public", "users", "status", idx, partialRows)

	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Even with no distinct values, IndexNode includes .first and .last
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have only pagination directories
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries (.first, .last), got %d: %v", len(entries), entries)
	}
}

// TestIndexNode_Readdir_WithMock_Error tests error handling
func TestIndexNode_Readdir_WithMock_Error(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{Name: "idx_status", Columns: []string{"status"}}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetDistinctValuesFunc = func(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
		return nil, context.DeadlineExceeded
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewIndexNode(cfg, mock, nil, "public", "users", "status", idx, partialRows)

	_, errno := node.Readdir(context.Background())

	if errno != syscall.EIO {
		t.Errorf("Expected errno=EIO, got %d", errno)
	}
}

// TestIndexValueNode_Readdir_WithMock tests listing rows matching an index value
func TestIndexValueNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{}

	mock := db.NewMockDBClient()
	// Pre-populated PKs from Lookup
	matchingPKs := []string{"5", "12", "27", "103"}

	partialRows := NewPartialRowTracker(nil)
	node := NewIndexValueNode(cfg, mock, nil, "public", "users", "status", "active", "id", matchingPKs, partialRows)

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

	// IndexValueNode includes rows plus .first and .last pagination
	// So we expect 4 PKs + 2 pagination = 6 entries
	if len(entries) != 6 {
		t.Errorf("Expected 6 entries (4 rows + .first + .last), got %d: %v", len(entries), entries)
	}

	// Verify PKs and pagination are present
	expected := map[string]bool{"5": true, "12": true, "27": true, "103": true, DirFirst: true, DirLast: true}
	for _, name := range entries {
		if !expected[name] {
			t.Errorf("Unexpected entry: %s", name)
		}
	}
}

// TestCompositeIndexNode_Readdir_WithMock tests listing first-level values
func TestCompositeIndexNode_Readdir_WithMock(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"country", "state"}
	idx := &db.Index{Name: "idx_location", Columns: columns}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetDistinctValuesFunc = func(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
		if column == "country" {
			return []string{"USA", "Canada", "Mexico"}, nil
		}
		return nil, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewCompositeIndexNode(cfg, mock, nil, "public", "locations", columns, idx, partialRows)

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

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}
}

// TestCompositeIndexLevelNode_Readdir_WithMock_Intermediate tests listing next-level values
func TestCompositeIndexLevelNode_Readdir_WithMock_Intermediate(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"country", "state", "city"}
	values := []string{"USA"}
	idx := &db.Index{Name: "idx_location", Columns: columns}

	mock := db.NewMockDBClient()
	mock.MockIndexReader.GetDistinctValuesFilteredFunc = func(ctx context.Context, schema, table, column string, filterColumns, filterValues []string, limit int) ([]string, error) {
		if column == "state" && len(filterValues) > 0 && filterValues[0] == "USA" {
			return []string{"California", "Texas", "New York"}, nil
		}
		return nil, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewCompositeIndexLevelNode(cfg, mock, nil, "public", "locations", columns, values, idx, partialRows)

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

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}
}

// TestCompositeIndexLevelNode_Readdir_WithMock_Final tests listing rows at final level
func TestCompositeIndexLevelNode_Readdir_WithMock_Final(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"country", "state"}
	values := []string{"USA", "California"} // All columns specified = final level
	idx := &db.Index{Name: "idx_location", Columns: columns}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}
	mock.MockIndexReader.GetRowsByCompositeIndexFunc = func(ctx context.Context, schema, table string, columns, values []string, pkColumns []string, limit int) ([]string, error) {
		return []string{"1", "5", "10"}, nil
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewCompositeIndexLevelNode(cfg, mock, nil, "public", "locations", columns, values, idx, partialRows)

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

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries (rows), got %d", len(entries))
	}
}

// TestIndexNode_Lookup_WithMock tests are skipped because Lookup requires
// FUSE bridge infrastructure (NewPersistentInode). See test/integration/.
