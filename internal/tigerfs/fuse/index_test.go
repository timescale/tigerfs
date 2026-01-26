package fuse

import (
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

	node := NewIndexNode(cfg, nil, "public", "users", "email", idx, nil)

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

	node := NewIndexValueNode(cfg, nil, "public", "users", "email", "foo@example.com", "id", matchingPKs, nil)

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
	node := NewIndexNode(cfg, nil, "public", "test", "col", idx, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestIndexValueNode_Interfaces verifies IndexValueNode implements required interfaces.
func TestIndexValueNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	node := NewIndexValueNode(cfg, nil, "public", "test", "col", "val", "id", []string{"1"}, nil)

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

	node := NewCompositeIndexNode(cfg, nil, "public", "users", columns, idx, nil)

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

	node := NewCompositeIndexLevelNode(cfg, nil, "public", "users", columns, values, idx, nil)

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
	node := NewCompositeIndexNode(cfg, nil, "public", "test", columns, idx, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestCompositeIndexLevelNode_Interfaces verifies CompositeIndexLevelNode implements required interfaces.
func TestCompositeIndexLevelNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	columns := []string{"col1", "col2"}
	values := []string{"val1"}
	idx := &db.Index{Name: "test_idx", Columns: columns}
	node := NewCompositeIndexLevelNode(cfg, nil, "public", "test", columns, values, idx, nil)

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
		{".email", nil},  // Single column, not composite
		{"email", nil},   // No leading dot
		{".", nil},       // Just a dot
		{"", nil},        // Empty
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
	level1 := NewCompositeIndexLevelNode(cfg, nil, "public", "locations", columns, []string{"USA"}, idx, nil)
	if len(level1.values) != 1 {
		t.Errorf("Level 1 should have 1 value, got %d", len(level1.values))
	}

	// Level 2: Two values specified
	level2 := NewCompositeIndexLevelNode(cfg, nil, "public", "locations", columns, []string{"USA", "California"}, idx, nil)
	if len(level2.values) != 2 {
		t.Errorf("Level 2 should have 2 values, got %d", len(level2.values))
	}

	// Level 3: All values specified (final level)
	level3 := NewCompositeIndexLevelNode(cfg, nil, "public", "locations", columns, []string{"USA", "California", "Los Angeles"}, idx, nil)
	if len(level3.values) != 3 {
		t.Errorf("Level 3 should have 3 values, got %d", len(level3.values))
	}
}
