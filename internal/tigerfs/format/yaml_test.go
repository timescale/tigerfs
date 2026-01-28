package format

import (
	"strings"
	"testing"
	"time"
)

func TestRowToYAML_Basic(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	result, err := RowToYAML(columns, values)
	if err != nil {
		t.Fatalf("RowToYAML failed: %v", err)
	}

	yaml := string(result)

	// Check document separator is present
	if !strings.HasPrefix(yaml, "---\n") {
		t.Error("YAML should start with document separator '---\\n'")
	}

	// Check that all columns and values are present
	if !strings.Contains(yaml, "id:") {
		t.Error("YAML should contain 'id:' key")
	}
	if !strings.Contains(yaml, "name:") {
		t.Error("YAML should contain 'name:' key")
	}
	if !strings.Contains(yaml, "email:") {
		t.Error("YAML should contain 'email:' key")
	}
	if !strings.Contains(yaml, "Alice") {
		t.Error("YAML should contain 'Alice' value")
	}
	if !strings.Contains(yaml, "alice@example.com") {
		t.Error("YAML should contain 'alice@example.com' value")
	}
}

func TestRowToYAML_WithNULL(t *testing.T) {
	columns := []string{"id", "name", "deleted_at"}
	values := []interface{}{1, "Alice", nil}

	result, err := RowToYAML(columns, values)
	if err != nil {
		t.Fatalf("RowToYAML failed: %v", err)
	}

	yaml := string(result)

	// NULL should be represented as "null"
	if !strings.Contains(yaml, "deleted_at: null") {
		t.Errorf("YAML should contain 'deleted_at: null', got:\n%s", yaml)
	}
}

func TestRowToYAML_AllNULL(t *testing.T) {
	columns := []string{"a", "b", "c"}
	values := []interface{}{nil, nil, nil}

	result, err := RowToYAML(columns, values)
	if err != nil {
		t.Fatalf("RowToYAML failed: %v", err)
	}

	yaml := string(result)

	if !strings.Contains(yaml, "a: null") {
		t.Errorf("Expected 'a: null' in YAML, got:\n%s", yaml)
	}
	if !strings.Contains(yaml, "b: null") {
		t.Errorf("Expected 'b: null' in YAML, got:\n%s", yaml)
	}
	if !strings.Contains(yaml, "c: null") {
		t.Errorf("Expected 'c: null' in YAML, got:\n%s", yaml)
	}
}

func TestRowToYAML_ColumnMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	values := []interface{}{1, "Alice", "extra"}

	_, err := RowToYAML(columns, values)
	if err == nil {
		t.Error("Expected error for column/value count mismatch")
	}
}

func TestRowToYAML_EmptyRow(t *testing.T) {
	columns := []string{}
	values := []interface{}{}

	result, err := RowToYAML(columns, values)
	if err != nil {
		t.Fatalf("RowToYAML failed: %v", err)
	}

	// Empty row should produce valid YAML (document separator + empty mapping)
	yaml := string(result)
	if yaml != "---\n{}\n" {
		t.Errorf("Expected '---\\n{}\\n', got: %q", yaml)
	}
}

func TestRowToYAML_MixedTypes(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	columns := []string{"id", "name", "active", "score", "created_at", "data"}
	values := []interface{}{
		int64(42),
		"Bob",
		true,
		3.14,
		now,
		[]byte(`{"key": "value"}`),
	}

	result, err := RowToYAML(columns, values)
	if err != nil {
		t.Fatalf("RowToYAML failed: %v", err)
	}

	yaml := string(result)

	// Check various types are present
	if !strings.Contains(yaml, "id:") {
		t.Error("YAML should contain 'id:' key")
	}
	if !strings.Contains(yaml, "42") {
		t.Error("YAML should contain '42' value")
	}
	if !strings.Contains(yaml, "Bob") {
		t.Error("YAML should contain 'Bob' value")
	}
	if !strings.Contains(yaml, "3.14") {
		t.Error("YAML should contain '3.14' value")
	}
}

func TestParseYAML_Basic(t *testing.T) {
	yaml := `id: 1
name: Alice
email: alice@example.com`

	columns, values, err := ParseYAML(yaml)
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}

	if len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(columns))
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	// Check that all columns are parsed
	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	if colMap["id"] != 1 {
		t.Errorf("Expected id=1, got %v", colMap["id"])
	}
	if colMap["name"] != "Alice" {
		t.Errorf("Expected name='Alice', got %v", colMap["name"])
	}
	if colMap["email"] != "alice@example.com" {
		t.Errorf("Expected email='alice@example.com', got %v", colMap["email"])
	}
}

func TestParseYAML_WithNull(t *testing.T) {
	yaml := `id: 1
name: Alice
deleted_at: null`

	columns, values, err := ParseYAML(yaml)
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}

	// Find deleted_at value
	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	if colMap["deleted_at"] != nil {
		t.Errorf("Expected deleted_at=nil, got %v", colMap["deleted_at"])
	}
}

func TestParseYAML_Invalid(t *testing.T) {
	yaml := `this is not valid yaml: [[[`

	_, _, err := ParseYAML(yaml)
	if err == nil {
		t.Error("Expected error for invalid YAML")
	}
}

func TestParseYAML_WithDocumentSeparator(t *testing.T) {
	// ParseYAML should handle YAML with leading document separator
	yaml := `---
id: 1
name: Alice`

	columns, values, err := ParseYAML(yaml)
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}

	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	// Check values are correct
	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	if colMap["id"] != 1 {
		t.Errorf("Expected id=1, got %v", colMap["id"])
	}
	if colMap["name"] != "Alice" {
		t.Errorf("Expected name='Alice', got %v", colMap["name"])
	}
}
