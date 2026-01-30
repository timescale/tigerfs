package format

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRowToJSON_Basic(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Parse to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	// Verify fields
	if parsed["id"].(float64) != 1 {
		t.Errorf("Expected id=1, got %v", parsed["id"])
	}

	if parsed["name"].(string) != "Alice" {
		t.Errorf("Expected name='Alice', got %v", parsed["name"])
	}

	if parsed["email"].(string) != "alice@example.com" {
		t.Errorf("Expected email='alice@example.com', got %v", parsed["email"])
	}
}

func TestRowToJSON_WithNULL(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", nil}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Parse to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	// Verify NULL is represented as nil
	if parsed["email"] != nil {
		t.Errorf("Expected email=null, got %v", parsed["email"])
	}

	// Verify key exists (not omitted)
	if _, exists := parsed["email"]; !exists {
		t.Error("Expected 'email' key to exist with null value")
	}
}

func TestRowToJSON_AllNULL(t *testing.T) {
	columns := []string{"col1", "col2", "col3"}
	values := []interface{}{nil, nil, nil}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Parse to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	// All values should be null
	for _, col := range columns {
		if parsed[col] != nil {
			t.Errorf("Expected %s=null, got %v", col, parsed[col])
		}
	}
}

func TestRowToJSON_ColumnMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	values := []interface{}{1, "Alice", "extra"}

	_, err := RowToJSON(columns, values)
	if err == nil {
		t.Fatal("Expected error for column count mismatch, got nil")
	}

	if !strings.Contains(err.Error(), "column count mismatch") {
		t.Errorf("Expected 'column count mismatch' error, got: %v", err)
	}
}

func TestRowToJSON_EmptyRow(t *testing.T) {
	columns := []string{}
	values := []interface{}{}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Should be empty object
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	if len(parsed) != 0 {
		t.Errorf("Expected empty object, got %v", parsed)
	}
}

func TestRowToJSON_MixedTypes(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	columns := []string{"id", "name", "age", "active", "created_at", "data"}
	values := []interface{}{
		1,
		"Alice",
		30,
		true,
		testTime,
		nil,
	}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Parse to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	// Verify each field
	if parsed["id"].(float64) != 1 {
		t.Errorf("Expected id=1, got %v", parsed["id"])
	}

	if parsed["name"].(string) != "Alice" {
		t.Errorf("Expected name='Alice', got %v", parsed["name"])
	}

	if parsed["age"].(float64) != 30 {
		t.Errorf("Expected age=30, got %v", parsed["age"])
	}

	if parsed["active"].(bool) != true {
		t.Errorf("Expected active=true, got %v", parsed["active"])
	}

	// Time should be serialized as RFC3339 string by encoding/json
	if parsed["created_at"] == nil {
		t.Error("Expected created_at to have a value")
	}

	if parsed["data"] != nil {
		t.Errorf("Expected data=null, got %v", parsed["data"])
	}
}

func TestRowToJSON_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name   string
		column string
		value  interface{}
	}{
		{
			name:   "quotes",
			column: "text",
			value:  "She said \"hello\"",
		},
		{
			name:   "newlines",
			column: "text",
			value:  "Line 1\nLine 2",
		},
		{
			name:   "tabs",
			column: "text",
			value:  "Col1\tCol2",
		},
		{
			name:   "backslashes",
			column: "path",
			value:  "C:\\Users\\Alice",
		},
		{
			name:   "unicode",
			column: "text",
			value:  "Hello 世界 🌍",
		},
		{
			name:   "control chars",
			column: "text",
			value:  "Text\x00with\x01control",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := []string{tt.column}
			values := []interface{}{tt.value}

			result, err := RowToJSON(columns, values)
			if err != nil {
				t.Fatalf("RowToJSON() failed: %v", err)
			}

			// Parse to verify it's valid JSON
			var parsed map[string]interface{}
			if err := json.Unmarshal(result, &parsed); err != nil {
				t.Fatalf("Result is not valid JSON: %v\nOutput: %s", err, string(result))
			}

			// Verify value preserved correctly (JSON package handles escaping)
			if parsed[tt.column] != tt.value {
				t.Errorf("Expected %s=%q, got %q", tt.column, tt.value, parsed[tt.column])
			}
		})
	}
}

func TestRowToJSON_CompactFormat(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Should be single line (no internal newlines)
	resultStr := string(result)
	lines := strings.Split(strings.TrimSpace(resultStr), "\n")

	if len(lines) != 1 {
		t.Errorf("Expected single line output, got %d lines:\n%s", len(lines), resultStr)
	}

	// Should not have extra whitespace
	if strings.Contains(resultStr, "  ") {
		t.Error("JSON output should be compact (no extra whitespace)")
	}
}

func TestRowToJSON_EndsWithNewline(t *testing.T) {
	columns := []string{"id"}
	values := []interface{}{1}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Should end with newline for consistency with TSV/CSV
	if !strings.HasSuffix(string(result), "\n") {
		t.Error("JSON output should end with newline")
	}
}

func TestRowToJSON_ByteArray(t *testing.T) {
	columns := []string{"id", "data"}
	values := []interface{}{1, []byte("binary data")}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Parse to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	// Byte arrays should be base64 encoded by encoding/json
	if parsed["data"] == nil {
		t.Error("Expected data to have a value")
	}
}

func TestRowToJSON_NestedTypes(t *testing.T) {
	// Test with types that json.Marshal can handle
	columns := []string{"id", "tags", "metadata"}
	values := []interface{}{
		1,
		[]string{"tag1", "tag2"},
		map[string]string{"key": "value"},
	}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	// Parse to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	// Verify nested structures preserved
	if parsed["tags"] == nil {
		t.Error("Expected tags array to exist")
	}

	if parsed["metadata"] == nil {
		t.Error("Expected metadata object to exist")
	}
}

// TestRowToJSON_PreservesAmpersand verifies that & is not escaped as \u0026.
// Regression test: encoding/json with SetEscapeHTML(true) escapes HTML chars.
func TestRowToJSON_PreservesAmpersand(t *testing.T) {
	columns := []string{"name"}
	values := []interface{}{"Tom & Jerry"}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	resultStr := string(result)

	// Should contain literal ampersand
	if !strings.Contains(resultStr, "Tom & Jerry") {
		t.Errorf("Expected 'Tom & Jerry' in output, got: %s", resultStr)
	}

	// Should NOT contain escaped ampersand
	if strings.Contains(resultStr, "\\u0026") {
		t.Errorf("Ampersand should not be escaped as \\u0026, got: %s", resultStr)
	}

	// Verify it's still valid JSON that parses correctly
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}
	if parsed["name"] != "Tom & Jerry" {
		t.Errorf("Expected name='Tom & Jerry', got %v", parsed["name"])
	}
}

// TestRowToJSON_PreservesHTMLChars verifies that < > & are not escaped.
// These are valid in JSON but encoding/json escapes them by default for HTML safety.
func TestRowToJSON_PreservesHTMLChars(t *testing.T) {
	columns := []string{"html"}
	values := []interface{}{"<script>alert('xss')</script> & more"}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	resultStr := string(result)

	// Should NOT contain HTML-escaped characters
	if strings.Contains(resultStr, "\\u003c") || strings.Contains(resultStr, "\\u003e") || strings.Contains(resultStr, "\\u0026") {
		t.Errorf("HTML characters should not be escaped, got: %s", resultStr)
	}

	// Verify round-trip works
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}
	if parsed["html"] != "<script>alert('xss')</script> & more" {
		t.Errorf("HTML chars not preserved in round-trip, got %v", parsed["html"])
	}
}

// TestRowsToJSON_PreservesAmpersand tests bulk JSON output preserves ampersand.
func TestRowsToJSON_PreservesAmpersand(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Ben & Jerry's"},
		{2, "AT&T"},
	}

	result, err := RowsToJSON(columns, rows)
	if err != nil {
		t.Fatalf("RowsToJSON() failed: %v", err)
	}

	resultStr := string(result)

	// Should contain literal ampersands
	if !strings.Contains(resultStr, "Ben & Jerry's") {
		t.Errorf("Expected 'Ben & Jerry's' in output, got: %s", resultStr)
	}
	if !strings.Contains(resultStr, "AT&T") {
		t.Errorf("Expected 'AT&T' in output, got: %s", resultStr)
	}

	// Should NOT contain escaped ampersands
	if strings.Contains(resultStr, "\\u0026") {
		t.Errorf("Ampersand should not be escaped, got: %s", resultStr)
	}
}

// TestRowToJSON_UUID tests that UUIDs are serialized as strings, not byte arrays.
// Regression test: pgx returns UUIDs as [16]byte which json.Marshal outputs as array.
func TestRowToJSON_UUID(t *testing.T) {
	// Simulate pgx returning a UUID as [16]byte
	uuidBytes := [16]byte{
		0x01, 0x9c, 0x0d, 0x00, 0x86, 0xea, 0x75, 0x14,
		0xa6, 0x8d, 0xc4, 0x92, 0x1b, 0xc7, 0x6e, 0x25,
	}

	columns := []string{"id", "name"}
	values := []interface{}{uuidBytes, "test"}

	result, err := RowToJSON(columns, values)
	if err != nil {
		t.Fatalf("RowToJSON() failed: %v", err)
	}

	resultStr := string(result)
	t.Logf("JSON output: %s", resultStr)

	// Should contain UUID as string, not as array
	if !strings.Contains(resultStr, "019c0d00-86ea-7514-a68d-c4921bc76e25") {
		t.Errorf("Expected UUID string in output, got: %s", resultStr)
	}

	// Should NOT contain array representation
	if strings.Contains(resultStr, "[1,156,") {
		t.Errorf("UUID should not be serialized as byte array, got: %s", resultStr)
	}
}

// TestRowsToJSON_UUID tests bulk JSON with UUIDs.
func TestRowsToJSON_UUID(t *testing.T) {
	// Simulate pgx returning UUIDs as [16]byte
	uuid1 := [16]byte{0x01, 0x9c, 0x0d, 0x00, 0x86, 0xea, 0x75, 0x14, 0xa6, 0x8d, 0xc4, 0x92, 0x1b, 0xc7, 0x6e, 0x25}
	uuid2 := [16]byte{0x01, 0x9c, 0x0d, 0x01, 0x86, 0xea, 0x75, 0x14, 0xa6, 0x8d, 0xc4, 0x92, 0x1b, 0xc7, 0x6e, 0x26}

	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{uuid1, "Alice"},
		{uuid2, "Bob"},
	}

	result, err := RowsToJSON(columns, rows)
	if err != nil {
		t.Fatalf("RowsToJSON() failed: %v", err)
	}

	resultStr := string(result)
	t.Logf("JSON output: %s", resultStr)

	// Should contain UUIDs as strings
	if !strings.Contains(resultStr, "019c0d00-86ea-7514-a68d-c4921bc76e25") {
		t.Errorf("Expected first UUID string in output, got: %s", resultStr)
	}
	if !strings.Contains(resultStr, "019c0d01-86ea-7514-a68d-c4921bc76e26") {
		t.Errorf("Expected second UUID string in output, got: %s", resultStr)
	}

	// Should NOT contain array representations
	if strings.Contains(resultStr, "[1,156,") {
		t.Errorf("UUIDs should not be serialized as byte arrays, got: %s", resultStr)
	}
}
