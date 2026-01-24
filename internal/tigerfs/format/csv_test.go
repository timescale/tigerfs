package format

import (
	"strings"
	"testing"
	"time"
)

func TestRowToCSV_Basic(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	expected := "1,Alice,alice@example.com\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_WithNULL(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", nil}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	expected := "1,Alice,\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_AllNULL(t *testing.T) {
	columns := []string{"col1", "col2", "col3"}
	values := []interface{}{nil, nil, nil}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	expected := ",,\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_WithComma(t *testing.T) {
	columns := []string{"id", "name", "address"}
	values := []interface{}{1, "Alice", "123 Main St, Apt 4"}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	// Field with comma should be quoted
	expected := "1,Alice,\"123 Main St, Apt 4\"\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_WithQuote(t *testing.T) {
	columns := []string{"id", "name", "description"}
	values := []interface{}{1, "Alice", "She said \"hello\""}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	// Field with quotes should be quoted and quotes escaped by doubling
	expected := "1,Alice,\"She said \"\"hello\"\"\"\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_WithNewline(t *testing.T) {
	columns := []string{"id", "name", "bio"}
	values := []interface{}{1, "Alice", "Line 1\nLine 2"}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	// Field with newline should be quoted
	expected := "1,Alice,\"Line 1\nLine 2\"\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_MixedSpecialChars(t *testing.T) {
	columns := []string{"id", "text"}
	values := []interface{}{1, "Text with: comma,quote\",newline\n"}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	// Multiple special chars - should be quoted and quotes doubled
	expected := "1,\"Text with: comma,quote\"\",newline\n\"\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_ColumnMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	values := []interface{}{1, "Alice", "extra"}

	_, err := RowToCSV(columns, values)
	if err == nil {
		t.Fatal("Expected error for column count mismatch, got nil")
	}

	if !strings.Contains(err.Error(), "column count mismatch") {
		t.Errorf("Expected 'column count mismatch' error, got: %v", err)
	}
}

func TestRowToCSV_EmptyRow(t *testing.T) {
	columns := []string{}
	values := []interface{}{}

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	expected := "\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToCSV_MixedTypes(t *testing.T) {
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

	result, err := RowToCSV(columns, values)
	if err != nil {
		t.Fatalf("RowToCSV() failed: %v", err)
	}

	expected := "1,Alice,30,t,2024-01-15T10:30:00Z,\n"
	if string(result) != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, string(result))
	}
}

func TestValueToCSVField_NoQuoting(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{"simple", "simple"},
		{123, "123"},
		{true, "t"},
		{nil, ""},
	}

	for _, tt := range tests {
		result := valueToCSVField(tt.value)
		if result != tt.expected {
			t.Errorf("valueToCSVField(%v) = '%s', expected '%s'", tt.value, result, tt.expected)
		}
	}
}

func TestValueToCSVField_WithQuoting(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{"has,comma", "\"has,comma\""},
		{"has\"quote", "\"has\"\"quote\""},
		{"has\newline", "\"has\newline\""},
		{"has\rcarriage", "\"has\rcarriage\""},
		{"has,comma\"and quote", "\"has,comma\"\"and quote\""},
	}

	for _, tt := range tests {
		result := valueToCSVField(tt.value)
		if result != tt.expected {
			t.Errorf("valueToCSVField(%v) = '%s', expected '%s'", tt.value, result, tt.expected)
		}
	}
}

func TestRowToCSV_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		values   []interface{}
		expected string
	}{
		{
			name:     "single column",
			columns:  []string{"id"},
			values:   []interface{}{1},
			expected: "1\n",
		},
		{
			name:     "all empty strings",
			columns:  []string{"a", "b", "c"},
			values:   []interface{}{"", "", ""},
			expected: ",,\n",
		},
		{
			name:     "mixed empty and null",
			columns:  []string{"a", "b", "c"},
			values:   []interface{}{"", nil, "value"},
			expected: ",,value\n",
		},
		{
			name:     "only quotes",
			columns:  []string{"text"},
			values:   []interface{}{"\"\"\""},
			expected: "\"\"\"\"\"\"\"\"\n", // Three quotes -> doubled (6) -> wrapped in quotes (8 total)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RowToCSV(tt.columns, tt.values)
			if err != nil {
				t.Fatalf("RowToCSV() failed: %v", err)
			}

			if string(result) != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, string(result))
			}
		})
	}
}
