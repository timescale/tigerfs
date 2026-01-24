package format

import (
	"strings"
	"testing"
	"time"
)

func TestRowToTSV_Basic(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "1\tAlice\talice@example.com\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToTSV_WithNULL(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice", nil}

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "1\tAlice\t\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToTSV_AllNULL(t *testing.T) {
	columns := []string{"col1", "col2", "col3"}
	values := []interface{}{nil, nil, nil}

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "\t\t\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestRowToTSV_ColumnMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	values := []interface{}{1, "Alice", "extra"}

	_, err := RowToTSV(columns, values)
	if err == nil {
		t.Fatal("Expected error for column count mismatch, got nil")
	}

	if !strings.Contains(err.Error(), "column count mismatch") {
		t.Errorf("Expected 'column count mismatch' error, got: %v", err)
	}
}

func TestRowToTSV_EmptyRow(t *testing.T) {
	columns := []string{}
	values := []interface{}{}

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "\n"
	if string(result) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(result))
	}
}

func TestValueToString_String(t *testing.T) {
	result := ValueToString("hello")
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestValueToString_Bytes(t *testing.T) {
	result := ValueToString([]byte("hello"))
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestValueToString_Int(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{int(42), "42"},
		{int8(42), "42"},
		{int16(42), "42"},
		{int32(42), "42"},
		{int64(42), "42"},
		{uint(42), "42"},
		{uint8(42), "42"},
		{uint16(42), "42"},
		{uint32(42), "42"},
		{uint64(42), "42"},
	}

	for _, tt := range tests {
		result := ValueToString(tt.value)
		if result != tt.expected {
			t.Errorf("ValueToString(%v) = '%s', expected '%s'", tt.value, result, tt.expected)
		}
	}
}

func TestValueToString_Float(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{float32(3.14), "3.14"},
		{float64(3.14159), "3.14159"},
		{float64(100.0), "100"},
		{float64(0.0), "0"},
	}

	for _, tt := range tests {
		result := ValueToString(tt.value)
		if result != tt.expected {
			t.Errorf("ValueToString(%v) = '%s', expected '%s'", tt.value, result, tt.expected)
		}
	}
}

func TestValueToString_Bool(t *testing.T) {
	if ValueToString(true) != "t" {
		t.Errorf("Expected 't' for true")
	}

	if ValueToString(false) != "f" {
		t.Errorf("Expected 'f' for false")
	}
}

func TestValueToString_Time(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	result := ValueToString(testTime)

	// Should be in RFC3339 format
	expected := "2024-01-15T10:30:00Z"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestValueToString_NULL(t *testing.T) {
	result := ValueToString(nil)
	if result != "" {
		t.Errorf("Expected empty string for NULL, got '%s'", result)
	}
}

func TestRowToTSV_MixedTypes(t *testing.T) {
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

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "1\tAlice\t30\tt\t2024-01-15T10:30:00Z\t\n"
	if string(result) != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, string(result))
	}
}
