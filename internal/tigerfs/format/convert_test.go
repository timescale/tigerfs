package format

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestConvertValueToText_String(t *testing.T) {
	result, err := ConvertValueToText("hello world")
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	if result != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", result)
	}
}

func TestConvertValueToText_Bytes(t *testing.T) {
	result, err := ConvertValueToText([]byte("binary data"))
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	if result != "binary data" {
		t.Errorf("Expected 'binary data', got '%s'", result)
	}
}

func TestConvertValueToText_Integer(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{int(42), "42"},
		{int8(-128), "-128"},
		{int16(32767), "32767"},
		{int32(-2147483648), "-2147483648"},
		{int64(9223372036854775807), "9223372036854775807"},
		{uint(42), "42"},
		{uint8(255), "255"},
		{uint16(65535), "65535"},
		{uint32(4294967295), "4294967295"},
		{uint64(18446744073709551615), "18446744073709551615"},
	}

	for _, tt := range tests {
		result, err := ConvertValueToText(tt.value)
		if err != nil {
			t.Fatalf("ConvertValueToText(%v) failed: %v", tt.value, err)
		}

		if result != tt.expected {
			t.Errorf("ConvertValueToText(%v) = '%s', expected '%s'", tt.value, result, tt.expected)
		}
	}
}

func TestConvertValueToText_Float(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{float32(3.14), "3.14"},
		{float64(3.14159), "3.14159"},
		{float64(100.0), "100"},
		{float64(0.0), "0"},
		{float64(-1.5), "-1.5"},
	}

	for _, tt := range tests {
		result, err := ConvertValueToText(tt.value)
		if err != nil {
			t.Fatalf("ConvertValueToText(%v) failed: %v", tt.value, err)
		}

		if result != tt.expected {
			t.Errorf("ConvertValueToText(%v) = '%s', expected '%s'", tt.value, result, tt.expected)
		}
	}
}

func TestConvertValueToText_Boolean(t *testing.T) {
	result, err := ConvertValueToText(true)
	if err != nil {
		t.Fatalf("ConvertValueToText(true) failed: %v", err)
	}
	if result != "t" {
		t.Errorf("Expected 't' for true, got '%s'", result)
	}

	result, err = ConvertValueToText(false)
	if err != nil {
		t.Fatalf("ConvertValueToText(false) failed: %v", err)
	}
	if result != "f" {
		t.Errorf("Expected 'f' for false, got '%s'", result)
	}
}

func TestConvertValueToText_Time(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	result, err := ConvertValueToText(testTime)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	expected := "2024-01-15T10:30:00Z"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestConvertValueToText_NULL(t *testing.T) {
	result, err := ConvertValueToText(nil)
	if err != nil {
		t.Fatalf("ConvertValueToText(nil) failed: %v", err)
	}

	if result != "" {
		t.Errorf("Expected empty string for NULL, got '%s'", result)
	}
}

func TestConvertValueToText_JSONBObject(t *testing.T) {
	jsonbValue := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
		"key3": true,
	}

	result, err := ConvertValueToText(jsonbValue)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v\nResult: %s", err, result)
	}

	// Verify values
	if parsed["key1"] != "value1" {
		t.Errorf("Expected key1='value1', got %v", parsed["key1"])
	}
	if parsed["key2"].(float64) != 42 {
		t.Errorf("Expected key2=42, got %v", parsed["key2"])
	}
	if parsed["key3"] != true {
		t.Errorf("Expected key3=true, got %v", parsed["key3"])
	}

	t.Logf("JSONB object result: %s", result)
}

func TestConvertValueToText_JSONBArray(t *testing.T) {
	jsonbArray := []interface{}{"a", "b", "c", 1, 2, 3}

	result, err := ConvertValueToText(jsonbArray)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	// Verify it's valid JSON array
	var parsed []interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON array: %v\nResult: %s", err, result)
	}

	// Verify length
	if len(parsed) != 6 {
		t.Errorf("Expected 6 elements, got %d", len(parsed))
	}

	// Verify some values
	if parsed[0] != "a" {
		t.Errorf("Expected first element='a', got %v", parsed[0])
	}

	t.Logf("JSONB array result: %s", result)
}

func TestConvertValueToText_PostgresArray(t *testing.T) {
	// PostgreSQL arrays come as []interface{}
	pgArray := []interface{}{"alice@example.com", "bob@example.com", "charlie@example.com"}

	result, err := ConvertValueToText(pgArray)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	// Verify it's valid JSON array
	var parsed []interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON array: %v\nResult: %s", err, result)
	}

	// Verify values
	if len(parsed) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(parsed))
	}

	if parsed[0] != "alice@example.com" {
		t.Errorf("Expected first element='alice@example.com', got %v", parsed[0])
	}

	t.Logf("Postgres array result: %s", result)
}

func TestConvertValueToText_NestedJSON(t *testing.T) {
	nestedValue := map[string]interface{}{
		"user": map[string]interface{}{
			"name":  "Alice",
			"email": "alice@example.com",
			"tags":  []interface{}{"admin", "verified"},
		},
		"count": 42,
	}

	result, err := ConvertValueToText(nestedValue)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v\nResult: %s", err, result)
	}

	// Verify nested structure
	user := parsed["user"].(map[string]interface{})
	if user["name"] != "Alice" {
		t.Errorf("Expected user.name='Alice', got %v", user["name"])
	}

	tags := user["tags"].([]interface{})
	if len(tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(tags))
	}

	t.Logf("Nested JSON result: %s", result)
}

func TestConvertValueToText_EmptyArray(t *testing.T) {
	emptyArray := []interface{}{}

	result, err := ConvertValueToText(emptyArray)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	if result != "[]" {
		t.Errorf("Expected '[]', got '%s'", result)
	}
}

func TestConvertValueToText_EmptyObject(t *testing.T) {
	emptyObject := map[string]interface{}{}

	result, err := ConvertValueToText(emptyObject)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	if result != "{}" {
		t.Errorf("Expected '{}', got '%s'", result)
	}
}

func TestBoolToPostgresText(t *testing.T) {
	if BoolToPostgresText(true) != "t" {
		t.Error("Expected 't' for true")
	}

	if BoolToPostgresText(false) != "f" {
		t.Error("Expected 'f' for false")
	}
}

func TestBoolToStandardText(t *testing.T) {
	if BoolToStandardText(true) != "true" {
		t.Error("Expected 'true' for true")
	}

	if BoolToStandardText(false) != "false" {
		t.Error("Expected 'false' for false")
	}
}

func TestTimeToText(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	result := TimeToText(testTime)

	expected := "2024-01-15T10:30:00Z"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestJSONBToText(t *testing.T) {
	jsonbValue := map[string]interface{}{
		"key": "value",
		"num": 42,
	}

	result, err := JSONBToText(jsonbValue)
	if err != nil {
		t.Fatalf("JSONBToText() failed: %v", err)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON: %v", err)
	}

	if parsed["key"] != "value" {
		t.Errorf("Expected key='value', got %v", parsed["key"])
	}
}

func TestJSONBToText_Invalid(t *testing.T) {
	// Create a value that can't be marshaled to JSON
	invalidValue := make(chan int)

	_, err := JSONBToText(invalidValue)
	if err == nil {
		t.Error("Expected error for invalid value, got nil")
	}

	if !strings.Contains(err.Error(), "failed to marshal") {
		t.Errorf("Expected 'failed to marshal' error, got: %v", err)
	}
}

func TestArrayToText(t *testing.T) {
	array := []interface{}{"a", "b", "c"}

	result, err := ArrayToText(array)
	if err != nil {
		t.Fatalf("ArrayToText() failed: %v", err)
	}

	// Verify it's valid JSON array
	var parsed []interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON array: %v", err)
	}

	if len(parsed) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(parsed))
	}
}

func TestArrayToText_MixedTypes(t *testing.T) {
	array := []interface{}{"string", 42, true, 3.14, nil}

	result, err := ArrayToText(array)
	if err != nil {
		t.Fatalf("ArrayToText() failed: %v", err)
	}

	// Verify it's valid JSON array
	var parsed []interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("Result is not valid JSON array: %v", err)
	}

	if len(parsed) != 5 {
		t.Errorf("Expected 5 elements, got %d", len(parsed))
	}

	// Verify types preserved
	if parsed[0] != "string" {
		t.Errorf("Expected string, got %v", parsed[0])
	}
	if parsed[1].(float64) != 42 {
		t.Errorf("Expected 42, got %v", parsed[1])
	}
	if parsed[2] != true {
		t.Errorf("Expected true, got %v", parsed[2])
	}
	if parsed[4] != nil {
		t.Errorf("Expected nil, got %v", parsed[4])
	}
}

func TestConvertValueToText_CompactJSON(t *testing.T) {
	// Verify JSON output is compact (no extra whitespace)
	jsonbValue := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
	}

	result, err := ConvertValueToText(jsonbValue)
	if err != nil {
		t.Fatalf("ConvertValueToText() failed: %v", err)
	}

	// Should not contain extra whitespace
	if strings.Contains(result, "  ") {
		t.Error("JSON output should be compact (no extra whitespace)")
	}

	// Should not contain newlines
	if strings.Contains(result, "\n") {
		t.Error("JSON output should be single line (no newlines)")
	}

	t.Logf("Compact JSON: %s", result)
}
