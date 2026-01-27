package format

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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

// mockNumeric is a test type that implements fmt.Stringer with a pointer receiver.
// This tests the Stringer code path in ConvertValueToText for types like pgtype.UUID.
//
// NOTE: This does NOT simulate pgtype.Numeric - pgtype.Numeric doesn't implement
// fmt.Stringer. For pgtype.Numeric, we rely on json.Marshal calling MarshalJSON.
// See TestValueToString_PGTypeNumeric for the actual pgtype.Numeric tests.
type mockNumeric struct {
	Int              int64
	Exp              int32
	NaN              bool
	InfinityModifier string
	Valid            bool
}

// String returns the human-readable decimal representation.
// This uses a pointer receiver to test that the Stringer check handles pointer methods.
func (n *mockNumeric) String() string {
	if !n.Valid {
		return ""
	}
	// Simplified: just format as decimal based on Int and Exp
	// e.g., Int=38800, Exp=-2 => 388.00
	if n.Exp == 0 {
		return fmt.Sprintf("%d", n.Int)
	} else if n.Exp < 0 {
		// Negative exponent means decimal places
		format := fmt.Sprintf("%%d.%%0%dd", -n.Exp)
		divisor := int64(1)
		for i := int32(0); i < -n.Exp; i++ {
			divisor *= 10
		}
		intPart := n.Int / divisor
		fracPart := n.Int % divisor
		return fmt.Sprintf(format, intPart, fracPart)
	}
	// Positive exponent (multiply)
	result := n.Int
	for i := int32(0); i < n.Exp; i++ {
		result *= 10
	}
	return fmt.Sprintf("%d", result)
}

func TestValueToString_Stringer(t *testing.T) {
	// Test that types implementing fmt.Stringer use String() method.
	// This tests types like pgtype.UUID that have String() methods.
	//
	// NOTE: pgtype.Numeric does NOT implement fmt.Stringer - it uses MarshalJSON
	// via the json.Marshal fallback. See TestValueToString_PGTypeNumeric for that.

	tests := []struct {
		name     string
		value    *mockNumeric // Pointer to test Stringer with pointer receiver
		expected string
	}{
		{
			name: "decimal 388.00",
			value: &mockNumeric{
				Int:   38800,
				Exp:   -2,
				Valid: true,
			},
			expected: "388.00",
		},
		{
			name: "decimal 25.00",
			value: &mockNumeric{
				Int:   2500,
				Exp:   -2,
				Valid: true,
			},
			expected: "25.00",
		},
		{
			name: "integer 100",
			value: &mockNumeric{
				Int:   100,
				Exp:   0,
				Valid: true,
			},
			expected: "100",
		},
		{
			name: "decimal 0.50",
			value: &mockNumeric{
				Int:   50,
				Exp:   -2,
				Valid: true,
			},
			expected: "0.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pass as interface{} to simulate database driver behavior
			var iface interface{} = tt.value
			result := ValueToString(iface)

			// Verify we get the String() output, not the %v struct output
			if result != tt.expected {
				t.Errorf("ValueToString() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestRowToTSV_WithNumericStringer(t *testing.T) {
	// Test full row serialization with a type that implements fmt.Stringer.
	// Uses mockNumeric (which has String() method) to verify the Stringer code path.
	columns := []string{"id", "user_id", "product_id", "quantity", "total", "status"}
	values := []interface{}{
		3610,
		11,
		111,
		1,
		&mockNumeric{Int: 17500, Exp: -2, Valid: true}, // 175.00
		"cancelled",
	}

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "3610\t11\t111\t1\t175.00\tcancelled\n"
	if string(result) != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, string(result))
	}

	// Verify the result does NOT contain struct representation
	if strings.Contains(string(result), "{17500") {
		t.Error("Result contains internal struct representation instead of String() output")
	}
}

// newPGNumeric creates a pgtype.Numeric from an integer and exponent
// e.g., newPGNumeric(17500, -2) represents 175.00
func newPGNumeric(intValue int64, exp int32) pgtype.Numeric {
	return pgtype.Numeric{
		Int:              big.NewInt(intValue),
		Exp:              exp,
		NaN:              false,
		InfinityModifier: pgtype.Finite,
		Valid:            true,
	}
}

func TestValueToString_PGTypeNumeric(t *testing.T) {
	// Test the ACTUAL pgtype.Numeric type (not a mock)
	// This is what the database driver returns for numeric columns

	tests := []struct {
		name     string
		value    pgtype.Numeric
		expected string
	}{
		{
			name:     "decimal 175.00",
			value:    newPGNumeric(17500, -2),
			expected: "175.00",
		},
		{
			name:     "decimal 25.00",
			value:    newPGNumeric(2500, -2),
			expected: "25.00",
		},
		{
			name:     "decimal 19.99",
			value:    newPGNumeric(1999, -2),
			expected: "19.99",
		},
		{
			name:     "integer 100",
			value:    newPGNumeric(100, 0),
			expected: "100",
		},
		{
			name:     "decimal 0.50",
			value:    newPGNumeric(50, -2),
			expected: "0.50",
		},
		{
			name:     "decimal 1234.56",
			value:    newPGNumeric(123456, -2),
			expected: "1234.56",
		},
		{
			name:     "negative -99.99",
			value:    newPGNumeric(-9999, -2),
			expected: "-99.99",
		},
		{
			// Note: In practice, database NULLs come through as Go nil, not
			// pgtype.Numeric{Valid: false}. This edge case is handled via
			// MarshalJSON which returns "null" - acceptable for this rare case.
			name: "invalid/null numeric via struct",
			value: pgtype.Numeric{
				Valid: false,
			},
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pass as interface{} to simulate database driver behavior
			var iface interface{} = tt.value
			result := ValueToString(iface)

			if result != tt.expected {
				t.Errorf("ValueToString() = %q, want %q", result, tt.expected)
			}

			// Verify no struct representation leaked through
			if strings.Contains(result, "{") {
				t.Errorf("Result %q contains struct representation", result)
			}
		})
	}
}

func TestRowToTSV_WithPGTypeNumeric(t *testing.T) {
	// Test full row serialization with ACTUAL pgtype.Numeric
	// This is the exact scenario from the bug report
	columns := []string{"id", "user_id", "product_id", "quantity", "total", "status"}
	values := []interface{}{
		3610,
		11,
		111,
		1,
		newPGNumeric(17500, -2), // 175.00 - actual pgtype.Numeric
		"cancelled",
	}

	result, err := RowToTSV(columns, values)
	if err != nil {
		t.Fatalf("RowToTSV() failed: %v", err)
	}

	expected := "3610\t11\t111\t1\t175.00\tcancelled\n"
	if string(result) != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, string(result))
	}

	// Verify the result does NOT contain struct representation
	if strings.Contains(string(result), "{17500") {
		t.Error("Result contains internal struct representation instead of decimal output")
	}
	if strings.Contains(string(result), "finite") {
		t.Error("Result contains 'finite' from internal struct representation")
	}
}

func TestValueToString_PGTypeNumericPointer(t *testing.T) {
	// Test pointer to pgtype.Numeric (in case pgx returns pointers)
	num := newPGNumeric(25000, -2) // 250.00

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "pointer to valid numeric",
			value:    &num,
			expected: "250.00",
		},
		{
			// Note: nil pointers are handled via json.Marshal which returns "null"
			// In practice, database NULLs come through as Go nil interface{}, not nil pointers
			name:     "nil pointer",
			value:    (*pgtype.Numeric)(nil),
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.value)
			if result != tt.expected {
				t.Errorf("ValueToString() = %q, want %q", result, tt.expected)
			}
		})
	}
}
