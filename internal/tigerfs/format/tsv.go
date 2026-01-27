package format

import (
	"fmt"
	"strings"
)

// RowToTSV converts a database row to TSV format
// Columns are in schema order, tab-separated, no header row
// NULL values are represented as empty fields
func RowToTSV(columns []string, values []interface{}) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Convert each value to string
	fields := make([]string, len(values))
	for i, value := range values {
		fields[i] = ValueToString(value)
	}

	// Join with tabs
	line := strings.Join(fields, "\t")

	// Add newline at end
	return []byte(line + "\n"), nil
}

// ValueToString converts a database value to its string representation
// NULL values become empty strings
// This is a public function used by TSV, CSV, and column file formatting
//
// Delegates to ConvertValueToText for consistent type handling across all formats,
// including proper serialization of PostgreSQL types like numeric, UUID, etc.
func ValueToString(value interface{}) string {
	str, err := ConvertValueToText(value)
	if err != nil {
		// Fallback: use fmt to convert to string
		return fmt.Sprintf("%v", value)
	}
	return str
}
