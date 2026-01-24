package format

import (
	"fmt"
	"strings"
)

// RowToCSV converts a database row to CSV format (RFC 4180)
// Columns are in schema order, comma-separated, no header row
// NULL values are represented as empty fields
// Fields containing commas, quotes, or newlines are quoted
func RowToCSV(columns []string, values []interface{}) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Convert each value to CSV field
	fields := make([]string, len(values))
	for i, value := range values {
		fields[i] = valueToCSVField(value)
	}

	// Join with commas
	line := strings.Join(fields, ",")

	// Add newline at end
	return []byte(line + "\n"), nil
}

// valueToCSVField converts a database value to CSV field
// Handles quoting according to RFC 4180
func valueToCSVField(value interface{}) string {
	// Convert to string first
	str := ValueToString(value)

	// Determine if field needs quoting
	needsQuote := strings.ContainsAny(str, ",\"\n\r")

	if !needsQuote {
		return str
	}

	// Quote the field and escape internal quotes by doubling
	escaped := strings.ReplaceAll(str, "\"", "\"\"")
	return "\"" + escaped + "\""
}
