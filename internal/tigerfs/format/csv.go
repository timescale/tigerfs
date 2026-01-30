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

// RowsToCSV converts multiple database rows to CSV format (RFC 4180).
// Data rows only, no header row (consistent with row-as-file reads).
// Column names available via .info/columns if needed.
// NULL values are represented as empty fields.
// Fields containing commas, quotes, or newlines are quoted.
// Empty input returns empty output.
//
// Parameters:
//   - columns: Column names in database order (used for validation only)
//   - rows: Row values as [][]interface{}
//
// Returns CSV data with trailing newline per row.
func RowsToCSV(columns []string, rows [][]interface{}) ([]byte, error) {
	if len(rows) == 0 {
		return []byte{}, nil
	}

	var sb strings.Builder

	// Write data rows (no header)
	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(row))
		}
		for i, value := range row {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(valueToCSVField(value))
		}
		sb.WriteString("\n")
	}

	return []byte(sb.String()), nil
}

// RowsToCSVWithHeaders converts multiple database rows to CSV format with a header row.
// First row is column names, subsequent rows are data.
// Used for round-trip compatibility with import.
func RowsToCSVWithHeaders(columns []string, rows [][]interface{}) ([]byte, error) {
	var sb strings.Builder

	// Write header row
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(valueToCSVField(col))
	}
	sb.WriteString("\n")

	// Write data rows
	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(row))
		}
		for i, value := range row {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(valueToCSVField(value))
		}
		sb.WriteString("\n")
	}

	return []byte(sb.String()), nil
}
