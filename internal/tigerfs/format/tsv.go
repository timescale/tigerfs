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

// RowsToTSV converts multiple database rows to TSV format.
// Data rows only, no header row (consistent with row-as-file reads).
// Column names available via .info/columns if needed.
// NULL values are represented as empty fields.
// Tab-separated, no quoting.
// Empty input returns empty output.
//
// Parameters:
//   - columns: Column names in database order (used for validation only)
//   - rows: Row values as [][]interface{}
//
// Returns TSV data with trailing newline per row.
func RowsToTSV(columns []string, rows [][]interface{}) ([]byte, error) {
	if len(rows) == 0 {
		return []byte{}, nil
	}

	var sb strings.Builder

	// Write data rows (no header)
	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(row))
		}
		fields := make([]string, len(row))
		for i, value := range row {
			fields[i] = ValueToString(value)
		}
		sb.WriteString(strings.Join(fields, "\t"))
		sb.WriteString("\n")
	}

	return []byte(sb.String()), nil
}

// RowsToTSVWithHeaders converts multiple database rows to TSV format with a header row.
// First row is column names, subsequent rows are data.
// Used for round-trip compatibility with import.
func RowsToTSVWithHeaders(columns []string, rows [][]interface{}) ([]byte, error) {
	var sb strings.Builder

	// Write header row
	sb.WriteString(strings.Join(columns, "\t"))
	sb.WriteString("\n")

	// Write data rows
	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(row))
		}
		fields := make([]string, len(row))
		for i, value := range row {
			fields[i] = ValueToString(value)
		}
		sb.WriteString(strings.Join(fields, "\t"))
		sb.WriteString("\n")
	}

	return []byte(sb.String()), nil
}
