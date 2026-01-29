package format

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// RowToJSON converts a database row to compact JSON format.
// Returns a single-line JSON object with column names as keys.
// NULL values are represented as JSON null.
// Characters like &, <, > are NOT escaped (no HTML safety escaping).
func RowToJSON(columns []string, values []interface{}) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Build map of column name -> value
	rowMap := make(map[string]interface{}, len(columns))
	for i, column := range columns {
		rowMap[column] = values[i]
	}

	// Marshal to JSON without HTML escaping
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(rowMap); err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Encode adds a newline, so we're done
	return buf.Bytes(), nil
}

// RowsToJSON converts multiple database rows to JSON array format.
// Returns a JSON array of objects, with column names as keys.
// NULL values are represented as JSON null.
// Empty input returns "[]" (empty array).
// Characters like &, <, > are NOT escaped (no HTML safety escaping).
//
// Parameters:
//   - columns: Column names in database order
//   - rows: Row values as [][]interface{}
//
// Returns JSON array with pretty printing for readability.
func RowsToJSON(columns []string, rows [][]interface{}) ([]byte, error) {
	// Build array of row objects
	result := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("row %d: column count mismatch: %d columns, %d values", i, len(columns), len(row))
		}
		rowMap := make(map[string]interface{}, len(columns))
		for j, column := range columns {
			rowMap[column] = row[j]
		}
		result[i] = rowMap
	}

	// Marshal to JSON with indentation, without HTML escaping
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Encode adds a newline, so we're done
	return buf.Bytes(), nil
}
