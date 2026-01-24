package format

import (
	"encoding/json"
	"fmt"
)

// RowToJSON converts a database row to compact JSON format
// Returns a single-line JSON object with column names as keys
// NULL values are represented as JSON null
func RowToJSON(columns []string, values []interface{}) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Build map of column name -> value
	rowMap := make(map[string]interface{}, len(columns))
	for i, column := range columns {
		rowMap[column] = values[i]
	}

	// Marshal to JSON (compact format, no pretty printing)
	data, err := json.Marshal(rowMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Add newline at end for consistency with TSV/CSV
	return append(data, '\n'), nil
}
