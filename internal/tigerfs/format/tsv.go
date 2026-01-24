package format

import (
	"fmt"
	"strings"
	"time"
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
		fields[i] = valueToString(value)
	}

	// Join with tabs
	line := strings.Join(fields, "\t")

	// Add newline at end
	return []byte(line + "\n"), nil
}

// valueToString converts a database value to its string representation for TSV
// NULL values become empty strings
func valueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case []byte:
		// BYTEA or text data
		return string(v)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "t"
		}
		return "f"
	case time.Time:
		// ISO 8601 format
		return v.Format(time.RFC3339)
	default:
		// Fallback: use fmt to convert to string
		return fmt.Sprintf("%v", v)
	}
}
