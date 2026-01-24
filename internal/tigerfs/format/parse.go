package format

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
)

// ParseTSV parses a TSV line into columns and values
// Format: value1\tvalue2\tvalue3
// Empty values are treated as NULL
// Note: This requires column names to be known externally
func ParseTSV(line string) ([]string, []interface{}, error) {
	// Split by tabs
	parts := strings.Split(line, "\t")

	// Convert to interface{} slice
	values := make([]interface{}, len(parts))
	for i, part := range parts {
		if part == "" {
			values[i] = nil // NULL
		} else {
			values[i] = part
		}
	}

	// Note: TSV doesn't include column names, so we return empty column list
	// The caller must provide column names separately
	return nil, values, nil
}

// ParseCSV parses a CSV line into columns and values
// Format: value1,value2,"quoted value, with comma"
// Empty values are treated as NULL
func ParseCSV(line string) ([]string, []interface{}, error) {
	// Use csv.Reader to handle quoted fields
	reader := csv.NewReader(strings.NewReader(line))
	reader.TrimLeadingSpace = true

	record, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	// Convert to interface{} slice
	values := make([]interface{}, len(record))
	for i, field := range record {
		if field == "" {
			values[i] = nil // NULL
		} else {
			values[i] = field
		}
	}

	// CSV doesn't include column names in data, return empty column list
	return nil, values, nil
}

// ParseJSON parses a JSON object into columns and values
// Format: {"col1": "value1", "col2": 42, "col3": null}
func ParseJSON(jsonStr string) ([]string, []interface{}, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Extract columns and values
	columns := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for col, val := range data {
		columns = append(columns, col)
		values = append(values, val)
	}

	return columns, values, nil
}
