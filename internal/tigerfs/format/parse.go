package format

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseTSV parses a TSV line into columns and values
// Format: value1\tvalue2\tvalue3
// Empty values are treated as NULL
// Note: This requires column names to be known externally (schema order)
// Used for bare row writes without explicit format extension.
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

// ParseTSVWithHeader parses TSV data with a header row (two lines).
// First line is column names, second line is values.
// Returns columns from header and corresponding values.
// Empty values are treated as NULL.
// Used for .tsv format writes which support PATCH semantics.
func ParseTSVWithHeader(data string) ([]string, []interface{}, error) {
	lines := strings.Split(data, "\n")

	// Remove empty trailing lines
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return nil, nil, fmt.Errorf("empty TSV data")
	}

	if len(lines) == 1 {
		return nil, nil, fmt.Errorf("TSV data missing value row (only header found)")
	}

	if len(lines) > 2 {
		return nil, nil, fmt.Errorf("TSV single-row write expects exactly 2 lines (header + values), got %d", len(lines))
	}

	// First line is header
	columns := strings.Split(lines[0], "\t")
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "") {
		return nil, nil, fmt.Errorf("TSV header row is empty")
	}

	// Second line is values
	valueParts := strings.Split(lines[1], "\t")
	if len(valueParts) != len(columns) {
		return nil, nil, fmt.Errorf("column count mismatch: header has %d columns, values has %d", len(columns), len(valueParts))
	}

	values := make([]interface{}, len(valueParts))
	for i, part := range valueParts {
		if part == "" {
			values[i] = nil // NULL
		} else {
			values[i] = part
		}
	}

	return columns, values, nil
}

// ParseCSV parses a CSV line into columns and values
// Format: value1,value2,"quoted value, with comma"
// Empty values are treated as NULL
// Note: This requires column names to be known externally (schema order)
// Used for bare row writes without explicit format extension.
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

// ParseCSVWithHeader parses CSV data with a header row (two lines).
// First line is column names, second line is values.
// Returns columns from header and corresponding values.
// Empty values are treated as NULL.
// Used for .csv format writes which support PATCH semantics.
func ParseCSVWithHeader(data string) ([]string, []interface{}, error) {
	reader := csv.NewReader(strings.NewReader(data))
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1 // Allow variable field count to give better error

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, nil, fmt.Errorf("empty CSV data")
	}

	if len(records) == 1 {
		return nil, nil, fmt.Errorf("CSV data missing value row (only header found)")
	}

	if len(records) > 2 {
		return nil, nil, fmt.Errorf("CSV single-row write expects exactly 2 lines (header + values), got %d", len(records))
	}

	// First record is header
	columns := records[0]
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "") {
		return nil, nil, fmt.Errorf("CSV header row is empty")
	}

	// Second record is values
	valueRecord := records[1]
	if len(valueRecord) != len(columns) {
		return nil, nil, fmt.Errorf("column count mismatch: header has %d columns, values has %d", len(columns), len(valueRecord))
	}

	values := make([]interface{}, len(valueRecord))
	for i, field := range valueRecord {
		if field == "" {
			values[i] = nil // NULL
		} else {
			values[i] = field
		}
	}

	return columns, values, nil
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

// ParseYAML parses a YAML document into columns and values
// Format:
//
//	col1: value1
//	col2: 42
//	col3: null
func ParseYAML(yamlStr string) ([]string, []interface{}, error) {
	var data map[string]interface{}
	if err := yaml.Unmarshal([]byte(yamlStr), &data); err != nil {
		return nil, nil, fmt.Errorf("failed to parse YAML: %w", err)
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

// =============================================================================
// Bulk Parsing Functions
// =============================================================================
// These functions parse multiple rows from bulk data formats.
// CSV and TSV require a header row as the first line.
// JSON expects an array of objects. YAML expects multiple documents.

// ParseCSVBulk parses CSV data with a header row into columns and rows.
// First line must be column names, subsequent lines are data rows.
// Empty fields are treated as NULL.
func ParseCSVBulk(data []byte) ([]string, [][]interface{}, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty CSV data")
	}

	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.TrimLeadingSpace = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, nil, fmt.Errorf("empty CSV data")
	}

	// First row is headers
	columns := records[0]
	if len(columns) == 0 {
		return nil, nil, fmt.Errorf("CSV header row is empty")
	}

	// Remaining rows are data
	rows := make([][]interface{}, 0, len(records)-1)
	for i, record := range records[1:] {
		if len(record) != len(columns) {
			return nil, nil, fmt.Errorf("row %d: column count mismatch: expected %d, got %d", i+1, len(columns), len(record))
		}
		row := make([]interface{}, len(record))
		for j, field := range record {
			if field == "" {
				row[j] = nil // NULL
			} else {
				row[j] = field
			}
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
}

// ParseTSVBulk parses TSV data with a header row into columns and rows.
// First line must be column names, subsequent lines are data rows.
// Empty fields are treated as NULL.
func ParseTSVBulk(data []byte) ([]string, [][]interface{}, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty TSV data")
	}

	lines := strings.Split(string(data), "\n")

	// Remove empty trailing line (common after final newline)
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return nil, nil, fmt.Errorf("empty TSV data")
	}

	// First line is headers
	columns := strings.Split(lines[0], "\t")
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "") {
		return nil, nil, fmt.Errorf("TSV header row is empty")
	}

	// Remaining lines are data
	rows := make([][]interface{}, 0, len(lines)-1)
	for i, line := range lines[1:] {
		if line == "" {
			continue // Skip empty lines
		}
		fields := strings.Split(line, "\t")
		if len(fields) != len(columns) {
			return nil, nil, fmt.Errorf("row %d: column count mismatch: expected %d, got %d", i+1, len(columns), len(fields))
		}
		row := make([]interface{}, len(fields))
		for j, field := range fields {
			if field == "" {
				row[j] = nil // NULL
			} else {
				row[j] = field
			}
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
}

// ParseJSONBulk parses a JSON array of objects into columns and rows.
// All objects must have the same keys (column names).
// Null values are preserved.
func ParseJSONBulk(data []byte) ([]string, [][]interface{}, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty JSON data")
	}

	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(records) == 0 {
		return []string{}, [][]interface{}{}, nil
	}

	// Get columns from first record
	columns := make([]string, 0, len(records[0]))
	for col := range records[0] {
		columns = append(columns, col)
	}

	// Sort columns for consistent ordering
	// (Go maps don't preserve order, but we need deterministic column order)
	sortStrings(columns)

	// Build rows
	rows := make([][]interface{}, len(records))
	for i, record := range records {
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			row[j] = record[col] // nil if key missing
		}
		rows[i] = row
	}

	return columns, rows, nil
}

// ParseYAMLBulk parses multi-document YAML into columns and rows.
// Each document (separated by ---) represents one row.
// All documents must have the same keys (column names).
func ParseYAMLBulk(data []byte) ([]string, [][]interface{}, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty YAML data")
	}

	// Split by document separator
	docs := splitYAMLDocuments(string(data))
	if len(docs) == 0 {
		return []string{}, [][]interface{}{}, nil
	}

	var columns []string
	rows := make([][]interface{}, 0, len(docs))

	for i, doc := range docs {
		if strings.TrimSpace(doc) == "" {
			continue
		}

		var record map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &record); err != nil {
			return nil, nil, fmt.Errorf("document %d: failed to parse YAML: %w", i+1, err)
		}

		if record == nil {
			continue // Empty document
		}

		// Get columns from first non-empty document
		if columns == nil {
			columns = make([]string, 0, len(record))
			for col := range record {
				columns = append(columns, col)
			}
			sortStrings(columns)
		}

		// Build row
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			row[j] = record[col] // nil if key missing
		}
		rows = append(rows, row)
	}

	if columns == nil {
		columns = []string{}
	}

	return columns, rows, nil
}

// splitYAMLDocuments splits YAML content by document separators (---).
func splitYAMLDocuments(content string) []string {
	var docs []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "---" {
			if current.Len() > 0 {
				docs = append(docs, current.String())
				current.Reset()
			}
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}

	// Don't forget the last document
	if current.Len() > 0 {
		docs = append(docs, current.String())
	}

	return docs
}

// sortStrings sorts a slice of strings in place (simple insertion sort for small slices).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// =============================================================================
// No-Headers Bulk Parsing Functions
// =============================================================================
// These functions parse CSV/TSV data without a header row.
// Column names must be provided externally (from schema).

// ParseCSVBulkNoHeaders parses CSV data without a header row.
// All rows are data rows, columns are provided by caller.
// Empty fields are treated as NULL.
func ParseCSVBulkNoHeaders(data []byte, columns []string) ([]string, [][]interface{}, error) {
	if len(data) == 0 {
		return columns, [][]interface{}{}, nil
	}

	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.TrimLeadingSpace = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return columns, [][]interface{}{}, nil
	}

	// All rows are data (no header row)
	rows := make([][]interface{}, 0, len(records))
	for i, record := range records {
		if len(record) != len(columns) {
			return nil, nil, fmt.Errorf("row %d: column count mismatch: expected %d, got %d", i+1, len(columns), len(record))
		}
		row := make([]interface{}, len(record))
		for j, field := range record {
			if field == "" {
				row[j] = nil // NULL
			} else {
				row[j] = field
			}
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
}

// ParseTSVBulkNoHeaders parses TSV data without a header row.
// All rows are data rows, columns are provided by caller.
// Empty fields are treated as NULL.
func ParseTSVBulkNoHeaders(data []byte, columns []string) ([]string, [][]interface{}, error) {
	if len(data) == 0 {
		return columns, [][]interface{}{}, nil
	}

	lines := strings.Split(string(data), "\n")

	// Remove empty trailing line (common after final newline)
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return columns, [][]interface{}{}, nil
	}

	// All lines are data (no header row)
	rows := make([][]interface{}, 0, len(lines))
	for i, line := range lines {
		if line == "" {
			continue // Skip empty lines
		}
		fields := strings.Split(line, "\t")
		if len(fields) != len(columns) {
			return nil, nil, fmt.Errorf("row %d: column count mismatch: expected %d, got %d", i+1, len(columns), len(fields))
		}
		row := make([]interface{}, len(fields))
		for j, field := range fields {
			if field == "" {
				row[j] = nil // NULL
			} else {
				row[j] = field
			}
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
}
