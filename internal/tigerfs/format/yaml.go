package format

import (
	"fmt"
	"strings"
)

// RowToYAML converts a database row to YAML format.
// Column names are used as keys, producing self-documenting output.
// NULL values are represented as YAML null.
// Output includes leading document separator (---) for proper multi-document concatenation.
// Unicode characters (including emoji) are preserved as-is, not escaped.
//
// Example output:
//
//	---
//	id: 1
//	name: Alice
//	email: alice@example.com
//	created_at: null
func RowToYAML(columns []string, values []interface{}) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count (%d) does not match value count (%d)", len(columns), len(values))
	}

	var sb strings.Builder
	sb.WriteString("---\n")

	// Handle empty row (empty mapping)
	if len(columns) == 0 {
		sb.WriteString("{}\n")
		return []byte(sb.String()), nil
	}

	for i, col := range columns {
		sb.WriteString(col)
		sb.WriteString(": ")

		if values[i] == nil {
			sb.WriteString("null")
		} else {
			str, err := ConvertValueToText(values[i])
			if err != nil {
				return nil, fmt.Errorf("failed to convert column %s: %w", col, err)
			}
			sb.WriteString(yamlScalar(str))
		}
		sb.WriteString("\n")
	}

	return []byte(sb.String()), nil
}

// yamlScalar formats a string value as a YAML scalar.
// Handles quoting for special characters while preserving Unicode as-is.
func yamlScalar(s string) string {
	if s == "" {
		return `""`
	}

	// Check if value needs quoting
	needsQuote := false

	// Values that look like YAML special values need quoting
	lower := strings.ToLower(s)
	switch lower {
	case "null", "true", "false", "yes", "no", "on", "off", "~":
		needsQuote = true
	}

	// Check for characters that require quoting
	if !needsQuote {
		for _, r := range s {
			if r == ':' || r == '#' || r == '\n' || r == '\r' || r == '\t' ||
				r == '"' || r == '\'' || r == '\\' || r == '[' || r == ']' ||
				r == '{' || r == '}' || r == ',' || r == '&' || r == '*' ||
				r == '!' || r == '|' || r == '>' || r == '%' || r == '@' || r == '`' {
				needsQuote = true
				break
			}
		}
	}

	// Check if starts/ends with whitespace
	if !needsQuote && (s[0] == ' ' || s[len(s)-1] == ' ') {
		needsQuote = true
	}

	if !needsQuote {
		return s
	}

	// Use double quotes and escape special characters
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// RowsToYAML converts multiple database rows to multi-document YAML format.
// Each row becomes a separate YAML document with "---" separator.
// Column names are used as keys, producing self-documenting output.
// NULL values are represented as YAML null.
// Empty input returns empty string.
//
// Parameters:
//   - columns: Column names in database order
//   - rows: Row values as [][]interface{}
//
// Returns multi-document YAML with "---" separators.
func RowsToYAML(columns []string, rows [][]interface{}) ([]byte, error) {
	if len(rows) == 0 {
		return []byte{}, nil
	}

	var result []byte
	for i, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("row %d: column count mismatch: %d columns, %d values", i, len(columns), len(row))
		}

		// Convert row to YAML using existing function
		data, err := RowToYAML(columns, row)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		result = append(result, data...)
	}

	return result, nil
}
