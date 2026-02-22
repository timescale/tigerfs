package synth

import "strings"

// SynthesizePlainText generates a plain text file from a database row.
// Returns only the body content — no frontmatter, no metadata.
func SynthesizePlainText(columns []string, values []interface{}, roles *ColumnRoles) ([]byte, error) {
	colMap := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		colMap[col] = values[i]
	}

	body := ValueToString(colMap[roles.Body])
	if body != "" && !strings.HasSuffix(body, "\n") {
		body += "\n"
	}

	return []byte(body), nil
}

// ParsePlainText parses a plain text file. The entire content becomes the body.
func ParsePlainText(content []byte) string {
	return string(content)
}

// GetPlainTextFilename generates the display filename for a row in a plain text view.
// Uses the filename column value, falling back to the primary key if NULL/empty.
// Returns the filename as-is — no extension is auto-appended.
func GetPlainTextFilename(columns []string, values []interface{}, roles *ColumnRoles) string {
	colMap := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		colMap[col] = values[i]
	}

	name := ValueToString(colMap[roles.Filename])
	if name == "" {
		name = ValueToString(colMap[roles.PrimaryKey])
		if name == "" {
			name = "untitled"
		}
	}

	name = sanitizeFilename(name)

	return name
}
