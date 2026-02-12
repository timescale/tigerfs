package synth

import (
	"fmt"
	"strings"
)

// GenerateSynthesizedViewSQL returns the SQL to create a synthesized view on
// an existing table. The view selects all columns from the table and is
// annotated with a TigerFS format comment.
//
// The view name is derived from the table name and format:
//   - markdown: {table}_md
//   - txt: {table}_txt
func GenerateSynthesizedViewSQL(schema, table string, format SynthFormat) (string, error) {
	viewName, err := SynthViewName(table, format)
	if err != nil {
		return "", err
	}

	viewSQL := GenerateViewSQL(schema, viewName, table)
	commentSQL := GenerateViewCommentSQL(schema, viewName, format)

	return viewSQL + ";\n" + commentSQL, nil
}

// SynthViewName returns the conventional view name for a table+format pair.
func SynthViewName(table string, format SynthFormat) (string, error) {
	switch format {
	case FormatMarkdown:
		return table + "_md", nil
	case FormatPlainText:
		return table + "_txt", nil
	case FormatTasks:
		return table + "_tasks", nil
	default:
		return "", fmt.Errorf("no view name convention for format: %s", format.String())
	}
}

// AvailableFormats returns the list of format names available for .format/.
func AvailableFormats() []string {
	return []string{"markdown", "txt"}
}

// ViewNameFromTableAndFormat returns the view name for a given table and format string.
// This is a convenience wrapper that parses the format name first.
func ViewNameFromTableAndFormat(table, formatStr string) (string, SynthFormat, error) {
	format := ParseFormatName(strings.TrimSpace(formatStr))
	if format == FormatNative || format == FormatTasks {
		return "", FormatNative, fmt.Errorf("unsupported format: %q (supported: markdown, txt)", formatStr)
	}

	viewName, err := SynthViewName(table, format)
	if err != nil {
		return "", FormatNative, err
	}

	return viewName, format, nil
}
