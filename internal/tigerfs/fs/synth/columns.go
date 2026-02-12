package synth

import (
	"fmt"
	"strings"
)

// Column conventions for auto-detecting column roles.
// The first match in each list wins (order matters).
var (
	// filenameConventions lists column names recognized as the filename source.
	filenameConventions = []string{"filename", "name", "title", "slug"}

	// bodyConventions lists column names recognized as the body content source.
	bodyConventions = []string{"body", "content", "description", "text"}
)

// ColumnRoles maps database columns to their roles in synthesized file rendering.
// Roles are determined by column naming conventions or explicit mapping.
type ColumnRoles struct {
	// Filename is the column used as the file's name (e.g., "slug" → "hello-world.md").
	Filename string

	// Body is the column used as the file's main content (markdown body or plain text).
	Body string

	// Frontmatter lists columns that become YAML frontmatter key-value pairs.
	// Columns appear in the order they were defined in the database schema.
	// Only used by FormatMarkdown.
	Frontmatter []string

	// PrimaryKey is the primary key column name, used for disambiguation
	// when multiple rows share the same filename.
	PrimaryKey string
}

// DetectColumnRoles identifies which columns serve which roles for a given format.
// Columns are matched by naming conventions (first match wins per ADR-008).
//
// Parameters:
//   - columnNames: column names in schema order
//   - format: the target synthesized format
//   - pkColumn: the primary key column name (excluded from frontmatter)
//
// Returns error if required columns (filename, body) cannot be identified.
func DetectColumnRoles(columnNames []string, format SynthFormat, pkColumn string) (*ColumnRoles, error) {
	if format == FormatNative {
		return nil, fmt.Errorf("cannot detect column roles for native format")
	}

	if format == FormatTasks {
		return nil, fmt.Errorf("tasks format not yet implemented")
	}

	roles := &ColumnRoles{PrimaryKey: pkColumn}

	// Find filename column
	roles.Filename = findMatchingColumn(columnNames, filenameConventions)
	if roles.Filename == "" {
		return nil, fmt.Errorf("no filename column found (expected one of: %s)",
			strings.Join(filenameConventions, ", "))
	}

	// Find body column
	roles.Body = findMatchingColumn(columnNames, bodyConventions)
	if roles.Body == "" {
		return nil, fmt.Errorf("no body column found (expected one of: %s)",
			strings.Join(bodyConventions, ", "))
	}

	// Remaining columns become frontmatter (markdown only), preserving schema order.
	// Exclude: filename, body, and primary key columns.
	if format == FormatMarkdown {
		excluded := map[string]bool{
			strings.ToLower(roles.Filename):   true,
			strings.ToLower(roles.Body):       true,
			strings.ToLower(roles.PrimaryKey): true,
		}
		for _, col := range columnNames {
			if !excluded[strings.ToLower(col)] {
				roles.Frontmatter = append(roles.Frontmatter, col)
			}
		}
	}

	return roles, nil
}

// findMatchingColumn returns the first column name matching any convention.
// Matching is case-insensitive.
func findMatchingColumn(columnNames []string, conventions []string) string {
	for _, conv := range conventions {
		for _, col := range columnNames {
			if strings.EqualFold(col, conv) {
				return col
			}
		}
	}
	return ""
}
