package fuse

import (
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// File extension mappings based on PostgreSQL data types.
// Extensions provide immediate context about content type and enable proper
// syntax highlighting/tooling when browsing via Claude Code or other tools.

// extensionMap maps PostgreSQL data types to file extensions.
// Only types where extensions provide meaningful value are included.
var extensionMap = map[string]string{
	// Text types → .txt
	"text":              ".txt",
	"character varying": ".txt",
	"varchar":           ".txt",
	"character":         ".txt",
	"char":              ".txt",
	"bpchar":            ".txt", // blank-padded char (internal name)

	// JSON types → .json
	"json":  ".json",
	"jsonb": ".json",

	// XML → .xml
	"xml": ".xml",

	// Binary → .bin
	"bytea": ".bin",

	// PostGIS geometry types → .wkb (hex-encoded Well-Known Binary)
	"geometry":  ".wkb",
	"geography": ".wkb",
}

// GetExtensionForType returns the file extension for a PostgreSQL data type.
// Returns empty string if no extension is appropriate for the type.
//
// Parameters:
//   - dataType: PostgreSQL data type name (e.g., "text", "jsonb", "integer")
//
// Returns the extension including the dot (e.g., ".txt", ".json") or empty string.
func GetExtensionForType(dataType string) string {
	// Normalize to lowercase for comparison
	dt := strings.ToLower(dataType)

	// Check direct mapping
	if ext, ok := extensionMap[dt]; ok {
		return ext
	}

	// Handle parameterized types like "character varying(255)"
	// Extract base type before any parentheses
	if idx := strings.Index(dt, "("); idx > 0 {
		baseType := strings.TrimSpace(dt[:idx])
		if ext, ok := extensionMap[baseType]; ok {
			return ext
		}
	}

	// No extension for this type
	return ""
}

// AddExtensionToColumn returns the filename for a column with extension added.
// If the column type doesn't have a mapped extension, returns just the column name.
//
// Parameters:
//   - columnName: The database column name
//   - dataType: PostgreSQL data type for the column
//
// Returns the filename with extension (e.g., "name" + "text" → "name.txt").
func AddExtensionToColumn(columnName, dataType string) string {
	ext := GetExtensionForType(dataType)
	if ext == "" {
		return columnName
	}
	return columnName + ext
}

// StripExtension removes a known extension from a filename to get the column name.
// If the filename doesn't have a known extension, returns the filename unchanged.
//
// This function is used during Lookup to map filenames back to column names.
// It only strips extensions that are in our mapping to avoid incorrectly
// stripping user-intended extensions in column names.
//
// Parameters:
//   - filename: The filename that may have an extension
//
// Returns the column name without extension and whether an extension was stripped.
func StripExtension(filename string) (columnName string, hadExtension bool) {
	// Check each known extension
	knownExtensions := []string{".txt", ".json", ".xml", ".bin", ".wkb"}
	for _, ext := range knownExtensions {
		if strings.HasSuffix(filename, ext) {
			return strings.TrimSuffix(filename, ext), true
		}
	}
	return filename, false
}

// FindColumnByFilename finds a column matching the given filename.
// Handles both exact matches and extension-stripped matches.
//
// Parameters:
//   - columns: Slice of columns to search
//   - filename: The filename to match (may have extension)
//
// Returns the matching column and true if found, or nil and false if not found.
func FindColumnByFilename(columns []db.Column, filename string) (*db.Column, bool) {
	// First, try exact match on column name
	for i := range columns {
		if columns[i].Name == filename {
			return &columns[i], true
		}
	}

	// Try stripping extension and matching
	stripped, hadExtension := StripExtension(filename)
	if hadExtension {
		for i := range columns {
			if columns[i].Name == stripped {
				// Verify the extension is correct for this column's type
				expectedExt := GetExtensionForType(columns[i].DataType)
				actualExt := filename[len(stripped):]
				if expectedExt == actualExt {
					return &columns[i], true
				}
			}
		}
	}

	return nil, false
}
