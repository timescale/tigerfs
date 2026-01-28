package util

import "strings"

// ParseRowFilename extracts the primary key value and format from a row filename
// Examples:
//   - "123" -> pkValue="123", format="tsv"
//   - "123.csv" -> pkValue="123", format="csv"
//   - "123.json" -> pkValue="123", format="json"
//   - "123.yaml" -> pkValue="123", format="yaml"
//   - "foo@example.com" -> pkValue="foo@example.com", format="tsv"
//   - "123.unknown" -> pkValue="123.unknown", format="tsv" (unrecognized extension kept as part of PK)
func ParseRowFilename(name string) (pkValue string, format string) {
	// Check for extension
	if idx := strings.LastIndex(name, "."); idx != -1 {
		ext := name[idx+1:]

		// Only strip recognized format extensions
		switch ext {
		case "csv":
			return name[:idx], "csv"
		case "json":
			return name[:idx], "json"
		case "tsv":
			return name[:idx], "tsv"
		case "yaml":
			return name[:idx], "yaml"
		}
	}

	// No recognized extension - keep full name as PK, default to TSV
	return name, "tsv"
}
