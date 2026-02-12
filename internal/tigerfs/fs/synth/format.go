// Package synth implements synthesized app formats for TigerFS.
//
// Synthesized apps present database views as directories of formatted files
// (markdown, plain text) instead of the native row-as-directory format.
// Format detection uses view naming conventions and column pattern matching.
//
// See ADR-008 (docs/adr/008-synthesized-apps.md) for the full specification.
package synth

import "strings"

// SynthFormat represents a synthesized file format.
type SynthFormat int

const (
	// FormatNative means no synthesized format — use standard row/column layout.
	FormatNative SynthFormat = iota

	// FormatMarkdown renders rows as .md files with YAML frontmatter.
	FormatMarkdown

	// FormatPlainText renders rows as .txt files (body only, no frontmatter).
	FormatPlainText

	// FormatTasks renders rows as numbered task .md files (Phase 6.2).
	FormatTasks
)

// String returns the human-readable format name.
func (f SynthFormat) String() string {
	switch f {
	case FormatMarkdown:
		return "markdown"
	case FormatPlainText:
		return "txt"
	case FormatTasks:
		return "tasks"
	default:
		return "native"
	}
}

// Extension returns the file extension for this format (with leading dot).
func (f SynthFormat) Extension() string {
	switch f {
	case FormatMarkdown:
		return ".md"
	case FormatPlainText:
		return ".txt"
	case FormatTasks:
		return ".md"
	default:
		return ""
	}
}

// ParseFormatName converts a format name string to SynthFormat.
// Returns FormatNative if the name is unrecognized.
func ParseFormatName(name string) SynthFormat {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "markdown", "md":
		return FormatMarkdown
	case "txt", "text", "plaintext", "plain":
		return FormatPlainText
	case "tasks", "todo", "items":
		return FormatTasks
	default:
		return FormatNative
	}
}

// viewSuffixes maps view name suffixes to their synthesized format.
// Suffix takes priority over column pattern detection (per ADR-008).
var viewSuffixes = map[string]SynthFormat{
	"_md":    FormatMarkdown,
	"_txt":   FormatPlainText,
	"_tasks": FormatTasks,
	"_todo":  FormatTasks,
	"_items": FormatTasks,
}

// DetectFormat determines the synthesized format for a view based on its name
// and column structure. Naming conventions take priority over column patterns.
//
// Detection precedence (per ADR-008):
//  1. View name suffix: _md → Markdown, _txt → PlainText, _tasks/_todo/_items → Tasks
//  2. Column patterns: filename+body+others → Markdown, filename+body only → PlainText,
//     number+name+status+body → Tasks
//  3. Default: FormatNative
func DetectFormat(viewName string, columnNames []string) SynthFormat {
	// Check suffix first (highest priority)
	if f := detectFormatFromSuffix(viewName); f != FormatNative {
		return f
	}

	// Fall back to column pattern detection
	return detectFormatFromColumns(columnNames)
}

// DetectFormatFromComment parses a PostgreSQL view comment for a TigerFS
// format marker. Comments are set by .build/ and .format/ operations.
//
// Recognized markers: "tigerfs:md", "tigerfs:txt", "tigerfs:tasks"
func DetectFormatFromComment(comment string) SynthFormat {
	comment = strings.TrimSpace(comment)
	switch {
	case strings.HasPrefix(comment, "tigerfs:md"):
		return FormatMarkdown
	case strings.HasPrefix(comment, "tigerfs:txt"):
		return FormatPlainText
	case strings.HasPrefix(comment, "tigerfs:tasks"):
		return FormatTasks
	default:
		return FormatNative
	}
}

// FormatComment returns the COMMENT ON VIEW marker string for a format.
func FormatComment(f SynthFormat) string {
	switch f {
	case FormatMarkdown:
		return "tigerfs:md"
	case FormatPlainText:
		return "tigerfs:txt"
	case FormatTasks:
		return "tigerfs:tasks"
	default:
		return ""
	}
}

// detectFormatFromSuffix checks if the view name ends with a known format suffix.
func detectFormatFromSuffix(viewName string) SynthFormat {
	lower := strings.ToLower(viewName)
	for suffix, format := range viewSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return format
		}
	}
	return FormatNative
}

// detectFormatFromColumns infers the format from column name patterns.
func detectFormatFromColumns(columnNames []string) SynthFormat {
	nameSet := make(map[string]bool, len(columnNames))
	for _, name := range columnNames {
		nameSet[strings.ToLower(name)] = true
	}

	hasFilename := hasAnyColumn(nameSet, filenameConventions)
	hasBody := hasAnyColumn(nameSet, bodyConventions)
	hasNumber := nameSet["number"]
	hasName := nameSet["name"]
	hasStatus := nameSet["status"]

	// Tasks: number + name + status + body
	if hasNumber && hasName && hasStatus && hasBody {
		return FormatTasks
	}

	// Markdown: filename + body + at least one other column
	if hasFilename && hasBody && len(columnNames) > 2 {
		return FormatMarkdown
	}

	// PlainText: filename + body only
	if hasFilename && hasBody && len(columnNames) == 2 {
		return FormatPlainText
	}

	return FormatNative
}

// hasAnyColumn returns true if any of the convention names exist in the column set.
func hasAnyColumn(nameSet map[string]bool, conventions []string) bool {
	for _, name := range conventions {
		if nameSet[name] {
			return true
		}
	}
	return false
}
