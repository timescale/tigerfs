// Package synth implements synthesized app formats for TigerFS.
//
// Synthesized apps present database views as directories of formatted files
// (markdown, plain text) instead of the native row-as-directory format.
// Format detection uses view naming conventions and column pattern matching.
//
// See ADR-008 (docs/adr/008-synthesized-apps.md) for the full specification.
package synth

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

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

// FeatureSet represents the complete set of features for a synthesized app.
// Parsed from .build/ content (e.g., "markdown,history") or view comments.
type FeatureSet struct {
	// Format is the synthesized file format (Markdown, PlainText, etc.).
	Format SynthFormat

	// History enables versioned history with a companion history table.
	History bool
}

// ParseFeatureString parses a comma-separated feature string from .build/ content.
// Examples:
//   - "markdown" → {Format: FormatMarkdown, History: false}
//   - "markdown,history" → {Format: FormatMarkdown, History: true}
//   - "history" → {Format: FormatNative, History: true} (add history to existing app)
//   - "txt,history" → {Format: FormatPlainText, History: true}
func ParseFeatureString(input string) FeatureSet {
	input = strings.TrimSpace(input)
	parts := strings.Split(input, ",")

	fs := FeatureSet{}
	for _, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		switch part {
		case "history":
			fs.History = true
		default:
			if f := ParseFormatName(part); f != FormatNative {
				fs.Format = f
			}
		}
	}
	return fs
}

// FeatureComment returns the COMMENT ON VIEW marker string for a feature set.
// Examples: "tigerfs:md", "tigerfs:md,history", "tigerfs:txt,history"
func FeatureComment(fs FeatureSet) string {
	base := FormatComment(fs.Format)
	if base == "" {
		if fs.History {
			return "tigerfs:history"
		}
		return ""
	}
	if fs.History {
		return base + ",history"
	}
	return base
}

// DetectFeaturesFromComment parses a view comment into a full FeatureSet.
// Handles both format-only comments ("tigerfs:md") and feature-enabled
// comments ("tigerfs:md,history").
func DetectFeaturesFromComment(comment string) FeatureSet {
	comment = strings.TrimSpace(comment)
	fs := FeatureSet{
		Format: DetectFormatFromComment(comment),
	}
	// Check for ",history" suffix after the format marker
	if strings.Contains(comment, ",history") {
		fs.History = true
	}
	return fs
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

// VersionIDLayout is the time format for history version IDs.
// Uses a filesystem-safe format without colons: "2006-01-02T150405Z"
const VersionIDLayout = "2006-01-02T150405Z"

// UUIDv7ToVersionID extracts the embedded timestamp from a UUIDv7 and
// formats it as a filesystem-safe version ID string.
func UUIDv7ToVersionID(id uuid.UUID) string {
	ts := extractUUIDv7Time(id)
	return ts.UTC().Format(VersionIDLayout)
}

// VersionIDToTimestamp parses a version ID string back to a time.Time.
func VersionIDToTimestamp(versionID string) (time.Time, error) {
	t, err := time.Parse(VersionIDLayout, versionID)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid version ID %q: %w", versionID, err)
	}
	return t, nil
}

// extractUUIDv7Time extracts the millisecond timestamp from a UUIDv7.
// UUIDv7 stores a Unix timestamp in milliseconds in the first 48 bits.
func extractUUIDv7Time(id uuid.UUID) time.Time {
	// First 6 bytes contain the 48-bit Unix timestamp in milliseconds
	b := id[:]
	msec := int64(binary.BigEndian.Uint16(b[0:2]))<<32 |
		int64(binary.BigEndian.Uint32(b[2:6]))
	return time.UnixMilli(msec)
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
