package synth

import "strings"

// ViewInfo holds the synthesized format configuration for a view.
type ViewInfo struct {
	// Format is the synthesized file format (Markdown, PlainText, etc.).
	Format SynthFormat

	// Roles maps columns to their roles (filename, body, frontmatter).
	Roles *ColumnRoles
}

// StripExtension removes the synthesized format extension from a filename.
// Returns the base name and whether an extension was stripped.
// For example: "hello-world.md" → "hello-world", true
func StripExtension(filename string, format SynthFormat) (string, bool) {
	ext := format.Extension()
	if ext != "" && strings.HasSuffix(filename, ext) {
		return strings.TrimSuffix(filename, ext), true
	}
	return filename, false
}
