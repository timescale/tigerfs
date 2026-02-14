package synth

import (
	"strings"
	"time"
)

// ViewInfo holds the synthesized format configuration for a view.
type ViewInfo struct {
	// Format is the synthesized file format (Markdown, PlainText, etc.).
	Format SynthFormat

	// Roles maps columns to their roles (filename, body, frontmatter).
	Roles *ColumnRoles

	// CachedMountTime is a stable timestamp captured when the synth cache was loaded.
	// Used as the fallback file mtime for views that lack a modified_at or created_at
	// column, avoiding the instability of time.Now() on every stat call (which causes
	// editors to falsely warn "file changed since visited").
	CachedMountTime time.Time

	// SupportsHierarchy is true when the view has a filetype column, enabling
	// hierarchical directory structure with slashes in the filename column.
	SupportsHierarchy bool

	// HasHistory is true when this view has versioned history enabled.
	// Detected from view comment (tigerfs:md,history) or companion _<name>_history table.
	HasHistory bool
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
