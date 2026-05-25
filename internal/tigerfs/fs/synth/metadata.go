package synth

// Per-app metadata table.
//
// Each history-enabled workspace has a sibling `<app>_metadata` table that
// records non-operational metadata about the workspace: format migrations,
// schema-version markers, and future system events. It is distinct from:
//
//   - <app>_log:       user/agent operations (create, edit, rename, delete, undo)
//   - <app>_history:   per-version snapshots referenced by undo
//   - <app>_savepoint: named bookmarks
//
// The metadata table holds facts about the workspace itself, not data
// changes. The undo engine consults it to refuse undo across boundaries
// (e.g. the 0.6→0.7 format migration left history rows with lossy
// parent_id information for moved files).

// MetadataTableSuffix is the per-app table suffix for the metadata table.
// Every consumer that needs the full table name builds it as
// `appName + synth.MetadataTableSuffix`. The literal "_metadata" must not
// appear elsewhere in the codebase — derived names (e.g. the subject
// index) also compose this constant.
const MetadataTableSuffix = "_metadata"

// Subject values for the per-app metadata table.
//
// Each constant names a category of metadata entry. Subjects are
// deliberately version-agnostic so future migrations or system markers of
// the same category reuse the same constant rather than minting a new one
// per release.
const (
	// SubjectHistoryFormatMigration marks a point at which a history-enabled
	// workspace's storage format was upgraded. The undo engine treats this
	// subject as a blocker: pre-migration log entries cannot be safely undone
	// because the historical parent_id information is lossy after the
	// rewrite (history rows' parent_id is populated from the current source
	// state, not the historical state — files moved between directories
	// lose their historical location).
	SubjectHistoryFormatMigration = "history-format-migration"
)
