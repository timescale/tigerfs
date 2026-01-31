package fuse

// This file centralizes all special path names, format extensions, and control
// file names used throughout the FUSE filesystem implementation. Using constants
// ensures consistency and enables single-point changes.

// Metadata directory (future: Task 4.24 will move files under .info/)
const DirInfo = ".info"

// Metadata files (currently at table level, will move under .info/ in Task 4.24)
const (
	FileCount   = ".count"   // Row count
	FileDDL     = ".ddl"     // CREATE TABLE statement
	FileSchema  = ".schema"  // Table schema (columns, types)
	FileColumns = ".columns" // Column listing
)

// Navigation capabilities
// These directories provide different ways to access rows.
const (
	DirBy     = ".by"     // Index-based navigation (.by/<column>/<value>/)
	DirFirst  = ".first"  // First N rows (.first/<N>)
	DirLast   = ".last"   // Last N rows (.last/<N>)
	DirSample = ".sample" // Random sample (.sample/<N>)
	DirAll    = ".all"    // All rows (no limit)
	DirOrder  = ".order"  // Ordered access (.order/<column>/)
)

// Bulk data capabilities
// These directories enable bulk import/export operations.
const (
	DirExport      = ".export"       // Bulk export
	DirImport      = ".import"       // Bulk import
	DirOverwrite   = ".overwrite"    // Import mode: replace all rows
	DirSync        = ".sync"         // Import mode: upsert by primary key
	DirAppend      = ".append"       // Import mode: insert only
	DirWithHeaders = ".with-headers" // Export option: include header row
	DirNoHeaders   = ".no-headers"   // Import option: no header row, use schema order
)

// Schema-level directories
// Top-level directories for schema and view management.
const (
	DirSchemas = ".schemas" // PostgreSQL schemas
	DirViews   = ".views"   // Database views
)

// DDL capabilities
// Directories for Data Definition Language operations.
const (
	DirIndexes = ".indexes" // Index management
	DirCreate  = ".create"  // Staging for CREATE operations
	DirModify  = ".modify"  // Staging for ALTER operations
	DirDelete  = ".delete"  // Staging for DROP operations
)

// Control files (DDL staging)
// Content files are visible (no dot prefix), trigger files are hidden (dot prefix).
const (
	FileSQL     = "sql"      // DDL content (content file, visible)
	FileTest    = ".test"    // Validation trigger (hidden)
	FileTestLog = "test.log" // Test results (content file, visible)
	FileCommit  = ".commit"  // Execution trigger (hidden)
	FileAbort   = ".abort"   // Abort trigger (hidden)
)

// Format extensions (with leading dot)
// Used for file extensions in format-specific access.
const (
	ExtJSON = ".json"
	ExtCSV  = ".csv"
	ExtTSV  = ".tsv"
	ExtYAML = ".yaml"
	ExtTxt  = ".txt"
	ExtBin  = ".bin"
)

// Format names (without leading dot)
// Used for directory entries and format identification.
const (
	FmtJSON = "json"
	FmtCSV  = "csv"
	FmtTSV  = "tsv"
	FmtYAML = "yaml"
)

// capabilityDirectories lists all pipeline capability directory names.
// Used to prevent these names from being interpreted as column values.
var capabilityDirectories = map[string]bool{
	DirBy:     true,
	DirFirst:  true,
	DirLast:   true,
	DirSample: true,
	DirAll:    true,
	DirOrder:  true,
	DirExport: true,
	DirImport: true,
	".filter": true, // DirFilter is defined in filter.go
}

// isCapabilityDirectory returns true if name is a reserved capability directory.
// This prevents capability names from being interpreted as column values.
func isCapabilityDirectory(name string) bool {
	return capabilityDirectories[name]
}
