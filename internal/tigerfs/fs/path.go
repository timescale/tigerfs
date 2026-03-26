package fs

import (
	"fmt"
	"strconv"
	"strings"
)

// PathType indicates what kind of filesystem path was parsed.
type PathType int

// Path types for different filesystem locations.
const (
	// PathRoot is the filesystem root directory.
	PathRoot PathType = iota

	// PathSchemaList is the /.schemas/ directory listing all schemas.
	PathSchemaList

	// PathSchema is a specific schema directory (/.schemas/<name>/).
	PathSchema

	// PathTable is a table directory (/table/ or /schema/table/).
	PathTable

	// PathCapability is a capability directory (.by/, .filter/, .order/, etc.)
	// that hasn't yet resolved to a specific result.
	PathCapability

	// PathRow is a specific row file (/table/<pk> or /table/<pk>.json).
	PathRow

	// PathColumn is a specific column within a row (/table/<pk>/<column>).
	PathColumn

	// PathInfo is the .info/ metadata directory or a file within it.
	PathInfo

	// PathExport is the .export/ directory or a file within it.
	PathExport

	// PathImport is the .import/ directory or subdirectory.
	PathImport

	// PathDDL is a DDL staging directory (/.create/, /.modify/, /.delete/).
	PathDDL

	// PathViewList is the /.views/ directory (only lists .create for view creation).
	PathViewList

	// PathBuild is the /.build/ directory for creating synthesized apps.
	// Writing a format name to /.build/<name> creates a backing table + view.
	PathBuild

	// PathFormat is the /{table}/.format/ directory for configuring synthesized views.
	// Writing a format name creates a synthesized view on the existing table.
	PathFormat

	// PathHistory is the /{table}/.history/ read-only directory for versioned history.
	// Shows past versions of synth app files captured by PostgreSQL triggers.
	PathHistory

	// PathTablesList is the /.tables/ directory listing backing tables in tigerfs schema.
	PathTablesList
)

// ParsedPath holds the result of parsing a filesystem path.
// It contains the FSContext for query state plus additional fields
// for row-level, column-level, and DDL operations.
type ParsedPath struct {
	// Type indicates what kind of path this is.
	Type PathType

	// Context holds the accumulated query state (table, filters, order, limits).
	Context *FSContext

	// PrimaryKey is set when Type is PathRow or PathColumn.
	PrimaryKey string

	// Column is set when Type is PathColumn.
	Column string

	// Format is the file format (json, csv, tsv, yaml) if specified via extension.
	Format string

	// InfoFile is the metadata file name when Type is PathInfo (e.g., ".count").
	InfoFile string

	// ImportMode is the import mode when Type is PathImport (sync, overwrite, append).
	ImportMode string

	// ImportNoHeaders is set when importing without headers (.import/{mode}/.no-headers/).
	ImportNoHeaders bool

	// DDLOp is the DDL operation when Type is PathDDL (create, modify, delete).
	DDLOp string

	// DDLName is the target name for DDL operations.
	DDLName string

	// DDLFile is the DDL file being accessed (sql, .test, .commit, .abort, test.log).
	DDLFile string

	// DDLObjectType is the type of object for DDL operations (table, index, schema, view).
	DDLObjectType string

	// DDLParentTable is the parent table for index DDL operations.
	DDLParentTable string

	// CapabilityDir is set when Type is PathCapability, indicating which
	// capability directory we're in (.by, .filter, .order, etc.).
	CapabilityDir string

	// CapabilityArg is the argument within a capability (column name for .by/).
	CapabilityArg string

	// ExportWithHeaders is set when exporting with headers (.export/.with-headers/).
	ExportWithHeaders bool

	// BuildName is the target app name when Type is PathBuild (e.g., "posts").
	BuildName string

	// FormatTarget is the target format name when Type is PathFormat (e.g., "markdown").
	FormatTarget string

	// HistoryFile is the filename within .history/ (e.g., "foo.md" in .history/foo.md/).
	HistoryFile string

	// HistoryVersionID is the version ID (timestamp) for a specific history entry.
	HistoryVersionID string

	// HistoryByID indicates we're navigating via .history/.by/<uuid>/ path.
	HistoryByID bool

	// HistoryRowID is the row UUID when navigating via .history/.by/<uuid>/.
	HistoryRowID string

	// RawSubPath captures all path segments after the table, before any PK/Column
	// processing. Used by synth hierarchy to reconstruct multi-segment filenames.
	// For example, /memory/projects/web/todo.md → ["projects", "web", "todo.md"].
	RawSubPath []string
}

// knownFormats maps format extensions to format names.
var knownFormats = map[string]string{
	".json": "json",
	".csv":  "csv",
	".tsv":  "tsv",
	".yaml": "yaml",
}

// ParsePath converts a filesystem path to a ParsedPath.
//
// Examples:
//
//	"/" → PathRoot
//	"/.schemas" → PathSchemaList
//	"/.schemas/myschema" → PathSchema
//	"/users" → PathTable (schema=public)
//	"/public/users" → PathTable (explicit schema)
//	"/users/123" → PathRow
//	"/users/123.json" → PathRow with format
//	"/users/123/name" → PathColumn
//	"/users/.info/.count" → PathInfo
//	"/users/.by/email" → PathCapability
//	"/users/.by/email/foo" → PathTable with filter
//	"/users/.first/10" → PathTable with limit
//	"/users/.export/all.csv" → PathExport
//	"/.create/myindex/sql" → PathDDL
func ParsePath(path string) (*ParsedPath, *FSError) {
	// Validate path
	if path == "" {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "empty path",
		}
	}
	if !strings.HasPrefix(path, "/") {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("path must start with /: %s", path),
		}
	}

	// Handle root
	if path == "/" {
		return &ParsedPath{Type: PathRoot}, nil
	}

	// Split path into segments, removing empty segments
	segments := splitPath(path)
	if len(segments) == 0 {
		return &ParsedPath{Type: PathRoot}, nil
	}

	// Check for special top-level directories
	first := segments[0]
	switch first {
	case ".schemas":
		return parseSchemaPath(segments)
	case ".create":
		// Only .create is at root level; .modify and .delete are inside tables
		return parseDDLPath(segments)
	case ".views":
		return parseViewPath(segments)
	case ".build":
		return parseBuildPath(segments)
	case ".tables":
		return parseTablesPath(segments)
	}

	// Otherwise, it's a table path (possibly with schema prefix)
	return parseTablePath(segments)
}

// splitPath splits a path into non-empty segments.
func splitPath(path string) []string {
	parts := strings.Split(path, "/")
	segments := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			segments = append(segments, p)
		}
	}
	return segments
}

// parseSchemaPath handles /.schemas/ paths.
// Supports:
//   - /.schemas/ → list schemas (plus .create)
//   - /.schemas/.create/{name}/ → create schema DDL
//   - /.schemas/<schema> → list tables in schema
//   - /.schemas/<schema>/.delete/ → delete schema DDL
//   - /.schemas/<schema>/.build/{name} → create synthesized app in schema
//   - /.schemas/<schema>/<table> → table path
//   - /.schemas/<schema>/<table>/<row> → row path
func parseSchemaPath(segments []string) (*ParsedPath, *FSError) {
	if len(segments) == 1 {
		// /.schemas/
		return &ParsedPath{Type: PathSchemaList}, nil
	}

	schema := segments[1]

	// Handle /.schemas/.create/{name}/
	if schema == ".create" {
		result := &ParsedPath{
			Type:          PathDDL,
			DDLOp:         "create",
			DDLObjectType: "schema",
		}
		if len(segments) >= 3 {
			result.DDLName = segments[2]
			if len(segments) >= 4 {
				result.DDLFile = segments[3]
			}
		}
		return result, nil
	}

	if len(segments) == 2 {
		// /.schemas/<name>/ - list tables in schema
		return &ParsedPath{
			Type: PathSchema,
			Context: &FSContext{
				Schema: schema,
			},
		}, nil
	}

	// Handle /.schemas/{schema}/.delete/
	if segments[2] == ".delete" {
		result := &ParsedPath{
			Type:          PathDDL,
			DDLOp:         "delete",
			DDLName:       schema,
			DDLObjectType: "schema",
		}
		if len(segments) >= 4 {
			result.DDLFile = segments[3]
		}
		return result, nil
	}

	// Handle /.schemas/{schema}/.build/{name}
	if segments[2] == ".build" {
		result := &ParsedPath{
			Type: PathBuild,
			Context: &FSContext{
				Schema: schema,
			},
		}
		if len(segments) >= 4 {
			result.BuildName = segments[3]
		}
		return result, nil
	}

	// /.schemas/<schema>/<table>/... - parse as table path with explicit schema
	table := segments[2]
	ctx := NewFSContext(schema, table, "")

	result := &ParsedPath{
		Type:    PathTable,
		Context: ctx,
	}

	// Process remaining segments (rows, columns, capabilities)
	return processSegments(result, segments[3:])
}

// parseDDLPath handles /.create/ paths at root level.
// Note: .modify and .delete are now handled at table level via processTableDDL.
func parseDDLPath(segments []string) (*ParsedPath, *FSError) {
	op := strings.TrimPrefix(segments[0], ".")

	if len(segments) == 1 {
		// Just /.create/ - list staging directories
		return &ParsedPath{
			Type:          PathDDL,
			DDLOp:         op,
			DDLObjectType: "table",
		}, nil
	}

	result := &ParsedPath{
		Type:          PathDDL,
		DDLOp:         op,
		DDLName:       segments[1],
		DDLObjectType: "table",
	}

	if len(segments) >= 3 {
		result.DDLFile = segments[2]
	}

	return result, nil
}

// parseViewPath handles /.views/ paths.
// Supports:
//   - /.views/ → list .create only (views themselves appear at root)
//   - /.views/.create/{name}/ → create view DDL
//
// Note: View delete uses /{view}/.delete/ (same as tables) since views appear
// at root level. View modify is NOT supported per spec.
func parseViewPath(segments []string) (*ParsedPath, *FSError) {
	if len(segments) == 1 {
		// /.views/ - list .create only
		return &ParsedPath{Type: PathViewList}, nil
	}

	next := segments[1]

	// Handle /.views/.create/{name}/
	if next == ".create" {
		result := &ParsedPath{
			Type:          PathDDL,
			DDLOp:         "create",
			DDLObjectType: "view",
		}
		if len(segments) >= 3 {
			result.DDLName = segments[2]
			if len(segments) >= 4 {
				result.DDLFile = segments[3]
			}
		}
		return result, nil
	}

	// /.views/{name} is INVALID - views are accessed from root directory
	return nil, &FSError{
		Code:    ErrInvalidPath,
		Message: "views are accessed from root directory, not from .views/",
		Hint:    fmt.Sprintf("use /%s instead of /.views/%s", next, next),
	}
}

// parseBuildPath handles /.build/ paths for creating synthesized apps.
// Supports:
//   - /.build/ → list nothing (write-only directory)
//   - /.build/<name> → writable file; writing a format name creates the app
func parseBuildPath(segments []string) (*ParsedPath, *FSError) {
	result := &ParsedPath{Type: PathBuild}

	if len(segments) >= 2 {
		result.BuildName = segments[1]
	}

	return result, nil
}

// parseTablesPath handles /.tables/ paths.
// Maps /.tables/<name>/... to table path with schema=tigerfs.
// Supports:
//   - /.tables/ -> list tables in tigerfs schema (PathTablesList)
//   - /.tables/<name>/... -> table path with schema="tigerfs"
func parseTablesPath(segments []string) (*ParsedPath, *FSError) {
	if len(segments) == 1 {
		return &ParsedPath{Type: PathTablesList}, nil
	}

	// /.tables/<table>/... - parse as table path with tigerfs schema
	table := segments[1]
	ctx := NewFSContext("tigerfs", table, "")

	result := &ParsedPath{
		Type:    PathTable,
		Context: ctx,
	}

	return processSegments(result, segments[2:])
}

// parseTablePath handles table paths and capability chains.
// Root-level paths always use public schema: /table or /table/row.
// For explicit schema access, use /.schemas/schemaname/table.
func parseTablePath(segments []string) (*ParsedPath, *FSError) {
	// Use empty schema for root-level table paths (e.g., /users, /products).
	// Empty means "resolve at runtime via current_schema()" — the Operations layer
	// fills this in from the database connection's search_path.
	// Explicit schema paths (/.schemas/myschema/table) bypass this function entirely
	// and set the schema directly in parseSchemaPath.
	schema := ""
	table := segments[0]
	startIdx := 1

	// Create initial context
	ctx := NewFSContext(schema, table, "")

	// Process remaining segments
	result := &ParsedPath{
		Type:    PathTable,
		Context: ctx,
	}

	return processSegments(result, segments[startIdx:])
}

// processSegments processes path segments after table identification.
// Supports capability directories appearing after non-dot directory segments,
// enabling paths like /docs/getting-started/.history/ in hierarchical synth views.
func processSegments(result *ParsedPath, segments []string) (*ParsedPath, *FSError) {
	i := 0
	for i < len(segments) {
		seg := segments[i]

		// Check for capability directories
		if strings.HasPrefix(seg, ".") {
			consumed, err := processCapability(result, seg, segments[i:])
			if err != nil {
				return nil, err
			}
			i += consumed
			continue
		}

		// Scan ahead: is there a known capability later in the path?
		// This handles hierarchical paths like getting-started/.history/foo.md
		// where non-dot segments precede a capability directory.
		capIdx := -1
		for j := i + 1; j < len(segments); j++ {
			if strings.HasPrefix(segments[j], ".") && isKnownCapability(segments[j]) {
				capIdx = j
				break
			}
		}

		if capIdx >= 0 {
			// Consume all segments before capability as directory path
			dirParts := segments[i:capIdx]
			result.PrimaryKey = strings.Join(dirParts, "/")
			result.Type = PathRow
			result.RawSubPath = append(result.RawSubPath, dirParts...)
			i = capIdx
			continue
		}

		// No capability ahead — terminal: process as row/column (existing behavior)
		return processRowOrColumn(result, segments[i:])
	}

	return result, nil
}

// isKnownCapability returns true if a dot-prefixed segment is a known capability
// directory handled by processCapability. Used by scan-ahead to distinguish
// capability segments from unknown dot-files.
func isKnownCapability(seg string) bool {
	switch seg {
	case DirInfo, DirBy, DirColumns, DirFilter, DirOrder, DirFirst, DirLast, DirSample,
		DirExport, DirImport, DirAll, DirModify, DirDelete, DirIndexes,
		DirFormat, DirHistory:
		return true
	default:
		return false
	}
}

// processCapability handles capability directories (.by/, .filter/, etc.)
// Returns the number of segments consumed.
func processCapability(result *ParsedPath, cap string, remaining []string) (int, *FSError) {
	switch cap {
	case DirInfo:
		return processInfo(result, remaining)
	case DirBy:
		return processBy(result, remaining)
	case DirColumns:
		return processColumns(result, remaining)
	case DirFilter:
		return processFilter(result, remaining)
	case DirOrder:
		return processOrder(result, remaining)
	case DirFirst:
		return processLimit(result, remaining, LimitFirst)
	case DirLast:
		return processLimit(result, remaining, LimitLast)
	case DirSample:
		return processLimit(result, remaining, LimitSample)
	case DirExport:
		return processExport(result, remaining)
	case DirImport:
		return processImport(result, remaining)
	case DirAll:
		// .all/ just continues to table listing
		return 1, nil
	case DirModify:
		return processTableDDL(result, remaining, "modify")
	case DirDelete:
		return processTableDDL(result, remaining, "delete")
	case DirIndexes:
		return processIndexes(result, remaining)
	case DirFormat:
		return processFormat(result, remaining)
	case DirHistory:
		return processHistory(result, remaining)
	default:
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown capability: %s", cap),
		}
	}
}

// processInfo handles .info/ paths.
func processInfo(result *ParsedPath, remaining []string) (int, *FSError) {
	result.Type = PathInfo
	if len(remaining) >= 2 {
		result.InfoFile = remaining[1]
		return 2, nil
	}
	return 1, nil
}

// processBy handles .by/ paths.
func processBy(result *ParsedPath, remaining []string) (int, *FSError) {
	// Check if filtering is allowed (filters are disallowed after .order/)
	if !result.Context.CanAddFilter() {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: "cannot add .by/ after .order/",
			Hint:    "filters must come before .order/ in the path",
		}
	}

	if len(remaining) == 1 {
		// Just .by/ - list indexed columns
		result.Type = PathCapability
		result.CapabilityDir = DirBy
		return 1, nil
	}

	if len(remaining) == 2 {
		// .by/<column> - list values for column
		result.Type = PathCapability
		result.CapabilityDir = DirBy
		result.CapabilityArg = remaining[1]
		return 2, nil
	}

	// .by/<column>/<value> - add filter
	column := remaining[1]
	value := remaining[2]
	result.Context = result.Context.WithFilter(column, value, true)
	result.Type = PathTable
	return 3, nil
}

// processColumns handles .columns/ paths.
// Supports:
//   - .columns/ → PathCapability (list available columns)
//   - .columns/col1,col2,col3 → PathTable with column projection applied
func processColumns(result *ParsedPath, remaining []string) (int, *FSError) {
	if !result.Context.CanAddColumns() {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: "cannot add .columns/ here",
			Hint:    ".columns/ is not allowed after .export/ or another .columns/",
		}
	}

	if len(remaining) == 1 {
		// Just .columns/ - list available columns
		result.Type = PathCapability
		result.CapabilityDir = DirColumns
		return 1, nil
	}

	// .columns/col1,col2,col3
	colArg := remaining[1]
	columns := strings.Split(colArg, ",")

	// Validate no empty column names
	for _, col := range columns {
		if col == "" {
			return 0, &FSError{
				Code:    ErrInvalidPath,
				Message: "empty column name in .columns/ list",
				Hint:    "column names must not be empty (check for consecutive commas)",
			}
		}
	}

	result.Context = result.Context.WithColumns(columns)
	result.Type = PathTable
	return 2, nil
}

// processFilter handles .filter/ paths.
func processFilter(result *ParsedPath, remaining []string) (int, *FSError) {
	// Check if filtering is allowed (filters are disallowed after .order/)
	if !result.Context.CanAddFilter() {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: "cannot add .filter/ after .order/",
			Hint:    "filters must come before .order/ in the path",
		}
	}

	if len(remaining) == 1 {
		result.Type = PathCapability
		result.CapabilityDir = DirFilter
		return 1, nil
	}

	if len(remaining) == 2 {
		result.Type = PathCapability
		result.CapabilityDir = DirFilter
		result.CapabilityArg = remaining[1]
		return 2, nil
	}

	// .filter/<column>/<value>
	column := remaining[1]
	value := remaining[2]
	result.Context = result.Context.WithFilter(column, value, false)
	result.Type = PathTable
	return 3, nil
}

// processOrder handles .order/ paths.
func processOrder(result *ParsedPath, remaining []string) (int, *FSError) {
	if len(remaining) == 1 {
		// Only show .order/ capability if it can be used
		if !result.Context.CanAddOrder() {
			return 0, &FSError{
				Code:    ErrInvalidPath,
				Message: "cannot add .order/ after previous .order/",
				Hint:    "only one .order/ is allowed per path",
			}
		}
		result.Type = PathCapability
		result.CapabilityDir = DirOrder
		return 1, nil
	}

	// Check if ordering is allowed before applying
	if !result.Context.CanAddOrder() {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: "cannot add .order/ after previous .order/",
			Hint:    "only one .order/ is allowed per path",
		}
	}

	// .order/<column> or .order/<column>.desc
	col := remaining[1]
	desc := false
	if strings.HasSuffix(col, ".desc") {
		col = strings.TrimSuffix(col, ".desc")
		desc = true
	}
	result.Context = result.Context.WithOrder(col, desc)
	result.Type = PathTable
	return 2, nil
}

// processLimit handles .first/, .last/, .sample/ paths.
func processLimit(result *ParsedPath, remaining []string, limitType LimitType) (int, *FSError) {
	// Check if this limit type is allowed
	if !result.Context.CanAddLimit(limitType) {
		var hint string
		switch {
		case result.Context.LimitType == LimitSample:
			hint = "no limits allowed after .sample/ - just sample fewer rows"
		case result.Context.LimitType == LimitFirst && limitType == LimitFirst:
			hint = ".first/.first is redundant - use a single .first/ with smaller value"
		case result.Context.LimitType == LimitLast && limitType == LimitLast:
			hint = ".last/.last is redundant - use a single .last/ with smaller value"
		default:
			hint = "this limit combination is not allowed"
		}
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot add %s after %s", remaining[0], limitTypeToDir(result.Context.LimitType)),
			Hint:    hint,
		}
	}

	if len(remaining) < 2 {
		result.Type = PathCapability
		result.CapabilityDir = remaining[0]
		return 1, nil
	}

	n, err := strconv.Atoi(remaining[1])
	if err != nil {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("invalid limit value: %s", remaining[1]),
			Hint:    "limit must be a positive integer",
		}
	}
	if n < 0 {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("negative limit: %d", n),
			Hint:    "limit must be a positive integer",
		}
	}

	result.Context = result.Context.WithLimit(n, limitType)
	result.Type = PathTable
	return 2, nil
}

// limitTypeToDir converts a LimitType back to its directory name.
func limitTypeToDir(lt LimitType) string {
	switch lt {
	case LimitFirst:
		return ".first"
	case LimitLast:
		return ".last"
	case LimitSample:
		return ".sample"
	default:
		return "unknown"
	}
}

// processExport handles .export/ paths.
func processExport(result *ParsedPath, remaining []string) (int, *FSError) {
	result.Type = PathExport
	result.Context = result.Context.WithTerminal()

	if len(remaining) >= 2 {
		// .export/<format> or .export/.with-headers/<format>
		filename := remaining[1]

		// Handle .with-headers subdirectory
		if filename == ".with-headers" {
			result.ExportWithHeaders = true
			if len(remaining) >= 3 {
				// .export/.with-headers/<format>
				filename = remaining[2]
				result.Format = extractFormatName(filename)
				return 3, nil
			}
			return 2, nil
		}

		result.Format = extractFormatName(filename)
		return 2, nil
	}
	return 1, nil
}

// extractFormatName extracts format from a filename.
// Handles both extension-based (data.csv) and direct format names (csv).
func extractFormatName(filename string) string {
	// First try extension-based extraction
	if fmt := extractFormat(filename); fmt != "" {
		return fmt
	}
	// Then try direct format name (for FUSE-style .export/csv)
	switch filename {
	case "csv", "tsv", "json", "yaml":
		return filename
	}
	return ""
}

// processImport handles .import/ paths.
// Supports: .import, .import/{mode}, .import/{mode}/{format}, .import/{mode}/.no-headers/{format}
func processImport(result *ParsedPath, remaining []string) (int, *FSError) {
	result.Type = PathImport

	if len(remaining) >= 2 {
		mode := remaining[1]
		switch mode {
		case DirSync:
			result.ImportMode = "sync"
		case DirOverwrite:
			result.ImportMode = "overwrite"
		case DirAppend:
			result.ImportMode = "append"
		default:
			result.ImportMode = strings.TrimPrefix(mode, ".")
		}

		// Check for .no-headers option or format
		if len(remaining) >= 3 {
			next := remaining[2]

			// Handle .no-headers option
			if next == DirNoHeaders {
				result.ImportNoHeaders = true
				// Check for format after .no-headers
				if len(remaining) >= 4 {
					result.Format = extractFormatName(remaining[3])
					return 4, nil
				}
				return 3, nil
			}

			// Direct format (e.g., csv or data.csv)
			result.Format = extractFormatName(next)
			return 3, nil
		}
		return 2, nil
	}
	return 1, nil
}

// processIndexes handles index paths (/{table}/.indexes/...).
// Supports:
//   - /{table}/.indexes/ - list indexes
//   - /{table}/.indexes/.create/{name}/ - create index DDL
//   - /{table}/.indexes/{name}/ - index info
//   - /{table}/.indexes/{name}/.delete/ - delete index DDL
//
// Returns the number of segments consumed.
func processIndexes(result *ParsedPath, remaining []string) (int, *FSError) {
	fsCtx := result.Context
	if fsCtx == nil {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: ".indexes/ requires a table context",
		}
	}

	if len(remaining) == 1 {
		// /{table}/.indexes/ - list indexes
		result.Type = PathCapability
		result.CapabilityDir = DirIndexes
		return 1, nil
	}

	next := remaining[1]

	// Handle /{table}/.indexes/.create/{name}/
	if next == ".create" {
		result.Type = PathDDL
		result.DDLOp = "create"
		result.DDLObjectType = "index"
		result.DDLParentTable = fsCtx.TableName
		if len(remaining) >= 3 {
			result.DDLName = remaining[2]
			if len(remaining) >= 4 {
				result.DDLFile = remaining[3]
				return 4, nil
			}
			return 3, nil
		}
		return 2, nil
	}

	// /{table}/.indexes/{name}/ or /{table}/.indexes/{name}/.delete/
	indexName := next

	if len(remaining) >= 3 && remaining[2] == ".delete" {
		// /{table}/.indexes/{name}/.delete/
		result.Type = PathDDL
		result.DDLOp = "delete"
		result.DDLName = indexName
		result.DDLObjectType = "index"
		result.DDLParentTable = fsCtx.TableName
		if len(remaining) >= 4 {
			result.DDLFile = remaining[3]
			return 4, nil
		}
		return 3, nil
	}

	// /{table}/.indexes/{name}/ - index info (or .schema file)
	result.Type = PathCapability
	result.CapabilityDir = DirIndexes
	result.CapabilityArg = indexName
	return 2, nil
}

// processFormat handles /{table}/.format/ paths.
// Supports:
//   - /{table}/.format/ → list available formats
//   - /{table}/.format/<format> → writable file to create synthesized view
func processFormat(result *ParsedPath, remaining []string) (int, *FSError) {
	fsCtx := result.Context
	if fsCtx == nil {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: ".format/ requires a table context",
		}
	}

	result.Type = PathFormat
	if len(remaining) >= 2 {
		result.FormatTarget = remaining[1]
		return 2, nil
	}
	return 1, nil
}

// processHistory handles /{table}/.history/ paths for versioned history.
// Supports two navigation modes:
//
// By filename (single or multi-segment):
//   - /{table}/.history/ → list filenames with history
//   - /{table}/.history/foo.md/ → list versions + .id file
//   - /{table}/.history/foo.md/.id → read row UUID
//   - /{table}/.history/foo.md/<versionID> → read past version
//
// By row UUID (.by/):
//   - /{table}/.history/.by/ → list all row UUIDs
//   - /{table}/.history/.by/<uuid>/ → list versions for UUID
//   - /{table}/.history/.by/<uuid>/<versionID> → read past version
func processHistory(result *ParsedPath, remaining []string) (int, *FSError) {
	fsCtx := result.Context
	if fsCtx == nil {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: ".history/ requires a table context",
		}
	}

	result.Type = PathHistory

	if len(remaining) == 1 {
		// /{table}/.history/
		return 1, nil
	}

	next := remaining[1]

	// Check for .by/ navigation mode
	if next == ".by" {
		result.HistoryByID = true
		if len(remaining) == 2 {
			// /{table}/.history/.by/
			return 2, nil
		}
		// /{table}/.history/.by/<uuid>/
		result.HistoryRowID = remaining[2]
		if len(remaining) == 3 {
			return 3, nil
		}
		// /{table}/.history/.by/<uuid>/<versionID>
		result.HistoryVersionID = remaining[3]
		return 4, nil
	}

	// By-filename navigation — greedily consume segments until .id or version ID.
	// This handles both single-segment filenames (per-directory model: "foo.md")
	// and multi-segment filenames (root .history/ with paths like "subdir/foo.md").
	var filenameParts []string
	consumed := 1 // .history itself
	for i := 1; i < len(remaining); i++ {
		seg := remaining[i]
		if seg == ".id" || isVersionID(seg) {
			result.HistoryVersionID = seg
			consumed = i + 1
			break
		}
		filenameParts = append(filenameParts, seg)
		consumed = i + 1
	}
	result.HistoryFile = strings.Join(filenameParts, "/")
	return consumed, nil
}

// isVersionID returns true if a segment looks like a history version ID timestamp.
// Version IDs have the format "2006-01-02T150405Z" (e.g., "2026-02-12T013000Z").
func isVersionID(seg string) bool {
	// Quick length check: "2006-01-02T150405Z" = 18 chars
	if len(seg) != 18 {
		return false
	}
	// Must end with Z and contain T at position 10
	return seg[17] == 'Z' && seg[10] == 'T' && seg[4] == '-' && seg[7] == '-'
}

// processTableDDL handles table-level DDL paths (/{table}/.modify/ and /{table}/.delete/).
// Returns the number of segments consumed.
//
// TODO: Views share the same root-level path structure as tables, so /{view}/.modify/
// and /{view}/.delete/ are parsed identically. Currently we allow .modify on views at
// parse time; the operation will fail at execution time if the database rejects it
// (e.g., PostgreSQL doesn't support ALTER VIEW for most modifications). An alternative
// would be to query the database here to distinguish views from tables, but that adds
// overhead to every path operation. Following FUSE's approach of failing at execution.
func processTableDDL(result *ParsedPath, remaining []string, op string) (int, *FSError) {
	fsCtx := result.Context
	if fsCtx == nil {
		return 0, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf(".%s/ requires a table context", op),
		}
	}
	result.Type = PathDDL
	result.DDLOp = op
	result.DDLName = fsCtx.TableName
	result.DDLObjectType = "table"
	if len(remaining) >= 2 {
		result.DDLFile = remaining[1]
		return 2, nil
	}
	return 1, nil
}

// processRowOrColumn handles row PK and column segments.
func processRowOrColumn(result *ParsedPath, remaining []string) (*ParsedPath, *FSError) {
	if len(remaining) == 0 {
		return result, nil
	}

	// Capture all remaining segments for synth hierarchy reconstruction.
	// This preserves the full sub-path before PK/Column logic discards segments.
	result.RawSubPath = append([]string{}, remaining...)

	// First segment is the primary key
	pk := remaining[0]
	format := ""

	// Check for format extension
	for ext, fmt := range knownFormats {
		if strings.HasSuffix(pk, ext) {
			pk = strings.TrimSuffix(pk, ext)
			format = fmt
			break
		}
	}

	result.Type = PathRow
	result.PrimaryKey = pk
	result.Format = format

	// If there's another segment, check if it's a format file or column
	if len(remaining) >= 2 {
		segment := remaining[1]
		// Check if it's a row format file (.json, .csv, .tsv, .yaml)
		if fmt, ok := knownFormats[segment]; ok {
			result.Format = fmt
		} else {
			result.Type = PathColumn
			result.Column = segment
		}
	}

	return result, nil
}

// extractFormat extracts format from a filename.
func extractFormat(filename string) string {
	for ext, fmt := range knownFormats {
		if strings.HasSuffix(filename, ext) {
			return fmt
		}
	}
	return ""
}
