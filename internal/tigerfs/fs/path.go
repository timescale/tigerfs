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

	// DDLOp is the DDL operation when Type is PathDDL (create, modify, delete).
	DDLOp string

	// DDLName is the target name for DDL operations.
	DDLName string

	// DDLFile is the DDL file being accessed (sql, .test, .commit, .abort, test.log).
	DDLFile string

	// CapabilityDir is set when Type is PathCapability, indicating which
	// capability directory we're in (.by, .filter, .order, etc.).
	CapabilityDir string

	// CapabilityArg is the argument within a capability (column name for .by/).
	CapabilityArg string

	// ExportWithHeaders is set when exporting with headers (.export/.with-headers/).
	ExportWithHeaders bool
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
	case ".create", ".modify", ".delete":
		return parseDDLPath(segments)
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
//   - /.schemas/ → list schemas
//   - /.schemas/<schema> → list tables in schema
//   - /.schemas/<schema>/<table> → table path
//   - /.schemas/<schema>/<table>/<row> → row path
func parseSchemaPath(segments []string) (*ParsedPath, *FSError) {
	if len(segments) == 1 {
		// /.schemas/
		return &ParsedPath{Type: PathSchemaList}, nil
	}

	schema := segments[1]

	if len(segments) == 2 {
		// /.schemas/<name>/ - list tables in schema
		return &ParsedPath{
			Type: PathSchema,
			Context: &FSContext{
				Schema: schema,
			},
		}, nil
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

// parseDDLPath handles /.create/, /.modify/, /.delete/ paths.
func parseDDLPath(segments []string) (*ParsedPath, *FSError) {
	op := strings.TrimPrefix(segments[0], ".")

	if len(segments) == 1 {
		// Just /.create/ etc - list staging directories
		return &ParsedPath{
			Type:  PathDDL,
			DDLOp: op,
		}, nil
	}

	result := &ParsedPath{
		Type:    PathDDL,
		DDLOp:   op,
		DDLName: segments[1],
	}

	if len(segments) >= 3 {
		result.DDLFile = segments[2]
	}

	return result, nil
}

// parseTablePath handles table paths and capability chains.
// Root-level paths always use public schema: /table or /table/row.
// For explicit schema access, use /.schemas/schemaname/table.
func parseTablePath(segments []string) (*ParsedPath, *FSError) {
	// Always use public schema for root-level table paths.
	// This avoids ambiguity between /schema/table and /table/row for text PKs.
	schema := "public"
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

		// Not a capability - must be row PK or column
		return processRowOrColumn(result, segments[i:])
	}

	return result, nil
}

// processCapability handles capability directories (.by/, .filter/, etc.)
// Returns the number of segments consumed.
func processCapability(result *ParsedPath, cap string, remaining []string) (int, *FSError) {
	switch cap {
	case DirInfo:
		return processInfo(result, remaining)
	case DirBy:
		return processBy(result, remaining)
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
		// Check for filename (e.g., data.csv)
		if len(remaining) >= 3 {
			filename := remaining[2]
			result.Format = extractFormat(filename)
			return 3, nil
		}
		return 2, nil
	}
	return 1, nil
}

// processRowOrColumn handles row PK and column segments.
func processRowOrColumn(result *ParsedPath, remaining []string) (*ParsedPath, *FSError) {
	if len(remaining) == 0 {
		return result, nil
	}

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
