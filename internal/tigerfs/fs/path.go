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
func parseSchemaPath(segments []string) (*ParsedPath, *FSError) {
	if len(segments) == 1 {
		// /.schemas/
		return &ParsedPath{Type: PathSchemaList}, nil
	}

	// /.schemas/<name>
	return &ParsedPath{
		Type: PathSchema,
		Context: &FSContext{
			Schema: segments[1],
		},
	}, nil
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
func parseTablePath(segments []string) (*ParsedPath, *FSError) {
	// Determine schema and table
	var schema, table string
	var startIdx int

	// Check if first segment could be a schema
	// If we have 2+ segments and second is not a capability, treat first as schema
	if len(segments) >= 2 && !strings.HasPrefix(segments[1], ".") {
		// Could be /schema/table/... or /table/<pk>/...
		// We need to distinguish. For now, assume single segment is table with public schema.
		// If second segment looks like a capability or row, first is table.
		// Otherwise, first is schema, second is table.

		// Heuristic: if second segment is all digits or has format extension, first is table
		if looksLikeRowPK(segments[1]) {
			schema = "public"
			table = segments[0]
			startIdx = 1
		} else {
			schema = segments[0]
			table = segments[1]
			startIdx = 2
		}
	} else {
		schema = "public"
		table = segments[0]
		startIdx = 1
	}

	// Create initial context
	ctx := NewFSContext(schema, table, "")

	// Process remaining segments
	result := &ParsedPath{
		Type:    PathTable,
		Context: ctx,
	}

	return processSegments(result, segments[startIdx:])
}

// looksLikeRowPK returns true if segment looks like a primary key value
// rather than a table name.
func looksLikeRowPK(s string) bool {
	// If it's all digits, it's a PK
	if _, err := strconv.Atoi(s); err == nil {
		return true
	}
	// If it has a format extension, it's a PK
	for ext := range knownFormats {
		if strings.HasSuffix(s, ext) {
			return true
		}
	}
	// If it contains characters unlikely in table names, it's a PK
	// Dots are allowed in PKs but rare in table names (except for schema.table notation
	// which we don't support in this position)
	if strings.ContainsAny(s, "-@.") {
		return true
	}
	return false
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
		result.Type = PathCapability
		result.CapabilityDir = DirOrder
		return 1, nil
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

// processExport handles .export/ paths.
func processExport(result *ParsedPath, remaining []string) (int, *FSError) {
	result.Type = PathExport
	result.Context = result.Context.WithTerminal()

	if len(remaining) >= 2 {
		// .export/<filename>
		filename := remaining[1]
		result.Format = extractFormat(filename)
		return 2, nil
	}
	return 1, nil
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

	// If there's another segment, it's a column
	if len(remaining) >= 2 {
		result.Type = PathColumn
		result.Column = remaining[1]
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
