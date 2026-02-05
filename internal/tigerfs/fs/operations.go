package fs

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Operations provides filesystem operations backed by PostgreSQL.
// This is the shared core that can be used by both FUSE and NFS handlers.
type Operations struct {
	config  *config.Config
	db      db.DBClient
	staging *StagingManager // Tracks partial rows for incremental creation
	ddl     *DDLManager     // Handles DDL staging operations
}

// NewOperations creates a new Operations instance.
func NewOperations(cfg *config.Config, dbClient db.DBClient) *Operations {
	return &Operations{
		config: cfg,
		db:     dbClient,
		ddl:    NewDDLManager(dbClient),
	}
}

// GetDDLManager returns the DDL manager for direct access to DDL operations.
// This is useful for adapters that need to manage DDL sessions.
func (o *Operations) GetDDLManager() *DDLManager {
	return o.ddl
}

// ReadDir lists directory contents for the given path.
func (o *Operations) ReadDir(ctx context.Context, path string) ([]Entry, *FSError) {
	parsed, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	return o.readDirWithParsed(ctx, parsed)
}

// ReadDirWithContext lists directory contents using a pre-parsed context.
// This is more efficient for FUSE which may have already parsed the path.
func (o *Operations) ReadDirWithContext(ctx context.Context, fsCtx *FSContext) ([]Entry, *FSError) {
	// Create a parsed path from the context
	parsed := &ParsedPath{
		Type:    PathTable,
		Context: fsCtx,
	}
	return o.readDirWithParsed(ctx, parsed)
}

// readDirWithParsed implements directory listing for a parsed path.
func (o *Operations) readDirWithParsed(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	switch parsed.Type {
	case PathRoot:
		return o.readDirRoot(ctx)
	case PathSchemaList:
		return o.readDirSchemaList(ctx)
	case PathSchema:
		return o.readDirSchema(ctx, parsed.Context.Schema)
	case PathTable:
		return o.readDirTable(ctx, parsed)
	case PathInfo:
		return o.readDirInfo(ctx, parsed)
	case PathRow:
		// Row directories list column files
		return o.readDirRow(ctx, parsed)
	case PathCapability:
		return o.readDirCapability(ctx, parsed)
	case PathExport:
		return o.readDirExport(ctx, parsed)
	case PathImport:
		return o.readDirImport(ctx, parsed)
	case PathDDL:
		return o.readDirDDL(ctx, parsed)
	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot list directory for path type: %d", parsed.Type),
		}
	}
}

// readDirRoot lists the root directory.
// Shows tables from the default schema (flattened) plus special directories.
func (o *Operations) readDirRoot(ctx context.Context) ([]Entry, *FSError) {
	// Get current schema (default is "public")
	currentSchema, err := o.db.GetCurrentSchema(ctx)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get current schema",
			Cause:   err,
		}
	}

	// Get tables from current schema
	tables, err := o.db.GetTables(ctx, currentSchema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list tables",
			Cause:   err,
		}
	}

	// Get views from current schema
	views, err := o.db.GetViews(ctx, currentSchema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list views",
			Cause:   err,
		}
	}

	entries := make([]Entry, 0, len(tables)+len(views)+3)

	now := time.Now()

	// Add special directories first
	entries = append(entries,
		Entry{Name: ".create", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		Entry{Name: ".delete", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		Entry{Name: ".schemas", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
	)

	// Add tables
	for _, t := range tables {
		entries = append(entries, Entry{Name: t, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	}

	// Add views
	for _, v := range views {
		entries = append(entries, Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0555, ModTime: now})
	}

	return entries, nil
}

// readDirSchemaList lists all schemas (/.schemas/).
func (o *Operations) readDirSchemaList(ctx context.Context) ([]Entry, *FSError) {
	schemas, err := o.db.GetSchemas(ctx)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list schemas",
			Cause:   err,
		}
	}

	now := time.Now()
	entries := make([]Entry, len(schemas))
	for i, s := range schemas {
		entries[i] = Entry{Name: s, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
	}

	return entries, nil
}

// readDirSchema lists tables in a specific schema.
func (o *Operations) readDirSchema(ctx context.Context, schema string) ([]Entry, *FSError) {
	tables, err := o.db.GetTables(ctx, schema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to list tables in schema %s", schema),
			Cause:   err,
		}
	}

	views, err := o.db.GetViews(ctx, schema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to list views in schema %s", schema),
			Cause:   err,
		}
	}

	now := time.Now()
	entries := make([]Entry, 0, len(tables)+len(views))

	for _, t := range tables {
		entries = append(entries, Entry{Name: t, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	}

	for _, v := range views {
		entries = append(entries, Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0555, ModTime: now})
	}

	return entries, nil
}

// readDirTable lists contents of a table directory.
// Shows rows (by primary key) plus capability directories.
// Respects pipeline context (filters, order, limits) when present.
func (o *Operations) readDirTable(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for table path",
		}
	}

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to get primary key for %s.%s", fsCtx.Schema, fsCtx.TableName),
			Cause:   err,
		}
	}

	pkColumn := pk.Columns[0]

	// Default limit from config
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	var rows []string

	// Check if we have any pipeline operations (filters, order, limits)
	if fsCtx.HasPipelineOperations() {
		// Use pipeline query to respect filters, order, and limits
		params := fsCtx.ToQueryParams()
		params.PKColumn = pkColumn

		// Apply default limit if none specified in pipeline
		if params.Limit == 0 {
			params.Limit = limit
		}

		rows, err = o.db.QueryRowsPipeline(ctx, params)
		if err != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to query rows with pipeline",
				Cause:   err,
			}
		}
	} else {
		// Simple table scan for raw table access
		rows, err = o.db.ListRows(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, limit)
		if err != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to list rows",
				Cause:   err,
			}
		}
	}

	now := time.Now()

	// Build entries: capability directories first, then rows
	entries := make([]Entry, 0, len(rows)+15)

	// Add capability directories based on what's available in current context
	if fsCtx.HasPipelineOperations() {
		// Use available capabilities based on pipeline state
		for _, cap := range fsCtx.AvailableCapabilities() {
			entries = append(entries, Entry{Name: cap, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
		}
		// Always include .info for metadata access
		entries = append(entries, Entry{Name: DirInfo, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	} else {
		// Show all capabilities for raw table access
		capabilities := []string{
			DirAll, DirBy, DirDelete, DirExport, DirFilter, DirFirst,
			DirImport, DirIndexes, DirInfo, DirLast, DirModify, DirOrder, DirSample,
		}
		for _, cap := range capabilities {
			entries = append(entries, Entry{Name: cap, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
		}
	}

	// Add rows as directories (row files like 1.json accessible but not listed)
	for _, rowPK := range rows {
		entries = append(entries, Entry{Name: rowPK, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	}

	return entries, nil
}

// readDirRow lists columns in a row directory.
func (o *Operations) readDirRow(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	columns, err := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	now := time.Now()

	// Include row export format files (.json, .tsv, .csv, .yaml)
	entries := []Entry{
		{Name: ".json", IsDir: false, Mode: 0444, ModTime: now},
		{Name: ".tsv", IsDir: false, Mode: 0444, ModTime: now},
		{Name: ".csv", IsDir: false, Mode: 0444, ModTime: now},
		{Name: ".yaml", IsDir: false, Mode: 0444, ModTime: now},
	}

	// Add column files
	for _, col := range columns {
		entries = append(entries, Entry{Name: col.Name, IsDir: false, Mode: 0600, ModTime: now})
	}

	return entries, nil
}

// readDirInfo lists the .info metadata directory.
// Files match FUSE behavior: count, ddl, schema, columns, indexes (no dot prefix).
func (o *Operations) readDirInfo(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()
	entries := []Entry{
		{Name: "count", IsDir: false, Mode: 0444, ModTime: now},
		{Name: "ddl", IsDir: false, Mode: 0444, ModTime: now},
		{Name: "schema", IsDir: false, Mode: 0444, ModTime: now},
		{Name: "columns", IsDir: false, Mode: 0444, ModTime: now},
		{Name: "indexes", IsDir: false, Mode: 0444, ModTime: now},
	}
	return entries, nil
}

// readDirCapability lists contents of a capability directory.
func (o *Operations) readDirCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	switch parsed.CapabilityDir {
	case DirBy:
		return o.readDirByCapability(ctx, parsed)
	case DirFilter:
		return o.readDirFilterCapability(ctx, parsed)
	case DirOrder:
		return o.readDirOrderCapability(ctx, parsed)
	case DirFirst, DirLast, DirSample:
		// These show numeric directories for limit values
		return o.readDirPaginationCapability(ctx, parsed)
	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown capability: %s", parsed.CapabilityDir),
		}
	}
}

// readDirByCapability lists indexed columns or values for .by/ navigation.
func (o *Operations) readDirByCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for .by path",
		}
	}

	now := time.Now()

	if parsed.CapabilityArg == "" {
		// List indexed columns
		indexes, err := o.db.GetSingleColumnIndexes(ctx, fsCtx.Schema, fsCtx.TableName)
		if err != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get indexes",
				Cause:   err,
			}
		}

		entries := make([]Entry, len(indexes))
		for i, idx := range indexes {
			entries[i] = Entry{Name: idx.Columns[0], IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
		}
		return entries, nil
	}

	// List distinct values for the column (use DirListingLimit for .by/)
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}
	return o.readDistinctColumnValues(ctx, fsCtx, parsed.CapabilityArg, limit)
}

// readDirFilterCapability lists columns or values for .filter/ navigation.
func (o *Operations) readDirFilterCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for .filter path",
		}
	}

	now := time.Now()

	if parsed.CapabilityArg == "" {
		// List all columns
		columns, err := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
		if err != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get columns",
				Cause:   err,
			}
		}

		entries := make([]Entry, len(columns))
		for i, col := range columns {
			entries[i] = Entry{Name: col.Name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
		}
		return entries, nil
	}

	// List distinct values for the column (use DirFilterLimit for .filter/)
	limit := o.config.DirFilterLimit
	if limit <= 0 {
		limit = 100000
	}
	return o.readDistinctColumnValues(ctx, fsCtx, parsed.CapabilityArg, limit)
}

// readDistinctColumnValues returns distinct values for a column as directory entries.
// Applies existing pipeline filters when getting distinct values.
func (o *Operations) readDistinctColumnValues(ctx context.Context, fsCtx *FSContext, column string, limit int) ([]Entry, *FSError) {
	var values []string
	var err error

	// If there are existing filters, apply them when getting distinct values
	if fsCtx.HasFilters() {
		filterColumns := make([]string, len(fsCtx.Filters))
		filterValues := make([]string, len(fsCtx.Filters))
		for i, f := range fsCtx.Filters {
			filterColumns[i] = f.Column
			filterValues[i] = f.Value
		}
		values, err = o.db.GetDistinctValuesFiltered(ctx, fsCtx.Schema, fsCtx.TableName, column, filterColumns, filterValues, limit)
	} else {
		values, err = o.db.GetDistinctValues(ctx, fsCtx.Schema, fsCtx.TableName, column, limit)
	}

	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get distinct values",
			Cause:   err,
		}
	}

	now := time.Now()
	entries := make([]Entry, len(values))
	for i, v := range values {
		entries[i] = Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
	}
	return entries, nil
}

// readDirOrderCapability lists columns for .order/ navigation.
func (o *Operations) readDirOrderCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for .order path",
		}
	}

	columns, err := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	now := time.Now()

	// Show both ascending and descending options for each column
	entries := make([]Entry, 0, len(columns)*2)
	for _, col := range columns {
		entries = append(entries,
			Entry{Name: col.Name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
			Entry{Name: col.Name + ".desc", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		)
	}
	return entries, nil
}

// readDirPaginationCapability lists options for .first/, .last/, .sample/.
func (o *Operations) readDirPaginationCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()
	// Show common limit values
	limits := []string{"10", "25", "50", "100", "500", "1000"}
	entries := make([]Entry, len(limits))
	for i, l := range limits {
		entries[i] = Entry{Name: l, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
	}
	return entries, nil
}

// readDirExport lists format files in .export/ or .export/.with-headers/.
// Matches FUSE behavior: .with-headers/ directory plus format files.
func (o *Operations) readDirExport(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()

	if parsed.ExportWithHeaders {
		// .with-headers only shows csv and tsv (JSON/YAML have built-in keys)
		return []Entry{
			{Name: "csv", IsDir: false, Mode: 0600, ModTime: now},
			{Name: "tsv", IsDir: false, Mode: 0600, ModTime: now},
		}, nil
	}

	return []Entry{
		{Name: ".with-headers", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now},
		{Name: "csv", IsDir: false, Mode: 0600, ModTime: now},
		{Name: "json", IsDir: false, Mode: 0600, ModTime: now},
		{Name: "tsv", IsDir: false, Mode: 0600, ModTime: now},
		{Name: "yaml", IsDir: false, Mode: 0600, ModTime: now},
	}, nil
}

// readDirImport lists import modes in .import/.
func (o *Operations) readDirImport(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()

	// .import/ directory - list modes
	if parsed.ImportMode == "" {
		entries := []Entry{
			{Name: DirSync, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
			{Name: DirOverwrite, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
			{Name: DirAppend, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		}
		return entries, nil
	}

	// .import/{mode}/.no-headers/ directory - list formats (csv, tsv only)
	if parsed.ImportNoHeaders {
		entries := []Entry{
			{Name: FmtCSV, IsDir: false, Mode: 0600, ModTime: now},
			{Name: FmtTSV, IsDir: false, Mode: 0600, ModTime: now},
		}
		return entries, nil
	}

	// .import/{mode}/ directory - list .no-headers and formats
	entries := []Entry{
		{Name: DirNoHeaders, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		{Name: FmtCSV, IsDir: false, Mode: 0600, ModTime: now},
		{Name: FmtJSON, IsDir: false, Mode: 0600, ModTime: now},
		{Name: FmtTSV, IsDir: false, Mode: 0600, ModTime: now},
		{Name: FmtYAML, IsDir: false, Mode: 0600, ModTime: now},
	}
	return entries, nil
}

// readDirDDL lists DDL staging directory contents.
func (o *Operations) readDirDDL(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()

	if parsed.DDLName == "" {
		// List staging directories by querying DDLManager for sessions of this operation type
		op, valid := ParseDDLOpType(parsed.DDLOp)
		if !valid {
			return nil, &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
		}
		sessions := o.ddl.ListSessionEntries(op)
		entries := make([]Entry, len(sessions))
		for i, session := range sessions {
			entries[i] = Entry{
				Name:    session.ObjectName,
				IsDir:   true,
				Mode:    os.ModeDir | 0755,
				ModTime: session.CreatedAt,
			}
		}
		return entries, nil
	}

	// For a specific staging directory, check if session exists first
	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return nil, &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
	}
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID == "" {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("no DDL session for %s", parsed.DDLName),
			Hint:    fmt.Sprintf("create session with: mkdir /.%s/%s", parsed.DDLOp, parsed.DDLName),
		}
	}

	// List control files for a specific staging operation
	// Note: Mode and Size must match what statDDLFile returns for NFS consistency
	entries := []Entry{
		{Name: "sql", IsDir: false, Mode: 0600, ModTime: now},
		{Name: ".test", IsDir: false, Mode: 0644, Size: 0, ModTime: now},
		{Name: ".commit", IsDir: false, Mode: 0644, Size: 0, ModTime: now},
		{Name: ".abort", IsDir: false, Mode: 0644, Size: 0, ModTime: now},
	}
	return entries, nil
}

// Stat returns metadata for a path.
func (o *Operations) Stat(ctx context.Context, path string) (*Entry, *FSError) {
	parsed, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	return o.statWithParsed(ctx, parsed, path)
}

// StatWithContext returns metadata using a pre-parsed context.
func (o *Operations) StatWithContext(ctx context.Context, fsCtx *FSContext) (*Entry, *FSError) {
	parsed := &ParsedPath{
		Type:    PathTable,
		Context: fsCtx,
	}
	return o.statWithParsed(ctx, parsed, "")
}

// statWithParsed implements stat for a parsed path.
func (o *Operations) statWithParsed(ctx context.Context, parsed *ParsedPath, originalPath string) (*Entry, *FSError) {
	now := time.Now()

	switch parsed.Type {
	case PathRoot:
		return &Entry{Name: "", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathSchemaList:
		return &Entry{Name: ".schemas", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathSchema:
		return &Entry{Name: parsed.Context.Schema, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathTable:
		fsCtx := parsed.Context
		if fsCtx == nil {
			return nil, &FSError{
				Code:    ErrInvalidPath,
				Message: "missing context for table path",
			}
		}
		// Verify table exists by checking if we can get its primary key.
		// This is necessary because table paths are parsed without database validation,
		// and NFS clients need accurate stat responses to avoid caching nonexistent entries.
		_, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
		if err != nil {
			return nil, &FSError{
				Code:    ErrNotExist,
				Message: fmt.Sprintf("table not found: %s.%s", fsCtx.Schema, fsCtx.TableName),
				Cause:   err,
			}
		}
		name := fsCtx.TableName
		// When we have the original path and there are pipeline operations,
		// use the path's basename as the entry name. This is critical for NFS
		// which expects stat responses to have names matching the path being stat'd.
		// E.g., stat("/table/.by/col/value") should return Name="value", not "table".
		if originalPath != "" && fsCtx.HasPipelineOperations() {
			baseName := path.Base(originalPath)
			logging.Debug("statWithParsed PathTable with pipeline",
				zap.String("originalPath", originalPath),
				zap.String("tableName", fsCtx.TableName),
				zap.String("baseName", baseName),
				zap.Int("filterCount", len(fsCtx.Filters)))
			if baseName != "" && baseName != "." && baseName != "/" {
				name = baseName
			}
		}
		return &Entry{Name: name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathRow:
		return o.statRow(ctx, parsed)

	case PathColumn:
		return o.statColumn(ctx, parsed)

	case PathInfo:
		if parsed.InfoFile == "" {
			return &Entry{Name: ".info", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
		}
		return o.statInfoFile(ctx, parsed)

	case PathCapability:
		return &Entry{Name: parsed.CapabilityDir, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathExport:
		return o.statExport(ctx, parsed)

	case PathImport:
		return o.statImport(ctx, parsed)

	case PathDDL:
		if parsed.DDLFile != "" {
			return o.statDDLFile(ctx, parsed)
		}
		// If DDLName is set, this is a staging directory - check if session exists
		if parsed.DDLName != "" {
			op, valid := ParseDDLOpType(parsed.DDLOp)
			if !valid {
				return nil, &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
			}
			sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
			if sessionID == "" {
				return nil, &FSError{
					Code:    ErrNotExist,
					Message: fmt.Sprintf("no DDL session for %s", parsed.DDLName),
					Hint:    fmt.Sprintf("create session with: mkdir /.%s/%s", parsed.DDLOp, parsed.DDLName),
				}
			}
			return &Entry{Name: parsed.DDLName, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
		}
		// Otherwise it's just /.create (the operation directory itself)
		name := "." + parsed.DDLOp
		return &Entry{Name: name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot stat path type: %d", parsed.Type),
		}
	}
}

// statRow returns metadata for a row path.
// Without a format extension (e.g., /users/1), returns a directory.
// With a format extension (e.g., /users/1.json), returns a file.
func (o *Operations) statRow(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	now := time.Now()

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	// Check if row exists by fetching it
	row, err := o.db.GetRow(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey)
	if err != nil {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("row not found: %s", parsed.PrimaryKey),
			Cause:   err,
		}
	}

	// No format extension means this is a row directory (can cd into it)
	if parsed.Format == "" {
		return &Entry{
			Name:    parsed.PrimaryKey,
			IsDir:   true,
			Mode:    os.ModeDir | 0755,
			ModTime: now,
		}, nil
	}

	// With format extension, it's a file containing the row data
	size, err := o.calculateRowSize(row, parsed.Format)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to calculate row size",
			Cause:   err,
		}
	}

	return &Entry{
		Name:    parsed.PrimaryKey + "." + parsed.Format,
		IsDir:   false,
		Size:    size,
		Mode:    0644,
		ModTime: now,
	}, nil
}

// statColumn returns metadata for a column file.
func (o *Operations) statColumn(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for column path",
		}
	}

	now := time.Now()

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	// Get column value
	val, err := o.db.GetColumn(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey, parsed.Column)
	if err != nil {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("column not found: %s", parsed.Column),
			Cause:   err,
		}
	}

	// Calculate size using same formatting as readColumnFile
	str, err := format.ConvertValueToText(val)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to format column value: %v", err),
			Cause:   err,
		}
	}
	size := int64(len(str) + 1) // +1 for trailing newline

	return &Entry{
		Name:    parsed.Column,
		IsDir:   false,
		Size:    size,
		Mode:    0600,
		ModTime: now,
	}, nil
}

// statInfoFile returns metadata for an info file.
// Fetches the actual content to report accurate file size.
func (o *Operations) statInfoFile(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	// Fetch actual content to get accurate size (required for NFS)
	content, fsErr := o.readInfoFile(ctx, parsed)
	if fsErr != nil {
		return nil, fsErr
	}

	return &Entry{
		Name:    parsed.InfoFile,
		IsDir:   false,
		Mode:    0444, // Read-only, matching FUSE behavior
		Size:    int64(len(content.Data)),
		ModTime: time.Now(),
	}, nil
}

// statDDLFile returns metadata for a DDL control file.
func (o *Operations) statDDLFile(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	// Validate operation type
	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return nil, &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
	}

	// Trigger files (.test, .commit, .abort) must always appear to exist for NFS compliance.
	// After a trigger fires (especially .commit or .abort), the session is removed. If NFS
	// then tries to stat the file (which it does after close), the session won't be found.
	// By returning a valid entry regardless of session state, we ensure NFS protocol
	// operations complete successfully. The actual operation will fail with a helpful
	// error if there's no session.
	//
	// We use mode 0644 (instead of 0200 write-only) because some NFS clients have issues
	// with write-only files. Size is 0 to match the actual empty content returned by read.
	if parsed.DDLFile == FileTest || parsed.DDLFile == FileCommit || parsed.DDLFile == FileAbort {
		return &Entry{
			Name:    parsed.DDLFile,
			IsDir:   false,
			Mode:    0644, // Read-write for NFS compatibility
			Size:    0,    // Matches actual empty content
			ModTime: time.Now(),
		}, nil
	}

	// For other files (sql, test.log), we need the session to exist
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID == "" {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("no DDL session for %s", parsed.DDLName),
			Hint:    fmt.Sprintf("create session with: mkdir /.%s/%s", parsed.DDLOp, parsed.DDLName),
		}
	}

	var mode os.FileMode
	var size int64
	switch parsed.DDLFile {
	case FileSQL:
		mode = 0600
		// Get actual size of SQL content (or template)
		content := o.ddl.GetSQL(sessionID)
		size = int64(len(content))
	case FileTestLog:
		mode = 0444
		// Get actual size of test log
		log := o.ddl.GetTestLog(sessionID)
		if log == "" {
			return nil, &FSError{
				Code:    ErrNotExist,
				Message: "no test results yet",
				Hint:    fmt.Sprintf("run validation with: touch /.%s/%s/.test", parsed.DDLOp, parsed.DDLName),
			}
		}
		size = int64(len(log))
	default:
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("unknown DDL file: %s", parsed.DDLFile)}
	}

	return &Entry{
		Name:    parsed.DDLFile,
		IsDir:   false,
		Mode:    mode,
		Size:    size,
		ModTime: time.Now(),
	}, nil
}

// statExport returns metadata for an export path (directory or file).
func (o *Operations) statExport(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	now := time.Now()

	// If format is set, this is an export file (e.g., /table/.export/csv)
	if parsed.Format != "" {
		// Calculate actual file size by generating the export data
		content, err := o.readExportFile(ctx, parsed)
		if err != nil {
			// If we can't read the file, return with size 0 but don't fail stat
			// Use 0600 for NFS compatibility (macOS requires owner read permission)
			return &Entry{
				Name:    parsed.Format,
				IsDir:   false,
				Mode:    0600,
				ModTime: now,
			}, nil
		}

		// Use 0600 for NFS compatibility (macOS requires owner read permission)
		return &Entry{
			Name:    parsed.Format,
			IsDir:   false,
			Size:    content.Size,
			Mode:    0600,
			ModTime: now,
		}, nil
	}

	// If ExportWithHeaders is set but no format, it's the .with-headers directory
	if parsed.ExportWithHeaders {
		return &Entry{Name: ".with-headers", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// Otherwise it's the .export directory itself
	return &Entry{Name: ".export", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
}

// statImport returns metadata for an import path (directory or file).
func (o *Operations) statImport(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	now := time.Now()

	// .import directory itself
	if parsed.ImportMode == "" {
		return &Entry{Name: ".import", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// .import/.overwrite (or .sync, .append) directory - no format yet, no .no-headers
	if parsed.Format == "" && !parsed.ImportNoHeaders {
		return &Entry{Name: "." + parsed.ImportMode, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// .import/.overwrite/.no-headers directory - no format yet
	if parsed.Format == "" && parsed.ImportNoHeaders {
		return &Entry{Name: DirNoHeaders, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// .import/.overwrite/csv or .import/.overwrite/.no-headers/csv - writable file
	return &Entry{
		Name:    parsed.Format,
		IsDir:   false,
		Mode:    0600, // writable
		Size:    0,    // size unknown until written
		ModTime: now,
	}, nil
}

// calculateRowSize calculates the serialized size of a row in the given format.
func (o *Operations) calculateRowSize(row *db.Row, fmt string) (int64, error) {
	var data []byte
	var err error

	switch fmt {
	case "json":
		data, err = format.RowToJSON(row.Columns, row.Values)
	case "csv":
		data, err = format.RowToCSV(row.Columns, row.Values)
	case "yaml":
		data, err = format.RowToYAML(row.Columns, row.Values)
	case "tsv", "":
		data, err = format.RowToTSV(row.Columns, row.Values)
	default:
		data, err = format.RowToTSV(row.Columns, row.Values)
	}

	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// ReadFile returns file contents for the given path.
func (o *Operations) ReadFile(ctx context.Context, path string) (*FileContent, *FSError) {
	parsed, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	return o.readFileWithParsed(ctx, parsed)
}

// ReadFileWithContext returns file contents using a pre-parsed context.
func (o *Operations) ReadFileWithContext(ctx context.Context, fsCtx *FSContext) (*FileContent, *FSError) {
	parsed := &ParsedPath{
		Type:    PathTable,
		Context: fsCtx,
	}
	return o.readFileWithParsed(ctx, parsed)
}

// readFileWithParsed implements file reading for a parsed path.
func (o *Operations) readFileWithParsed(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	switch parsed.Type {
	case PathRow:
		return o.readRowFile(ctx, parsed)
	case PathColumn:
		return o.readColumnFile(ctx, parsed)
	case PathInfo:
		return o.readInfoFile(ctx, parsed)
	case PathExport:
		return o.readExportFile(ctx, parsed)
	case PathImport:
		// Import files are write-only, return empty content
		return &FileContent{Data: []byte{}}, nil
	case PathDDL:
		return o.readDDLFile(ctx, parsed)
	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot read file for path type: %d", parsed.Type),
		}
	}
}

// readRowFile reads a row file in the specified format.
func (o *Operations) readRowFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	// Get row data
	row, err := o.db.GetRow(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey)
	if err != nil {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("row not found: %s", parsed.PrimaryKey),
			Cause:   err,
		}
	}

	// Serialize to format
	var data []byte
	switch parsed.Format {
	case "json":
		data, err = format.RowToJSON(row.Columns, row.Values)
	case "csv":
		data, err = format.RowToCSV(row.Columns, row.Values)
	case "yaml":
		data, err = format.RowToYAML(row.Columns, row.Values)
	case "tsv", "":
		data, err = format.RowToTSV(row.Columns, row.Values)
	default:
		data, err = format.RowToTSV(row.Columns, row.Values)
	}

	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to serialize row",
			Cause:   err,
		}
	}

	return &FileContent{
		Data: data,
		Size: int64(len(data)),
		Mode: 0600,
	}, nil
}

// readColumnFile reads a single column value.
func (o *Operations) readColumnFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for column path",
		}
	}

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	// Get column value
	val, err := o.db.GetColumn(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey, parsed.Column)
	if err != nil {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("column not found: %s", parsed.Column),
			Cause:   err,
		}
	}

	// Format value using proper type conversion (same as FUSE)
	str, err := format.ConvertValueToText(val)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to format column value: %v", err),
			Cause:   err,
		}
	}
	data := str + "\n"

	return &FileContent{
		Data: []byte(data),
		Size: int64(len(data)),
		Mode: 0600,
	}, nil
}

// readInfoFile reads an info metadata file.
func (o *Operations) readInfoFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for info file",
		}
	}

	var data string
	var err error

	switch parsed.InfoFile {
	case FileCount: // "count"
		count, dbErr := o.db.GetRowCount(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get row count",
				Cause:   dbErr,
			}
		}
		data = strconv.FormatInt(count, 10) + "\n"

	case FileDDL: // "ddl" - full DDL with indexes, constraints, triggers
		ddl, dbErr := o.db.GetFullDDL(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get DDL",
				Cause:   dbErr,
			}
		}
		data = ddl

	case FileColumns: // "columns" - one column name per line (no types)
		columns, dbErr := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get columns",
				Cause:   dbErr,
			}
		}
		for _, col := range columns {
			data += col.Name + "\n"
		}

	case FileSchema: // "schema" - basic CREATE TABLE DDL
		ddl, dbErr := o.db.GetTableDDL(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get schema",
				Cause:   dbErr,
			}
		}
		data = ddl

	case FileIndexes: // "indexes" - one index name per line
		indexes, dbErr := o.db.GetIndexes(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get indexes",
				Cause:   dbErr,
			}
		}
		for _, idx := range indexes {
			data += idx.Name + "\n"
		}

	default:
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("unknown info file: %s", parsed.InfoFile),
		}
	}

	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to read info file",
			Cause:   err,
		}
	}

	return &FileContent{
		Data: []byte(data),
		Size: int64(len(data)),
		Mode: 0600,
	}, nil
}

// readExportFile reads an export file (bulk data).
// Respects pipeline context (filters, order, limits) when present.
func (o *Operations) readExportFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for export path",
		}
	}

	// Default limit from config
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	var columns []string
	var rows [][]interface{}
	var err error

	// Check if we have any pipeline operations (filters, order, limits)
	if fsCtx.HasPipelineOperations() {
		// Get primary key for pipeline query ordering
		pk, pkErr := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
		if pkErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get primary key for export",
				Cause:   pkErr,
			}
		}

		// Use pipeline query to respect filters, order, and limits
		params := fsCtx.ToQueryParams()
		params.PKColumn = pk.Columns[0]

		// Apply default limit if none specified in pipeline
		if params.Limit == 0 {
			params.Limit = limit
		}

		columns, rows, err = o.db.QueryRowsWithDataPipeline(ctx, params)
	} else {
		// Simple table scan for raw table access
		columns, rows, err = o.db.GetAllRows(ctx, fsCtx.Schema, fsCtx.TableName, limit)
	}
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get rows for export",
			Cause:   err,
		}
	}

	// Serialize based on format
	var data []byte
	switch parsed.Format {
	case "json":
		data, err = format.RowsToJSON(columns, rows)
	case "csv":
		if parsed.ExportWithHeaders {
			data, err = format.RowsToCSVWithHeaders(columns, rows)
		} else {
			data, err = format.RowsToCSV(columns, rows)
		}
	case "tsv", "":
		if parsed.ExportWithHeaders {
			data, err = format.RowsToTSVWithHeaders(columns, rows)
		} else {
			data, err = format.RowsToTSV(columns, rows)
		}
	case "yaml":
		data, err = format.RowsToYAML(columns, rows)
	default:
		data, err = format.RowsToTSV(columns, rows)
	}

	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to serialize export data",
			Cause:   err,
		}
	}

	return &FileContent{
		Data: data,
		Size: int64(len(data)),
		Mode: 0600,
	}, nil
}

// joinStrings joins strings with comma separator.
func joinStrings(strs []string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += ", "
		}
		result += s
	}
	return result
}

// readDDLFile reads a DDL staging file.
//
// Supported files:
//   - sql: Returns DDL content or template if empty
//   - test.log: Returns validation results
func (o *Operations) readDDLFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	// Convert operation string to DDLOpType
	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp),
		}
	}

	// Find session by name
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID == "" {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("no DDL session found for %s", parsed.DDLName),
			Hint:    fmt.Sprintf("create session first with: mkdir /.%s/%s", parsed.DDLOp, parsed.DDLName),
		}
	}

	// Handle the specific file
	switch parsed.DDLFile {
	case FileSQL:
		// Return DDL content or template
		content := o.ddl.GetSQL(sessionID)
		return &FileContent{
			Data: []byte(content),
			Size: int64(len(content)),
			Mode: 0600,
		}, nil

	case FileTestLog:
		// Return test log
		log := o.ddl.GetTestLog(sessionID)
		if log == "" {
			return nil, &FSError{
				Code:    ErrNotExist,
				Message: "no test results yet",
				Hint:    fmt.Sprintf("run validation with: touch /.%s/%s/.test", parsed.DDLOp, parsed.DDLName),
			}
		}
		return &FileContent{
			Data: []byte(log),
			Size: int64(len(log)),
			Mode: 0444,
		}, nil

	case FileTest, FileCommit, FileAbort:
		// Trigger files return empty content when read (they're write-only triggers).
		// The size is reported as 4096 in stat for NFS compatibility, but actual content is empty.
		return &FileContent{
			Data: []byte{},
			Size: 0,
			Mode: 0644,
		}, nil

	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot read DDL file: %s", parsed.DDLFile),
		}
	}
}
