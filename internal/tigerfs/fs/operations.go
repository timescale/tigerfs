package fs

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
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

	// Add special directories first
	entries = append(entries,
		Entry{Name: ".create", IsDir: true, Mode: os.ModeDir | 0755},
		Entry{Name: ".delete", IsDir: true, Mode: os.ModeDir | 0755},
		Entry{Name: ".schemas", IsDir: true, Mode: os.ModeDir | 0755},
	)

	// Add tables
	for _, t := range tables {
		entries = append(entries, Entry{Name: t, IsDir: true, Mode: os.ModeDir | 0755})
	}

	// Add views
	for _, v := range views {
		entries = append(entries, Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0555})
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

	entries := make([]Entry, len(schemas))
	for i, s := range schemas {
		entries[i] = Entry{Name: s, IsDir: true, Mode: os.ModeDir | 0755}
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

	entries := make([]Entry, 0, len(tables)+len(views))

	for _, t := range tables {
		entries = append(entries, Entry{Name: t, IsDir: true, Mode: os.ModeDir | 0755})
	}

	for _, v := range views {
		entries = append(entries, Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0555})
	}

	return entries, nil
}

// readDirTable lists contents of a table directory.
// Shows rows (by primary key) plus capability directories.
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

	// List rows (limited by dir_listing_limit)
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	rows, err := o.db.ListRows(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list rows",
			Cause:   err,
		}
	}

	// Build entries: capability directories first, then rows
	entries := make([]Entry, 0, len(rows)+15)

	// Add capability directories
	capabilities := []string{
		DirAll, DirBy, DirDelete, DirExport, DirFilter, DirFirst,
		DirImport, DirIndexes, DirInfo, DirLast, DirModify, DirOrder, DirSample,
	}
	for _, cap := range capabilities {
		entries = append(entries, Entry{Name: cap, IsDir: true, Mode: os.ModeDir | 0755})
	}

	// Add rows as directories (row files like 1.json accessible but not listed)
	for _, rowPK := range rows {
		entries = append(entries, Entry{Name: rowPK, IsDir: true, Mode: os.ModeDir | 0755})
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

	entries := make([]Entry, len(columns))
	for i, col := range columns {
		entries[i] = Entry{Name: col.Name, IsDir: false, Mode: 0644}
	}

	return entries, nil
}

// readDirInfo lists the .info metadata directory.
func (o *Operations) readDirInfo(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	entries := []Entry{
		{Name: ".count", IsDir: false, Mode: 0444},
		{Name: ".ddl", IsDir: false, Mode: 0444},
		{Name: ".columns", IsDir: false, Mode: 0444},
		{Name: ".indexes", IsDir: false, Mode: 0444},
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
			entries[i] = Entry{Name: idx.Columns[0], IsDir: true, Mode: os.ModeDir | 0755}
		}
		return entries, nil
	}

	// List distinct values for the column
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	values, err := o.db.GetDistinctValues(ctx, fsCtx.Schema, fsCtx.TableName, parsed.CapabilityArg, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get distinct values",
			Cause:   err,
		}
	}

	entries := make([]Entry, len(values))
	for i, v := range values {
		entries[i] = Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0755}
	}
	return entries, nil
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
			entries[i] = Entry{Name: col.Name, IsDir: true, Mode: os.ModeDir | 0755}
		}
		return entries, nil
	}

	// List distinct values for the column
	limit := o.config.DirFilterLimit
	if limit <= 0 {
		limit = 100000
	}

	values, err := o.db.GetDistinctValues(ctx, fsCtx.Schema, fsCtx.TableName, parsed.CapabilityArg, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get distinct values",
			Cause:   err,
		}
	}

	entries := make([]Entry, len(values))
	for i, v := range values {
		entries[i] = Entry{Name: v, IsDir: true, Mode: os.ModeDir | 0755}
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

	// Show both ascending and descending options for each column
	entries := make([]Entry, 0, len(columns)*2)
	for _, col := range columns {
		entries = append(entries,
			Entry{Name: col.Name, IsDir: true, Mode: os.ModeDir | 0755},
			Entry{Name: col.Name + ".desc", IsDir: true, Mode: os.ModeDir | 0755},
		)
	}
	return entries, nil
}

// readDirPaginationCapability lists options for .first/, .last/, .sample/.
func (o *Operations) readDirPaginationCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	// Show common limit values
	limits := []string{"10", "25", "50", "100", "500", "1000"}
	entries := make([]Entry, len(limits))
	for i, l := range limits {
		entries[i] = Entry{Name: l, IsDir: true, Mode: os.ModeDir | 0755}
	}
	return entries, nil
}

// readDirExport lists format files in .export/ or .export/.with-headers/.
// Matches FUSE behavior: .with-headers/ directory plus format files.
func (o *Operations) readDirExport(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	if parsed.ExportWithHeaders {
		// .with-headers only shows csv and tsv (JSON/YAML have built-in keys)
		return []Entry{
			{Name: "csv", IsDir: false, Mode: 0444},
			{Name: "tsv", IsDir: false, Mode: 0444},
		}, nil
	}

	return []Entry{
		{Name: ".with-headers", IsDir: true, Mode: os.ModeDir | 0555},
		{Name: "csv", IsDir: false, Mode: 0444},
		{Name: "json", IsDir: false, Mode: 0444},
		{Name: "tsv", IsDir: false, Mode: 0444},
		{Name: "yaml", IsDir: false, Mode: 0444},
	}, nil
}

// readDirImport lists import modes in .import/.
func (o *Operations) readDirImport(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	entries := []Entry{
		{Name: DirSync, IsDir: true, Mode: os.ModeDir | 0755},
		{Name: DirOverwrite, IsDir: true, Mode: os.ModeDir | 0755},
		{Name: DirAppend, IsDir: true, Mode: os.ModeDir | 0755},
	}
	return entries, nil
}

// readDirDDL lists DDL staging directory contents.
func (o *Operations) readDirDDL(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	if parsed.DDLName == "" {
		// List staging directories (currently empty until operations are staged)
		return []Entry{}, nil
	}

	// List control files for a specific staging operation
	entries := []Entry{
		{Name: "sql", IsDir: false, Mode: 0644},
		{Name: ".test", IsDir: false, Mode: 0444},
		{Name: ".commit", IsDir: false, Mode: 0200},
		{Name: ".abort", IsDir: false, Mode: 0200},
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
	switch parsed.Type {
	case PathRoot:
		return &Entry{Name: "", IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathSchemaList:
		return &Entry{Name: ".schemas", IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathSchema:
		return &Entry{Name: parsed.Context.Schema, IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathTable:
		return &Entry{Name: parsed.Context.TableName, IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathRow:
		return o.statRow(ctx, parsed)

	case PathColumn:
		return o.statColumn(ctx, parsed)

	case PathInfo:
		if parsed.InfoFile == "" {
			return &Entry{Name: ".info", IsDir: true, Mode: os.ModeDir | 0755}, nil
		}
		return o.statInfoFile(ctx, parsed)

	case PathCapability:
		return &Entry{Name: parsed.CapabilityDir, IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathExport:
		return &Entry{Name: ".export", IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathImport:
		return &Entry{Name: ".import", IsDir: true, Mode: os.ModeDir | 0755}, nil

	case PathDDL:
		if parsed.DDLFile != "" {
			return o.statDDLFile(ctx, parsed)
		}
		name := "." + parsed.DDLOp
		return &Entry{Name: name, IsDir: true, Mode: os.ModeDir | 0755}, nil

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
			Name:  parsed.PrimaryKey,
			IsDir: true,
			Mode:  os.ModeDir | 0755,
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
		Name:  parsed.PrimaryKey + "." + parsed.Format,
		IsDir: false,
		Size:  size,
		Mode:  0644,
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

	// Calculate size
	data := fmt.Sprintf("%v\n", val)
	size := int64(len(data))

	return &Entry{
		Name:  parsed.Column,
		IsDir: false,
		Size:  size,
		Mode:  0600,
	}, nil
}

// statInfoFile returns metadata for an info file.
func (o *Operations) statInfoFile(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	// Info files are small metadata files, size varies
	return &Entry{
		Name:  parsed.InfoFile,
		IsDir: false,
		Mode:  0400,
	}, nil
}

// statDDLFile returns metadata for a DDL control file.
func (o *Operations) statDDLFile(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	var mode os.FileMode
	switch parsed.DDLFile {
	case "sql":
		mode = 0600
	case ".test":
		mode = 0400
	case ".commit", ".abort":
		mode = 0200
	default:
		mode = 0400
	}

	return &Entry{
		Name:  parsed.DDLFile,
		IsDir: false,
		Mode:  mode,
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
		Mode: 0644,
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

	// Format value with trailing newline
	data := fmt.Sprintf("%v\n", val)

	return &FileContent{
		Data: []byte(data),
		Size: int64(len(data)),
		Mode: 0644,
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
	case ".count":
		count, dbErr := o.db.GetRowCount(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get row count",
				Cause:   dbErr,
			}
		}
		data = strconv.FormatInt(count, 10) + "\n"

	case ".ddl":
		ddl, dbErr := o.db.GetFullDDL(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get DDL",
				Cause:   dbErr,
			}
		}
		data = ddl

	case ".columns":
		columns, dbErr := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get columns",
				Cause:   dbErr,
			}
		}
		for _, col := range columns {
			data += fmt.Sprintf("%s\t%s\n", col.Name, col.DataType)
		}

	case ".indexes":
		indexes, dbErr := o.db.GetIndexes(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get indexes",
				Cause:   dbErr,
			}
		}
		for _, idx := range indexes {
			unique := ""
			if idx.IsUnique {
				unique = "UNIQUE "
			}
			data += fmt.Sprintf("%s%s (%s)\n", unique, idx.Name, joinStrings(idx.Columns))
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
		Mode: 0444,
	}, nil
}

// readExportFile reads an export file (bulk data).
func (o *Operations) readExportFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for export path",
		}
	}

	// Get all rows
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	columns, rows, err := o.db.GetAllRows(ctx, fsCtx.Schema, fsCtx.TableName, limit)
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
		Mode: 0444,
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
