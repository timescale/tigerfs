package fs

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
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
	config    *config.Config
	db        db.DBClient
	staging   *StagingManager // Tracks partial rows for incremental creation
	ddl       *DDLManager     // Handles DDL staging operations
	metaCache *MetadataCache  // Shared metadata cache for tables/views/PKs

	// Schema caching: the current schema doesn't change during a mount session,
	// so we resolve it once and reuse. This eliminates ~4-6 DB queries per NFS RPC
	// that were caused by parsePath calling GetCurrentSchema on every operation.
	schemaOnce   sync.Once
	cachedSchema string
	schemaErr    error

	// synthState caches detected synth views per schema.
	// Lazily loaded, invalidated on .build/ or .format/ writes.
	synthState synthCacheState

	// statCache caches Entry metadata from ReadDir results.
	// Stat checks this before querying the DB. ReadFile bypasses it (always fresh).
	// Invalidated by write operations; 2-second TTL as safety net.
	statCache statCache
}

// NewOperations creates a new Operations instance.
func NewOperations(cfg *config.Config, dbClient db.DBClient) *Operations {
	return &Operations{
		config:    cfg,
		db:        dbClient,
		ddl:       NewDDLManager(dbClient, cfg.DDLGracePeriod),
		metaCache: NewMetadataCache(cfg, dbClient),
	}
}

// statCache caches Entry metadata from ReadDir results.
// Stat checks this before querying the DB. ReadFile bypasses it (always fresh).
// Invalidated by write operations; 2-second TTL as safety net.
// Used for both native tables (row directory entries) and synth views (file entries).
type statCache struct {
	mu     sync.RWMutex
	tables map[string]*tableCache // key: "schema\x00table"
}

// tableCache holds cached entries for a single table.
type tableCache struct {
	entries map[string]Entry // key: filename (matches parsed.PrimaryKey)
	created time.Time
}

// statCacheTTL is the maximum age of cached entries before they expire.
const statCacheTTL = 2 * time.Second

// lookup returns a cached entry if it exists and hasn't expired.
func (c *statCache) lookup(schema, table, filename string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.tables == nil {
		return Entry{}, false
	}

	key := schema + "\x00" + table
	tc := c.tables[key]
	if tc == nil {
		return Entry{}, false
	}

	if time.Since(tc.created) > statCacheTTL {
		return Entry{}, false
	}

	entry, ok := tc.entries[filename]
	return entry, ok
}

// prime stores entries from a ReadDir result into the cache.
func (c *statCache) prime(schema, table string, entries map[string]Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tables == nil {
		c.tables = make(map[string]*tableCache)
	}

	key := schema + "\x00" + table
	c.tables[key] = &tableCache{
		entries: entries,
		created: time.Now(),
	}
}

// set adds or updates a single entry in the cache.
// Unlike prime() which replaces the whole table cache, set() adds one entry.
// If the table cache doesn't exist or is expired, creates a new one with just this entry.
func (c *statCache) set(schema, table, filename string, entry Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tables == nil {
		c.tables = make(map[string]*tableCache)
	}

	key := schema + "\x00" + table
	tc := c.tables[key]
	if tc == nil || time.Since(tc.created) > statCacheTTL {
		c.tables[key] = &tableCache{
			entries: map[string]Entry{filename: entry},
			created: time.Now(),
		}
		return
	}
	tc.entries[filename] = entry
}

// invalidate clears cached entries for a table (called on writes).
// Also clears any row-level column caches (keys with "table/" prefix).
func (c *statCache) invalidate(schema, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tables == nil {
		return
	}

	key := schema + "\x00" + table
	delete(c.tables, key)

	// Also clear row-level column caches (e.g., "schema\x00table/rowPK")
	prefix := key + "/"
	for k := range c.tables {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(c.tables, k)
		}
	}
}

// MetaCache returns the shared metadata cache.
// Used by FUSE nodes that need access to cached table/view/PK metadata.
func (o *Operations) MetaCache() *MetadataCache {
	return o.metaCache
}

// GetDDLManager returns the DDL manager for direct access to DDL operations.
// This is useful for adapters that need to manage DDL sessions.
func (o *Operations) GetDDLManager() *DDLManager {
	return o.ddl
}

// parsePath parses a filesystem path and resolves the default schema.
//
// Unlike ParsePath (a pure function), this method uses the database connection
// to resolve the default schema. Paths like /users parse with an empty schema,
// which this method fills in from the connection's current_schema().
// Explicit schema paths (/.schemas/myschema/table) are left unchanged.
func (o *Operations) parsePath(ctx context.Context, path string) (*ParsedPath, *FSError) {
	parsed, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	if err := o.resolveSchema(ctx, parsed); err != nil {
		return nil, err
	}
	return parsed, nil
}

// resolveSchema fills in empty schema fields with the database connection's
// current_schema(). This is needed because ParsePath uses empty string as the
// default schema for root-level table paths (e.g., /users), since it has no
// database access. The actual schema depends on the connection's search_path.
//
// The schema is cached using sync.Once since it doesn't change during a mount
// session. This eliminates redundant DB queries on every parsePath call.
func (o *Operations) resolveSchema(ctx context.Context, parsed *ParsedPath) *FSError {
	if parsed.Context == nil || parsed.Context.Schema != "" {
		return nil // No context, or schema already set (explicit path like /.schemas/foo/)
	}
	o.schemaOnce.Do(func() {
		o.cachedSchema, o.schemaErr = o.db.GetCurrentSchema(ctx)
	})
	if o.schemaErr != nil {
		return &FSError{Code: ErrIO, Message: "failed to resolve current schema", Cause: o.schemaErr}
	}
	parsed.Context.Schema = o.cachedSchema
	return nil
}

// ReadDir lists directory contents for the given path.
func (o *Operations) ReadDir(ctx context.Context, path string) ([]Entry, *FSError) {
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return nil, err
	}
	o.resolveSynthHierarchy(ctx, parsed)

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
	if err := o.resolveSchema(ctx, parsed); err != nil {
		return nil, err
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
	case PathViewList:
		return o.readDirViews(ctx)
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
	case PathBuild:
		return o.readDirBuild(ctx, parsed)
	case PathFormat:
		return o.readDirFormat(ctx, parsed)
	case PathHistory:
		return o.readDirHistoryDispatch(ctx, parsed)
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
	// Get current schema (default is "public") - uses cached value
	o.schemaOnce.Do(func() {
		o.cachedSchema, o.schemaErr = o.db.GetCurrentSchema(ctx)
	})
	if o.schemaErr != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get current schema",
			Cause:   o.schemaErr,
		}
	}
	currentSchema := o.cachedSchema

	// Get tables from current schema (via cache)
	tables, err := o.metaCache.GetTablesForSchema(ctx, currentSchema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list tables",
			Cause:   err,
		}
	}

	// Get views from current schema (via cache)
	views, err := o.metaCache.GetViewsForSchema(ctx, currentSchema)
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
	// Note: .delete is NOT at root level - it's inside tables (/{table}/.delete/)
	// .views only contains .create (views themselves appear at root like tables)
	entries = append(entries,
		Entry{Name: ".build", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		Entry{Name: ".create", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		Entry{Name: ".schemas", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		Entry{Name: ".views", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
	)

	// Add tables
	for _, t := range tables {
		entries = append(entries, Entry{Name: t, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	}

	// Add views — synth views get 0755 (writable), others get 0555
	for _, v := range views {
		mode := os.FileMode(0555)
		if info := o.getSynthViewInfo(ctx, currentSchema, v); info != nil {
			mode = 0755
		}
		entries = append(entries, Entry{Name: v, IsDir: true, Mode: os.ModeDir | mode, ModTime: now})
	}

	return entries, nil
}

// readDirSchemaList lists all schemas (/.schemas/).
// Includes .create for schema creation DDL.
func (o *Operations) readDirSchemaList(ctx context.Context) ([]Entry, *FSError) {
	schemas, err := o.metaCache.GetSchemas(ctx)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list schemas",
			Cause:   err,
		}
	}

	now := time.Now()
	entries := make([]Entry, 0, len(schemas)+1)

	// Add .create first for schema creation DDL
	entries = append(entries, Entry{Name: ".create", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})

	for _, s := range schemas {
		entries = append(entries, Entry{Name: s, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	}

	return entries, nil
}

// readDirViews lists the /.views/ directory.
// Only shows .create since views themselves appear at root level alongside tables.
func (o *Operations) readDirViews(ctx context.Context) ([]Entry, *FSError) {
	now := time.Now()
	entries := []Entry{
		{Name: ".create", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
	}
	return entries, nil
}

// readDirSchema lists tables in a specific schema.
// Includes .delete for schema deletion DDL.
func (o *Operations) readDirSchema(ctx context.Context, schema string) ([]Entry, *FSError) {
	tables, err := o.metaCache.GetTablesForSchema(ctx, schema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to list tables in schema %s", schema),
			Cause:   err,
		}
	}

	views, err := o.metaCache.GetViewsForSchema(ctx, schema)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("failed to list views in schema %s", schema),
			Cause:   err,
		}
	}

	now := time.Now()
	entries := make([]Entry, 0, len(tables)+len(views)+2)

	// Add special directories for schema
	entries = append(entries,
		Entry{Name: ".build", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		Entry{Name: ".delete", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
	)

	for _, t := range tables {
		entries = append(entries, Entry{Name: t, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
	}

	for _, v := range views {
		mode := os.FileMode(0555)
		if info := o.getSynthViewInfo(ctx, schema, v); info != nil {
			mode = 0755
		}
		entries = append(entries, Entry{Name: v, IsDir: true, Mode: os.ModeDir | mode, ModTime: now})
	}

	return entries, nil
}

// readDirTable lists contents of a table directory.
// Shows rows (by primary key) plus capability directories.
// Respects pipeline context (filters, order, limits) when present.
// For synthesized views, lists synthesized filenames as file entries instead.
func (o *Operations) readDirTable(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for table path",
		}
	}

	// Check if this is a synthesized view
	if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil {
		return o.readDirSynthView(ctx, parsed, info)
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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
			DirAll, DirBy, DirColumns, DirDelete, DirExport, DirFilter, DirFirst,
			DirFormat, DirImport, DirIndexes, DirInfo, DirLast, DirModify, DirOrder, DirSample,
		}
		for _, cap := range capabilities {
			entries = append(entries, Entry{Name: cap, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
		}
	}

	// Add rows as directories (row files like 1.json accessible but not listed)
	cacheEntries := make(map[string]Entry, len(rows))
	for _, rowPK := range rows {
		entry := Entry{Name: rowPK, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
		entries = append(entries, entry)
		cacheEntries[rowPK] = entry
	}

	// Prime the stat cache so subsequent Stat calls (from ls -l, NFS GETATTR, etc.)
	// return cached entries instead of issuing per-row SELECT queries.
	o.statCache.prime(fsCtx.Schema, fsCtx.TableName, cacheEntries)

	return entries, nil
}

// readDirRow lists columns in a row directory.
// For synth views with hierarchy, lists hierarchical subdirectory children instead.
func (o *Operations) readDirRow(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Synth hierarchy: list subdirectory children instead of columns
	if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil && info.SupportsHierarchy {
		return o.readDirSynthHierarchical(ctx, parsed, info)
	}

	columns, err := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	// Filter columns by projection when .columns/ is active.
	// This makes `ls .columns/id,status/<pk>/` show only projected column files.
	if fsCtx.HasColumns && len(fsCtx.Columns) > 0 {
		colSet := make(map[string]bool, len(fsCtx.Columns))
		for _, c := range fsCtx.Columns {
			colSet[c] = true
		}
		filtered := columns[:0]
		for _, col := range columns {
			if colSet[col.Name] {
				filtered = append(filtered, col)
			}
		}
		columns = filtered
	}

	now := time.Now()

	// Include row export format files (.json, .tsv, .csv, .yaml)
	entries := []Entry{
		{Name: ".json", IsDir: false, Mode: 0444, ModTime: now},
		{Name: ".tsv", IsDir: false, Mode: 0444, ModTime: now},
		{Name: ".csv", IsDir: false, Mode: 0444, ModTime: now},
		{Name: ".yaml", IsDir: false, Mode: 0444, ModTime: now},
	}

	// Try to fetch row data for accurate column file sizes.
	// This avoids per-column GetColumn queries during ls -l.
	var valueMap map[string]interface{}
	pk, pkErr := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if pkErr == nil {
		row, rowErr := o.db.GetRow(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey)
		if rowErr == nil && len(row.Columns) == len(row.Values) {
			valueMap = make(map[string]interface{}, len(row.Columns))
			for i, col := range row.Columns {
				valueMap[col] = row.Values[i]
			}
		}
	}

	// Build column file entries, with sizes if row data is available
	cacheEntries := make(map[string]Entry, len(columns))
	for _, col := range columns {
		filename := col.Name
		if !o.config.NoFilenameExtensions {
			filename = AddExtensionToColumn(col.Name, col.DataType)
		}
		var size int64
		if valueMap != nil {
			if val, ok := valueMap[col.Name]; ok {
				if str, fmtErr := format.ConvertValueToText(val); fmtErr == nil {
					size = int64(len(str) + 1) // +1 for trailing newline
				}
			}
		}
		entry := Entry{Name: filename, IsDir: false, Size: size, Mode: 0600, ModTime: now}
		entries = append(entries, entry)
		cacheEntries[filename] = entry
	}

	// Prime row-level stat cache so statColumn can use it
	if len(cacheEntries) > 0 {
		o.statCache.prime(fsCtx.Schema, fsCtx.TableName+"/"+parsed.PrimaryKey, cacheEntries)
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
	case DirColumns:
		return o.readDirColumnsCapability(ctx, parsed)
	case DirFilter:
		return o.readDirFilterCapability(ctx, parsed)
	case DirOrder:
		return o.readDirOrderCapability(ctx, parsed)
	case DirFirst, DirLast, DirSample:
		// These show numeric directories for limit values
		return o.readDirPaginationCapability(ctx, parsed)
	case DirIndexes:
		return o.readDirIndexesCapability(ctx, parsed)
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

// readDirColumnsCapability lists available column names for .columns/ navigation.
func (o *Operations) readDirColumnsCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for .columns path",
		}
	}

	columns, err := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	now := time.Now()
	entries := make([]Entry, len(columns))
	for i, col := range columns {
		entries[i] = Entry{Name: col.Name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}
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

	now := time.Now()

	if parsed.CapabilityArg == "" {
		// List all columns
		columns, err := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
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

	columns, err := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
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

// readDirIndexesCapability lists indexes for /{table}/.indexes/.
// Includes .create for index creation DDL.
func (o *Operations) readDirIndexesCapability(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for .indexes path",
		}
	}

	now := time.Now()

	// Get all indexes for this table
	indexes, err := o.db.GetIndexes(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get indexes",
			Cause:   err,
		}
	}

	entries := make([]Entry, 0, len(indexes)+1)

	// Add .create first for index creation DDL
	entries = append(entries, Entry{Name: ".create", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})

	// Add existing indexes
	for _, idx := range indexes {
		entries = append(entries, Entry{Name: idx.Name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now})
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
				ModTime: session.UpdatedAt,
			}
		}
		return entries, nil
	}

	// For a specific staging directory, ensure session exists
	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return nil, &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
	}

	// Auto-create session for table-level DDL (modify/delete on existing tables)
	// This matches FUSE behavior where accessing /table/.modify/ works without explicit mkdir
	if err := o.ensureDDLSession(parsed, op); err != nil {
		return nil, err
	}

	// Use session's UpdatedAt for stable mtime
	modTime := time.Now() // fallback
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID != "" {
		if s := o.ddl.GetSession(sessionID); s != nil {
			modTime = s.UpdatedAt
		}
	}

	// List control files for a specific staging operation
	// Note: Mode and Size must match what statDDLFile returns for NFS consistency
	entries := []Entry{
		{Name: "sql", IsDir: false, Mode: 0600, ModTime: modTime},
		{Name: ".test", IsDir: false, Mode: 0644, Size: 0, ModTime: modTime},
		{Name: ".commit", IsDir: false, Mode: 0644, Size: 0, ModTime: modTime},
		{Name: ".abort", IsDir: false, Mode: 0644, Size: 0, ModTime: modTime},
	}

	// Include test.log if a test has been run (matches FUSE StagingDirNode behavior)
	if sessionID != "" {
		if log := o.ddl.GetTestLog(sessionID); log != "" {
			entries = append(entries, Entry{
				Name:    FileTestLog,
				IsDir:   false,
				Mode:    0444,
				Size:    int64(len(log)),
				ModTime: modTime,
			})
		}

		// Include editor temp files (swap files, backups, etc.)
		for _, name := range o.ddl.ListExtraFiles(sessionID) {
			ef := o.ddl.GetExtraFileInfo(sessionID, name)
			if ef != nil {
				entries = append(entries, Entry{
					Name:    name,
					IsDir:   false,
					Mode:    0644,
					Size:    int64(len(ef.Data)),
					ModTime: ef.ModTime,
				})
			}
		}
	}

	return entries, nil
}

// ensureDDLSession ensures a DDL session exists, auto-creating for table-level DDL.
// For table-level DDL (modify/delete on existing tables, index operations), sessions are
// auto-created on access to match FUSE behavior. For root-level create (/.create/), explicit
// mkdir is required since the object doesn't exist yet.
func (o *Operations) ensureDDLSession(parsed *ParsedPath, op DDLOpType) *FSError {
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID != "" {
		s := o.ddl.GetSession(sessionID)
		if s != nil && !s.Completed {
			return nil // Active session exists
		}
		// Completed session — for auto-created (table-level) DDL, replace with
		// a fresh session. For root-level creates, keep the completed session
		// so post-commit stat/readdir operations still succeed.
		canAutoCreate := parsed.Context != nil || parsed.DDLObjectType == "schema" || parsed.DDLObjectType == "index" || parsed.DDLObjectType == "view"
		if canAutoCreate {
			o.ddl.RemoveSession(sessionID)
			// Fall through to auto-creation below
		} else {
			return nil // Root-level create — completed session suffices
		}
	}

	// Auto-create for table-level DDL where we have context (table exists)
	// This includes: /{table}/.modify/, /{table}/.delete/, /{table}/.indexes/.create/{idx}/,
	// /{table}/.indexes/{idx}/.delete/, /.schemas/{schema}/.delete/, /.views/.create/{name}/
	if parsed.Context != nil || parsed.DDLObjectType == "schema" || parsed.DDLObjectType == "index" || parsed.DDLObjectType == "view" {
		objectType := parsed.DDLObjectType
		if objectType == "" {
			objectType = "table"
		}

		schema := "public"
		if objectType == "schema" {
			schema = ""
		} else if parsed.Context != nil && parsed.Context.Schema != "" {
			schema = parsed.Context.Schema
		}

		_, err := o.ddl.CreateSession(op, objectType, schema, parsed.DDLName, parsed.DDLParentTable)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to create DDL session",
				Cause:   err,
			}
		}
		return nil
	}

	// For root-level create (/.create/name), require explicit mkdir
	return &FSError{
		Code:    ErrNotExist,
		Message: fmt.Sprintf("no DDL session for %s", parsed.DDLName),
		Hint:    fmt.Sprintf("create session with: mkdir /.%s/%s", parsed.DDLOp, parsed.DDLName),
	}
}

// Stat returns metadata for a path.
func (o *Operations) Stat(ctx context.Context, path string) (*Entry, *FSError) {
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return nil, err
	}
	o.resolveSynthHierarchy(ctx, parsed)

	return o.statWithParsed(ctx, parsed, path)
}

// StatWithContext returns metadata using a pre-parsed context.
func (o *Operations) StatWithContext(ctx context.Context, fsCtx *FSContext) (*Entry, *FSError) {
	parsed := &ParsedPath{
		Type:    PathTable,
		Context: fsCtx,
	}
	if err := o.resolveSchema(ctx, parsed); err != nil {
		return nil, err
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
		// Verify the schema actually exists (via cache)
		schemaName := parsed.Context.Schema
		exists, err := o.metaCache.HasSchema(ctx, schemaName)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to list schemas", Cause: err}
		}
		if !exists {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("schema %s not found", schemaName)}
		}
		return &Entry{Name: schemaName, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathViewList:
		return &Entry{Name: ".views", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathTable:
		fsCtx := parsed.Context
		if fsCtx == nil {
			return nil, &FSError{
				Code:    ErrInvalidPath,
				Message: "missing context for table path",
			}
		}
		// Verify table or view exists using the metadata cache.
		// This replaces the previous pattern of GetPrimaryKey + GetViews fallback,
		// turning 2 DB queries into a single cache lookup (0 DB queries on cache hit).
		isTable, isView, err := o.metaCache.HasTableOrViewInSchema(ctx, fsCtx.Schema, fsCtx.TableName)
		if err != nil {
			return nil, &FSError{
				Code:    ErrNotExist,
				Message: fmt.Sprintf("table or view not found: %s.%s", fsCtx.Schema, fsCtx.TableName),
				Cause:   err,
			}
		}
		if !isTable && !isView {
			return nil, &FSError{
				Code:    ErrNotExist,
				Message: fmt.Sprintf("table or view not found: %s.%s", fsCtx.Schema, fsCtx.TableName),
			}
		}
		// Validate projected column names if .columns/ is active.
		// This makes `cd .columns/nonexistent/` fail with ENOENT immediately
		// rather than silently succeeding and only failing at export time.
		if fsCtx.HasColumns && len(fsCtx.Columns) > 0 {
			columns, colErr := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
			if colErr != nil {
				return nil, &FSError{
					Code:    ErrIO,
					Message: "failed to get columns for validation",
					Cause:   colErr,
				}
			}
			validCols := make(map[string]bool, len(columns))
			for _, col := range columns {
				validCols[col.Name] = true
			}
			for _, name := range fsCtx.Columns {
				if !validCols[name] {
					return nil, &FSError{
						Code:    ErrNotExist,
						Message: fmt.Sprintf("column %q not found in %s.%s", name, fsCtx.Schema, fsCtx.TableName),
					}
				}
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
		// For specific resources (e.g., /table/.indexes/idx_name), verify they exist
		if parsed.CapabilityArg != "" && parsed.CapabilityDir == DirIndexes {
			return o.statIndexEntry(ctx, parsed)
		}
		return &Entry{Name: parsed.CapabilityDir, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil

	case PathExport:
		return o.statExport(ctx, parsed)

	case PathImport:
		return o.statImport(ctx, parsed)

	case PathBuild:
		return o.statBuild(ctx, parsed)

	case PathFormat:
		return o.statFormat(ctx, parsed)

	case PathHistory:
		return o.statHistoryDispatch(ctx, parsed)

	case PathDDL:
		if parsed.DDLFile != "" {
			return o.statDDLFile(ctx, parsed)
		}
		// If DDLName is set, this is a staging directory - ensure session exists
		if parsed.DDLName != "" {
			op, valid := ParseDDLOpType(parsed.DDLOp)
			if !valid {
				return nil, &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
			}
			// Auto-create session for table-level DDL (modify/delete on existing tables)
			if err := o.ensureDDLSession(parsed, op); err != nil {
				return nil, err
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
// For synthesized views, always returns a file stat.
func (o *Operations) statRow(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Check if this is a synthesized view — synth files are always files, not directories
	if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil {
		return o.statSynthFile(ctx, parsed, info)
	}

	// For directory stats (no format extension), check ReadDir-primed cache first.
	// This avoids per-row SELECT queries during ls -l / NFS GETATTR bursts.
	if parsed.Format == "" {
		if entry, ok := o.statCache.lookup(fsCtx.Schema, fsCtx.TableName, parsed.PrimaryKey); ok {
			return &entry, nil
		}
	}

	now := time.Now()

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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
		entry := Entry{
			Name:    parsed.PrimaryKey,
			IsDir:   true,
			Mode:    os.ModeDir | 0755,
			ModTime: now,
		}
		// Self-prime cache so subsequent statRow calls hit cache
		o.statCache.set(fsCtx.Schema, fsCtx.TableName, parsed.PrimaryKey, entry)
		return &entry, nil
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
// Resolves the filename (which may have an extension like "name.txt") to the
// actual column name, and returns the Entry with the filename as provided.
func (o *Operations) statColumn(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for column path",
		}
	}

	// Check readDirRow-primed cache for this column file.
	// This avoids per-column DB queries during ls -l in row directories.
	if entry, ok := o.statCache.lookup(fsCtx.Schema, fsCtx.TableName+"/"+parsed.PrimaryKey, parsed.Column); ok {
		return &entry, nil
	}

	now := time.Now()

	// Get columns to resolve the filename
	columns, err := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	// Resolve filename to actual column name (handles extensions)
	actualColumn, found := o.resolveColumn(columns, parsed.Column)
	if !found {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("column not found: %s", parsed.Column),
		}
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	// Get column value using the resolved column name
	val, err := o.db.GetColumn(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey, actualColumn)
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

	// Return Entry with the filename as provided (may include extension)
	return &Entry{
		Name:    parsed.Column,
		IsDir:   false,
		Size:    size,
		Mode:    0600,
		ModTime: now,
	}, nil
}

// resolveColumn finds a column by filename, handling extensions.
// Returns the actual column name and whether it was found.
//
// When NoFilenameExtensions is enabled, only exact matches are allowed.
// Otherwise, uses FindColumnByFilename which supports both exact matches
// and extension-stripped matches (e.g., "name.txt" → "name" for TEXT columns).
func (o *Operations) resolveColumn(columns []db.Column, filename string) (string, bool) {
	if o.config.NoFilenameExtensions {
		// Exact match only
		for _, col := range columns {
			if col.Name == filename {
				return col.Name, true
			}
		}
		return "", false
	}
	col, found := FindColumnByFilename(columns, filename)
	if found {
		return col.Name, true
	}
	return "", false
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

// statIndexEntry returns metadata for a specific index directory.
// Checks if the index actually exists in the database.
func (o *Operations) statIndexEntry(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for index path",
		}
	}

	// Query database for indexes on this table
	indexes, err := o.db.GetIndexes(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get indexes",
			Cause:   err,
		}
	}

	// Check if the specific index exists
	indexName := parsed.CapabilityArg
	for _, idx := range indexes {
		if idx.Name == indexName {
			return &Entry{
				Name:    indexName,
				IsDir:   true,
				Mode:    os.ModeDir | 0755,
				ModTime: time.Now(),
			}, nil
		}
	}

	return nil, &FSError{
		Code:    ErrNotExist,
		Message: fmt.Sprintf("index %s not found", indexName),
	}
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
		// Use session's UpdatedAt for stable mtime. Fall back to epoch if session
		// was already removed (post-commit/abort NFS compliance path).
		modTime := time.Unix(0, 0)
		if sid := o.ddl.FindSessionByName(op, parsed.DDLName); sid != "" {
			if s := o.ddl.GetSession(sid); s != nil {
				modTime = s.UpdatedAt
			}
		}
		return &Entry{
			Name:    parsed.DDLFile,
			IsDir:   false,
			Mode:    0644, // Read-write for NFS compatibility
			Size:    0,    // Matches actual empty content
			ModTime: modTime,
		}, nil
	}

	// For other files (sql, test.log), ensure session exists (auto-create for table-level DDL)
	if err := o.ensureDDLSession(parsed, op); err != nil {
		return nil, err
	}

	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)

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
		// Check for editor temp files (swap files, backups, etc.)
		ef := o.ddl.GetExtraFileInfo(sessionID, parsed.DDLFile)
		if ef != nil {
			mode = 0644
			size = int64(len(ef.Data))
			// Return with the extra file's own mtime
			return &Entry{
				Name:    parsed.DDLFile,
				IsDir:   false,
				Mode:    mode,
				Size:    size,
				ModTime: ef.ModTime,
			}, nil
		}
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("unknown DDL file: %s", parsed.DDLFile)}
	}

	// Use session's UpdatedAt for stable mtime instead of time.Now()
	modTime := time.Now() // fallback (should not happen since session was just found)
	if s := o.ddl.GetSession(sessionID); s != nil {
		modTime = s.UpdatedAt
	}

	return &Entry{
		Name:    parsed.DDLFile,
		IsDir:   false,
		Mode:    mode,
		Size:    size,
		ModTime: modTime,
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
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return nil, err
	}
	o.resolveSynthHierarchy(ctx, parsed)

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
	case PathBuild, PathFormat:
		// Write-only virtual files. Return empty content so that NFS SETATTR
		// (which calls OpenFile→ReadFile during Apply's truncate path) succeeds
		// instead of returning ErrInvalidPath that becomes EBADRPC.
		return &FileContent{Data: []byte{}}, nil
	case PathHistory:
		return o.readHistoryFileDispatch(ctx, parsed)
	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot read file for path type: %d", parsed.Type),
		}
	}
}

// readRowFile reads a row file in the specified format.
// For synthesized views, returns the synthesized content (markdown/plaintext).
func (o *Operations) readRowFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Check if this is a synthesized view
	if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil {
		data, fsErr := o.readFileSynthView(ctx, parsed, info)
		if fsErr != nil {
			return nil, fsErr
		}
		return &FileContent{Data: data}, nil
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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
// The filename may include an extension (e.g., "name.txt" for a TEXT column),
// which is resolved to the actual column name before querying.
func (o *Operations) readColumnFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for column path",
		}
	}

	// Get columns to resolve the filename
	columns, err := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	// Resolve filename to actual column name (handles extensions)
	actualColumn, found := o.resolveColumn(columns, parsed.Column)
	if !found {
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("column not found: %s", parsed.Column),
		}
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	// Get column value using the resolved column name
	val, err := o.db.GetColumn(ctx, fsCtx.Schema, fsCtx.TableName, pk.Columns[0], parsed.PrimaryKey, actualColumn)
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
		columns, dbErr := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
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

	// Validate column names if column projection is specified
	if len(fsCtx.Columns) > 0 {
		tableColumns, colErr := o.metaCache.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
		if colErr != nil {
			return nil, &FSError{
				Code:    ErrIO,
				Message: "failed to get columns for validation",
				Cause:   colErr,
			}
		}
		validCols := make(map[string]bool, len(tableColumns))
		for _, c := range tableColumns {
			validCols[c.Name] = true
		}
		for _, col := range fsCtx.Columns {
			if !validCols[col] {
				return nil, &FSError{
					Code:    ErrNotExist,
					Message: fmt.Sprintf("column %q does not exist in table %q", col, fsCtx.TableName),
					Hint:    "check column names with: ls .columns/",
				}
			}
		}
	}

	var columns []string
	var rows [][]interface{}
	var err error

	// Check if we have any pipeline operations (filters, order, limits)
	if fsCtx.HasPipelineOperations() {
		// Get primary key for pipeline query ordering
		pk, pkErr := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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

	// Ensure session exists (auto-create for table-level DDL)
	if err := o.ensureDDLSession(parsed, op); err != nil {
		return nil, err
	}

	// Find session by name
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)

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
		// Check for editor temp files (swap files, backups, etc.)
		if o.ddl.HasExtraFile(sessionID, parsed.DDLFile) {
			data := o.ddl.GetExtraFile(sessionID, parsed.DDLFile)
			return &FileContent{
				Data: data,
				Size: int64(len(data)),
				Mode: 0644,
			}, nil
		}
		return nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("DDL file not found: %s", parsed.DDLFile),
		}
	}
}
