package fs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// synthCacheState holds the synth view cache for the Operations struct.
// Lazily loaded per schema on first access.
type synthCacheState struct {
	mu    sync.Mutex
	cache map[string]map[string]*synth.ViewInfo // schema → viewName → ViewInfo
}

// getSynthViewInfo returns the synth view info for a view, or nil if it's not a synth view.
// Loads the cache lazily for the given schema.
func (o *Operations) getSynthViewInfo(ctx context.Context, schema, viewName string) *synth.ViewInfo {
	o.synthState.mu.Lock()
	defer o.synthState.mu.Unlock()

	if o.synthState.cache == nil {
		o.synthState.cache = make(map[string]map[string]*synth.ViewInfo)
	}

	// Check if schema is already cached
	schemaCache, loaded := o.synthState.cache[schema]
	if !loaded {
		// Load cache for this schema
		var err error
		schemaCache, err = o.loadSynthCache(ctx, schema)
		if err != nil {
			logging.Debug("failed to load synth cache",
				zap.String("schema", schema),
				zap.Error(err))
			// Store empty cache so we don't retry
			schemaCache = make(map[string]*synth.ViewInfo)
		}
		o.synthState.cache[schema] = schemaCache
	}

	return schemaCache[viewName]
}

// invalidateSynthCache clears the synth cache (called after .build/ or .format/ writes).
func (o *Operations) invalidateSynthCache() {
	o.synthState.mu.Lock()
	defer o.synthState.mu.Unlock()
	o.synthState.cache = nil
}

// loadSynthCache queries the database to detect all synth views in a schema.
func (o *Operations) loadSynthCache(ctx context.Context, schema string) (map[string]*synth.ViewInfo, error) {
	// Capture a stable timestamp for all views in this schema.
	// Used as fallback mtime when no timestamp column is available.
	mountTime := time.Now()

	// Get all views
	views, err := o.metaCache.GetViewsForSchema(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to list views: %w", err)
	}

	// Batch-query all view comments
	comments, err := o.db.GetViewCommentsBatch(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to load view comments: %w", err)
	}

	cache := make(map[string]*synth.ViewInfo)

	for _, viewName := range views {
		// Try comment-based detection first (most reliable)
		var format synth.SynthFormat
		var hasHistory bool
		if comment, ok := comments[viewName]; ok && comment != "" {
			features := synth.DetectFeaturesFromComment(comment)
			format = features.Format
			hasHistory = features.History
		}

		// Fall back to suffix + column detection
		if format == synth.FormatNative {
			cols, err := o.metaCache.GetColumns(ctx, schema, viewName)
			if err != nil {
				continue
			}
			colNames := make([]string, len(cols))
			for i, c := range cols {
				colNames[i] = c.Name
			}
			format = synth.DetectFormat(viewName, colNames)
		}

		// Skip non-synth and unsupported views
		if format == synth.FormatNative || format == synth.FormatTasks {
			continue
		}

		// Get columns for role detection
		cols, err := o.metaCache.GetColumns(ctx, schema, viewName)
		if err != nil {
			continue
		}
		colNames := make([]string, len(cols))
		for i, c := range cols {
			colNames[i] = c.Name
		}

		// Detect PK column (views might not have one, try anyway).
		// Synth views always have single-column PKs; use first column for role detection.
		pkColumn := "id"
		pk, err := o.metaCache.GetPrimaryKey(ctx, schema, viewName)
		if err == nil {
			pkColumn = pk.Columns[0]
		}

		// Detect column roles
		roles, err := synth.DetectColumnRoles(colNames, format, pkColumn)
		if err != nil {
			continue
		}

		// Fallback history detection: check if companion history table exists in tigerfs schema
		if !hasHistory {
			historyTable := viewName + "_history"
			exists, tblErr := o.db.TableExists(ctx, synth.TigerFSSchema, historyTable)
			if tblErr == nil && exists {
				hasHistory = true
			}
		}

		info := &synth.ViewInfo{
			Format:            format,
			Roles:             roles,
			CachedMountTime:   mountTime,
			SupportsHierarchy: roles.Filetype != "",
			HasHistory:        hasHistory,
		}
		cache[viewName] = info

		// Warn if this view uses the legacy directory model (has filetype but no parent_id).
		// The relational-directories migration adds parent_id for improved performance.
		if info.SupportsHierarchy && roles.ParentID == "" {
			var dirPath string
			if o.mountPoint != "" {
				if schema == o.cachedSchema {
					// Default schema: tables at mount root
					dirPath = o.mountPoint + "/" + viewName
				} else {
					// Non-default schema: under .schemas/
					dirPath = o.mountPoint + "/" + DirSchemas + "/" + schema + "/" + viewName
				}
			} else {
				dirPath = viewName
			}
			logging.Warn("legacy directory format detected",
				zap.String("directory", dirPath),
				zap.String("hint", "run 'tigerfs migrate' to upgrade"))
		}
	}

	return cache, nil
}

// resolveSynthHierarchy converts PathColumn → PathRow for synth views with hierarchy.
// For deep paths like /memory/projects/web/todo.md, the parser produces
// PathColumn(PK=projects, Column=web, RawSubPath=[projects,web,todo.md]).
// This method detects synth views with hierarchy support and converts to
// PathRow(PK="projects/web/todo.md") so existing synth hooks handle the rest.
// No-op for non-synth views, views without hierarchy, or non-PathColumn paths.
func (o *Operations) resolveSynthHierarchy(ctx context.Context, parsed *ParsedPath) {
	if parsed.Context == nil || parsed.Type != PathColumn {
		return
	}
	info := o.getSynthViewInfo(ctx, parsed.Context.Schema, parsed.Context.TableName)
	if info == nil || !info.SupportsHierarchy {
		return
	}
	// Convert PathColumn → PathRow with full hierarchical filename
	parsed.Type = PathRow
	parsed.PrimaryKey = strings.Join(parsed.RawSubPath, "/")
	parsed.Column = ""
	parsed.Format = ""
}

// extractModTime returns the best available modification time for a synth row.
// It checks the ModifiedAt column first, then CreatedAt, falling back to
// ViewInfo.CachedMountTime (a stable timestamp captured when the cache was loaded).
func extractModTime(columns []string, values []interface{}, info *synth.ViewInfo) time.Time {
	for _, target := range []string{info.Roles.ModifiedAt, info.Roles.CreatedAt} {
		if target == "" {
			continue
		}
		for i, col := range columns {
			if col == target {
				if t, ok := values[i].(time.Time); ok {
					return t
				}
			}
		}
	}
	return info.CachedMountTime
}

// primeSynthStatCache populates the stat cache from row data.
// pathPrefix is prepended to leaf filenames for the cache key (e.g., "projects/web"
// for subdirectory entries). Empty for root-level entries.
func (o *Operations) primeSynthStatCache(schema, table, pathPrefix string, columns []string, rows [][]interface{}, info *synth.ViewInfo) {
	entries := make(map[string]Entry, len(rows))

	for _, row := range rows {
		// For hierarchical views, check filetype
		if info.SupportsHierarchy {
			filetypeIdx := -1
			for i, col := range columns {
				if col == info.Roles.Filetype {
					filetypeIdx = i
					break
				}
			}
			if filetypeIdx >= 0 && synth.ValueToString(row[filetypeIdx]) == "directory" {
				// Cache directory entry keyed by its full path
				leafName := synth.ValueToString(row[findColIdx(columns, info.Roles.Filename)])
				fullPath := joinPathPrefix(pathPrefix, leafName)
				modTime := extractModTime(columns, row, info)
				entries[fullPath] = Entry{
					Name:    leafName,
					IsDir:   true,
					Mode:    0755,
					ModTime: modTime,
				}
				continue
			}
		}

		// File row: synthesize filename and content size
		var filename string
		switch info.Format {
		case synth.FormatMarkdown:
			filename = synth.GetMarkdownFilename(columns, row, info.Roles)
		case synth.FormatPlainText:
			filename = synth.GetPlainTextFilename(columns, row, info.Roles)
		default:
			continue
		}

		fullPath := joinPathPrefix(pathPrefix, filename)
		modTime := extractModTime(columns, row, info)
		var size int64
		if content, err := o.synthesizeContent(columns, row, info); err == nil {
			size = int64(len(content))
		}

		entries[fullPath] = Entry{
			Name:    filename,
			IsDir:   false,
			Mode:    0644,
			Size:    size,
			ModTime: modTime,
		}
	}

	o.statCache.prime(schema, table, entries)
}

// joinPathPrefix joins a directory prefix with a leaf name.
// Returns leaf unchanged when prefix is empty (root level).
func joinPathPrefix(prefix, leaf string) string {
	if prefix == "" {
		return leaf
	}
	return prefix + "/" + leaf
}

// findColIdx returns the index of a column name, or -1 if not found.
func findColIdx(columns []string, name string) int {
	for i, col := range columns {
		if col == name {
			return i
		}
	}
	return -1
}

// readDirSynthView lists synthesized filenames as file entries.
// For views with hierarchy support, shows only root-level entries (files and directories).
func (o *Operations) readDirSynthView(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]Entry, *FSError) {
	fsCtx := parsed.Context

	// Get all rows from the view
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	// Parent-pointer model (ADR-017): query only root-level entries
	if info.SupportsHierarchy && info.Roles.ParentID != "" {
		columns, rows, err := o.db.GetRowsByParent(ctx, fsCtx.Schema, fsCtx.TableName, "", limit)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to list root entries", Cause: err}
		}
		o.primeSynthStatCache(fsCtx.Schema, fsCtx.TableName, "", columns, rows, info)
		children := o.buildEntriesFromRows(columns, rows, info)
		if info.HasHistory {
			children = append([]Entry{{Name: DirHistory, IsDir: true, Mode: os.ModeDir | 0555, ModTime: info.CachedMountTime}}, children...)
		}
		return children, nil
	}

	columns, rows, err := o.db.GetAllRows(ctx, fsCtx.Schema, fsCtx.TableName, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list synth view rows",
			Cause:   err,
		}
	}

	// Prime Stat cache so subsequent Stat calls avoid DB queries
	o.primeSynthStatCache(fsCtx.Schema, fsCtx.TableName, "", columns, rows, info)

	// Old hierarchy model (path-encoded filenames, pre-ADR-017): filter to root-level
	if info.SupportsHierarchy {
		children := o.filterHierarchicalChildren(columns, rows, "", info)
		if info.HasHistory {
			children = append([]Entry{{Name: DirHistory, IsDir: true, Mode: os.ModeDir | 0555, ModTime: info.CachedMountTime}}, children...)
		}
		return children, nil
	}

	entries := make([]Entry, 0, len(rows)+1)

	// Add .history/ if versioned history is enabled
	if info.HasHistory {
		entries = append(entries, Entry{Name: DirHistory, IsDir: true, Mode: os.ModeDir | 0555, ModTime: info.CachedMountTime})
	}

	for _, row := range rows {
		var filename string
		switch info.Format {
		case synth.FormatMarkdown:
			filename = synth.GetMarkdownFilename(columns, row, info.Roles)
		case synth.FormatPlainText:
			filename = synth.GetPlainTextFilename(columns, row, info.Roles)
		default:
			continue
		}

		modTime := extractModTime(columns, row, info)
		// Synthesize content to get accurate size (CPU-only, no DB query)
		var size int64
		if content, err := o.synthesizeContent(columns, row, info); err == nil {
			size = int64(len(content))
		}
		entries = append(entries, Entry{
			Name:    filename,
			IsDir:   false,
			Mode:    0644,
			Size:    size,
			ModTime: modTime,
		})
	}

	return entries, nil
}

// fetchSynthRowByPath resolves a parent path via cache, then fetches the leaf row
// with a combined parent_id + filename query. This is one round-trip for the leaf
// (vs resolve_path + GetRow = two round-trips). Used by ReadFile where the leaf
// must always be fetched fresh from DB (consistency model: "ReadFile must always hit the DB").
func (o *Operations) fetchSynthRowByPath(ctx context.Context, schema, table string, info *synth.ViewInfo, fullPath string) ([]string, []interface{}, *FSError) {
	parts := strings.Split(fullPath, "/")
	leafName := parts[len(parts)-1]
	parentSegments := parts[:len(parts)-1]

	// Resolve parent path via cache (0 DB calls if fully cached)
	var parentID string
	if len(parentSegments) > 0 {
		var ok bool
		parentID, ok = o.resolveSynthPath(ctx, schema, table, parentSegments)
		if !ok {
			return nil, nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("directory not found: %s", strings.Join(parentSegments, "/"))}
		}
	}

	// Single query: SELECT * WHERE parent_id = X AND filename = leaf
	columns, row, err := o.db.GetRowByParentAndName(ctx, schema, table, parentID, leafName)
	if err != nil {
		return nil, nil, &FSError{Code: ErrIO, Message: "failed to fetch row", Cause: err}
	}
	if row == nil {
		return nil, nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("file not found: %s", fullPath)}
	}

	return columns, row, nil
}

// resolveSynthRow resolves a full path to its row data using the path cache and DB.
// For parent-pointer model only. Returns (columns, row, pkValue, error).
func (o *Operations) resolveSynthRow(ctx context.Context, schema, table string, info *synth.ViewInfo, fullPath string) ([]string, []interface{}, string, *FSError) {
	parts := strings.Split(fullPath, "/")
	fileID, ok := o.resolveSynthPath(ctx, schema, table, parts)
	if !ok {
		return nil, nil, "", &FSError{Code: ErrNotExist, Message: fmt.Sprintf("file not found: %s", fullPath)}
	}

	row, err := o.db.GetRow(ctx, schema, table, db.SinglePKMatch(info.Roles.PrimaryKey, fileID))
	if err != nil {
		return nil, nil, "", &FSError{Code: ErrIO, Message: "failed to fetch row by ID", Cause: err}
	}
	if row == nil {
		return nil, nil, "", &FSError{Code: ErrNotExist, Message: fmt.Sprintf("row not found: %s", fileID)}
	}

	return row.Columns, row.Values, fileID, nil
}

// statSynthFile returns metadata for a synthesized file.
// For views with hierarchy, also handles directory stat (filetype='directory').
func (o *Operations) statSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) (*Entry, *FSError) {
	filename := parsed.PrimaryKey
	schema := parsed.Context.Schema
	table := parsed.Context.TableName

	// Check ReadDir-primed cache first (0 DB queries on hit)
	if entry, ok := o.statCache.lookup(schema, table, filename); ok {
		return &entry, nil
	}

	// Check negative cache (file known to not exist)
	if o.statCache.isNegative(schema, table, filename) {
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("file not found: %s (cached)", filename)}
	}

	// Parent-pointer model: resolve path to UUID, then fetch row
	var columns []string
	var row []interface{}
	var fsErr *FSError
	if info.Roles.ParentID != "" {
		columns, row, _, fsErr = o.resolveSynthRow(ctx, schema, table, info, filename)
	} else {
		columns, row, fsErr = o.statSynthRowByFilename(ctx, schema, table, info, filename)
	}
	if fsErr != nil {
		if fsErr.Code == ErrNotExist {
			o.statCache.setNegative(schema, table, filename)
		}
		return nil, fsErr
	}

	// Check if this is a directory row (hierarchical views only)
	if info.SupportsHierarchy {
		filetypeIdx := findColIdx(columns, info.Roles.Filetype)
		if filetypeIdx >= 0 && synth.ValueToString(row[filetypeIdx]) == "directory" {
			entry := Entry{
				Name:    filename,
				IsDir:   true,
				Mode:    0755,
				ModTime: info.CachedMountTime,
			}
			o.statCache.set(schema, table, filename, entry)
			return &entry, nil
		}
	}

	// It's a file row — synthesize content to get size
	content, err := o.synthesizeContent(columns, row, info)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to synthesize content for stat",
			Cause:   err,
		}
	}

	modTime := extractModTime(columns, row, info)

	entry := Entry{
		Name:    filename,
		IsDir:   false,
		Mode:    0644,
		Size:    int64(len(content)),
		ModTime: modTime,
	}
	o.statCache.set(schema, table, filename, entry)
	return &entry, nil
}

// readFileSynthView reads synthesized file content.
// For parent-pointer model: resolves parent path via cache, then fetches the
// leaf row with a combined parent_id + filename query (one round-trip for the
// leaf instead of resolve_path + GetRow). ADR-017 Section "ReadFile / Stat".
func (o *Operations) readFileSynthView(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]byte, *FSError) {
	filename := parsed.PrimaryKey

	var columns []string
	var row []interface{}
	var fsErr *FSError
	if info.Roles.ParentID != "" {
		columns, row, fsErr = o.fetchSynthRowByPath(ctx, parsed.Context.Schema, parsed.Context.TableName, info, filename)
	} else {
		columns, row, fsErr = o.getSynthRow(ctx, parsed.Context.Schema, parsed.Context.TableName, info, filename)
	}
	if fsErr != nil {
		return nil, fsErr
	}

	content, err := o.synthesizeContent(columns, row, info)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to synthesize content",
			Cause:   err,
		}
	}

	return content, nil
}

// logSynthOp records an operation in the undo log for history-enabled synth apps.
// This is a best-effort operation -- log failures are logged but don't fail the
// original write. The userID parameter is empty until user identity is wired (Task 12.5).
//
// Operation types are filesystem-centric (ADR-017):
//   - "create": new file/directory created
//   - "edit": file content modified
//   - "rename": file/directory renamed or moved (parent_id or filename change)
//   - "delete": file/directory deleted
//   - "undo": undo operation restoring previous state
func (o *Operations) logSynthOp(ctx context.Context, schema, tableName string, info *synth.ViewInfo, opType, fileID, filename string) {
	if !info.HasHistory {
		return
	}

	logTable := tableName + "_log"
	historyTable := tableName + "_history"

	// For edit/rename/delete/undo, capture the version_id of the before-state.
	// The BEFORE trigger has already fired, so the most recent history entry
	// for this file_id is the one we want.
	var versionID string
	if opType != "create" && fileID != "" {
		vid, err := o.db.QueryLatestVersionID(ctx, synth.TigerFSSchema, historyTable, fileID)
		if err != nil {
			logging.Debug("failed to capture version_id for log entry",
				zap.String("table", tableName),
				zap.String("file_id", fileID),
				zap.Error(err))
		} else {
			versionID = vid
		}
	}

	userID := o.userID

	err := o.db.InsertLogEntry(ctx, synth.TigerFSSchema, logTable, userID, opType, fileID, filename, versionID, "")
	if err != nil {
		logging.Warn("failed to insert undo log entry",
			zap.String("table", tableName),
			zap.String("type", opType),
			zap.String("filename", filename),
			zap.Error(err))
	}
}

// resolveSynthPath resolves a sequence of path segments to a row ID using
// the path cache and the resolve_path PL/pgSQL function (ADR-017).
//
// Walks segments from left to right, checking the cache at each level.
// On the first cache miss, calls resolve_path with the remaining segments
// starting from the last cached parent. Populates the cache from the results.
//
// Returns the final row ID and true if the full path resolved, or empty string
// and false if any segment didn't resolve (path doesn't exist).
func (o *Operations) resolveSynthPath(ctx context.Context, schema, table string, segments []string) (string, bool) {
	if len(segments) == 0 {
		return "", true // root level — no ID, but "exists"
	}

	// Walk segments, checking cache at each level
	parentID := "" // empty = root (NULL parent_id)
	cacheHits := 0
	for _, seg := range segments {
		if id, ok := o.pathCache.lookup(schema, table, parentID, seg); ok {
			parentID = id
			cacheHits++
		} else {
			break
		}
	}

	// All segments resolved from cache
	if cacheHits == len(segments) {
		return parentID, true
	}

	// Call DB for remaining segments
	remaining := segments[cacheHits:]
	results, err := o.db.ResolvePath(ctx, synth.TigerFSSchema, table, parentID, remaining)
	if err != nil {
		logging.Debug("resolve_path failed",
			zap.String("table", table),
			zap.Strings("segments", remaining),
			zap.Error(err))
		return "", false
	}

	// Populate cache from results
	curParent := parentID
	for _, seg := range results {
		o.pathCache.put(schema, table, curParent, seg.Name, seg.ID)
		curParent = seg.ID
	}

	// Check if all remaining segments resolved
	if len(results) < len(remaining) {
		return "", false
	}

	return results[len(results)-1].ID, true
}

// writeSynthFile handles writes to synthesized view files (create or update).
// For views with hierarchy, auto-creates parent directory rows on insert.
// Binary data (null bytes or invalid UTF-8) is base64-encoded for TEXT column storage.
func (o *Operations) writeSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo, data []byte) *FSError {
	fsCtx := parsed.Context
	filename := parsed.PrimaryKey

	var colValues map[string]interface{}

	// Check if data is binary (null bytes or invalid UTF-8)
	if synth.IsBinary(data) {
		// Binary: base64-encode raw bytes, skip format-specific parsing
		colValues = map[string]interface{}{
			info.Roles.Body: synth.EncodeBody(data),
		}
		if info.Roles.Encoding != "" {
			colValues[info.Roles.Encoding] = "base64"
		}
	} else {
		// Text: parse as markdown/plaintext (existing behavior)
		var err error
		colValues, err = o.parseSynthContent(data, info)
		if err != nil {
			return &FSError{
				Code:    ErrInvalidPath,
				Message: "failed to parse file content",
				Cause:   err,
			}
		}
		// Explicitly set encoding to utf8 if column exists
		if info.Roles.Encoding != "" {
			colValues[info.Roles.Encoding] = "utf8"
		}
	}

	// For hierarchical views, set filetype='file' explicitly
	if info.SupportsHierarchy {
		colValues[info.Roles.Filetype] = "file"
	}

	// Parent-pointer model (ADR-017): use leaf filename + parent_id
	if info.Roles.ParentID != "" {
		parts := strings.Split(filename, "/")
		leafName := parts[len(parts)-1]
		colValues[info.Roles.Filename] = leafName

		// Check if file exists by resolving full path
		fileID, fileExists := o.resolveSynthPath(ctx, fsCtx.Schema, fsCtx.TableName, parts)

		// Build columns/values slices
		columns := make([]string, 0, len(colValues))
		values := make([]interface{}, 0, len(colValues))
		for col, val := range colValues {
			columns = append(columns, col)
			values = append(values, val)
		}

		if fileExists {
			// UPDATE by UUID — parent_id and filename don't change on content edit
			dbErr := o.db.UpdateRow(ctx, fsCtx.Schema, fsCtx.TableName, db.SinglePKMatch(info.Roles.PrimaryKey, fileID), columns, values)
			if dbErr != nil {
				return &FSError{Code: ErrIO, Message: "failed to update synth file", Cause: dbErr}
			}
			o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "edit", fileID, filename)
		} else {
			// Ensure parent directories exist, get parent UUID
			parentID, fsErr := o.ensureSynthParentDirs(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
			if fsErr != nil {
				return fsErr
			}
			if parentID != "" {
				colValues[info.Roles.ParentID] = parentID
				// Rebuild columns/values with parent_id
				columns = columns[:0]
				values = values[:0]
				for col, val := range colValues {
					columns = append(columns, col)
					values = append(values, val)
				}
			}

			insertedPK, dbErr := o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
			if dbErr != nil {
				return &FSError{Code: ErrIO, Message: "failed to create synth file", Cause: dbErr}
			}
			o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "create", insertedPK, filename)
		}

		o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
		o.pathCache.invalidate(fsCtx.Schema, fsCtx.TableName)
		return nil
	}

	// Old model (pre-ADR-017): full-path filenames
	colValues[info.Roles.Filename] = filename

	// Convert map to columns/values slices
	columns := make([]string, 0, len(colValues))
	values := make([]interface{}, 0, len(colValues))
	for col, val := range colValues {
		columns = append(columns, col)
		values = append(values, val)
	}

	// Check if row exists by looking up the filename
	_, _, lookupErr := o.getSynthRow(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
	rowExists := lookupErr == nil

	if rowExists {
		pkValue, fsErr := o.getSynthRowPK(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
		if fsErr != nil {
			return fsErr
		}

		dbErr := o.db.UpdateRow(ctx, fsCtx.Schema, fsCtx.TableName, db.SinglePKMatch(info.Roles.PrimaryKey, pkValue), columns, values)
		if dbErr != nil {
			return &FSError{Code: ErrIO, Message: "failed to update synth file", Cause: dbErr}
		}
		o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "edit", pkValue, filename)
	} else {
		if info.SupportsHierarchy {
			if _, fsErr := o.ensureSynthParentDirs(ctx, fsCtx.Schema, fsCtx.TableName, info, filename); fsErr != nil {
				return fsErr
			}
		}

		insertedPK, dbErr := o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
		if dbErr != nil {
			return &FSError{Code: ErrIO, Message: "failed to create synth file", Cause: dbErr}
		}
		o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "create", insertedPK, filename)
	}

	o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
	return nil
}

// deleteSynthFile deletes a synthesized file or directory.
// For directories in hierarchical views, checks for children and returns ENOTEMPTY.
func (o *Operations) deleteSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) *FSError {
	fsCtx := parsed.Context
	filename := parsed.PrimaryKey

	// Parent-pointer model (ADR-017): resolve path then check/delete by UUID
	if info.Roles.ParentID != "" {
		columns, row, fileID, fsErr := o.resolveSynthRow(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
		if fsErr != nil {
			return fsErr
		}

		// Check if it's a directory
		filetypeIdx := findColIdx(columns, info.Roles.Filetype)
		if filetypeIdx >= 0 && synth.ValueToString(row[filetypeIdx]) == "directory" {
			// Check for children by parent_id (ADR-017: WHERE parent_id = dir_id)
			_, childRows, err := o.db.GetRowsByParent(ctx, fsCtx.Schema, fsCtx.TableName, fileID, 1)
			if err != nil {
				return &FSError{Code: ErrIO, Message: "failed to check directory children", Cause: err}
			}
			if len(childRows) > 0 {
				return &FSError{Code: ErrNotEmpty, Message: "directory not empty"}
			}
		}

		err := o.db.DeleteRow(ctx, fsCtx.Schema, fsCtx.TableName, db.SinglePKMatch(info.Roles.PrimaryKey, fileID))
		if err != nil {
			return &FSError{Code: ErrIO, Message: "failed to delete synth file", Cause: err}
		}
		o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "delete", fileID, filename)
		o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
		o.pathCache.invalidate(fsCtx.Schema, fsCtx.TableName)
		return nil
	}

	// Old model: path-encoded filenames
	if info.SupportsHierarchy {
		dirPath := filename
		exists, fsErr := o.synthRowExists(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath, "directory")
		if fsErr != nil {
			return fsErr
		}
		if exists {
			hasChildren, err := o.db.HasChildrenWithPrefix(ctx, fsCtx.Schema, fsCtx.TableName, info.Roles.Filename, dirPath)
			if err != nil {
				return &FSError{Code: ErrIO, Message: "failed to check directory children", Cause: err}
			}
			if hasChildren {
				return &FSError{Code: ErrNotEmpty, Message: "directory not empty"}
			}

			pkValue, lookupErr := o.getSynthRowPKByFiletype(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath, "directory")
			if lookupErr != nil {
				return lookupErr
			}
			err = o.db.DeleteRow(ctx, fsCtx.Schema, fsCtx.TableName, db.SinglePKMatch(info.Roles.PrimaryKey, pkValue))
			if err != nil {
				return &FSError{Code: ErrIO, Message: "failed to delete directory", Cause: err}
			}
			o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "delete", pkValue, dirPath)
			o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
			return nil
		}
	}

	pkValue, fsErr := o.getSynthRowPK(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
	if fsErr != nil {
		return fsErr
	}

	err := o.db.DeleteRow(ctx, fsCtx.Schema, fsCtx.TableName, db.SinglePKMatch(info.Roles.PrimaryKey, pkValue))
	if err != nil {
		return &FSError{Code: ErrIO, Message: "failed to delete synth file", Cause: err}
	}
	o.logSynthOp(ctx, fsCtx.Schema, fsCtx.TableName, info, "delete", pkValue, filename)
	o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
	return nil
}

// getSynthRowPKByFiletype looks up the primary key for a row with a specific filetype.
// Uses a targeted WHERE query on filename + filetype columns.
func (o *Operations) getSynthRowPKByFiletype(ctx context.Context, schema, table string, info *synth.ViewInfo, filename, filetype string) (string, *FSError) {
	columns, row, err := o.db.GetRowByColumns(ctx, schema, table,
		[]string{info.Roles.Filename, info.Roles.Filetype},
		[]interface{}{filename, filetype})
	if err != nil {
		return "", &FSError{
			Code:    ErrIO,
			Message: "failed to query synth view",
			Cause:   err,
		}
	}

	if row == nil {
		return "", &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("row not found: %s (filetype=%s)", filename, filetype),
		}
	}

	pkIdx := findColIdx(columns, info.Roles.PrimaryKey)
	if pkIdx < 0 {
		return "", &FSError{
			Code:    ErrIO,
			Message: "primary key column not found in view",
		}
	}

	return synth.ValueToString(row[pkIdx]), nil
}

// statSynthRowByFilename looks up any row (file or directory) by the filename column.
// Used by statSynthFile to determine what exists at a path in a single query.
// Unlike getSynthRow, this does NOT skip directory rows — the caller inspects filetype.
func (o *Operations) statSynthRowByFilename(ctx context.Context, schema, table string, info *synth.ViewInfo, filename string) ([]string, []interface{}, *FSError) {
	columns, row, err := o.db.GetRowByColumns(ctx, schema, table,
		[]string{info.Roles.Filename}, []interface{}{filename})
	if err != nil {
		return nil, nil, &FSError{
			Code:    ErrIO,
			Message: "failed to query synth row",
			Cause:   err,
		}
	}
	if row == nil {
		return nil, nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("file not found: %s", filename),
		}
	}
	return columns, row, nil
}

// getSynthRow looks up a row in a synth view by the synthesized filename.
// Returns the column names and row values, or an error if not found.
// For hierarchical views, only matches file rows (skips directory rows).
//
// Uses a targeted WHERE query on the filename column for O(1) lookup.
// Falls back to full scan if the targeted query misses (handles NULL filename
// rows where the FS name is derived from the PK, and rare sanitization mismatches).
func (o *Operations) getSynthRow(ctx context.Context, schema, table string, info *synth.ViewInfo, filename string) ([]string, []interface{}, *FSError) {
	// Try targeted query first: WHERE filename = $1 [AND filetype != 'directory']
	whereCols := []string{info.Roles.Filename}
	whereVals := []interface{}{filename}

	columns, row, err := o.db.GetRowByColumns(ctx, schema, table, whereCols, whereVals)
	if err != nil {
		return nil, nil, &FSError{
			Code:    ErrIO,
			Message: "failed to query synth row",
			Cause:   err,
		}
	}

	// Targeted query returned no rows — file doesn't exist.
	// No need for a full scan fallback since there's nothing to match.
	if row == nil {
		return nil, nil, &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("file not found: %s", filename),
		}
	}

	// For hierarchical views, verify it's not a directory row
	needsFullScan := false
	if info.SupportsHierarchy {
		filetypeIdx := findColIdx(columns, info.Roles.Filetype)
		if filetypeIdx >= 0 && synth.ValueToString(row[filetypeIdx]) == "directory" {
			// Got a directory row; there might still be a file row with the same name.
			needsFullScan = true
		}
	}

	if !needsFullScan {
		// Verify the synthesized filename matches (handles sanitization edge cases)
		var rowFilename string
		switch info.Format {
		case synth.FormatMarkdown:
			rowFilename = synth.GetMarkdownFilename(columns, row, info.Roles)
		case synth.FormatPlainText:
			rowFilename = synth.GetPlainTextFilename(columns, row, info.Roles)
		}
		if rowFilename == filename {
			return columns, row, nil
		}
		// Sanitized name differs from DB value; fall through to full scan
	}

	// Fallback: full scan for directory/file name collisions or sanitization mismatches.
	// This is rare -- only happens when a directory and file share the same filename
	// column value, or the filename contains sanitized characters (\, \x00, :).
	return o.getSynthRowFullScan(ctx, schema, table, info, filename)
}

// getSynthRowFullScan finds a row by scanning all rows and matching synthesized filenames.
// Used as fallback when the targeted WHERE query misses.
func (o *Operations) getSynthRowFullScan(ctx context.Context, schema, table string, info *synth.ViewInfo, filename string) ([]string, []interface{}, *FSError) {
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	columns, rows, err := o.db.GetAllRows(ctx, schema, table, limit)
	if err != nil {
		return nil, nil, &FSError{
			Code:    ErrIO,
			Message: "failed to query synth view",
			Cause:   err,
		}
	}

	// Find filetype column index for hierarchical filtering
	filetypeIdx := -1
	if info.SupportsHierarchy {
		filetypeIdx = findColIdx(columns, info.Roles.Filetype)
	}

	for _, row := range rows {
		// For hierarchical views, skip directory rows
		if filetypeIdx >= 0 {
			if synth.ValueToString(row[filetypeIdx]) == "directory" {
				continue
			}
		}

		var rowFilename string
		switch info.Format {
		case synth.FormatMarkdown:
			rowFilename = synth.GetMarkdownFilename(columns, row, info.Roles)
		case synth.FormatPlainText:
			rowFilename = synth.GetPlainTextFilename(columns, row, info.Roles)
		}

		if rowFilename == filename {
			return columns, row, nil
		}
	}

	return nil, nil, &FSError{
		Code:    ErrNotExist,
		Message: fmt.Sprintf("file not found: %s", filename),
	}
}

// getSynthRowPK looks up the primary key value for a synth file by its filename.
func (o *Operations) getSynthRowPK(ctx context.Context, schema, table string, info *synth.ViewInfo, filename string) (string, *FSError) {
	columns, row, fsErr := o.getSynthRow(ctx, schema, table, info, filename)
	if fsErr != nil {
		return "", fsErr
	}

	// Find PK column index
	for i, col := range columns {
		if col == info.Roles.PrimaryKey {
			return synth.ValueToString(row[i]), nil
		}
	}

	return "", &FSError{
		Code:    ErrIO,
		Message: fmt.Sprintf("primary key column %q not found in view", info.Roles.PrimaryKey),
	}
}

// synthesizeContent generates file content from a database row.
// If the row's encoding column is "base64", the body is decoded from base64
// and returned as raw bytes (binary round-trip). Otherwise, the row is
// synthesized into markdown or plaintext format.
func (o *Operations) synthesizeContent(columns []string, row []interface{}, info *synth.ViewInfo) ([]byte, error) {
	// Check if this row is base64-encoded binary
	if info.Roles.Encoding != "" {
		for i, col := range columns {
			if col == info.Roles.Encoding {
				if enc, ok := row[i].(string); ok && enc == "base64" {
					// Decode base64 body and return raw bytes
					for j, c := range columns {
						if c == info.Roles.Body {
							bodyStr := synth.ValueToString(row[j])
							return synth.DecodeBody(bodyStr)
						}
					}
				}
				break
			}
		}
	}

	// Text synthesis (markdown or plaintext)
	switch info.Format {
	case synth.FormatMarkdown:
		return synth.SynthesizeMarkdown(columns, row, info.Roles)
	case synth.FormatPlainText:
		return synth.SynthesizePlainText(columns, row, info.Roles)
	default:
		return nil, fmt.Errorf("unsupported synth format: %s", info.Format.String())
	}
}

// renameSynthFile renames a synthesized file or directory.
// For directories in hierarchical views, performs an atomic prefix rename
// that updates the directory row and all its descendants.
func (o *Operations) renameSynthFile(ctx context.Context, schema, table string, info *synth.ViewInfo, oldFilename, newFilename string) *FSError {
	// Parent-pointer model (ADR-017): rename is a single-row UPDATE
	if info.Roles.ParentID != "" {
		_, _, fileID, fsErr := o.resolveSynthRow(ctx, schema, table, info, oldFilename)
		if fsErr != nil {
			return fsErr
		}

		// Extract new leaf name (same directory → just change filename)
		newParts := strings.Split(newFilename, "/")
		newLeaf := newParts[len(newParts)-1]
		oldParts := strings.Split(oldFilename, "/")

		// UPDATE filename (and parent_id if moving to different directory)
		updateCols := []string{info.Roles.Filename}
		updateVals := []interface{}{newLeaf}

		// Check if parent directory changed (move vs rename)
		oldParentPath := strings.Join(oldParts[:len(oldParts)-1], "/")
		newParentPath := strings.Join(newParts[:len(newParts)-1], "/")
		if oldParentPath != newParentPath {
			// Move to different directory — resolve new parent
			var newParentID string
			if newParentPath != "" {
				newParentSegs := strings.Split(newParentPath, "/")
				var ok bool
				newParentID, ok = o.resolveSynthPath(ctx, schema, table, newParentSegs)
				if !ok {
					return &FSError{Code: ErrNotExist, Message: fmt.Sprintf("target directory not found: %s", newParentPath)}
				}
			}
			updateCols = append(updateCols, info.Roles.ParentID)
			if newParentID != "" {
				updateVals = append(updateVals, newParentID)
			} else {
				updateVals = append(updateVals, nil) // root level
			}
		}

		err := o.db.UpdateRow(ctx, schema, table, db.SinglePKMatch(info.Roles.PrimaryKey, fileID), updateCols, updateVals)
		if err != nil {
			return &FSError{Code: ErrIO, Message: "failed to rename synth file", Cause: err}
		}

		o.logSynthOp(ctx, schema, table, info, "rename", fileID, newFilename)
		o.statCache.invalidate(schema, table)
		o.pathCache.invalidate(schema, table)
		return nil
	}

	// Old model: path-encoded filenames
	if info.SupportsHierarchy {
		oldDirPath := oldFilename
		newDirPath := newFilename

		exists, fsErr := o.synthRowExists(ctx, schema, table, info, oldDirPath, "directory")
		if fsErr != nil {
			return fsErr
		}
		if exists {
			rowsAffected, err := o.db.RenameByPrefix(ctx, schema, table, info.Roles.Filename, oldDirPath, newDirPath)
			if err != nil {
				return &FSError{Code: ErrIO, Message: "failed to rename directory", Cause: err}
			}
			if rowsAffected == 0 {
				return &FSError{Code: ErrNotExist, Message: "directory already moved by another process"}
			}
			o.statCache.invalidate(schema, table)
			return nil
		}
	}

	columns, row, fsErr := o.getSynthRow(ctx, schema, table, info, oldFilename)
	if fsErr != nil {
		return fsErr
	}

	var pkValue, rawOldFilename string
	for i, col := range columns {
		switch col {
		case info.Roles.PrimaryKey:
			pkValue = synth.ValueToString(row[i])
		case info.Roles.Filename:
			rawOldFilename = synth.ValueToString(row[i])
		}
	}
	if pkValue == "" {
		return &FSError{Code: ErrIO, Message: fmt.Sprintf("primary key column %q not found in view", info.Roles.PrimaryKey)}
	}

	err := o.db.UpdateColumnCAS(ctx, schema, table, db.SinglePKMatch(info.Roles.PrimaryKey, pkValue), info.Roles.Filename, newFilename, info.Roles.Filename, rawOldFilename)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return &FSError{Code: ErrNotExist, Message: "file already moved by another process", Cause: err}
		}
		return &FSError{Code: ErrIO, Message: "failed to rename synth file", Cause: err}
	}

	o.logSynthOp(ctx, schema, table, info, "rename", pkValue, newFilename)
	o.statCache.invalidate(schema, table)
	return nil
}

// readDirSynthHierarchical lists children of a hierarchical directory in a synth view.
// Called when PathRow resolves to a directory in a view with SupportsHierarchy.
// The parsed.PrimaryKey contains the directory path (e.g., "projects/web").
func (o *Operations) readDirSynthHierarchical(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	prefix := parsed.PrimaryKey

	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	// Parent-pointer model (ADR-017): resolve directory path, then query children
	if info.Roles.ParentID != "" {
		segments := strings.Split(prefix, "/")
		parentID, ok := o.resolveSynthPath(ctx, fsCtx.Schema, fsCtx.TableName, segments)
		if !ok {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("directory not found: %s", prefix)}
		}

		columns, rows, err := o.db.GetRowsByParent(ctx, fsCtx.Schema, fsCtx.TableName, parentID, limit)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to list directory entries", Cause: err}
		}

		o.primeSynthStatCache(fsCtx.Schema, fsCtx.TableName, prefix, columns, rows, info)
		children := o.buildEntriesFromRows(columns, rows, info)
		if info.HasHistory {
			children = append([]Entry{{Name: DirHistory, IsDir: true, Mode: os.ModeDir | 0555, ModTime: info.CachedMountTime}}, children...)
		}
		return children, nil
	}

	// Old hierarchy model (path-encoded filenames, pre-ADR-017)
	columns, rows, err := o.db.GetAllRows(ctx, fsCtx.Schema, fsCtx.TableName, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to query synth view for hierarchy",
			Cause:   err,
		}
	}

	o.primeSynthStatCache(fsCtx.Schema, fsCtx.TableName, prefix, columns, rows, info)
	children := o.filterHierarchicalChildren(columns, rows, prefix, info)
	if info.HasHistory {
		children = append([]Entry{{Name: DirHistory, IsDir: true, Mode: os.ModeDir | 0555, ModTime: info.CachedMountTime}}, children...)
	}
	return children, nil
}

// buildEntriesFromRows converts query result rows into Entry slices for ReadDir.
// Used by the parent-pointer model where GetRowsByParent already filtered to the
// correct directory -- no in-memory filtering needed. Each row's filename column
// contains the leaf name.
func (o *Operations) buildEntriesFromRows(columns []string, rows [][]interface{}, info *synth.ViewInfo) []Entry {
	filetypeIdx := findColIdx(columns, info.Roles.Filetype)
	entries := make([]Entry, 0, len(rows))

	for _, row := range rows {
		isDir := filetypeIdx >= 0 && synth.ValueToString(row[filetypeIdx]) == "directory"
		modTime := extractModTime(columns, row, info)

		if isDir {
			leafName := synth.ValueToString(row[findColIdx(columns, info.Roles.Filename)])
			entries = append(entries, Entry{
				Name:    leafName,
				IsDir:   true,
				Mode:    0755,
				ModTime: modTime,
			})
		} else {
			var filename string
			switch info.Format {
			case synth.FormatMarkdown:
				filename = synth.GetMarkdownFilename(columns, row, info.Roles)
			case synth.FormatPlainText:
				filename = synth.GetPlainTextFilename(columns, row, info.Roles)
			default:
				continue
			}

			var size int64
			if content, err := o.synthesizeContent(columns, row, info); err == nil {
				size = int64(len(content))
			}
			entries = append(entries, Entry{
				Name:    filename,
				IsDir:   false,
				Mode:    0644,
				Size:    size,
				ModTime: modTime,
			})
		}
	}

	return entries
}

// filterHierarchicalChildren filters rows to immediate children of a prefix.
// For prefix "projects/web", returns entries like "todo.md" (file) and "docs" (dir).
// For prefix "" (root), returns top-level files and directories.
func (o *Operations) filterHierarchicalChildren(columns []string, rows [][]interface{}, prefix string, info *synth.ViewInfo) []Entry {
	// Find column indexes
	filenameIdx := -1
	filetypeIdx := -1
	for i, col := range columns {
		if col == info.Roles.Filename {
			filenameIdx = i
		}
		if col == info.Roles.Filetype {
			filetypeIdx = i
		}
	}
	if filenameIdx < 0 || filetypeIdx < 0 {
		return nil
	}

	seen := make(map[string]bool)
	var entries []Entry

	for _, row := range rows {
		rawFilename := synth.ValueToString(row[filenameIdx])
		filetype := synth.ValueToString(row[filetypeIdx])

		// Check if this row is a direct child of the prefix
		var childName string
		if prefix == "" {
			// Root level: no slash in filename means top-level
			if !strings.Contains(rawFilename, "/") {
				childName = rawFilename
			} else {
				continue
			}
		} else {
			// Subdirectory: must start with prefix + "/"
			pfx := prefix + "/"
			if !strings.HasPrefix(rawFilename, pfx) {
				continue
			}
			rest := rawFilename[len(pfx):]
			// Must be immediate child (no more slashes)
			if strings.Contains(rest, "/") {
				continue
			}
			childName = rest
		}

		if childName == "" || seen[childName] {
			continue
		}

		isDir := filetype == "directory"
		modTime := extractModTime(columns, row, info)

		if isDir {
			seen[childName] = true
			entries = append(entries, Entry{
				Name:    childName,
				IsDir:   true,
				Mode:    0755,
				ModTime: modTime,
			})
		} else {
			// Synthesize content to get accurate size (CPU-only, no DB query)
			var size int64
			if content, err := o.synthesizeContent(columns, row, info); err == nil {
				size = int64(len(content))
			}
			seen[childName] = true
			entries = append(entries, Entry{
				Name:    childName,
				IsDir:   false,
				Mode:    0644,
				Size:    size,
				ModTime: modTime,
			})
		}
	}

	return entries
}

// mkdirSynth creates a directory row in a hierarchical synth view.
// Inserts a row with filetype='directory' and auto-creates parent directories.
func (o *Operations) mkdirSynth(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) *FSError {
	fsCtx := parsed.Context
	dirPath := parsed.PrimaryKey

	// Parent-pointer model (ADR-017): use leaf name + parent_id
	if info.Roles.ParentID != "" {
		parts := strings.Split(dirPath, "/")
		leafName := parts[len(parts)-1]

		// Check if directory already exists by resolving full path
		if _, ok := o.resolveSynthPath(ctx, fsCtx.Schema, fsCtx.TableName, parts); ok {
			return &FSError{Code: ErrExists, Message: "directory already exists"}
		}

		// Ensure parent directories exist, get parent UUID
		parentID, fsErr := o.ensureSynthParentDirs(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath)
		if fsErr != nil {
			return fsErr
		}

		columns := []string{info.Roles.Filename, info.Roles.Filetype}
		values := []interface{}{leafName, "directory"}
		if parentID != "" {
			columns = append(columns, info.Roles.ParentID)
			values = append(values, parentID)
		}

		_, dbErr := o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
		if dbErr != nil {
			return &FSError{Code: ErrIO, Message: "failed to create directory", Cause: dbErr}
		}

		o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
		o.pathCache.invalidate(fsCtx.Schema, fsCtx.TableName)
		return nil
	}

	// Old model (pre-ADR-017): full-path filenames
	exists, err := o.synthRowExists(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath, "directory")
	if err != nil {
		return err
	}
	if exists {
		return &FSError{Code: ErrExists, Message: "directory already exists"}
	}

	if _, fsErr := o.ensureSynthParentDirs(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath); fsErr != nil {
		return fsErr
	}

	columns := []string{info.Roles.Filename, info.Roles.Filetype}
	values := []interface{}{dirPath, "directory"}
	_, dbErr := o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
	if dbErr != nil {
		return &FSError{Code: ErrIO, Message: "failed to create directory", Cause: dbErr}
	}

	o.statCache.invalidate(fsCtx.Schema, fsCtx.TableName)
	return nil
}

// ensureSynthParentDirs auto-creates parent directory rows for a given path.
// For "projects/web/todo", creates "projects" and "projects/web" directory rows.
//
// Returns the parent UUID for the leaf entry. Empty string for root level.
// When parent_id column exists (ADR-017), creates directories with proper
// parent_id chain. Otherwise falls back to the old path-encoded model.
func (o *Operations) ensureSynthParentDirs(ctx context.Context, schema, table string, info *synth.ViewInfo, path string) (string, *FSError) {
	parts := strings.Split(path, "/")
	if len(parts) <= 1 {
		return "", nil // Root level, no parents to create
	}

	// Parent-pointer model (ADR-017): chain parent_id values
	if info.Roles.ParentID != "" {
		parentID := "" // Start from root (NULL parent_id)
		for i := 0; i < len(parts)-1; i++ {
			segName := parts[i]

			// Check cache first
			if id, ok := o.pathCache.lookup(schema, table, parentID, segName); ok {
				parentID = id
				continue
			}

			// Insert directory if not exists
			cols := []string{info.Roles.Filename, info.Roles.Filetype}
			vals := []interface{}{segName, "directory"}
			if parentID != "" {
				cols = append(cols, info.Roles.ParentID)
				vals = append(vals, parentID)
			}
			if err := o.db.InsertIfNotExists(ctx, schema, table, cols, vals); err != nil {
				return "", &FSError{Code: ErrIO, Message: "failed to create parent directory", Cause: err}
			}

			// Resolve to get the UUID (either just-created or already-existed)
			results, err := o.db.ResolvePath(ctx, synth.TigerFSSchema, table, parentID, []string{segName})
			if err != nil || len(results) == 0 {
				return "", &FSError{Code: ErrIO, Message: fmt.Sprintf("failed to resolve parent directory: %s", segName)}
			}

			dirID := results[0].ID
			o.pathCache.put(schema, table, parentID, segName, dirID)
			parentID = dirID
		}
		return parentID, nil
	}

	// Old path-encoded model (pre-ADR-017)
	for i := 1; i < len(parts); i++ {
		parentPath := strings.Join(parts[:i], "/")
		columns := []string{info.Roles.Filename, info.Roles.Filetype}
		values := []interface{}{parentPath, "directory"}
		err := o.db.InsertIfNotExists(ctx, schema, table, columns, values)
		if err != nil {
			return "", &FSError{
				Code:    ErrIO,
				Message: "failed to create parent directory",
				Cause:   err,
			}
		}
	}

	return "", nil
}

// synthRowExists checks if a row exists in a synth view with the given filename and filetype.
// Uses a targeted WHERE query instead of scanning all rows.
func (o *Operations) synthRowExists(ctx context.Context, schema, table string, info *synth.ViewInfo, filename, filetype string) (bool, *FSError) {
	exists, err := o.db.RowExistsByColumns(ctx, schema, table,
		[]string{info.Roles.Filename, info.Roles.Filetype},
		[]interface{}{filename, filetype})
	if err != nil {
		return false, &FSError{
			Code:    ErrIO,
			Message: "failed to check synth row existence",
			Cause:   err,
		}
	}
	return exists, nil
}

// parseSynthContent parses file content back into column values.
func (o *Operations) parseSynthContent(data []byte, info *synth.ViewInfo) (map[string]interface{}, error) {
	switch info.Format {
	case synth.FormatMarkdown:
		parsed, err := synth.ParseMarkdown(data)
		if err != nil {
			return nil, err
		}
		return synth.MapToColumns(parsed, info.Roles)
	case synth.FormatPlainText:
		body := synth.ParsePlainText(data)
		return map[string]interface{}{
			info.Roles.Body: body,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported synth format: %s", info.Format.String())
	}
}
