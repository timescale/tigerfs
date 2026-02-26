package fs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

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
			cols, err := o.db.GetColumns(ctx, schema, viewName)
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
		cols, err := o.db.GetColumns(ctx, schema, viewName)
		if err != nil {
			continue
		}
		colNames := make([]string, len(cols))
		for i, c := range cols {
			colNames[i] = c.Name
		}

		// Detect PK column (views might not have one, try anyway)
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

		// Fallback history detection: check if companion _<view>_history table exists
		if !hasHistory {
			historyTable := "_" + viewName + "_history"
			exists, tblErr := o.db.TableExists(ctx, schema, historyTable)
			if tblErr == nil && exists {
				hasHistory = true
			}
		}

		cache[viewName] = &synth.ViewInfo{
			Format:            format,
			Roles:             roles,
			CachedMountTime:   mountTime,
			SupportsHierarchy: roles.Filetype != "",
			HasHistory:        hasHistory,
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

// readDirSynthView lists synthesized filenames as file entries.
// For views with hierarchy support, shows only root-level entries (files and directories).
func (o *Operations) readDirSynthView(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]Entry, *FSError) {
	fsCtx := parsed.Context

	// Get all rows from the view
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	columns, rows, err := o.db.GetAllRows(ctx, fsCtx.Schema, fsCtx.TableName, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to list synth view rows",
			Cause:   err,
		}
	}

	// For hierarchical views, filter to root-level entries only
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

// statSynthFile returns metadata for a synthesized file.
// For views with hierarchy, also handles directory stat (filetype='directory').
func (o *Operations) statSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) (*Entry, *FSError) {
	// The PrimaryKey field contains the filename (e.g., "hello-world.md")
	filename := parsed.PrimaryKey

	// For hierarchical views, check for directory first (directory takes priority)
	if info.SupportsHierarchy {
		dirPath := filename

		exists, fsErr := o.synthRowExists(ctx, parsed.Context.Schema, parsed.Context.TableName, info, dirPath, "directory")
		if fsErr != nil {
			return nil, fsErr
		}
		if exists {
			return &Entry{
				Name:    dirPath,
				IsDir:   true,
				Mode:    0755,
				ModTime: info.CachedMountTime,
			}, nil
		}
	}

	// Look up the row by filename to verify it exists and get content size
	columns, row, fsErr := o.getSynthRow(ctx, parsed.Context.Schema, parsed.Context.TableName, info, filename)
	if fsErr != nil {
		return nil, fsErr
	}

	// Synthesize content to get size
	content, err := o.synthesizeContent(columns, row, info)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to synthesize content for stat",
			Cause:   err,
		}
	}

	modTime := extractModTime(columns, row, info)

	return &Entry{
		Name:    filename,
		IsDir:   false,
		Mode:    0644,
		Size:    int64(len(content)),
		ModTime: modTime,
	}, nil
}

// readFileSynthView reads synthesized file content.
func (o *Operations) readFileSynthView(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]byte, *FSError) {
	filename := parsed.PrimaryKey

	columns, row, fsErr := o.getSynthRow(ctx, parsed.Context.Schema, parsed.Context.TableName, info, filename)
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

	// Set the filename column from the path (FS name == DB name)
	colValues[info.Roles.Filename] = filename

	// For hierarchical views, set filetype='file' explicitly
	if info.SupportsHierarchy {
		colValues[info.Roles.Filetype] = "file"
	}

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
		// UPDATE existing row — need to find the PK value
		pkColumn := info.Roles.PrimaryKey
		pkValue, fsErr := o.getSynthRowPK(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
		if fsErr != nil {
			return fsErr
		}

		dbErr := o.db.UpdateRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, pkValue, columns, values)
		if dbErr != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to update synth file",
				Cause:   dbErr,
			}
		}
	} else {
		// For hierarchical views, auto-create parent directories before inserting
		if info.SupportsHierarchy {
			if fsErr := o.ensureSynthParentDirs(ctx, fsCtx.Schema, fsCtx.TableName, info, filename); fsErr != nil {
				return fsErr
			}
		}

		// INSERT new row
		_, dbErr := o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
		if dbErr != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to create synth file",
				Cause:   dbErr,
			}
		}
	}

	return nil
}

// deleteSynthFile deletes a synthesized file or directory.
// For directories in hierarchical views, checks for children and returns ENOTEMPTY.
func (o *Operations) deleteSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) *FSError {
	fsCtx := parsed.Context
	filename := parsed.PrimaryKey

	// For hierarchical views, check if this is a directory
	if info.SupportsHierarchy {
		dirPath := filename

		exists, fsErr := o.synthRowExists(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath, "directory")
		if fsErr != nil {
			return fsErr
		}
		if exists {
			// It's a directory — check for children
			hasChildren, err := o.db.HasChildrenWithPrefix(ctx, fsCtx.Schema, fsCtx.TableName, info.Roles.Filename, dirPath)
			if err != nil {
				return &FSError{
					Code:    ErrIO,
					Message: "failed to check directory children",
					Cause:   err,
				}
			}
			if hasChildren {
				return &FSError{
					Code:    ErrNotEmpty,
					Message: "directory not empty",
				}
			}

			// Delete the directory row by looking up its PK
			pkValue, lookupErr := o.getSynthRowPKByFiletype(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath, "directory")
			if lookupErr != nil {
				return lookupErr
			}
			err = o.db.DeleteRow(ctx, fsCtx.Schema, fsCtx.TableName, info.Roles.PrimaryKey, pkValue)
			if err != nil {
				return &FSError{
					Code:    ErrIO,
					Message: "failed to delete directory",
					Cause:   err,
				}
			}
			return nil
		}
	}

	// Regular file delete
	pkValue, fsErr := o.getSynthRowPK(ctx, fsCtx.Schema, fsCtx.TableName, info, filename)
	if fsErr != nil {
		return fsErr
	}

	err := o.db.DeleteRow(ctx, fsCtx.Schema, fsCtx.TableName, info.Roles.PrimaryKey, pkValue)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to delete synth file",
			Cause:   err,
		}
	}

	return nil
}

// getSynthRowPKByFiletype looks up the primary key for a row with a specific filetype.
func (o *Operations) getSynthRowPKByFiletype(ctx context.Context, schema, table string, info *synth.ViewInfo, filename, filetype string) (string, *FSError) {
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	columns, rows, err := o.db.GetAllRows(ctx, schema, table, limit)
	if err != nil {
		return "", &FSError{
			Code:    ErrIO,
			Message: "failed to query synth view",
			Cause:   err,
		}
	}

	filenameIdx := -1
	filetypeIdx := -1
	pkIdx := -1
	for i, col := range columns {
		if col == info.Roles.Filename {
			filenameIdx = i
		}
		if col == info.Roles.Filetype {
			filetypeIdx = i
		}
		if col == info.Roles.PrimaryKey {
			pkIdx = i
		}
	}

	if filenameIdx < 0 || filetypeIdx < 0 || pkIdx < 0 {
		return "", &FSError{
			Code:    ErrIO,
			Message: "required columns not found in view",
		}
	}

	for _, row := range rows {
		fn := synth.ValueToString(row[filenameIdx])
		ft := synth.ValueToString(row[filetypeIdx])
		if fn == filename && ft == filetype {
			return synth.ValueToString(row[pkIdx]), nil
		}
	}

	return "", &FSError{
		Code:    ErrNotExist,
		Message: fmt.Sprintf("row not found: %s (filetype=%s)", filename, filetype),
	}
}

// getSynthRow looks up a row in a synth view by the synthesized filename.
// Returns the column names and row values, or an error if not found.
// For hierarchical views, only matches file rows (skips directory rows).
func (o *Operations) getSynthRow(ctx context.Context, schema, table string, info *synth.ViewInfo, filename string) ([]string, []interface{}, *FSError) {
	// Get all rows and find the one matching the filename
	// TODO: optimize with a direct query on the filename column
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
		for i, col := range columns {
			if col == info.Roles.Filetype {
				filetypeIdx = i
				break
			}
		}
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
	// For hierarchical views, check if old path is a directory
	if info.SupportsHierarchy {
		oldDirPath := oldFilename
		newDirPath := newFilename

		exists, fsErr := o.synthRowExists(ctx, schema, table, info, oldDirPath, "directory")
		if fsErr != nil {
			return fsErr
		}
		if exists {
			// Directory rename — atomic prefix swap.
			// RenameByPrefix WHERE matches old value, so concurrent renames
			// are safe: the loser gets rowsAffected=0.
			rowsAffected, err := o.db.RenameByPrefix(ctx, schema, table, info.Roles.Filename, oldDirPath, newDirPath)
			if err != nil {
				return &FSError{
					Code:    ErrIO,
					Message: "failed to rename directory",
					Cause:   err,
				}
			}
			if rowsAffected == 0 {
				return &FSError{
					Code:    ErrNotExist,
					Message: "directory already moved by another process",
				}
			}
			return nil
		}
	}

	// Regular file rename — FS name == DB name, no extension normalization needed.
	columns, row, fsErr := o.getSynthRow(ctx, schema, table, info, oldFilename)
	if fsErr != nil {
		return fsErr
	}

	// Extract PK and raw filename from the actual DB row
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
		return &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("primary key column %q not found in view", info.Roles.PrimaryKey),
		}
	}

	// Atomic rename: UPDATE SET filename = new WHERE pk = X AND filename = old.
	// If another process already renamed this file, the WHERE won't match
	// and we get "row not found" — exactly one concurrent rename wins.
	err := o.db.UpdateColumnCAS(ctx, schema, table, info.Roles.PrimaryKey, pkValue, info.Roles.Filename, newFilename, info.Roles.Filename, rawOldFilename)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return &FSError{
				Code:    ErrNotExist,
				Message: "file already moved by another process",
				Cause:   err,
			}
		}
		return &FSError{
			Code:    ErrIO,
			Message: "failed to rename synth file",
			Cause:   err,
		}
	}

	return nil
}

// readDirSynthHierarchical lists children of a hierarchical directory in a synth view.
// Called when PathRow resolves to a directory in a view with SupportsHierarchy.
// The parsed.PrimaryKey contains the directory path (e.g., "projects/web").
func (o *Operations) readDirSynthHierarchical(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	prefix := parsed.PrimaryKey

	// Get all rows from the view
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	columns, rows, err := o.db.GetAllRows(ctx, fsCtx.Schema, fsCtx.TableName, limit)
	if err != nil {
		return nil, &FSError{
			Code:    ErrIO,
			Message: "failed to query synth view for hierarchy",
			Cause:   err,
		}
	}

	children := o.filterHierarchicalChildren(columns, rows, prefix, info)
	if info.HasHistory {
		children = append([]Entry{{Name: DirHistory, IsDir: true, Mode: os.ModeDir | 0555, ModTime: info.CachedMountTime}}, children...)
	}
	return children, nil
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

	// Check if directory already exists
	exists, err := o.synthRowExists(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath, "directory")
	if err != nil {
		return err
	}
	if exists {
		return &FSError{
			Code:    ErrExists,
			Message: "directory already exists",
		}
	}

	// Auto-create parent directories
	if fsErr := o.ensureSynthParentDirs(ctx, fsCtx.Schema, fsCtx.TableName, info, dirPath); fsErr != nil {
		return fsErr
	}

	// Insert the directory row
	columns := []string{info.Roles.Filename, info.Roles.Filetype}
	values := []interface{}{dirPath, "directory"}
	_, dbErr := o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
	if dbErr != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to create directory",
			Cause:   dbErr,
		}
	}

	return nil
}

// ensureSynthParentDirs auto-creates parent directory rows for a given path.
// For "projects/web/todo", creates "projects" and "projects/web" directory rows.
func (o *Operations) ensureSynthParentDirs(ctx context.Context, schema, table string, info *synth.ViewInfo, path string) *FSError {
	parts := strings.Split(path, "/")
	if len(parts) <= 1 {
		return nil // No parents to create
	}

	// Create each ancestor directory
	for i := 1; i < len(parts); i++ {
		parentPath := strings.Join(parts[:i], "/")
		columns := []string{info.Roles.Filename, info.Roles.Filetype}
		values := []interface{}{parentPath, "directory"}
		err := o.db.InsertIfNotExists(ctx, schema, table, columns, values)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to create parent directory",
				Cause:   err,
			}
		}
	}

	return nil
}

// synthRowExists checks if a row exists in a synth view with the given filename and filetype.
func (o *Operations) synthRowExists(ctx context.Context, schema, table string, info *synth.ViewInfo, filename, filetype string) (bool, *FSError) {
	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	columns, rows, err := o.db.GetAllRows(ctx, schema, table, limit)
	if err != nil {
		return false, &FSError{
			Code:    ErrIO,
			Message: "failed to query synth view",
			Cause:   err,
		}
	}

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
		return false, nil
	}

	for _, row := range rows {
		fn := synth.ValueToString(row[filenameIdx])
		ft := synth.ValueToString(row[filetypeIdx])
		if fn == filename && ft == filetype {
			return true, nil
		}
	}

	return false, nil
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
