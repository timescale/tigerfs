package fs

import (
	"context"
	"fmt"
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
	// Get all views
	views, err := o.db.GetViews(ctx, schema)
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
		if comment, ok := comments[viewName]; ok && comment != "" {
			format = synth.DetectFormatFromComment(comment)
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
		pk, err := o.db.GetPrimaryKey(ctx, schema, viewName)
		if err == nil {
			pkColumn = pk.Columns[0]
		}

		// Detect column roles
		roles, err := synth.DetectColumnRoles(colNames, format, pkColumn)
		if err != nil {
			continue
		}

		cache[viewName] = &synth.ViewInfo{
			Format: format,
			Roles:  roles,
		}
	}

	return cache, nil
}

// readDirSynthView lists synthesized filenames as file entries.
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

	now := time.Now()
	entries := make([]Entry, 0, len(rows))

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

		entries = append(entries, Entry{
			Name:    filename,
			IsDir:   false,
			Mode:    0644,
			ModTime: now,
		})
	}

	return entries, nil
}

// statSynthFile returns metadata for a synthesized file.
func (o *Operations) statSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) (*Entry, *FSError) {
	now := time.Now()

	// The PrimaryKey field contains the filename (e.g., "hello-world.md")
	filename := parsed.PrimaryKey

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

	return &Entry{
		Name:    filename,
		IsDir:   false,
		Mode:    0644,
		Size:    int64(len(content)),
		ModTime: now,
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
func (o *Operations) writeSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo, data []byte) *FSError {
	fsCtx := parsed.Context
	filename := parsed.PrimaryKey

	// Parse the written content into column values
	colValues, err := o.parseSynthContent(data, info)
	if err != nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "failed to parse file content",
			Cause:   err,
		}
	}

	// Set the filename column from the path
	// Strip extension to get the raw filename value
	rawFilename := filename
	if ext := info.Format.Extension(); ext != "" && strings.HasSuffix(rawFilename, ext) {
		rawFilename = strings.TrimSuffix(rawFilename, ext)
	}
	colValues[info.Roles.Filename] = rawFilename

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

		err = o.db.UpdateRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, pkValue, columns, values)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to update synth file",
				Cause:   err,
			}
		}
	} else {
		// INSERT new row
		_, err = o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to create synth file",
				Cause:   err,
			}
		}
	}

	return nil
}

// deleteSynthFile deletes a synthesized file (DELETE by filename).
func (o *Operations) deleteSynthFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) *FSError {
	fsCtx := parsed.Context
	filename := parsed.PrimaryKey

	// Find the PK value for this filename
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

// getSynthRow looks up a row in a synth view by the synthesized filename.
// Returns the column names and row values, or an error if not found.
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

	for _, row := range rows {
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
func (o *Operations) synthesizeContent(columns []string, row []interface{}, info *synth.ViewInfo) ([]byte, error) {
	switch info.Format {
	case synth.FormatMarkdown:
		return synth.SynthesizeMarkdown(columns, row, info.Roles)
	case synth.FormatPlainText:
		return synth.SynthesizePlainText(columns, row, info.Roles)
	default:
		return nil, fmt.Errorf("unsupported synth format: %s", info.Format.String())
	}
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
