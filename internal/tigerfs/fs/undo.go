package fs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// UndoResult holds the outcome of an undo operation.
type UndoResult struct {
	FilesDeleted  int // rows created after target that were removed
	FilesRestored int // rows restored from history (edit/rename/delete)
	FilesSkipped  int // no-op (e.g., created then already deleted)
}

// ExecuteUndoToSavepoint undoes all operations after a named savepoint.
// Looks up savepoint_id by name, then delegates to ExecuteUndo.
func (o *Operations) ExecuteUndoToSavepoint(ctx context.Context, schema, tableName, savepointName string, filters []db.UndoFilter) (*UndoResult, error) {
	savepointTable := tableName + "_savepoint"

	// Look up savepoint_id by name (name is PK)
	row, err := o.db.GetRow(ctx, synth.TigerFSSchema, savepointTable, db.SinglePKMatch("name", savepointName))
	if err != nil {
		return nil, fmt.Errorf("savepoint not found: %s", savepointName)
	}

	// Extract savepoint_id from the row (pgx returns UUIDs as [16]byte)
	var savepointID string
	for i, col := range row.Columns {
		if col == "savepoint_id" {
			savepointID, _ = format.ConvertValueToText(row.Values[i])
			break
		}
	}
	if savepointID == "" {
		return nil, fmt.Errorf("savepoint %s has no savepoint_id", savepointName)
	}

	desc := fmt.Sprintf("Undo to savepoint %s", savepointName)
	return o.ExecuteUndo(ctx, schema, tableName, savepointID, desc, filters)
}

// ExecuteUndoToLogID undoes all operations after a specific log entry.
func (o *Operations) ExecuteUndoToLogID(ctx context.Context, schema, tableName, logID string, filters []db.UndoFilter) (*UndoResult, error) {
	desc := fmt.Sprintf("Undo to log entry %s", logID)
	return o.ExecuteUndo(ctx, schema, tableName, logID, desc, filters)
}

// ExecuteUndo undoes all operations after a target point (savepoint_id or log_id).
// Executes all changes in a single PostgreSQL transaction.
// The schema parameter is unused -- synth backing tables are always in the tigerfs schema.
func (o *Operations) ExecuteUndo(ctx context.Context, schema, tableName, afterID, description string, filters []db.UndoFilter) (*UndoResult, error) {
	logTable := tableName + "_log"
	historyTable := tableName + "_history"

	// Extract optional user_id filter for the query
	var userID string
	var queryFilters []db.UndoFilter
	for _, f := range filters {
		if f.Column == "user_id" {
			userID = f.Value
		} else {
			queryFilters = append(queryFilters, f)
		}
	}

	// Find all affected files
	affected, err := o.db.QueryUndoAffectedFiles(ctx, synth.TigerFSSchema, logTable, afterID, userID, queryFilters)
	if err != nil {
		return nil, fmt.Errorf("failed to query affected files: %w", err)
	}

	if len(affected) == 0 {
		return &UndoResult{}, nil
	}

	// Classify affected files by action
	var deleteFileIDs, deleteFilenames []string
	var restoreVersionIDs, restoreFileIDs, restoreFilenames []string
	skipped := 0

	for _, f := range affected {
		switch f.Type {
		case "create":
			// Row was created after target -- check if it still exists
			exists, err := o.db.QueryFileExists(ctx, synth.TigerFSSchema, tableName, f.FileID)
			if err != nil {
				logging.Warn("failed to check file existence during undo",
					zap.String("file_id", f.FileID), zap.Error(err))
				skipped++
				continue
			}
			if exists {
				deleteFileIDs = append(deleteFileIDs, f.FileID)
				deleteFilenames = append(deleteFilenames, f.Filename)
			} else {
				skipped++ // Created then already deleted -- no-op
			}

		case "edit", "rename", "delete", "undo":
			if f.VersionID == "" {
				logging.Warn("undo: missing version_id for non-create operation",
					zap.String("file_id", f.FileID), zap.String("type", f.Type))
				skipped++
				continue
			}
			restoreVersionIDs = append(restoreVersionIDs, f.VersionID)
			restoreFileIDs = append(restoreFileIDs, f.FileID)
			restoreFilenames = append(restoreFilenames, f.Filename)

		default:
			logging.Warn("undo: unknown operation type",
				zap.String("type", f.Type), zap.String("file_id", f.FileID))
			skipped++
		}
	}

	if len(deleteFileIDs) == 0 && len(restoreVersionIDs) == 0 {
		return &UndoResult{FilesSkipped: skipped}, nil
	}

	// Execute all changes atomically
	err = o.db.ExecuteUndoTransaction(ctx, &db.UndoTransactionParams{
		Schema:            synth.TigerFSSchema,
		SourceTable:       tableName,
		LogTable:          logTable,
		HistoryTable:      historyTable,
		Description:       description,
		DeleteFileIDs:     deleteFileIDs,
		DeleteFilenames:   deleteFilenames,
		RestoreVersionIDs: restoreVersionIDs,
		RestoreFileIDs:    restoreFileIDs,
		RestoreFilenames:  restoreFilenames,
		UserID:            o.userID,
	})
	if err != nil {
		return nil, fmt.Errorf("undo transaction failed: %w", err)
	}

	// Invalidate caches
	o.statCache.invalidate(synth.TigerFSSchema, tableName)

	result := &UndoResult{
		FilesDeleted:  len(deleteFileIDs),
		FilesRestored: len(restoreVersionIDs),
		FilesSkipped:  skipped,
	}

	logging.Info("undo completed",
		zap.String("table", tableName),
		zap.Int("deleted", result.FilesDeleted),
		zap.Int("restored", result.FilesRestored),
		zap.Int("skipped", result.FilesSkipped))

	return result, nil
}

// resolveLogID converts a display name (e.g., "2026-04-08T143015.234Z-i9j0k1l2m3n4b")
// to a raw UUID string. If already a UUID, returns as-is.
func resolveLogID(id string) string {
	if format.IsDisplayName(id) {
		uuid, err := format.DisplayNameToUUIDv7(id)
		if err == nil {
			return uuid.String()
		}
	}
	return id
}

// ExecuteUndoSingle undoes a single log entry.
func (o *Operations) ExecuteUndoSingle(ctx context.Context, schema, tableName, logID string) (*UndoResult, error) {
	logTable := tableName + "_log"
	historyTable := tableName + "_history"

	logID = resolveLogID(logID)

	// Fetch the log entry
	entry, err := o.db.QueryLogEntry(ctx, synth.TigerFSSchema, logTable, logID)
	if err != nil {
		return nil, fmt.Errorf("log entry not found: %s", logID)
	}

	desc := fmt.Sprintf("Undo single operation %s", logID)

	var deleteFileIDs, deleteFilenames []string
	var restoreVersionIDs, restoreFileIDs, restoreFilenames []string
	skipped := 0

	switch entry.Type {
	case "create":
		exists, err := o.db.QueryFileExists(ctx, synth.TigerFSSchema, tableName, entry.FileID)
		if err != nil || !exists {
			skipped = 1
		} else {
			deleteFileIDs = append(deleteFileIDs, entry.FileID)
			deleteFilenames = append(deleteFilenames, entry.Filename)
		}

	case "edit", "rename", "delete", "undo":
		if entry.VersionID == "" {
			return nil, fmt.Errorf("cannot undo %s operation: no version_id (before-state not captured)", entry.Type)
		}
		restoreVersionIDs = append(restoreVersionIDs, entry.VersionID)
		restoreFileIDs = append(restoreFileIDs, entry.FileID)
		restoreFilenames = append(restoreFilenames, entry.Filename)

	default:
		return nil, fmt.Errorf("unknown operation type: %s", entry.Type)
	}

	if len(deleteFileIDs) == 0 && len(restoreVersionIDs) == 0 {
		return &UndoResult{FilesSkipped: skipped}, nil
	}

	err = o.db.ExecuteUndoTransaction(ctx, &db.UndoTransactionParams{
		Schema:            synth.TigerFSSchema,
		SourceTable:       tableName,
		LogTable:          logTable,
		HistoryTable:      historyTable,
		Description:       desc,
		DeleteFileIDs:     deleteFileIDs,
		DeleteFilenames:   deleteFilenames,
		RestoreVersionIDs: restoreVersionIDs,
		RestoreFileIDs:    restoreFileIDs,
		RestoreFilenames:  restoreFilenames,
		UserID:            o.userID,
	})
	if err != nil {
		return nil, fmt.Errorf("undo transaction failed: %w", err)
	}

	o.statCache.invalidate(schema, tableName)

	return &UndoResult{
		FilesDeleted:  len(deleteFileIDs),
		FilesRestored: len(restoreVersionIDs),
		FilesSkipped:  skipped,
	}, nil
}

// --- Filesystem interface (.undo/ navigation, preview, apply) ---

// readDirUndo handles ReadDir for .undo/ paths.
func (o *Operations) readDirUndo(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()

	// Level 0: .undo/ -- list modes
	if parsed.UndoMode == "" {
		return []Entry{
			{Name: "id", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
			{Name: "to-id", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
			{Name: "to-savepoint", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		}, nil
	}

	// Level 1: .undo/<mode>/ -- list targets (log entries or savepoints)
	if parsed.UndoTarget == "" {
		return o.readDirUndoTargets(ctx, parsed)
	}

	// Level 2: .undo/<mode>/<target>/ -- preview directory
	if parsed.UndoFile == "" && parsed.InfoFile == "" && !parsed.UndoApply {
		return o.readDirUndoPreview(ctx, parsed)
	}

	// Level 3: .undo/<mode>/<target>/.info/ -- info directory
	if parsed.InfoFile == "" && parsed.UndoFile == "" {
		return []Entry{
			{Name: FileSummary, IsDir: false, Mode: 0444, ModTime: now},
		}, nil
	}

	return nil, &FSError{Code: ErrInvalidPath, Message: "invalid .undo/ path for ReadDir"}
}

// readDirUndoTargets lists log entries or savepoints for a given undo mode.
func (o *Operations) readDirUndoTargets(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	// Redirect context to the appropriate table
	switch parsed.UndoMode {
	case "id", "to-id":
		parsed.Context.TableName = parsed.OrigTableName + "_log"
		parsed.Context.Schema = synth.TigerFSSchema
	case "to-savepoint":
		parsed.Context.TableName = parsed.OrigTableName + "_savepoint"
		parsed.Context.Schema = synth.TigerFSSchema
	}

	// Apply default limit if none set by pipeline
	if parsed.Context.Limit == 0 {
		limit := o.config.UndoListLimit
		if limit <= 0 {
			limit = 100
		}
		parsed.Context.Limit = limit
		// Default to most recent (descending)
		if parsed.Context.LimitType == 0 {
			parsed.Context.LimitType = LimitLast
		}
	}

	return o.readDirTable(ctx, parsed)
}

// readDirUndoPreview builds a virtual directory listing for the undo preview.
func (o *Operations) readDirUndoPreview(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()

	affected, err := o.queryUndoAffected(ctx, parsed)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query undo preview", Cause: err}
	}

	// Start with .info/ and .apply
	entries := []Entry{
		{Name: DirInfo, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
		{Name: FileApply, IsDir: false, Mode: 0644, ModTime: now},
	}

	// Build file entries from affected files.
	// Track directories we've seen to add intermediate dir entries.
	seenDirs := make(map[string]bool)

	for _, f := range affected {
		filename := f.Filename

		// Add intermediate directories
		parts := strings.Split(filename, "/")
		if len(parts) > 1 {
			for i := 1; i < len(parts); i++ {
				dir := strings.Join(parts[:i], "/")
				if !seenDirs[dir] {
					seenDirs[dir] = true
					// Only add top-level directory entries
					if !strings.Contains(parts[0], "/") {
						entries = append(entries, Entry{
							Name:    parts[0],
							IsDir:   true,
							Mode:    os.ModeDir | 0755,
							ModTime: now,
						})
					}
				}
			}
		}

		// Determine if this is a delete (symlink to /dev/null) or restore (regular file)
		if f.Type == "create" {
			// File was created after target -- will be deleted on apply.
			// Show as symlink to /dev/null in preview.
			entries = append(entries, Entry{
				Name:    filename,
				IsDir:   false,
				Mode:    os.ModeSymlink | 0777,
				Target:  "/dev/null",
				ModTime: now,
			})
		} else {
			// File will be restored from history -- show as regular file
			entries = append(entries, Entry{
				Name:    filename,
				IsDir:   false,
				Mode:    0444,
				ModTime: now,
			})
		}
	}

	return entries, nil
}

// queryUndoAffected returns the affected files for the current undo path.
func (o *Operations) queryUndoAffected(ctx context.Context, parsed *ParsedPath) ([]db.UndoAffectedFile, error) {
	tableName := parsed.OrigTableName
	logTable := tableName + "_log"

	// Build filters from pipeline context
	var filters []db.UndoFilter
	var userID string
	if parsed.Context != nil {
		for _, f := range parsed.Context.Filters {
			if f.Column == "user_id" {
				userID = f.Value
			} else {
				filters = append(filters, db.UndoFilter{Column: f.Column, Value: f.Value})
			}
		}
	}

	switch parsed.UndoMode {
	case "id":
		// Single operation: return one entry
		logID := resolveLogID(parsed.UndoTarget)
		entry, err := o.db.QueryLogEntry(ctx, synth.TigerFSSchema, logTable, logID)
		if err != nil {
			return nil, err
		}
		return []db.UndoAffectedFile{*entry}, nil

	case "to-id":
		afterID := resolveLogID(parsed.UndoTarget)
		return o.db.QueryUndoAffectedFiles(ctx, synth.TigerFSSchema, logTable, afterID, userID, filters)

	case "to-savepoint":
		// Look up savepoint_id by name
		savepointTable := tableName + "_savepoint"
		row, err := o.db.GetRow(ctx, synth.TigerFSSchema, savepointTable, db.SinglePKMatch("name", parsed.UndoTarget))
		if err != nil {
			return nil, fmt.Errorf("savepoint not found: %s", parsed.UndoTarget)
		}
		var savepointID string
		for i, col := range row.Columns {
			if col == "savepoint_id" {
				savepointID, _ = format.ConvertValueToText(row.Values[i])
				break
			}
		}
		if savepointID == "" {
			return nil, fmt.Errorf("savepoint %s has no savepoint_id", parsed.UndoTarget)
		}
		return o.db.QueryUndoAffectedFiles(ctx, synth.TigerFSSchema, logTable, savepointID, userID, filters)
	}

	return nil, fmt.Errorf("unknown undo mode: %s", parsed.UndoMode)
}

// statUndo handles Stat for .undo/ paths beyond the basic directory entry.
func (o *Operations) statUndo(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	now := time.Now()

	// .undo/ root or .undo/<mode>/
	if parsed.UndoTarget == "" {
		name := DirUndo
		if parsed.UndoMode != "" {
			name = parsed.UndoMode
		}
		return &Entry{Name: name, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// .undo/<mode>/<target>/ -- validate target exists
	if parsed.UndoFile == "" && parsed.InfoFile == "" && !parsed.UndoApply {
		if err := o.validateUndoTarget(ctx, parsed); err != nil {
			return nil, err
		}
		return &Entry{Name: parsed.UndoTarget, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// For all sub-paths under a target, validate the target exists first
	if err := o.validateUndoTarget(ctx, parsed); err != nil {
		return nil, err
	}

	// .apply
	if parsed.UndoApply {
		return &Entry{Name: FileApply, IsDir: false, Mode: 0644, ModTime: now}, nil
	}

	// .info directory
	if parsed.InfoFile == "" && parsed.UndoFile == "" {
		return &Entry{Name: DirInfo, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// .info/summary
	if parsed.InfoFile != "" {
		name := parsed.InfoFile
		if name == FileSummary || strings.HasPrefix(name, FileSummary+".") {
			return &Entry{Name: name, IsDir: false, Mode: 0444, ModTime: now}, nil
		}
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("unknown info file: %s", name)}
	}

	// Preview file
	if parsed.UndoFile != "" {
		// Check if it's an intermediate directory in the preview tree
		affected, qErr := o.queryUndoAffected(ctx, parsed)
		if qErr != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to query undo preview", Cause: qErr}
		}
		for _, f := range affected {
			if f.Filename == parsed.UndoFile {
				if f.Type == "create" {
					return &Entry{Name: parsed.UndoFile, IsDir: false, Mode: os.ModeSymlink | 0777, Target: "/dev/null", ModTime: now}, nil
				}
				return &Entry{Name: parsed.UndoFile, IsDir: false, Mode: 0444, ModTime: now}, nil
			}
			// Check if UndoFile is a directory prefix
			if strings.HasPrefix(f.Filename, parsed.UndoFile+"/") {
				return &Entry{Name: parsed.UndoFile, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
			}
		}
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("file not found in undo preview: %s", parsed.UndoFile)}
	}

	return nil, &FSError{Code: ErrNotExist, Message: "invalid .undo/ path"}
}

// readFileUndo handles ReadFile for .undo/ paths.
func (o *Operations) readFileUndo(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	// .info/summary (or summary.json, summary.csv, etc.)
	if parsed.InfoFile != "" {
		return o.readUndoSummary(ctx, parsed)
	}

	// .apply -- write-only, return empty for NFS SETATTR compatibility
	if parsed.UndoApply {
		return &FileContent{Data: []byte{}}, nil
	}

	// Preview file content
	if parsed.UndoFile != "" {
		return o.readUndoPreviewFile(ctx, parsed)
	}

	return nil, &FSError{Code: ErrInvalidPath, Message: "cannot read .undo/ directory as file"}
}

// readUndoSummary returns the .info/summary file for an undo preview.
func (o *Operations) readUndoSummary(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	affected, err := o.queryUndoAffected(ctx, parsed)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query undo summary", Cause: err}
	}

	// Determine output format from InfoFile name (summary.json, summary.csv, etc.)
	outputFormat := "tsv"
	infoFile := parsed.InfoFile
	if idx := strings.LastIndex(infoFile, "."); idx > 0 {
		ext := infoFile[idx:]
		switch ext {
		case ".json":
			outputFormat = "json"
		case ".csv":
			outputFormat = "csv"
		case ".yaml":
			outputFormat = "yaml"
		}
	}

	// Build summary data
	type summaryRow struct {
		Action   string `json:"action"`
		Filename string `json:"filename"`
	}
	var rows []summaryRow
	for _, f := range affected {
		action := "restore"
		if f.Type == "create" {
			action = "delete"
		}
		rows = append(rows, summaryRow{Action: action, Filename: f.Filename})
	}

	var data []byte
	switch outputFormat {
	case "json":
		// Build JSON array manually to match format package patterns
		columns := []string{"action", "filename"}
		var jsonRows [][]interface{}
		for _, r := range rows {
			jsonRows = append(jsonRows, []interface{}{r.Action, r.Filename})
		}
		data, err = format.RowsToJSON(columns, jsonRows)
	case "csv":
		var lines []string
		lines = append(lines, "action,filename")
		for _, r := range rows {
			lines = append(lines, r.Action+","+r.Filename)
		}
		data = []byte(strings.Join(lines, "\n") + "\n")
	case "yaml":
		var lines []string
		for _, r := range rows {
			lines = append(lines, fmt.Sprintf("- action: %s\n  filename: %s", r.Action, r.Filename))
		}
		data = []byte(strings.Join(lines, "\n") + "\n")
	default: // tsv
		var lines []string
		for _, r := range rows {
			lines = append(lines, r.Action+"\t"+r.Filename)
		}
		data = []byte(strings.Join(lines, "\n") + "\n")
	}
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to format undo summary", Cause: err}
	}

	return &FileContent{Data: data}, nil
}

// readUndoPreviewFile returns the content of a file in the undo preview.
// For restore actions: returns the before-state from history.
// For delete actions: returns current file content (the file that will be deleted).
func (o *Operations) readUndoPreviewFile(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	affected, err := o.queryUndoAffected(ctx, parsed)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query undo preview", Cause: err}
	}

	// Find the matching file
	for _, f := range affected {
		if f.Filename != parsed.UndoFile {
			continue
		}

		if f.Type == "create" {
			// File will be deleted on apply -- return current content
			tableName := parsed.OrigTableName
			return o.readSynthFileByID(ctx, synth.TigerFSSchema, tableName, f.FileID)
		}

		// Restore: return before-state from history
		if f.VersionID == "" {
			return nil, &FSError{Code: ErrIO, Message: "no history version for undo preview"}
		}
		tableName := parsed.OrigTableName
		historyTable := tableName + "_history"
		return o.readHistoryByVersionID(ctx, synth.TigerFSSchema, historyTable, tableName, f.VersionID)
	}

	return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("file not found in undo preview: %s", parsed.UndoFile)}
}

// readSynthFileByID reads a synth file by its UUID (for preview of files to be deleted).
func (o *Operations) readSynthFileByID(ctx context.Context, schema, tableName, fileID string) (*FileContent, *FSError) {
	row, err := o.db.GetRow(ctx, schema, tableName, db.SinglePKMatch("id", fileID))
	if err != nil {
		return nil, &FSError{Code: ErrNotExist, Message: "file not found"}
	}

	// Get synth view info to render the content
	info := o.getSynthViewInfo(ctx, schema, tableName)
	if info == nil {
		// Fallback: return raw TSV
		data, fmtErr := format.RowToTSV(row.Columns, interfaceSlice(row.Values))
		if fmtErr != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to format row", Cause: fmtErr}
		}
		return &FileContent{Data: data}, nil
	}

	// Render through synth format layer
	return o.renderSynthContent(row, info)
}

// readHistoryByVersionID reads a history entry and renders it as synth content.
func (o *Operations) readHistoryByVersionID(ctx context.Context, schema, historyTable, sourceTable, versionID string) (*FileContent, *FSError) {
	row, err := o.db.GetRow(ctx, schema, historyTable, db.SinglePKMatch("version_id", versionID))
	if err != nil {
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("history entry not found: %s", versionID)}
	}

	// Map history columns to source columns (file_id -> id, skip version_id/operation)
	var columns []string
	var values []interface{}
	for i, col := range row.Columns {
		if col == "version_id" || col == "operation" {
			continue
		}
		if col == "file_id" {
			col = "id"
		}
		columns = append(columns, col)
		values = append(values, row.Values[i])
	}

	// Get synth view info to render
	info := o.getSynthViewInfo(ctx, schema, sourceTable)
	if info == nil {
		data, fmtErr := format.RowToTSV(columns, values)
		if fmtErr != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to format history row", Cause: fmtErr}
		}
		return &FileContent{Data: data}, nil
	}

	mappedRow := &db.Row{Columns: columns, Values: values}
	return o.renderSynthContent(mappedRow, info)
}

// renderSynthContent renders a row through the synth format layer.
func (o *Operations) renderSynthContent(row *db.Row, info *synth.ViewInfo) (*FileContent, *FSError) {
	var data []byte
	var err error
	switch info.Format {
	case synth.FormatMarkdown:
		data, err = synth.SynthesizeMarkdown(row.Columns, interfaceSlice(row.Values), info.Roles)
	case synth.FormatPlainText:
		data, err = synth.SynthesizePlainText(row.Columns, interfaceSlice(row.Values), info.Roles)
	default:
		data, err = format.RowToTSV(row.Columns, interfaceSlice(row.Values))
	}
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to synthesize content", Cause: err}
	}
	return &FileContent{Data: data}, nil
}

// writeUndoApply triggers an undo operation when .apply is written.
func (o *Operations) writeUndoApply(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	if !parsed.UndoApply {
		return &FSError{
			Code:    ErrPermission,
			Message: ".undo/ is read-only except for .apply",
		}
	}

	// Reject .sample/ + .apply
	if parsed.Context != nil && parsed.Context.LimitType == LimitSample {
		return &FSError{
			Code:    ErrInvalidArgument,
			Message: ".sample/ cannot be combined with .apply (random undo is not supported)",
		}
	}

	tableName := parsed.OrigTableName
	if tableName == "" {
		return &FSError{Code: ErrInvalidPath, Message: ".undo/ requires a table context"}
	}

	// Build filters from pipeline context
	var filters []db.UndoFilter
	if parsed.Context != nil {
		for _, f := range parsed.Context.Filters {
			filters = append(filters, db.UndoFilter{Column: f.Column, Value: f.Value})
		}
	}

	var result *UndoResult
	var err error

	switch parsed.UndoMode {
	case "id":
		result, err = o.ExecuteUndoSingle(ctx, "public", tableName, parsed.UndoTarget)
	case "to-id":
		result, err = o.ExecuteUndoToLogID(ctx, "public", tableName, parsed.UndoTarget, filters)
	case "to-savepoint":
		result, err = o.ExecuteUndoToSavepoint(ctx, "public", tableName, parsed.UndoTarget, filters)
	default:
		return &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown undo mode: %s", parsed.UndoMode)}
	}

	if err != nil {
		return &FSError{Code: ErrIO, Message: "undo failed", Cause: err}
	}

	logging.Info("undo applied via .apply",
		zap.String("table", tableName),
		zap.String("mode", parsed.UndoMode),
		zap.String("target", parsed.UndoTarget),
		zap.Int("files_restored", result.FilesRestored),
		zap.Int("files_deleted", result.FilesDeleted),
		zap.Int("files_skipped", result.FilesSkipped))

	// Invalidate caches after undo
	o.statCache.invalidate(synth.TigerFSSchema, tableName)
	o.pathCache.invalidate(synth.TigerFSSchema, tableName)

	return nil
}

// validateUndoTarget checks that the undo target (savepoint name or log_id) exists.
func (o *Operations) validateUndoTarget(ctx context.Context, parsed *ParsedPath) *FSError {
	tableName := parsed.OrigTableName

	switch parsed.UndoMode {
	case "to-savepoint":
		savepointTable := tableName + "_savepoint"
		_, err := o.db.GetRow(ctx, synth.TigerFSSchema, savepointTable, db.SinglePKMatch("name", parsed.UndoTarget))
		if err != nil {
			return &FSError{Code: ErrNotExist, Message: fmt.Sprintf("savepoint not found: %s", parsed.UndoTarget)}
		}
	case "id", "to-id":
		logTable := tableName + "_log"
		logID := resolveLogID(parsed.UndoTarget)
		_, err := o.db.QueryLogEntry(ctx, synth.TigerFSSchema, logTable, logID)
		if err != nil {
			return &FSError{Code: ErrNotExist, Message: fmt.Sprintf("log entry not found: %s", parsed.UndoTarget)}
		}
	}
	return nil
}

// interfaceSlice converts []interface{} (already the right type) for format functions.
func interfaceSlice(vals []interface{}) []interface{} {
	return vals
}
