package fs

import (
	"context"
	"encoding/json"
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

// blockingSubjects enumerates the metadata-entry subjects whose presence
// refuses undo across them. An entry whose subject is in this set acts as
// a boundary: any target log_id strictly less than the entry's entry_id
// (UUIDv7-encoded timestamp) cannot be safely undone and must be refused
// with a clear errno.
//
// Subjects are version-agnostic by design: a future history-format
// migration writes another row with SubjectHistoryFormatMigration and
// pushes the boundary forward without any engine change.
//
// The set is hard-coded here (not stored on the row) so the *consumer* of
// the metadata table decides what affects its behavior. Adding a new
// blocking category is a deliberate code change reviewed in a PR.
var blockingSubjects = map[string]struct{}{
	synth.SubjectHistoryFormatMigration: {},
}

// checkBoundary refuses if targetLogID precedes any metadata entry whose
// subject is in blockingSubjects. Returns nil for fresh-install workspaces
// (no metadata) -- this is the hot-path fast return.
//
// The check uses string compare on UUIDv7 values (entry_id and log_id are
// both UUIDv7 in the same clock domain, so lexical compare == time order).
// Equality is treated as "not before" -- the boundary entry itself is not
// blocked.
func (o *Operations) checkBoundary(ctx context.Context, schema, tableName, targetLogID string) *FSError {
	info := o.getSynthViewInfo(ctx, schema, tableName)
	if info == nil || len(info.Metadata) == 0 {
		return nil
	}
	for _, m := range info.Metadata {
		if _, isBlocker := blockingSubjects[m.Subject]; !isBlocker {
			continue
		}
		if targetLogID < m.EntryID {
			return &FSError{
				Code:    ErrPermission,
				Message: fmt.Sprintf("undo refused: target precedes %s boundary", m.Subject),
				Hint:    m.Description,
			}
		}
	}
	return nil
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
// Accepts either a raw UUIDv7 or the display-name form
// ("2026-04-08T143015.234Z-...") -- the latter is what users see in
// .log/.by/... listings. ExecuteUndoSingle has the same resolution; this
// keeps the two log-id entry points symmetric and makes
// `.undo/to-id/<display-name>/.apply` work end-to-end.
func (o *Operations) ExecuteUndoToLogID(ctx context.Context, schema, tableName, logID string, filters []db.UndoFilter) (*UndoResult, error) {
	logID = resolveLogID(logID)
	desc := fmt.Sprintf("Undo to log entry %s", logID)
	return o.ExecuteUndo(ctx, schema, tableName, logID, desc, filters)
}

// ExecuteUndo undoes all operations after a target point (savepoint_id or log_id).
// Executes all changes in a single PostgreSQL transaction.
// The schema parameter identifies the user-facing schema for cache
// invalidation (where the view lives). The synth backing tables themselves
// always live in synth.TigerFSSchema.
func (o *Operations) ExecuteUndo(ctx context.Context, schema, tableName, afterID, description string, filters []db.UndoFilter) (*UndoResult, error) {
	logTable := tableName + "_log"
	historyTable := tableName + "_history"

	// Refuse if the target precedes a recorded boundary (e.g. the v0.7
	// history-format migration). Checked at the top so no transaction is
	// begun and no partial work happens when the window crosses the
	// boundary. Applies to both ExecuteUndoToLogID and
	// ExecuteUndoToSavepoint via this shared core.
	if fsErr := o.checkBoundary(ctx, schema, tableName, afterID); fsErr != nil {
		return nil, fsErr
	}

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

	// Find all affected files, ordered child-first (see QueryUndoAffectedFiles
	// for the topological-sort rationale).
	affected, err := o.db.QueryUndoAffectedFiles(ctx, synth.TigerFSSchema, logTable, tableName, historyTable, afterID, userID, queryFilters)
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

		case "edit", "rename", "delete":
			if f.VersionID == "" {
				logging.Warn("undo: missing version_id for non-create operation",
					zap.String("file_id", f.FileID), zap.String("type", f.Type))
				skipped++
				continue
			}
			restoreVersionIDs = append(restoreVersionIDs, f.VersionID)
			restoreFileIDs = append(restoreFileIDs, f.FileID)
			restoreFilenames = append(restoreFilenames, f.Filename)

		case "undo":
			// Undo-of-undo: dispatch by the history row's operation column.
			// 'create' tombstone => the original entry restored a row, so
			// reversing it means re-deleting. Other ops => the standard
			// "restore from version_id" path produces the right state.
			if f.VersionID == "" {
				logging.Warn("undo: missing version_id for undo entry",
					zap.String("file_id", f.FileID))
				skipped++
				continue
			}
			histOp, err := o.db.QueryHistoryOperation(ctx, synth.TigerFSSchema, historyTable, f.VersionID)
			if err != nil {
				logging.Warn("undo: failed to query history operation for undo entry; falling back to restore-from-version_id",
					zap.String("file_id", f.FileID), zap.String("version_id", f.VersionID), zap.Error(err))
				restoreVersionIDs = append(restoreVersionIDs, f.VersionID)
				restoreFileIDs = append(restoreFileIDs, f.FileID)
				restoreFilenames = append(restoreFilenames, f.Filename)
				continue
			}
			switch histOp {
			case "create":
				exists, err := o.db.QueryFileExists(ctx, synth.TigerFSSchema, tableName, f.FileID)
				if err != nil || !exists {
					skipped++
					continue
				}
				deleteFileIDs = append(deleteFileIDs, f.FileID)
				deleteFilenames = append(deleteFilenames, f.Filename)
			case "edit", "rename", "delete":
				restoreVersionIDs = append(restoreVersionIDs, f.VersionID)
				restoreFileIDs = append(restoreFileIDs, f.FileID)
				restoreFilenames = append(restoreFilenames, f.Filename)
			default:
				logging.Warn("undo: unexpected history operation",
					zap.String("op", histOp), zap.String("file_id", f.FileID))
				skipped++
			}

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

	// Invalidate caches under the user's schema (where the view lives and
	// where statSynthFile populates the cache). Using synth.TigerFSSchema
	// here would leave cached entries -- including negative entries -- in
	// place after undo.
	o.statCache.invalidate(schema, tableName)
	o.undoCache.invalidate()

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

	// Refuse if the target precedes a recorded boundary (e.g. the v0.7
	// history-format migration). Pre-boundary entries are readable in
	// .log/ and .history/ but cannot be safely undone.
	if fsErr := o.checkBoundary(ctx, schema, tableName, logID); fsErr != nil {
		return nil, fsErr
	}

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

	case "edit", "rename", "delete":
		if entry.VersionID == "" {
			return nil, fmt.Errorf("cannot undo %s operation: no version_id (before-state not captured)", entry.Type)
		}
		restoreVersionIDs = append(restoreVersionIDs, entry.VersionID)
		restoreFileIDs = append(restoreFileIDs, entry.FileID)
		restoreFilenames = append(restoreFilenames, entry.Filename)

	case "undo":
		// Undo-of-undo: dispatch by the history row's operation column to
		// distinguish the cases that need DELETE (operation='create' tombstone)
		// from those that need restore (operation in 'edit','rename','delete').
		if entry.VersionID == "" {
			return nil, fmt.Errorf("cannot undo undo operation: no version_id (before-state not captured)")
		}
		histOp, err := o.db.QueryHistoryOperation(ctx, synth.TigerFSSchema, historyTable, entry.VersionID)
		if err != nil {
			// Legacy undo entry from before the tombstone trigger landed (or a
			// transient DB error). Fall back to restore-from-version_id, which
			// matches pre-fix behavior for backwards compatibility.
			logging.Warn("undo-of-undo: failed to query history operation; falling back to restore-from-version_id",
				zap.String("version_id", entry.VersionID), zap.Error(err))
			restoreVersionIDs = append(restoreVersionIDs, entry.VersionID)
			restoreFileIDs = append(restoreFileIDs, entry.FileID)
			restoreFilenames = append(restoreFilenames, entry.Filename)
			break
		}
		switch histOp {
		case "create":
			exists, err := o.db.QueryFileExists(ctx, synth.TigerFSSchema, tableName, entry.FileID)
			if err != nil || !exists {
				skipped = 1
			} else {
				deleteFileIDs = append(deleteFileIDs, entry.FileID)
				deleteFilenames = append(deleteFilenames, entry.Filename)
			}
		case "edit", "rename", "delete":
			restoreVersionIDs = append(restoreVersionIDs, entry.VersionID)
			restoreFileIDs = append(restoreFileIDs, entry.FileID)
			restoreFilenames = append(restoreFilenames, entry.Filename)
		default:
			return nil, fmt.Errorf("unexpected history operation for undo entry: %s", histOp)
		}

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
		// id/ mode: single operation, just show .info/ and .apply (no preview tree).
		// Use .log/<id>/before and .log/<id>/after for diffing.
		if parsed.UndoMode == "id" {
			return []Entry{
				{Name: DirInfo, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
				{Name: FileApply, IsDir: false, Mode: 0644, ModTime: now},
			}, nil
		}
		return o.readDirUndoPreview(ctx, parsed)
	}

	// Level 3: .undo/<mode>/<target>/.info/ -- info directory
	if parsed.InfoFile == "." {
		return []Entry{
			{Name: FileSummary, IsDir: false, Mode: 0444, ModTime: now},
		}, nil
	}

	// Level 3: subdirectory within the preview tree (e.g., tutorials/)
	if parsed.UndoFile != "" {
		return o.readDirUndoSubdir(ctx, parsed)
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

	// Build top-level entries only. Nested files appear inside their directories.
	seenDirs := make(map[string]bool)

	for _, f := range affected {
		parts := strings.Split(f.Filename, "/")

		if len(parts) > 1 {
			// Nested file -- add top-level directory if not seen
			topDir := parts[0]
			if !seenDirs[topDir] {
				seenDirs[topDir] = true
				entries = append(entries, Entry{
					Name: topDir, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now,
				})
			}
		} else {
			// Root-level file
			if f.Type == "create" {
				entries = append(entries, Entry{
					Name: f.Filename, IsDir: false, Mode: os.ModeSymlink | 0777, Target: "/dev/null", ModTime: now,
				})
			} else {
				entries = append(entries, Entry{
					Name: f.Filename, IsDir: false, Mode: 0444, ModTime: now,
				})
			}
		}
	}

	return entries, nil
}

// readDirUndoSubdir lists files within a subdirectory of the undo preview tree.
func (o *Operations) readDirUndoSubdir(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()
	prefix := parsed.UndoFile + "/"

	affected, err := o.queryUndoAffected(ctx, parsed)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query undo preview", Cause: err}
	}

	// Collect entries that are direct children of this directory prefix
	seenDirs := make(map[string]bool)
	var entries []Entry

	for _, f := range affected {
		if !strings.HasPrefix(f.Filename, prefix) {
			continue
		}
		// Get the relative path after the prefix
		rel := f.Filename[len(prefix):]
		// If rel contains a slash, the immediate child is a directory
		if idx := strings.Index(rel, "/"); idx >= 0 {
			dirName := rel[:idx]
			if !seenDirs[dirName] {
				seenDirs[dirName] = true
				entries = append(entries, Entry{
					Name: dirName, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now,
				})
			}
		} else {
			// Direct child file
			if f.Type == "create" {
				entries = append(entries, Entry{
					Name: rel, IsDir: false, Mode: os.ModeSymlink | 0777, Target: "/dev/null", ModTime: now,
				})
			} else {
				entries = append(entries, Entry{
					Name: rel, IsDir: false, Mode: 0444, ModTime: now,
				})
			}
		}
	}

	if len(entries) == 0 {
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("directory not found in undo preview: %s", parsed.UndoFile)}
	}

	return entries, nil
}

// queryUndoAffected returns the affected files for the current undo path.
func (o *Operations) queryUndoAffected(ctx context.Context, parsed *ParsedPath) ([]db.UndoAffectedFile, error) {
	tableName := parsed.OrigTableName
	logTable := tableName + "_log"
	historyTable := tableName + "_history"

	// Build filters from pipeline context
	var filters []db.UndoFilter
	var userID string
	var filterKey string
	if parsed.Context != nil {
		for _, f := range parsed.Context.Filters {
			if f.Column == "user_id" {
				userID = f.Value
			} else {
				filters = append(filters, db.UndoFilter{Column: f.Column, Value: f.Value})
			}
			filterKey += f.Column + "=" + f.Value + ","
		}
	}

	// Check affected files cache
	if cached, ok := o.undoCache.lookupAffected(parsed.UndoMode, parsed.UndoTarget, filterKey); ok {
		return cached, nil
	}

	var result []db.UndoAffectedFile
	var err error

	switch parsed.UndoMode {
	case "id":
		// Single operation: return one entry (uses log entry cache)
		logID := resolveLogID(parsed.UndoTarget)
		entry, qErr := o.cachedQueryLogEntry(ctx, synth.TigerFSSchema, logTable, logID)
		if qErr != nil {
			return nil, qErr
		}
		result = []db.UndoAffectedFile{*entry}

	case "to-id":
		afterID := resolveLogID(parsed.UndoTarget)
		result, err = o.db.QueryUndoAffectedFiles(ctx, synth.TigerFSSchema, logTable, tableName, historyTable, afterID, userID, filters)

	case "to-savepoint":
		// Look up savepoint_id by name (uses savepoint cache)
		savepointID, spErr := o.cachedSavepointID(ctx, tableName, parsed.UndoTarget)
		if spErr != nil {
			return nil, spErr
		}
		result, err = o.db.QueryUndoAffectedFiles(ctx, synth.TigerFSSchema, logTable, tableName, historyTable, savepointID, userID, filters)

	default:
		return nil, fmt.Errorf("unknown undo mode: %s", parsed.UndoMode)
	}

	if err != nil {
		return nil, err
	}

	// Cache the result
	o.undoCache.storeAffected(parsed.UndoMode, parsed.UndoTarget, filterKey, result)
	return result, nil
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
	if parsed.InfoFile == "." {
		return &Entry{Name: DirInfo, IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// .info/summary
	if parsed.InfoFile != "" && parsed.InfoFile != "." {
		name := parsed.InfoFile
		if name == FileSummary || strings.HasPrefix(name, FileSummary+".") {
			// Compute actual size by generating the summary content
			fc, summaryErr := o.readUndoSummary(ctx, parsed)
			size := int64(0)
			if summaryErr == nil {
				size = int64(len(fc.Data))
			}
			return &Entry{Name: name, IsDir: false, Size: size, Mode: 0444, ModTime: now}, nil
		}
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("unknown info file: %s", name)}
	}

	// Preview file (not available in id/ mode -- use .log/<id>/before instead)
	if parsed.UndoFile != "" && parsed.UndoMode == "id" {
		return nil, &FSError{Code: ErrNotExist, Message: "preview files not available in .undo/id/ mode; use .log/<id>/before for diffs"}
	}
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
				// Compute actual size by rendering the preview content
				fc, previewErr := o.readUndoPreviewFile(ctx, parsed)
				size := int64(0)
				if previewErr == nil {
					size = int64(len(fc.Data))
				}
				return &Entry{Name: parsed.UndoFile, IsDir: false, Size: size, Mode: 0444, ModTime: now}, nil
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

	// Preview file content (not available in id/ mode)
	if parsed.UndoFile != "" {
		if parsed.UndoMode == "id" {
			return nil, &FSError{Code: ErrNotExist, Message: "preview files not available in .undo/id/ mode; use .log/<id>/before for diffs"}
		}
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

	// Gather metadata about the target
	meta := o.undoSummaryMetadata(ctx, parsed)

	// Build file rows with enriched columns
	var rows []summaryFileRow
	for _, f := range affected {
		ts := uuidTimestamp(f.LogID)
		rows = append(rows, summaryFileRow{
			Type:      f.Type,
			Filename:  f.Filename,
			User:      f.UserID,
			Timestamp: ts,
		})
	}

	var data []byte
	switch outputFormat {
	case "json":
		data, err = o.formatSummaryJSON(meta, rows)
	case "csv":
		data = o.formatSummaryCSV(rows)
	case "yaml":
		data = o.formatSummaryYAML(meta, rows)
	default: // tsv
		data = o.formatSummaryTSV(meta, rows)
	}
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to format undo summary", Cause: err}
	}

	return &FileContent{Data: data}, nil
}

// undoSummaryMeta holds metadata about the undo target for summary headers.
type undoSummaryMeta struct {
	Mode        string // "id", "to-id", "to-savepoint"
	Target      string // savepoint name or log_id display name
	TargetTime  string // timestamp of the target (from UUIDv7)
	User        string // user who created the savepoint (empty for log targets)
	Description string // savepoint description (empty for log targets)
	Affected    int    // number of affected files
}

// undoSummaryMetadata gathers metadata about the undo target.
func (o *Operations) undoSummaryMetadata(ctx context.Context, parsed *ParsedPath) undoSummaryMeta {
	meta := undoSummaryMeta{
		Mode:   parsed.UndoMode,
		Target: parsed.UndoTarget,
	}

	if parsed.UndoMode == "to-savepoint" {
		savepointTable := parsed.OrigTableName + "_savepoint"
		row, ok := o.undoCache.lookupSavepoint(synth.TigerFSSchema, savepointTable, parsed.UndoTarget)
		if !ok {
			var err error
			row, err = o.db.GetRow(ctx, synth.TigerFSSchema, savepointTable, db.SinglePKMatch("name", parsed.UndoTarget))
			if err == nil {
				o.undoCache.storeSavepoint(synth.TigerFSSchema, savepointTable, parsed.UndoTarget, row)
			}
		}
		if row != nil {
			for i, col := range row.Columns {
				switch col {
				case "savepoint_id":
					id, _ := format.ConvertValueToText(row.Values[i])
					meta.TargetTime = uuidTimestamp(id)
				case "user_id":
					if row.Values[i] != nil {
						meta.User, _ = format.ConvertValueToText(row.Values[i])
					}
				case "description":
					if row.Values[i] != nil {
						meta.Description, _ = format.ConvertValueToText(row.Values[i])
					}
				}
			}
		}
	} else {
		// For id/ and to-id/, extract timestamp from the target log_id
		meta.TargetTime = uuidTimestamp(resolveLogID(parsed.UndoTarget))
	}

	return meta
}

// cachedSavepointID looks up a savepoint's savepoint_id by name, using the cache.
func (o *Operations) cachedSavepointID(ctx context.Context, tableName, savepointName string) (string, error) {
	savepointTable := tableName + "_savepoint"

	// Check cache
	if row, ok := o.undoCache.lookupSavepoint(synth.TigerFSSchema, savepointTable, savepointName); ok {
		return extractSavepointID(row)
	}

	// Query DB
	row, err := o.db.GetRow(ctx, synth.TigerFSSchema, savepointTable, db.SinglePKMatch("name", savepointName))
	if err != nil {
		return "", fmt.Errorf("savepoint not found: %s", savepointName)
	}

	// Cache the row
	o.undoCache.storeSavepoint(synth.TigerFSSchema, savepointTable, savepointName, row)

	return extractSavepointID(row)
}

// extractSavepointID extracts the savepoint_id string from a savepoint row.
func extractSavepointID(row *db.Row) (string, error) {
	for i, col := range row.Columns {
		if col == "savepoint_id" {
			id, _ := format.ConvertValueToText(row.Values[i])
			if id != "" {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("savepoint row has no savepoint_id")
}

// cachedQueryLogEntry fetches a log entry by ID, using the cache.
func (o *Operations) cachedQueryLogEntry(ctx context.Context, schema, logTable, logID string) (*db.UndoAffectedFile, error) {
	// Check cache
	if entry, ok := o.undoCache.lookupLogEntry(schema, logTable, logID); ok {
		return entry, nil
	}

	// Query DB
	entry, err := o.db.QueryLogEntry(ctx, schema, logTable, logID)
	if err != nil {
		return nil, err
	}

	// Cache
	o.undoCache.storeLogEntry(schema, logTable, logID, entry)
	return entry, nil
}

// uuidTimestamp extracts a UTC timestamp string from a UUID string.
func uuidTimestamp(uuidStr string) string {
	t := uuidv7ModTime(uuidStr)
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

type summaryFileRow struct {
	Type      string
	Filename  string
	User      string
	Timestamp string
}

func (o *Operations) formatSummaryTSV(meta undoSummaryMeta, rows []summaryFileRow) []byte {
	var lines []string

	// Metadata headers as # comments
	switch meta.Mode {
	case "to-savepoint":
		lines = append(lines, "# savepoint: "+meta.Target)
		if meta.TargetTime != "" {
			lines = append(lines, "# created: "+meta.TargetTime)
		}
		if meta.User != "" {
			lines = append(lines, "# user: "+meta.User)
		}
		if meta.Description != "" {
			lines = append(lines, "# description: "+meta.Description)
		}
	default: // id, to-id
		if meta.TargetTime != "" {
			lines = append(lines, "# target: "+meta.TargetTime)
		}
	}

	fileWord := "files"
	if len(rows) == 1 {
		fileWord = "file"
	}
	lines = append(lines, fmt.Sprintf("# affected: %d %s", len(rows), fileWord))
	lines = append(lines, "# type\tfilename\tuser\ttimestamp")

	for _, r := range rows {
		lines = append(lines, r.Type+"\t"+r.Filename+"\t"+r.User+"\t"+r.Timestamp)
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func (o *Operations) formatSummaryCSV(rows []summaryFileRow) []byte {
	var lines []string
	lines = append(lines, "type,filename,user,timestamp")
	for _, r := range rows {
		lines = append(lines, r.Type+","+r.Filename+","+r.User+","+r.Timestamp)
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func (o *Operations) formatSummaryJSON(meta undoSummaryMeta, rows []summaryFileRow) ([]byte, error) {
	// Build a structured JSON object
	obj := map[string]interface{}{}
	switch meta.Mode {
	case "to-savepoint":
		obj["savepoint"] = meta.Target
		if meta.TargetTime != "" {
			obj["created"] = meta.TargetTime
		}
		if meta.User != "" {
			obj["user"] = meta.User
		}
		if meta.Description != "" {
			obj["description"] = meta.Description
		}
	default:
		if meta.TargetTime != "" {
			obj["target"] = meta.TargetTime
		}
	}
	obj["affected"] = len(rows)

	var files []map[string]string
	for _, r := range rows {
		files = append(files, map[string]string{
			"type": r.Type, "filename": r.Filename, "user": r.User, "timestamp": r.Timestamp,
		})
	}
	obj["files"] = files

	data, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (o *Operations) formatSummaryYAML(meta undoSummaryMeta, rows []summaryFileRow) []byte {
	var lines []string

	switch meta.Mode {
	case "to-savepoint":
		lines = append(lines, "savepoint: "+meta.Target)
		if meta.TargetTime != "" {
			lines = append(lines, "created: "+meta.TargetTime)
		}
		if meta.User != "" {
			lines = append(lines, "user: "+meta.User)
		}
		if meta.Description != "" {
			lines = append(lines, "description: "+meta.Description)
		}
	default:
		if meta.TargetTime != "" {
			lines = append(lines, "target: "+meta.TargetTime)
		}
	}

	lines = append(lines, fmt.Sprintf("affected: %d", len(rows)))
	lines = append(lines, "files:")
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("  - type: %s\n    filename: %s\n    user: %s\n    timestamp: %s",
			r.Type, r.Filename, r.User, r.Timestamp))
	}
	return []byte(strings.Join(lines, "\n") + "\n")
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
// The backing table is in the tigerfs schema, but the synth view info is registered
// under the public schema (where the view lives).
func (o *Operations) readSynthFileByID(ctx context.Context, schema, tableName, fileID string) (*FileContent, *FSError) {
	row, err := o.db.GetRow(ctx, schema, tableName, db.SinglePKMatch("id", fileID))
	if err != nil {
		return nil, &FSError{Code: ErrNotExist, Message: "file not found"}
	}

	// Synth view is registered under public schema, not tigerfs schema.
	viewSchema := o.cachedSchema
	info := o.getSynthViewInfo(ctx, viewSchema, tableName)
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
	// Check history row cache (immutable data, safe to cache)
	row, ok := o.undoCache.lookupHistoryRow(schema, historyTable, versionID)
	if !ok {
		var err error
		row, err = o.db.GetRow(ctx, schema, historyTable, db.SinglePKMatch("version_id", versionID))
		if err != nil {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("history entry not found: %s", versionID)}
		}
		o.undoCache.storeHistoryRow(schema, historyTable, versionID, row)
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

	// Synth view is registered under public schema (where the view lives),
	// not the tigerfs schema (where the backing table lives).
	viewSchema := o.cachedSchema
	info := o.getSynthViewInfo(ctx, viewSchema, sourceTable)
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

	// Cache keys are (user_schema, view_name); the synth backing tables live
	// in synth.TigerFSSchema but stat/path lookups happen against the user's
	// view. Use the user's schema for invalidation so cached entries
	// (including negatives) actually clear after undo.
	//
	// parsed.Context and parsed.Context.Schema are invariants here:
	// processUndo (path.go) errors out on nil Context, and resolveSchema
	// (operations.go) errors out when current_schema() can't be resolved.
	// Either failure aborts WriteFile before this function is called.
	cacheSchema := parsed.Context.Schema

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
		result, err = o.ExecuteUndoSingle(ctx, cacheSchema, tableName, parsed.UndoTarget)
	case "to-id":
		result, err = o.ExecuteUndoToLogID(ctx, cacheSchema, tableName, parsed.UndoTarget, filters)
	case "to-savepoint":
		result, err = o.ExecuteUndoToSavepoint(ctx, cacheSchema, tableName, parsed.UndoTarget, filters)
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
	o.statCache.invalidate(cacheSchema, tableName)
	o.pathCache.invalidate(cacheSchema, tableName)
	o.undoCache.invalidate()

	return nil
}

// validateUndoTarget checks that the undo target (savepoint name or log_id) exists.
func (o *Operations) validateUndoTarget(ctx context.Context, parsed *ParsedPath) *FSError {
	tableName := parsed.OrigTableName

	switch parsed.UndoMode {
	case "to-savepoint":
		// Uses savepoint cache -- populates cache for subsequent queryUndoAffected calls
		_, err := o.cachedSavepointID(ctx, tableName, parsed.UndoTarget)
		if err != nil {
			return &FSError{Code: ErrNotExist, Message: fmt.Sprintf("savepoint not found: %s", parsed.UndoTarget)}
		}
	case "id", "to-id":
		// Uses log entry cache
		logTable := tableName + "_log"
		logID := resolveLogID(parsed.UndoTarget)
		_, err := o.cachedQueryLogEntry(ctx, synth.TigerFSSchema, logTable, logID)
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
