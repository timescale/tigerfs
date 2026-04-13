package fs

import (
	"context"
	"fmt"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
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

	// Extract savepoint_id from the row
	var savepointID string
	for i, col := range row.Columns {
		if col == "savepoint_id" {
			if v, ok := row.Values[i].(string); ok {
				savepointID = v
			} else {
				savepointID = fmt.Sprintf("%v", row.Values[i])
			}
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
	var deleteFileIDs []string
	var restoreVersionIDs, restoreFileIDs, restoreFilenames []string
	skipped := 0

	for _, f := range affected {
		switch f.Type {
		case "create":
			// Row was created after target -- check if it still exists
			exists, err := o.db.QueryFileExists(ctx, schema, tableName, f.FileID)
			if err != nil {
				logging.Warn("failed to check file existence during undo",
					zap.String("file_id", f.FileID), zap.Error(err))
				skipped++
				continue
			}
			if exists {
				deleteFileIDs = append(deleteFileIDs, f.FileID)
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
		RestoreVersionIDs: restoreVersionIDs,
		RestoreFileIDs:    restoreFileIDs,
		RestoreFilenames:  restoreFilenames,
		UserID:            o.userID,
	})
	if err != nil {
		return nil, fmt.Errorf("undo transaction failed: %w", err)
	}

	// Invalidate caches
	o.statCache.invalidate(schema, tableName)

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

// ExecuteUndoSingle undoes a single log entry.
func (o *Operations) ExecuteUndoSingle(ctx context.Context, schema, tableName, logID string) (*UndoResult, error) {
	logTable := tableName + "_log"
	historyTable := tableName + "_history"

	// Fetch the log entry
	entry, err := o.db.QueryLogEntry(ctx, synth.TigerFSSchema, logTable, logID)
	if err != nil {
		return nil, fmt.Errorf("log entry not found: %s", logID)
	}

	desc := fmt.Sprintf("Undo single operation %s", logID)

	var deleteFileIDs []string
	var restoreVersionIDs, restoreFileIDs, restoreFilenames []string
	skipped := 0

	switch entry.Type {
	case "create":
		exists, err := o.db.QueryFileExists(ctx, schema, tableName, entry.FileID)
		if err != nil || !exists {
			skipped = 1
		} else {
			deleteFileIDs = append(deleteFileIDs, entry.FileID)
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
