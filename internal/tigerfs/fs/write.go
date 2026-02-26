package fs

import (
	"context"
	"fmt"
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// WriteFile writes content to a file path.
//
// Supported path types and behaviors:
//   - /table/pk.json → UPDATE existing row or INSERT new row
//   - /table/pk/column → UPDATE single column value
//   - /table/.import/.sync/data.csv → Bulk sync import
//   - /table/.import/.overwrite/data.csv → Bulk overwrite import
//   - /table/.import/.append/data.csv → Bulk append import
//
// Parameters:
//   - ctx: context for database operations and cancellation
//   - path: filesystem path to write to
//   - data: file content (format determined by path extension)
//
// Returns nil on success, or an FSError describing the failure.
// Common errors:
//   - ErrInvalidPath: unsupported path type for writes
//   - ErrPermission: view is not updatable
//   - ErrIO: database operation failed
func (o *Operations) WriteFile(ctx context.Context, path string, data []byte) *FSError {
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return err
	}
	o.resolveSynthHierarchy(ctx, parsed)

	return o.writeFileWithParsed(ctx, parsed, data)
}

// writeFileWithParsed implements write logic for a parsed path.
func (o *Operations) writeFileWithParsed(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	switch parsed.Type {
	case PathRow:
		return o.writeRowFile(ctx, parsed, data)
	case PathColumn:
		return o.writeColumnFile(ctx, parsed, data)
	case PathImport:
		return o.writeImportFile(ctx, parsed, data)
	case PathDDL:
		return o.writeDDLFile(ctx, parsed, data)
	case PathBuild:
		return o.writeBuildFile(ctx, parsed, data)
	case PathFormat:
		return o.writeFormatFile(ctx, parsed, data)
	case PathHistory:
		return &FSError{
			Code:    ErrPermission,
			Message: ".history/ is read-only",
			Hint:    "history files are archived versions and cannot be modified",
		}
	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot write to path type: %d", parsed.Type),
		}
	}
}

// writeRowFile writes a row file (UPDATE or INSERT).
// For synthesized views, parses the content and writes to the synth view.
func (o *Operations) writeRowFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Check if this is a synthesized view
	if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil {
		return o.writeSynthFile(ctx, parsed, info, data)
	}

	// Check if this is a view and if it's updatable
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	pkColumn := pk.Columns[0]

	// Parse data based on format
	var columns []string
	var values []interface{}
	var parseErr error

	if parsed.Format == "" {
		// Bare path: PUT semantics - values in schema column order (no headers)
		// This matches FUSE behavior for paths like /table/123 without format extension
		columns, values, parseErr = o.parseWriteDataNoHeaders(ctx, fsCtx.Schema, fsCtx.TableName, data)
	} else {
		// Format specified: PATCH semantics - data includes column names
		columns, values, parseErr = o.parseWriteData(data, parsed.Format)
	}

	if parseErr != nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "failed to parse write data",
			Cause:   parseErr,
		}
	}

	// Check if row exists
	_, err = o.db.GetRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey)
	rowExists := err == nil

	if rowExists {
		// UPDATE existing row
		err = o.db.UpdateRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, columns, values)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to update row",
				Cause:   err,
			}
		}
	} else {
		// INSERT new row
		_, err = o.db.InsertRow(ctx, fsCtx.Schema, fsCtx.TableName, columns, values)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to insert row",
				Cause:   err,
			}
		}
	}

	return nil
}

// writeColumnFile writes a single column value.
// The filename may include an extension (e.g., "name.txt" for a TEXT column),
// which is resolved to the actual column name before updating.
func (o *Operations) writeColumnFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for column path",
		}
	}

	// Check write permission
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Get columns to resolve the filename
	columns, err := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	// Resolve filename to actual column name (handles extensions)
	actualColumn, found := o.resolveColumn(columns, parsed.Column)
	if !found {
		return &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("column not found: %s", parsed.Column),
		}
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	pkColumn := pk.Columns[0]

	// Convert data to string, trim trailing newline
	value := string(data)
	if len(value) > 0 && value[len(value)-1] == '\n' {
		value = value[:len(value)-1]
	}

	// Check if row exists
	_, err = o.db.GetRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey)
	rowExists := err == nil

	if !rowExists {
		// Row doesn't exist - use staging for incremental creation
		if o.staging != nil {
			o.staging.SetColumn(fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, actualColumn, value)
			// Try to commit if enough columns provided
			_, commitErr := o.staging.TryCommit(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, o.db)
			if commitErr != nil {
				return &FSError{
					Code:    ErrIO,
					Message: "failed to commit staged row",
					Cause:   commitErr,
				}
			}
			return nil
		}
		return &FSError{
			Code:    ErrNotExist,
			Message: "row does not exist",
		}
	}

	// Row exists - update column using the resolved column name
	err = o.db.UpdateColumn(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, actualColumn, value)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to update column",
			Cause:   err,
		}
	}

	return nil
}

// writeImportFile handles bulk import writes.
func (o *Operations) writeImportFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for import path",
		}
	}

	// Check write permission
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Parse data based on format and no-headers option
	columns, rows, err := o.parseImportData(ctx, parsed, data)
	if err != nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "failed to parse import data",
			Cause:   err,
		}
	}

	// Execute based on import mode
	switch parsed.ImportMode {
	case "sync":
		err = o.db.ImportSync(ctx, fsCtx.Schema, fsCtx.TableName, columns, rows)
	case "overwrite":
		err = o.db.ImportOverwrite(ctx, fsCtx.Schema, fsCtx.TableName, columns, rows)
	case "append":
		err = o.db.ImportAppend(ctx, fsCtx.Schema, fsCtx.TableName, columns, rows)
	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown import mode: %s", parsed.ImportMode),
		}
	}

	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to import data",
			Cause:   err,
		}
	}

	return nil
}

// parseImportData parses import data based on format and no-headers option.
func (o *Operations) parseImportData(ctx context.Context, parsed *ParsedPath, data []byte) ([]string, [][]interface{}, error) {
	fsCtx := parsed.Context
	importFormat := parsed.Format
	if importFormat == "" {
		importFormat = "csv" // default
	}

	// Handle no-headers mode for CSV/TSV
	if parsed.ImportNoHeaders {
		// Fetch column names from schema
		cols, err := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get columns from schema: %w", err)
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("no columns found for table %s.%s", fsCtx.Schema, fsCtx.TableName)
		}

		// Extract column names in schema order
		columns := make([]string, len(cols))
		for i, col := range cols {
			columns[i] = col.Name
		}

		switch importFormat {
		case "csv":
			return format.ParseCSVBulkNoHeaders(data, columns)
		case "tsv":
			return format.ParseTSVBulkNoHeaders(data, columns)
		default:
			return nil, nil, fmt.Errorf("no-headers mode not supported for %s format", importFormat)
		}
	}

	// Standard parsing with headers
	switch importFormat {
	case "csv":
		return format.ParseCSVBulk(data)
	case "tsv":
		return format.ParseTSVBulk(data)
	case "json":
		return format.ParseJSONBulk(data)
	case "yaml":
		return format.ParseYAMLBulk(data)
	default:
		return nil, nil, fmt.Errorf("unknown import format: %s", importFormat)
	}
}

// writeDDLFile handles DDL staging file writes.
//
// Supported files:
//   - sql: Write DDL content
//   - .test: Trigger validation (data ignored)
//   - .commit: Execute DDL (data ignored)
//   - .abort: Cancel staging session (data ignored)
func (o *Operations) writeDDLFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	// Convert operation string to DDLOpType
	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp),
		}
	}

	// Trigger files on completed sessions are no-ops. This handles re-firing
	// after commit/abort (FUSE dup2 close, Setattr(fh=nil), NFS Chtimes).
	if parsed.DDLFile == FileTest || parsed.DDLFile == FileCommit || parsed.DDLFile == FileAbort {
		sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
		if sessionID == "" {
			return nil // No session at all — no-op
		}
		if s := o.ddl.GetSession(sessionID); s != nil && s.Completed {
			return nil // Already committed/aborted — no-op
		}
	}

	// Check for context cancellation before proceeding
	if ctx.Err() != nil {
		return &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("DDL write aborted: context cancelled for %s/%s", parsed.DDLOp, parsed.DDLName),
			Hint:    fmt.Sprintf("context error: %v", ctx.Err()),
			Cause:   ctx.Err(),
		}
	}

	// Ensure session exists (auto-create for table-level DDL)
	if err := o.ensureDDLSession(parsed, op); err != nil {
		return err
	}

	// Find session by name
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID == "" {
		return &FSError{
			Code:    ErrIO,
			Message: fmt.Sprintf("DDL session vanished for %s/%s", parsed.DDLOp, parsed.DDLName),
			Hint:    "session existed in ensureDDLSession but not in FindSessionByName — possible race condition",
		}
	}

	// Handle the specific file
	switch parsed.DDLFile {
	case FileSQL:
		// Protect against writes to completed sessions
		if session := o.ddl.GetSession(sessionID); session != nil && session.Completed {
			return &FSError{
				Code:    ErrInvalidOperation,
				Message: "DDL session already completed",
				Hint:    "create a new session with mkdir",
			}
		}
		// Write DDL content
		err := o.ddl.WriteSQL(sessionID, string(data))
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to write DDL content",
				Cause:   err,
			}
		}
		return nil

	case FileTest:
		// Trigger validation — log result to stderr for user feedback
		result, err := o.ddl.Test(ctx, sessionID)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to test DDL",
				Cause:   err,
			}
		}
		// Trim whitespace/newlines to prevent garbled JSON log output
		result = strings.TrimSpace(result)
		if strings.HasPrefix(result, "OK") {
			logging.Info(result,
				zap.String("object", parsed.DDLName),
				zap.String("hint", "cat test.log for details"))
		} else {
			logging.Warn(result,
				zap.String("object", parsed.DDLName),
				zap.String("hint", "cat test.log for details"))
		}
		return nil

	case FileCommit:
		// Execute DDL — log result to stderr for user feedback
		err := o.ddl.Commit(ctx, sessionID)
		if err != nil {
			logging.Error("DDL commit failed",
				zap.String("object", parsed.DDLName),
				zap.Error(err))
			return &FSError{
				Code:    ErrIO,
				Message: fmt.Sprintf("DDL commit failed for %s/%s", parsed.DDLOp, parsed.DDLName),
				Hint:    fmt.Sprintf("session=%s error: %v", sessionID, err),
				Cause:   err,
			}
		}
		// Invalidate metadata cache after DDL changes
		o.metaCache.Invalidate()
		logging.Info("DDL committed successfully",
			zap.String("object", parsed.DDLName))
		return nil

	case FileAbort:
		// Cancel staging session — log result to stderr for user feedback
		err := o.ddl.Abort(sessionID)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to abort DDL session",
				Cause:   err,
			}
		}
		logging.Info("DDL session aborted",
			zap.String("object", parsed.DDLName))
		return nil

	default:
		// Store as editor temp file (swap files, backups, etc.)
		o.ddl.SetExtraFile(sessionID, parsed.DDLFile, data)
		return nil
	}
}

// Rename moves a file from oldPath to newPath.
//
// Supported scenarios:
//   - Synth view files: UPDATE the filename column (e.g., mv post-a.md post-b.md)
//   - Native table rows: UPDATE the primary key value (e.g., mv 3.json 99.json)
//
// Both paths must resolve to PathRow in the same schema and table.
// Cross-table renames are not supported.
//
// Parameters:
//   - ctx: context for database operations and cancellation
//   - oldPath: current filesystem path of the file
//   - newPath: desired new filesystem path
//
// Returns nil on success, or an FSError describing the failure.
// Common errors:
//   - ErrNotExist: source file doesn't exist
//   - ErrInvalidPath: paths are not both row files in the same table
//   - ErrIO: database operation failed (type mismatch, duplicate PK, etc.)
func (o *Operations) Rename(ctx context.Context, oldPath, newPath string) *FSError {
	oldParsed, err := o.parsePath(ctx, oldPath)
	if err != nil {
		return err
	}
	newParsed, err := o.parsePath(ctx, newPath)
	if err != nil {
		return err
	}
	o.resolveSynthHierarchy(ctx, oldParsed)
	o.resolveSynthHierarchy(ctx, newParsed)

	// Both must be PathRow
	if oldParsed.Type != PathRow || newParsed.Type != PathRow {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "rename only supported for row files",
		}
	}

	// Both must be in the same schema and table
	oldCtx := oldParsed.Context
	newCtx := newParsed.Context
	if oldCtx == nil || newCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for rename paths",
		}
	}
	if oldCtx.Schema != newCtx.Schema || oldCtx.TableName != newCtx.TableName {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "cross-table rename not supported",
			Hint:    "both paths must be in the same table",
		}
	}

	schema := oldCtx.Schema
	table := oldCtx.TableName

	// Check if this is a synthesized view
	if info := o.getSynthViewInfo(ctx, schema, table); info != nil {
		return o.renameSynthFile(ctx, schema, table, info, oldParsed.PrimaryKey, newParsed.PrimaryKey)
	}

	// Native table: rename = update the primary key value
	if fsErr := o.checkWritePermission(ctx, schema, table); fsErr != nil {
		return fsErr
	}

	pk, dbErr := o.metaCache.GetPrimaryKey(ctx, schema, table)
	if dbErr != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   dbErr,
		}
	}

	pkColumn := pk.Columns[0]

	// Update the PK column from old value to new value
	dbErr = o.db.UpdateColumn(ctx, schema, table, pkColumn, oldParsed.PrimaryKey, pkColumn, newParsed.PrimaryKey)
	if dbErr != nil {
		if strings.Contains(dbErr.Error(), "not found") {
			return &FSError{
				Code:    ErrNotExist,
				Message: "source row not found",
				Cause:   dbErr,
			}
		}
		return &FSError{
			Code:    ErrIO,
			Message: "failed to rename row",
			Cause:   dbErr,
		}
	}

	return nil
}

// Delete removes a file or directory.
//
// Supported path types and behaviors:
//   - /table/pk.json → DELETE row from database
//   - /table/pk/column → SET column to NULL
//
// Parameters:
//   - ctx: context for database operations and cancellation
//   - path: filesystem path to delete
//
// Returns nil on success, or an FSError describing the failure.
// Common errors:
//   - ErrNotExist: row or column doesn't exist
//   - ErrPermission: view is not updatable
//   - ErrIO: database operation failed
func (o *Operations) Delete(ctx context.Context, path string) *FSError {
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return err
	}
	o.resolveSynthHierarchy(ctx, parsed)

	return o.deleteWithParsed(ctx, parsed)
}

// deleteWithParsed implements delete logic for a parsed path.
func (o *Operations) deleteWithParsed(ctx context.Context, parsed *ParsedPath) *FSError {
	switch parsed.Type {
	case PathRow:
		return o.deleteRow(ctx, parsed)
	case PathColumn:
		return o.deleteColumn(ctx, parsed)
	case PathDDL:
		return o.deleteDDLFile(ctx, parsed)
	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot delete path type: %d", parsed.Type),
		}
	}
}

// deleteDDLFile removes an editor temp file from a DDL staging directory.
// Control files (sql, .test, .commit, .abort, test.log) cannot be deleted.
func (o *Operations) deleteDDLFile(ctx context.Context, parsed *ParsedPath) *FSError {
	// Don't allow deleting control files
	switch parsed.DDLFile {
	case FileSQL, FileTest, FileTestLog, FileCommit, FileAbort:
		return &FSError{
			Code:    ErrPermission,
			Message: fmt.Sprintf("cannot delete DDL control file: %s", parsed.DDLFile),
		}
	case "":
		return &FSError{
			Code:    ErrPermission,
			Message: "cannot delete DDL staging directory via rm",
			Hint:    "use touch .abort to cancel the DDL session",
		}
	}

	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return &FSError{Code: ErrInvalidPath, Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp)}
	}

	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if sessionID == "" {
		return &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no DDL session for %s", parsed.DDLName)}
	}

	if !o.ddl.HasExtraFile(sessionID, parsed.DDLFile) {
		return &FSError{Code: ErrNotExist, Message: fmt.Sprintf("file not found: %s", parsed.DDLFile)}
	}

	o.ddl.DeleteExtraFile(sessionID, parsed.DDLFile)
	return nil
}

// deleteRow deletes a row from the database.
// For synthesized views, deletes by filename instead of primary key.
func (o *Operations) deleteRow(ctx context.Context, parsed *ParsedPath) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Check if this is a synthesized view
	if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil {
		return o.deleteSynthFile(ctx, parsed, info)
	}

	// Check write permission
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	pkColumn := pk.Columns[0]

	// Delete row
	err = o.db.DeleteRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey)
	if err != nil {
		// Check if row doesn't exist
		if strings.Contains(err.Error(), "not found") {
			return &FSError{
				Code:    ErrNotExist,
				Message: "row not found",
				Cause:   err,
			}
		}
		return &FSError{
			Code:    ErrIO,
			Message: "failed to delete row",
			Cause:   err,
		}
	}

	return nil
}

// deleteColumn sets a column to NULL.
// The filename may include an extension (e.g., "name.txt" for a TEXT column),
// which is resolved to the actual column name before updating.
func (o *Operations) deleteColumn(ctx context.Context, parsed *ParsedPath) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for column path",
		}
	}

	// Check write permission
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Get columns to resolve the filename
	columns, err := o.db.GetColumns(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get columns",
			Cause:   err,
		}
	}

	// Resolve filename to actual column name (handles extensions)
	actualColumn, found := o.resolveColumn(columns, parsed.Column)
	if !found {
		return &FSError{
			Code:    ErrNotExist,
			Message: fmt.Sprintf("column not found: %s", parsed.Column),
		}
	}

	// Get primary key
	pk, err := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to get primary key",
			Cause:   err,
		}
	}

	pkColumn := pk.Columns[0]

	// Set column to NULL using the resolved column name
	err = o.db.UpdateColumn(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, actualColumn, "")
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to set column to NULL",
			Cause:   err,
		}
	}

	return nil
}

// Create creates a new file and returns a WriteHandle for writing content.
//
// Supported path types:
//   - /table/pk.json → Create handle for new row (written on close)
//
// Parameters:
//   - ctx: context for database operations and cancellation
//   - path: filesystem path for the new file
//
// Returns a WriteHandle that buffers writes and executes the database
// operation when closed, or an FSError if the path type doesn't support
// creation.
func (o *Operations) Create(ctx context.Context, path string) (*WriteHandle, *FSError) {
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return nil, err
	}

	switch parsed.Type {
	case PathRow:
		// Create write handle for new row
		return &WriteHandle{
			Path: path,
			OnClose: func(data []byte) error {
				fsErr := o.writeFileWithParsed(ctx, parsed, data)
				if fsErr != nil {
					return fsErr.Cause
				}
				return nil
			},
		}, nil
	default:
		return nil, &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot create path type: %d", parsed.Type),
		}
	}
}

// Mkdir creates a directory for incremental row creation.
//
// Creating a directory with a primary key value starts the incremental
// row creation workflow:
//  1. mkdir /table/pk → creates staging entry
//  2. echo "value" > /table/pk/column → sets column value
//  3. When all NOT NULL columns are provided, row is auto-committed
//
// Parameters:
//   - ctx: context for database operations and cancellation
//   - path: filesystem path for the new directory
//
// Returns nil on success, or an FSError describing the failure.
// Common errors:
//   - ErrExists: row already exists in the database
//   - ErrPermission: view is not updatable
//   - ErrIO: database operation failed
func (o *Operations) Mkdir(ctx context.Context, path string) *FSError {
	parsed, err := o.parsePath(ctx, path)
	if err != nil {
		return err
	}
	o.resolveSynthHierarchy(ctx, parsed)

	switch parsed.Type {
	case PathRow:
		// Create staging entry for new row
		fsCtx := parsed.Context
		if fsCtx == nil {
			return &FSError{
				Code:    ErrInvalidPath,
				Message: "missing context for row path",
			}
		}

		// Synth views: hierarchy-enabled views create directory rows,
		// non-hierarchy views reject mkdir (directories not supported without filetype column)
		if info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName); info != nil {
			if info.SupportsHierarchy {
				return o.mkdirSynth(ctx, parsed, info)
			}
			return &FSError{
				Code:    ErrPermission,
				Message: "mkdir not supported: view lacks filetype column — recreate with .build/ to enable directories",
			}
		}

		// Check write permission
		if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
			return fsErr
		}

		// Get primary key
		pk, dbErr := o.metaCache.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
		if dbErr != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to get primary key",
				Cause:   dbErr,
			}
		}

		pkColumn := pk.Columns[0]

		// Check if row already exists
		_, dbErr = o.db.GetRow(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey)
		if dbErr == nil {
			return &FSError{
				Code:    ErrExists,
				Message: "row already exists",
			}
		}

		// Create staging entry
		if o.staging == nil {
			o.staging = NewStagingManager()
		}
		o.staging.GetOrCreate(fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey)
		return nil

	case PathDDL:
		return o.mkdirDDL(ctx, parsed)

	case PathHistory:
		return &FSError{
			Code:    ErrPermission,
			Message: ".history/ is read-only",
			Hint:    "history files are archived versions and cannot be modified",
		}

	case PathTable, PathRoot, PathSchema, PathSchemaList, PathCapability,
		PathInfo, PathExport, PathImport, PathViewList, PathBuild, PathFormat:
		return &FSError{
			Code:    ErrAlreadyExists,
			Message: "directory already exists",
		}

	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot mkdir for path type: %d", parsed.Type),
		}
	}
}

// checkWritePermission checks if writes are allowed for the table/view.
func (o *Operations) checkWritePermission(ctx context.Context, schema, table string) *FSError {
	// Check if this is a view (via cache)
	isView, err := o.metaCache.HasView(ctx, table)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to check views",
			Cause:   err,
		}
	}

	if isView {
		// Check if view is updatable
		updatable, err := o.db.IsViewUpdatable(ctx, schema, table)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to check view updatability",
				Cause:   err,
			}
		}
		if !updatable {
			return &FSError{
				Code:    ErrPermission,
				Message: "view is not updatable",
			}
		}
	}

	return nil
}

// parseWriteData parses write data based on format (PATCH semantics - data includes column names).
func (o *Operations) parseWriteData(data []byte, formatType string) ([]string, []interface{}, error) {
	switch formatType {
	case "json":
		return format.ParseJSON(string(data))
	case "csv":
		return format.ParseCSVWithHeader(string(data))
	case "tsv":
		return format.ParseTSVWithHeader(string(data))
	case "yaml":
		return format.ParseYAML(string(data))
	default:
		// Default to JSON for unknown formats
		return format.ParseJSON(string(data))
	}
}

// parseWriteDataNoHeaders parses write data without headers (PUT semantics).
// Values are expected in schema column order, parsed as TSV.
// This matches FUSE behavior for bare paths like /table/123 without format extension.
func (o *Operations) parseWriteDataNoHeaders(ctx context.Context, schema, table string, data []byte) ([]string, []interface{}, error) {
	// Trim trailing newline
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}

	// Get columns from schema
	tableColumns, err := o.db.GetColumns(ctx, schema, table)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get table columns: %w", err)
	}

	// Extract column names in schema order
	columns := make([]string, len(tableColumns))
	for i, col := range tableColumns {
		columns[i] = col.Name
	}

	// Parse as TSV without headers
	_, values, err := format.ParseTSV(string(data))
	if err != nil {
		return nil, nil, err
	}

	return columns, values, nil
}

// mkdirDDL creates a DDL staging session.
//
// Handles DDL paths including:
//   - /.create/<name>/       - create table/view (root level)
//   - /<table>/.modify/      - modify table
//   - /<table>/.delete/      - delete table
//   - /.schemas/.create/<name>/ - create schema
//   - /.schemas/<name>/.delete/ - delete schema
//   - /.views/.create/<name>/   - create view
//   - /<table>/.indexes/.create/<name>/ - create index
//   - /<table>/.indexes/<name>/.delete/ - delete index
//
// The parsed path contains DDLObjectType, DDLParentTable, and Context.Schema
// which determine the parameters passed to CreateSession.
func (o *Operations) mkdirDDL(ctx context.Context, parsed *ParsedPath) *FSError {
	// Validate DDLName is not empty
	if parsed.DDLName == "" {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "DDL object name is required",
			Hint:    "use mkdir /.create/<name> to create a staging session",
		}
	}

	// Convert operation string to DDLOpType
	op, valid := ParseDDLOpType(parsed.DDLOp)
	if !valid {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown DDL operation: %s", parsed.DDLOp),
		}
	}

	// Check if session already exists
	existingID := o.ddl.FindSessionByName(op, parsed.DDLName)
	if existingID != "" {
		if s := o.ddl.GetSession(existingID); s != nil && !s.Completed {
			return &FSError{
				Code:    ErrExists,
				Message: fmt.Sprintf("DDL session already exists for %s", parsed.DDLName),
				Hint:    "use the existing session or abort it first with touch /.create/<name>/.abort",
			}
		}
		// Completed session — remove it to make way for new one
		o.ddl.RemoveSession(existingID)
	}

	// Determine object type - default to "table" if not specified
	// TODO: For view operations (delete/modify), the path parser can't distinguish
	// views from tables without a DB call. This is handled at commit time when the
	// actual DDL is executed. If we want to validate earlier, we'd need to query
	// pg_class to check relkind, but that adds latency to every mkdir.
	objectType := parsed.DDLObjectType
	if objectType == "" {
		objectType = "table"
	}

	// Determine schema from context, default to "public"
	// Schema DDL operations don't use a schema parameter
	schema := "public"
	if objectType == "schema" {
		schema = ""
	} else if parsed.Context != nil && parsed.Context.Schema != "" {
		schema = parsed.Context.Schema
	}

	// Create new session with parsed fields
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
