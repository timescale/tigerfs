package fs

import (
	"context"
	"fmt"
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/format"
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
	parsed, err := ParsePath(path)
	if err != nil {
		return err
	}

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
	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot write to path type: %d", parsed.Type),
		}
	}
}

// writeRowFile writes a row file (UPDATE or INSERT).
func (o *Operations) writeRowFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Check if this is a view and if it's updatable
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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

	// Ensure session exists (auto-create for table-level DDL)
	if err := o.ensureDDLSession(parsed, op); err != nil {
		return err
	}

	// Find session by name
	sessionID := o.ddl.FindSessionByName(op, parsed.DDLName)

	// Handle the specific file
	switch parsed.DDLFile {
	case FileSQL:
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
		// Trigger validation
		_, err := o.ddl.Test(ctx, sessionID)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to test DDL",
				Cause:   err,
			}
		}
		return nil

	case FileCommit:
		// Execute DDL
		err := o.ddl.Commit(ctx, sessionID)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "DDL commit failed",
				Cause:   err,
			}
		}
		return nil

	case FileAbort:
		// Cancel staging session
		err := o.ddl.Abort(sessionID)
		if err != nil {
			return &FSError{
				Code:    ErrIO,
				Message: "failed to abort DDL session",
				Cause:   err,
			}
		}
		return nil

	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unknown DDL file: %s", parsed.DDLFile),
		}
	}
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
	parsed, err := ParsePath(path)
	if err != nil {
		return err
	}

	return o.deleteWithParsed(ctx, parsed)
}

// deleteWithParsed implements delete logic for a parsed path.
func (o *Operations) deleteWithParsed(ctx context.Context, parsed *ParsedPath) *FSError {
	switch parsed.Type {
	case PathRow:
		return o.deleteRow(ctx, parsed)
	case PathColumn:
		return o.deleteColumn(ctx, parsed)
	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot delete path type: %d", parsed.Type),
		}
	}
}

// deleteRow deletes a row from the database.
func (o *Operations) deleteRow(ctx context.Context, parsed *ParsedPath) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing context for row path",
		}
	}

	// Check write permission
	if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
		return fsErr
	}

	// Get primary key
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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
	pk, err := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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
	parsed, err := ParsePath(path)
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
	parsed, err := ParsePath(path)
	if err != nil {
		return err
	}

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

		// Check write permission
		if fsErr := o.checkWritePermission(ctx, fsCtx.Schema, fsCtx.TableName); fsErr != nil {
			return fsErr
		}

		// Get primary key
		pk, dbErr := o.db.GetPrimaryKey(ctx, fsCtx.Schema, fsCtx.TableName)
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

	default:
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("cannot mkdir for path type: %d", parsed.Type),
		}
	}
}

// checkWritePermission checks if writes are allowed for the table/view.
func (o *Operations) checkWritePermission(ctx context.Context, schema, table string) *FSError {
	// Check if this is a view
	views, err := o.db.GetViews(ctx, schema)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to check views",
			Cause:   err,
		}
	}

	isView := false
	for _, v := range views {
		if v == table {
			isView = true
			break
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
		return &FSError{
			Code:    ErrExists,
			Message: fmt.Sprintf("DDL session already exists for %s", parsed.DDLName),
			Hint:    "use the existing session or abort it first with touch /.create/<name>/.abort",
		}
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
