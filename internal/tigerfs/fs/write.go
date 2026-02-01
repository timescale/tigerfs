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
	columns, values, parseErr := o.parseWriteData(data, parsed.Format)
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
			o.staging.SetColumn(fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, parsed.Column, value)
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

	// Row exists - update column
	err = o.db.UpdateColumn(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, parsed.Column, value)
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

	// Parse data as CSV (default format for imports)
	columns, rows, err := format.ParseCSVBulk(data)
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

// writeDDLFile handles DDL staging file writes.
func (o *Operations) writeDDLFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	// DDL writes will be implemented in Task 9.5
	return &FSError{
		Code:    ErrNotImplemented,
		Message: "DDL writes not yet implemented",
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

	// Set column to NULL (represented as empty string in UpdateColumn)
	err = o.db.UpdateColumn(ctx, fsCtx.Schema, fsCtx.TableName, pkColumn, parsed.PrimaryKey, parsed.Column, "")
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
		// DDL mkdir will be implemented in Task 9.5
		return &FSError{
			Code:    ErrNotImplemented,
			Message: "DDL mkdir not yet implemented",
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

// parseWriteData parses write data based on format.
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
		// Default to JSON
		return format.ParseJSON(string(data))
	}
}
