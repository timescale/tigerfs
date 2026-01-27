package fuse

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// RowFileNode represents a single row as a file.
// Reading the file returns the entire row in the specified format (TSV, CSV, or JSON).
// File permissions are determined by the user's PostgreSQL privileges on the table.
type RowFileNode struct {
	fs.Inode

	cfg       *config.Config // TigerFS configuration
	db        *db.Client     // Database client for queries
	cache     *MetadataCache // Metadata cache for permissions lookup
	schema    string         // PostgreSQL schema name
	tableName string         // Table name
	pkColumn  string         // Primary key column name
	pkValue   string         // Primary key value identifying this row
	format    string         // Output format: "tsv", "csv", or "json"

	// Cached row data - populated on first read, invalidated on write
	data []byte
}

var _ fs.InodeEmbedder = (*RowFileNode)(nil)
var _ fs.NodeOpener = (*RowFileNode)(nil)
var _ fs.NodeGetattrer = (*RowFileNode)(nil)
var _ fs.NodeSetattrer = (*RowFileNode)(nil)

// NewRowFileNode creates a new row file node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: PostgreSQL schema name
//   - tableName: Table name
//   - pkColumn: Primary key column name
//   - pkValue: Primary key value identifying this row
//   - format: Output format ("tsv", "csv", or "json")
func NewRowFileNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName, pkColumn, pkValue, format string) *RowFileNode {
	return &RowFileNode{
		cfg:       cfg,
		db:        dbClient,
		cache:     cache,
		schema:    schema,
		tableName: tableName,
		pkColumn:  pkColumn,
		pkValue:   pkValue,
		format:    format,
	}
}

// Getattr returns attributes for the row file.
// File permissions are mapped from PostgreSQL privileges:
//   - SELECT → read (0400)
//   - UPDATE/INSERT → write (0200)
//
// Falls back to 0644 if cache is unavailable or permissions can't be fetched.
func (r *RowFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("RowFileNode.Getattr called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue))

	// Fetch row data to get size
	if r.data == nil {
		if err := r.fetchData(ctx); err != nil {
			logging.Error("Failed to fetch row data for getattr",
				zap.String("table", r.tableName),
				zap.String("pk", r.pkValue),
				zap.Error(err))
			return syscall.EIO
		}
	}

	// Determine file mode based on table permissions
	mode := r.getFileMode(ctx)

	out.Mode = uint32(mode) | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(r.data))

	return 0
}

// getFileMode returns the file permission bits based on table permissions.
// Falls back to 0600 (owner read-write) if permissions can't be determined.
func (r *RowFileNode) getFileMode(ctx context.Context) uint32 {
	// Default to owner read-write if no cache available
	if r.cache == nil {
		return 0600
	}

	perms, err := r.cache.GetTablePermissions(ctx, r.tableName)
	if err != nil {
		logging.Warn("Failed to get table permissions, using default mode",
			zap.String("table", r.tableName),
			zap.Error(err))
		return 0600
	}

	if perms == nil {
		// No cached permissions, use default
		return 0600
	}

	// Map PostgreSQL privileges to Unix file permissions
	mode := util.MapPermissions(perms.CanSelect, perms.CanUpdate, perms.CanInsert, perms.CanDelete)

	logging.Debug("Mapped table permissions to file mode",
		zap.String("table", r.tableName),
		zap.Bool("select", perms.CanSelect),
		zap.Bool("update", perms.CanUpdate),
		zap.Bool("insert", perms.CanInsert),
		zap.Bool("delete", perms.CanDelete),
		zap.Uint32("mode", uint32(mode)))

	return uint32(mode)
}

// Setattr handles attribute changes (used for truncation during writes)
func (r *RowFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("RowFileNode.Setattr called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue))

	// Handle truncation (e.g., > /path/to/file)
	if sz, ok := in.GetSize(); ok {
		if sz == 0 {
			// Truncate to zero - clear cached data
			r.data = nil
			logging.Debug("Row file truncated",
				zap.String("table", r.tableName),
				zap.String("pk", r.pkValue))
		}
	}

	// Return current attributes
	return r.Getattr(ctx, fh, out)
}

// Open opens the row file for reading or writing
func (r *RowFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("RowFileNode.Open called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.Uint32("flags", flags))

	// Check if opening for write
	accessMode := flags & syscall.O_ACCMODE
	isWrite := accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR

	// Check if row exists (for INSERT vs UPDATE detection)
	rowExists := false
	if !isWrite || r.data != nil {
		// Try to fetch row data
		if r.data == nil {
			err := r.fetchData(ctx)
			if err == nil {
				rowExists = true
			} else {
				// Row doesn't exist - will need INSERT on write
				logging.Debug("Row does not exist",
					zap.String("table", r.tableName),
					zap.String("pk", r.pkValue))
			}
		} else {
			rowExists = true
		}
	}

	// Create file handle
	fh := &RowFileHandle{
		node:      r,
		data:      r.data,
		rowExists: rowExists,
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// fetchData retrieves the row data from the database and converts to the specified format
func (r *RowFileNode) fetchData(ctx context.Context) error {
	// Query row from database
	row, err := r.db.GetRow(ctx, r.schema, r.tableName, r.pkColumn, r.pkValue)
	if err != nil {
		return err
	}

	// Convert to requested format
	var data []byte
	switch r.format {
	case "csv":
		data, err = format.RowToCSV(row.Columns, row.Values)
	case "json":
		data, err = format.RowToJSON(row.Columns, row.Values)
	case "tsv":
		fallthrough
	default:
		data, err = format.RowToTSV(row.Columns, row.Values)
	}

	if err != nil {
		return err
	}

	r.data = data
	return nil
}

// RowFileHandle represents an open file handle for reading/writing row data
type RowFileHandle struct {
	node      *RowFileNode
	data      []byte
	rowExists bool
}

var _ fs.FileReader = (*RowFileHandle)(nil)
var _ fs.FileWriter = (*RowFileHandle)(nil)
var _ fs.FileFlusher = (*RowFileHandle)(nil)

// Read reads row data from the file
func (fh *RowFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	logging.Debug("RowFileHandle.Read called", zap.Int64("offset", off), zap.Int("size", len(dest)))

	// Calculate read bounds
	end := off + int64(len(dest))
	if end > int64(len(fh.data)) {
		end = int64(len(fh.data))
	}

	// Handle EOF
	if off >= int64(len(fh.data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	// Return data slice
	return fuse.ReadResultData(fh.data[off:end]), 0
}

// Write writes data to the row file
func (fh *RowFileHandle) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	logging.Debug("RowFileHandle.Write called",
		zap.Int64("offset", off),
		zap.Int("size", len(data)))

	// For simplicity, only support writes starting at offset 0
	if off == 0 {
		fh.data = make([]byte, len(data))
		copy(fh.data, data)
	} else {
		// Extend data buffer if necessary
		newLen := off + int64(len(data))
		if newLen > int64(len(fh.data)) {
			newData := make([]byte, newLen)
			copy(newData, fh.data)
			fh.data = newData
		}
		// Write at offset
		copy(fh.data[off:], data)
	}

	logging.Debug("RowFileHandle.Write buffered",
		zap.Int("bytes_written", len(data)),
		zap.Int("total_size", len(fh.data)))

	return uint32(len(data)), 0
}

// Flush writes buffered data to the database (INSERT or UPDATE)
func (fh *RowFileHandle) Flush(ctx context.Context) syscall.Errno {
	logging.Debug("RowFileHandle.Flush called",
		zap.String("table", fh.node.tableName),
		zap.String("pk", fh.node.pkValue),
		zap.Bool("row_exists", fh.rowExists),
		zap.Int("data_size", len(fh.data)))

	// Parse row data based on format
	columns, values, err := fh.parseRowData()
	if err != nil {
		logging.Error("Failed to parse row data",
			zap.String("table", fh.node.tableName),
			zap.String("format", fh.node.format),
			zap.Error(err))
		return syscall.EIO
	}

	if fh.rowExists {
		// UPDATE existing row
		err = fh.node.db.UpdateRow(
			ctx,
			fh.node.schema,
			fh.node.tableName,
			fh.node.pkColumn,
			fh.node.pkValue,
			columns,
			values,
		)
	} else {
		// INSERT new row
		_, err = fh.node.db.InsertRow(
			ctx,
			fh.node.schema,
			fh.node.tableName,
			columns,
			values,
		)
	}

	if err != nil {
		logging.Error("Failed to write row",
			zap.String("table", fh.node.tableName),
			zap.String("pk", fh.node.pkValue),
			zap.Bool("is_insert", !fh.rowExists),
			zap.Error(err))
		return syscall.EIO
	}

	// Update cached data and row existence flag
	fh.node.data = fh.data
	fh.rowExists = true

	logging.Debug("Row written successfully",
		zap.String("table", fh.node.tableName),
		zap.String("pk", fh.node.pkValue),
		zap.Bool("was_insert", !fh.rowExists))

	return 0
}

// parseRowData parses the buffered data based on the file format
// Returns columns and values for INSERT/UPDATE
func (fh *RowFileHandle) parseRowData() ([]string, []interface{}, error) {
	// Trim trailing newline if present
	data := fh.data
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}

	var columns []string
	var values []interface{}
	var err error

	switch fh.node.format {
	case "json":
		// JSON includes column names
		columns, values, err = format.ParseJSON(string(data))
	case "tsv", "csv":
		// TSV/CSV don't include column names, get from schema
		ctx := context.Background()
		var tableColumns []db.Column
		tableColumns, err = fh.node.db.GetColumns(ctx, fh.node.schema, fh.node.tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get table columns: %w", err)
		}

		// Extract column names
		columns = make([]string, len(tableColumns))
		for i, col := range tableColumns {
			columns[i] = col.Name
		}

		// Parse values
		if fh.node.format == "csv" {
			_, values, err = format.ParseCSV(string(data))
		} else {
			_, values, err = format.ParseTSV(string(data))
		}
	default:
		// Default to TSV
		ctx := context.Background()
		var tableColumns []db.Column
		tableColumns, err = fh.node.db.GetColumns(ctx, fh.node.schema, fh.node.tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get table columns: %w", err)
		}

		columns = make([]string, len(tableColumns))
		for i, col := range tableColumns {
			columns[i] = col.Name
		}

		_, values, err = format.ParseTSV(string(data))
	}

	if err != nil {
		return nil, nil, err
	}

	return columns, values, nil
}
