package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// ColumnFileNode represents a single column value as a file
// Reading the file returns just that column's value as text
type ColumnFileNode struct {
	fs.Inode

	cfg         *config.Config
	db          *db.Client
	schema      string
	tableName   string
	pkColumn    string
	pkValue     string
	columnName  string
	partialRows *PartialRowTracker

	// Cached column data
	data []byte
}

var _ fs.InodeEmbedder = (*ColumnFileNode)(nil)
var _ fs.NodeOpener = (*ColumnFileNode)(nil)
var _ fs.NodeGetattrer = (*ColumnFileNode)(nil)
var _ fs.NodeSetattrer = (*ColumnFileNode)(nil)

// NewColumnFileNode creates a new column file node
func NewColumnFileNode(cfg *config.Config, dbClient *db.Client, schema, tableName, pkColumn, pkValue, columnName string, partialRows *PartialRowTracker) *ColumnFileNode {
	return &ColumnFileNode{
		cfg:         cfg,
		db:          dbClient,
		schema:      schema,
		tableName:   tableName,
		pkColumn:    pkColumn,
		pkValue:     pkValue,
		columnName:  columnName,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the column file
func (c *ColumnFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ColumnFileNode.Getattr called",
		zap.String("table", c.tableName),
		zap.String("pk", c.pkValue),
		zap.String("column", c.columnName))

	// Fetch column data to get size
	if c.data == nil {
		if err := c.fetchData(ctx); err != nil {
			logging.Error("Failed to fetch column data for getattr",
				zap.String("table", c.tableName),
				zap.String("pk", c.pkValue),
				zap.String("column", c.columnName),
				zap.Error(err))
			return syscall.EIO
		}
	}

	out.Mode = 0644 | syscall.S_IFREG // Read-write
	out.Nlink = 1
	out.Size = uint64(len(c.data))

	return 0
}

// Setattr handles attribute changes (used for truncation during writes)
func (c *ColumnFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ColumnFileNode.Setattr called",
		zap.String("table", c.tableName),
		zap.String("pk", c.pkValue),
		zap.String("column", c.columnName))

	// Handle truncation (e.g., > /path/to/file or echo "text" > /path/to/file)
	if sz, ok := in.GetSize(); ok {
		if sz == 0 {
			// Truncate to zero - clear cached data
			c.data = nil
			logging.Debug("Column file truncated",
				zap.String("table", c.tableName),
				zap.String("pk", c.pkValue),
				zap.String("column", c.columnName))
		}
	}

	// Return current attributes
	return c.Getattr(ctx, fh, out)
}

// Open opens the column file for reading or writing
func (c *ColumnFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("ColumnFileNode.Open called",
		zap.String("table", c.tableName),
		zap.String("pk", c.pkValue),
		zap.String("column", c.columnName),
		zap.Uint32("flags", flags))

	// Check if opening for write
	accessMode := flags & syscall.O_ACCMODE
	isWrite := accessMode == syscall.O_WRONLY || accessMode == syscall.O_RDWR

	// Fetch column data if not already cached and not truncating
	if c.data == nil && !isWrite {
		if err := c.fetchData(ctx); err != nil {
			logging.Error("Failed to fetch column data",
				zap.String("table", c.tableName),
				zap.String("pk", c.pkValue),
				zap.String("column", c.columnName),
				zap.Error(err))
			return nil, 0, syscall.EIO
		}
	}

	// Create file handle
	fh := &ColumnFileHandle{
		node: c,
		data: c.data,
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// fetchData retrieves the column value from the database and converts to text
func (c *ColumnFileNode) fetchData(ctx context.Context) error {
	// First check if this is an uncommitted partial row
	partialRow := c.partialRows.Get(c.schema, c.tableName, c.pkValue)
	if partialRow != nil && !partialRow.Committed {
		// Row exists in partial state - check if this column has been set
		if value, ok := partialRow.Columns[c.columnName]; ok {
			// Column value exists in partial row
			logging.Debug("Fetching column from partial row",
				zap.String("table", c.tableName),
				zap.String("pk", c.pkValue),
				zap.String("column", c.columnName))

			// Convert value to string
			str, err := format.ConvertValueToText(value)
			if err != nil {
				return err
			}

			if c.cfg.TrailingNewlines {
				c.data = []byte(str + "\n")
			} else {
				c.data = []byte(str)
			}
			return nil
		}

		// Column not yet set in partial row - return empty
		logging.Debug("Column not yet set in partial row",
			zap.String("table", c.tableName),
			zap.String("pk", c.pkValue),
			zap.String("column", c.columnName))
		c.data = []byte{}
		return nil
	}

	// Row is committed or doesn't exist in partial state - query database
	value, err := c.db.GetColumn(ctx, c.schema, c.tableName, c.pkColumn, c.pkValue, c.columnName)
	if err != nil {
		return err
	}

	// Convert value to string representation using enhanced type conversion
	// Handles JSONB, arrays, and all PostgreSQL types
	// NULL values become empty string (0 bytes)
	str, err := format.ConvertValueToText(value)
	if err != nil {
		return err
	}

	if c.cfg.TrailingNewlines {
		c.data = []byte(str + "\n")
	} else {
		c.data = []byte(str)
	}
	return nil
}

// ColumnFileHandle represents an open file handle for reading/writing column data
type ColumnFileHandle struct {
	node *ColumnFileNode
	data []byte
}

var _ fs.FileReader = (*ColumnFileHandle)(nil)
var _ fs.FileWriter = (*ColumnFileHandle)(nil)
var _ fs.FileFlusher = (*ColumnFileHandle)(nil)

// Read reads column data from the file
func (fh *ColumnFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	logging.Debug("ColumnFileHandle.Read called", zap.Int64("offset", off), zap.Int("size", len(dest)))

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

// Write writes data to the column file
func (fh *ColumnFileHandle) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	logging.Debug("ColumnFileHandle.Write called",
		zap.Int64("offset", off),
		zap.Int("size", len(data)))

	// For simplicity, only support writes starting at offset 0
	// This handles the common case: echo "value" > file
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

	logging.Debug("ColumnFileHandle.Write buffered",
		zap.Int("bytes_written", len(data)),
		zap.Int("total_size", len(fh.data)))

	return uint32(len(data)), 0
}

// Flush writes buffered data to the database
func (fh *ColumnFileHandle) Flush(ctx context.Context) syscall.Errno {
	logging.Debug("ColumnFileHandle.Flush called",
		zap.String("table", fh.node.tableName),
		zap.String("pk", fh.node.pkValue),
		zap.String("column", fh.node.columnName),
		zap.Int("data_size", len(fh.data)))

	// Convert data to string, trim trailing newline if present
	value := string(fh.data)
	if len(value) > 0 && value[len(value)-1] == '\n' {
		value = value[:len(value)-1]
	}

	// Check if row exists in database
	_, err := fh.node.db.GetRow(ctx, fh.node.schema, fh.node.tableName, fh.node.pkColumn, fh.node.pkValue)
	rowExists := (err == nil)

	if !rowExists {
		// Row doesn't exist - use incremental row creation
		logging.Debug("Row doesn't exist, using incremental creation",
			zap.String("table", fh.node.tableName),
			zap.String("pk", fh.node.pkValue),
			zap.String("column", fh.node.columnName))

		// Add column to partial row
		fh.node.partialRows.SetColumn(fh.node.schema, fh.node.tableName, fh.node.pkColumn, fh.node.pkValue, fh.node.columnName, value)

		// Try to commit if enough columns provided
		committed, err := fh.node.partialRows.TryCommit(ctx, fh.node.schema, fh.node.tableName, fh.node.pkColumn, fh.node.pkValue)
		if err != nil {
			logging.Error("Failed to commit partial row",
				zap.String("table", fh.node.tableName),
				zap.String("pk", fh.node.pkValue),
				zap.Error(err))
			return syscall.EIO
		}

		if committed {
			logging.Debug("Partial row committed successfully",
				zap.String("table", fh.node.tableName),
				zap.String("pk", fh.node.pkValue))
			// Update cached data in node
			fh.node.data = fh.data
		} else {
			logging.Debug("Partial row not yet ready to commit",
				zap.String("table", fh.node.tableName),
				zap.String("pk", fh.node.pkValue))
			// Update cached data in node even if not committed yet
			fh.node.data = fh.data
		}

		return 0
	}

	// Row exists - use regular UPDATE
	logging.Debug("Row exists, using regular update",
		zap.String("table", fh.node.tableName),
		zap.String("pk", fh.node.pkValue),
		zap.String("column", fh.node.columnName))

	err = fh.node.db.UpdateColumn(
		ctx,
		fh.node.schema,
		fh.node.tableName,
		fh.node.pkColumn,
		fh.node.pkValue,
		fh.node.columnName,
		value,
	)

	if err != nil {
		logging.Error("Failed to update column",
			zap.String("table", fh.node.tableName),
			zap.String("pk", fh.node.pkValue),
			zap.String("column", fh.node.columnName),
			zap.Error(err))
		return syscall.EIO
	}

	// Update cached data in node
	fh.node.data = fh.data

	logging.Debug("Column updated successfully",
		zap.String("table", fh.node.tableName),
		zap.String("pk", fh.node.pkValue),
		zap.String("column", fh.node.columnName))

	return 0
}
