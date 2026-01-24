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

	cfg        *config.Config
	db         *db.Client
	schema     string
	tableName  string
	pkColumn   string
	pkValue    string
	columnName string

	// Cached column data
	data []byte
}

var _ fs.InodeEmbedder = (*ColumnFileNode)(nil)
var _ fs.NodeOpener = (*ColumnFileNode)(nil)
var _ fs.NodeGetattrer = (*ColumnFileNode)(nil)

// NewColumnFileNode creates a new column file node
func NewColumnFileNode(cfg *config.Config, dbClient *db.Client, schema, tableName, pkColumn, pkValue, columnName string) *ColumnFileNode {
	return &ColumnFileNode{
		cfg:        cfg,
		db:         dbClient,
		schema:     schema,
		tableName:  tableName,
		pkColumn:   pkColumn,
		pkValue:    pkValue,
		columnName: columnName,
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

	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(c.data))

	return 0
}

// Open opens the column file for reading
func (c *ColumnFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("ColumnFileNode.Open called",
		zap.String("table", c.tableName),
		zap.String("pk", c.pkValue),
		zap.String("column", c.columnName))

	// Fetch column data if not already cached
	if c.data == nil {
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
		data: c.data,
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// fetchData retrieves the column value from the database and converts to text
func (c *ColumnFileNode) fetchData(ctx context.Context) error {
	// Query column value from database
	value, err := c.db.GetColumn(ctx, c.schema, c.tableName, c.pkColumn, c.pkValue, c.columnName)
	if err != nil {
		return err
	}

	// Convert value to string representation
	// NULL values become empty string (0 bytes)
	str := format.ValueToString(value)

	c.data = []byte(str)
	return nil
}

// ColumnFileHandle represents an open file handle for reading column data
type ColumnFileHandle struct {
	data []byte
}

var _ fs.FileReader = (*ColumnFileHandle)(nil)

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
