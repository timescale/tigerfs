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

// RowFileNode represents a single row as a file
// Reading the file returns the entire row in TSV format
type RowFileNode struct {
	fs.Inode

	cfg       *config.Config
	db        *db.Client
	schema    string
	tableName string
	pkColumn  string
	pkValue   string

	// Cached row data
	data []byte
}

var _ fs.InodeEmbedder = (*RowFileNode)(nil)
var _ fs.NodeOpener = (*RowFileNode)(nil)
var _ fs.NodeGetattrer = (*RowFileNode)(nil)

// NewRowFileNode creates a new row file node
func NewRowFileNode(cfg *config.Config, dbClient *db.Client, schema, tableName, pkColumn, pkValue string) *RowFileNode {
	return &RowFileNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		pkColumn:  pkColumn,
		pkValue:   pkValue,
	}
}

// Getattr returns attributes for the row file
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

	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(r.data))

	return 0
}

// Open opens the row file for reading
func (r *RowFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("RowFileNode.Open called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue))

	// Fetch row data if not already cached
	if r.data == nil {
		if err := r.fetchData(ctx); err != nil {
			logging.Error("Failed to fetch row data",
				zap.String("table", r.tableName),
				zap.String("pk", r.pkValue),
				zap.Error(err))
			return nil, 0, syscall.EIO
		}
	}

	// Create file handle
	fh := &RowFileHandle{
		data: r.data,
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// fetchData retrieves the row data from the database and converts to TSV
func (r *RowFileNode) fetchData(ctx context.Context) error {
	// Query row from database
	row, err := r.db.GetRow(ctx, r.schema, r.tableName, r.pkColumn, r.pkValue)
	if err != nil {
		return err
	}

	// Convert to TSV format
	data, err := format.RowToTSV(row.Columns, row.Values)
	if err != nil {
		return err
	}

	r.data = data
	return nil
}

// RowFileHandle represents an open file handle for reading row data
type RowFileHandle struct {
	data []byte
}

var _ fs.FileReader = (*RowFileHandle)(nil)

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
