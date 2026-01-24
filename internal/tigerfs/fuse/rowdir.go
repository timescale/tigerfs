package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// RowDirectoryNode represents a single row as a directory
// Listing the directory shows column names as files
type RowDirectoryNode struct {
	fs.Inode

	cfg       *config.Config
	db        *db.Client
	schema    string
	tableName string
	pkColumn  string
	pkValue   string
}

var _ fs.InodeEmbedder = (*RowDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*RowDirectoryNode)(nil)
var _ fs.NodeReaddirer = (*RowDirectoryNode)(nil)
var _ fs.NodeLookuper = (*RowDirectoryNode)(nil)

// NewRowDirectoryNode creates a new row directory node
func NewRowDirectoryNode(cfg *config.Config, dbClient *db.Client, schema, tableName, pkColumn, pkValue string) *RowDirectoryNode {
	return &RowDirectoryNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		pkColumn:  pkColumn,
		pkValue:   pkValue,
	}
}

// Getattr returns attributes for the row directory
func (r *RowDirectoryNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("RowDirectoryNode.Getattr called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the column names in the row
func (r *RowDirectoryNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("RowDirectoryNode.Readdir called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue))

	// Get columns for table
	columns, err := r.db.GetColumns(ctx, r.schema, r.tableName)
	if err != nil {
		logging.Error("Failed to get columns",
			zap.String("table", r.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert columns to directory entries
	entries := make([]fuse.DirEntry, 0, len(columns))
	for _, column := range columns {
		entries = append(entries, fuse.DirEntry{
			Name: column.Name,
			Mode: syscall.S_IFREG, // Columns are files
		})
	}

	logging.Debug("Row directory listing",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.Int("column_count", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a column file by name
func (r *RowDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("RowDirectoryNode.Lookup called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("column", name))

	// Get columns for table to validate column exists
	columns, err := r.db.GetColumns(ctx, r.schema, r.tableName)
	if err != nil {
		logging.Error("Failed to get columns",
			zap.String("table", r.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Check if column exists
	found := false
	for _, col := range columns {
		if col.Name == name {
			found = true
			break
		}
	}

	if !found {
		logging.Debug("Column not found",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("column", name))
		return nil, syscall.ENOENT
	}

	// Column exists - create column file node
	logging.Debug("Column found",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("column", name))

	// Create stable inode for the column file
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	// Create column file node (TODO: implement ColumnFileNode in next task)
	// For now, return ENOTSUP as placeholder
	_ = stableAttr
	return nil, syscall.ENOTSUP
}
