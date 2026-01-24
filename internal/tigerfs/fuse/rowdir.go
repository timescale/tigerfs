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

	cfg         *config.Config
	db          *db.Client
	schema      string
	tableName   string
	pkColumn    string
	pkValue     string
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*RowDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*RowDirectoryNode)(nil)
var _ fs.NodeReaddirer = (*RowDirectoryNode)(nil)
var _ fs.NodeLookuper = (*RowDirectoryNode)(nil)
var _ fs.NodeUnlinker = (*RowDirectoryNode)(nil)

// NewRowDirectoryNode creates a new row directory node
func NewRowDirectoryNode(cfg *config.Config, dbClient *db.Client, schema, tableName, pkColumn, pkValue string, partialRows *PartialRowTracker) *RowDirectoryNode {
	return &RowDirectoryNode{
		cfg:         cfg,
		db:          dbClient,
		schema:      schema,
		tableName:   tableName,
		pkColumn:    pkColumn,
		pkValue:     pkValue,
		partialRows: partialRows,
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

	// Create column file node with partial row tracker
	columnNode := NewColumnFileNode(r.cfg, r.db, r.schema, r.tableName, r.pkColumn, r.pkValue, name, r.partialRows)

	child := r.NewPersistentInode(ctx, columnNode, stableAttr)
	return child, 0
}

// Unlink deletes a column file (rm /users/123/email) by setting column to NULL
func (r *RowDirectoryNode) Unlink(ctx context.Context, name string) syscall.Errno {
	logging.Debug("RowDirectoryNode.Unlink called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("column", name))

	// Get column information to validate it's nullable
	columns, err := r.db.GetColumns(ctx, r.schema, r.tableName)
	if err != nil {
		logging.Error("Failed to get columns",
			zap.String("table", r.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	// Find the column being deleted
	var targetColumn *db.Column
	for i := range columns {
		if columns[i].Name == name {
			targetColumn = &columns[i]
			break
		}
	}

	if targetColumn == nil {
		logging.Debug("Column not found",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("column", name))
		return syscall.ENOENT
	}

	// Check if column is nullable
	if !targetColumn.IsNullable && targetColumn.Default == "" {
		// Column is NOT NULL without default - cannot set to NULL
		logging.Debug("Cannot delete NOT NULL column without default",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("column", name))
		return syscall.EACCES
	}

	// Set column to NULL via UPDATE (empty string is converted to NULL)
	err = r.db.UpdateColumn(ctx, r.schema, r.tableName, r.pkColumn, r.pkValue, name, "")
	if err != nil {
		logging.Error("Failed to set column to NULL",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("column", name),
			zap.Error(err))
		return syscall.EIO
	}

	logging.Debug("Column set to NULL successfully",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("column", name))

	return 0
}
