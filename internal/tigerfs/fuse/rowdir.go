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

// RowDirectoryNode represents a single row as a directory.
// Listing the directory shows column names as files.
// Each column file's permissions are determined by the user's PostgreSQL privileges.
type RowDirectoryNode struct {
	fs.Inode

	cfg         *config.Config     // TigerFS configuration
	db          db.DBClient        // Database client for queries
	cache       *MetadataCache     // Metadata cache for permissions lookup
	schema      string             // PostgreSQL schema name
	tableName   string             // Table name
	pkColumn    string             // Primary key column name
	pkValue     string             // Primary key value identifying this row
	partialRows *PartialRowTracker // Tracker for uncommitted partial rows
}

var _ fs.InodeEmbedder = (*RowDirectoryNode)(nil)
var _ fs.NodeGetattrer = (*RowDirectoryNode)(nil)
var _ fs.NodeReaddirer = (*RowDirectoryNode)(nil)
var _ fs.NodeLookuper = (*RowDirectoryNode)(nil)
var _ fs.NodeUnlinker = (*RowDirectoryNode)(nil)

// NewRowDirectoryNode creates a new row directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: PostgreSQL schema name
//   - tableName: Table name
//   - pkColumn: Primary key column name
//   - pkValue: Primary key value identifying this row
//   - partialRows: Tracker for uncommitted partial rows
func NewRowDirectoryNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, pkColumn, pkValue string, partialRows *PartialRowTracker) *RowDirectoryNode {
	return &RowDirectoryNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
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

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Format files available in row directories for reading full row in different formats
var rowFormatFiles = []string{".json", ".csv", ".tsv"}

// Readdir lists the column names in the row plus format files
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
	entries := make([]fuse.DirEntry, 0, len(columns)+len(rowFormatFiles))
	for _, column := range columns {
		// Add extension based on column type unless disabled
		filename := column.Name
		if !r.cfg.NoFilenameExtensions {
			filename = AddExtensionToColumn(column.Name, column.DataType)
		}
		entries = append(entries, fuse.DirEntry{
			Name: filename,
			Mode: syscall.S_IFREG, // Columns are files
		})
	}

	// Add format files (.json, .csv, .tsv) for reading full row
	for _, formatFile := range rowFormatFiles {
		entries = append(entries, fuse.DirEntry{
			Name: formatFile,
			Mode: syscall.S_IFREG,
		})
	}

	logging.Debug("Row directory listing",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.Int("column_count", len(columns)),
		zap.Int("total_entries", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a column file or format file by name
func (r *RowDirectoryNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("RowDirectoryNode.Lookup called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("name", name))

	// Check if this is a format file (.json, .csv, .tsv)
	switch name {
	case ".json":
		return r.createFormatFileNode(ctx, "json", out)
	case ".csv":
		return r.createFormatFileNode(ctx, "csv", out)
	case ".tsv":
		return r.createFormatFileNode(ctx, "tsv", out)
	}

	// Otherwise, look up as a column name
	columns, err := r.db.GetColumns(ctx, r.schema, r.tableName)
	if err != nil {
		logging.Error("Failed to get columns",
			zap.String("table", r.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Find the column - supports both exact match and extension-based match
	var columnName string
	if r.cfg.NoFilenameExtensions {
		// Extensions disabled - exact match only
		for _, col := range columns {
			if col.Name == name {
				columnName = col.Name
				break
			}
		}
	} else {
		// Extensions enabled - use smart matching
		col, found := FindColumnByFilename(columns, name)
		if found {
			columnName = col.Name
		}
	}

	if columnName == "" {
		logging.Debug("Column not found",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("filename", name))
		return nil, syscall.ENOENT
	}

	// Column exists - create column file node
	logging.Debug("Column found",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("filename", name),
		zap.String("column", columnName))

	// Create stable inode for the column file
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	// Create column file node with cache for permissions and partial row tracker
	// Use the actual column name (without extension) for database operations
	columnNode := NewColumnFileNode(r.cfg, r.db, r.cache, r.schema, r.tableName, r.pkColumn, r.pkValue, columnName, r.partialRows)

	child := r.NewPersistentInode(ctx, columnNode, stableAttr)

	// Fill in entry attributes (size, permissions, etc.) so they're cached correctly
	var attrOut fuse.AttrOut
	if errno := columnNode.Getattr(ctx, nil, &attrOut); errno != 0 {
		return nil, errno
	}
	out.Attr = attrOut.Attr

	return child, 0
}

// createFormatFileNode creates a RowFileNode for reading the full row in a specific format
func (r *RowDirectoryNode) createFormatFileNode(ctx context.Context, format string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("Creating format file node",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("format", format))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	rowNode := NewRowFileNode(r.cfg, r.db, r.cache, r.schema, r.tableName, r.pkColumn, r.pkValue, format)
	child := r.NewPersistentInode(ctx, rowNode, stableAttr)

	// Fill in entry attributes (size, permissions, etc.) so they're cached correctly
	var attrOut fuse.AttrOut
	if errno := rowNode.Getattr(ctx, nil, &attrOut); errno != 0 {
		return nil, errno
	}
	out.Attr = attrOut.Attr

	return child, 0
}

// Unlink deletes a column file (rm /users/123/email.txt) by setting column to NULL
func (r *RowDirectoryNode) Unlink(ctx context.Context, name string) syscall.Errno {
	logging.Debug("RowDirectoryNode.Unlink called",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("filename", name))

	// Get column information to validate it's nullable
	columns, err := r.db.GetColumns(ctx, r.schema, r.tableName)
	if err != nil {
		logging.Error("Failed to get columns",
			zap.String("table", r.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	// Find the column being deleted - supports both exact match and extension-based match
	var targetColumn *db.Column
	if r.cfg.NoFilenameExtensions {
		// Extensions disabled - exact match only
		for i := range columns {
			if columns[i].Name == name {
				targetColumn = &columns[i]
				break
			}
		}
	} else {
		// Extensions enabled - use smart matching
		targetColumn, _ = FindColumnByFilename(columns, name)
	}

	if targetColumn == nil {
		logging.Debug("Column not found",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("filename", name))
		return syscall.ENOENT
	}

	// Check if column is nullable
	if !targetColumn.IsNullable && targetColumn.Default == "" {
		// Column is NOT NULL without default - cannot set to NULL
		logging.Debug("Cannot delete NOT NULL column without default",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("column", targetColumn.Name))
		return syscall.EACCES
	}

	// Set column to NULL via UPDATE (empty string is converted to NULL)
	err = r.db.UpdateColumn(ctx, r.schema, r.tableName, r.pkColumn, r.pkValue, targetColumn.Name, "")
	if err != nil {
		logging.Error("Failed to set column to NULL",
			zap.String("table", r.tableName),
			zap.String("pk", r.pkValue),
			zap.String("column", targetColumn.Name),
			zap.Error(err))
		return syscall.EIO
	}

	logging.Debug("Column set to NULL successfully",
		zap.String("table", r.tableName),
		zap.String("pk", r.pkValue),
		zap.String("column", targetColumn.Name))

	return 0
}
