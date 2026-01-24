package fuse

import (
	"context"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// TableNode represents a table directory in the filesystem
// Lists rows by their primary key values
type TableNode struct {
	fs.Inode

	cfg       *config.Config
	db        *db.Client
	tableName string
	schema    string
}

var _ fs.InodeEmbedder = (*TableNode)(nil)
var _ fs.NodeGetattrer = (*TableNode)(nil)
var _ fs.NodeReaddirer = (*TableNode)(nil)
var _ fs.NodeLookuper = (*TableNode)(nil)
var _ fs.NodeUnlinker = (*TableNode)(nil)
var _ fs.NodeRmdirer = (*TableNode)(nil)

// NewTableNode creates a new table directory node
func NewTableNode(cfg *config.Config, dbClient *db.Client, schema, tableName string) *TableNode {
	return &TableNode{
		cfg:       cfg,
		db:        dbClient,
		tableName: tableName,
		schema:    schema,
	}
}

// Getattr returns attributes for the table directory
func (t *TableNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("TableNode.Getattr called", zap.String("table", t.tableName))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the contents of the table directory (row primary keys)
func (t *TableNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("TableNode.Readdir called", zap.String("table", t.tableName))

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Get first column of primary key (only single-column PKs supported in MVP)
	pkColumn := pk.Columns[0]

	// List rows (limited by max_ls_rows)
	rows, err := t.db.ListRows(ctx, t.schema, t.tableName, pkColumn, t.cfg.MaxLsRows)
	if err != nil {
		logging.Error("Failed to list rows",
			zap.String("table", t.tableName),
			zap.String("pk_column", pkColumn),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert rows to directory entries
	entries := make([]fuse.DirEntry, 0, len(rows)+3) // +3 for metadata files

	// Add metadata files first
	entries = append(entries,
		fuse.DirEntry{
			Name: ".columns",
			Mode: syscall.S_IFREG,
		},
		fuse.DirEntry{
			Name: ".schema",
			Mode: syscall.S_IFREG,
		},
		fuse.DirEntry{
			Name: ".count",
			Mode: syscall.S_IFREG,
		},
	)

	// Add rows
	for _, rowPK := range rows {
		entries = append(entries, fuse.DirEntry{
			Name: rowPK,
			Mode: syscall.S_IFREG, // Rows are files (row-as-file representation)
		})
	}

	logging.Debug("Table directory listing",
		zap.String("table", t.tableName),
		zap.Int("row_count", len(rows)),
		zap.Int("total_entries", len(entries)),
		zap.Int("max_ls_rows", t.cfg.MaxLsRows))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row by primary key value or metadata file
func (t *TableNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("TableNode.Lookup called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Check if this is a metadata file lookup
	if name == ".columns" || name == ".schema" || name == ".count" {
		return t.lookupMetadataFile(ctx, name)
	}

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	logging.Debug("Parsed row filename",
		zap.String("table", t.tableName),
		zap.String("filename", name),
		zap.String("pk_value", pkValue),
		zap.String("format", format))

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row exists by trying to fetch it (use PK value without extension)
	// This validates the row exists before creating the inode
	_, err = t.db.GetRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	// Row exists - decide whether to create directory or file node
	logging.Debug("Row found",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue),
		zap.String("format", format))

	// If name has explicit format extension, create row file node
	// Otherwise create row directory node
	if name != pkValue {
		// Name has extension (e.g., "1.csv"), create row file node
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}

		rowNode := NewRowFileNode(t.cfg, t.db, t.schema, t.tableName, pkColumn, pkValue, format)
		child := t.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(t.cfg, t.db, t.schema, t.tableName, pkColumn, pkValue)
	child := t.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// lookupMetadataFile handles lookups for metadata files (.columns, .schema, .count)
func (t *TableNode) lookupMetadataFile(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up metadata file",
		zap.String("table", t.tableName),
		zap.String("file", name))

	// Determine metadata file type
	var fileType string
	switch name {
	case ".columns":
		fileType = "columns"
	case ".schema":
		fileType = "schema"
	case ".count":
		fileType = "count"
	default:
		logging.Debug("Unknown metadata file",
			zap.String("table", t.tableName),
			zap.String("file", name))
		return nil, syscall.ENOENT
	}

	// Create stable inode for metadata file
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	// Create metadata file node
	metadataNode := NewMetadataFileNode(t.cfg, t.db, t.schema, t.tableName, fileType)
	child := t.NewPersistentInode(ctx, metadataNode, stableAttr)

	logging.Debug("Created metadata file node",
		zap.String("table", t.tableName),
		zap.String("file", name),
		zap.String("type", fileType))

	return child, 0
}

// Unlink deletes a row file (rm /users/123.csv)
func (t *TableNode) Unlink(ctx context.Context, name string) syscall.Errno {
	logging.Debug("TableNode.Unlink called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Can't delete metadata files
	if name == ".columns" || name == ".schema" || name == ".count" {
		logging.Debug("Cannot delete metadata file",
			zap.String("table", t.tableName),
			zap.String("file", name))
		return syscall.EACCES
	}

	// Parse filename to get PK value (strip format extension)
	pkValue, _ := util.ParseRowFilename(name)

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Delete row from database
	err = t.db.DeleteRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Error("Failed to delete row",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		// Map constraint violations to EACCES
		if strings.Contains(err.Error(), "foreign key") ||
			strings.Contains(err.Error(), "constraint") {
			return syscall.EACCES
		}
		return syscall.EIO
	}

	logging.Debug("Row deleted successfully",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	return 0
}

// Rmdir deletes a row directory (rm -r /users/123/)
func (t *TableNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	logging.Debug("TableNode.Rmdir called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Can't rmdir row files (must use unlink)
	// Check if name has format extension
	pkValue, _ := util.ParseRowFilename(name)
	if name != pkValue {
		// Has extension - this is a file, not a directory
		logging.Debug("Cannot rmdir a file",
			zap.String("table", t.tableName),
			zap.String("name", name))
		return syscall.ENOTDIR
	}

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Delete row from database
	err = t.db.DeleteRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Error("Failed to delete row",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		// Map constraint violations to EACCES
		if strings.Contains(err.Error(), "foreign key") ||
			strings.Contains(err.Error(), "constraint") {
			return syscall.EACCES
		}
		return syscall.EIO
	}

	logging.Debug("Row directory deleted successfully",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	return 0
}
