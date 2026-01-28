package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// AllRowsNode represents the .all/ directory that bypasses dir_listing_limit.
// Users can access this directory to explicitly list all rows in a large table.
// A warning is logged when accessing .all/ on tables exceeding dir_listing_limit.
type AllRowsNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *MetadataCache
	schema      string
	tableName   string
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*AllRowsNode)(nil)
var _ fs.NodeGetattrer = (*AllRowsNode)(nil)
var _ fs.NodeReaddirer = (*AllRowsNode)(nil)
var _ fs.NodeLookuper = (*AllRowsNode)(nil)

// NewAllRowsNode creates a new .all/ directory node
func NewAllRowsNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker) *AllRowsNode {
	return &AllRowsNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the .all directory
func (a *AllRowsNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("AllRowsNode.Getattr called", zap.String("table", a.tableName))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all rows without the dir_listing_limit.
// Logs a warning if the table exceeds dir_listing_limit.
func (a *AllRowsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("AllRowsNode.Readdir called", zap.String("table", a.tableName))

	// Check row count estimate and warn if large
	if a.cache != nil {
		estimate, err := a.cache.GetRowCountEstimate(ctx, a.tableName)
		if err != nil {
			logging.Warn("Failed to get row count estimate",
				zap.String("table", a.tableName),
				zap.Error(err))
		} else if estimate > int64(a.cfg.DirListingLimit) {
			logging.Warn("Listing all rows for large table via .all/ path",
				zap.String("table", a.tableName),
				zap.Int64("estimated_rows", estimate),
				zap.Int("dir_listing_limit", a.cfg.DirListingLimit))
		}
	}

	// Get primary key for table
	pk, err := a.db.GetPrimaryKey(ctx, a.schema, a.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", a.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// List ALL rows (no limit)
	rows, err := a.db.ListAllRows(ctx, a.schema, a.tableName, pkColumn)
	if err != nil {
		logging.Error("Failed to list all rows",
			zap.String("table", a.tableName),
			zap.String("pk_column", pkColumn),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert rows to directory entries
	entries := make([]fuse.DirEntry, 0, len(rows)+3)

	// Add metadata files
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
			Mode: syscall.S_IFREG,
		})
	}

	logging.Debug(".all directory listing",
		zap.String("table", a.tableName),
		zap.Int("row_count", len(rows)),
		zap.Int("total_entries", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row or metadata file within .all/
func (a *AllRowsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("AllRowsNode.Lookup called",
		zap.String("table", a.tableName),
		zap.String("name", name))

	// Check if this is a metadata file lookup
	if name == ".columns" || name == ".schema" || name == ".count" {
		return a.lookupMetadataFile(ctx, name)
	}

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	// Get primary key for table
	pk, err := a.db.GetPrimaryKey(ctx, a.schema, a.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", a.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row exists
	_, err = a.db.GetRow(ctx, a.schema, a.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", a.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	// Row exists - create appropriate node
	if name != pkValue {
		// Name has extension, create row file node
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}

		rowNode := NewRowFileNode(a.cfg, a.db, a.cache, a.schema, a.tableName, pkColumn, pkValue, format)
		child := a.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(a.cfg, a.db, a.cache, a.schema, a.tableName, pkColumn, pkValue, a.partialRows)
	child := a.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// lookupMetadataFile handles lookups for metadata files within .all/
func (a *AllRowsNode) lookupMetadataFile(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	var fileType string
	switch name {
	case ".columns":
		fileType = "columns"
	case ".schema":
		fileType = "schema"
	case ".count":
		fileType = "count"
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	metadataNode := NewMetadataFileNode(a.cfg, a.db, a.schema, a.tableName, fileType)
	child := a.NewPersistentInode(ctx, metadataNode, stableAttr)

	return child, 0
}
