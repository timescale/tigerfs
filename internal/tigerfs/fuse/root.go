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

// RootNode represents the root directory of the TigerFS filesystem
// Lists tables from the default schema (schema flattening)
type RootNode struct {
	fs.Inode

	cfg         *config.Config
	db          *db.Client
	cache       *MetadataCache
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*RootNode)(nil)
var _ fs.NodeReaddirer = (*RootNode)(nil)
var _ fs.NodeLookuper = (*RootNode)(nil)
var _ fs.NodeGetattrer = (*RootNode)(nil)

// NewRootNode creates a new root directory node
func NewRootNode(cfg *config.Config, dbClient *db.Client, partialRows *PartialRowTracker) *RootNode {
	cache := NewMetadataCache(cfg, dbClient)

	return &RootNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the root directory
func (r *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("RootNode.Getattr called")

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the contents of the root directory
// Returns tables from the default schema (schema flattening)
func (r *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("RootNode.Readdir called")

	// Get tables from cache
	tables, err := r.cache.GetTables(ctx)
	if err != nil {
		logging.Error("Failed to get tables", zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert tables to directory entries
	entries := make([]fuse.DirEntry, 0, len(tables))
	for _, table := range tables {
		entries = append(entries, fuse.DirEntry{
			Name: table,
			Mode: syscall.S_IFDIR, // Tables are directories
		})
	}

	logging.Debug("Root directory listing",
		zap.Int("table_count", len(entries)),
		zap.Strings("tables", tables))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a table name in the root directory
func (r *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("RootNode.Lookup called", zap.String("table", name))

	// Check if table exists in cache
	exists, err := r.cache.HasTable(ctx, name)
	if err != nil {
		logging.Error("Failed to check table existence", zap.String("table", name), zap.Error(err))
		return nil, syscall.EIO
	}

	if !exists {
		logging.Debug("Table not found", zap.String("table", name))
		return nil, syscall.ENOENT
	}

	// Table exists - create table directory node
	logging.Debug("Table found", zap.String("table", name))

	// Create a stable inode for the table directory
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Create table node with database client, cache, and partial row tracker
	tableNode := NewTableNode(r.cfg, r.db, r.cache, r.cfg.DefaultSchema, name, r.partialRows)

	child := r.NewPersistentInode(ctx, tableNode, stableAttr)
	return child, 0
}
