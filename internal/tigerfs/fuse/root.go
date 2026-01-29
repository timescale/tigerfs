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
	staging     *StagingTracker
}

var _ fs.InodeEmbedder = (*RootNode)(nil)
var _ fs.NodeReaddirer = (*RootNode)(nil)
var _ fs.NodeLookuper = (*RootNode)(nil)
var _ fs.NodeGetattrer = (*RootNode)(nil)

// NewRootNode creates a new root directory node
func NewRootNode(cfg *config.Config, dbClient *db.Client, partialRows *PartialRowTracker) *RootNode {
	cache := NewMetadataCache(cfg, dbClient)
	staging := NewStagingTracker()

	return &RootNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		partialRows: partialRows,
		staging:     staging,
	}
}

// Getattr returns attributes for the root directory
func (r *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("RootNode.Getattr called")

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the contents of the root directory.
// Returns tables from the default schema (schema flattening) plus .schemas and .create directories.
func (r *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("RootNode.Readdir called")

	// Get tables from cache
	tables, err := r.cache.GetTables(ctx)
	if err != nil {
		logging.Error("Failed to get tables", zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert tables to directory entries (+ 2 for .schemas and .create)
	entries := make([]fuse.DirEntry, 0, len(tables)+2)

	// Add .create directory for creating new tables in default schema
	entries = append(entries, fuse.DirEntry{
		Name: DirCreate,
		Mode: syscall.S_IFDIR,
	})

	// Add .schemas directory
	entries = append(entries, fuse.DirEntry{
		Name: DirSchemas,
		Mode: syscall.S_IFDIR,
	})

	// Add tables from default schema
	for _, table := range tables {
		entries = append(entries, fuse.DirEntry{
			Name: table,
			Mode: syscall.S_IFDIR, // Tables are directories
		})
	}

	logging.Debug("Root directory listing",
		zap.Int("table_count", len(tables)),
		zap.Strings("tables", tables))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a table name, .schemas, or .create in the root directory.
func (r *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("RootNode.Lookup called", zap.String("name", name))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Handle .create directory for creating new tables in default schema
	if name == DirCreate {
		logging.Debug("Looking up .create directory for tables")

		// Get the resolved default schema from the cache
		defaultSchema := r.cache.GetDefaultSchema()
		if defaultSchema == "" {
			defaultSchema = r.cfg.DefaultSchema
		}

		createNode := NewCreateDirNode(
			r.cfg,
			r.db, // db.DDLExecutor
			r.cache,
			r.staging,
			"table",       // objectType
			defaultSchema, // schema for the new table
			"",            // tableName (not applicable for table creation)
			DirCreate,     // pathPrefix for staging
		)
		child := r.NewPersistentInode(ctx, createNode, stableAttr)
		return child, 0
	}

	// Handle .schemas directory
	if name == DirSchemas {
		logging.Debug("Looking up .schemas directory")

		schemasNode := NewSchemasNode(r.cfg, r.db, r.cache, r.partialRows, r.staging)
		child := r.NewPersistentInode(ctx, schemasNode, stableAttr)
		return child, 0
	}

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

	// Get the resolved default schema from the cache
	defaultSchema := r.cache.GetDefaultSchema()
	if defaultSchema == "" {
		// Fallback to config if cache hasn't resolved it yet
		defaultSchema = r.cfg.DefaultSchema
	}

	// Create table node with database client, cache, partial row tracker, and staging
	tableNode := NewTableNode(r.cfg, r.db, r.cache, defaultSchema, name, r.partialRows, r.staging)

	child := r.NewPersistentInode(ctx, tableNode, stableAttr)
	return child, 0
}
