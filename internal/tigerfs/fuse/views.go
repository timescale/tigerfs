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

// ViewsNode represents the .views/ directory at the filesystem root.
// Lists all views in the default schema and provides navigation to individual view directories.
// Also provides .create/ for creating new views.
type ViewsNode struct {
	fs.Inode

	cfg         *config.Config
	db          *db.Client
	cache       *MetadataCache
	partialRows *PartialRowTracker
	staging     *StagingTracker
}

var _ fs.InodeEmbedder = (*ViewsNode)(nil)
var _ fs.NodeReaddirer = (*ViewsNode)(nil)
var _ fs.NodeLookuper = (*ViewsNode)(nil)
var _ fs.NodeGetattrer = (*ViewsNode)(nil)

// NewViewsNode creates a new .views directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for view info
//   - partialRows: Tracker for partial row creation
//   - staging: Tracker for DDL staging operations
func NewViewsNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, partialRows *PartialRowTracker, staging *StagingTracker) *ViewsNode {
	return &ViewsNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		partialRows: partialRows,
		staging:     staging,
	}
}

// Getattr returns attributes for the .views directory.
func (v *ViewsNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ViewsNode.Getattr called")

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all views plus .create/ for creating new views.
func (v *ViewsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("ViewsNode.Readdir called")

	// Get all views from cache (default schema)
	views, err := v.cache.GetViews(ctx)
	if err != nil {
		logging.Error("Failed to get views", zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert views to directory entries (+ 1 for .create)
	entries := make([]fuse.DirEntry, 0, len(views)+1)

	// Add .create directory first
	entries = append(entries, fuse.DirEntry{
		Name: DirCreate,
		Mode: syscall.S_IFDIR,
	})

	// Add existing views
	for _, view := range views {
		entries = append(entries, fuse.DirEntry{
			Name: view,
			Mode: syscall.S_IFDIR, // Views appear as directories
		})
	}

	logging.Debug(".views directory listing",
		zap.Int("view_count", len(views)),
		zap.Strings("views", views))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a view name or .create in the .views directory.
func (v *ViewsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("ViewsNode.Lookup called", zap.String("name", name))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Handle .create directory for creating new views
	if name == DirCreate {
		logging.Debug("Looking up .create directory for views")

		// Get the resolved default schema from the cache
		defaultSchema := v.cache.GetDefaultSchema()
		if defaultSchema == "" {
			defaultSchema = v.cfg.DefaultSchema
		}

		createNode := NewCreateDirNode(
			v.cfg,
			v.db, // db.DDLExecutor
			v.cache,
			v.staging,
			"view",                 // objectType
			defaultSchema,          // schema for the new view
			"",                     // tableName (not applicable)
			DirViews+"/"+DirCreate, // pathPrefix for staging
		)
		child := v.NewPersistentInode(ctx, createNode, stableAttr)
		return child, 0
	}

	// Check if view exists in cache
	exists, err := v.cache.HasView(ctx, name)
	if err != nil {
		logging.Error("Failed to check view existence",
			zap.String("view", name),
			zap.Error(err))
		return nil, syscall.EIO
	}

	if !exists {
		logging.Debug("View not found", zap.String("view", name))
		return nil, syscall.ENOENT
	}

	// View exists - create view directory node
	logging.Debug("View found", zap.String("view", name))

	// Get the resolved default schema from the cache
	defaultSchema := v.cache.GetDefaultSchema()
	if defaultSchema == "" {
		defaultSchema = v.cfg.DefaultSchema
	}

	// Create a view node (uses the same TableNode with isView=true)
	viewNode := NewViewNode(v.cfg, v.db, v.cache, defaultSchema, name, v.partialRows, v.staging)
	child := v.NewPersistentInode(ctx, viewNode, stableAttr)

	return child, 0
}
