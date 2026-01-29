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
// Provides .create/ for creating new views via DDL staging.
// Note: Views are accessed directly from the root directory alongside tables,
// not through .views/. This directory only provides DDL operations.
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

// Readdir lists .create/ for creating new views.
// Views themselves are accessed from the root directory, not from .views/.
func (v *ViewsNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("ViewsNode.Readdir called")

	// Only show .create directory for DDL operations
	entries := []fuse.DirEntry{
		{
			Name: DirCreate,
			Mode: syscall.S_IFDIR,
		},
	}

	logging.Debug(".views directory listing (DDL only)")

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up .create in the .views directory.
// Views themselves are accessed from the root directory, not from .views/.
func (v *ViewsNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("ViewsNode.Lookup called", zap.String("name", name))

	// Only handle .create directory for creating new views
	if name == DirCreate {
		logging.Debug("Looking up .create directory for views")

		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
		}

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

	// Views are accessed from root directory, not from .views/
	logging.Debug("View lookup in .views/ - views accessed from root",
		zap.String("name", name))
	return nil, syscall.ENOENT
}
