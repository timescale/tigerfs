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

// SchemasNode represents the .schemas/ directory at the filesystem root.
// Lists all user-defined schemas and provides navigation to individual schema directories.
// Also provides .create/ for creating new schemas.
type SchemasNode struct {
	fs.Inode

	cfg         *config.Config
	db          *db.Client
	cache       *MetadataCache
	partialRows *PartialRowTracker
	staging     *StagingTracker
}

var _ fs.InodeEmbedder = (*SchemasNode)(nil)
var _ fs.NodeReaddirer = (*SchemasNode)(nil)
var _ fs.NodeLookuper = (*SchemasNode)(nil)
var _ fs.NodeGetattrer = (*SchemasNode)(nil)

// NewSchemasNode creates a new .schemas directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for schema info
//   - partialRows: Tracker for partial row creation
//   - staging: Tracker for DDL staging operations
func NewSchemasNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, partialRows *PartialRowTracker, staging *StagingTracker) *SchemasNode {
	return &SchemasNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		partialRows: partialRows,
		staging:     staging,
	}
}

// Getattr returns attributes for the .schemas directory.
func (s *SchemasNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SchemasNode.Getattr called")

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all user-defined schemas plus .create/ for creating new schemas.
func (s *SchemasNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("SchemasNode.Readdir called")

	// Get all schemas from cache
	schemas, err := s.cache.GetSchemas(ctx)
	if err != nil {
		logging.Error("Failed to get schemas", zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert schemas to directory entries (+ 1 for .create)
	entries := make([]fuse.DirEntry, 0, len(schemas)+1)

	// Add .create directory first
	entries = append(entries, fuse.DirEntry{
		Name: DirCreate,
		Mode: syscall.S_IFDIR,
	})

	// Add existing schemas
	for _, schema := range schemas {
		entries = append(entries, fuse.DirEntry{
			Name: schema,
			Mode: syscall.S_IFDIR, // Schemas are directories
		})
	}

	logging.Debug(".schemas directory listing",
		zap.Int("schema_count", len(schemas)),
		zap.Strings("schemas", schemas))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a schema name or .create in the .schemas directory.
func (s *SchemasNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("SchemasNode.Lookup called", zap.String("name", name))

	// Handle .create directory for creating new schemas
	if name == DirCreate {
		logging.Debug("Looking up .create directory for schemas")

		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
		}

		createNode := NewCreateDirNode(
			s.cfg,
			s.db, // db.DDLExecutor
			s.cache,
			s.staging,
			"schema",                 // objectType
			"",                       // schema (not applicable for schema creation)
			"",                       // tableName (not applicable)
			DirSchemas+"/"+DirCreate, // pathPrefix for staging
		)
		child := s.NewPersistentInode(ctx, createNode, stableAttr)
		return child, 0
	}

	// Check if schema exists
	exists, err := s.cache.HasSchema(ctx, name)
	if err != nil {
		logging.Error("Failed to check schema existence",
			zap.String("schema", name),
			zap.Error(err))
		return nil, syscall.EIO
	}

	if !exists {
		logging.Debug("Schema not found", zap.String("schema", name))
		return nil, syscall.ENOENT
	}

	// Schema exists - create schema directory node
	logging.Debug("Schema found", zap.String("schema", name))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	schemaNode := NewSchemaNode(s.cfg, s.db, s.cache, name, s.partialRows, s.staging)
	child := s.NewPersistentInode(ctx, schemaNode, stableAttr)

	return child, 0
}
