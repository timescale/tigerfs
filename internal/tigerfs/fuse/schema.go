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

// SchemaNode represents a directory for a specific schema under .schemas/.
// Lists tables in this schema and provides navigation to table directories.
// Also provides .create/ for creating tables and .delete/ for deleting the schema.
type SchemaNode struct {
	fs.Inode

	cfg         *config.Config
	db          *db.Client
	cache       *MetadataCache
	schema      string // The schema this node represents
	partialRows *PartialRowTracker
	staging     *StagingTracker
}

var _ fs.InodeEmbedder = (*SchemaNode)(nil)
var _ fs.NodeReaddirer = (*SchemaNode)(nil)
var _ fs.NodeLookuper = (*SchemaNode)(nil)
var _ fs.NodeGetattrer = (*SchemaNode)(nil)

// NewSchemaNode creates a new schema directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for schema/table info
//   - schema: The schema name this node represents
//   - partialRows: Tracker for partial row creation
//   - staging: Tracker for DDL staging operations
func NewSchemaNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema string, partialRows *PartialRowTracker, staging *StagingTracker) *SchemaNode {
	return &SchemaNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		partialRows: partialRows,
		staging:     staging,
	}
}

// Getattr returns attributes for the schema directory.
func (s *SchemaNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SchemaNode.Getattr called", zap.String("schema", s.schema))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the tables in this schema plus .create/ and .delete/ control directories.
func (s *SchemaNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("SchemaNode.Readdir called", zap.String("schema", s.schema))

	// Get tables for this schema from cache
	tables, err := s.cache.GetTablesForSchema(ctx, s.schema)
	if err != nil {
		logging.Error("Failed to get tables for schema",
			zap.String("schema", s.schema),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert tables to directory entries (+ 2 for .create and .delete)
	entries := make([]fuse.DirEntry, 0, len(tables)+2)

	// Add .create directory for creating new tables in this schema
	entries = append(entries, fuse.DirEntry{
		Name: DirCreate,
		Mode: syscall.S_IFDIR,
	})

	// Add .delete directory for deleting this schema
	entries = append(entries, fuse.DirEntry{
		Name: DirDelete,
		Mode: syscall.S_IFDIR,
	})

	// Add existing tables
	for _, table := range tables {
		entries = append(entries, fuse.DirEntry{
			Name: table,
			Mode: syscall.S_IFDIR, // Tables are directories
		})
	}

	logging.Debug("Schema directory listing",
		zap.String("schema", s.schema),
		zap.Int("table_count", len(tables)),
		zap.Strings("tables", tables))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a table name, .create, or .delete in the schema directory.
func (s *SchemaNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("SchemaNode.Lookup called",
		zap.String("schema", s.schema),
		zap.String("name", name))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Handle .create directory for creating new tables in this schema
	if name == DirCreate {
		logging.Debug("Looking up .create directory for tables",
			zap.String("schema", s.schema))

		pathPrefix := DirSchemas + "/" + s.schema + "/" + DirCreate
		createNode := NewCreateDirNode(
			s.cfg,
			s.db, // db.DDLExecutor
			s.cache,
			s.staging,
			"table",  // objectType
			s.schema, // schema for the new table
			"",       // tableName (not applicable for table creation)
			pathPrefix,
		)
		child := s.NewPersistentInode(ctx, createNode, stableAttr)
		return child, 0
	}

	// Handle .delete directory for deleting this schema
	if name == DirDelete {
		logging.Debug("Looking up .delete directory for schema",
			zap.String("schema", s.schema))

		stagingCtx := StagingContext{
			ObjectType: "schema",
			Schema:     s.schema,
			Operation:  DDLDelete,
		}
		deleteNode := NewStagingDirNode(
			s.cfg,
			s.db, // db.DDLExecutor
			s.staging,
			stagingCtx,
		)
		child := s.NewPersistentInode(ctx, deleteNode, stableAttr)
		return child, 0
	}

	// Get tables for this schema
	tables, err := s.cache.GetTablesForSchema(ctx, s.schema)
	if err != nil {
		logging.Error("Failed to get tables for schema",
			zap.String("schema", s.schema),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Check if table exists
	found := false
	for _, table := range tables {
		if table == name {
			found = true
			break
		}
	}

	if !found {
		logging.Debug("Table not found in schema",
			zap.String("schema", s.schema),
			zap.String("table", name))
		return nil, syscall.ENOENT
	}

	// Table exists - create table directory node
	logging.Debug("Table found in schema",
		zap.String("schema", s.schema),
		zap.String("table", name))

	// Create table node with this schema (not the default schema)
	tableNode := NewTableNode(s.cfg, s.db, s.cache, s.schema, name, s.partialRows, s.staging)
	child := s.NewPersistentInode(ctx, tableNode, stableAttr)

	return child, 0
}
