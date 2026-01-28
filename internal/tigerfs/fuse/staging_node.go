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

// DDLOperation represents the type of DDL operation being staged.
type DDLOperation int

const (
	// DDLCreate represents a CREATE operation (table, index, schema, view)
	DDLCreate DDLOperation = iota
	// DDLModify represents an ALTER/MODIFY operation
	DDLModify
	// DDLDelete represents a DROP/DELETE operation
	DDLDelete
)

// StagingContext provides context for template generation and DDL execution.
type StagingContext struct {
	Operation   DDLOperation // Type of DDL operation
	ObjectType  string       // "table", "index", "schema", "view"
	ObjectName  string       // Name of the object being created/modified/deleted
	Schema      string       // PostgreSQL schema (for tables, indexes)
	TableName   string       // Parent table (for indexes)
	StagingPath string       // Full path in staging tracker
}

// StagingDirNode represents a staging directory for DDL operations.
// Contains control files: .schema, .test, .commit, .abort
//
// Examples:
//   - .create/orders/ (staging for CREATE TABLE orders)
//   - users/.modify/ (staging for ALTER TABLE users)
//   - users/.delete/ (staging for DROP TABLE users)
type StagingDirNode struct {
	fs.Inode

	cfg     *config.Config
	db      db.DDLExecutor
	staging *StagingTracker
	ctx     StagingContext // Context for this staging operation
}

var _ fs.InodeEmbedder = (*StagingDirNode)(nil)
var _ fs.NodeGetattrer = (*StagingDirNode)(nil)
var _ fs.NodeReaddirer = (*StagingDirNode)(nil)
var _ fs.NodeLookuper = (*StagingDirNode)(nil)

// NewStagingDirNode creates a new staging directory node.
func NewStagingDirNode(cfg *config.Config, dbClient db.DDLExecutor, staging *StagingTracker, ctx StagingContext) *StagingDirNode {
	return &StagingDirNode{
		cfg:     cfg,
		db:      dbClient,
		staging: staging,
		ctx:     ctx,
	}
}

// Getattr returns attributes for the staging directory.
func (s *StagingDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("StagingDirNode.Getattr called",
		zap.String("path", s.ctx.StagingPath))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the control files in the staging directory.
func (s *StagingDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("StagingDirNode.Readdir called",
		zap.String("path", s.ctx.StagingPath))

	entries := []fuse.DirEntry{
		{Name: ".sql", Mode: syscall.S_IFREG},
		{Name: ".test", Mode: syscall.S_IFREG},
		{Name: ".commit", Mode: syscall.S_IFREG},
		{Name: ".abort", Mode: syscall.S_IFREG},
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a control file in the staging directory.
func (s *StagingDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("StagingDirNode.Lookup called",
		zap.String("path", s.ctx.StagingPath),
		zap.String("name", name))

	switch name {
	case ".sql":
		node := NewSchemaFileNode(s.cfg, s.db, s.staging, s.ctx)
		stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
		child := s.NewPersistentInode(ctx, node, stableAttr)
		// Set file attributes - .sql has content (staged DDL or template)
		content := s.staging.GetContent(s.ctx.StagingPath)
		if content == "" {
			// Generate template to get size
			content = node.getContent(ctx)
		}
		out.Mode = 0644 | syscall.S_IFREG
		out.Nlink = 1
		out.Size = uint64(len(content))
		return child, 0

	case ".test":
		node := NewTestFileNode(s.cfg, s.db, s.staging, s.ctx)
		stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
		child := s.NewPersistentInode(ctx, node, stableAttr)
		// Set file attributes - .test shows test results if any
		result := s.staging.GetTestResult(s.ctx.StagingPath)
		out.Mode = 0644 | syscall.S_IFREG
		out.Nlink = 1
		out.Size = uint64(len(result))
		return child, 0

	case ".commit":
		node := NewCommitFileNode(s.cfg, s.db, s.staging, s.ctx)
		stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
		child := s.NewPersistentInode(ctx, node, stableAttr)
		// Set file attributes - .commit is a trigger file (no readable content)
		out.Mode = 0644 | syscall.S_IFREG
		out.Nlink = 1
		out.Size = 0 // Trigger-only, no content
		return child, 0

	case ".abort":
		node := NewAbortFileNode(s.cfg, s.db, s.staging, s.ctx)
		stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
		child := s.NewPersistentInode(ctx, node, stableAttr)
		// Set file attributes - .abort is a trigger file (no readable content)
		out.Mode = 0644 | syscall.S_IFREG
		out.Nlink = 1
		out.Size = 0 // Trigger-only, no content
		return child, 0

	default:
		return nil, syscall.ENOENT
	}
}

// CreateDirNode represents a .create/ directory that lists pending creations
// and allows mkdir to create new staging entries.
//
// Examples:
//   - /.create/ (create tables in default schema)
//   - /.schemas/.create/ (create new schemas)
//   - /users/.indexes/.create/ (create indexes on users table)
type CreateDirNode struct {
	fs.Inode

	cfg        *config.Config
	db         db.DDLExecutor
	cache      *MetadataCache
	staging    *StagingTracker
	objectType string // "table", "index", "schema", "view"
	schema     string // PostgreSQL schema (for tables)
	tableName  string // Parent table (for indexes)
	pathPrefix string // Prefix for staging paths
}

var _ fs.InodeEmbedder = (*CreateDirNode)(nil)
var _ fs.NodeGetattrer = (*CreateDirNode)(nil)
var _ fs.NodeReaddirer = (*CreateDirNode)(nil)
var _ fs.NodeLookuper = (*CreateDirNode)(nil)
var _ fs.NodeMkdirer = (*CreateDirNode)(nil)

// NewCreateDirNode creates a new .create directory node.
func NewCreateDirNode(cfg *config.Config, dbClient db.DDLExecutor, cache *MetadataCache, staging *StagingTracker, objectType, schema, tableName, pathPrefix string) *CreateDirNode {
	return &CreateDirNode{
		cfg:        cfg,
		db:         dbClient,
		cache:      cache,
		staging:    staging,
		objectType: objectType,
		schema:     schema,
		tableName:  tableName,
		pathPrefix: pathPrefix,
	}
}

// Getattr returns attributes for the .create directory.
func (c *CreateDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("CreateDirNode.Getattr called",
		zap.String("objectType", c.objectType))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists pending creations from the staging tracker.
func (c *CreateDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("CreateDirNode.Readdir called",
		zap.String("objectType", c.objectType),
		zap.String("pathPrefix", c.pathPrefix))

	// List pending creations from staging tracker
	pending := c.staging.ListPending(c.pathPrefix)

	entries := make([]fuse.DirEntry, len(pending))
	for i, name := range pending {
		entries[i] = fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFDIR,
		}
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a pending creation or returns ENOENT for new entries.
func (c *CreateDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("CreateDirNode.Lookup called",
		zap.String("objectType", c.objectType),
		zap.String("name", name))

	stagingPath := c.pathPrefix + "/" + name

	// Check if staging entry exists
	entry := c.staging.Get(stagingPath)
	if entry == nil {
		// Entry doesn't exist - return ENOENT so mkdir can create it
		// Note: This means direct writes like `echo ... > .create/name/.sql`
		// require `mkdir .create/name` first. See README for workflow options.
		return nil, syscall.ENOENT
	}

	// Create staging context
	stagingCtx := StagingContext{
		Operation:   DDLCreate,
		ObjectType:  c.objectType,
		ObjectName:  name,
		Schema:      c.schema,
		TableName:   c.tableName,
		StagingPath: stagingPath,
	}

	node := NewStagingDirNode(c.cfg, c.db, c.staging, stagingCtx)
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	child := c.NewPersistentInode(ctx, node, stableAttr)

	return child, 0
}

// Mkdir creates a new staging entry for the given name.
func (c *CreateDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("CreateDirNode.Mkdir called",
		zap.String("objectType", c.objectType),
		zap.String("name", name))

	stagingPath := c.pathPrefix + "/" + name

	// Create staging entry
	c.staging.GetOrCreate(stagingPath)

	// Create staging context
	stagingCtx := StagingContext{
		Operation:   DDLCreate,
		ObjectType:  c.objectType,
		ObjectName:  name,
		Schema:      c.schema,
		TableName:   c.tableName,
		StagingPath: stagingPath,
	}

	node := NewStagingDirNode(c.cfg, c.db, c.staging, stagingCtx)
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	child := c.NewPersistentInode(ctx, node, stableAttr)

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2

	logging.Info("Created staging entry",
		zap.String("objectType", c.objectType),
		zap.String("name", name),
		zap.String("stagingPath", stagingPath))

	return child, 0
}
