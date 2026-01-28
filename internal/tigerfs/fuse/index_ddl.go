// Package fuse provides FUSE filesystem nodes for TigerFS.
//
// This file implements DDL operations for indexes, including:
// - IndexesNode: The .indexes directory listing all indexes with create/delete support
// - IndexDDLNode: Individual index directory with .delete/ support
// - IndexCreateDirNode: The .create directory for staging new index creation
package fuse

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// ============================================================================
// IndexesNode - The .indexes directory
// ============================================================================

// IndexesNode represents the .indexes directory within a table.
// Lists all indexes on the table and provides .create/ for new index creation.
type IndexesNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client for index queries
	db db.DBClient

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table this indexes directory belongs to
	tableName string

	// staging tracks DDL operations in progress
	staging *StagingTracker
}

var _ fs.InodeEmbedder = (*IndexesNode)(nil)
var _ fs.NodeGetattrer = (*IndexesNode)(nil)
var _ fs.NodeReaddirer = (*IndexesNode)(nil)
var _ fs.NodeLookuper = (*IndexesNode)(nil)

// NewIndexesNode creates a new indexes directory node.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client for index queries
//   - schema: Schema name containing the table
//   - tableName: Table name
//   - staging: Tracker for DDL operations in progress
func NewIndexesNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string, staging *StagingTracker) *IndexesNode {
	return &IndexesNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		staging:   staging,
	}
}

// Getattr returns directory attributes for the indexes node.
func (n *IndexesNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("IndexesNode.Getattr called",
		zap.String("table", n.tableName))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all indexes on the table plus .create/ directory.
func (n *IndexesNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexesNode.Readdir called",
		zap.String("table", n.tableName))

	// Start with .create directory
	entries := []fuse.DirEntry{
		{Name: ".create", Mode: syscall.S_IFDIR},
	}

	// Get all indexes for this table
	indexes, err := n.db.GetIndexes(ctx, n.schema, n.tableName)
	if err != nil {
		logging.Error("Failed to get indexes",
			zap.String("table", n.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Add each index as a directory entry (by index name)
	for _, idx := range indexes {
		entries = append(entries, fuse.DirEntry{
			Name: idx.Name,
			Mode: syscall.S_IFDIR,
		})
	}

	logging.Debug("Indexes directory listing",
		zap.String("table", n.tableName),
		zap.Int("index_count", len(indexes)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to .create/ or individual index directories.
func (n *IndexesNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexesNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("name", name))

	// Handle .create directory
	if name == ".create" {
		return n.lookupCreateDirectory(ctx, out)
	}

	// Look up existing index by name
	return n.lookupIndexByName(ctx, name, out)
}

// lookupCreateDirectory returns the IndexCreateDirNode for .create/.
func (n *IndexesNode) lookupCreateDirectory(ctx context.Context, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	createNode := NewIndexCreateDirNode(n.cfg, n.db, n.schema, n.tableName, n.staging)
	child := n.NewPersistentInode(ctx, createNode, stableAttr)

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	logging.Debug("Created .create directory for indexes",
		zap.String("table", n.tableName))

	return child, 0
}

// lookupIndexByName returns an IndexDDLNode for an existing index.
func (n *IndexesNode) lookupIndexByName(ctx context.Context, indexName string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Get all indexes and find matching one
	indexes, err := n.db.GetIndexes(ctx, n.schema, n.tableName)
	if err != nil {
		logging.Error("Failed to get indexes",
			zap.String("table", n.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	var foundIndex *db.Index
	for _, idx := range indexes {
		if idx.Name == indexName {
			foundIndex = &idx
			break
		}
	}

	if foundIndex == nil {
		logging.Debug("Index not found",
			zap.String("table", n.tableName),
			zap.String("index", indexName))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	indexDDLNode := NewIndexDDLNode(n.cfg, n.db, n.schema, n.tableName, foundIndex, n.staging)
	child := n.NewPersistentInode(ctx, indexDDLNode, stableAttr)

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	logging.Debug("Created IndexDDLNode",
		zap.String("table", n.tableName),
		zap.String("index", indexName))

	return child, 0
}

// ============================================================================
// IndexDDLNode - Individual index directory with .delete support
// ============================================================================

// IndexDDLNode represents an individual index directory within .indexes/.
// Provides .delete/ for dropping the index.
type IndexDDLNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client
	db db.DBClient

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table this index belongs to
	tableName string

	// index is the index metadata
	index *db.Index

	// staging tracks DDL operations in progress
	staging *StagingTracker
}

var _ fs.InodeEmbedder = (*IndexDDLNode)(nil)
var _ fs.NodeGetattrer = (*IndexDDLNode)(nil)
var _ fs.NodeReaddirer = (*IndexDDLNode)(nil)
var _ fs.NodeLookuper = (*IndexDDLNode)(nil)

// NewIndexDDLNode creates a new index DDL node.
func NewIndexDDLNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string, index *db.Index, staging *StagingTracker) *IndexDDLNode {
	return &IndexDDLNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		index:     index,
		staging:   staging,
	}
}

// Getattr returns directory attributes.
func (n *IndexDDLNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists .delete/ and index info.
func (n *IndexDDLNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexDDLNode.Readdir called",
		zap.String("index", n.index.Name))

	entries := []fuse.DirEntry{
		{Name: ".delete", Mode: syscall.S_IFDIR},
		{Name: ".info", Mode: syscall.S_IFREG}, // Shows index definition
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles .delete/ and .info lookups.
func (n *IndexDDLNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexDDLNode.Lookup called",
		zap.String("index", n.index.Name),
		zap.String("name", name))

	switch name {
	case ".delete":
		return n.lookupDeleteDirectory(ctx, out)
	case ".info":
		return n.lookupInfoFile(ctx, out)
	default:
		return nil, syscall.ENOENT
	}
}

// lookupDeleteDirectory returns a StagingDirNode for dropping this index.
func (n *IndexDDLNode) lookupDeleteDirectory(ctx context.Context, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stagingCtx := StagingContext{
		ObjectType:  "index",
		ObjectName:  n.index.Name,
		Schema:      n.schema,
		TableName:   n.tableName,
		Operation:   DDLDelete,
		StagingPath: fmt.Sprintf("%s/.indexes/%s/.delete", n.tableName, n.index.Name),
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	deleteNode := NewStagingDirNode(n.cfg, n.db, n.staging, stagingCtx)
	child := n.NewPersistentInode(ctx, deleteNode, stableAttr)

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	logging.Debug("Created .delete directory for index",
		zap.String("index", n.index.Name))

	return child, 0
}

// lookupInfoFile returns a file containing the index definition.
func (n *IndexDDLNode) lookupInfoFile(ctx context.Context, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	infoNode := NewIndexInfoNode(n.cfg, n.db, n.schema, n.tableName, n.index)
	child := n.NewPersistentInode(ctx, infoNode, stableAttr)

	// Set file attributes
	content := n.getInfoContent()
	out.Mode = 0400 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(content))

	return child, 0
}

// getInfoContent generates the content for .info file.
func (n *IndexDDLNode) getInfoContent() string {
	var content string
	content += fmt.Sprintf("Name: %s\n", n.index.Name)
	content += fmt.Sprintf("Columns: %s\n", joinStrings(n.index.Columns, ", "))
	content += fmt.Sprintf("Unique: %v\n", n.index.IsUnique)
	content += fmt.Sprintf("Primary: %v\n", n.index.IsPrimary)
	return content
}

// joinStrings joins strings with a separator.
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// ============================================================================
// IndexInfoNode - Read-only file showing index information
// ============================================================================

// IndexInfoNode represents the .info file showing index definition.
type IndexInfoNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
	index     *db.Index
}

var _ fs.InodeEmbedder = (*IndexInfoNode)(nil)
var _ fs.NodeGetattrer = (*IndexInfoNode)(nil)
var _ fs.NodeOpener = (*IndexInfoNode)(nil)
var _ fs.NodeReader = (*IndexInfoNode)(nil)

// NewIndexInfoNode creates a new index info file node.
func NewIndexInfoNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string, index *db.Index) *IndexInfoNode {
	return &IndexInfoNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		index:     index,
	}
}

// Getattr returns file attributes.
func (n *IndexInfoNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content := n.getContent()
	out.Mode = 0400 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(content))
	return 0
}

// Open opens the file for reading.
func (n *IndexInfoNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Read returns the index information.
func (n *IndexInfoNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	content := n.getContent()
	if off >= int64(len(content)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(content)) {
		end = int64(len(content))
	}
	return fuse.ReadResultData([]byte(content)[off:end]), 0
}

// getContent generates the .info file content.
func (n *IndexInfoNode) getContent() string {
	var content string
	content += fmt.Sprintf("Name: %s\n", n.index.Name)
	content += fmt.Sprintf("Columns: %s\n", joinStrings(n.index.Columns, ", "))
	content += fmt.Sprintf("Unique: %v\n", n.index.IsUnique)
	content += fmt.Sprintf("Primary: %v\n", n.index.IsPrimary)
	return content
}

// ============================================================================
// IndexCreateDirNode - Directory for staging new index creation
// ============================================================================

// IndexCreateDirNode represents the .create directory within .indexes/.
// mkdir operations create new staging entries for index creation.
type IndexCreateDirNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client
	db db.DBClient

	// schema is the target schema for new indexes
	schema string

	// tableName is the table to create indexes on
	tableName string

	// staging tracks DDL operations in progress
	staging *StagingTracker
}

var _ fs.InodeEmbedder = (*IndexCreateDirNode)(nil)
var _ fs.NodeGetattrer = (*IndexCreateDirNode)(nil)
var _ fs.NodeReaddirer = (*IndexCreateDirNode)(nil)
var _ fs.NodeLookuper = (*IndexCreateDirNode)(nil)
var _ fs.NodeMkdirer = (*IndexCreateDirNode)(nil)

// NewIndexCreateDirNode creates a new index create directory node.
func NewIndexCreateDirNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string, staging *StagingTracker) *IndexCreateDirNode {
	return &IndexCreateDirNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		staging:   staging,
	}
}

// Getattr returns directory attributes.
func (n *IndexCreateDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists pending index creations from staging.
func (n *IndexCreateDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexCreateDirNode.Readdir called",
		zap.String("table", n.tableName))

	var entries []fuse.DirEntry

	// List staged index creations for this table
	if n.staging != nil {
		// The staging prefix for this table's index creations
		prefix := fmt.Sprintf("%s/.indexes/.create", n.tableName)
		pending := n.staging.ListPending(prefix)
		for _, name := range pending {
			entries = append(entries, fuse.DirEntry{
				Name: name,
				Mode: syscall.S_IFDIR,
			})
		}
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles lookups for staged index creations.
func (n *IndexCreateDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexCreateDirNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("name", name))

	// Check if this index is already staged
	if n.staging != nil {
		path := fmt.Sprintf("%s/.indexes/.create/%s", n.tableName, name)
		if entry := n.staging.Get(path); entry != nil {
			return n.createStagingNode(ctx, name, out)
		}
	}

	// Not found
	return nil, syscall.ENOENT
}

// Mkdir creates a new staging entry for index creation.
func (n *IndexCreateDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Info("Creating index staging entry",
		zap.String("table", n.tableName),
		zap.String("index", name))

	if n.staging == nil {
		logging.Error("No staging tracker available")
		return nil, syscall.EIO
	}

	// Create staging entry
	path := fmt.Sprintf("%s/.indexes/.create/%s", n.tableName, name)

	// Generate template for index creation
	template := n.generateIndexTemplate(name)
	n.staging.Set(path, template)

	return n.createStagingNode(ctx, name, out)
}

// createStagingNode creates a StagingDirNode for the named index.
func (n *IndexCreateDirNode) createStagingNode(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := fmt.Sprintf("%s/.indexes/.create/%s", n.tableName, name)
	stagingCtx := StagingContext{
		ObjectType:  "index",
		ObjectName:  name,
		Schema:      n.schema,
		TableName:   n.tableName,
		Operation:   DDLCreate,
		StagingPath: path,
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	stagingNode := NewStagingDirNode(n.cfg, n.db, n.staging, stagingCtx)
	child := n.NewPersistentInode(ctx, stagingNode, stableAttr)

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return child, 0
}

// generateIndexTemplate generates a CREATE INDEX template.
func (n *IndexCreateDirNode) generateIndexTemplate(indexName string) string {
	qualifiedTable := n.tableName
	if n.schema != "" && n.schema != "public" {
		qualifiedTable = fmt.Sprintf("%s.%s", n.schema, n.tableName)
	}

	return fmt.Sprintf(`-- Create index: %s
-- Table: %s
--
-- Modify this template to define your index.
-- Then run: touch .test  (to validate)
-- Finally:  touch .commit (to execute)
--
-- Examples:
--   CREATE INDEX %s ON %s (column_name);
--   CREATE UNIQUE INDEX %s ON %s (column_name);
--   CREATE INDEX %s ON %s (col1, col2);
--   CREATE INDEX %s ON %s USING gin (jsonb_column);
--
CREATE INDEX %s ON %s (column_name);
`,
		indexName, qualifiedTable,
		indexName, qualifiedTable,
		indexName, qualifiedTable,
		indexName, qualifiedTable,
		indexName, qualifiedTable,
		indexName, qualifiedTable,
	)
}
