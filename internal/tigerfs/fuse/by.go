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
	"go.uber.org/zap"
)

// ByDirNode represents the .by/ index navigation directory within a table.
// This directory aggregates all indexed columns, providing navigation by column values:
//   - Single-column indexes: email/ (navigate by email values)
//   - Composite indexes: last_name.first_name/ (navigate by multiple columns)
//
// Unlike the legacy index paths (e.g., .email/ at table root), the .by/ directory
// uses names without leading dots, avoiding collisions with reserved capability names.
//
// See ADR-005 for rationale on separating index navigation from the table root.
type ByDirNode struct {
	fs.Inode

	// cfg holds filesystem configuration including max listing limits
	cfg *config.Config

	// db is the database client for querying indexes and values
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name (e.g., "public")
	schema string

	// tableName is the table this .by/ directory belongs to
	tableName string

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*ByDirNode)(nil)
var _ fs.NodeGetattrer = (*ByDirNode)(nil)
var _ fs.NodeReaddirer = (*ByDirNode)(nil)
var _ fs.NodeLookuper = (*ByDirNode)(nil)

// NewByDirNode creates a new .by directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil)
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table this index directory belongs to
//   - partialRows: Tracker for incremental row creation
func NewByDirNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker) *ByDirNode {
	return &ByDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the .by directory.
// The directory is read-only (mode 0500) since index structure is determined by the database.
func (b *ByDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ByDirNode.Getattr called",
		zap.String("schema", b.schema),
		zap.String("table", b.tableName))

	out.Mode = 0500 | syscall.S_IFDIR // read-only directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all indexed columns in the .by directory.
// Returns both single-column indexes (e.g., "email") and composite indexes
// (e.g., "last_name.first_name"). Primary key indexes are excluded since
// rows are already directly accessible by PK at the table root.
func (b *ByDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("ByDirNode.Readdir called",
		zap.String("schema", b.schema),
		zap.String("table", b.tableName))

	// Get single-column indexes
	singleIndexes, err := b.db.GetSingleColumnIndexes(ctx, b.schema, b.tableName)
	if err != nil {
		logging.Error("Failed to get single-column indexes",
			zap.String("table", b.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Get composite indexes
	compositeIndexes, err := b.db.GetCompositeIndexes(ctx, b.schema, b.tableName)
	if err != nil {
		logging.Error("Failed to get composite indexes",
			zap.String("table", b.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(singleIndexes)+len(compositeIndexes))

	// Add single-column index directories (without leading dot)
	// Skip primary key since rows are already accessible by PK at table root
	for _, idx := range singleIndexes {
		if idx.IsPrimary {
			continue
		}
		if len(idx.Columns) > 0 {
			entries = append(entries, fuse.DirEntry{
				Name: idx.Columns[0], // Just column name, no dot prefix
				Mode: syscall.S_IFDIR,
			})
		}
	}

	// Add composite index directories (dot-separated column names)
	// e.g., "last_name.first_name" for index on (last_name, first_name)
	for _, idx := range compositeIndexes {
		if idx.IsPrimary {
			continue
		}
		entries = append(entries, fuse.DirEntry{
			Name: strings.Join(idx.Columns, "."),
			Mode: syscall.S_IFDIR,
		})
	}

	logging.Debug("By directory listing",
		zap.String("table", b.tableName),
		zap.Int("single_index_count", len(singleIndexes)),
		zap.Int("composite_index_count", len(compositeIndexes)),
		zap.Int("total_entries", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up an index by column name(s).
// For single-column indexes, name is just the column (e.g., "email").
// For composite indexes, name is dot-separated columns (e.g., "last_name.first_name").
//
// Returns the appropriate IndexNode or CompositeIndexNode for navigation.
func (b *ByDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("ByDirNode.Lookup called",
		zap.String("schema", b.schema),
		zap.String("table", b.tableName),
		zap.String("name", name))

	// Check if this is a composite index (contains dots)
	if strings.Contains(name, ".") {
		columns := strings.Split(name, ".")
		return b.lookupCompositeIndex(ctx, columns)
	}

	// Single-column index lookup
	return b.lookupSingleIndex(ctx, name)
}

// lookupSingleIndex handles lookup for single-column indexes.
// Verifies the column has an index before creating the node.
func (b *ByDirNode) lookupSingleIndex(ctx context.Context, columnName string) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up single-column index",
		zap.String("parent", DirBy),
		zap.String("table", b.tableName),
		zap.String("column", columnName))

	// Check if this column has an index
	idx, err := b.db.GetIndexByColumn(ctx, b.schema, b.tableName, columnName)
	if err != nil {
		logging.Error("Failed to check for index",
			zap.String("table", b.tableName),
			zap.String("column", columnName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	if idx == nil {
		logging.Debug("No index found for column",
			zap.String("table", b.tableName),
			zap.String("column", columnName))
		return nil, syscall.ENOENT
	}

	// Primary key indexes are excluded - rows accessible via PK directly
	if idx.IsPrimary {
		logging.Debug("Skipping primary key index",
			zap.String("table", b.tableName),
			zap.String("column", columnName))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	indexNode := NewIndexNode(b.cfg, b.db, b.cache, b.schema, b.tableName, columnName, idx, b.partialRows)
	child := b.NewPersistentInode(ctx, indexNode, stableAttr)

	logging.Debug("Created index node",
		zap.String("parent", DirBy),
		zap.String("table", b.tableName),
		zap.String("column", columnName),
		zap.String("index", idx.Name))

	return child, 0
}

// columnsMatch checks if two column slices are equal (same length and elements in order).
func columnsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// lookupCompositeIndex handles lookup for composite indexes.
// Verifies a matching composite index exists with exactly these columns in order.
func (b *ByDirNode) lookupCompositeIndex(ctx context.Context, columns []string) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up composite index",
		zap.String("parent", DirBy),
		zap.String("table", b.tableName),
		zap.Strings("columns", columns))

	// Get all composite indexes and find one that matches these columns exactly
	compositeIndexes, err := b.db.GetCompositeIndexes(ctx, b.schema, b.tableName)
	if err != nil {
		logging.Error("Failed to get composite indexes",
			zap.String("table", b.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Find index with matching columns
	var matchingIdx *db.Index
	for i := range compositeIndexes {
		idx := &compositeIndexes[i]
		if idx.IsPrimary {
			continue
		}
		if columnsMatch(idx.Columns, columns) {
			matchingIdx = idx
			break
		}
	}

	if matchingIdx == nil {
		logging.Debug("No composite index found for columns",
			zap.String("table", b.tableName),
			zap.Strings("columns", columns))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	compositeNode := NewCompositeIndexNode(b.cfg, b.db, b.cache, b.schema, b.tableName, columns, matchingIdx, b.partialRows)
	child := b.NewPersistentInode(ctx, compositeNode, stableAttr)

	logging.Debug("Created composite index node",
		zap.String("parent", DirBy),
		zap.String("table", b.tableName),
		zap.Strings("columns", columns),
		zap.String("index", matchingIdx.Name))

	return child, 0
}
