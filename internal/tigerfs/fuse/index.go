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

// IndexNode represents a single-column index directory (e.g., .email/).
// Lists distinct values in the indexed column, enabling navigation like:
//
//	ls /mnt/db/users/.email/           -> lists distinct emails
//	cat /mnt/db/users/.email/foo@x.com -> accesses row(s) with that email
type IndexNode struct {
	fs.Inode

	// cfg holds filesystem configuration including max listing limits
	cfg *config.Config

	// db is the database client for querying distinct values
	db *db.Client

	// schema is the PostgreSQL schema name (e.g., "public")
	schema string

	// tableName is the table this index belongs to
	tableName string

	// column is the indexed column name (e.g., "email")
	column string

	// index contains the full index metadata from PostgreSQL
	index *db.Index

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*IndexNode)(nil)
var _ fs.NodeGetattrer = (*IndexNode)(nil)
var _ fs.NodeReaddirer = (*IndexNode)(nil)
var _ fs.NodeLookuper = (*IndexNode)(nil)

// NewIndexNode creates a new index directory node.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client for queries
//   - schema: Schema name
//   - tableName: Table name
//   - column: Indexed column name
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewIndexNode(cfg *config.Config, dbClient *db.Client, schema, tableName, column string, index *db.Index, partialRows *PartialRowTracker) *IndexNode {
	return &IndexNode{
		cfg:         cfg,
		db:          dbClient,
		schema:      schema,
		tableName:   tableName,
		column:      column,
		index:       index,
		partialRows: partialRows,
	}
}

// Getattr returns directory attributes for the index node.
func (n *IndexNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("IndexNode.Getattr called",
		zap.String("table", n.tableName),
		zap.String("column", n.column))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists distinct values in the indexed column.
// Limited to prevent huge listings on high-cardinality columns.
// Returns values as directory entries (each value can be navigated into).
func (n *IndexNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexNode.Readdir called",
		zap.String("table", n.tableName),
		zap.String("column", n.column))

	// Limit distinct values to prevent huge directory listings
	// Use a reasonable default; could make this configurable
	const maxDistinctValues = 100

	values, err := n.db.GetDistinctValues(ctx, n.schema, n.tableName, n.column, maxDistinctValues)
	if err != nil {
		logging.Error("Failed to get distinct values",
			zap.String("table", n.tableName),
			zap.String("column", n.column),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert values to directory entries
	// Each value is shown as a directory (can navigate into it)
	entries := make([]fuse.DirEntry, 0, len(values))
	for _, val := range values {
		entries = append(entries, fuse.DirEntry{
			Name: val,
			Mode: syscall.S_IFDIR, // Values are directories containing matching rows
		})
	}

	logging.Debug("Index directory listing",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.Int("value_count", len(values)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a specific value within the index.
// For example, looking up "foo@example.com" within .email/.
//
// The lookup queries for rows matching this column value:
//   - If exactly one row matches: returns that row (as directory or file)
//   - If multiple rows match: returns a directory listing matching PKs
//
// This is implemented in Task 4.3; for now returns ENOENT.
func (n *IndexNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", name))

	// Task 4.3 will implement full index-based queries
	// For now, just verify the value exists in the index

	// Query for rows with this value
	pk, err := n.db.GetPrimaryKey(ctx, n.schema, n.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", n.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Get rows matching this index value
	rows, err := n.db.GetRowsByIndexValue(ctx, n.schema, n.tableName, n.column, name, pkColumn, 100)
	if err != nil {
		logging.Error("Failed to query by index value",
			zap.String("table", n.tableName),
			zap.String("column", n.column),
			zap.String("value", name),
			zap.Error(err))
		return nil, syscall.EIO
	}

	if len(rows) == 0 {
		logging.Debug("No rows found for index value",
			zap.String("table", n.tableName),
			zap.String("column", n.column),
			zap.String("value", name))
		return nil, syscall.ENOENT
	}

	logging.Debug("Found rows for index value",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", name),
		zap.Int("count", len(rows)))

	// Create an IndexValueNode to handle the result
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	valueNode := NewIndexValueNode(n.cfg, n.db, n.schema, n.tableName, n.column, name, pkColumn, rows, n.partialRows)
	child := n.NewPersistentInode(ctx, valueNode, stableAttr)

	return child, 0
}

// IndexValueNode represents the result of an index lookup (e.g., .email/foo@x.com/).
// Contains the rows that match the index value.
type IndexValueNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client
	db *db.Client

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table name
	tableName string

	// column is the indexed column name
	column string

	// value is the looked-up value (e.g., "foo@x.com")
	value string

	// pkColumn is the primary key column name
	pkColumn string

	// matchingPKs contains the primary keys of matching rows
	matchingPKs []string

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*IndexValueNode)(nil)
var _ fs.NodeGetattrer = (*IndexValueNode)(nil)
var _ fs.NodeReaddirer = (*IndexValueNode)(nil)
var _ fs.NodeLookuper = (*IndexValueNode)(nil)

// NewIndexValueNode creates a node for an index value lookup result.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client
//   - schema: Schema name
//   - tableName: Table name
//   - column: Indexed column name
//   - value: The value being looked up
//   - pkColumn: Primary key column name
//   - matchingPKs: Primary keys of rows matching the value
//   - partialRows: Tracker for incremental row creation
func NewIndexValueNode(cfg *config.Config, dbClient *db.Client, schema, tableName, column, value, pkColumn string, matchingPKs []string, partialRows *PartialRowTracker) *IndexValueNode {
	return &IndexValueNode{
		cfg:         cfg,
		db:          dbClient,
		schema:      schema,
		tableName:   tableName,
		column:      column,
		value:       value,
		pkColumn:    pkColumn,
		matchingPKs: matchingPKs,
		partialRows: partialRows,
	}
}

// Getattr returns directory attributes for the index value node.
func (n *IndexValueNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("IndexValueNode.Getattr called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the primary keys of rows matching the index value.
func (n *IndexValueNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexValueNode.Readdir called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value),
		zap.Int("match_count", len(n.matchingPKs)))

	// Convert matching PKs to directory entries
	entries := make([]fuse.DirEntry, 0, len(n.matchingPKs))
	for _, pk := range n.matchingPKs {
		entries = append(entries, fuse.DirEntry{
			Name: pk,
			Mode: syscall.S_IFREG, // PKs shown as files (row-as-file)
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a specific row within the index value results.
func (n *IndexValueNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexValueNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value),
		zap.String("name", name))

	// Check if name is one of the matching PKs
	found := false
	for _, pk := range n.matchingPKs {
		if pk == name {
			found = true
			break
		}
	}

	if !found {
		return nil, syscall.ENOENT
	}

	// Create a row directory node for this PK
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(n.cfg, n.db, n.schema, n.tableName, n.pkColumn, name, n.partialRows)
	child := n.NewPersistentInode(ctx, rowDirNode, stableAttr)

	return child, 0
}
