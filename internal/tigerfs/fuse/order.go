package fuse

import (
	"context"
	"strconv"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// OrderDirNode represents the .order/ directory within a table.
// This directory lists all columns that can be used for ordering.
// Navigation continues to .order/<column>/.first/N/ or .order/<column>/.last/N/.
type OrderDirNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client for querying columns
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table this .order/ directory belongs to
	tableName string

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*OrderDirNode)(nil)
var _ fs.NodeGetattrer = (*OrderDirNode)(nil)
var _ fs.NodeReaddirer = (*OrderDirNode)(nil)
var _ fs.NodeLookuper = (*OrderDirNode)(nil)

// NewOrderDirNode creates a new .order directory node.
func NewOrderDirNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker) *OrderDirNode {
	return &OrderDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the .order directory.
func (o *OrderDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("OrderDirNode.Getattr called",
		zap.String("schema", o.schema),
		zap.String("table", o.tableName))

	out.Mode = 0500 | syscall.S_IFDIR // read-only directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all columns that can be used for ordering.
func (o *OrderDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OrderDirNode.Readdir called",
		zap.String("schema", o.schema),
		zap.String("table", o.tableName))

	columns, err := o.db.GetColumns(ctx, o.schema, o.tableName)
	if err != nil {
		logging.Error("Failed to get columns for order directory",
			zap.String("table", o.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(columns))
	for _, col := range columns {
		entries = append(entries, fuse.DirEntry{
			Name: col.Name,
			Mode: syscall.S_IFDIR,
		})
	}

	logging.Debug("Order directory listing",
		zap.String("table", o.tableName),
		zap.Int("column_count", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a column name to create an OrderColumnNode.
func (o *OrderDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("OrderDirNode.Lookup called",
		zap.String("schema", o.schema),
		zap.String("table", o.tableName),
		zap.String("name", name))

	// Verify the column exists
	columns, err := o.db.GetColumns(ctx, o.schema, o.tableName)
	if err != nil {
		logging.Error("Failed to get columns",
			zap.String("table", o.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	var found bool
	for _, col := range columns {
		if col.Name == name {
			found = true
			break
		}
	}

	if !found {
		logging.Debug("Column not found for ordering",
			zap.String("table", o.tableName),
			zap.String("column", name))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	columnNode := NewOrderColumnNode(o.cfg, o.db, o.cache, o.schema, o.tableName, name, o.partialRows)
	child := o.NewPersistentInode(ctx, columnNode, stableAttr)

	logging.Debug("Created order column node",
		zap.String("table", o.tableName),
		zap.String("column", name))

	return child, 0
}

// OrderColumnNode represents a column directory under .order/<column>/.
// Lists .first and .last as ordering direction options.
type OrderColumnNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client for queries
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table name
	tableName string

	// orderColumn is the column to order by
	orderColumn string

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*OrderColumnNode)(nil)
var _ fs.NodeGetattrer = (*OrderColumnNode)(nil)
var _ fs.NodeReaddirer = (*OrderColumnNode)(nil)
var _ fs.NodeLookuper = (*OrderColumnNode)(nil)

// NewOrderColumnNode creates a new order column node.
func NewOrderColumnNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, partialRows *PartialRowTracker) *OrderColumnNode {
	return &OrderColumnNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		orderColumn: orderColumn,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the order column directory.
func (o *OrderColumnNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("OrderColumnNode.Getattr called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn))

	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists .first and .last directories.
func (o *OrderColumnNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OrderColumnNode.Readdir called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn))

	entries := []fuse.DirEntry{
		{Name: DirFirst[1:], Mode: syscall.S_IFDIR}, // "first" without leading dot
		{Name: DirLast[1:], Mode: syscall.S_IFDIR},  // "last" without leading dot
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles .first and .last lookups.
func (o *OrderColumnNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("OrderColumnNode.Lookup called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("name", name))

	// Check for pagination directories (without leading dot in this context)
	var paginationType PaginationType
	switch name {
	case DirFirst[1:]: // "first"
		paginationType = PaginationFirst
	case DirLast[1:]: // "last"
		paginationType = PaginationLast
	default:
		logging.Debug("Invalid order direction",
			zap.String("name", name))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	paginationNode := NewOrderedPaginationNode(o.cfg, o.db, o.cache, o.schema, o.tableName, o.orderColumn, paginationType, o.partialRows)
	child := o.NewPersistentInode(ctx, paginationNode, stableAttr)

	logging.Debug("Created ordered pagination node",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(paginationType)))

	return child, 0
}

// OrderedPaginationNode represents .order/<column>/first/ or .order/<column>/last/.
// User specifies N to get first/last N rows ordered by the column.
type OrderedPaginationNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client for queries
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table name
	tableName string

	// orderColumn is the column to order by
	orderColumn string

	// paginationType is first or last
	paginationType PaginationType

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*OrderedPaginationNode)(nil)
var _ fs.NodeGetattrer = (*OrderedPaginationNode)(nil)
var _ fs.NodeReaddirer = (*OrderedPaginationNode)(nil)
var _ fs.NodeLookuper = (*OrderedPaginationNode)(nil)

// NewOrderedPaginationNode creates a new ordered pagination node.
func NewOrderedPaginationNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, paginationType PaginationType, partialRows *PartialRowTracker) *OrderedPaginationNode {
	return &OrderedPaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		orderColumn:    orderColumn,
		paginationType: paginationType,
		partialRows:    partialRows,
	}
}

// Getattr returns attributes for the ordered pagination directory.
func (o *OrderedPaginationNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("OrderedPaginationNode.Getattr called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir returns empty - user must specify N.
func (o *OrderedPaginationNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OrderedPaginationNode.Readdir called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)))

	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Lookup parses N and creates OrderedPaginationLimitNode.
func (o *OrderedPaginationNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("OrderedPaginationNode.Lookup called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.String("name", name))

	limit, err := strconv.Atoi(name)
	if err != nil || limit < 1 {
		logging.Debug("Invalid pagination limit",
			zap.String("name", name),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	limitNode := NewOrderedPaginationLimitNode(o.cfg, o.db, o.cache, o.schema, o.tableName, o.orderColumn, o.paginationType, limit, o.partialRows)
	child := o.NewPersistentInode(ctx, limitNode, stableAttr)

	logging.Debug("Created ordered pagination limit node",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", limit))

	return child, 0
}

// OrderedPaginationLimitNode represents .order/<column>/first/N/ or .order/<column>/last/N/.
// Lists the first or last N rows ordered by the specified column.
type OrderedPaginationLimitNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client for queries
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table name
	tableName string

	// orderColumn is the column to order by
	orderColumn string

	// paginationType is first or last
	paginationType PaginationType

	// limit is the maximum number of rows
	limit int

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*OrderedPaginationLimitNode)(nil)
var _ fs.NodeGetattrer = (*OrderedPaginationLimitNode)(nil)
var _ fs.NodeReaddirer = (*OrderedPaginationLimitNode)(nil)
var _ fs.NodeLookuper = (*OrderedPaginationLimitNode)(nil)

// NewOrderedPaginationLimitNode creates a new ordered pagination limit node.
func NewOrderedPaginationLimitNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, paginationType PaginationType, limit int, partialRows *PartialRowTracker) *OrderedPaginationLimitNode {
	return &OrderedPaginationLimitNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		orderColumn:    orderColumn,
		paginationType: paginationType,
		limit:          limit,
		partialRows:    partialRows,
	}
}

// Getattr returns attributes for the ordered pagination limit directory.
func (o *OrderedPaginationLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("OrderedPaginationLimitNode.Getattr called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", o.limit))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists rows ordered by the specified column.
func (o *OrderedPaginationLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OrderedPaginationLimitNode.Readdir called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", o.limit))

	// Get primary key for table
	pk, err := o.db.GetPrimaryKey(ctx, o.schema, o.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", o.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Get rows ordered by the specified column
	var rows []string
	if o.paginationType == PaginationFirst {
		rows, err = o.db.GetFirstNRowsOrdered(ctx, o.schema, o.tableName, pkColumn, o.orderColumn, o.limit)
	} else {
		rows, err = o.db.GetLastNRowsOrdered(ctx, o.schema, o.tableName, pkColumn, o.orderColumn, o.limit)
	}

	if err != nil {
		logging.Error("Failed to get ordered rows",
			zap.String("table", o.tableName),
			zap.String("column", o.orderColumn),
			zap.String("type", string(o.paginationType)),
			zap.Int("limit", o.limit),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(rows))
	for _, rowPK := range rows {
		entries = append(entries, fuse.DirEntry{
			Name: rowPK,
			Mode: syscall.S_IFREG,
		})
	}

	logging.Debug("Ordered pagination directory listing",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", o.limit),
		zap.Int("row_count", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row within the ordered results.
func (o *OrderedPaginationLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("OrderedPaginationLimitNode.Lookup called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", o.limit),
		zap.String("name", name))

	pkValue, format := util.ParseRowFilename(name)

	pk, err := o.db.GetPrimaryKey(ctx, o.schema, o.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", o.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row exists
	_, err = o.db.GetRow(ctx, o.schema, o.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", o.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	// Row exists - create appropriate node
	if name != pkValue {
		// Name has extension, create row file node
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}

		rowNode := NewRowFileNode(o.cfg, o.db, o.cache, o.schema, o.tableName, pkColumn, pkValue, format)
		child := o.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(o.cfg, o.db, o.cache, o.schema, o.tableName, pkColumn, pkValue, o.partialRows)
	child := o.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}
