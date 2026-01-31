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

	// pipeline holds the pipeline context for capability chaining (may be nil)
	pipeline *PipelineContext
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

// NewOrderDirNodeWithPipeline creates a new .order directory node with pipeline context.
// The pipeline context is passed through to child nodes for capability chaining.
func NewOrderDirNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker, pipeline *PipelineContext) *OrderDirNode {
	return &OrderDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
		pipeline:    pipeline,
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

	columnNode := NewOrderColumnNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, name, o.partialRows, o.pipeline)
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

	// pipeline holds the pipeline context for capability chaining (may be nil)
	pipeline *PipelineContext
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

// NewOrderColumnNodeWithPipeline creates a new order column node with pipeline context.
// The pipeline context is passed through to child nodes for capability chaining.
func NewOrderColumnNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, partialRows *PartialRowTracker, pipeline *PipelineContext) *OrderColumnNode {
	return &OrderColumnNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		orderColumn: orderColumn,
		partialRows: partialRows,
		pipeline:    pipeline,
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

// Readdir lists available capabilities after .order/<col>/.
// Per ADR-007: .first/, .last/, .sample/, .export/ are allowed after ordering.
func (o *OrderColumnNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OrderColumnNode.Readdir called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn))

	entries := []fuse.DirEntry{
		{Name: DirExport, Mode: syscall.S_IFDIR},
		{Name: DirFirst, Mode: syscall.S_IFDIR},
		{Name: DirLast, Mode: syscall.S_IFDIR},
		{Name: DirSample, Mode: syscall.S_IFDIR},
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles .first, .last, .sample, and .export lookups.
// Per ADR-007: .first/, .last/, .sample/, .export/ are allowed after .order/<col>/.
func (o *OrderColumnNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("OrderColumnNode.Lookup called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("name", name))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Build ordered pipeline - order is ASC for all cases except .last pagination
	var orderedPipelineAsc *PipelineContext
	var orderedPipelineDesc *PipelineContext
	if o.pipeline != nil {
		orderedPipelineAsc = o.pipeline.WithOrder(o.orderColumn, false)
		orderedPipelineDesc = o.pipeline.WithOrder(o.orderColumn, true)
	}

	switch name {
	case DirFirst:
		paginationNode := NewOrderedPaginationNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.orderColumn, PaginationFirst, o.partialRows, orderedPipelineAsc)
		child := o.NewPersistentInode(ctx, paginationNode, stableAttr)
		logging.Debug("Created ordered pagination node",
			zap.String("table", o.tableName),
			zap.String("column", o.orderColumn),
			zap.String("type", string(PaginationFirst)))
		return child, 0

	case DirLast:
		paginationNode := NewOrderedPaginationNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.orderColumn, PaginationLast, o.partialRows, orderedPipelineDesc)
		child := o.NewPersistentInode(ctx, paginationNode, stableAttr)
		logging.Debug("Created ordered pagination node",
			zap.String("table", o.tableName),
			zap.String("column", o.orderColumn),
			zap.String("type", string(PaginationLast)))
		return child, 0

	case DirSample:
		// Sample after order uses ASC ordering
		sampleNode := NewSampleNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.partialRows, orderedPipelineAsc)
		child := o.NewPersistentInode(ctx, sampleNode, stableAttr)
		logging.Debug("Created sample node after order",
			zap.String("table", o.tableName),
			zap.String("column", o.orderColumn))
		return child, 0

	case DirExport:
		// Export after order uses ASC ordering
		exportNode := NewPipelineExportDirNode(o.cfg, o.db, o.cache, o.schema, o.tableName, orderedPipelineAsc)
		child := o.NewPersistentInode(ctx, exportNode, stableAttr)
		logging.Debug("Created export node after order",
			zap.String("table", o.tableName),
			zap.String("column", o.orderColumn))
		return child, 0

	default:
		logging.Debug("Unknown name in OrderColumnNode",
			zap.String("name", name))
		return nil, syscall.ENOENT
	}
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

	// pipeline holds the pipeline context for capability chaining (may be nil)
	pipeline *PipelineContext
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

// NewOrderedPaginationNodeWithPipeline creates a new ordered pagination node with pipeline context.
// The pipeline context has the order already applied; it's passed through to child nodes.
func NewOrderedPaginationNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, paginationType PaginationType, partialRows *PartialRowTracker, pipeline *PipelineContext) *OrderedPaginationNode {
	return &OrderedPaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		orderColumn:    orderColumn,
		paginationType: paginationType,
		partialRows:    partialRows,
		pipeline:       pipeline,
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

	limitNode := NewOrderedPaginationLimitNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.orderColumn, o.paginationType, limit, o.partialRows, o.pipeline)
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
// When pipeline context is present, also exposes pipeline capabilities.
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

	// pipeline holds the pipeline context with order and limit applied (may be nil)
	pipeline *PipelineContext
}

var _ fs.InodeEmbedder = (*OrderedPaginationLimitNode)(nil)
var _ fs.NodeGetattrer = (*OrderedPaginationLimitNode)(nil)
var _ fs.NodeReaddirer = (*OrderedPaginationLimitNode)(nil)
var _ fs.NodeLookuper = (*OrderedPaginationLimitNode)(nil)

// NewOrderedPaginationLimitNode creates a new ordered pagination limit node.
// For backward compatibility, this creates a node without pipeline context.
func NewOrderedPaginationLimitNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, paginationType PaginationType, limit int, partialRows *PartialRowTracker) *OrderedPaginationLimitNode {
	return NewOrderedPaginationLimitNodeWithPipeline(cfg, dbClient, cache, schema, tableName, orderColumn, paginationType, limit, partialRows, nil)
}

// NewOrderedPaginationLimitNodeWithPipeline creates a new ordered pagination limit node with pipeline context.
// When basePipeline is provided, the limit is applied to create the node's pipeline.
func NewOrderedPaginationLimitNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, orderColumn string, paginationType PaginationType, limit int, partialRows *PartialRowTracker, basePipeline *PipelineContext) *OrderedPaginationLimitNode {
	var pipeline *PipelineContext

	// Apply limit to pipeline if context exists
	if basePipeline != nil {
		limitType := LimitFirst
		if paginationType == PaginationLast {
			limitType = LimitLast
		}
		pipeline = basePipeline.WithLimit(limit, limitType)
	}

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
		pipeline:       pipeline,
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
// When pipeline context is present, also lists available capabilities.
func (o *OrderedPaginationLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("OrderedPaginationLimitNode.Readdir called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", o.limit),
		zap.Bool("has_pipeline", o.pipeline != nil))

	// Get primary key for table
	pk, err := o.db.GetPrimaryKey(ctx, o.schema, o.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", o.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Get rows - use pipeline query if context exists
	var rows []string
	if o.pipeline != nil {
		rows, err = o.db.QueryRowsPipeline(ctx, o.pipeline.ToQueryParams())
	} else {
		// Legacy behavior: direct query
		if o.paginationType == PaginationFirst {
			rows, err = o.db.GetFirstNRowsOrdered(ctx, o.schema, o.tableName, pkColumn, o.orderColumn, o.limit)
		} else {
			rows, err = o.db.GetLastNRowsOrdered(ctx, o.schema, o.tableName, pkColumn, o.orderColumn, o.limit)
		}
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

	// Build entries: capabilities first (if pipeline), then rows
	var entries []fuse.DirEntry

	// Add pipeline capabilities if context exists
	if o.pipeline != nil {
		caps := o.pipeline.AvailableCapabilities()
		for _, cap := range caps {
			entries = append(entries, fuse.DirEntry{
				Name: cap,
				Mode: syscall.S_IFDIR,
			})
		}
	}

	// Add rows
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
		zap.Int("row_count", len(rows)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row or capability within the ordered results.
// Per ADR-007: After .first/N/ or .last/N/ (ordered), expose .by/, .filter/, .order/,
// .first/ (if in .last), .last/ (if in .first), .sample/, .export/.
func (o *OrderedPaginationLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("OrderedPaginationLimitNode.Lookup called",
		zap.String("table", o.tableName),
		zap.String("column", o.orderColumn),
		zap.String("type", string(o.paginationType)),
		zap.Int("limit", o.limit),
		zap.String("name", name))

	// Check for pipeline capabilities first
	if o.pipeline != nil {
		switch name {
		case DirExport:
			if o.pipeline.CanExport() {
				return o.lookupExport(ctx)
			}
		case DirBy:
			if o.pipeline.CanAddFilter() {
				return o.lookupBy(ctx)
			}
		case DirFilter:
			if o.pipeline.CanAddFilter() {
				return o.lookupFilter(ctx)
			}
		case DirOrder:
			if o.pipeline.CanAddOrder() {
				return o.lookupOrder(ctx)
			}
		case DirFirst:
			if o.pipeline.CanAddLimit(LimitFirst) {
				return o.lookupPagination(ctx, PaginationFirst)
			}
		case DirLast:
			if o.pipeline.CanAddLimit(LimitLast) {
				return o.lookupPagination(ctx, PaginationLast)
			}
		case DirSample:
			if o.pipeline.CanAddLimit(LimitSample) {
				return o.lookupSample(ctx)
			}
		}
	}

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

// lookupExport creates an export node using the pipeline context.
func (o *OrderedPaginationLimitNode) lookupExport(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	exportNode := NewPipelineExportDirNode(o.cfg, o.db, o.cache, o.schema, o.tableName, o.pipeline)
	child := o.NewPersistentInode(ctx, exportNode, stableAttr)
	return child, 0
}

// lookupBy creates a .by/ node using the pipeline context.
func (o *OrderedPaginationLimitNode) lookupBy(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	byNode := NewByDirNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.partialRows, o.pipeline)
	child := o.NewPersistentInode(ctx, byNode, stableAttr)
	return child, 0
}

// lookupFilter creates a .filter/ node using the pipeline context.
func (o *OrderedPaginationLimitNode) lookupFilter(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	filterNode := NewFilterDirNode(o.cfg, o.db, o.cache, o.schema, o.tableName, o.pipeline, o.partialRows)
	child := o.NewPersistentInode(ctx, filterNode, stableAttr)
	return child, 0
}

// lookupOrder creates a .order/ node using the pipeline context.
func (o *OrderedPaginationLimitNode) lookupOrder(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	orderNode := NewOrderDirNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.partialRows, o.pipeline)
	child := o.NewPersistentInode(ctx, orderNode, stableAttr)
	return child, 0
}

// lookupPagination creates a .first/ or .last/ node for nested pagination.
func (o *OrderedPaginationLimitNode) lookupPagination(ctx context.Context, paginationType PaginationType) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	paginationNode := NewPaginationNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, paginationType, o.partialRows, o.pipeline)
	child := o.NewPersistentInode(ctx, paginationNode, stableAttr)
	return child, 0
}

// lookupSample creates a .sample/ node using the pipeline context.
func (o *OrderedPaginationLimitNode) lookupSample(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	sampleNode := NewSampleNodeWithPipeline(o.cfg, o.db, o.cache, o.schema, o.tableName, o.partialRows, o.pipeline)
	child := o.NewPersistentInode(ctx, sampleNode, stableAttr)
	return child, 0
}
