// Package fuse provides FUSE filesystem operations for TigerFS.
//
// This file implements PipelineNode, the central node for capability chaining.
// PipelineNode accumulates query state via PipelineContext and routes to child
// capability nodes based on the current context state.
//
// PipelineNode is used after capability operations to expose:
// - Available next capabilities based on context rules
// - Rows matching the accumulated filters/limits

package fuse

import (
	"context"
	"strconv"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// PipelineNode represents a directory within a pipeline navigation path.
// It exposes available capabilities based on the accumulated PipelineContext
// and lists rows matching the pipeline's filters and limits.
//
// Example paths that result in a PipelineNode:
//   - /schema/table/.by/status/active/
//   - /schema/table/.filter/category/books/.order/name/
//   - /schema/table/.first/100/
type PipelineNode struct {
	fs.Inode

	cfg    *config.Config
	db     db.DBClient
	cache  *tigerfs.MetadataCache
	schema string
	table  string

	// pipeline holds the accumulated query state
	pipeline *PipelineContext

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineNode)(nil)
var _ fs.NodeGetattrer = (*PipelineNode)(nil)
var _ fs.NodeReaddirer = (*PipelineNode)(nil)
var _ fs.NodeLookuper = (*PipelineNode)(nil)

// NewPipelineNode creates a new pipeline node with the given context.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil)
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pipeline: Current pipeline context (must not be nil)
//   - partialRows: Tracker for incremental row creation
func NewPipelineNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext, partialRows *PartialRowTracker) *PipelineNode {
	return &PipelineNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the pipeline directory.
func (p *PipelineNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("PipelineNode.Getattr called",
		zap.String("schema", p.schema),
		zap.String("table", p.table),
		zap.Int("filter_count", len(p.pipeline.Filters)))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists available capabilities and matching rows.
// Capabilities are listed based on PipelineContext rules.
// Rows are queried using the accumulated filters and limits.
func (p *PipelineNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("PipelineNode.Readdir called",
		zap.String("schema", p.schema),
		zap.String("table", p.table),
		zap.Int("filter_count", len(p.pipeline.Filters)),
		zap.Bool("has_order", p.pipeline.HasOrdered),
		zap.Bool("has_limit", p.pipeline.HasLimit()))

	// Get available capabilities from context
	caps := p.pipeline.AvailableCapabilities()

	// Start with capability directories
	entries := make([]fuse.DirEntry, 0, len(caps)+100)
	for _, cap := range caps {
		entries = append(entries, fuse.DirEntry{
			Name: cap,
			Mode: syscall.S_IFDIR,
		})
	}

	// If not terminal, query for matching rows
	if !p.pipeline.IsTerminal {
		pks, err := p.queryRows(ctx)
		if err != nil {
			logging.Error("Failed to query rows for pipeline",
				zap.String("table", p.table),
				zap.Error(err))
			return nil, syscall.EIO
		}

		for _, pk := range pks {
			entries = append(entries, fuse.DirEntry{
				Name: pk,
				Mode: syscall.S_IFREG, // Row as file
			})
		}

		logging.Debug("PipelineNode directory listing",
			zap.String("table", p.table),
			zap.Int("capability_count", len(caps)),
			zap.Int("row_count", len(pks)))
	}

	return fs.NewListDirStream(entries), 0
}

// queryRows executes the pipeline query and returns matching primary keys.
func (p *PipelineNode) queryRows(ctx context.Context) ([]string, error) {
	params := p.pipeline.ToQueryParams()

	// Apply default limit if none specified
	if params.Limit == 0 {
		params.Limit = p.cfg.DirListingLimit
	}

	pks, err := p.db.QueryRowsPipeline(ctx, params)
	if err != nil {
		return nil, err
	}

	return pks, nil
}

// Lookup handles access to capabilities or rows within the pipeline.
func (p *PipelineNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("PipelineNode.Lookup called",
		zap.String("schema", p.schema),
		zap.String("table", p.table),
		zap.String("name", name))

	// Route to capabilities
	switch name {
	case DirBy:
		return p.lookupBy(ctx)
	case DirColumns:
		return p.lookupColumns(ctx)
	case DirFilter:
		return p.lookupFilter(ctx)
	case DirOrder:
		return p.lookupOrder(ctx)
	case DirFirst:
		return p.lookupLimit(ctx, LimitFirst)
	case DirLast:
		return p.lookupLimit(ctx, LimitLast)
	case DirSample:
		return p.lookupLimit(ctx, LimitSample)
	case DirExport:
		return p.lookupExport(ctx)
	}

	// Check if this is a row lookup
	return p.lookupRow(ctx, name)
}

// lookupBy creates a ByDirNode for .by/ navigation.
// Uses the current pipeline context for filter accumulation.
func (p *PipelineNode) lookupBy(ctx context.Context) (*fs.Inode, syscall.Errno) {
	if !p.pipeline.CanAddFilter() {
		logging.Debug("Cannot add filter after order",
			zap.String("table", p.table))
		return nil, syscall.ENOENT
	}

	logging.Debug("PipelineNode routing to .by/",
		zap.String("table", p.table))

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	byNode := NewPipelineByDirNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline, p.partialRows)
	child := p.NewPersistentInode(ctx, byNode, stableAttr)
	return child, 0
}

// lookupColumns creates a PipelineColumnsDirNode for .columns/ navigation.
func (p *PipelineNode) lookupColumns(ctx context.Context) (*fs.Inode, syscall.Errno) {
	if !p.pipeline.CanAddColumns() {
		logging.Debug("Cannot add columns - already set or terminal",
			zap.String("table", p.table))
		return nil, syscall.ENOENT
	}

	logging.Debug("PipelineNode routing to .columns/",
		zap.String("table", p.table))

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	columnsNode := NewPipelineColumnsDirNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline, p.partialRows)
	child := p.NewPersistentInode(ctx, columnsNode, stableAttr)
	return child, 0
}

// lookupFilter creates a FilterDirNode for .filter/ navigation.
func (p *PipelineNode) lookupFilter(ctx context.Context) (*fs.Inode, syscall.Errno) {
	if !p.pipeline.CanAddFilter() {
		logging.Debug("Cannot add filter after order",
			zap.String("table", p.table))
		return nil, syscall.ENOENT
	}

	logging.Debug("PipelineNode routing to .filter/",
		zap.String("table", p.table))

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	filterNode := NewFilterDirNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline, p.partialRows)
	child := p.NewPersistentInode(ctx, filterNode, stableAttr)
	return child, 0
}

// lookupOrder creates a PipelineOrderDirNode for .order/ navigation.
func (p *PipelineNode) lookupOrder(ctx context.Context) (*fs.Inode, syscall.Errno) {
	if !p.pipeline.CanAddOrder() {
		logging.Debug("Cannot add order - already ordered",
			zap.String("table", p.table))
		return nil, syscall.ENOENT
	}

	logging.Debug("PipelineNode routing to .order/",
		zap.String("table", p.table))

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	orderNode := NewPipelineOrderDirNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline, p.partialRows)
	child := p.NewPersistentInode(ctx, orderNode, stableAttr)
	return child, 0
}

// lookupLimit creates a PipelineLimitNode for .first/, .last/, or .sample/ navigation.
func (p *PipelineNode) lookupLimit(ctx context.Context, limitType LimitType) (*fs.Inode, syscall.Errno) {
	if !p.pipeline.CanAddLimit(limitType) {
		logging.Debug("Cannot add limit of this type",
			zap.String("table", p.table),
			zap.Int("limit_type", int(limitType)))
		return nil, syscall.ENOENT
	}

	logging.Debug("PipelineNode routing to limit capability",
		zap.String("table", p.table),
		zap.Int("limit_type", int(limitType)))

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	limitNode := NewPipelineLimitNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline, limitType, p.partialRows)
	child := p.NewPersistentInode(ctx, limitNode, stableAttr)
	return child, 0
}

// lookupExport creates a PipelineExportDirNode for .export/ navigation.
func (p *PipelineNode) lookupExport(ctx context.Context) (*fs.Inode, syscall.Errno) {
	if !p.pipeline.CanExport() {
		logging.Debug("Cannot export - pipeline is terminal",
			zap.String("table", p.table))
		return nil, syscall.ENOENT
	}

	logging.Debug("PipelineNode routing to .export/",
		zap.String("table", p.table))

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	exportNode := NewPipelineExportDirNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline)
	child := p.NewPersistentInode(ctx, exportNode, stableAttr)
	return child, 0
}

// lookupRow handles row lookup by primary key (with optional format extension).
func (p *PipelineNode) lookupRow(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	// Verify row exists
	_, err := p.db.GetRow(ctx, p.schema, p.table, p.pipeline.PKColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", p.table),
			zap.String("pk", pkValue),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	// If name has format extension, create row file node
	if name != pkValue {
		stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
		rowNode := NewRowFileNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline.PKColumn, pkValue, format)
		child := p.NewPersistentInode(ctx, rowNode, stableAttr)

		// Fill in entry attributes
		var attrOut fuse.AttrOut
		if errno := rowNode.Getattr(ctx, nil, &attrOut); errno != 0 {
			return nil, errno
		}

		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	rowDirNode := NewRowDirectoryNode(p.cfg, p.db, p.cache, p.schema, p.table, p.pipeline.PKColumn, pkValue, p.partialRows)
	child := p.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// PipelineByDirNode is a .by/ node that works with PipelineContext.
// It shows indexed columns and routes to PipelineByColumnNode.
type PipelineByDirNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	pipeline    *PipelineContext
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineByDirNode)(nil)
var _ fs.NodeGetattrer = (*PipelineByDirNode)(nil)
var _ fs.NodeReaddirer = (*PipelineByDirNode)(nil)
var _ fs.NodeLookuper = (*PipelineByDirNode)(nil)

// NewPipelineByDirNode creates a pipeline-aware .by/ node.
func NewPipelineByDirNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext, partialRows *PartialRowTracker) *PipelineByDirNode {
	return &PipelineByDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the .by directory.
func (b *PipelineByDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists indexed columns.
func (b *PipelineByDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Get single-column indexes
	singleIndexes, err := b.db.GetSingleColumnIndexes(ctx, b.schema, b.table)
	if err != nil {
		logging.Error("Failed to get single-column indexes",
			zap.String("table", b.table),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(singleIndexes))
	for _, idx := range singleIndexes {
		if idx.IsPrimary {
			continue
		}
		if len(idx.Columns) > 0 {
			entries = append(entries, fuse.DirEntry{
				Name: idx.Columns[0],
				Mode: syscall.S_IFDIR,
			})
		}
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles column lookup.
func (b *PipelineByDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Check if column has an index
	idx, err := b.db.GetIndexByColumn(ctx, b.schema, b.table, name)
	if err != nil {
		logging.Error("Failed to check for index",
			zap.String("table", b.table),
			zap.String("column", name),
			zap.Error(err))
		return nil, syscall.EIO
	}

	if idx == nil || idx.IsPrimary {
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	columnNode := NewPipelineByColumnNode(b.cfg, b.db, b.cache, b.schema, b.table, name, b.pipeline, b.partialRows)
	child := b.NewPersistentInode(ctx, columnNode, stableAttr)
	return child, 0
}

// PipelineByColumnNode is a column node under .by/<column>/ that works with PipelineContext.
type PipelineByColumnNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	column      string
	pipeline    *PipelineContext
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineByColumnNode)(nil)
var _ fs.NodeGetattrer = (*PipelineByColumnNode)(nil)
var _ fs.NodeReaddirer = (*PipelineByColumnNode)(nil)
var _ fs.NodeLookuper = (*PipelineByColumnNode)(nil)

// NewPipelineByColumnNode creates a pipeline-aware index column node.
func NewPipelineByColumnNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table, column string, pipeline *PipelineContext, partialRows *PartialRowTracker) *PipelineByColumnNode {
	return &PipelineByColumnNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		column:      column,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes.
func (c *PipelineByColumnNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists distinct values.
func (c *PipelineByColumnNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	const maxDistinctValues = 100

	values, err := c.db.GetDistinctValues(ctx, c.schema, c.table, c.column, maxDistinctValues)
	if err != nil {
		logging.Error("Failed to get distinct values",
			zap.String("table", c.table),
			zap.String("column", c.column),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(values))
	for _, val := range values {
		entries = append(entries, fuse.DirEntry{
			Name: val,
			Mode: syscall.S_IFDIR,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles value lookup - creates PipelineNode with filter applied.
func (c *PipelineByColumnNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Add filter to pipeline (indexed = true for .by/)
	newPipeline := c.pipeline.WithFilter(c.column, name, true)

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	pipelineNode := NewPipelineNode(c.cfg, c.db, c.cache, c.schema, c.table, newPipeline, c.partialRows)
	child := c.NewPersistentInode(ctx, pipelineNode, stableAttr)
	return child, 0
}

// PipelineOrderDirNode is a .order/ node that works with PipelineContext.
type PipelineOrderDirNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	pipeline    *PipelineContext
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineOrderDirNode)(nil)
var _ fs.NodeGetattrer = (*PipelineOrderDirNode)(nil)
var _ fs.NodeReaddirer = (*PipelineOrderDirNode)(nil)
var _ fs.NodeLookuper = (*PipelineOrderDirNode)(nil)

// NewPipelineOrderDirNode creates a pipeline-aware .order/ node.
func NewPipelineOrderDirNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext, partialRows *PartialRowTracker) *PipelineOrderDirNode {
	return &PipelineOrderDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes.
func (o *PipelineOrderDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists columns.
func (o *PipelineOrderDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	columns, err := o.db.GetColumns(ctx, o.schema, o.table)
	if err != nil {
		logging.Error("Failed to get columns for order directory",
			zap.String("table", o.table),
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

	return fs.NewListDirStream(entries), 0
}

// Lookup handles column lookup.
func (o *PipelineOrderDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Verify column exists
	columns, err := o.db.GetColumns(ctx, o.schema, o.table)
	if err != nil {
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
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	columnNode := NewPipelineOrderColumnNode(o.cfg, o.db, o.cache, o.schema, o.table, name, o.pipeline, o.partialRows)
	child := o.NewPersistentInode(ctx, columnNode, stableAttr)
	return child, 0
}

// PipelineOrderColumnNode is a column node under .order/<column>/.
// It shows "first" and "last" direction options.
type PipelineOrderColumnNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	column      string
	pipeline    *PipelineContext
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineOrderColumnNode)(nil)
var _ fs.NodeGetattrer = (*PipelineOrderColumnNode)(nil)
var _ fs.NodeReaddirer = (*PipelineOrderColumnNode)(nil)
var _ fs.NodeLookuper = (*PipelineOrderColumnNode)(nil)

// NewPipelineOrderColumnNode creates a pipeline-aware order column node.
func NewPipelineOrderColumnNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table, column string, pipeline *PipelineContext, partialRows *PartialRowTracker) *PipelineOrderColumnNode {
	return &PipelineOrderColumnNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		column:      column,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes.
func (c *PipelineOrderColumnNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists direction options.
func (c *PipelineOrderColumnNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "first", Mode: syscall.S_IFDIR}, // ASC
		{Name: "last", Mode: syscall.S_IFDIR},  // DESC
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup handles direction lookup - applies order and returns PipelineNode.
func (c *PipelineOrderColumnNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var desc bool
	switch name {
	case "first":
		desc = false // ASC
	case "last":
		desc = true // DESC
	default:
		return nil, syscall.ENOENT
	}

	// Apply order to pipeline
	newPipeline := c.pipeline.WithOrder(c.column, desc)

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	pipelineNode := NewPipelineNode(c.cfg, c.db, c.cache, c.schema, c.table, newPipeline, c.partialRows)
	child := c.NewPersistentInode(ctx, pipelineNode, stableAttr)
	return child, 0
}

// PipelineLimitNode represents .first/, .last/, or .sample/ within a pipeline.
// User specifies N to get a limit applied.
type PipelineLimitNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	pipeline    *PipelineContext
	limitType   LimitType
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineLimitNode)(nil)
var _ fs.NodeGetattrer = (*PipelineLimitNode)(nil)
var _ fs.NodeReaddirer = (*PipelineLimitNode)(nil)
var _ fs.NodeLookuper = (*PipelineLimitNode)(nil)

// NewPipelineLimitNode creates a pipeline-aware limit node.
func NewPipelineLimitNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext, limitType LimitType, partialRows *PartialRowTracker) *PipelineLimitNode {
	return &PipelineLimitNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		pipeline:    pipeline,
		limitType:   limitType,
		partialRows: partialRows,
	}
}

// Getattr returns attributes.
func (l *PipelineLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir returns empty - user must specify N.
func (l *PipelineLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Lookup parses N and creates PipelineNode with limit applied.
func (l *PipelineLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	limit, err := strconv.Atoi(name)
	if err != nil || limit < 1 {
		logging.Debug("Invalid limit value",
			zap.String("name", name),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	// Apply limit to pipeline
	newPipeline := l.pipeline.WithLimit(limit, l.limitType)

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	pipelineNode := NewPipelineNode(l.cfg, l.db, l.cache, l.schema, l.table, newPipeline, l.partialRows)
	child := l.NewPersistentInode(ctx, pipelineNode, stableAttr)
	return child, 0
}

// PipelineExportDirNode represents .export/ within a pipeline.
// It lists available formats and creates export files using the pipeline context.
type PipelineExportDirNode struct {
	fs.Inode

	cfg      *config.Config
	db       db.DBClient
	cache    *tigerfs.MetadataCache
	schema   string
	table    string
	pipeline *PipelineContext
}

var _ fs.InodeEmbedder = (*PipelineExportDirNode)(nil)
var _ fs.NodeGetattrer = (*PipelineExportDirNode)(nil)
var _ fs.NodeReaddirer = (*PipelineExportDirNode)(nil)
var _ fs.NodeLookuper = (*PipelineExportDirNode)(nil)

// NewPipelineExportDirNode creates a pipeline-aware export directory node.
func NewPipelineExportDirNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext) *PipelineExportDirNode {
	return &PipelineExportDirNode{
		cfg:      cfg,
		db:       dbClient,
		cache:    cache,
		schema:   schema,
		table:    table,
		pipeline: pipeline,
	}
}

// Getattr returns attributes.
func (e *PipelineExportDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists available formats and options.
func (e *PipelineExportDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: DirWithHeaders, Mode: syscall.S_IFDIR},
		{Name: FmtCSV, Mode: syscall.S_IFREG},
		{Name: FmtJSON, Mode: syscall.S_IFREG},
		{Name: FmtTSV, Mode: syscall.S_IFREG},
		{Name: FmtYAML, Mode: syscall.S_IFREG},
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup handles format or option lookup.
func (e *PipelineExportDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Handle .with-headers option
	if name == DirWithHeaders {
		stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
		withHeadersNode := NewPipelineExportWithHeadersNode(e.cfg, e.db, e.cache, e.schema, e.table, e.pipeline)
		child := e.NewPersistentInode(ctx, withHeadersNode, stableAttr)
		return child, 0
	}

	// Validate format
	switch name {
	case FmtCSV, FmtTSV, FmtJSON, FmtYAML:
		// Valid
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
	fileNode := NewPipelineExportFileNode(e.cfg, e.db, e.cache, e.schema, e.table, name, e.pipeline, false)
	child := e.NewPersistentInode(ctx, fileNode, stableAttr)
	return child, 0
}

// PipelineExportWithHeadersNode represents .export/.with-headers/ within a pipeline.
type PipelineExportWithHeadersNode struct {
	fs.Inode

	cfg      *config.Config
	db       db.DBClient
	cache    *tigerfs.MetadataCache
	schema   string
	table    string
	pipeline *PipelineContext
}

var _ fs.InodeEmbedder = (*PipelineExportWithHeadersNode)(nil)
var _ fs.NodeGetattrer = (*PipelineExportWithHeadersNode)(nil)
var _ fs.NodeReaddirer = (*PipelineExportWithHeadersNode)(nil)
var _ fs.NodeLookuper = (*PipelineExportWithHeadersNode)(nil)

// NewPipelineExportWithHeadersNode creates a pipeline-aware with-headers node.
func NewPipelineExportWithHeadersNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext) *PipelineExportWithHeadersNode {
	return &PipelineExportWithHeadersNode{
		cfg:      cfg,
		db:       dbClient,
		cache:    cache,
		schema:   schema,
		table:    table,
		pipeline: pipeline,
	}
}

// Getattr returns attributes.
func (w *PipelineExportWithHeadersNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists formats that support headers.
func (w *PipelineExportWithHeadersNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: FmtCSV, Mode: syscall.S_IFREG},
		{Name: FmtTSV, Mode: syscall.S_IFREG},
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup handles format lookup.
func (w *PipelineExportWithHeadersNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case FmtCSV, FmtTSV:
		// Valid
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
	fileNode := NewPipelineExportFileNode(w.cfg, w.db, w.cache, w.schema, w.table, name, w.pipeline, true)
	child := w.NewPersistentInode(ctx, fileNode, stableAttr)
	return child, 0
}

// PipelineExportFileNode represents a format file under .export/ that uses the pipeline.
type PipelineExportFileNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	format      string
	pipeline    *PipelineContext
	withHeaders bool

	// Cached materialized data
	data []byte
}

var _ fs.InodeEmbedder = (*PipelineExportFileNode)(nil)
var _ fs.NodeGetattrer = (*PipelineExportFileNode)(nil)
var _ fs.NodeOpener = (*PipelineExportFileNode)(nil)

// NewPipelineExportFileNode creates a pipeline-aware export file node.
func NewPipelineExportFileNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table, format string, pipeline *PipelineContext, withHeaders bool) *PipelineExportFileNode {
	return &PipelineExportFileNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		format:      format,
		pipeline:    pipeline,
		withHeaders: withHeaders,
	}
}

// Getattr returns attributes.
func (e *PipelineExportFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Materialize to get size
	if e.data == nil {
		if err := e.materialize(ctx); err != nil {
			logging.Error("Failed to materialize pipeline export",
				zap.String("table", e.table),
				zap.String("format", e.format),
				zap.Error(err))
			return syscall.EIO
		}
	}

	out.Mode = 0444 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(e.data))
	return 0
}

// Open opens the export file.
func (e *PipelineExportFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if e.data == nil {
		if err := e.materialize(ctx); err != nil {
			logging.Error("Failed to materialize pipeline export",
				zap.String("table", e.table),
				zap.String("format", e.format),
				zap.Error(err))
			return nil, 0, syscall.EIO
		}
	}

	fh := &ExportFileHandle{data: e.data}
	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// materialize fetches data using the pipeline and serializes it.
func (e *PipelineExportFileNode) materialize(ctx context.Context) error {
	logging.Debug("Materializing pipeline export",
		zap.String("table", e.table),
		zap.String("format", e.format),
		zap.Int("filter_count", len(e.pipeline.Filters)))

	params := e.pipeline.ToQueryParams()

	// Use limit from pipeline or default
	if params.Limit == 0 {
		params.Limit = e.cfg.DirListingLimit
	}

	// Query with data
	columns, rows, err := e.db.QueryRowsWithDataPipeline(ctx, params)
	if err != nil {
		return err
	}

	logging.Debug("Fetched pipeline rows for export",
		zap.String("table", e.table),
		zap.Int("row_count", len(rows)),
		zap.Int("column_count", len(columns)))

	// Serialize to format
	var serializeErr error
	switch e.format {
	case FmtCSV:
		if e.withHeaders {
			e.data, serializeErr = format.RowsToCSVWithHeaders(columns, rows)
		} else {
			e.data, serializeErr = format.RowsToCSV(columns, rows)
		}
	case FmtTSV:
		if e.withHeaders {
			e.data, serializeErr = format.RowsToTSVWithHeaders(columns, rows)
		} else {
			e.data, serializeErr = format.RowsToTSV(columns, rows)
		}
	case FmtJSON:
		e.data, serializeErr = format.RowsToJSON(columns, rows)
	case FmtYAML:
		e.data, serializeErr = format.RowsToYAML(columns, rows)
	default:
		return &unknownFormatError{format: e.format}
	}

	return serializeErr
}
