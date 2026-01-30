package fuse

import (
	"context"
	"strconv"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
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
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

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

	// pipeline holds the pipeline context for capability chaining (may be nil)
	pipeline *PipelineContext
}

var _ fs.InodeEmbedder = (*IndexNode)(nil)
var _ fs.NodeGetattrer = (*IndexNode)(nil)
var _ fs.NodeReaddirer = (*IndexNode)(nil)
var _ fs.NodeLookuper = (*IndexNode)(nil)

// NewIndexNode creates a new index directory node.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - column: Indexed column name
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewIndexNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column string, index *db.Index, partialRows *PartialRowTracker) *IndexNode {
	return &IndexNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		column:      column,
		index:       index,
		partialRows: partialRows,
	}
}

// NewIndexNodeWithPipeline creates a new index directory node with pipeline context.
// The pipeline context is passed through to child nodes for capability chaining.
func NewIndexNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column string, index *db.Index, partialRows *PartialRowTracker, pipeline *PipelineContext) *IndexNode {
	return &IndexNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		column:      column,
		index:       index,
		partialRows: partialRows,
		pipeline:    pipeline,
	}
}

// Getattr returns directory attributes for the index node.
func (n *IndexNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("IndexNode.Getattr called",
		zap.String("table", n.tableName),
		zap.String("column", n.column))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists distinct values in the indexed column.
// Limited to prevent huge listings on high-cardinality columns.
// Returns values as directory entries (each value can be navigated into).
// Also includes .first and .last for ordered pagination access.
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
	// Start with pagination directories
	entries := make([]fuse.DirEntry, 0, len(values)+2)
	entries = append(entries,
		fuse.DirEntry{Name: DirFirst, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirLast, Mode: syscall.S_IFDIR},
	)

	// Each value is shown as a directory (can navigate into it)
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

// Lookup handles access to a specific value within the index or pagination directories.
// For example, looking up "foo@example.com" within .email/ or .first within .email/.
//
// The lookup queries for rows matching this column value:
//   - If exactly one row matches: returns that row (as directory or file)
//   - If multiple rows match: returns a directory listing matching PKs
func (n *IndexNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", name))

	// Handle pagination directories
	if name == DirFirst || name == DirLast {
		return n.lookupPagination(ctx, name)
	}

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

	// Apply filter to pipeline if present
	var filteredPipeline *PipelineContext
	if n.pipeline != nil {
		filteredPipeline = n.pipeline.WithFilter(n.column, name, true) // true = indexed
	}

	// Create an IndexValueNode to handle the result
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	valueNode := NewIndexValueNodeWithPipeline(n.cfg, n.db, n.cache, n.schema, n.tableName, n.column, name, pkColumn, rows, n.partialRows, filteredPipeline)
	child := n.NewPersistentInode(ctx, valueNode, stableAttr)

	return child, 0
}

// lookupPagination handles .first and .last directory lookups within an index.
func (n *IndexNode) lookupPagination(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	var paginationType PaginationType
	if name == DirFirst {
		paginationType = PaginationFirst
	} else {
		paginationType = PaginationLast
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	pagNode := NewIndexPaginationNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.column, paginationType, n.partialRows)
	child := n.NewPersistentInode(ctx, pagNode, stableAttr)

	return child, 0
}

// IndexValueNode represents the result of an index lookup (e.g., .email/foo@x.com/).
// Contains the rows that match the index value.
// When pipeline context is present, also exposes pipeline capabilities.
type IndexValueNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

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

	// pipeline holds the pipeline context with the filter applied (may be nil)
	pipeline *PipelineContext
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
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - column: Indexed column name
//   - value: The value being looked up
//   - pkColumn: Primary key column name
//   - matchingPKs: Primary keys of rows matching the value
//   - partialRows: Tracker for incremental row creation
func NewIndexValueNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column, value, pkColumn string, matchingPKs []string, partialRows *PartialRowTracker) *IndexValueNode {
	return &IndexValueNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		column:      column,
		value:       value,
		pkColumn:    pkColumn,
		matchingPKs: matchingPKs,
		partialRows: partialRows,
	}
}

// NewIndexValueNodeWithPipeline creates a node for an index value lookup result with pipeline context.
// The pipeline context has the filter already applied.
func NewIndexValueNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column, value, pkColumn string, matchingPKs []string, partialRows *PartialRowTracker, pipeline *PipelineContext) *IndexValueNode {
	return &IndexValueNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		column:      column,
		value:       value,
		pkColumn:    pkColumn,
		matchingPKs: matchingPKs,
		partialRows: partialRows,
		pipeline:    pipeline,
	}
}

// Getattr returns directory attributes for the index value node.
func (n *IndexValueNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("IndexValueNode.Getattr called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the primary keys of rows matching the index value.
// When pipeline context is present, lists available capabilities.
// Also includes .first and .last for ordered pagination access.
func (n *IndexValueNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexValueNode.Readdir called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value),
		zap.Int("match_count", len(n.matchingPKs)),
		zap.Bool("has_pipeline", n.pipeline != nil))

	var entries []fuse.DirEntry

	// Add pipeline capabilities if context exists
	if n.pipeline != nil {
		caps := n.pipeline.AvailableCapabilities()
		for _, cap := range caps {
			entries = append(entries, fuse.DirEntry{
				Name: cap,
				Mode: syscall.S_IFDIR,
			})
		}
	} else {
		// Legacy behavior: add pagination directories
		entries = append(entries,
			fuse.DirEntry{Name: DirFirst, Mode: syscall.S_IFDIR},
			fuse.DirEntry{Name: DirLast, Mode: syscall.S_IFDIR},
		)
	}

	// Add matching PKs as files
	for _, pk := range n.matchingPKs {
		entries = append(entries, fuse.DirEntry{
			Name: pk,
			Mode: syscall.S_IFREG, // PKs shown as files (row-as-file)
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a specific row within the index value results.
// Supports bare PKs, PKs with format extensions, and pipeline capabilities.
func (n *IndexValueNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexValueNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value),
		zap.String("name", name))

	// Check for pipeline capabilities first
	if n.pipeline != nil {
		switch name {
		case DirExport:
			if n.pipeline.CanExport() {
				return n.lookupExport(ctx)
			}
		case DirFirst:
			if n.pipeline.CanAddLimit(LimitFirst) {
				return n.lookupPipelineLimit(ctx, PaginationFirst)
			}
		case DirLast:
			if n.pipeline.CanAddLimit(LimitLast) {
				return n.lookupPipelineLimit(ctx, PaginationLast)
			}
		case DirSample:
			if n.pipeline.CanAddLimit(LimitSample) {
				return n.lookupSample(ctx)
			}
		case DirOrder:
			if n.pipeline.CanAddOrder() {
				return n.lookupOrder(ctx)
			}
		case DirBy:
			if n.pipeline.CanAddFilter() {
				return n.lookupBy(ctx)
			}
		case DirFilter:
			if n.pipeline.CanAddFilter() {
				return n.lookupFilter(ctx)
			}
		}
	} else {
		// Legacy behavior: handle pagination directories
		if name == DirFirst || name == DirLast {
			return n.lookupPagination(ctx, name)
		}
	}

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	// Check if pkValue is one of the matching PKs
	found := false
	for _, pk := range n.matchingPKs {
		if pk == pkValue {
			found = true
			break
		}
	}

	if !found {
		return nil, syscall.ENOENT
	}

	// If name has format extension, create row file node
	if name != pkValue {
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}
		rowNode := NewRowFileNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.pkColumn, pkValue, format)
		child := n.NewPersistentInode(ctx, rowNode, stableAttr)

		// Fill in entry attributes
		var attrOut fuse.AttrOut
		if errno := rowNode.Getattr(ctx, nil, &attrOut); errno != 0 {
			return nil, errno
		}
		out.Attr = attrOut.Attr

		return child, 0
	}

	// No extension, create a row directory node for this PK
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.pkColumn, pkValue, n.partialRows)
	child := n.NewPersistentInode(ctx, rowDirNode, stableAttr)

	return child, 0
}

// lookupExport creates an export node using the pipeline context.
func (n *IndexValueNode) lookupExport(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	exportNode := NewPipelineExportDirNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.pipeline)
	child := n.NewPersistentInode(ctx, exportNode, stableAttr)
	return child, 0
}

// lookupPipelineLimit creates a pagination node using the pipeline context.
func (n *IndexValueNode) lookupPipelineLimit(ctx context.Context, paginationType PaginationType) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	limitNode := NewPaginationNodeWithPipeline(n.cfg, n.db, n.cache, n.schema, n.tableName, paginationType, n.partialRows, n.pipeline)
	child := n.NewPersistentInode(ctx, limitNode, stableAttr)
	return child, 0
}

// lookupSample creates a sample node using the pipeline context.
func (n *IndexValueNode) lookupSample(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	sampleNode := NewSampleNodeWithPipeline(n.cfg, n.db, n.cache, n.schema, n.tableName, n.partialRows, n.pipeline)
	child := n.NewPersistentInode(ctx, sampleNode, stableAttr)
	return child, 0
}

// lookupOrder creates an order node using the pipeline context.
func (n *IndexValueNode) lookupOrder(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	orderNode := NewOrderDirNodeWithPipeline(n.cfg, n.db, n.cache, n.schema, n.tableName, n.partialRows, n.pipeline)
	child := n.NewPersistentInode(ctx, orderNode, stableAttr)
	return child, 0
}

// lookupBy creates a .by/ node using the pipeline context.
func (n *IndexValueNode) lookupBy(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	byNode := NewByDirNodeWithPipeline(n.cfg, n.db, n.cache, n.schema, n.tableName, n.partialRows, n.pipeline)
	child := n.NewPersistentInode(ctx, byNode, stableAttr)
	return child, 0
}

// lookupFilter creates a .filter/ node using the pipeline context.
func (n *IndexValueNode) lookupFilter(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	filterNode := NewFilterDirNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.pipeline, n.partialRows)
	child := n.NewPersistentInode(ctx, filterNode, stableAttr)
	return child, 0
}

// lookupPagination handles .first and .last directory lookups within an index value.
func (n *IndexValueNode) lookupPagination(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	var paginationType PaginationType
	if name == DirFirst {
		paginationType = PaginationFirst
	} else {
		paginationType = PaginationLast
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	pagNode := NewIndexValuePaginationNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.column, n.value, n.pkColumn, paginationType, n.partialRows)
	child := n.NewPersistentInode(ctx, pagNode, stableAttr)

	return child, 0
}

// IndexValuePaginationNode represents the .first/ or .last/ directory within an index value.
type IndexValuePaginationNode struct {
	fs.Inode

	cfg            *config.Config
	db             db.DBClient
	cache          *MetadataCache
	schema         string
	tableName      string
	column         string
	value          string
	pkColumn       string
	paginationType PaginationType
	partialRows    *PartialRowTracker
}

var _ fs.InodeEmbedder = (*IndexValuePaginationNode)(nil)
var _ fs.NodeGetattrer = (*IndexValuePaginationNode)(nil)
var _ fs.NodeReaddirer = (*IndexValuePaginationNode)(nil)
var _ fs.NodeLookuper = (*IndexValuePaginationNode)(nil)

// NewIndexValuePaginationNode creates a new pagination node within an index value.
func NewIndexValuePaginationNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column, value, pkColumn string, paginationType PaginationType, partialRows *PartialRowTracker) *IndexValuePaginationNode {
	return &IndexValuePaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		column:         column,
		value:          value,
		pkColumn:       pkColumn,
		paginationType: paginationType,
		partialRows:    partialRows,
	}
}

// Getattr returns attributes for the index value pagination directory
func (p *IndexValuePaginationNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists the pagination directory (empty - user must specify N)
func (p *IndexValuePaginationNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Lookup looks up a number N to create an IndexValuePaginationLimitNode.
func (p *IndexValuePaginationNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	limit, err := strconv.Atoi(name)
	if err != nil || limit < 1 {
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	limitNode := NewIndexValuePaginationLimitNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.column, p.value, p.pkColumn, p.paginationType, limit, p.partialRows)
	child := p.NewPersistentInode(ctx, limitNode, stableAttr)

	return child, 0
}

// IndexValuePaginationLimitNode represents the .first/N/ or .last/N/ directory within an index value.
// Lists the first or last N rows matching the index value, ordered by PK.
type IndexValuePaginationLimitNode struct {
	fs.Inode

	cfg            *config.Config
	db             db.DBClient
	cache          *MetadataCache
	schema         string
	tableName      string
	column         string
	value          string
	pkColumn       string
	paginationType PaginationType
	limit          int
	partialRows    *PartialRowTracker
}

var _ fs.InodeEmbedder = (*IndexValuePaginationLimitNode)(nil)
var _ fs.NodeGetattrer = (*IndexValuePaginationLimitNode)(nil)
var _ fs.NodeReaddirer = (*IndexValuePaginationLimitNode)(nil)
var _ fs.NodeLookuper = (*IndexValuePaginationLimitNode)(nil)

// NewIndexValuePaginationLimitNode creates a new pagination limit node within an index value.
func NewIndexValuePaginationLimitNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column, value, pkColumn string, paginationType PaginationType, limit int, partialRows *PartialRowTracker) *IndexValuePaginationLimitNode {
	return &IndexValuePaginationLimitNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		column:         column,
		value:          value,
		pkColumn:       pkColumn,
		paginationType: paginationType,
		limit:          limit,
		partialRows:    partialRows,
	}
}

// Getattr returns attributes for the index value pagination limit directory
func (p *IndexValuePaginationLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists the first or last N rows matching the index value, ordered by PK.
func (p *IndexValuePaginationLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexValuePaginationLimitNode.Readdir called",
		zap.String("table", p.tableName),
		zap.String("column", p.column),
		zap.String("value", p.value),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit))

	var pks []string
	var err error
	if p.paginationType == PaginationFirst {
		pks, err = p.db.GetRowsByIndexValueOrdered(ctx, p.schema, p.tableName, p.column, p.value, p.pkColumn, p.limit, true)
	} else {
		pks, err = p.db.GetRowsByIndexValueOrdered(ctx, p.schema, p.tableName, p.column, p.value, p.pkColumn, p.limit, false)
	}

	if err != nil {
		logging.Error("Failed to get ordered rows by index value",
			zap.String("table", p.tableName),
			zap.String("column", p.column),
			zap.String("value", p.value),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(pks))
	for _, pk := range pks {
		entries = append(entries, fuse.DirEntry{
			Name: pk,
			Mode: syscall.S_IFREG,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles looking up a row within the paginated index value results.
func (p *IndexValuePaginationLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	pkValue, format := util.ParseRowFilename(name)

	// Verify row exists
	_, err := p.db.GetRow(ctx, p.schema, p.tableName, p.pkColumn, pkValue)
	if err != nil {
		return nil, syscall.ENOENT
	}

	if name != pkValue {
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}
		rowNode := NewRowFileNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.pkColumn, pkValue, format)
		child := p.NewPersistentInode(ctx, rowNode, stableAttr)

		var attrOut fuse.AttrOut
		if errno := rowNode.Getattr(ctx, nil, &attrOut); errno != 0 {
			return nil, errno
		}
		out.Attr = attrOut.Attr

		return child, 0
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}
	rowDirNode := NewRowDirectoryNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.pkColumn, pkValue, p.partialRows)
	child := p.NewPersistentInode(ctx, rowDirNode, stableAttr)

	return child, 0
}

// CompositeIndexNode represents a composite index directory (e.g., .last_name.first_name/).
// Lists distinct values for the first column in the composite index.
//
// Navigation through composite indexes works level-by-level:
//
//	.last_name.first_name/           -> distinct last_name values
//	.last_name.first_name/Smith/     -> distinct first_name values WHERE last_name='Smith'
//	.last_name.first_name/Smith/John -> rows WHERE last_name='Smith' AND first_name='John'
type CompositeIndexNode struct {
	fs.Inode

	// cfg holds filesystem configuration including max listing limits
	cfg *config.Config

	// db is the database client for queries
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name (e.g., "public")
	schema string

	// tableName is the table this index belongs to
	tableName string

	// columns contains all column names in the composite index, in order
	columns []string

	// index contains the full index metadata from PostgreSQL
	index *db.Index

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*CompositeIndexNode)(nil)
var _ fs.NodeGetattrer = (*CompositeIndexNode)(nil)
var _ fs.NodeReaddirer = (*CompositeIndexNode)(nil)
var _ fs.NodeLookuper = (*CompositeIndexNode)(nil)

// NewCompositeIndexNode creates a new composite index directory node.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - columns: All column names in the composite index (in index order)
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewCompositeIndexNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, columns []string, index *db.Index, partialRows *PartialRowTracker) *CompositeIndexNode {
	return &CompositeIndexNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		columns:     columns,
		index:       index,
		partialRows: partialRows,
	}
}

// Getattr returns directory attributes for the composite index node.
func (n *CompositeIndexNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("CompositeIndexNode.Getattr called",
		zap.String("table", n.tableName),
		zap.Strings("columns", n.columns))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists distinct values for the first column in the composite index.
func (n *CompositeIndexNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("CompositeIndexNode.Readdir called",
		zap.String("table", n.tableName),
		zap.Strings("columns", n.columns))

	const maxDistinctValues = 100

	// Get distinct values for the first column (no filter conditions yet)
	values, err := n.db.GetDistinctValues(ctx, n.schema, n.tableName, n.columns[0], maxDistinctValues)
	if err != nil {
		logging.Error("Failed to get distinct values for composite index",
			zap.String("table", n.tableName),
			zap.String("column", n.columns[0]),
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

	logging.Debug("Composite index directory listing",
		zap.String("table", n.tableName),
		zap.String("column", n.columns[0]),
		zap.Int("value_count", len(values)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a value within the composite index.
// Creates a CompositeIndexLevelNode for the next level of navigation.
func (n *CompositeIndexNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("CompositeIndexNode.Lookup called",
		zap.String("table", n.tableName),
		zap.Strings("columns", n.columns),
		zap.String("value", name))

	// Create level node with this value as the first filter
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	levelNode := NewCompositeIndexLevelNode(
		n.cfg, n.db, n.cache, n.schema, n.tableName,
		n.columns,
		[]string{name}, // First value in the filter chain
		n.index,
		n.partialRows,
	)
	child := n.NewPersistentInode(ctx, levelNode, stableAttr)

	return child, 0
}

// CompositeIndexLevelNode represents an intermediate level in composite index navigation.
// Tracks which column values have been specified so far and either:
//   - Shows distinct values for the next column (if more columns remain)
//   - Shows matching row PKs (if all columns have values)
type CompositeIndexLevelNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client
	db db.DBClient

	// cache holds metadata cache for permission lookups
	cache *MetadataCache

	// schema is the PostgreSQL schema name
	schema string

	// tableName is the table name
	tableName string

	// columns contains all column names in the composite index
	columns []string

	// values contains the values specified so far (same length as depth in navigation)
	values []string

	// index contains the full index metadata
	index *db.Index

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*CompositeIndexLevelNode)(nil)
var _ fs.NodeGetattrer = (*CompositeIndexLevelNode)(nil)
var _ fs.NodeReaddirer = (*CompositeIndexLevelNode)(nil)
var _ fs.NodeLookuper = (*CompositeIndexLevelNode)(nil)

// NewCompositeIndexLevelNode creates a node for an intermediate composite index level.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client (accepts db.DBClient interface)
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - columns: All column names in the composite index
//   - values: Values specified so far (for columns[0] through columns[len(values)-1])
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewCompositeIndexLevelNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, columns, values []string, index *db.Index, partialRows *PartialRowTracker) *CompositeIndexLevelNode {
	return &CompositeIndexLevelNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		columns:     columns,
		values:      values,
		index:       index,
		partialRows: partialRows,
	}
}

// Getattr returns directory attributes for the composite index level node.
func (n *CompositeIndexLevelNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("CompositeIndexLevelNode.Getattr called",
		zap.String("table", n.tableName),
		zap.Strings("columns", n.columns),
		zap.Strings("values", n.values))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists either distinct values for the next column or matching row PKs.
// If all columns have values, lists matching PKs. Otherwise lists distinct values
// for the next column filtered by the values specified so far.
func (n *CompositeIndexLevelNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("CompositeIndexLevelNode.Readdir called",
		zap.String("table", n.tableName),
		zap.Strings("columns", n.columns),
		zap.Strings("values", n.values),
		zap.Int("depth", len(n.values)))

	const maxResults = 100

	// Check if all columns have values - if so, show matching PKs
	if len(n.values) >= len(n.columns) {
		return n.readdirFinalLevel(ctx, maxResults)
	}

	// More columns remain - show distinct values for the next column
	return n.readdirIntermediateLevel(ctx, maxResults)
}

// readdirIntermediateLevel lists distinct values for the next column in the index.
func (n *CompositeIndexLevelNode) readdirIntermediateLevel(ctx context.Context, limit int) (fs.DirStream, syscall.Errno) {
	nextColumn := n.columns[len(n.values)]
	filterColumns := n.columns[:len(n.values)]

	values, err := n.db.GetDistinctValuesFiltered(ctx, n.schema, n.tableName, nextColumn, filterColumns, n.values, limit)
	if err != nil {
		logging.Error("Failed to get filtered distinct values",
			zap.String("table", n.tableName),
			zap.String("next_column", nextColumn),
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

	logging.Debug("Composite index intermediate listing",
		zap.String("table", n.tableName),
		zap.String("next_column", nextColumn),
		zap.Int("value_count", len(values)))

	return fs.NewListDirStream(entries), 0
}

// readdirFinalLevel lists PKs of rows matching all column conditions.
func (n *CompositeIndexLevelNode) readdirFinalLevel(ctx context.Context, limit int) (fs.DirStream, syscall.Errno) {
	pk, err := n.db.GetPrimaryKey(ctx, n.schema, n.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", n.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	pks, err := n.db.GetRowsByCompositeIndex(ctx, n.schema, n.tableName, n.columns, n.values, pkColumn, limit)
	if err != nil {
		logging.Error("Failed to get rows by composite index",
			zap.String("table", n.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(pks))
	for _, pkVal := range pks {
		entries = append(entries, fuse.DirEntry{
			Name: pkVal,
			Mode: syscall.S_IFREG,
		})
	}

	logging.Debug("Composite index final listing",
		zap.String("table", n.tableName),
		zap.Int("pk_count", len(pks)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to either the next level or a specific row.
func (n *CompositeIndexLevelNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("CompositeIndexLevelNode.Lookup called",
		zap.String("table", n.tableName),
		zap.Strings("columns", n.columns),
		zap.Strings("values", n.values),
		zap.String("name", name))

	// Check if all columns have values - if so, looking up a row PK
	if len(n.values) >= len(n.columns) {
		return n.lookupRow(ctx, name)
	}

	// More columns remain - create next level node
	return n.lookupNextLevel(ctx, name)
}

// lookupNextLevel creates a CompositeIndexLevelNode for the next navigation level.
func (n *CompositeIndexLevelNode) lookupNextLevel(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	newValues := make([]string, len(n.values)+1)
	copy(newValues, n.values)
	newValues[len(n.values)] = name

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	levelNode := NewCompositeIndexLevelNode(
		n.cfg, n.db, n.cache, n.schema, n.tableName,
		n.columns, newValues, n.index, n.partialRows,
	)
	child := n.NewPersistentInode(ctx, levelNode, stableAttr)

	return child, 0
}

// lookupRow handles looking up a specific row PK at the final navigation level.
// Supports both bare PKs and PKs with format extensions (e.g., "123.json").
func (n *CompositeIndexLevelNode) lookupRow(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	pk, err := n.db.GetPrimaryKey(ctx, n.schema, n.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", n.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	// Verify row exists
	_, err = n.db.GetRow(ctx, n.schema, n.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found via composite index",
			zap.String("table", n.tableName),
			zap.String("pk", pkValue))
		return nil, syscall.ENOENT
	}

	// If name has format extension, create row file node
	if name != pkValue {
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}
		rowNode := NewRowFileNode(n.cfg, n.db, n.cache, n.schema, n.tableName, pkColumn, pkValue, format)
		child := n.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}
	rowDirNode := NewRowDirectoryNode(n.cfg, n.db, n.cache, n.schema, n.tableName, pkColumn, pkValue, n.partialRows)
	child := n.NewPersistentInode(ctx, rowDirNode, stableAttr)

	return child, 0
}

// FormatCompositeIndexName creates the dotfile directory name for a composite index.
// For example, columns ["last_name", "first_name"] becomes ".last_name.first_name".
func FormatCompositeIndexName(columns []string) string {
	return "." + strings.Join(columns, ".")
}

// ParseCompositeIndexName extracts column names from a composite index directory name.
// For example, ".last_name.first_name" returns ["last_name", "first_name"].
// Returns nil if the name doesn't look like a composite index (no dots after the leading dot).
func ParseCompositeIndexName(name string) []string {
	if !strings.HasPrefix(name, ".") || len(name) < 2 {
		return nil
	}
	// Remove leading dot and split by dots
	parts := strings.Split(name[1:], ".")
	if len(parts) < 2 {
		return nil // Single-column index, not composite
	}
	return parts
}

// IndexPaginationNode represents the .first/ or .last/ directory within an index.
// When a number is looked up, it creates an IndexPaginationLimitNode.
type IndexPaginationNode struct {
	fs.Inode

	cfg            *config.Config
	db             db.DBClient
	cache          *MetadataCache
	schema         string
	tableName      string
	column         string
	paginationType PaginationType
	partialRows    *PartialRowTracker
}

var _ fs.InodeEmbedder = (*IndexPaginationNode)(nil)
var _ fs.NodeGetattrer = (*IndexPaginationNode)(nil)
var _ fs.NodeReaddirer = (*IndexPaginationNode)(nil)
var _ fs.NodeLookuper = (*IndexPaginationNode)(nil)

// NewIndexPaginationNode creates a new .first/ or .last/ directory node within an index.
func NewIndexPaginationNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column string, paginationType PaginationType, partialRows *PartialRowTracker) *IndexPaginationNode {
	return &IndexPaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		column:         column,
		paginationType: paginationType,
		partialRows:    partialRows,
	}
}

// Getattr returns attributes for the index pagination directory
func (p *IndexPaginationNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists the pagination directory (empty - user must specify N)
func (p *IndexPaginationNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Lookup looks up a number N to create an IndexPaginationLimitNode.
func (p *IndexPaginationNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	limit, err := strconv.Atoi(name)
	if err != nil || limit < 1 {
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	limitNode := NewIndexPaginationLimitNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.column, p.paginationType, limit, p.partialRows)
	child := p.NewPersistentInode(ctx, limitNode, stableAttr)

	return child, 0
}

// IndexPaginationLimitNode represents the .first/N/ or .last/N/ directory within an index.
// Lists the first or last N distinct values for the indexed column.
type IndexPaginationLimitNode struct {
	fs.Inode

	cfg            *config.Config
	db             db.DBClient
	cache          *MetadataCache
	schema         string
	tableName      string
	column         string
	paginationType PaginationType
	limit          int
	partialRows    *PartialRowTracker
}

var _ fs.InodeEmbedder = (*IndexPaginationLimitNode)(nil)
var _ fs.NodeGetattrer = (*IndexPaginationLimitNode)(nil)
var _ fs.NodeReaddirer = (*IndexPaginationLimitNode)(nil)
var _ fs.NodeLookuper = (*IndexPaginationLimitNode)(nil)

// NewIndexPaginationLimitNode creates a new .first/N/ or .last/N/ directory node within an index.
func NewIndexPaginationLimitNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName, column string, paginationType PaginationType, limit int, partialRows *PartialRowTracker) *IndexPaginationLimitNode {
	return &IndexPaginationLimitNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		column:         column,
		paginationType: paginationType,
		limit:          limit,
		partialRows:    partialRows,
	}
}

// Getattr returns attributes for the index pagination limit directory
func (p *IndexPaginationLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists the first or last N distinct values for the indexed column.
func (p *IndexPaginationLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexPaginationLimitNode.Readdir called",
		zap.String("table", p.tableName),
		zap.String("column", p.column),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit))

	var values []string
	var err error
	if p.paginationType == PaginationFirst {
		values, err = p.db.GetDistinctValuesOrdered(ctx, p.schema, p.tableName, p.column, p.limit, true)
	} else {
		values, err = p.db.GetDistinctValuesOrdered(ctx, p.schema, p.tableName, p.column, p.limit, false)
	}

	if err != nil {
		logging.Error("Failed to get ordered distinct values",
			zap.String("table", p.tableName),
			zap.String("column", p.column),
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

// Lookup handles looking up a value within the paginated index results.
func (p *IndexPaginationLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexPaginationLimitNode.Lookup called",
		zap.String("table", p.tableName),
		zap.String("column", p.column),
		zap.String("value", name))

	// Query for rows with this value
	pk, err := p.db.GetPrimaryKey(ctx, p.schema, p.tableName)
	if err != nil {
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	rows, err := p.db.GetRowsByIndexValue(ctx, p.schema, p.tableName, p.column, name, pkColumn, 100)
	if err != nil {
		return nil, syscall.EIO
	}

	if len(rows) == 0 {
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	valueNode := NewIndexValueNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.column, name, pkColumn, rows, p.partialRows)
	child := p.NewPersistentInode(ctx, valueNode, stableAttr)

	return child, 0
}
