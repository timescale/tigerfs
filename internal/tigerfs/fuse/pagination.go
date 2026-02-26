package fuse

import (
	"context"
	"strconv"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// PaginationType represents the type of pagination (first or last)
type PaginationType string

const (
	PaginationFirst PaginationType = "first"
	PaginationLast  PaginationType = "last"
)

// PaginationNode represents the .first/ or .last/ directory.
// When listed, it shows nothing (user must specify N).
// When a number is looked up, it creates a PaginationLimitNode.
type PaginationNode struct {
	fs.Inode

	cfg            *config.Config         // TigerFS configuration
	db             db.DBClient            // Database client for queries
	cache          *tigerfs.MetadataCache // Metadata cache for permissions lookup
	schema         string                 // PostgreSQL schema name
	tableName      string                 // Table this pagination belongs to
	paginationType PaginationType         // Whether this is .first or .last
	partialRows    *PartialRowTracker     // Tracker for partial row writes

	// pipeline is the current pipeline context (may be nil for table root)
	pipeline *PipelineContext
}

var _ fs.InodeEmbedder = (*PaginationNode)(nil)
var _ fs.NodeGetattrer = (*PaginationNode)(nil)
var _ fs.NodeReaddirer = (*PaginationNode)(nil)
var _ fs.NodeLookuper = (*PaginationNode)(nil)

// NewPaginationNode creates a new .first/ or .last/ directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table
//   - paginationType: PaginationFirst or PaginationLast
//   - partialRows: Tracker for partial row writes
func NewPaginationNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, paginationType PaginationType, partialRows *PartialRowTracker) *PaginationNode {
	return &PaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		paginationType: paginationType,
		partialRows:    partialRows,
		pipeline:       nil, // No pipeline context at table root
	}
}

// NewPaginationNodeWithPipeline creates a new pagination node with pipeline context.
// Used when .first/ or .last/ appears within a pipeline path.
func NewPaginationNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, paginationType PaginationType, partialRows *PartialRowTracker, pipeline *PipelineContext) *PaginationNode {
	return &PaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		paginationType: paginationType,
		partialRows:    partialRows,
		pipeline:       pipeline,
	}
}

// Getattr returns attributes for the pagination directory
func (p *PaginationNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("PaginationNode.Getattr called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the pagination directory (empty - user must specify N)
func (p *PaginationNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("PaginationNode.Readdir called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)))

	// Return empty directory - user must specify a limit like .first/100/
	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Lookup looks up a number N to create a PaginationLimitNode.
// Returns ENOENT if the name is not a valid positive integer.
func (p *PaginationNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("PaginationNode.Lookup called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.String("name", name))

	// Parse the limit from the name
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

	limitNode := NewPaginationLimitNodeWithPipeline(p.cfg, p.db, p.cache, p.schema, p.tableName, p.paginationType, limit, p.partialRows, p.pipeline)
	child := p.NewPersistentInode(ctx, limitNode, stableAttr)

	logging.Debug("Created pagination limit node",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", limit))

	return child, 0
}

// PaginationLimitNode represents the .first/N/ or .last/N/ directory.
// Lists the first or last N rows from the table ordered by primary key.
// When pipeline context is present, also exposes pipeline capabilities.
type PaginationLimitNode struct {
	fs.Inode

	cfg            *config.Config         // TigerFS configuration
	db             db.DBClient            // Database client for queries
	cache          *tigerfs.MetadataCache // Metadata cache for permissions lookup
	schema         string                 // PostgreSQL schema name
	tableName      string                 // Table this pagination belongs to
	paginationType PaginationType         // Whether this is .first or .last
	limit          int                    // Maximum number of rows to return
	partialRows    *PartialRowTracker     // Tracker for partial row writes

	// pipeline is the current pipeline context with this limit applied
	pipeline *PipelineContext
}

var _ fs.InodeEmbedder = (*PaginationLimitNode)(nil)
var _ fs.NodeGetattrer = (*PaginationLimitNode)(nil)
var _ fs.NodeReaddirer = (*PaginationLimitNode)(nil)
var _ fs.NodeLookuper = (*PaginationLimitNode)(nil)

// NewPaginationLimitNode creates a new .first/N/ or .last/N/ directory node.
// For backward compatibility, this creates a node without pipeline context.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table
//   - paginationType: PaginationFirst or PaginationLast
//   - limit: Maximum number of rows to list
//   - partialRows: Tracker for partial row writes
func NewPaginationLimitNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, paginationType PaginationType, limit int, partialRows *PartialRowTracker) *PaginationLimitNode {
	return NewPaginationLimitNodeWithPipeline(cfg, dbClient, cache, schema, tableName, paginationType, limit, partialRows, nil)
}

// NewPaginationLimitNodeWithPipeline creates a new pagination limit node with pipeline context.
// When basePipeline is provided, the limit is applied to create the node's pipeline.
func NewPaginationLimitNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, paginationType PaginationType, limit int, partialRows *PartialRowTracker, basePipeline *PipelineContext) *PaginationLimitNode {
	var pipeline *PipelineContext

	// Apply limit to pipeline if context exists
	if basePipeline != nil {
		limitType := LimitFirst
		if paginationType == PaginationLast {
			limitType = LimitLast
		}
		pipeline = basePipeline.WithLimit(limit, limitType)
	}

	return &PaginationLimitNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		paginationType: paginationType,
		limit:          limit,
		partialRows:    partialRows,
		pipeline:       pipeline,
	}
}

// Getattr returns attributes for the pagination limit directory
func (p *PaginationLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("PaginationLimitNode.Getattr called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the first or last N rows ordered by primary key.
// When pipeline context is present, also lists available capabilities.
// Returns EIO if the database query fails.
func (p *PaginationLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("PaginationLimitNode.Readdir called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit),
		zap.Bool("has_pipeline", p.pipeline != nil))

	// Get primary key for table
	pk, err := p.db.GetPrimaryKey(ctx, p.schema, p.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", p.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Get rows based on pagination type
	var rows []string
	if p.pipeline != nil {
		// Use pipeline query
		rows, err = p.db.QueryRowsPipeline(ctx, p.pipeline.ToQueryParams())
	} else {
		// Legacy behavior: direct query
		if p.paginationType == PaginationFirst {
			rows, err = p.db.GetFirstNRows(ctx, p.schema, p.tableName, pkColumn, p.limit)
		} else {
			rows, err = p.db.GetLastNRows(ctx, p.schema, p.tableName, pkColumn, p.limit)
		}
	}

	if err != nil {
		logging.Error("Failed to get paginated rows",
			zap.String("table", p.tableName),
			zap.String("type", string(p.paginationType)),
			zap.Int("limit", p.limit),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Build entries: capabilities first (if pipeline), then rows
	var entries []fuse.DirEntry

	// Add pipeline capabilities if context exists
	if p.pipeline != nil {
		caps := p.pipeline.AvailableCapabilities()
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

	logging.Debug("Pagination directory listing",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit),
		zap.Int("row_count", len(rows)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row or capability within the paginated results.
// Returns ENOENT if the row doesn't exist, EIO on database errors.
func (p *PaginationLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("PaginationLimitNode.Lookup called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit),
		zap.String("name", name))

	// Check for pipeline capabilities first
	if p.pipeline != nil {
		switch name {
		case DirExport:
			if p.pipeline.CanExport() {
				return p.lookupExport(ctx)
			}
		case DirFirst:
			if p.pipeline.CanAddLimit(LimitFirst) {
				return p.lookupNestedLimit(ctx, PaginationFirst)
			}
		case DirLast:
			if p.pipeline.CanAddLimit(LimitLast) {
				return p.lookupNestedLimit(ctx, PaginationLast)
			}
		case DirSample:
			if p.pipeline.CanAddLimit(LimitSample) {
				return p.lookupSample(ctx)
			}
		case DirOrder:
			if p.pipeline.CanAddOrder() {
				return p.lookupOrder(ctx)
			}
		case DirBy:
			if p.pipeline.CanAddFilter() {
				return p.lookupBy(ctx)
			}
		case DirFilter:
			if p.pipeline.CanAddFilter() {
				return p.lookupFilter(ctx)
			}
		}
	}

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	// Get primary key for table
	pk, err := p.db.GetPrimaryKey(ctx, p.schema, p.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", p.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row exists
	_, err = p.db.GetRow(ctx, p.schema, p.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", p.tableName),
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

		rowNode := NewRowFileNode(p.cfg, p.db, p.cache, p.schema, p.tableName, pkColumn, pkValue, format)
		child := p.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(p.cfg, p.db, p.cache, p.schema, p.tableName, pkColumn, pkValue, p.partialRows)
	child := p.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// lookupExport creates an export node using the pipeline context.
func (p *PaginationLimitNode) lookupExport(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	exportNode := NewPipelineExportDirNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.pipeline)
	child := p.NewPersistentInode(ctx, exportNode, stableAttr)
	return child, 0
}

// lookupNestedLimit creates a nested pagination node for .first/ or .last/.
func (p *PaginationLimitNode) lookupNestedLimit(ctx context.Context, paginationType PaginationType) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	limitNode := NewPaginationNodeWithPipeline(p.cfg, p.db, p.cache, p.schema, p.tableName, paginationType, p.partialRows, p.pipeline)
	child := p.NewPersistentInode(ctx, limitNode, stableAttr)
	return child, 0
}

// lookupSample creates a sample node using the pipeline context.
func (p *PaginationLimitNode) lookupSample(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	sampleNode := NewSampleNodeWithPipeline(p.cfg, p.db, p.cache, p.schema, p.tableName, p.partialRows, p.pipeline)
	child := p.NewPersistentInode(ctx, sampleNode, stableAttr)
	return child, 0
}

// lookupOrder creates an order node using the pipeline context.
func (p *PaginationLimitNode) lookupOrder(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	orderNode := NewOrderDirNodeWithPipeline(p.cfg, p.db, p.cache, p.schema, p.tableName, p.partialRows, p.pipeline)
	child := p.NewPersistentInode(ctx, orderNode, stableAttr)
	return child, 0
}

// lookupBy creates a .by/ node using the pipeline context.
func (p *PaginationLimitNode) lookupBy(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	byNode := NewByDirNodeWithPipeline(p.cfg, p.db, p.cache, p.schema, p.tableName, p.partialRows, p.pipeline)
	child := p.NewPersistentInode(ctx, byNode, stableAttr)
	return child, 0
}

// lookupFilter creates a .filter/ node using the pipeline context.
func (p *PaginationLimitNode) lookupFilter(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	filterNode := NewFilterDirNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.pipeline, p.partialRows)
	child := p.NewPersistentInode(ctx, filterNode, stableAttr)
	return child, 0
}
