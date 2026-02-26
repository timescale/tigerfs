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

// SampleNode represents the .sample/ directory.
// When listed, it shows nothing (user must specify N).
// When a number is looked up, it creates a SampleLimitNode that returns
// approximately N random rows using PostgreSQL's TABLESAMPLE.
type SampleNode struct {
	fs.Inode

	cfg         *config.Config         // TigerFS configuration
	db          db.DBClient            // Database client for queries
	cache       *tigerfs.MetadataCache // Cache for row count estimates
	schema      string                 // PostgreSQL schema name
	tableName   string                 // Table this sample belongs to
	partialRows *PartialRowTracker     // Tracker for partial row writes
	pipeline    *PipelineContext       // Pipeline context for capability chaining (may be nil)
}

var _ fs.InodeEmbedder = (*SampleNode)(nil)
var _ fs.NodeGetattrer = (*SampleNode)(nil)
var _ fs.NodeReaddirer = (*SampleNode)(nil)
var _ fs.NodeLookuper = (*SampleNode)(nil)

// NewSampleNode creates a new .sample/ directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - cache: Metadata cache for row count estimates (used for TABLESAMPLE optimization)
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table
//   - partialRows: Tracker for partial row writes
func NewSampleNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, partialRows *PartialRowTracker) *SampleNode {
	return &SampleNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
	}
}

// NewSampleNodeWithPipeline creates a new .sample/ directory node with pipeline context.
// The pipeline context is passed through to child nodes for capability chaining.
func NewSampleNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, partialRows *PartialRowTracker, pipeline *PipelineContext) *SampleNode {
	return &SampleNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
		pipeline:    pipeline,
	}
}

// Getattr returns attributes for the sample directory.
func (s *SampleNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SampleNode.Getattr called", zap.String("table", s.tableName))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the sample directory (empty - user must specify N).
func (s *SampleNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("SampleNode.Readdir called", zap.String("table", s.tableName))

	// Return empty directory - user must specify a sample size like .sample/100/
	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Lookup looks up a number N to create a SampleLimitNode.
// Returns ENOENT if the name is not a valid positive integer.
func (s *SampleNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("SampleNode.Lookup called",
		zap.String("table", s.tableName),
		zap.String("name", name))

	// Parse the sample size from the name
	sampleSize, err := strconv.Atoi(name)
	if err != nil || sampleSize < 1 {
		logging.Debug("Invalid sample size",
			zap.String("name", name),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	limitNode := NewSampleLimitNodeWithPipeline(s.cfg, s.db, s.cache, s.schema, s.tableName, sampleSize, s.partialRows, s.pipeline)
	child := s.NewPersistentInode(ctx, limitNode, stableAttr)

	logging.Debug("Created sample limit node",
		zap.String("table", s.tableName),
		zap.Int("sample_size", sampleSize))

	return child, 0
}

// SampleLimitNode represents the .sample/N/ directory.
// Lists approximately N random rows from the table using TABLESAMPLE BERNOULLI
// for large tables or ORDER BY RANDOM() for small tables.
// When pipeline context is present, also exposes pipeline capabilities.
type SampleLimitNode struct {
	fs.Inode

	cfg         *config.Config         // TigerFS configuration
	db          db.DBClient            // Database client for queries
	cache       *tigerfs.MetadataCache // Cache for row count estimates
	schema      string                 // PostgreSQL schema name
	tableName   string                 // Table this sample belongs to
	sampleSize  int                    // Target number of random rows to return
	partialRows *PartialRowTracker     // Tracker for partial row writes
	pipeline    *PipelineContext       // Pipeline context for capability chaining (may be nil)
}

var _ fs.InodeEmbedder = (*SampleLimitNode)(nil)
var _ fs.NodeGetattrer = (*SampleLimitNode)(nil)
var _ fs.NodeReaddirer = (*SampleLimitNode)(nil)
var _ fs.NodeLookuper = (*SampleLimitNode)(nil)

// NewSampleLimitNode creates a new .sample/N/ directory node.
// For backward compatibility, this creates a node without pipeline context.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - cache: Metadata cache for row count estimates
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table
//   - sampleSize: Target number of random rows to return
//   - partialRows: Tracker for partial row writes
func NewSampleLimitNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, sampleSize int, partialRows *PartialRowTracker) *SampleLimitNode {
	return NewSampleLimitNodeWithPipeline(cfg, dbClient, cache, schema, tableName, sampleSize, partialRows, nil)
}

// NewSampleLimitNodeWithPipeline creates a new sample limit node with pipeline context.
// When basePipeline is provided, the sample limit is applied to create the node's pipeline.
func NewSampleLimitNodeWithPipeline(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string, sampleSize int, partialRows *PartialRowTracker, basePipeline *PipelineContext) *SampleLimitNode {
	var pipeline *PipelineContext

	// Apply sample limit to pipeline if context exists
	if basePipeline != nil {
		pipeline = basePipeline.WithLimit(sampleSize, LimitSample)
	}

	return &SampleLimitNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		sampleSize:  sampleSize,
		partialRows: partialRows,
		pipeline:    pipeline,
	}
}

// Getattr returns attributes for the sample limit directory.
func (s *SampleLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SampleLimitNode.Getattr called",
		zap.String("table", s.tableName),
		zap.Int("sample_size", s.sampleSize))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists approximately N random rows using TABLESAMPLE or ORDER BY RANDOM().
// When pipeline context is present, also lists available capabilities.
// Returns EIO if the database query fails.
// Note: The actual count may vary from sampleSize due to the probabilistic
// nature of TABLESAMPLE BERNOULLI.
func (s *SampleLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("SampleLimitNode.Readdir called",
		zap.String("table", s.tableName),
		zap.Int("sample_size", s.sampleSize),
		zap.Bool("has_pipeline", s.pipeline != nil))

	// Get primary key for table
	pk, err := s.db.GetPrimaryKey(ctx, s.schema, s.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", s.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Get rows - use pipeline query if context exists
	var rows []string
	if s.pipeline != nil {
		rows, err = s.db.QueryRowsPipeline(ctx, s.pipeline.ToQueryParams())
	} else {
		// Legacy behavior: direct query
		var estimatedRows int64 = -1
		if s.cache != nil {
			estimate, err := s.cache.GetRowCountEstimate(ctx, s.tableName)
			if err != nil {
				logging.Warn("Failed to get row count estimate for sampling",
					zap.String("table", s.tableName),
					zap.Error(err))
			} else {
				estimatedRows = estimate
			}
		}
		rows, err = s.db.GetRandomSampleRows(ctx, s.schema, s.tableName, pkColumn, s.sampleSize, estimatedRows)
	}

	if err != nil {
		logging.Error("Failed to get random sample",
			zap.String("table", s.tableName),
			zap.Int("sample_size", s.sampleSize),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Build entries: capabilities first (if pipeline), then rows
	var entries []fuse.DirEntry

	// Add pipeline capabilities if context exists
	if s.pipeline != nil {
		caps := s.pipeline.AvailableCapabilities()
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

	logging.Debug("Sample directory listing",
		zap.String("table", s.tableName),
		zap.Int("requested", s.sampleSize),
		zap.Int("returned", len(rows)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row or capability within the sampled results.
// Returns ENOENT if the row doesn't exist, EIO on database errors.
func (s *SampleLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("SampleLimitNode.Lookup called",
		zap.String("table", s.tableName),
		zap.Int("sample_size", s.sampleSize),
		zap.String("name", name))

	// Check for pipeline capabilities first
	if s.pipeline != nil {
		switch name {
		case DirExport:
			if s.pipeline.CanExport() {
				return s.lookupExport(ctx)
			}
		case DirOrder:
			if s.pipeline.CanAddOrder() {
				return s.lookupOrder(ctx)
			}
		case DirBy:
			if s.pipeline.CanAddFilter() {
				return s.lookupBy(ctx)
			}
		case DirFilter:
			if s.pipeline.CanAddFilter() {
				return s.lookupFilter(ctx)
			}
		}
	}

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	// Get primary key for table
	pk, err := s.db.GetPrimaryKey(ctx, s.schema, s.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", s.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row exists
	_, err = s.db.GetRow(ctx, s.schema, s.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", s.tableName),
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

		rowNode := NewRowFileNode(s.cfg, s.db, s.cache, s.schema, s.tableName, pkColumn, pkValue, format)
		child := s.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(s.cfg, s.db, s.cache, s.schema, s.tableName, pkColumn, pkValue, s.partialRows)
	child := s.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// lookupExport creates an export node using the pipeline context.
func (s *SampleLimitNode) lookupExport(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	exportNode := NewPipelineExportDirNode(s.cfg, s.db, s.cache, s.schema, s.tableName, s.pipeline)
	child := s.NewPersistentInode(ctx, exportNode, stableAttr)
	return child, 0
}

// lookupOrder creates an order node using the pipeline context.
func (s *SampleLimitNode) lookupOrder(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	orderNode := NewOrderDirNodeWithPipeline(s.cfg, s.db, s.cache, s.schema, s.tableName, s.partialRows, s.pipeline)
	child := s.NewPersistentInode(ctx, orderNode, stableAttr)
	return child, 0
}

// lookupBy creates a .by/ node using the pipeline context.
func (s *SampleLimitNode) lookupBy(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	byNode := NewByDirNodeWithPipeline(s.cfg, s.db, s.cache, s.schema, s.tableName, s.partialRows, s.pipeline)
	child := s.NewPersistentInode(ctx, byNode, stableAttr)
	return child, 0
}

// lookupFilter creates a .filter/ node using the pipeline context.
func (s *SampleLimitNode) lookupFilter(ctx context.Context) (*fs.Inode, syscall.Errno) {
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	filterNode := NewFilterDirNode(s.cfg, s.db, s.cache, s.schema, s.tableName, s.pipeline, s.partialRows)
	child := s.NewPersistentInode(ctx, filterNode, stableAttr)
	return child, 0
}
