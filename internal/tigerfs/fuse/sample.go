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

// SampleNode represents the .sample/ directory.
// When listed, it shows nothing (user must specify N).
// When a number is looked up, it creates a SampleLimitNode that returns
// approximately N random rows using PostgreSQL's TABLESAMPLE.
type SampleNode struct {
	fs.Inode

	cfg         *config.Config     // TigerFS configuration
	db          *db.Client         // Database client for queries
	cache       *MetadataCache     // Cache for row count estimates
	schema      string             // PostgreSQL schema name
	tableName   string             // Table this sample belongs to
	partialRows *PartialRowTracker // Tracker for partial row writes
}

var _ fs.InodeEmbedder = (*SampleNode)(nil)
var _ fs.NodeGetattrer = (*SampleNode)(nil)
var _ fs.NodeReaddirer = (*SampleNode)(nil)
var _ fs.NodeLookuper = (*SampleNode)(nil)

// NewSampleNode creates a new .sample/ directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for row count estimates (used for TABLESAMPLE optimization)
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table
//   - partialRows: Tracker for partial row writes
func NewSampleNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker) *SampleNode {
	return &SampleNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the sample directory.
func (s *SampleNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SampleNode.Getattr called", zap.String("table", s.tableName))

	out.Mode = 0755 | syscall.S_IFDIR
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

	limitNode := NewSampleLimitNode(s.cfg, s.db, s.cache, s.schema, s.tableName, sampleSize, s.partialRows)
	child := s.NewPersistentInode(ctx, limitNode, stableAttr)

	logging.Debug("Created sample limit node",
		zap.String("table", s.tableName),
		zap.Int("sample_size", sampleSize))

	return child, 0
}

// SampleLimitNode represents the .sample/N/ directory.
// Lists approximately N random rows from the table using TABLESAMPLE BERNOULLI
// for large tables or ORDER BY RANDOM() for small tables.
type SampleLimitNode struct {
	fs.Inode

	cfg         *config.Config     // TigerFS configuration
	db          *db.Client         // Database client for queries
	cache       *MetadataCache     // Cache for row count estimates
	schema      string             // PostgreSQL schema name
	tableName   string             // Table this sample belongs to
	sampleSize  int                // Target number of random rows to return
	partialRows *PartialRowTracker // Tracker for partial row writes
}

var _ fs.InodeEmbedder = (*SampleLimitNode)(nil)
var _ fs.NodeGetattrer = (*SampleLimitNode)(nil)
var _ fs.NodeReaddirer = (*SampleLimitNode)(nil)
var _ fs.NodeLookuper = (*SampleLimitNode)(nil)

// NewSampleLimitNode creates a new .sample/N/ directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for row count estimates
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table
//   - sampleSize: Target number of random rows to return
//   - partialRows: Tracker for partial row writes
func NewSampleLimitNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName string, sampleSize int, partialRows *PartialRowTracker) *SampleLimitNode {
	return &SampleLimitNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		tableName:   tableName,
		sampleSize:  sampleSize,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the sample limit directory.
func (s *SampleLimitNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SampleLimitNode.Getattr called",
		zap.String("table", s.tableName),
		zap.Int("sample_size", s.sampleSize))

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists approximately N random rows using TABLESAMPLE or ORDER BY RANDOM().
// Returns EIO if the database query fails.
// Note: The actual count may vary from sampleSize due to the probabilistic
// nature of TABLESAMPLE BERNOULLI.
func (s *SampleLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("SampleLimitNode.Readdir called",
		zap.String("table", s.tableName),
		zap.Int("sample_size", s.sampleSize))

	// Get primary key for table
	pk, err := s.db.GetPrimaryKey(ctx, s.schema, s.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", s.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Get row count estimate for TABLESAMPLE optimization
	// If cache is unavailable, use -1 to trigger ORDER BY RANDOM() fallback
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

	// Get random sample of rows
	rows, err := s.db.GetRandomSampleRows(ctx, s.schema, s.tableName, pkColumn, s.sampleSize, estimatedRows)
	if err != nil {
		logging.Error("Failed to get random sample",
			zap.String("table", s.tableName),
			zap.Int("sample_size", s.sampleSize),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert rows to directory entries
	entries := make([]fuse.DirEntry, 0, len(rows))
	for _, rowPK := range rows {
		entries = append(entries, fuse.DirEntry{
			Name: rowPK,
			Mode: syscall.S_IFREG,
		})
	}

	logging.Debug("Sample directory listing",
		zap.String("table", s.tableName),
		zap.Int("requested", s.sampleSize),
		zap.Int("returned", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row within the sampled results.
// Returns ENOENT if the row doesn't exist, EIO on database errors.
func (s *SampleLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("SampleLimitNode.Lookup called",
		zap.String("table", s.tableName),
		zap.Int("sample_size", s.sampleSize),
		zap.String("name", name))

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

		rowNode := NewRowFileNode(s.cfg, s.db, s.schema, s.tableName, pkColumn, pkValue, format)
		child := s.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(s.cfg, s.db, s.schema, s.tableName, pkColumn, pkValue, s.partialRows)
	child := s.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}
