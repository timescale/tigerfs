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

	cfg            *config.Config     // TigerFS configuration
	db             db.DBClient        // Database client for queries
	cache          *MetadataCache     // Metadata cache for permissions lookup
	schema         string             // PostgreSQL schema name
	tableName      string             // Table this pagination belongs to
	paginationType PaginationType     // Whether this is .first or .last
	partialRows    *PartialRowTracker // Tracker for partial row writes
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
func NewPaginationNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, paginationType PaginationType, partialRows *PartialRowTracker) *PaginationNode {
	return &PaginationNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		paginationType: paginationType,
		partialRows:    partialRows,
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

	limitNode := NewPaginationLimitNode(p.cfg, p.db, p.cache, p.schema, p.tableName, p.paginationType, limit, p.partialRows)
	child := p.NewPersistentInode(ctx, limitNode, stableAttr)

	logging.Debug("Created pagination limit node",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", limit))

	return child, 0
}

// PaginationLimitNode represents the .first/N/ or .last/N/ directory.
// Lists the first or last N rows from the table ordered by primary key.
type PaginationLimitNode struct {
	fs.Inode

	cfg            *config.Config     // TigerFS configuration
	db             db.DBClient        // Database client for queries
	cache          *MetadataCache     // Metadata cache for permissions lookup
	schema         string             // PostgreSQL schema name
	tableName      string             // Table this pagination belongs to
	paginationType PaginationType     // Whether this is .first or .last
	limit          int                // Maximum number of rows to return
	partialRows    *PartialRowTracker // Tracker for partial row writes
}

var _ fs.InodeEmbedder = (*PaginationLimitNode)(nil)
var _ fs.NodeGetattrer = (*PaginationLimitNode)(nil)
var _ fs.NodeReaddirer = (*PaginationLimitNode)(nil)
var _ fs.NodeLookuper = (*PaginationLimitNode)(nil)

// NewPaginationLimitNode creates a new .first/N/ or .last/N/ directory node.
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
func NewPaginationLimitNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, paginationType PaginationType, limit int, partialRows *PartialRowTracker) *PaginationLimitNode {
	return &PaginationLimitNode{
		cfg:            cfg,
		db:             dbClient,
		cache:          cache,
		schema:         schema,
		tableName:      tableName,
		paginationType: paginationType,
		limit:          limit,
		partialRows:    partialRows,
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
// Returns EIO if the database query fails.
func (p *PaginationLimitNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("PaginationLimitNode.Readdir called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit))

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
	if p.paginationType == PaginationFirst {
		rows, err = p.db.GetFirstNRows(ctx, p.schema, p.tableName, pkColumn, p.limit)
	} else {
		rows, err = p.db.GetLastNRows(ctx, p.schema, p.tableName, pkColumn, p.limit)
	}

	if err != nil {
		logging.Error("Failed to get paginated rows",
			zap.String("table", p.tableName),
			zap.String("type", string(p.paginationType)),
			zap.Int("limit", p.limit),
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

	logging.Debug("Pagination directory listing",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit),
		zap.Int("row_count", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row within the paginated results.
// Returns ENOENT if the row doesn't exist, EIO on database errors.
func (p *PaginationLimitNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("PaginationLimitNode.Lookup called",
		zap.String("table", p.tableName),
		zap.String("type", string(p.paginationType)),
		zap.Int("limit", p.limit),
		zap.String("name", name))

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
