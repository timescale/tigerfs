package fuse

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// ExportDirNode represents the .export/ directory.
// Lists available export formats (csv, tsv, json, yaml).
// When a format is accessed, returns an ExportFileNode that materializes the data.
type ExportDirNode struct {
	fs.Inode

	cfg       *config.Config         // TigerFS configuration
	db        db.DBClient            // Database client for queries
	cache     *tigerfs.MetadataCache // Cache for row count estimates
	schema    string                 // PostgreSQL schema name
	tableName string                 // Table to export from
	limit     int                    // Row limit for export (0 = use DirListingLimit)
	pkColumn  string                 // Primary key column (empty = no ordering)
	orderBy   string                 // Column to order by (empty = default order)
	ascending bool                   // Order direction (true = ASC, false = DESC)
}

var _ fs.InodeEmbedder = (*ExportDirNode)(nil)
var _ fs.NodeGetattrer = (*ExportDirNode)(nil)
var _ fs.NodeReaddirer = (*ExportDirNode)(nil)
var _ fs.NodeLookuper = (*ExportDirNode)(nil)

// NewExportDirNode creates a new .export/ directory node for a table.
// Used when .export/ appears directly under a table.
func NewExportDirNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName string) *ExportDirNode {
	return &ExportDirNode{
		cfg:       cfg,
		db:        dbClient,
		cache:     cache,
		schema:    schema,
		tableName: tableName,
		limit:     0, // Will use DirListingLimit
	}
}

// NewExportDirNodeWithLimit creates a new .export/ directory node with a row limit.
// Used for .first/N/.export/, .last/N/.export/, etc.
func NewExportDirNodeWithLimit(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName, pkColumn string, limit int, ascending bool) *ExportDirNode {
	return &ExportDirNode{
		cfg:       cfg,
		db:        dbClient,
		cache:     cache,
		schema:    schema,
		tableName: tableName,
		limit:     limit,
		pkColumn:  pkColumn,
		ascending: ascending,
	}
}

// NewExportDirNodeOrdered creates a new .export/ directory node with ordering.
// Used for .order/<column>/.first/N/.export/, etc.
func NewExportDirNodeOrdered(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, tableName, pkColumn, orderBy string, limit int, ascending bool) *ExportDirNode {
	return &ExportDirNode{
		cfg:       cfg,
		db:        dbClient,
		cache:     cache,
		schema:    schema,
		tableName: tableName,
		limit:     limit,
		pkColumn:  pkColumn,
		orderBy:   orderBy,
		ascending: ascending,
	}
}

// Getattr returns attributes for the export directory.
func (e *ExportDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ExportDirNode.Getattr called", zap.String("table", e.tableName))

	out.Mode = 0500 | syscall.S_IFDIR // Read-only directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists available export formats and options.
func (e *ExportDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("ExportDirNode.Readdir called", zap.String("table", e.tableName))

	entries := []fuse.DirEntry{
		{Name: DirWithHeaders, Mode: syscall.S_IFDIR}, // .with-headers/ option
		{Name: FmtCSV, Mode: syscall.S_IFREG},
		{Name: FmtJSON, Mode: syscall.S_IFREG},
		{Name: FmtTSV, Mode: syscall.S_IFREG},
		{Name: FmtYAML, Mode: syscall.S_IFREG},
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a format name (csv, tsv, json, yaml) or the .with-headers option.
func (e *ExportDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("ExportDirNode.Lookup called",
		zap.String("table", e.tableName),
		zap.String("name", name))

	// Handle .with-headers option directory
	if name == DirWithHeaders {
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
		}
		optNode := &ExportWithHeadersNode{
			cfg:       e.cfg,
			db:        e.db,
			cache:     e.cache,
			schema:    e.schema,
			tableName: e.tableName,
			limit:     e.limit,
			pkColumn:  e.pkColumn,
			orderBy:   e.orderBy,
			ascending: e.ascending,
		}
		child := e.NewPersistentInode(ctx, optNode, stableAttr)
		return child, 0
	}

	// Validate format name
	switch name {
	case FmtCSV, FmtTSV, FmtJSON, FmtYAML:
		// Valid format
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	fileNode := &ExportFileNode{
		cfg:         e.cfg,
		db:          e.db,
		cache:       e.cache,
		schema:      e.schema,
		tableName:   e.tableName,
		format:      name,
		limit:       e.limit,
		pkColumn:    e.pkColumn,
		orderBy:     e.orderBy,
		ascending:   e.ascending,
		withHeaders: false, // Default: no headers
	}

	child := e.NewPersistentInode(ctx, fileNode, stableAttr)
	return child, 0
}

// ExportFileNode represents a format file in the .export/ directory (csv, tsv, json, yaml).
// When read, it materializes the table data in the requested format.
type ExportFileNode struct {
	fs.Inode

	cfg         *config.Config         // TigerFS configuration
	db          db.DBClient            // Database client for queries
	cache       *tigerfs.MetadataCache // Cache for row count estimates
	schema      string                 // PostgreSQL schema name
	tableName   string                 // Table to export from
	format      string                 // Export format (csv, tsv, json, yaml)
	limit       int                    // Row limit for export (0 = use DirListingLimit)
	pkColumn    string                 // Primary key column (empty = no ordering)
	orderBy     string                 // Column to order by (empty = default order)
	ascending   bool                   // Order direction (true = ASC, false = DESC)
	withHeaders bool                   // Include header row in CSV/TSV output

	// Cached data (materialized on first access)
	data []byte
}

var _ fs.InodeEmbedder = (*ExportFileNode)(nil)
var _ fs.NodeGetattrer = (*ExportFileNode)(nil)
var _ fs.NodeOpener = (*ExportFileNode)(nil)

// Getattr returns attributes for the export file.
// Materializes the data to get accurate file size.
func (e *ExportFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ExportFileNode.Getattr called",
		zap.String("table", e.tableName),
		zap.String("format", e.format))

	// Materialize data to get size
	if e.data == nil {
		if err := e.materialize(ctx); err != nil {
			logging.Error("Failed to materialize export data",
				zap.String("table", e.tableName),
				zap.String("format", e.format),
				zap.Error(err))
			return syscall.EIO
		}
	}

	out.Mode = 0444 | syscall.S_IFREG // Read-only file
	out.Nlink = 1
	out.Size = uint64(len(e.data))

	return 0
}

// Open opens the export file for reading.
func (e *ExportFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("ExportFileNode.Open called",
		zap.String("table", e.tableName),
		zap.String("format", e.format))

	// Materialize data if not already done
	if e.data == nil {
		if err := e.materialize(ctx); err != nil {
			logging.Error("Failed to materialize export data",
				zap.String("table", e.tableName),
				zap.String("format", e.format),
				zap.Error(err))
			return nil, 0, syscall.EIO
		}
	}

	fh := &ExportFileHandle{data: e.data}
	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// materialize fetches the data and serializes it to the requested format.
func (e *ExportFileNode) materialize(ctx context.Context) error {
	logging.Debug("Materializing export data",
		zap.String("table", e.tableName),
		zap.String("format", e.format),
		zap.Int("limit", e.limit))

	// Determine row limit
	limit := e.limit
	if limit == 0 {
		limit = e.cfg.DirListingLimit
	}

	// Check if table exceeds limit
	if e.cache != nil {
		estimate, err := e.cache.GetRowCountEstimate(ctx, e.tableName)
		if err == nil && estimate > int64(limit) {
			logging.Error("Table too large for export",
				zap.String("table", e.tableName),
				zap.Int64("estimated_rows", estimate),
				zap.Int("limit", limit),
				zap.String("suggestion", "Use .first/N/.export/ or .last/N/.export/ for pagination"))
			return &exportLimitError{
				table:    e.tableName,
				estimate: estimate,
				limit:    limit,
			}
		}
	}

	// Fetch rows from database
	columns, rows, err := e.db.GetAllRows(ctx, e.schema, e.tableName, limit)
	if err != nil {
		return err
	}

	logging.Debug("Fetched rows for export",
		zap.String("table", e.tableName),
		zap.Int("row_count", len(rows)),
		zap.Int("column_count", len(columns)))

	// Serialize to requested format
	switch e.format {
	case FmtCSV:
		if e.withHeaders {
			e.data, err = format.RowsToCSVWithHeaders(columns, rows)
		} else {
			e.data, err = format.RowsToCSV(columns, rows)
		}
	case FmtTSV:
		if e.withHeaders {
			e.data, err = format.RowsToTSVWithHeaders(columns, rows)
		} else {
			e.data, err = format.RowsToTSV(columns, rows)
		}
	case FmtJSON:
		e.data, err = format.RowsToJSON(columns, rows)
	case FmtYAML:
		e.data, err = format.RowsToYAML(columns, rows)
	default:
		return &unknownFormatError{format: e.format}
	}

	if err != nil {
		return err
	}

	logging.Debug("Materialized export data",
		zap.String("table", e.tableName),
		zap.String("format", e.format),
		zap.Int("size", len(e.data)))

	return nil
}

// ExportFileHandle represents an open handle to an export file.
type ExportFileHandle struct {
	data []byte
}

var _ fs.FileReader = (*ExportFileHandle)(nil)

// Read reads export data from the file at the specified offset.
func (fh *ExportFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	logging.Debug("ExportFileHandle.Read called",
		zap.Int64("offset", off),
		zap.Int("size", len(dest)))

	// Handle EOF
	if off >= int64(len(fh.data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	// Calculate read bounds
	end := off + int64(len(dest))
	if end > int64(len(fh.data)) {
		end = int64(len(fh.data))
	}

	return fuse.ReadResultData(fh.data[off:end]), 0
}

// Error types for export operations

type exportLimitError struct {
	table    string
	estimate int64
	limit    int
}

func (e *exportLimitError) Error() string {
	return "table " + e.table + " exceeds export limit"
}

type unknownFormatError struct {
	format string
}

func (e *unknownFormatError) Error() string {
	return "unknown export format: " + e.format
}

// ExportWithHeadersNode represents the .with-headers/ directory under .export/.
// Only lists CSV and TSV formats since JSON/YAML already have keys as headers.
type ExportWithHeadersNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	cache     *tigerfs.MetadataCache
	schema    string
	tableName string
	limit     int
	pkColumn  string
	orderBy   string
	ascending bool
}

var _ fs.InodeEmbedder = (*ExportWithHeadersNode)(nil)
var _ fs.NodeGetattrer = (*ExportWithHeadersNode)(nil)
var _ fs.NodeReaddirer = (*ExportWithHeadersNode)(nil)
var _ fs.NodeLookuper = (*ExportWithHeadersNode)(nil)

// Getattr returns attributes for the .with-headers directory.
func (w *ExportWithHeadersNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists formats that support headers (csv, tsv only).
func (w *ExportWithHeadersNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: FmtCSV, Mode: syscall.S_IFREG},
		{Name: FmtTSV, Mode: syscall.S_IFREG},
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a format file with headers enabled.
func (w *ExportWithHeadersNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Only CSV and TSV support explicit headers option
	switch name {
	case FmtCSV, FmtTSV:
		// Valid
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	fileNode := &ExportFileNode{
		cfg:         w.cfg,
		db:          w.db,
		cache:       w.cache,
		schema:      w.schema,
		tableName:   w.tableName,
		format:      name,
		limit:       w.limit,
		pkColumn:    w.pkColumn,
		orderBy:     w.orderBy,
		ascending:   w.ascending,
		withHeaders: true, // Include headers
	}

	child := w.NewPersistentInode(ctx, fileNode, stableAttr)
	return child, 0
}
