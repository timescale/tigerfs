// Package fuse provides FUSE filesystem operations for TigerFS.
//
// This file implements the .filter/ capability for filtering rows by any column.
// Unlike .by/ which only shows indexed columns, .filter/ shows all columns
// but uses safety mechanisms to prevent expensive queries on large tables.

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

// DirFilter is the name of the filter capability directory.
const DirFilter = ".filter"

// FileTableTooLarge is shown when a non-indexed column's table exceeds DirFilterLimit.
// Users can still navigate directly to values, but value listing is not available.
const FileTableTooLarge = ".table-too-large"

// FilterDirNode represents the .filter/ directory within a table.
// Lists all columns (not just indexed ones), enabling filtering by any column.
//
// Path: /schema/table/.filter/
type FilterDirNode struct {
	fs.Inode

	cfg    *config.Config
	db     db.DBClient
	cache  *MetadataCache
	schema string
	table  string

	// pipeline is the current pipeline context (may have prior operations)
	pipeline *PipelineContext

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*FilterDirNode)(nil)
var _ fs.NodeGetattrer = (*FilterDirNode)(nil)
var _ fs.NodeReaddirer = (*FilterDirNode)(nil)
var _ fs.NodeLookuper = (*FilterDirNode)(nil)

// NewFilterDirNode creates a new .filter directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil)
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pipeline: Current pipeline context (nil for fresh context)
//   - partialRows: Tracker for incremental row creation
func NewFilterDirNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, table string, pipeline *PipelineContext, partialRows *PartialRowTracker) *FilterDirNode {
	return &FilterDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the .filter directory.
func (f *FilterDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("FilterDirNode.Getattr called",
		zap.String("schema", f.schema),
		zap.String("table", f.table))

	out.Mode = 0500 | syscall.S_IFDIR // read-only directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists all columns in the table.
// Unlike .by/ which only shows indexed columns, .filter/ shows all columns.
func (f *FilterDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("FilterDirNode.Readdir called",
		zap.String("schema", f.schema),
		zap.String("table", f.table))

	columns, err := f.db.GetColumns(ctx, f.schema, f.table)
	if err != nil {
		logging.Error("Failed to get columns for .filter/",
			zap.String("table", f.table),
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

	logging.Debug("Filter directory listing",
		zap.String("table", f.table),
		zap.Int("column_count", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a column within .filter/.
func (f *FilterDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("FilterDirNode.Lookup called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", name))

	// Verify column exists
	columns, err := f.db.GetColumns(ctx, f.schema, f.table)
	if err != nil {
		logging.Error("Failed to verify column",
			zap.String("table", f.table),
			zap.String("column", name),
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
		logging.Debug("Column not found",
			zap.String("table", f.table),
			zap.String("column", name))
		return nil, syscall.ENOENT
	}

	// Create FilterColumnNode for this column
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	columnNode := NewFilterColumnNode(f.cfg, f.db, f.cache, f.schema, f.table, name, f.pipeline, f.partialRows)
	child := f.NewPersistentInode(ctx, columnNode, stableAttr)

	return child, 0
}

// FilterColumnNode represents a column within .filter/<column>/.
// Lists distinct values for the column, with safety mechanisms for large tables.
//
// Path: /schema/table/.filter/<column>/
type FilterColumnNode struct {
	fs.Inode

	cfg    *config.Config
	db     db.DBClient
	cache  *MetadataCache
	schema string
	table  string
	column string

	// pipeline is the current pipeline context
	pipeline *PipelineContext

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*FilterColumnNode)(nil)
var _ fs.NodeGetattrer = (*FilterColumnNode)(nil)
var _ fs.NodeReaddirer = (*FilterColumnNode)(nil)
var _ fs.NodeLookuper = (*FilterColumnNode)(nil)

// NewFilterColumnNode creates a new filter column node.
func NewFilterColumnNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, table, column string, pipeline *PipelineContext, partialRows *PartialRowTracker) *FilterColumnNode {
	return &FilterColumnNode{
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

// Getattr returns attributes for the filter column directory.
func (f *FilterColumnNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("FilterColumnNode.Getattr called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", f.column))

	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists distinct values for the column.
// Uses safety mechanisms:
//   - Indexed columns: Always list values (fast index scan)
//   - Non-indexed columns on small tables (<DirFilterLimit): List values
//   - Non-indexed columns on large tables: Show .table-too-large indicator
func (f *FilterColumnNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("FilterColumnNode.Readdir called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", f.column))

	// Check if column is indexed
	isIndexed, err := f.isColumnIndexed(ctx)
	if err != nil {
		logging.Error("Failed to check if column is indexed",
			zap.String("table", f.table),
			zap.String("column", f.column),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// If indexed, always list values (fast index scan)
	if isIndexed {
		return f.listDistinctValues(ctx)
	}

	// Non-indexed column: check table size
	estimate, err := f.db.GetTableRowCountEstimate(ctx, f.schema, f.table)
	if err != nil {
		logging.Error("Failed to get table row count estimate",
			zap.String("table", f.table),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// If estimate is -1 (table not found in pg_class) or small, list values
	if estimate == -1 || estimate < int64(f.cfg.DirFilterLimit) {
		return f.listDistinctValues(ctx)
	}

	// Large table with non-indexed column: show indicator only
	logging.Debug("Table too large for non-indexed column value listing",
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.Int64("estimate", estimate),
		zap.Int("limit", f.cfg.DirFilterLimit))

	entries := []fuse.DirEntry{
		{Name: FileTableTooLarge, Mode: syscall.S_IFREG},
	}
	return fs.NewListDirStream(entries), 0
}

// isColumnIndexed checks if the column has an index.
func (f *FilterColumnNode) isColumnIndexed(ctx context.Context) (bool, error) {
	idx, err := f.db.GetIndexByColumn(ctx, f.schema, f.table, f.column)
	if err != nil {
		return false, err
	}
	return idx != nil, nil
}

// listDistinctValues returns a directory stream of distinct column values.
func (f *FilterColumnNode) listDistinctValues(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Use a reasonable limit for distinct values
	const maxDistinctValues = 1000

	values, err := f.db.GetDistinctValues(ctx, f.schema, f.table, f.column, maxDistinctValues)
	if err != nil {
		// Check if this is a timeout error
		if db.IsTimeoutError(err) {
			logging.Warn("Timeout getting distinct values, showing indicator",
				zap.String("table", f.table),
				zap.String("column", f.column),
				zap.Error(err))
			entries := []fuse.DirEntry{
				{Name: FileTableTooLarge, Mode: syscall.S_IFREG},
			}
			return fs.NewListDirStream(entries), 0
		}

		logging.Error("Failed to get distinct values",
			zap.String("table", f.table),
			zap.String("column", f.column),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, 0, len(values))
	for _, val := range values {
		entries = append(entries, fuse.DirEntry{
			Name: val,
			Mode: syscall.S_IFDIR, // Values are directories
		})
	}

	logging.Debug("Filter column value listing",
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.Int("value_count", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a value or the .table-too-large indicator.
// Direct path access always works, even if the value wasn't listed.
func (f *FilterColumnNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("FilterColumnNode.Lookup called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.String("value", name))

	// Handle .table-too-large indicator file
	if name == FileTableTooLarge {
		stableAttr := fs.StableAttr{Mode: syscall.S_IFREG}
		fileNode := NewTableTooLargeNode(f.table, f.column)
		child := f.NewPersistentInode(ctx, fileNode, stableAttr)
		return child, 0
	}

	// Create FilterValueNode for this value
	// Direct path access always works, even if value wasn't in listing
	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	valueNode := NewFilterValueNode(f.cfg, f.db, f.cache, f.schema, f.table, f.column, name, f.pipeline, f.partialRows)
	child := f.NewPersistentInode(ctx, valueNode, stableAttr)

	return child, 0
}

// FilterValueNode represents a specific value within .filter/<column>/<value>/.
// Applies the filter to the pipeline and exposes rows and next capabilities.
//
// Path: /schema/table/.filter/<column>/<value>/
type FilterValueNode struct {
	fs.Inode

	cfg    *config.Config
	db     db.DBClient
	cache  *MetadataCache
	schema string
	table  string
	column string
	value  string

	// pipeline is the current pipeline context (with this filter applied)
	pipeline *PipelineContext

	// partialRows tracks incremental row creation state
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*FilterValueNode)(nil)
var _ fs.NodeGetattrer = (*FilterValueNode)(nil)
var _ fs.NodeReaddirer = (*FilterValueNode)(nil)
var _ fs.NodeLookuper = (*FilterValueNode)(nil)

// NewFilterValueNode creates a new filter value node.
func NewFilterValueNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, table, column, value string, basePipeline *PipelineContext, partialRows *PartialRowTracker) *FilterValueNode {
	// Get primary key column for pipeline context
	// Note: In real usage, this should be passed in or cached
	pkColumn := "id" // Default, will be overridden

	// Create or extend pipeline context with this filter
	var pipeline *PipelineContext
	if basePipeline != nil {
		pipeline = basePipeline.WithFilter(column, value, false) // false = not indexed (.filter/)
	} else {
		pipeline = NewPipelineContext(schema, table, pkColumn)
		pipeline = pipeline.WithFilter(column, value, false)
	}

	return &FilterValueNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		column:      column,
		value:       value,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the filter value directory.
func (f *FilterValueNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("FilterValueNode.Getattr called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.String("value", f.value))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists available capabilities and matching rows.
func (f *FilterValueNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("FilterValueNode.Readdir called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.String("value", f.value))

	// Get available capabilities from pipeline context
	caps := f.pipeline.AvailableCapabilities()

	entries := make([]fuse.DirEntry, 0, len(caps)+10)

	// Add capability directories
	for _, cap := range caps {
		entries = append(entries, fuse.DirEntry{
			Name: cap,
			Mode: syscall.S_IFDIR,
		})
	}

	// Query for matching rows (limited)
	// For now, just show capabilities - row listing will be added in Task 5.6
	// when PipelineNode is implemented to handle row queries

	logging.Debug("FilterValueNode directory listing",
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.String("value", f.value),
		zap.Int("capability_count", len(caps)))

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to capabilities or rows.
func (f *FilterValueNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("FilterValueNode.Lookup called",
		zap.String("schema", f.schema),
		zap.String("table", f.table),
		zap.String("column", f.column),
		zap.String("value", f.value),
		zap.String("name", name))

	// Route to capabilities
	switch name {
	case DirFilter:
		if f.pipeline.CanAddFilter() {
			stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
			filterNode := NewFilterDirNode(f.cfg, f.db, f.cache, f.schema, f.table, f.pipeline, f.partialRows)
			child := f.NewPersistentInode(ctx, filterNode, stableAttr)
			return child, 0
		}
		return nil, syscall.ENOENT

	case DirBy:
		if f.pipeline.CanAddFilter() {
			stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
			byNode := NewByDirNode(f.cfg, f.db, f.cache, f.schema, f.table, f.partialRows)
			child := f.NewPersistentInode(ctx, byNode, stableAttr)
			return child, 0
		}
		return nil, syscall.ENOENT

	case DirExport:
		// Export will be fully supported when PipelineNode is implemented (Task 5.6/5.7)
		// For now, export at filter value level is not yet supported
		if f.pipeline.CanExport() {
			stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
			exportNode := NewExportDirNode(f.cfg, f.db, f.cache, f.schema, f.table)
			child := f.NewPersistentInode(ctx, exportNode, stableAttr)
			return child, 0
		}
		return nil, syscall.ENOENT

		// TODO: Add routing for .order, .first, .last, .sample in Task 5.6/5.7
	}

	// For now, return ENOENT for unrecognized names
	// Row lookup will be added when PipelineNode is implemented
	return nil, syscall.ENOENT
}

// TableTooLargeNode represents the .table-too-large indicator file.
// This file appears when a non-indexed column's table exceeds DirFilterLimit.
type TableTooLargeNode struct {
	fs.Inode
	table  string
	column string
}

var _ fs.InodeEmbedder = (*TableTooLargeNode)(nil)
var _ fs.NodeGetattrer = (*TableTooLargeNode)(nil)
var _ fs.NodeOpener = (*TableTooLargeNode)(nil)
var _ fs.NodeReader = (*TableTooLargeNode)(nil)

// NewTableTooLargeNode creates a new indicator file node.
func NewTableTooLargeNode(table, column string) *TableTooLargeNode {
	return &TableTooLargeNode{
		table:  table,
		column: column,
	}
}

// Getattr returns attributes for the indicator file.
func (t *TableTooLargeNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	content := t.content()
	out.Mode = 0444 | syscall.S_IFREG
	out.Size = uint64(len(content))
	out.Nlink = 1
	return 0
}

// Open opens the indicator file.
func (t *TableTooLargeNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Read reads the indicator file content.
func (t *TableTooLargeNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	content := t.content()
	if off >= int64(len(content)) {
		return fuse.ReadResultData(nil), 0
	}
	return fuse.ReadResultData(content[off:]), 0
}

// content returns the explanatory text for the indicator file.
func (t *TableTooLargeNode) content() []byte {
	msg := "Table " + t.table + " is too large to list distinct values for column " + t.column + ".\n" +
		"You can still filter by navigating directly to a value:\n" +
		"  .filter/" + t.column + "/<value>/\n"
	return []byte(msg)
}
