package fuse

import (
	"context"
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

// TableNode represents a table or view directory in the filesystem
// Lists rows by their primary key values
type TableNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *MetadataCache
	tableName   string
	schema      string
	isView      bool // true if this is a view, not a table
	partialRows *PartialRowTracker
	staging     *StagingTracker
}

var _ fs.InodeEmbedder = (*TableNode)(nil)
var _ fs.NodeGetattrer = (*TableNode)(nil)
var _ fs.NodeReaddirer = (*TableNode)(nil)
var _ fs.NodeLookuper = (*TableNode)(nil)
var _ fs.NodeUnlinker = (*TableNode)(nil)
var _ fs.NodeRmdirer = (*TableNode)(nil)
var _ fs.NodeMkdirer = (*TableNode)(nil)

// NewTableNode creates a new table directory node
func NewTableNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker, staging *StagingTracker) *TableNode {
	return &TableNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		tableName:   tableName,
		schema:      schema,
		isView:      false,
		partialRows: partialRows,
		staging:     staging,
	}
}

// NewViewNode creates a new view directory node.
// Views are treated like tables for reading but may have write restrictions.
func NewViewNode(cfg *config.Config, dbClient db.DBClient, cache *MetadataCache, schema, viewName string, partialRows *PartialRowTracker, staging *StagingTracker) *TableNode {
	return &TableNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		tableName:   viewName,
		schema:      schema,
		isView:      true,
		partialRows: partialRows,
		staging:     staging,
	}
}

// checkWriteAllowed verifies that write operations are permitted on this table/view.
// Returns nil if writes are allowed, or EACCES if this is a non-updatable view.
func (t *TableNode) checkWriteAllowed(ctx context.Context) syscall.Errno {
	if !t.isView {
		return 0 // Tables always allow writes (permission checks happen later)
	}

	// Check if view is updatable
	updatable, err := t.db.IsViewUpdatable(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to check if view is updatable",
			zap.String("view", t.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	if !updatable {
		logging.Debug("Write denied: view is not updatable",
			zap.String("view", t.tableName))
		return syscall.EACCES
	}

	return 0
}

// Getattr returns attributes for the table directory
func (t *TableNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("TableNode.Getattr called", zap.String("table", t.tableName))

	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the contents of the table directory (row primary keys)
func (t *TableNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("TableNode.Readdir called", zap.String("table", t.tableName))

	// Check if table exceeds dir_listing_limit using cached estimate
	if t.cache != nil {
		estimate, err := t.cache.GetRowCountEstimate(ctx, t.tableName)
		if err != nil {
			logging.Warn("Failed to get row count estimate, continuing",
				zap.String("table", t.tableName),
				zap.Error(err))
		} else if estimate > int64(t.cfg.DirListingLimit) {
			logging.Error("Table too large for directory listing",
				zap.String("table", t.tableName),
				zap.Int64("estimated_rows", estimate),
				zap.Int("dir_listing_limit", t.cfg.DirListingLimit),
				zap.String("suggestion", "Use .first/N/, .sample/N/, or index paths like .column/value/"))
			return nil, syscall.EIO
		}
	}

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Get first column of primary key (only single-column PKs supported in MVP)
	pkColumn := pk.Columns[0]

	// List rows (limited by dir_listing_limit)
	rows, err := t.db.ListRows(ctx, t.schema, t.tableName, pkColumn, t.cfg.DirListingLimit)
	if err != nil {
		logging.Error("Failed to list rows",
			zap.String("table", t.tableName),
			zap.String("pk_column", pkColumn),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Convert rows to directory entries
	// Capability directories come first, then rows
	entries := make([]fuse.DirEntry, 0, len(rows)+10)

	// Add capability directories (alphabetical order)
	entries = append(entries,
		fuse.DirEntry{Name: DirAll, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirBy, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirDelete, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirExport, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirFilter, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirFirst, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirImport, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirIndexes, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirInfo, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirLast, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirModify, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirOrder, Mode: syscall.S_IFDIR},
		fuse.DirEntry{Name: DirSample, Mode: syscall.S_IFDIR},
	)

	// Add rows
	for _, rowPK := range rows {
		entries = append(entries, fuse.DirEntry{
			Name: rowPK,
			Mode: syscall.S_IFREG, // Rows are files (row-as-file representation)
		})
	}

	logging.Debug("Table directory listing",
		zap.String("table", t.tableName),
		zap.Int("row_count", len(rows)),
		zap.Int("total_entries", len(entries)),
		zap.Int("dir_listing_limit", t.cfg.DirListingLimit))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row by primary key value, metadata file, or index directory
func (t *TableNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("TableNode.Lookup called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Handle capability directories (alphabetical order)
	switch name {
	case DirAll:
		return t.lookupAllDirectory(ctx)
	case DirBy:
		return t.lookupByDirectory(ctx)
	case DirDelete:
		return t.lookupDeleteDirectory(ctx)
	case DirExport:
		return t.lookupExportDirectory(ctx)
	case DirFilter:
		return t.lookupFilterDirectory(ctx)
	case DirFirst:
		return t.lookupPaginationDirectory(ctx, PaginationFirst)
	case DirImport:
		return t.lookupImportDirectory(ctx)
	case DirIndexes:
		return t.lookupIndexesDirectory(ctx)
	case DirInfo:
		return t.lookupInfoDirectory(ctx)
	case DirLast:
		return t.lookupPaginationDirectory(ctx, PaginationLast)
	case DirModify:
		return t.lookupModifyDirectory(ctx)
	case DirOrder:
		return t.lookupOrderDirectory(ctx)
	case DirSample:
		return t.lookupSampleDirectory(ctx)
	}

	// Parse filename to extract PK value and format
	pkValue, format := util.ParseRowFilename(name)

	logging.Debug("Parsed row filename",
		zap.String("table", t.tableName),
		zap.String("filename", name),
		zap.String("pk_value", pkValue),
		zap.String("format", format))

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row exists by trying to fetch it (use PK value without extension)
	// This validates the row exists before creating the inode
	_, err = t.db.GetRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Debug("Row not found",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		return nil, syscall.ENOENT
	}

	// Row exists - decide whether to create directory or file node
	logging.Debug("Row found",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue),
		zap.String("format", format))

	// If name has explicit format extension, create row file node
	// Otherwise create row directory node
	if name != pkValue {
		// Name has extension (e.g., "1.csv"), create row file node
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFREG,
		}

		rowNode := NewRowFileNode(t.cfg, t.db, t.cache, t.schema, t.tableName, pkColumn, pkValue, format)
		child := t.NewPersistentInode(ctx, rowNode, stableAttr)

		// Fill in entry attributes (size, permissions, etc.) so they're cached correctly
		var attrOut fuse.AttrOut
		if errno := rowNode.Getattr(ctx, nil, &attrOut); errno != 0 {
			return nil, errno
		}
		out.Attr = attrOut.Attr

		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(t.cfg, t.db, t.cache, t.schema, t.tableName, pkColumn, pkValue, t.partialRows)
	child := t.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// lookupSampleDirectory handles lookup for the .sample directory.
func (t *TableNode) lookupSampleDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .sample directory",
		zap.String("table", t.tableName))

	// Get primary key for table to create pipeline context
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key for sample directory",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Create initial pipeline context for table root
	pipeline := NewPipelineContext(t.schema, t.tableName, pkColumn)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	sampleNode := NewSampleNodeWithPipeline(t.cfg, t.db, t.cache, t.schema, t.tableName, t.partialRows, pipeline)
	child := t.NewPersistentInode(ctx, sampleNode, stableAttr)

	logging.Debug("Created .sample directory node",
		zap.String("table", t.tableName))

	return child, 0
}

// lookupPaginationDirectory handles lookup for .first and .last directories
func (t *TableNode) lookupPaginationDirectory(ctx context.Context, paginationType PaginationType) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up pagination directory",
		zap.String("table", t.tableName),
		zap.String("type", string(paginationType)))

	// Get primary key for table to create pipeline context
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key for pagination directory",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Create initial pipeline context for table root
	pipeline := NewPipelineContext(t.schema, t.tableName, pkColumn)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	paginationNode := NewPaginationNodeWithPipeline(t.cfg, t.db, t.cache, t.schema, t.tableName, paginationType, t.partialRows, pipeline)
	child := t.NewPersistentInode(ctx, paginationNode, stableAttr)

	logging.Debug("Created pagination directory node",
		zap.String("table", t.tableName),
		zap.String("type", string(paginationType)))

	return child, 0
}

// lookupAllDirectory handles lookup for the .all directory that bypasses dir_listing_limit
func (t *TableNode) lookupAllDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .all directory",
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	allNode := NewAllRowsNode(t.cfg, t.db, t.cache, t.schema, t.tableName, t.partialRows)
	child := t.NewPersistentInode(ctx, allNode, stableAttr)

	logging.Debug("Created .all directory node",
		zap.String("table", t.tableName))

	return child, 0
}

// Unlink deletes a row file (rm /users/123.csv)
func (t *TableNode) Unlink(ctx context.Context, name string) syscall.Errno {
	logging.Debug("TableNode.Unlink called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Check if writes are allowed (views may be read-only)
	if errno := t.checkWriteAllowed(ctx); errno != 0 {
		return errno
	}

	// Parse filename to get PK value (strip format extension)
	pkValue, _ := util.ParseRowFilename(name)

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Delete row from database
	err = t.db.DeleteRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Error("Failed to delete row",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		// Map constraint violations to EACCES
		if strings.Contains(err.Error(), "foreign key") ||
			strings.Contains(err.Error(), "constraint") {
			return syscall.EACCES
		}
		return syscall.EIO
	}

	logging.Debug("Row deleted successfully",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	return 0
}

// Rmdir deletes a row directory (rm -r /users/123/)
func (t *TableNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	logging.Debug("TableNode.Rmdir called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Check if writes are allowed (views may be read-only)
	if errno := t.checkWriteAllowed(ctx); errno != 0 {
		return errno
	}

	// Can't rmdir row files (must use unlink)
	// Check if name has format extension
	pkValue, _ := util.ParseRowFilename(name)
	if name != pkValue {
		// Has extension - this is a file, not a directory
		logging.Debug("Cannot rmdir a file",
			zap.String("table", t.tableName),
			zap.String("name", name))
		return syscall.ENOTDIR
	}

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Delete row from database
	err = t.db.DeleteRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err != nil {
		logging.Error("Failed to delete row",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue),
			zap.Error(err))
		// Map constraint violations to EACCES
		if strings.Contains(err.Error(), "foreign key") ||
			strings.Contains(err.Error(), "constraint") {
			return syscall.EACCES
		}
		return syscall.EIO
	}

	logging.Debug("Row directory deleted successfully",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	return 0
}

// Mkdir creates a new row directory (for incremental row creation)
func (t *TableNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("TableNode.Mkdir called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Check if writes are allowed (views may be read-only)
	if errno := t.checkWriteAllowed(ctx); errno != 0 {
		return nil, errno
	}

	// Parse filename - should be just PK value with no extension
	pkValue, _ := util.ParseRowFilename(name)
	if name != pkValue {
		// Has extension - can't mkdir a file name
		logging.Debug("Cannot mkdir with format extension",
			zap.String("table", t.tableName),
			zap.String("name", name))
		return nil, syscall.EINVAL
	}

	// Get primary key for table
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Check if row already exists
	_, err = t.db.GetRow(ctx, t.schema, t.tableName, pkColumn, pkValue)
	if err == nil {
		// Row already exists
		logging.Debug("Row already exists",
			zap.String("table", t.tableName),
			zap.String("pk", pkValue))
		return nil, syscall.EEXIST
	}

	// Row doesn't exist - create row directory node for new row
	logging.Debug("Creating new row directory",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	// Create stable inode for row directory
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Create row directory node (will be empty until columns are written)
	rowDirNode := NewRowDirectoryNode(t.cfg, t.db, t.cache, t.schema, t.tableName, pkColumn, pkValue, t.partialRows)
	child := t.NewPersistentInode(ctx, rowDirNode, stableAttr)

	logging.Debug("Row directory created",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	return child, 0
}

// lookupIndexesDirectory handles lookup for the .indexes directory (index DDL operations).
func (t *TableNode) lookupIndexesDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .indexes directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	indexesNode := NewIndexesNode(t.cfg, t.db, t.schema, t.tableName, t.staging)
	child := t.NewPersistentInode(ctx, indexesNode, stableAttr)

	logging.Debug("Created .indexes directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupModifyDirectory handles lookup for the .modify directory (ALTER TABLE staging).
func (t *TableNode) lookupModifyDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .modify directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	stagingCtx := StagingContext{
		ObjectType:  "table",
		ObjectName:  t.tableName,
		Schema:      t.schema,
		TableName:   t.tableName,
		Operation:   DDLModify,
		StagingPath: t.tableName + "/.modify",
	}

	modifyNode := NewStagingDirNode(t.cfg, t.db, t.staging, stagingCtx)
	child := t.NewPersistentInode(ctx, modifyNode, stableAttr)

	logging.Debug("Created .modify directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupDeleteDirectory handles lookup for the .delete directory (DROP TABLE staging).
func (t *TableNode) lookupDeleteDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .delete directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	stagingCtx := StagingContext{
		ObjectType:  "table",
		ObjectName:  t.tableName,
		Schema:      t.schema,
		TableName:   t.tableName,
		Operation:   DDLDelete,
		StagingPath: t.tableName + "/.delete",
	}

	deleteNode := NewStagingDirNode(t.cfg, t.db, t.staging, stagingCtx)
	child := t.NewPersistentInode(ctx, deleteNode, stableAttr)

	logging.Debug("Created .delete directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupExportDirectory handles lookup for the .export directory (bulk data export).
func (t *TableNode) lookupExportDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .export directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	exportNode := NewExportDirNode(t.cfg, t.db, t.cache, t.schema, t.tableName)
	child := t.NewPersistentInode(ctx, exportNode, stableAttr)

	logging.Debug("Created .export directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupFilterDirectory handles lookup for the .filter directory (column filtering).
func (t *TableNode) lookupFilterDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .filter directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	// Get primary key for table to create pipeline context
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key for filter directory",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Create initial pipeline context for table root
	pipeline := NewPipelineContext(t.schema, t.tableName, pkColumn)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	filterNode := NewFilterDirNode(t.cfg, t.db, t.cache, t.schema, t.tableName, pipeline, t.partialRows)
	child := t.NewPersistentInode(ctx, filterNode, stableAttr)

	logging.Debug("Created .filter directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupImportDirectory handles lookup for the .import directory (bulk data import).
func (t *TableNode) lookupImportDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .import directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	importNode := NewImportDirNode(t.cfg, t.db, t.schema, t.tableName)
	child := t.NewPersistentInode(ctx, importNode, stableAttr)

	logging.Debug("Created .import directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupInfoDirectory handles lookup for the .info metadata directory.
func (t *TableNode) lookupInfoDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .info directory",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName),
		zap.Bool("isView", t.isView))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	// Use view-specific constructor if this is a view
	var infoNode *InfoDirNode
	if t.isView {
		infoNode = NewViewInfoDirNode(t.cfg, t.db, t.schema, t.tableName)
	} else {
		infoNode = NewInfoDirNode(t.cfg, t.db, t.schema, t.tableName)
	}
	child := t.NewPersistentInode(ctx, infoNode, stableAttr)

	logging.Debug("Created .info directory node",
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupByDirectory handles lookup for the .by index navigation directory.
func (t *TableNode) lookupByDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up directory",
		zap.String("directory", DirBy),
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	// Get primary key for table to create pipeline context
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key for .by directory",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Create initial pipeline context for table root
	pipeline := NewPipelineContext(t.schema, t.tableName, pkColumn)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	byNode := NewByDirNodeWithPipeline(t.cfg, t.db, t.cache, t.schema, t.tableName, t.partialRows, pipeline)
	child := t.NewPersistentInode(ctx, byNode, stableAttr)

	logging.Debug("Created directory node",
		zap.String("directory", DirBy),
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}

// lookupOrderDirectory handles lookup for the .order directory.
func (t *TableNode) lookupOrderDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up directory",
		zap.String("directory", DirOrder),
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	// Get primary key for table to create pipeline context
	pk, err := t.db.GetPrimaryKey(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get primary key for .order directory",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	pkColumn := pk.Columns[0]

	// Create initial pipeline context for table root
	pipeline := NewPipelineContext(t.schema, t.tableName, pkColumn)

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	orderNode := NewOrderDirNodeWithPipeline(t.cfg, t.db, t.cache, t.schema, t.tableName, t.partialRows, pipeline)
	child := t.NewPersistentInode(ctx, orderNode, stableAttr)

	logging.Debug("Created directory node",
		zap.String("directory", DirOrder),
		zap.String("schema", t.schema),
		zap.String("table", t.tableName))

	return child, 0
}
