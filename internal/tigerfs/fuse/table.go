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

// TableNode represents a table directory in the filesystem
// Lists rows by their primary key values
type TableNode struct {
	fs.Inode

	cfg         *config.Config
	db          *db.Client
	cache       *MetadataCache
	tableName   string
	schema      string
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*TableNode)(nil)
var _ fs.NodeGetattrer = (*TableNode)(nil)
var _ fs.NodeReaddirer = (*TableNode)(nil)
var _ fs.NodeLookuper = (*TableNode)(nil)
var _ fs.NodeUnlinker = (*TableNode)(nil)
var _ fs.NodeRmdirer = (*TableNode)(nil)
var _ fs.NodeMkdirer = (*TableNode)(nil)

// NewTableNode creates a new table directory node
func NewTableNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName string, partialRows *PartialRowTracker) *TableNode {
	return &TableNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		tableName:   tableName,
		schema:      schema,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the table directory
func (t *TableNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("TableNode.Getattr called", zap.String("table", t.tableName))

	out.Mode = 0755 | syscall.S_IFDIR
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

	// Get single-column indexes for index directory entries
	singleIndexes, err := t.db.GetSingleColumnIndexes(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Warn("Failed to get single-column indexes, continuing without",
			zap.String("table", t.tableName),
			zap.Error(err))
		singleIndexes = nil
	}

	// Get composite indexes for multi-column navigation
	compositeIndexes, err := t.db.GetCompositeIndexes(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Warn("Failed to get composite indexes, continuing without",
			zap.String("table", t.tableName),
			zap.Error(err))
		compositeIndexes = nil
	}

	// Convert rows to directory entries
	entries := make([]fuse.DirEntry, 0, len(rows)+len(singleIndexes)+len(compositeIndexes)+3)

	// Add metadata files first
	entries = append(entries,
		fuse.DirEntry{
			Name: ".columns",
			Mode: syscall.S_IFREG,
		},
		fuse.DirEntry{
			Name: ".schema",
			Mode: syscall.S_IFREG,
		},
		fuse.DirEntry{
			Name: ".count",
			Mode: syscall.S_IFREG,
		},
		fuse.DirEntry{
			Name: ".all",
			Mode: syscall.S_IFDIR,
		},
		fuse.DirEntry{
			Name: ".first",
			Mode: syscall.S_IFDIR,
		},
		fuse.DirEntry{
			Name: ".last",
			Mode: syscall.S_IFDIR,
		},
		fuse.DirEntry{
			Name: ".sample",
			Mode: syscall.S_IFDIR,
		},
	)

	// Add single-column index directories (e.g., .email/, .category/)
	// Skip primary key index since rows are already accessible by PK
	for _, idx := range singleIndexes {
		if idx.IsPrimary {
			continue
		}
		if len(idx.Columns) > 0 {
			entries = append(entries, fuse.DirEntry{
				Name: "." + idx.Columns[0],
				Mode: syscall.S_IFDIR,
			})
		}
	}

	// Add composite index directories (e.g., .last_name.first_name/)
	for _, idx := range compositeIndexes {
		if idx.IsPrimary {
			continue
		}
		entries = append(entries, fuse.DirEntry{
			Name: FormatCompositeIndexName(idx.Columns),
			Mode: syscall.S_IFDIR,
		})
	}

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
		zap.Int("single_index_count", len(singleIndexes)),
		zap.Int("composite_index_count", len(compositeIndexes)),
		zap.Int("total_entries", len(entries)),
		zap.Int("dir_listing_limit", t.cfg.DirListingLimit))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a row by primary key value, metadata file, or index directory
func (t *TableNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("TableNode.Lookup called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Check if this is a metadata file lookup
	if name == ".columns" || name == ".schema" || name == ".count" {
		return t.lookupMetadataFile(ctx, name)
	}

	// Check if this is the .all directory (bypass dir_listing_limit)
	if name == ".all" {
		return t.lookupAllDirectory(ctx)
	}

	// Check if this is a pagination directory (.first or .last)
	if name == ".first" {
		return t.lookupPaginationDirectory(ctx, PaginationFirst)
	}
	if name == ".last" {
		return t.lookupPaginationDirectory(ctx, PaginationLast)
	}

	// Check if this is a sample directory (.sample)
	if name == ".sample" {
		return t.lookupSampleDirectory(ctx)
	}

	// Check if this is an index directory lookup (e.g., .email)
	if strings.HasPrefix(name, ".") && len(name) > 1 {
		return t.lookupIndexDirectory(ctx, name)
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

		rowNode := NewRowFileNode(t.cfg, t.db, t.schema, t.tableName, pkColumn, pkValue, format)
		child := t.NewPersistentInode(ctx, rowNode, stableAttr)
		return child, 0
	}

	// No extension, create row directory node
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(t.cfg, t.db, t.schema, t.tableName, pkColumn, pkValue, t.partialRows)
	child := t.NewPersistentInode(ctx, rowDirNode, stableAttr)
	return child, 0
}

// lookupSampleDirectory handles lookup for the .sample directory.
func (t *TableNode) lookupSampleDirectory(ctx context.Context) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up .sample directory",
		zap.String("table", t.tableName))

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	sampleNode := NewSampleNode(t.cfg, t.db, t.cache, t.schema, t.tableName, t.partialRows)
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

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	paginationNode := NewPaginationNode(t.cfg, t.db, t.schema, t.tableName, paginationType, t.partialRows)
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

// lookupMetadataFile handles lookups for metadata files (.columns, .schema, .count)
func (t *TableNode) lookupMetadataFile(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up metadata file",
		zap.String("table", t.tableName),
		zap.String("file", name))

	// Determine metadata file type
	var fileType string
	switch name {
	case ".columns":
		fileType = "columns"
	case ".schema":
		fileType = "schema"
	case ".count":
		fileType = "count"
	default:
		logging.Debug("Unknown metadata file",
			zap.String("table", t.tableName),
			zap.String("file", name))
		return nil, syscall.ENOENT
	}

	// Create stable inode for metadata file
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	// Create metadata file node
	metadataNode := NewMetadataFileNode(t.cfg, t.db, t.schema, t.tableName, fileType)
	child := t.NewPersistentInode(ctx, metadataNode, stableAttr)

	logging.Debug("Created metadata file node",
		zap.String("table", t.tableName),
		zap.String("file", name),
		zap.String("type", fileType))

	return child, 0
}

// lookupIndexDirectory handles lookups for index directories.
// Supports both single-column indexes (e.g., .email/) and composite indexes
// (e.g., .last_name.first_name/). Index directories provide alternative navigation
// paths using indexed columns for efficient lookups.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - name: The dotfile name being looked up (e.g., ".email" or ".last_name.first_name")
//
// Returns an IndexNode for single-column indexes or CompositeIndexNode for multi-column.
// Primary key indexes are excluded since rows are already accessible by PK directly.
func (t *TableNode) lookupIndexDirectory(ctx context.Context, name string) (*fs.Inode, syscall.Errno) {
	// Check if this is a composite index name (e.g., ".last_name.first_name")
	compositeColumns := ParseCompositeIndexName(name)
	if compositeColumns != nil {
		return t.lookupCompositeIndexDirectory(ctx, name, compositeColumns)
	}

	// Single-column index: extract column name from dotfile name (e.g., ".email" -> "email")
	columnName := name[1:]

	logging.Debug("Looking up single-column index directory",
		zap.String("table", t.tableName),
		zap.String("column", columnName))

	// Check if this column has an index (must be leading column)
	idx, err := t.db.GetIndexByColumn(ctx, t.schema, t.tableName, columnName)
	if err != nil {
		logging.Error("Failed to check for index",
			zap.String("table", t.tableName),
			zap.String("column", columnName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	if idx == nil {
		logging.Debug("No index found for column",
			zap.String("table", t.tableName),
			zap.String("column", columnName))
		return nil, syscall.ENOENT
	}

	// Primary key indexes are excluded - rows already accessible via PK
	if idx.IsPrimary {
		logging.Debug("Skipping primary key index",
			zap.String("table", t.tableName),
			zap.String("column", columnName))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	indexNode := NewIndexNode(t.cfg, t.db, t.schema, t.tableName, columnName, idx, t.partialRows)
	child := t.NewPersistentInode(ctx, indexNode, stableAttr)

	logging.Debug("Created index directory node",
		zap.String("table", t.tableName),
		zap.String("column", columnName),
		zap.String("index", idx.Name))

	return child, 0
}

// lookupCompositeIndexDirectory handles lookups for composite index directories.
// Composite indexes span multiple columns and use dot-separated names like .last_name.first_name/.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - name: The full dotfile name (e.g., ".last_name.first_name")
//   - columns: The parsed column names (e.g., ["last_name", "first_name"])
//
// Verifies that a composite index exists with exactly these columns in this order.
// Returns ENOENT if no matching composite index is found.
func (t *TableNode) lookupCompositeIndexDirectory(ctx context.Context, name string, columns []string) (*fs.Inode, syscall.Errno) {
	logging.Debug("Looking up composite index directory",
		zap.String("table", t.tableName),
		zap.Strings("columns", columns))

	// Get all composite indexes and find one that matches these columns exactly
	compositeIndexes, err := t.db.GetCompositeIndexes(ctx, t.schema, t.tableName)
	if err != nil {
		logging.Error("Failed to get composite indexes",
			zap.String("table", t.tableName),
			zap.Error(err))
		return nil, syscall.EIO
	}

	// Find index with matching columns
	var matchingIdx *db.Index
	for i := range compositeIndexes {
		idx := &compositeIndexes[i]
		if idx.IsPrimary {
			continue
		}
		if columnsMatch(idx.Columns, columns) {
			matchingIdx = idx
			break
		}
	}

	if matchingIdx == nil {
		logging.Debug("No composite index found for columns",
			zap.String("table", t.tableName),
			zap.Strings("columns", columns))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	compositeNode := NewCompositeIndexNode(t.cfg, t.db, t.schema, t.tableName, columns, matchingIdx, t.partialRows)
	child := t.NewPersistentInode(ctx, compositeNode, stableAttr)

	logging.Debug("Created composite index directory node",
		zap.String("table", t.tableName),
		zap.Strings("columns", columns),
		zap.String("index", matchingIdx.Name))

	return child, 0
}

// columnsMatch checks if two column slices are equal (same length and elements).
func columnsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Unlink deletes a row file (rm /users/123.csv)
func (t *TableNode) Unlink(ctx context.Context, name string) syscall.Errno {
	logging.Debug("TableNode.Unlink called",
		zap.String("table", t.tableName),
		zap.String("name", name))

	// Can't delete metadata files
	if name == ".columns" || name == ".schema" || name == ".count" {
		logging.Debug("Cannot delete metadata file",
			zap.String("table", t.tableName),
			zap.String("file", name))
		return syscall.EACCES
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
	rowDirNode := NewRowDirectoryNode(t.cfg, t.db, t.schema, t.tableName, pkColumn, pkValue, t.partialRows)
	child := t.NewPersistentInode(ctx, rowDirNode, stableAttr)

	logging.Debug("Row directory created",
		zap.String("table", t.tableName),
		zap.String("pk", pkValue))

	return child, 0
}
