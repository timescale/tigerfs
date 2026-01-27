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
	db *db.Client

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
}

var _ fs.InodeEmbedder = (*IndexNode)(nil)
var _ fs.NodeGetattrer = (*IndexNode)(nil)
var _ fs.NodeReaddirer = (*IndexNode)(nil)
var _ fs.NodeLookuper = (*IndexNode)(nil)

// NewIndexNode creates a new index directory node.
//
// Parameters:
//   - cfg: Filesystem configuration
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - column: Indexed column name
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewIndexNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName, column string, index *db.Index, partialRows *PartialRowTracker) *IndexNode {
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
	// Each value is shown as a directory (can navigate into it)
	entries := make([]fuse.DirEntry, 0, len(values))
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

// Lookup handles access to a specific value within the index.
// For example, looking up "foo@example.com" within .email/.
//
// The lookup queries for rows matching this column value:
//   - If exactly one row matches: returns that row (as directory or file)
//   - If multiple rows match: returns a directory listing matching PKs
//
// This is implemented in Task 4.3; for now returns ENOENT.
func (n *IndexNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", name))

	// Task 4.3 will implement full index-based queries
	// For now, just verify the value exists in the index

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

	// Create an IndexValueNode to handle the result
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	valueNode := NewIndexValueNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.column, name, pkColumn, rows, n.partialRows)
	child := n.NewPersistentInode(ctx, valueNode, stableAttr)

	return child, 0
}

// IndexValueNode represents the result of an index lookup (e.g., .email/foo@x.com/).
// Contains the rows that match the index value.
type IndexValueNode struct {
	fs.Inode

	// cfg holds filesystem configuration
	cfg *config.Config

	// db is the database client
	db *db.Client

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
func NewIndexValueNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName, column, value, pkColumn string, matchingPKs []string, partialRows *PartialRowTracker) *IndexValueNode {
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
func (n *IndexValueNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("IndexValueNode.Readdir called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value),
		zap.Int("match_count", len(n.matchingPKs)))

	// Convert matching PKs to directory entries
	entries := make([]fuse.DirEntry, 0, len(n.matchingPKs))
	for _, pk := range n.matchingPKs {
		entries = append(entries, fuse.DirEntry{
			Name: pk,
			Mode: syscall.S_IFREG, // PKs shown as files (row-as-file)
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles access to a specific row within the index value results.
func (n *IndexValueNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("IndexValueNode.Lookup called",
		zap.String("table", n.tableName),
		zap.String("column", n.column),
		zap.String("value", n.value),
		zap.String("name", name))

	// Check if name is one of the matching PKs
	found := false
	for _, pk := range n.matchingPKs {
		if pk == name {
			found = true
			break
		}
	}

	if !found {
		return nil, syscall.ENOENT
	}

	// Create a row directory node for this PK
	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	rowDirNode := NewRowDirectoryNode(n.cfg, n.db, n.cache, n.schema, n.tableName, n.pkColumn, name, n.partialRows)
	child := n.NewPersistentInode(ctx, rowDirNode, stableAttr)

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
	db *db.Client

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
//   - dbClient: Database client for queries
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - columns: All column names in the composite index (in index order)
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewCompositeIndexNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName string, columns []string, index *db.Index, partialRows *PartialRowTracker) *CompositeIndexNode {
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
	db *db.Client

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
//   - dbClient: Database client
//   - cache: Metadata cache for permission lookups (may be nil for fallback to 0644)
//   - schema: Schema name
//   - tableName: Table name
//   - columns: All column names in the composite index
//   - values: Values specified so far (for columns[0] through columns[len(values)-1])
//   - index: Full index metadata
//   - partialRows: Tracker for incremental row creation
func NewCompositeIndexLevelNode(cfg *config.Config, dbClient *db.Client, cache *MetadataCache, schema, tableName string, columns, values []string, index *db.Index, partialRows *PartialRowTracker) *CompositeIndexLevelNode {
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
