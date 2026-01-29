package fuse

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// MetadataFileNode represents a metadata file (.columns, .schema, .ddl, .count, .indexes).
// These read-only files provide table/view metadata without requiring SQL queries.
//
// Supported file types:
//   - "columns": Lists column names, one per line
//   - "schema": Shows the CREATE TABLE/VIEW DDL statement (fast)
//   - "ddl": Shows complete DDL (table/view, indexes, constraints, triggers, comments)
//   - "count": Shows row count (exact for small tables, estimated for large)
//   - "indexes": Lists available index navigation paths with annotations
//
// The .count file uses an adaptive strategy: exact COUNT(*) for tables with
// fewer than 100K rows (estimated), and pg_class.reltuples estimate for larger
// tables to avoid expensive full table scans.
type MetadataFileNode struct {
	fs.Inode

	cfg       *config.Config // TigerFS configuration
	db        db.DBClient    // Database client for queries
	schema    string         // PostgreSQL schema name
	tableName string         // Table/view this metadata describes
	fileType  string         // Metadata type: "columns", "schema", or "count"
	isView    bool           // true if this metadata is for a view, not a table

	// data holds the cached metadata content, fetched on first access.
	// Cached to avoid repeated database queries during file operations.
	data []byte
}

var _ fs.InodeEmbedder = (*MetadataFileNode)(nil)
var _ fs.NodeOpener = (*MetadataFileNode)(nil)
var _ fs.NodeGetattrer = (*MetadataFileNode)(nil)

// NewMetadataFileNode creates a new metadata file node for a table.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table this metadata describes
//   - fileType: Type of metadata file ("columns", "schema", or "count")
//
// Returns a new MetadataFileNode ready for FUSE operations.
func NewMetadataFileNode(cfg *config.Config, dbClient db.DBClient, schema, tableName, fileType string) *MetadataFileNode {
	return &MetadataFileNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		fileType:  fileType,
		isView:    false,
	}
}

// NewViewMetadataFileNode creates a new metadata file node for a view.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//   - schema: PostgreSQL schema name
//   - viewName: Name of the view this metadata describes
//   - fileType: Type of metadata file ("columns", "schema", or "count")
//
// Returns a new MetadataFileNode ready for FUSE operations.
func NewViewMetadataFileNode(cfg *config.Config, dbClient db.DBClient, schema, viewName, fileType string) *MetadataFileNode {
	return &MetadataFileNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: viewName,
		fileType:  fileType,
		isView:    true,
	}
}

// Getattr returns attributes for the metadata file.
// Fetches the metadata content to determine accurate file size.
//
// Parameters:
//   - ctx: Context for cancellation
//   - fh: File handle (unused for getattr)
//   - out: Output structure for file attributes
//
// Returns 0 on success, EIO if metadata fetch fails.
func (m *MetadataFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("MetadataFileNode.Getattr called",
		zap.String("table", m.tableName),
		zap.String("type", m.fileType))

	// Fetch metadata to get size
	if m.data == nil {
		if err := m.fetchData(ctx); err != nil {
			logging.Error("Failed to fetch metadata for getattr",
				zap.String("table", m.tableName),
				zap.String("type", m.fileType),
				zap.Error(err))
			return syscall.EIO
		}
	}

	out.Mode = 0444 | syscall.S_IFREG // Read-only
	out.Nlink = 1
	out.Size = uint64(len(m.data))

	return 0
}

// Open opens the metadata file for reading.
// Fetches the metadata content if not already cached.
//
// Parameters:
//   - ctx: Context for cancellation
//   - flags: Open flags (ignored, metadata files are read-only)
//
// Returns a file handle for reading, FOPEN_DIRECT_IO flag, and errno.
// Returns EIO if metadata fetch fails.
func (m *MetadataFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("MetadataFileNode.Open called",
		zap.String("table", m.tableName),
		zap.String("type", m.fileType))

	// Fetch metadata if not already cached
	if m.data == nil {
		if err := m.fetchData(ctx); err != nil {
			logging.Error("Failed to fetch metadata",
				zap.String("table", m.tableName),
				zap.String("type", m.fileType),
				zap.Error(err))
			return nil, 0, syscall.EIO
		}
	}

	// Create file handle
	fh := &MetadataFileHandle{
		data: m.data,
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// fetchData retrieves the metadata content based on file type.
// Dispatches to the appropriate fetch method for columns, schema, or count.
// Caches the result in m.data to avoid repeated database queries.
//
// Returns error if the database query fails or the file type is unknown.
func (m *MetadataFileNode) fetchData(ctx context.Context) error {
	switch m.fileType {
	case "columns":
		return m.fetchColumns(ctx)
	case "schema":
		return m.fetchSchema(ctx)
	case "ddl":
		return m.fetchDDL(ctx)
	case "count":
		return m.fetchCount(ctx)
	case "indexes":
		return m.fetchIndexes(ctx)
	default:
		return fmt.Errorf("unknown metadata file type: %s", m.fileType)
	}
}

// fetchColumns retrieves the list of column names for the table.
// Returns one column name per line, in schema order.
//
// Returns error if the database query fails.
func (m *MetadataFileNode) fetchColumns(ctx context.Context) error {
	columns, err := m.db.GetColumns(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Build column list (one per line)
	var lines []string
	for _, col := range columns {
		lines = append(lines, col.Name)
	}

	m.data = []byte(strings.Join(lines, "\n") + "\n")
	return nil
}

// fetchSchema retrieves the CREATE statement for a table or view.
// For tables, returns the CREATE TABLE DDL with constraints.
// For views, returns the CREATE VIEW AS SELECT statement.
//
// The .info/ddl file provides additional metadata (indexes, triggers, etc.).
//
// Returns error if the database query fails.
func (m *MetadataFileNode) fetchSchema(ctx context.Context) error {
	if m.isView {
		// For views, show the CREATE VIEW statement
		viewDef, err := m.db.GetViewDefinition(ctx, m.schema, m.tableName)
		if err != nil {
			return fmt.Errorf("failed to get view definition: %w", err)
		}

		var sb strings.Builder
		sb.WriteString("CREATE VIEW ")
		sb.WriteString(m.tableName)
		sb.WriteString(" AS\n")
		sb.WriteString(viewDef)
		if !strings.HasSuffix(viewDef, "\n") {
			sb.WriteString("\n")
		}

		m.data = []byte(sb.String())
		return nil
	}

	// For tables, get the CREATE TABLE DDL
	ddl, err := m.db.GetTableDDL(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table DDL: %w", err)
	}

	m.data = []byte(ddl)
	return nil
}

// fetchDDL retrieves the complete DDL for the table/view.
// For tables includes:
// - CREATE TABLE statement
// - Indexes
// - Foreign keys
// - Check constraints
// - Triggers
// - Comments
//
// For views includes:
// - CREATE VIEW statement
// - Dependent views
//
// Returns error if any database query fails.
func (m *MetadataFileNode) fetchDDL(ctx context.Context) error {
	if m.isView {
		// For views, get the view definition and dependent views
		viewDef, err := m.db.GetViewDefinition(ctx, m.schema, m.tableName)
		if err != nil {
			return fmt.Errorf("failed to get view definition: %w", err)
		}

		var sb strings.Builder
		sb.WriteString("-- VIEW: ")
		sb.WriteString(m.tableName)
		sb.WriteString("\n\n")
		sb.WriteString("CREATE VIEW ")
		sb.WriteString(m.tableName)
		sb.WriteString(" AS\n")
		sb.WriteString(viewDef)
		sb.WriteString("\n")

		// Get dependent views
		dependents, err := m.db.GetDependentViews(ctx, m.schema, m.tableName)
		if err == nil && len(dependents) > 0 {
			sb.WriteString("\n-- Dependent views:\n")
			for _, dep := range dependents {
				sb.WriteString("--   ")
				sb.WriteString(dep)
				sb.WriteString("\n")
			}
		}

		m.data = []byte(sb.String())
		return nil
	}

	ddl, err := m.db.GetFullDDL(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get full DDL: %w", err)
	}

	m.data = []byte(ddl)
	return nil
}

// fetchIndexes retrieves the list of available index navigation paths.
// Lists both single-column and composite indexes with annotations:
//   - (unique) for unique indexes
//   - (composite) for multi-column indexes
//
// Primary key indexes are excluded since rows are already accessible by PK.
// Returns error if the database query fails.
func (m *MetadataFileNode) fetchIndexes(ctx context.Context) error {
	// Get single-column indexes
	singleIndexes, err := m.db.GetSingleColumnIndexes(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get single-column indexes: %w", err)
	}

	// Get composite indexes
	compositeIndexes, err := m.db.GetCompositeIndexes(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get composite indexes: %w", err)
	}

	var lines []string

	// Add single-column indexes (skip primary key)
	for _, idx := range singleIndexes {
		if idx.IsPrimary {
			continue
		}
		if len(idx.Columns) > 0 {
			line := "." + idx.Columns[0] + "/"
			if idx.IsUnique {
				line += "                    (unique)"
			}
			lines = append(lines, line)
		}
	}

	// Add composite indexes (skip primary key)
	for _, idx := range compositeIndexes {
		if idx.IsPrimary {
			continue
		}
		line := FormatCompositeIndexName(idx.Columns) + "/"
		annotation := "(composite"
		if idx.IsUnique {
			annotation += ", unique"
		}
		annotation += ")"
		line += "                    " + annotation
		lines = append(lines, line)
	}

	if len(lines) == 0 {
		m.data = []byte("(no indexes)\n")
	} else {
		m.data = []byte(strings.Join(lines, "\n") + "\n")
	}
	return nil
}

// fetchCount retrieves the row count for the table using an adaptive strategy.
// For small tables (< 100K rows estimated), performs exact COUNT(*).
// For large tables (>= 100K rows estimated), returns the pg_class.reltuples estimate
// to avoid expensive full table scans.
//
// The estimate is based on PostgreSQL statistics maintained by VACUUM and ANALYZE.
// Returns error if the database query fails.
func (m *MetadataFileNode) fetchCount(ctx context.Context) error {
	count, err := m.db.GetRowCountSmart(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}

	if m.cfg.TrailingNewlines {
		m.data = []byte(fmt.Sprintf("%d\n", count))
	} else {
		m.data = []byte(fmt.Sprintf("%d", count))
	}
	return nil
}

// MetadataFileHandle represents an open file handle for reading metadata.
// Holds the fetched metadata content for read operations.
type MetadataFileHandle struct {
	// data contains the cached metadata content to be read.
	data []byte
}

var _ fs.FileReader = (*MetadataFileHandle)(nil)

// Read reads metadata from the file at the specified offset.
// Handles partial reads and returns empty data at EOF.
//
// Parameters:
//   - ctx: Context for cancellation (unused)
//   - dest: Destination buffer (determines max read size)
//   - off: Byte offset to start reading from
//
// Returns the read data and 0 on success.
func (fh *MetadataFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	logging.Debug("MetadataFileHandle.Read called", zap.Int64("offset", off), zap.Int("size", len(dest)))

	// Calculate read bounds
	end := off + int64(len(dest))
	if end > int64(len(fh.data)) {
		end = int64(len(fh.data))
	}

	// Handle EOF
	if off >= int64(len(fh.data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	// Return data slice
	return fuse.ReadResultData(fh.data[off:end]), 0
}
