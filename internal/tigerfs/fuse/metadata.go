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

// MetadataFileNode represents a metadata file (.columns, .schema, .count)
type MetadataFileNode struct {
	fs.Inode

	cfg       *config.Config
	db        *db.Client
	schema    string
	tableName string
	fileType  string // "columns", "schema", "count"

	// Cached metadata content
	data []byte
}

var _ fs.InodeEmbedder = (*MetadataFileNode)(nil)
var _ fs.NodeOpener = (*MetadataFileNode)(nil)
var _ fs.NodeGetattrer = (*MetadataFileNode)(nil)

// NewMetadataFileNode creates a new metadata file node
func NewMetadataFileNode(cfg *config.Config, dbClient *db.Client, schema, tableName, fileType string) *MetadataFileNode {
	return &MetadataFileNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		fileType:  fileType,
	}
}

// Getattr returns attributes for the metadata file
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

// Open opens the metadata file for reading
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

// fetchData retrieves the metadata content based on type
func (m *MetadataFileNode) fetchData(ctx context.Context) error {
	switch m.fileType {
	case "columns":
		return m.fetchColumns(ctx)
	case "schema":
		return m.fetchSchema(ctx)
	case "count":
		return m.fetchCount(ctx)
	default:
		return fmt.Errorf("unknown metadata file type: %s", m.fileType)
	}
}

// fetchColumns retrieves the list of column names
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

// fetchSchema retrieves the CREATE TABLE statement
func (m *MetadataFileNode) fetchSchema(ctx context.Context) error {
	ddl, err := m.db.GetTableDDL(ctx, m.schema, m.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table DDL: %w", err)
	}

	m.data = []byte(ddl)
	return nil
}

// fetchCount retrieves the row count
func (m *MetadataFileNode) fetchCount(ctx context.Context) error {
	count, err := m.db.GetRowCount(ctx, m.schema, m.tableName)
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

// MetadataFileHandle represents an open file handle for reading metadata
type MetadataFileHandle struct {
	data []byte
}

var _ fs.FileReader = (*MetadataFileHandle)(nil)

// Read reads metadata from the file
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
