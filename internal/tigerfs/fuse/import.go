package fuse

import (
	"context"
	"fmt"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// ImportMode represents the type of import operation.
type ImportMode string

const (
	ImportModeOverwrite ImportMode = "overwrite" // Truncate + insert
	ImportModeSync      ImportMode = "sync"      // Upsert (requires PK)
	ImportModeAppend    ImportMode = "append"    // Insert only (fail on conflict)
)

// ImportDirNode represents the .import/ directory.
// Lists available import modes (.overwrite, .sync, .append).
type ImportDirNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
}

var _ fs.InodeEmbedder = (*ImportDirNode)(nil)
var _ fs.NodeGetattrer = (*ImportDirNode)(nil)
var _ fs.NodeReaddirer = (*ImportDirNode)(nil)
var _ fs.NodeLookuper = (*ImportDirNode)(nil)

// NewImportDirNode creates a new .import/ directory node for a table.
func NewImportDirNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string) *ImportDirNode {
	return &ImportDirNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
	}
}

// Getattr returns attributes for the import directory.
func (i *ImportDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ImportDirNode.Getattr called", zap.String("table", i.tableName))

	out.Mode = 0700 | syscall.S_IFDIR // Read-write directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists available import modes.
func (i *ImportDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("ImportDirNode.Readdir called", zap.String("table", i.tableName))

	entries := []fuse.DirEntry{
		{Name: DirAppend, Mode: syscall.S_IFDIR},
		{Name: DirOverwrite, Mode: syscall.S_IFDIR},
		{Name: DirSync, Mode: syscall.S_IFDIR},
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up an import mode directory.
func (i *ImportDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("ImportDirNode.Lookup called",
		zap.String("table", i.tableName),
		zap.String("name", name))

	var mode ImportMode
	switch name {
	case DirOverwrite:
		mode = ImportModeOverwrite
	case DirSync:
		mode = ImportModeSync
	case DirAppend:
		mode = ImportModeAppend
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFDIR,
	}

	modeNode := NewImportModeNode(i.cfg, i.db, i.schema, i.tableName, mode)
	child := i.NewPersistentInode(ctx, modeNode, stableAttr)
	return child, 0
}

// ImportModeNode represents an import mode directory (.overwrite/, .sync/, .append/).
// Lists available formats (csv, tsv, json, yaml).
type ImportModeNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
	mode      ImportMode
}

var _ fs.InodeEmbedder = (*ImportModeNode)(nil)
var _ fs.NodeGetattrer = (*ImportModeNode)(nil)
var _ fs.NodeReaddirer = (*ImportModeNode)(nil)
var _ fs.NodeLookuper = (*ImportModeNode)(nil)

// NewImportModeNode creates a new import mode directory node.
func NewImportModeNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string, mode ImportMode) *ImportModeNode {
	return &ImportModeNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		mode:      mode,
	}
}

// Getattr returns attributes for the import mode directory.
func (m *ImportModeNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ImportModeNode.Getattr called",
		zap.String("table", m.tableName),
		zap.String("mode", string(m.mode)))

	out.Mode = 0700 | syscall.S_IFDIR // Read-write directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists available import formats and options.
func (m *ImportModeNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("ImportModeNode.Readdir called",
		zap.String("table", m.tableName),
		zap.String("mode", string(m.mode)))

	entries := []fuse.DirEntry{
		{Name: DirNoHeaders, Mode: syscall.S_IFDIR}, // .no-headers/ option
		{Name: FmtCSV, Mode: syscall.S_IFREG},
		{Name: FmtJSON, Mode: syscall.S_IFREG},
		{Name: FmtTSV, Mode: syscall.S_IFREG},
		{Name: FmtYAML, Mode: syscall.S_IFREG},
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a format file or .no-headers option for import.
func (m *ImportModeNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("ImportModeNode.Lookup called",
		zap.String("table", m.tableName),
		zap.String("mode", string(m.mode)),
		zap.String("name", name))

	// Handle .no-headers option directory
	if name == DirNoHeaders {
		stableAttr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
		}
		optNode := &ImportNoHeadersNode{
			cfg:       m.cfg,
			db:        m.db,
			schema:    m.schema,
			tableName: m.tableName,
			mode:      m.mode,
		}
		child := m.NewPersistentInode(ctx, optNode, stableAttr)
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

	fileNode := NewImportFileNode(m.cfg, m.db, m.schema, m.tableName, m.mode, name, false)
	child := m.NewPersistentInode(ctx, fileNode, stableAttr)
	return child, 0
}

// ImportFileNode represents a format file in an import mode directory.
// Receives data on write and executes the import on release.
type ImportFileNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
	mode      ImportMode
	format    string
	noHeaders bool // If true, data has no header row; use schema column order
}

var _ fs.InodeEmbedder = (*ImportFileNode)(nil)
var _ fs.NodeGetattrer = (*ImportFileNode)(nil)
var _ fs.NodeSetattrer = (*ImportFileNode)(nil)
var _ fs.NodeOpener = (*ImportFileNode)(nil)

// NewImportFileNode creates a new import file node.
func NewImportFileNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string, mode ImportMode, format string, noHeaders bool) *ImportFileNode {
	return &ImportFileNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
		mode:      mode,
		format:    format,
		noHeaders: noHeaders,
	}
}

// Getattr returns attributes for the import file.
func (f *ImportFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ImportFileNode.Getattr called",
		zap.String("table", f.tableName),
		zap.String("mode", string(f.mode)),
		zap.String("format", f.format))

	out.Mode = 0600 | syscall.S_IFREG // Read-write file
	out.Nlink = 1
	out.Size = 0

	return 0
}

// Setattr handles attribute changes (required for truncation via shell redirection).
func (f *ImportFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("ImportFileNode.Setattr called",
		zap.String("table", f.tableName),
		zap.String("mode", string(f.mode)),
		zap.String("format", f.format))

	// Accept truncation (size=0) which happens on shell redirection with >
	out.Mode = 0600 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0

	return 0
}

// Open opens the import file for writing.
func (f *ImportFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("ImportFileNode.Open called",
		zap.String("table", f.tableName),
		zap.String("mode", string(f.mode)),
		zap.String("format", f.format),
		zap.Bool("noHeaders", f.noHeaders),
		zap.Uint32("flags", flags))

	// Check access mode - reject read-only opens
	accessMode := flags & syscall.O_ACCMODE
	if accessMode == syscall.O_RDONLY {
		logging.Debug("Rejecting read-only open on import file",
			zap.String("table", f.tableName))
		return nil, 0, syscall.EACCES
	}

	fh := &ImportFileHandle{
		cfg:       f.cfg,
		db:        f.db,
		schema:    f.schema,
		tableName: f.tableName,
		mode:      f.mode,
		format:    f.format,
		noHeaders: f.noHeaders,
		buffer:    make([]byte, 0),
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// ImportFileHandle represents an open handle to an import file.
// Buffers data during writes and executes the import on flush/release.
type ImportFileHandle struct {
	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
	mode      ImportMode
	format    string
	noHeaders bool // If true, data has no header row; use schema column order

	mu     sync.Mutex
	buffer []byte
}

var _ fs.FileWriter = (*ImportFileHandle)(nil)
var _ fs.FileFlusher = (*ImportFileHandle)(nil)

// Write appends data to the buffer.
func (fh *ImportFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	logging.Debug("ImportFileHandle.Write called",
		zap.String("table", fh.tableName),
		zap.Int("data_len", len(data)),
		zap.Int64("offset", off))

	// Append to buffer (ignore offset for simplicity - treat as sequential write)
	fh.buffer = append(fh.buffer, data...)

	// Check size limit
	if len(fh.buffer) > fh.cfg.DirWritingLimit*1024 { // Rough estimate: 1KB per row
		logging.Error("Import data exceeds size limit",
			zap.String("table", fh.tableName),
			zap.Int("buffer_size", len(fh.buffer)))
		return 0, syscall.EFBIG
	}

	return uint32(len(data)), 0
}

// Flush executes the import when the file is closed.
func (fh *ImportFileHandle) Flush(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	logging.Debug("ImportFileHandle.Flush called",
		zap.String("table", fh.tableName),
		zap.String("mode", string(fh.mode)),
		zap.String("format", fh.format),
		zap.Bool("noHeaders", fh.noHeaders),
		zap.Int("buffer_size", len(fh.buffer)))

	if len(fh.buffer) == 0 {
		return 0 // Nothing to import
	}

	// Parse data based on format
	columns, rows, err := fh.parseData(ctx)
	if err != nil {
		logging.Error("Failed to parse import data",
			zap.String("table", fh.tableName),
			zap.String("format", fh.format),
			zap.Error(err))
		return syscall.EINVAL
	}

	logging.Debug("Parsed import data",
		zap.String("table", fh.tableName),
		zap.Int("columns", len(columns)),
		zap.Int("rows", len(rows)))

	// Check row limit
	if len(rows) > fh.cfg.DirWritingLimit {
		logging.Error("Import row count exceeds limit",
			zap.String("table", fh.tableName),
			zap.Int("rows", len(rows)),
			zap.Int("limit", fh.cfg.DirWritingLimit))
		return syscall.EFBIG
	}

	// Execute import based on mode
	if err := fh.executeImport(ctx, columns, rows); err != nil {
		logging.Error("Failed to execute import",
			zap.String("table", fh.tableName),
			zap.String("mode", string(fh.mode)),
			zap.Error(err))
		return syscall.EIO
	}

	logging.Info("Import completed successfully",
		zap.String("table", fh.tableName),
		zap.String("mode", string(fh.mode)),
		zap.Int("rows", len(rows)))

	// Clear buffer to prevent duplicate execution
	fh.buffer = nil

	return 0
}

// parseData parses the buffered data based on format.
// If noHeaders is true for CSV/TSV, fetches column names from schema.
func (fh *ImportFileHandle) parseData(ctx context.Context) ([]string, [][]interface{}, error) {
	// Handle no-headers mode for CSV/TSV
	if fh.noHeaders {
		// Fetch column names from schema
		cols, err := fh.db.GetColumns(ctx, fh.schema, fh.tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get columns from schema: %w", err)
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("no columns found for table %s.%s", fh.schema, fh.tableName)
		}

		// Extract column names in schema order
		columns := make([]string, len(cols))
		for i, col := range cols {
			columns[i] = col.Name
		}

		switch fh.format {
		case FmtCSV:
			return format.ParseCSVBulkNoHeaders(fh.buffer, columns)
		case FmtTSV:
			return format.ParseTSVBulkNoHeaders(fh.buffer, columns)
		default:
			// JSON/YAML always have column names in the data
			return nil, nil, fmt.Errorf("no-headers mode not supported for %s format", fh.format)
		}
	}

	// Standard parsing with headers
	switch fh.format {
	case FmtCSV:
		return format.ParseCSVBulk(fh.buffer)
	case FmtTSV:
		return format.ParseTSVBulk(fh.buffer)
	case FmtJSON:
		return format.ParseJSONBulk(fh.buffer)
	case FmtYAML:
		return format.ParseYAMLBulk(fh.buffer)
	default:
		return nil, nil, &unknownFormatError{format: fh.format}
	}
}

// executeImport executes the appropriate import operation.
func (fh *ImportFileHandle) executeImport(ctx context.Context, columns []string, rows [][]interface{}) error {
	switch fh.mode {
	case ImportModeOverwrite:
		return fh.db.ImportOverwrite(ctx, fh.schema, fh.tableName, columns, rows)
	case ImportModeSync:
		return fh.db.ImportSync(ctx, fh.schema, fh.tableName, columns, rows)
	case ImportModeAppend:
		return fh.db.ImportAppend(ctx, fh.schema, fh.tableName, columns, rows)
	default:
		return &unknownImportModeError{mode: string(fh.mode)}
	}
}

// ImportNoHeadersNode represents the .no-headers/ directory under an import mode.
// Only lists CSV and TSV formats since JSON/YAML always have column names in data.
type ImportNoHeadersNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
	mode      ImportMode
}

var _ fs.InodeEmbedder = (*ImportNoHeadersNode)(nil)
var _ fs.NodeGetattrer = (*ImportNoHeadersNode)(nil)
var _ fs.NodeReaddirer = (*ImportNoHeadersNode)(nil)
var _ fs.NodeLookuper = (*ImportNoHeadersNode)(nil)

// Getattr returns attributes for the .no-headers directory.
func (n *ImportNoHeadersNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0700 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists formats that support no-headers mode (csv, tsv only).
func (n *ImportNoHeadersNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: FmtCSV, Mode: syscall.S_IFREG},
		{Name: FmtTSV, Mode: syscall.S_IFREG},
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a format file with no-headers mode enabled.
func (n *ImportNoHeadersNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Only CSV and TSV support no-headers mode
	switch name {
	case FmtCSV, FmtTSV:
		// Valid
	default:
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	fileNode := NewImportFileNode(n.cfg, n.db, n.schema, n.tableName, n.mode, name, true)
	child := n.NewPersistentInode(ctx, fileNode, stableAttr)
	return child, 0
}

// Error types for import operations

type unknownImportModeError struct {
	mode string
}

func (e *unknownImportModeError) Error() string {
	return "unknown import mode: " + e.mode
}
