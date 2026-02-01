// Package nfs provides NFS v3 server functionality for TigerFS on macOS.
//
// macOS users cannot use FUSE without installing third-party kernel extensions
// (MacFUSE) or FUSE-T. This package provides an alternative that uses the
// built-in NFS client, requiring no additional software.
//
// The NFS server exposes the same filesystem structure as the FUSE implementation:
//
//	/                    - Root directory (tables from default schema)
//	/tablename/          - Table directory (rows by primary key)
//	/tablename/123/      - Row directory (column files)
//	/tablename/123.csv   - Row file (data in CSV format)
//	/tablename/123/name.txt - Column file (single column value)
//	/.schemas/           - All schemas
//	/.schemas/public/    - Tables in public schema
package nfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/util"
	"go.uber.org/zap"
)

// Format file extensions available in row directories
var rowFormatFiles = []string{".json", ".csv", ".tsv", ".yaml"}

// Filesystem implements billy.Filesystem by bridging to TigerFS database operations.
// This allows the NFS server to expose PostgreSQL tables as files and directories.
type Filesystem struct {
	cfg    *config.Config
	db     *db.Client
	ctx    context.Context
	cancel context.CancelFunc

	// defaultSchema is resolved from PostgreSQL if not configured
	defaultSchema     string
	defaultSchemaMu   sync.RWMutex
	schemaInitialized bool
}

// NewFilesystem creates a new TigerFS filesystem for NFS.
func NewFilesystem(ctx context.Context, cfg *config.Config, dbClient *db.Client) *Filesystem {
	ctx, cancel := context.WithCancel(ctx)
	return &Filesystem{
		cfg:           cfg,
		db:            dbClient,
		ctx:           ctx,
		cancel:        cancel,
		defaultSchema: cfg.DefaultSchema,
	}
}

// Close releases resources associated with the filesystem.
func (fs *Filesystem) Close() error {
	fs.cancel()
	return nil
}

// ensureDefaultSchema resolves the default schema from PostgreSQL if not configured.
func (fs *Filesystem) ensureDefaultSchema() error {
	fs.defaultSchemaMu.RLock()
	if fs.schemaInitialized {
		fs.defaultSchemaMu.RUnlock()
		return nil
	}
	fs.defaultSchemaMu.RUnlock()

	fs.defaultSchemaMu.Lock()
	defer fs.defaultSchemaMu.Unlock()

	// Double-check after acquiring write lock
	if fs.schemaInitialized {
		return nil
	}

	if fs.defaultSchema == "" {
		schema, err := fs.db.GetCurrentSchema(fs.ctx)
		if err != nil {
			logging.Warn("Failed to get current_schema, defaulting to 'public'", zap.Error(err))
			fs.defaultSchema = "public"
		} else {
			fs.defaultSchema = schema
		}
		logging.Info("Resolved default schema", zap.String("schema", fs.defaultSchema))
	}

	fs.schemaInitialized = true
	return nil
}

// pathInfo holds parsed path components
type pathInfo struct {
	schema    string
	table     string
	pkValue   string
	formatExt string // e.g., "csv", "json", "tsv", "yaml" or "" for directory
	column    string // column name (with possible extension)
	isRowDir  bool   // true if accessing /table/pk/ as directory (no format ext)
}

// hasFormatExtension checks if a filename has an explicit format extension
func hasFormatExtension(name string) bool {
	if idx := strings.LastIndex(name, "."); idx != -1 {
		ext := name[idx+1:]
		switch ext {
		case "csv", "json", "tsv", "yaml":
			return true
		}
	}
	return false
}

// parsePath parses a filesystem path into its components.
func (fs *Filesystem) parsePath(filepath string) pathInfo {
	_ = fs.ensureDefaultSchema()

	info := pathInfo{schema: fs.defaultSchema}

	// Clean and normalize path
	filepath = path.Clean(filepath)
	if filepath == "/" || filepath == "." || filepath == "" {
		return info
	}

	// Remove leading slash
	filepath = strings.TrimPrefix(filepath, "/")
	parts := strings.Split(filepath, "/")

	// Handle .schemas directory
	if len(parts) > 0 && parts[0] == ".schemas" {
		info.schema = "" // Signal .schemas directory
		if len(parts) == 1 {
			return info
		}
		info.schema = parts[1]
		if len(parts) == 2 {
			return info
		}
		info.table = parts[2]
		if len(parts) == 3 {
			return info
		}
		// Check if the filename has an explicit extension BEFORE parsing
		info.isRowDir = !hasFormatExtension(parts[3])
		info.pkValue, info.formatExt = util.ParseRowFilename(parts[3])
		if len(parts) >= 5 {
			info.column = parts[4]
		}
		return info
	}

	// Default schema paths
	if len(parts) >= 1 {
		info.table = parts[0]
	}
	if len(parts) >= 2 {
		// Check if the filename has an explicit extension BEFORE parsing
		info.isRowDir = !hasFormatExtension(parts[1])
		info.pkValue, info.formatExt = util.ParseRowFilename(parts[1])
	}
	if len(parts) >= 3 {
		info.column = parts[2]
	}
	return info
}

// Create creates a new file. Not fully supported - returns error.
func (fs *Filesystem) Create(filename string) (billy.File, error) {
	logging.Debug("Filesystem.Create", zap.String("filename", filename))
	return nil, fmt.Errorf("create not supported: use existing row paths")
}

// Open opens a file for reading.
func (fs *Filesystem) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile opens a file with the specified flags and mode.
func (fs *Filesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	logging.Debug("Filesystem.OpenFile", zap.String("filename", filename), zap.Int("flag", flag))

	info := fs.parsePath(filename)

	// Can only open files within row paths
	if info.table == "" || info.pkValue == "" {
		return nil, os.ErrNotExist
	}

	// Get primary key column
	pk, err := fs.db.GetPrimaryKey(fs.ctx, info.schema, info.table)
	if err != nil {
		logging.Error("Failed to get primary key", zap.String("table", info.table), zap.Error(err))
		return nil, err
	}
	pkColumn := pk.Columns[0]

	// Fetch row data
	row, err := fs.db.GetRow(fs.ctx, info.schema, info.table, pkColumn, info.pkValue)
	if err != nil {
		logging.Debug("Row not found", zap.String("table", info.table), zap.String("pk", info.pkValue))
		return nil, os.ErrNotExist
	}

	// If accessing a column file (/table/pk/column.txt)
	if info.column != "" {
		// Check if it's a format file (.json, .csv, .tsv, .yaml) inside row directory
		switch info.column {
		case ".json":
			data, err := format.RowToJSON(row.Columns, row.Values)
			if err != nil {
				return nil, err
			}
			return &memFile{name: info.column, data: data}, nil
		case ".csv":
			data, err := format.RowToCSV(row.Columns, row.Values)
			if err != nil {
				return nil, err
			}
			return &memFile{name: info.column, data: data}, nil
		case ".tsv":
			data, err := format.RowToTSV(row.Columns, row.Values)
			if err != nil {
				return nil, err
			}
			return &memFile{name: info.column, data: data}, nil
		case ".yaml":
			data, err := format.RowToYAML(row.Columns, row.Values)
			if err != nil {
				return nil, err
			}
			return &memFile{name: info.column, data: data}, nil
		}
		// Otherwise it's a column file
		return fs.openColumnFile(info, row)
	}

	// If accessing a format file inside row directory (/table/pk/.json)
	if info.isRowDir {
		return nil, os.ErrNotExist // Can't open directory as file
	}

	// Convert to requested format
	var data []byte
	switch info.formatExt {
	case "csv":
		data, err = format.RowToCSV(row.Columns, row.Values)
	case "json":
		data, err = format.RowToJSON(row.Columns, row.Values)
	case "yaml":
		data, err = format.RowToYAML(row.Columns, row.Values)
	default:
		data, err = format.RowToTSV(row.Columns, row.Values)
	}
	if err != nil {
		return nil, err
	}

	return &memFile{
		name: path.Base(filename),
		data: data,
	}, nil
}

// openColumnFile opens a single column value as a file
func (fs *Filesystem) openColumnFile(info pathInfo, row *db.Row) (billy.File, error) {
	// Strip extension from column name if present
	colName := info.column
	for _, ext := range []string{".txt", ".json", ".bin", ".xml", ".wkb"} {
		if strings.HasSuffix(colName, ext) {
			colName = strings.TrimSuffix(colName, ext)
			break
		}
	}

	// Find column in row data
	for i, col := range row.Columns {
		if col == colName {
			value := ""
			if row.Values[i] != nil {
				value = fmt.Sprintf("%v", row.Values[i])
			}
			return &memFile{
				name: info.column,
				data: []byte(value + "\n"),
			}, nil
		}
	}

	return nil, os.ErrNotExist
}

// Stat returns file info for the given path.
func (fs *Filesystem) Stat(filename string) (os.FileInfo, error) {
	logging.Debug("Filesystem.Stat", zap.String("filename", filename))

	info := fs.parsePath(filename)

	// Root directory
	if info.table == "" && info.schema == fs.defaultSchema {
		return &fileInfo{name: "/", isDir: true, mode: 0755}, nil
	}

	// .schemas directory
	if info.schema == "" && info.table == "" {
		return &fileInfo{name: ".schemas", isDir: true, mode: 0755}, nil
	}

	// Schema directory (/.schemas/public)
	if info.table == "" && info.schema != fs.defaultSchema {
		schemas, err := fs.db.GetSchemas(fs.ctx)
		if err != nil {
			return nil, err
		}
		for _, s := range schemas {
			if s == info.schema {
				return &fileInfo{name: info.schema, isDir: true, mode: 0755}, nil
			}
		}
		return nil, os.ErrNotExist
	}

	// Table directory (/users)
	if info.pkValue == "" {
		tables, err := fs.db.GetTables(fs.ctx, info.schema)
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			if t == info.table {
				return &fileInfo{name: info.table, isDir: true, mode: 0755}, nil
			}
		}
		views, err := fs.db.GetViews(fs.ctx, info.schema)
		if err != nil {
			return nil, err
		}
		for _, v := range views {
			if v == info.table {
				return &fileInfo{name: info.table, isDir: true, mode: 0755}, nil
			}
		}
		return nil, os.ErrNotExist
	}

	// Get primary key and verify row exists
	pk, err := fs.db.GetPrimaryKey(fs.ctx, info.schema, info.table)
	if err != nil {
		return nil, err
	}
	pkColumn := pk.Columns[0]

	row, err := fs.db.GetRow(fs.ctx, info.schema, info.table, pkColumn, info.pkValue)
	if err != nil {
		return nil, os.ErrNotExist
	}

	// Column file (/table/pk/column.txt) or format file (/table/pk/.json)
	if info.column != "" {
		// Check if it's a format file (.json, .csv, .tsv, .yaml)
		switch info.column {
		case ".json":
			data, _ := format.RowToJSON(row.Columns, row.Values)
			return &fileInfo{name: info.column, size: int64(len(data)), mode: 0644}, nil
		case ".csv":
			data, _ := format.RowToCSV(row.Columns, row.Values)
			return &fileInfo{name: info.column, size: int64(len(data)), mode: 0644}, nil
		case ".tsv":
			data, _ := format.RowToTSV(row.Columns, row.Values)
			return &fileInfo{name: info.column, size: int64(len(data)), mode: 0644}, nil
		case ".yaml":
			data, _ := format.RowToYAML(row.Columns, row.Values)
			return &fileInfo{name: info.column, size: int64(len(data)), mode: 0644}, nil
		}

		// Otherwise it's a column file
		colName := info.column
		for _, ext := range []string{".txt", ".json", ".bin", ".xml", ".wkb"} {
			if strings.HasSuffix(colName, ext) {
				colName = strings.TrimSuffix(colName, ext)
				break
			}
		}
		for i, col := range row.Columns {
			if col == colName {
				value := ""
				if row.Values[i] != nil {
					value = fmt.Sprintf("%v", row.Values[i])
				}
				return &fileInfo{name: info.column, size: int64(len(value) + 1), mode: 0644}, nil
			}
		}
		return nil, os.ErrNotExist
	}

	// Row directory (/table/pk/) - no format extension
	if info.isRowDir {
		return &fileInfo{name: info.pkValue, isDir: true, mode: 0755}, nil
	}

	// Row file with format extension (/table/pk.csv)
	var data []byte
	switch info.formatExt {
	case "csv":
		data, _ = format.RowToCSV(row.Columns, row.Values)
	case "json":
		data, _ = format.RowToJSON(row.Columns, row.Values)
	case "yaml":
		data, _ = format.RowToYAML(row.Columns, row.Values)
	default:
		data, _ = format.RowToTSV(row.Columns, row.Values)
	}

	name := info.pkValue
	if info.formatExt != "" {
		name = info.pkValue + "." + info.formatExt
	}

	return &fileInfo{name: name, size: int64(len(data)), mode: 0644}, nil
}

// Rename is not supported.
func (fs *Filesystem) Rename(oldpath, newpath string) error {
	return fmt.Errorf("rename not supported")
}

// Remove is not fully supported.
func (fs *Filesystem) Remove(filename string) error {
	logging.Debug("Filesystem.Remove", zap.String("filename", filename))

	info := fs.parsePath(filename)
	if info.table == "" || info.pkValue == "" {
		return fmt.Errorf("can only remove row files")
	}

	pk, err := fs.db.GetPrimaryKey(fs.ctx, info.schema, info.table)
	if err != nil {
		return err
	}

	return fs.db.DeleteRow(fs.ctx, info.schema, info.table, pk.Columns[0], info.pkValue)
}

// Join joins path elements.
func (fs *Filesystem) Join(elem ...string) string {
	return path.Join(elem...)
}

// TempFile creates a temporary file. Not supported.
func (fs *Filesystem) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("temp files not supported")
}

// ReadDir returns directory entries for the given path.
func (fs *Filesystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	logging.Debug("Filesystem.ReadDir", zap.String("dirname", dirname))

	info := fs.parsePath(dirname)

	// .schemas directory listing
	if info.schema == "" && info.table == "" {
		schemas, err := fs.db.GetSchemas(fs.ctx)
		if err != nil {
			return nil, err
		}
		entries := make([]os.FileInfo, len(schemas))
		for i, s := range schemas {
			entries[i] = &fileInfo{name: s, isDir: true, mode: 0755}
		}
		return entries, nil
	}

	// Root or schema directory - list tables
	if info.table == "" {
		tables, err := fs.db.GetTables(fs.ctx, info.schema)
		if err != nil {
			return nil, err
		}
		views, err := fs.db.GetViews(fs.ctx, info.schema)
		if err != nil {
			logging.Warn("Failed to get views", zap.Error(err))
			views = []string{}
		}

		entries := make([]os.FileInfo, 0, len(tables)+len(views)+1)

		// Add .schemas directory at root level only
		if info.schema == fs.defaultSchema {
			entries = append(entries, &fileInfo{name: ".schemas", isDir: true, mode: 0755})
		}

		for _, t := range tables {
			entries = append(entries, &fileInfo{name: t, isDir: true, mode: 0755})
		}
		for _, v := range views {
			entries = append(entries, &fileInfo{name: v, isDir: true, mode: 0755})
		}
		return entries, nil
	}

	// Row directory - list columns and format files
	if info.pkValue != "" && info.isRowDir {
		return fs.readRowDir(info)
	}

	// Table directory - list rows
	pk, err := fs.db.GetPrimaryKey(fs.ctx, info.schema, info.table)
	if err != nil {
		return nil, err
	}
	pkColumn := pk.Columns[0]

	rows, err := fs.db.ListRows(fs.ctx, info.schema, info.table, pkColumn, fs.cfg.DirListingLimit)
	if err != nil {
		return nil, err
	}

	// Each row appears as a directory (can cd into it)
	entries := make([]os.FileInfo, len(rows))
	for i, rowPK := range rows {
		entries[i] = &fileInfo{name: rowPK, isDir: true, mode: 0755}
	}

	return entries, nil
}

// readRowDir lists the contents of a row directory (columns + format files)
func (fs *Filesystem) readRowDir(info pathInfo) ([]os.FileInfo, error) {
	columns, err := fs.db.GetColumns(fs.ctx, info.schema, info.table)
	if err != nil {
		return nil, err
	}

	entries := make([]os.FileInfo, 0, len(columns)+len(rowFormatFiles))

	// Add column files with appropriate extensions
	for _, col := range columns {
		filename := col.Name
		ext := getExtensionForType(col.DataType)
		if ext != "" {
			filename = col.Name + ext
		}
		entries = append(entries, &fileInfo{name: filename, mode: 0644})
	}

	// Add format files (.json, .csv, .tsv, .yaml)
	for _, formatFile := range rowFormatFiles {
		entries = append(entries, &fileInfo{name: formatFile, mode: 0644})
	}

	return entries, nil
}

// getExtensionForType returns file extension for a PostgreSQL data type
func getExtensionForType(dataType string) string {
	dt := strings.ToLower(dataType)

	// Handle parameterized types
	if idx := strings.Index(dt, "("); idx > 0 {
		dt = strings.TrimSpace(dt[:idx])
	}

	switch dt {
	case "text", "character varying", "varchar", "character", "char", "bpchar":
		return ".txt"
	case "json", "jsonb":
		return ".json"
	case "xml":
		return ".xml"
	case "bytea":
		return ".bin"
	case "geometry", "geography":
		return ".wkb"
	default:
		return ""
	}
}

// MkdirAll creates directories. Not supported for NFS.
func (fs *Filesystem) MkdirAll(filename string, perm os.FileMode) error {
	return fmt.Errorf("mkdir not supported")
}

// Lstat is the same as Stat (no symlinks).
func (fs *Filesystem) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

// Symlink is not supported.
func (fs *Filesystem) Symlink(target, link string) error {
	return fmt.Errorf("symlinks not supported")
}

// Readlink is not supported.
func (fs *Filesystem) Readlink(link string) (string, error) {
	return "", fmt.Errorf("symlinks not supported")
}

// Chroot returns a filesystem rooted at the given path.
func (fs *Filesystem) Chroot(p string) (billy.Filesystem, error) {
	return &chrootFilesystem{fs: fs, root: p}, nil
}

// Root returns the root path of the filesystem.
func (fs *Filesystem) Root() string {
	return "/"
}

// chrootFilesystem wraps a Filesystem with a root prefix.
type chrootFilesystem struct {
	fs   *Filesystem
	root string
}

func (c *chrootFilesystem) Create(filename string) (billy.File, error) {
	return c.fs.Create(path.Join(c.root, filename))
}

func (c *chrootFilesystem) Open(filename string) (billy.File, error) {
	return c.fs.Open(path.Join(c.root, filename))
}

func (c *chrootFilesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	return c.fs.OpenFile(path.Join(c.root, filename), flag, perm)
}

func (c *chrootFilesystem) Stat(filename string) (os.FileInfo, error) {
	return c.fs.Stat(path.Join(c.root, filename))
}

func (c *chrootFilesystem) Rename(oldpath, newpath string) error {
	return c.fs.Rename(path.Join(c.root, oldpath), path.Join(c.root, newpath))
}

func (c *chrootFilesystem) Remove(filename string) error {
	return c.fs.Remove(path.Join(c.root, filename))
}

func (c *chrootFilesystem) Join(elem ...string) string {
	return c.fs.Join(elem...)
}

func (c *chrootFilesystem) TempFile(dir, prefix string) (billy.File, error) {
	return c.fs.TempFile(path.Join(c.root, dir), prefix)
}

func (c *chrootFilesystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.fs.ReadDir(path.Join(c.root, dirname))
}

func (c *chrootFilesystem) MkdirAll(filename string, perm os.FileMode) error {
	return c.fs.MkdirAll(path.Join(c.root, filename), perm)
}

func (c *chrootFilesystem) Lstat(filename string) (os.FileInfo, error) {
	return c.fs.Lstat(path.Join(c.root, filename))
}

func (c *chrootFilesystem) Symlink(target, link string) error {
	return c.fs.Symlink(target, path.Join(c.root, link))
}

func (c *chrootFilesystem) Readlink(link string) (string, error) {
	return c.fs.Readlink(path.Join(c.root, link))
}

func (c *chrootFilesystem) Chroot(p string) (billy.Filesystem, error) {
	return &chrootFilesystem{fs: c.fs, root: path.Join(c.root, p)}, nil
}

func (c *chrootFilesystem) Root() string {
	return c.root
}

// fileInfo implements os.FileInfo for directory entries.
type fileInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	isDir bool
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() os.FileMode  { return fi.mode | fi.dirMode() }
func (fi *fileInfo) ModTime() time.Time { return time.Now() }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() interface{}   { return nil }

func (fi *fileInfo) dirMode() os.FileMode {
	if fi.isDir {
		return os.ModeDir
	}
	return 0
}

// memFile is an in-memory file for reading row data.
type memFile struct {
	name   string
	data   []byte
	offset int64
}

func (f *memFile) Name() string {
	return f.name
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.offset:])
	f.offset += int64(n)
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(len(f.data)) + offset
	}
	return f.offset, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("write not supported")
}

func (f *memFile) Close() error {
	return nil
}

func (f *memFile) Lock() error {
	return nil
}

func (f *memFile) Unlock() error {
	return nil
}

func (f *memFile) Truncate(size int64) error {
	return fmt.Errorf("truncate not supported")
}
