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

// Metadata file names inside .info/ directory (without leading dots)
const (
	InfoFileCount   = "count"
	InfoFileDDL     = "ddl"
	InfoFileSchema  = "schema"
	InfoFileColumns = "columns"
)

// InfoDirNode represents the .info/ metadata directory within a table.
// This directory contains read-only files describing the table:
//   - count: row count
//   - ddl: CREATE TABLE statement with indexes, constraints, etc.
//   - schema: column definitions
//   - columns: column names (one per line)
//
// See ADR-005 for rationale on separating metadata from capabilities.
type InfoDirNode struct {
	fs.Inode

	cfg       *config.Config
	db        db.DBClient
	schema    string
	tableName string
}

var _ fs.InodeEmbedder = (*InfoDirNode)(nil)
var _ fs.NodeGetattrer = (*InfoDirNode)(nil)
var _ fs.NodeReaddirer = (*InfoDirNode)(nil)
var _ fs.NodeLookuper = (*InfoDirNode)(nil)

// NewInfoDirNode creates a new .info directory node.
//
// Parameters:
//   - cfg: TigerFS configuration
//   - dbClient: Database client for queries
//   - schema: PostgreSQL schema name
//   - tableName: Name of the table this metadata describes
func NewInfoDirNode(cfg *config.Config, dbClient db.DBClient, schema, tableName string) *InfoDirNode {
	return &InfoDirNode{
		cfg:       cfg,
		db:        dbClient,
		schema:    schema,
		tableName: tableName,
	}
}

// Getattr returns attributes for the .info directory.
func (i *InfoDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("InfoDirNode.Getattr called",
		zap.String("schema", i.schema),
		zap.String("table", i.tableName))

	out.Mode = 0500 | syscall.S_IFDIR // read-only directory
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the metadata files in the .info directory.
func (i *InfoDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("InfoDirNode.Readdir called",
		zap.String("schema", i.schema),
		zap.String("table", i.tableName))

	entries := []fuse.DirEntry{
		{Name: InfoFileCount, Mode: syscall.S_IFREG},
		{Name: InfoFileDDL, Mode: syscall.S_IFREG},
		{Name: InfoFileSchema, Mode: syscall.S_IFREG},
		{Name: InfoFileColumns, Mode: syscall.S_IFREG},
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a metadata file by name.
func (i *InfoDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("InfoDirNode.Lookup called",
		zap.String("schema", i.schema),
		zap.String("table", i.tableName),
		zap.String("name", name))

	// Map .info/ file names to MetadataFileNode fileType
	var fileType string
	switch name {
	case InfoFileCount:
		fileType = "count"
	case InfoFileDDL:
		fileType = "ddl"
	case InfoFileSchema:
		fileType = "schema"
	case InfoFileColumns:
		fileType = "columns"
	default:
		logging.Debug("Unknown file in .info directory",
			zap.String("table", i.tableName),
			zap.String("name", name))
		return nil, syscall.ENOENT
	}

	stableAttr := fs.StableAttr{
		Mode: syscall.S_IFREG,
	}

	metadataNode := NewMetadataFileNode(i.cfg, i.db, i.schema, i.tableName, fileType)
	child := i.NewPersistentInode(ctx, metadataNode, stableAttr)

	// Fill in entry attributes
	var attrOut fuse.AttrOut
	if errno := metadataNode.Getattr(ctx, nil, &attrOut); errno != 0 {
		return nil, errno
	}
	out.Attr = attrOut.Attr

	logging.Debug("Created metadata file node in .info",
		zap.String("table", i.tableName),
		zap.String("name", name),
		zap.String("type", fileType))

	return child, 0
}
