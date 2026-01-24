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

// RootNode represents the root directory of the TigerFS filesystem
// Lists tables from the default schema (schema flattening)
type RootNode struct {
	fs.Inode

	cfg *config.Config
	db  *db.Client
}

var _ fs.InodeEmbedder = (*RootNode)(nil)
var _ fs.NodeReaddirer = (*RootNode)(nil)
var _ fs.NodeLookuper = (*RootNode)(nil)
var _ fs.NodeGetattrer = (*RootNode)(nil)

// NewRootNode creates a new root directory node
func NewRootNode(cfg *config.Config, dbClient *db.Client) *RootNode {
	return &RootNode{
		cfg: cfg,
		db:  dbClient,
	}
}

// Getattr returns attributes for the root directory
func (r *RootNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("RootNode.Getattr called")

	out.Mode = 0755 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096

	return 0
}

// Readdir lists the contents of the root directory
// For now, returns empty list (tables will be added in Task 1.5)
func (r *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debug("RootNode.Readdir called")

	// TODO: Implement schema discovery in Task 1.5
	// For now, return empty directory listing
	entries := []fuse.DirEntry{}

	logging.Debug("Root directory listing", zap.Int("entries", len(entries)))

	return fs.NewListDirStream(entries), 0
}

// Lookup looks up a table name in the root directory
// For now, returns ENOENT (tables will be added in Task 1.5)
func (r *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debug("RootNode.Lookup called", zap.String("name", name))

	// TODO: Implement table lookup in Task 1.5
	// For now, all lookups fail
	logging.Debug("Table not found", zap.String("name", name))
	return nil, syscall.ENOENT
}
