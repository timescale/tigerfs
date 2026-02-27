// Package fuse provides FUSE filesystem operations for TigerFS.
//
// This file implements PipelineColumnsDirNode, the node for .columns/ capability.
// It lists available table columns and handles column selection via comma-separated
// column names (e.g., .columns/id,name,status).

package fuse

import (
	"context"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// PipelineColumnsDirNode represents .columns/ within a pipeline.
// Readdir lists table column names; Lookup accepts comma-separated column names
// and returns a PipelineNode with column projection applied.
type PipelineColumnsDirNode struct {
	fs.Inode

	cfg         *config.Config
	db          db.DBClient
	cache       *tigerfs.MetadataCache
	schema      string
	table       string
	pipeline    *PipelineContext
	partialRows *PartialRowTracker
}

var _ fs.InodeEmbedder = (*PipelineColumnsDirNode)(nil)
var _ fs.NodeGetattrer = (*PipelineColumnsDirNode)(nil)
var _ fs.NodeReaddirer = (*PipelineColumnsDirNode)(nil)
var _ fs.NodeLookuper = (*PipelineColumnsDirNode)(nil)

// NewPipelineColumnsDirNode creates a pipeline-aware .columns/ node.
func NewPipelineColumnsDirNode(cfg *config.Config, dbClient db.DBClient, cache *tigerfs.MetadataCache, schema, table string, pipeline *PipelineContext, partialRows *PartialRowTracker) *PipelineColumnsDirNode {
	return &PipelineColumnsDirNode{
		cfg:         cfg,
		db:          dbClient,
		cache:       cache,
		schema:      schema,
		table:       table,
		pipeline:    pipeline,
		partialRows: partialRows,
	}
}

// Getattr returns attributes for the .columns directory.
func (c *PipelineColumnsDirNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0500 | syscall.S_IFDIR
	out.Nlink = 2
	out.Size = 4096
	return 0
}

// Readdir lists all table column names.
func (c *PipelineColumnsDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	columns, err := c.db.GetColumns(ctx, c.schema, c.table)
	if err != nil {
		logging.Error("Failed to get columns for .columns/ directory",
			zap.String("table", c.table),
			zap.Error(err))
		return nil, syscall.EIO
	}

	entries := make([]fuse.DirEntry, len(columns))
	for i, col := range columns {
		entries[i] = fuse.DirEntry{
			Name: col.Name,
			Mode: syscall.S_IFDIR,
		}
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup handles column selection.
// Name may be a single column or comma-separated list (e.g., "id,name,status").
// Each column is validated against the table schema.
func (c *PipelineColumnsDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Split comma-separated column names
	colNames := strings.Split(name, ",")

	// Validate: no empty names
	for _, col := range colNames {
		if col == "" {
			logging.Debug("Empty column name in .columns/ lookup",
				zap.String("name", name))
			return nil, syscall.ENOENT
		}
	}

	// Validate all columns exist
	columns, err := c.db.GetColumns(ctx, c.schema, c.table)
	if err != nil {
		logging.Error("Failed to get columns for validation",
			zap.String("table", c.table),
			zap.Error(err))
		return nil, syscall.EIO
	}

	validCols := make(map[string]bool, len(columns))
	for _, col := range columns {
		validCols[col.Name] = true
	}

	for _, col := range colNames {
		if !validCols[col] {
			logging.Debug("Invalid column in .columns/ lookup",
				zap.String("column", col),
				zap.String("table", c.table))
			return nil, syscall.ENOENT
		}
	}

	// Apply column projection to pipeline
	newPipeline := c.pipeline.WithColumns(colNames)

	stableAttr := fs.StableAttr{Mode: syscall.S_IFDIR}
	pipelineNode := NewPipelineNode(c.cfg, c.db, c.cache, c.schema, c.table, newPipeline, c.partialRows)
	child := c.NewPersistentInode(ctx, pipelineNode, stableAttr)
	return child, 0
}
