package fuse

import (
	"context"
	"fmt"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// FS represents a mounted TigerFS filesystem
type FS struct {
	cfg           *config.Config
	db            *db.Client
	mountpoint    string
	server        *fuse.Server
	root          *RootNode
	partialRows   *PartialRowTracker
}

// Mount creates and mounts a new TigerFS filesystem
func Mount(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (*FS, error) {
	logging.Debug("Mounting filesystem",
		zap.String("mountpoint", mountpoint),
		zap.String("connection", connStr),
	)

	// 1. Connect to database
	dbClient, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	logging.Info("Database connection established")

	// 2. Create partial row tracker
	partialRows := NewPartialRowTracker(dbClient)

	// 3. Create root node with partial row tracker
	root := NewRootNode(cfg, dbClient, partialRows)

	// 4. Configure FUSE mount options
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:   "tigerfs",
			FsName: "tigerfs",
			Debug:  cfg.Debug,

			// Use DirectMount for macOS compatibility
			DirectMount: true,
		},

		// Set timeouts for attribute caching
		AttrTimeout:  &cfg.AttrTimeout,
		EntryTimeout: &cfg.EntryTimeout,
	}

	logging.Debug("FUSE mount options",
		zap.Duration("attr_timeout", cfg.AttrTimeout),
		zap.Duration("entry_timeout", cfg.EntryTimeout),
		zap.Bool("debug", cfg.Debug),
	)

	// 5. Mount the filesystem
	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		dbClient.Close()
		return nil, fmt.Errorf("failed to mount FUSE filesystem: %w", err)
	}

	logging.Info("FUSE filesystem mounted successfully",
		zap.String("mountpoint", mountpoint),
	)

	return &FS{
		cfg:         cfg,
		db:          dbClient,
		mountpoint:  mountpoint,
		server:      server,
		root:        root,
		partialRows: partialRows,
	}, nil
}

// Serve serves FUSE requests (blocks until unmount or context cancellation)
func (f *FS) Serve(ctx context.Context) error {
	logging.Debug("Starting FUSE server")

	if f.server == nil {
		return fmt.Errorf("FUSE server not initialized")
	}

	// Wait for either unmount or context cancellation
	// The server.Wait() blocks until the filesystem is unmounted
	go func() {
		<-ctx.Done()
		logging.Info("Context cancelled, unmounting filesystem")
		f.server.Unmount()
	}()

	// Wait for server to finish (blocks until unmount)
	f.server.Wait()

	logging.Info("FUSE server stopped")
	return nil
}

// Close cleanly shuts down the filesystem
func (f *FS) Close() error {
	logging.Debug("Closing filesystem", zap.String("mountpoint", f.mountpoint))

	var lastErr error

	// 1. Unmount filesystem if still mounted
	if f.server != nil {
		logging.Debug("Unmounting filesystem")
		if err := f.server.Unmount(); err != nil {
			logging.Warn("Failed to unmount filesystem", zap.Error(err))
			lastErr = err
		}
	}

	// 2. Close database connection
	if f.db != nil {
		logging.Debug("Closing database connection")
		if err := f.db.Close(); err != nil {
			logging.Error("Failed to close database connection", zap.Error(err))
			lastErr = err
		}
	}

	if lastErr != nil {
		return fmt.Errorf("errors during cleanup: %w", lastErr)
	}

	logging.Info("Filesystem closed successfully")
	return nil
}
