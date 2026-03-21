package fuse

import (
	"context"
	"fmt"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// OpsFS is a FUSE filesystem that delegates to fs.Operations via FSAdapter.
// It uses a single OpsNode type for all inodes, mirroring how the NFS backend
// uses OpsFilesystem. This is the default FUSE backend (--legacy-fuse uses FS).
type OpsFS struct {
	cfg        *config.Config
	db         *db.Client
	ops        *tigerfs.Operations
	adapter    *FSAdapter
	server     *fuse.Server
	mountpoint string
}

// MountOps creates and mounts a new FUSE filesystem using the shared
// fs.Operations core. This is the default FUSE code path.
func MountOps(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (*OpsFS, error) {
	logging.Debug("MountOps: mounting filesystem via Operations",
		zap.String("mountpoint", mountpoint),
		zap.String("connection", db.SanitizeConnectionString(connStr)),
	)

	// 1. Connect to database
	dbClient, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	logging.Info("Database connection established")

	// 2. Create shared Operations core (same as NFS uses)
	ops := tigerfs.NewOperations(cfg, dbClient)

	// 3. Create FSAdapter bridge
	adapter := NewFSAdapter(ops)

	// 4. Create root OpsNode at "/"
	root := newOpsNode(adapter, "/")

	// 5. Configure FUSE mount options
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:        "tigerfs",
			FsName:      "tigerfs",
			Debug:       cfg.LogLevel == "debug",
			DirectMount: true,
		},
		AttrTimeout:  &cfg.AttrTimeout,
		EntryTimeout: &cfg.EntryTimeout,
	}

	logging.Debug("FUSE+Operations mount options",
		zap.Duration("attr_timeout", cfg.AttrTimeout),
		zap.Duration("entry_timeout", cfg.EntryTimeout),
		zap.String("log_level", cfg.LogLevel),
	)

	// 6. Mount the filesystem
	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		if closeErr := dbClient.Close(); closeErr != nil {
			logging.Error("Failed to close database client during cleanup", zap.Error(closeErr))
		}
		return nil, fmt.Errorf("failed to mount FUSE filesystem: %w", err)
	}

	logging.Info("FUSE+Operations filesystem mounted successfully",
		zap.String("mountpoint", mountpoint),
	)

	return &OpsFS{
		cfg:        cfg,
		db:         dbClient,
		ops:        ops,
		adapter:    adapter,
		server:     server,
		mountpoint: mountpoint,
	}, nil
}

// Serve serves FUSE requests (blocks until unmount or context cancellation).
func (f *OpsFS) Serve(ctx context.Context) error {
	logging.Debug("Starting FUSE+Operations server")

	if f.server == nil {
		return fmt.Errorf("FUSE server not initialized")
	}

	// Wait for either unmount or context cancellation
	go func() {
		<-ctx.Done()
		logging.Info("Context cancelled, unmounting filesystem")
		if err := f.server.Unmount(); err != nil {
			logging.Error("Failed to unmount filesystem", zap.Error(err))
		}
	}()

	f.server.Wait()

	logging.Info("FUSE+Operations server stopped")
	return nil
}

// Close cleanly shuts down the filesystem.
func (f *OpsFS) Close() error {
	logging.Debug("Closing FUSE+Operations filesystem", zap.String("mountpoint", f.mountpoint))

	var lastErr error

	// 1. Unmount filesystem if still mounted
	if f.server != nil && isMounted(f.mountpoint) {
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

	logging.Info("FUSE+Operations filesystem closed successfully")
	return nil
}
