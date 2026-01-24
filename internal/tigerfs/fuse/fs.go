package fuse

import (
	"context"
	"fmt"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// FS represents a mounted TigerFS filesystem
type FS struct {
	cfg        *config.Config
	db         *db.Client
	mountpoint string
	// TODO: Add FUSE server instance (TBD based on library choice)
}

// Mount creates and mounts a new TigerFS filesystem
func Mount(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (*FS, error) {
	logging.Debug("Mounting filesystem",
		zap.String("mountpoint", mountpoint),
		zap.String("connection", connStr),
	)

	// TODO: Implement FUSE mount
	// 1. Connect to database (db.NewClient)
	// 2. Initialize FUSE filesystem
	// 3. Register FUSE operations (Lookup, Read, Write, etc.)
	// 4. Mount at mountpoint

	// Stub: Create filesystem instance
	fs := &FS{
		cfg:        cfg,
		mountpoint: mountpoint,
	}

	// TODO: Connect to database
	dbClient, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	fs.db = dbClient

	logging.Info("Database connection established")

	// TODO: Mount FUSE filesystem
	return fs, fmt.Errorf("FUSE mount not yet implemented")
}

// Serve serves FUSE requests (blocks until unmount or context cancellation)
func (fs *FS) Serve(ctx context.Context) error {
	logging.Debug("Starting FUSE server")

	// TODO: Serve FUSE requests
	// This should block until:
	// - Filesystem is unmounted
	// - Context is cancelled (signal received)
	// - Error occurs

	<-ctx.Done()
	return ctx.Err()
}

// Close cleanly shuts down the filesystem
func (fs *FS) Close() error {
	logging.Debug("Closing filesystem", zap.String("mountpoint", fs.mountpoint))

	// TODO: Cleanup
	// 1. Stop accepting new requests
	// 2. Wait for in-flight operations
	// 3. Close database connections
	// 4. Unmount filesystem

	if fs.db != nil {
		if err := fs.db.Close(); err != nil {
			logging.Error("Failed to close database connection", zap.Error(err))
			return err
		}
	}

	return nil
}
