package fuse

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// FS represents a mounted TigerFS filesystem
type FS struct {
	cfg         *config.Config
	db          *db.Client
	mountpoint  string
	server      *fuse.Server
	root        *RootNode
	partialRows *PartialRowTracker
	staging     *StagingTracker
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

	// 3. Create staging tracker for DDL operations
	staging := NewStagingTracker()

	// 4. Create root node with partial row tracker and staging
	root := NewRootNode(cfg, dbClient, partialRows)
	root.staging = staging

	// 5. Configure FUSE mount options
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

	// 6. Mount the filesystem
	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		if closeErr := dbClient.Close(); closeErr != nil {
			logging.Error("Failed to close database client during cleanup", zap.Error(closeErr))
		}
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
		staging:     staging,
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
		if err := f.server.Unmount(); err != nil {
			logging.Error("Failed to unmount filesystem", zap.Error(err))
		}
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
	if f.server != nil && isMounted(f.mountpoint) {
		logging.Debug("Unmounting filesystem")
		if err := f.server.Unmount(); err != nil {
			logging.Warn("Failed to unmount filesystem", zap.Error(err))
			lastErr = err
		}
	} else if f.server != nil {
		logging.Debug("Filesystem already unmounted, skipping unmount")
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

// isMounted checks if the given path is currently a FUSE mount point.
// This is used to avoid trying to unmount an already-unmounted filesystem.
func isMounted(mountpoint string) bool {
	if mountpoint == "" {
		return false
	}

	// Check /proc/mounts on Linux
	file, err := os.Open("/proc/mounts")
	if err != nil {
		// On non-Linux systems (macOS), assume mounted if we can't check
		// The unmount will fail gracefully if already unmounted
		return true
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[1] == mountpoint {
			return true
		}
	}

	return false
}
