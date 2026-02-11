//go:build linux

package cmd

import (
	"context"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Mounter interface abstracts the filesystem mount implementation.
// On Linux, FUSE is used; on macOS, NFS is used.
type Mounter interface {
	Serve(ctx context.Context) error
	Close() error
}

// mountFilesystem mounts the TigerFS filesystem using the appropriate backend.
// On Linux, this uses FUSE. By default, the shared fs.Operations core is used
// (same as NFS on macOS). Use --legacy-fuse to fall back to the specialized
// FUSE node tree.
func mountFilesystem(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (Mounter, error) {
	if cfg.LegacyFuse {
		logging.Info("Using legacy FUSE backend for Linux",
			zap.String("mountpoint", mountpoint))
		return fuse.Mount(ctx, cfg, connStr, mountpoint)
	}

	logging.Info("Using FUSE+Operations backend for Linux",
		zap.String("mountpoint", mountpoint))
	return fuse.MountOps(ctx, cfg, connStr, mountpoint)
}
