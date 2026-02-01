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
// On Linux, this uses FUSE.
func mountFilesystem(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (Mounter, error) {
	logging.Info("Using FUSE backend for Linux",
		zap.String("mountpoint", mountpoint))

	return fuse.Mount(ctx, cfg, connStr, mountpoint)
}
