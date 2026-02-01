//go:build darwin

package cmd

import (
	"context"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/nfs"
	"go.uber.org/zap"
)

// Mounter interface abstracts the filesystem mount implementation.
// On macOS, NFS is used; on Linux, FUSE is used.
type Mounter interface {
	Serve(ctx context.Context) error
	Close() error
}

// mountFilesystem mounts the TigerFS filesystem using the appropriate backend.
// On macOS, this uses NFS since FUSE requires third-party kernel extensions.
func mountFilesystem(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (Mounter, error) {
	logging.Info("Using NFS backend for macOS",
		zap.String("mountpoint", mountpoint))

	return nfs.Mount(ctx, cfg, connStr, mountpoint)
}
