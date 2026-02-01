//go:build linux

package nfs

import (
	"context"
	"fmt"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// FS represents a mounted NFS filesystem.
// On Linux, NFS is not used - use FUSE instead.
type FS struct{}

// Mount returns an error on Linux as NFS is only used on macOS.
// Linux should use the FUSE mount implementation.
func Mount(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (*FS, error) {
	return nil, fmt.Errorf("NFS mount is only supported on macOS; use FUSE on Linux")
}

// Serve is not implemented on Linux.
func (f *FS) Serve(ctx context.Context) error {
	return fmt.Errorf("NFS not supported on Linux")
}

// Close is not implemented on Linux.
func (f *FS) Close() error {
	return nil
}

// IsMounted always returns false on Linux.
func IsMounted(mountpoint string) bool {
	return false
}
