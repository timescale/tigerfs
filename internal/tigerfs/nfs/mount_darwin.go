//go:build darwin

package nfs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// FS represents a mounted NFS filesystem on macOS.
type FS struct {
	cfg        *config.Config
	db         *db.Client
	server     *Server
	mountpoint string
	ctx        context.Context
	cancel     context.CancelFunc
}

// Mount creates and mounts a new TigerFS filesystem using NFS on macOS.
func Mount(ctx context.Context, cfg *config.Config, connStr, mountpoint string) (*FS, error) {
	logging.Info("Mounting TigerFS via NFS on macOS",
		zap.String("mountpoint", mountpoint))

	// Resolve mountpoint to absolute path
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve mountpoint: %w", err)
	}

	// Ensure mountpoint exists
	if err := os.MkdirAll(absMountpoint, 0755); err != nil {
		return nil, fmt.Errorf("failed to create mountpoint: %w", err)
	}

	// Connect to database
	dbClient, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	logging.Info("Database connection established")

	// Create NFS server
	server, err := NewServer(ctx, cfg, dbClient)
	if err != nil {
		dbClient.Close()
		return nil, fmt.Errorf("failed to create NFS server: %w", err)
	}

	// Start NFS server
	port, err := server.Start()
	if err != nil {
		dbClient.Close()
		return nil, fmt.Errorf("failed to start NFS server: %w", err)
	}

	logging.Info("NFS server started", zap.Int("port", port))

	// Give the server a moment to be ready
	time.Sleep(100 * time.Millisecond)

	// Mount using macOS mount_nfs command
	// Options explanation:
	// - locallocks: Use local locking (NFS server doesn't support NLM)
	// - vers=3: Use NFS v3 protocol
	// - tcp: Use TCP transport
	// - port=N: NFS server port
	// - mountport=N: Mount protocol port (same as NFS port for go-nfs)
	// - soft: Return errors on timeout (rather than hanging)
	// - timeo=300: Timeout in deciseconds (30 seconds). Large writes (1MB)
	//   to Docker PostgreSQL can take several seconds for XDR decode + DB commit.
	// - retrans=2: Number of retries
	// - resvport: Don't require reserved port (needed for non-root)
	// - nolocks: Disable NFS locking (not supported by go-nfs)
	// - wsize=131072: 128KB write chunks. macOS default is 32KB-64KB, which
	//   causes many small RPCs with O(n²) DB writes. Larger values like 1MB
	//   can trigger GC deadlocks in test environments where the NFS server
	//   (go-nfs) runs in the same process as the client. 128KB is a safe
	//   balance: 4x fewer RPCs than the default with no GC issues.
	// - rsize=131072: 128KB read chunks (match wsize for consistency)
	mountOpts := fmt.Sprintf("locallocks,vers=3,tcp,port=%d,mountport=%d,soft,timeo=300,retrans=2,noresvport,nolocks,wsize=131072,rsize=131072", port, port)

	cmd := exec.CommandContext(ctx, "/sbin/mount_nfs",
		"-o", mountOpts,
		fmt.Sprintf("127.0.0.1:/"),
		absMountpoint)

	output, err := cmd.CombinedOutput()
	if err != nil {
		server.Stop()
		dbClient.Close()
		return nil, fmt.Errorf("mount_nfs failed: %w\nOutput: %s", err, string(output))
	}

	logging.Info("NFS filesystem mounted successfully",
		zap.String("mountpoint", absMountpoint),
		zap.Int("port", port))

	ctx, cancel := context.WithCancel(ctx)

	return &FS{
		cfg:        cfg,
		db:         dbClient,
		server:     server,
		mountpoint: absMountpoint,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Serve waits for the filesystem to be unmounted or context cancelled.
func (f *FS) Serve(ctx context.Context) error {
	logging.Debug("NFS filesystem serving")

	// Wait for context cancellation
	<-ctx.Done()

	logging.Info("Context cancelled, unmounting NFS filesystem")
	return nil
}

// Close cleanly shuts down the filesystem.
func (f *FS) Close() error {
	logging.Debug("Closing NFS filesystem", zap.String("mountpoint", f.mountpoint))

	var lastErr error

	// Cancel context
	f.cancel()

	// Unmount filesystem
	if err := unmount(f.mountpoint); err != nil {
		logging.Warn("Failed to unmount filesystem", zap.Error(err))
		lastErr = err
	}

	// Stop NFS server
	if f.server != nil {
		if err := f.server.Stop(); err != nil {
			logging.Warn("Failed to stop NFS server", zap.Error(err))
			lastErr = err
		}
	}

	// Close database connection
	if f.db != nil {
		if err := f.db.Close(); err != nil {
			logging.Error("Failed to close database connection", zap.Error(err))
			lastErr = err
		}
	}

	if lastErr != nil {
		return fmt.Errorf("errors during cleanup: %w", lastErr)
	}

	logging.Info("NFS filesystem closed successfully")
	return nil
}

// unmount unmounts the NFS filesystem on macOS.
func unmount(mountpoint string) error {
	// Try umount first
	cmd := exec.Command("/sbin/umount", mountpoint)
	if err := cmd.Run(); err != nil {
		// Try diskutil unmount force as fallback
		// Force is needed because during shutdown the NFS server may not respond cleanly
		cmd = exec.Command("/usr/sbin/diskutil", "unmount", "force", mountpoint)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to unmount %s: %w", mountpoint, err)
		}
	}
	return nil
}

// IsMounted checks if the given path is currently an NFS mount point.
func IsMounted(mountpoint string) bool {
	// Use mount command to check
	cmd := exec.Command("/sbin/mount")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// Check if mountpoint appears in mount output
	return containsMountpoint(string(output), mountpoint)
}

// containsMountpoint checks if the mount output contains the mountpoint.
func containsMountpoint(output, mountpoint string) bool {
	// macOS mount output format: "server:/ on /path type nfs (...)"
	// Look for " on /path " pattern
	searchStr := " on " + mountpoint + " "
	return len(output) > 0 && (len(searchStr) > 0 && (output[0] == searchStr[0] || true)) && contains(output, searchStr)
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
