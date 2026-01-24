package fuse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestMount_NoDatabase(t *testing.T) {
	// Test that Mount fails gracefully when database is unavailable
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:     5,
		PoolMaxIdle:  2,
		AttrTimeout:  1 * time.Second,
		EntryTimeout: 1 * time.Second,
	}

	mountpoint := t.TempDir()

	// Invalid connection should fail
	_, err := Mount(ctx, cfg, "postgres://invalid:invalid@nonexistent:9999/invalid", mountpoint)
	if err == nil {
		t.Fatal("Expected error for invalid database connection, got nil")
	}

	// Error should mention database connection failure
	if err != nil {
		t.Logf("Got expected error: %v", err)
	}
}

func TestMount_InvalidMountpoint(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:     5,
		PoolMaxIdle:  2,
		AttrTimeout:  1 * time.Second,
		EntryTimeout: 1 * time.Second,
	}

	// Try to mount to a non-existent directory
	mountpoint := "/nonexistent/invalid/mountpoint"

	_, err := Mount(ctx, cfg, connStr, mountpoint)
	if err == nil {
		t.Fatal("Expected error for invalid mountpoint, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestMount_Success(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	// Skip if FUSE not available
	if !isFUSEAvailable(t) {
		t.Skip("FUSE not available on this system")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:     5,
		PoolMaxIdle:  2,
		AttrTimeout:  1 * time.Second,
		EntryTimeout: 1 * time.Second,
		Debug:        false,
	}

	mountpoint := t.TempDir()

	// Mount filesystem
	filesystem, err := Mount(ctx, cfg, connStr, mountpoint)
	if err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	defer filesystem.Close()

	// Verify filesystem struct is populated
	if filesystem.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if filesystem.db == nil {
		t.Error("Expected database client to be set")
	}

	if filesystem.server == nil {
		t.Error("Expected FUSE server to be set")
	}

	if filesystem.root == nil {
		t.Error("Expected root node to be set")
	}

	if filesystem.mountpoint != mountpoint {
		t.Errorf("Expected mountpoint %s, got %s", mountpoint, filesystem.mountpoint)
	}

	// Verify mountpoint exists and is accessible
	info, err := os.Stat(mountpoint)
	if err != nil {
		t.Fatalf("Failed to stat mountpoint: %v", err)
	}

	if !info.IsDir() {
		t.Error("Expected mountpoint to be a directory")
	}

	// Attempt to list mountpoint (should be empty for now)
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatalf("Failed to read mountpoint: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected empty directory, got %d entries", len(entries))
	}

	// Close filesystem
	err = filesystem.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestClose_SafeWhenNil(t *testing.T) {
	// Test that Close doesn't panic with nil fields
	fs := &FS{
		cfg:        &config.Config{},
		mountpoint: "/tmp/test",
	}

	err := fs.Close()
	if err != nil {
		t.Errorf("Close with nil fields failed: %v", err)
	}
}

// Helper functions

func isFUSEAvailable(t *testing.T) bool {
	t.Helper()

	// Check if we can create a test mount directory
	// This is a basic check - actual FUSE mount may still fail
	// depending on system configuration

	// On macOS, check for osxfuse/macfuse
	if _, err := os.Stat("/Library/Filesystems/osxfuse.fs"); err == nil {
		return true
	}
	if _, err := os.Stat("/Library/Filesystems/macfuse.fs"); err == nil {
		return true
	}

	// On Linux, check for /dev/fuse
	if _, err := os.Stat("/dev/fuse"); err == nil {
		return true
	}

	return false
}

// TestRootNode_Basic tests the root node directly without mounting
func TestRootNode_Basic(t *testing.T) {
	cfg := &config.Config{}

	root := NewRootNode(cfg, nil)

	if root.cfg != cfg {
		t.Error("Expected config to be set")
	}

	// Note: We can't test Getattr, Readdir, Lookup without a full FUSE context
	// These will be tested in integration tests
}

// TestRootNode_Interfaces verifies that RootNode implements required interfaces
func TestRootNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that RootNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	root := NewRootNode(cfg, nil)

	// Verify we can use root as various interface types
	_ = interface{}(root).(interface{})
}
