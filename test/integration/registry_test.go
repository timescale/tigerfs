package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
)

// TestRegistry_MountUnmountRoundTrip verifies that mounts are properly registered
// and unregistered during the mount/unmount lifecycle.
func TestRegistry_MountUnmountRoundTrip(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	mountpoint := t.TempDir()
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	// Use a test-specific registry to avoid interfering with real mounts
	registryPath := filepath.Join(t.TempDir(), "test-mounts.json")
	registry, err := mount.NewRegistry(registryPath)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Verify registry is initially empty
	entries, err := registry.List()
	if err != nil {
		t.Fatalf("Failed to list registry: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Expected empty registry, got %d entries", len(entries))
	}

	// Mount filesystem
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}

	// Register the mount (simulating what the mount command does)
	entry := mount.Entry{
		Mountpoint: absMountpoint,
		PID:        os.Getpid(),
		Database:   "test-database",
		StartTime:  time.Now(),
	}
	if err := registry.Register(entry); err != nil {
		filesystem.Close()
		t.Fatalf("Failed to register mount: %v", err)
	}

	// Verify mount appears in registry
	entries, err = registry.List()
	if err != nil {
		filesystem.Close()
		t.Fatalf("Failed to list registry after register: %v", err)
	}
	if len(entries) != 1 {
		filesystem.Close()
		t.Fatalf("Expected 1 entry in registry, got %d", len(entries))
	}
	if entries[0].Mountpoint != absMountpoint {
		filesystem.Close()
		t.Errorf("Expected mountpoint %q, got %q", absMountpoint, entries[0].Mountpoint)
	}

	// Verify we can retrieve the entry by mountpoint
	retrieved, err := registry.Get(absMountpoint)
	if err != nil {
		filesystem.Close()
		t.Fatalf("Failed to get entry: %v", err)
	}
	if retrieved == nil {
		filesystem.Close()
		t.Fatal("Expected to find entry, got nil")
	}
	if retrieved.PID != os.Getpid() {
		filesystem.Close()
		t.Errorf("Expected PID %d, got %d", os.Getpid(), retrieved.PID)
	}

	// Verify ListActive includes our mount (since our process is running)
	active, err := registry.ListActive()
	if err != nil {
		filesystem.Close()
		t.Fatalf("Failed to list active: %v", err)
	}
	if len(active) != 1 {
		filesystem.Close()
		t.Fatalf("Expected 1 active entry, got %d", len(active))
	}

	// Close/unmount the filesystem
	if err := filesystem.Close(); err != nil {
		t.Logf("Warning: filesystem.Close() returned error: %v", err)
	}

	// Unregister the mount (simulating what unmount command does)
	if err := registry.Unregister(absMountpoint); err != nil {
		t.Fatalf("Failed to unregister mount: %v", err)
	}

	// Verify mount is removed from registry
	entries, err = registry.List()
	if err != nil {
		t.Fatalf("Failed to list registry after unregister: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected empty registry after unregister, got %d entries", len(entries))
	}

	// Verify Get returns nil for unregistered mount
	retrieved, err = registry.Get(absMountpoint)
	if err != nil {
		t.Fatalf("Failed to get entry after unregister: %v", err)
	}
	if retrieved != nil {
		t.Errorf("Expected nil after unregister, got %+v", retrieved)
	}
}

// TestRegistry_StaleEntryCleanup verifies that stale entries (with non-running PIDs)
// are properly detected and can be cleaned up.
func TestRegistry_StaleEntryCleanup(t *testing.T) {
	// Use a test-specific registry
	registryPath := filepath.Join(t.TempDir(), "test-mounts.json")
	registry, err := mount.NewRegistry(registryPath)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Register an entry with the current PID (active)
	activeEntry := mount.Entry{
		Mountpoint: "/mnt/active",
		PID:        os.Getpid(),
		Database:   "active-db",
		StartTime:  time.Now(),
	}
	if err := registry.Register(activeEntry); err != nil {
		t.Fatalf("Failed to register active entry: %v", err)
	}

	// Register an entry with a non-existent PID (stale)
	staleEntry := mount.Entry{
		Mountpoint: "/mnt/stale",
		PID:        999999, // Unlikely to exist
		Database:   "stale-db",
		StartTime:  time.Now().Add(-1 * time.Hour),
	}
	if err := registry.Register(staleEntry); err != nil {
		t.Fatalf("Failed to register stale entry: %v", err)
	}

	// Verify both entries exist in List()
	entries, err := registry.List()
	if err != nil {
		t.Fatalf("Failed to list registry: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Verify ListActive() only returns the active entry
	active, err := registry.ListActive()
	if err != nil {
		t.Fatalf("Failed to list active: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("Expected 1 active entry, got %d", len(active))
	}
	if active[0].Mountpoint != "/mnt/active" {
		t.Errorf("Expected active mountpoint /mnt/active, got %s", active[0].Mountpoint)
	}

	// Clean up stale entries
	removed, err := registry.Cleanup()
	if err != nil {
		t.Fatalf("Failed to cleanup: %v", err)
	}
	if removed != 1 {
		t.Errorf("Expected 1 entry removed, got %d", removed)
	}

	// Verify only active entry remains
	entries, err = registry.List()
	if err != nil {
		t.Fatalf("Failed to list registry after cleanup: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry after cleanup, got %d", len(entries))
	}
	if entries[0].Mountpoint != "/mnt/active" {
		t.Errorf("Expected /mnt/active to remain, got %s", entries[0].Mountpoint)
	}
}
