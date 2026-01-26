package mount

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNewRegistry verifies registry creation with explicit and default paths.
func TestNewRegistry(t *testing.T) {
	t.Run("with explicit path", func(t *testing.T) {
		tmpDir := t.TempDir()
		registryPath := filepath.Join(tmpDir, "subdir", "mounts.json")

		registry, err := NewRegistry(registryPath)
		if err != nil {
			t.Fatalf("NewRegistry failed: %v", err)
		}

		if registry.Path() != registryPath {
			t.Errorf("Path() = %q, want %q", registry.Path(), registryPath)
		}

		// Parent directory should have been created
		parentDir := filepath.Dir(registryPath)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			t.Errorf("parent directory was not created: %s", parentDir)
		}
	})

	t.Run("with empty path uses default", func(t *testing.T) {
		registry, err := NewRegistry("")
		if err != nil {
			t.Fatalf("NewRegistry with empty path failed: %v", err)
		}

		if registry.Path() == "" {
			t.Error("Path() returned empty string for default path")
		}
	})
}

// TestRegistryRegisterAndGet verifies entries can be registered and retrieved.
func TestRegistryRegisterAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	entry := Entry{
		Mountpoint: "/mnt/testdb",
		PID:        12345,
		Database:   "postgres://localhost/testdb",
		StartTime:  time.Now().Truncate(time.Second),
	}

	if err := registry.Register(entry); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	got, err := registry.Get("/mnt/testdb")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil, expected entry")
	}

	if got.Mountpoint != entry.Mountpoint {
		t.Errorf("Mountpoint = %q, want %q", got.Mountpoint, entry.Mountpoint)
	}
	if got.PID != entry.PID {
		t.Errorf("PID = %d, want %d", got.PID, entry.PID)
	}
	if got.Database != entry.Database {
		t.Errorf("Database = %q, want %q", got.Database, entry.Database)
	}
}

// TestRegistryGetNotFound verifies Get returns nil for non-existent mountpoints.
func TestRegistryGetNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	got, err := registry.Get("/mnt/nonexistent")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != nil {
		t.Errorf("Get returned %+v, expected nil", got)
	}
}

// TestRegistryUnregister verifies entries can be removed from the registry.
func TestRegistryUnregister(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	entry := Entry{
		Mountpoint: "/mnt/testdb",
		PID:        12345,
		Database:   "postgres://localhost/testdb",
		StartTime:  time.Now(),
	}
	if err := registry.Register(entry); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Verify it exists
	got, err := registry.Get("/mnt/testdb")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil {
		t.Fatal("Entry should exist before unregister")
	}

	// Unregister
	if err := registry.Unregister("/mnt/testdb"); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}

	// Verify it no longer exists
	got, err = registry.Get("/mnt/testdb")
	if err != nil {
		t.Fatalf("Get after unregister failed: %v", err)
	}
	if got != nil {
		t.Errorf("Entry should not exist after unregister, got %+v", got)
	}
}

// TestRegistryUnregisterIdempotent verifies unregistering a non-existent
// mountpoint doesn't cause an error.
func TestRegistryUnregisterIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	// Should not error when unregistering something that doesn't exist
	if err := registry.Unregister("/mnt/nonexistent"); err != nil {
		t.Errorf("Unregister of non-existent entry should not error, got: %v", err)
	}
}

// TestRegistryList verifies List returns all registered entries.
func TestRegistryList(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	entries := []Entry{
		{Mountpoint: "/mnt/db1", PID: 1001, Database: "postgres://localhost/db1", StartTime: time.Now()},
		{Mountpoint: "/mnt/db2", PID: 1002, Database: "postgres://localhost/db2", StartTime: time.Now()},
		{Mountpoint: "/mnt/db3", PID: 1003, Database: "postgres://localhost/db3", StartTime: time.Now()},
	}

	for _, e := range entries {
		if err := registry.Register(e); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
	}

	got, err := registry.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(got) != len(entries) {
		t.Errorf("List returned %d entries, want %d", len(got), len(entries))
	}
}

// TestRegistryListEmpty verifies List returns an empty slice (not nil) when empty.
func TestRegistryListEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	got, err := registry.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if got == nil {
		t.Error("List returned nil, expected empty slice")
	}
	if len(got) != 0 {
		t.Errorf("List returned %d entries, expected 0", len(got))
	}
}

// TestRegistryListActive verifies ListActive only returns entries with running processes.
func TestRegistryListActive(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	// One with current PID (running), one with fake PID (not running)
	currentPID := os.Getpid()
	entries := []Entry{
		{Mountpoint: "/mnt/active", PID: currentPID, Database: "postgres://localhost/active", StartTime: time.Now()},
		{Mountpoint: "/mnt/stale", PID: 999999, Database: "postgres://localhost/stale", StartTime: time.Now()},
	}

	for _, e := range entries {
		if err := registry.Register(e); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
	}

	active, err := registry.ListActive()
	if err != nil {
		t.Fatalf("ListActive failed: %v", err)
	}

	if len(active) != 1 {
		t.Errorf("ListActive returned %d entries, want 1", len(active))
	}
	if len(active) > 0 && active[0].Mountpoint != "/mnt/active" {
		t.Errorf("ListActive returned wrong entry: %q", active[0].Mountpoint)
	}
}

// TestRegistryCleanup verifies Cleanup removes stale entries.
func TestRegistryCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	currentPID := os.Getpid()
	entries := []Entry{
		{Mountpoint: "/mnt/active", PID: currentPID, Database: "postgres://localhost/active", StartTime: time.Now()},
		{Mountpoint: "/mnt/stale", PID: 999999, Database: "postgres://localhost/stale", StartTime: time.Now()},
	}

	for _, e := range entries {
		if err := registry.Register(e); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
	}

	removed, err := registry.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if removed != 1 {
		t.Errorf("Cleanup removed %d entries, want 1", removed)
	}

	// Should now only have the active entry
	got, err := registry.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(got) != 1 {
		t.Errorf("List after cleanup returned %d entries, want 1", len(got))
	}
}

// TestRegistryRegisterReplaces verifies registering with same mountpoint replaces existing.
func TestRegistryRegisterReplaces(t *testing.T) {
	tmpDir := t.TempDir()
	registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	entry1 := Entry{
		Mountpoint: "/mnt/testdb",
		PID:        1001,
		Database:   "postgres://localhost/db1",
		StartTime:  time.Now(),
	}
	if err := registry.Register(entry1); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Register again with same mountpoint but different data
	entry2 := Entry{
		Mountpoint: "/mnt/testdb",
		PID:        2002,
		Database:   "postgres://localhost/db2",
		StartTime:  time.Now(),
	}
	if err := registry.Register(entry2); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	got, err := registry.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(got) != 1 {
		t.Errorf("List returned %d entries, want 1", len(got))
	}

	if len(got) > 0 && got[0].PID != 2002 {
		t.Errorf("Entry PID = %d, want 2002", got[0].PID)
	}
}

// TestRegistryPersistence verifies entries survive across Registry instances.
func TestRegistryPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	registryPath := filepath.Join(tmpDir, "mounts.json")

	// Create first registry and register an entry
	registry1, err := NewRegistry(registryPath)
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	entry := Entry{
		Mountpoint: "/mnt/persistent",
		PID:        12345,
		Database:   "postgres://localhost/persistent",
		StartTime:  time.Now().Truncate(time.Second),
	}
	if err := registry1.Register(entry); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Create new registry instance pointing to same file
	registry2, err := NewRegistry(registryPath)
	if err != nil {
		t.Fatalf("NewRegistry (second instance) failed: %v", err)
	}

	got, err := registry2.Get("/mnt/persistent")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil {
		t.Fatal("Entry was not persisted")
	}
	if got.Mountpoint != entry.Mountpoint {
		t.Errorf("Mountpoint = %q, want %q", got.Mountpoint, entry.Mountpoint)
	}
}

// TestIsProcessRunning verifies the process detection logic.
func TestIsProcessRunning(t *testing.T) {
	t.Run("current process is running", func(t *testing.T) {
		if !isProcessRunning(os.Getpid()) {
			t.Error("isProcessRunning returned false for current process")
		}
	})

	t.Run("invalid PID", func(t *testing.T) {
		if isProcessRunning(0) {
			t.Error("isProcessRunning returned true for PID 0")
		}
		if isProcessRunning(-1) {
			t.Error("isProcessRunning returned true for negative PID")
		}
	})

	t.Run("non-existent PID", func(t *testing.T) {
		// PID 999999 is unlikely to exist
		if isProcessRunning(999999) {
			t.Skip("PID 999999 unexpectedly exists, skipping test")
		}
	})
}
