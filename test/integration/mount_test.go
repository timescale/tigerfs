package integration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fuse"
)

func TestMount_ListTables(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE availability
	if !isFUSEAvailable() {
		t.Skip("FUSE not available on this system")
	}

	// Create config
	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MaxLsRows:               10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	filesystem, err := fuse.Mount(ctx, cfg, dbResult.ConnStr, mountpoint)
	if err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// List tables
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatalf("Failed to list mountpoint: %v", err)
	}

	// Verify tables exist
	tableNames := make([]string, len(entries))
	for i, entry := range entries {
		tableNames[i] = entry.Name()
		t.Logf("Table: %s (isDir=%v)", entry.Name(), entry.IsDir())
	}

	// Should have users and products tables
	if !contains(tableNames, "users") {
		t.Errorf("Expected 'users' table, got: %v", tableNames)
	}

	if !contains(tableNames, "products") {
		t.Errorf("Expected 'products' table, got: %v", tableNames)
	}
}

func TestMount_ListRows(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE availability
	if !isFUSEAvailable() {
		t.Skip("FUSE not available on this system")
	}

	// Create config
	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MaxLsRows:               10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	filesystem, err := fuse.Mount(ctx, cfg, dbResult.ConnStr, mountpoint)
	if err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// List rows in users table
	usersDir := mountpoint + "/users"
	entries, err := os.ReadDir(usersDir)
	if err != nil {
		t.Fatalf("Failed to list users table: %v", err)
	}

	// Verify rows exist
	rowIDs := make([]string, len(entries))
	for i, entry := range entries {
		rowIDs[i] = entry.Name()
		t.Logf("Row: %s (isDir=%v)", entry.Name(), entry.IsDir())
	}

	// Should have 3 rows (IDs 1, 2, 3)
	if len(rowIDs) != 3 {
		t.Errorf("Expected 3 rows, got %d: %v", len(rowIDs), rowIDs)
	}

	expectedIDs := []string{"1", "2", "3"}
	for _, expectedID := range expectedIDs {
		if !contains(rowIDs, expectedID) {
			t.Errorf("Expected row ID '%s', got: %v", expectedID, rowIDs)
		}
	}
}

func TestMount_ReadRow(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE availability
	if !isFUSEAvailable() {
		t.Skip("FUSE not available on this system")
	}

	// Create config
	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MaxLsRows:               10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	filesystem, err := fuse.Mount(ctx, cfg, dbResult.ConnStr, mountpoint)
	if err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Read row from users table
	rowFile := mountpoint + "/users/1"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	// Verify TSV format
	content := string(data)
	t.Logf("Row content: %s", content)

	// Should contain tab-separated values
	if !strings.Contains(content, "\t") {
		t.Error("Expected tab-separated values in row content")
	}

	// Should contain user data (Alice)
	if !strings.Contains(content, "Alice") {
		t.Errorf("Expected 'Alice' in row content, got: %s", content)
	}

	if !strings.Contains(content, "alice@example.com") {
		t.Errorf("Expected 'alice@example.com' in row content, got: %s", content)
	}

	// Verify format: id\tname\temail\tage\tactive\tcreated_at
	fields := strings.Split(strings.TrimSpace(content), "\t")
	if len(fields) != 6 {
		t.Errorf("Expected 6 fields (id, name, email, age, active, created_at), got %d: %v", len(fields), fields)
	}

	// First field should be ID (1)
	if fields[0] != "1" {
		t.Errorf("Expected ID=1, got: %s", fields[0])
	}

	// Second field should be name (Alice)
	if fields[1] != "Alice" {
		t.Errorf("Expected name='Alice', got: %s", fields[1])
	}

	// Third field should be email
	if fields[2] != "alice@example.com" {
		t.Errorf("Expected email='alice@example.com', got: %s", fields[2])
	}
}

func TestMount_ReadNonExistentRow(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE availability
	if !isFUSEAvailable() {
		t.Skip("FUSE not available on this system")
	}

	// Create config
	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MaxLsRows:               10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	filesystem, err := fuse.Mount(ctx, cfg, dbResult.ConnStr, mountpoint)
	if err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Try to read non-existent row
	rowFile := mountpoint + "/users/999"
	_, err = os.ReadFile(rowFile)
	if err == nil {
		t.Fatal("Expected error reading non-existent row, got nil")
	}

	// Should be "no such file or directory" error
	if !os.IsNotExist(err) {
		t.Errorf("Expected IsNotExist error, got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

// Helper functions

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
