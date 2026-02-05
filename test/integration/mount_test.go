package integration

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestMount_ListTables(t *testing.T) {
	// Check FUSE availability (skips if not available)
	checkFUSEMountCapability(t)

	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Create config
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

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem with timeout (skips if FUSE unavailable)
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
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
	// Check FUSE availability (skips if not available)
	checkFUSEMountCapability(t)

	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Create config
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

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem with timeout (skips if FUSE unavailable)
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
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

	// Verify rows exist (filter out metadata files starting with .)
	var rowIDs []string
	for _, entry := range entries {
		t.Logf("Entry: %s (isDir=%v)", entry.Name(), entry.IsDir())
		// Skip metadata entries (files/dirs starting with .)
		if !strings.HasPrefix(entry.Name(), ".") {
			rowIDs = append(rowIDs, entry.Name())
		}
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
	// Check FUSE availability (skips if not available)
	checkFUSEMountCapability(t)

	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Create config
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

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem with timeout (skips if FUSE unavailable)
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Read row from users table as TSV
	// Note: Rows without extension are directories (row-as-directory), use .tsv for row-as-file
	rowFile := mountpoint + "/users/1.tsv"
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
	// Check FUSE availability (skips if not available)
	checkFUSEMountCapability(t)

	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Create config
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

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem with timeout (skips if FUSE unavailable)
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Try to read non-existent row
	rowFile := mountpoint + "/users/999"
	_, err := os.ReadFile(rowFile)
	if err == nil {
		t.Fatal("Expected error reading non-existent row, got nil")
	}

	// Should be "no such file or directory" error
	if !os.IsNotExist(err) {
		t.Errorf("Expected IsNotExist error, got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

func TestMount_FileSizes(t *testing.T) {
	// Check FUSE availability (skips if not available)
	checkFUSEMountCapability(t)

	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Create config
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

	// Create mountpoint
	mountpoint := t.TempDir()

	// Mount filesystem with timeout (skips if FUSE unavailable)
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test 1: Verify column file size matches content
	columnPath := mountpoint + "/users/1/email"
	columnContent, err := os.ReadFile(columnPath)
	if err != nil {
		t.Fatalf("Failed to read column file: %v", err)
	}

	columnStat, err := os.Stat(columnPath)
	if err != nil {
		t.Fatalf("Failed to stat column file: %v", err)
	}

	if columnStat.Size() != int64(len(columnContent)) {
		t.Errorf("Column file size mismatch: stat=%d, content=%d", columnStat.Size(), len(columnContent))
	}
	t.Logf("Column file size: %d bytes (content=%q)", columnStat.Size(), string(columnContent))

	// Test 2: Verify row file (.json) size matches content
	rowPath := mountpoint + "/users/1/.json"
	rowContent, err := os.ReadFile(rowPath)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	rowStat, err := os.Stat(rowPath)
	if err != nil {
		t.Fatalf("Failed to stat row file: %v", err)
	}

	if rowStat.Size() != int64(len(rowContent)) {
		t.Errorf("Row file size mismatch: stat=%d, content=%d", rowStat.Size(), len(rowContent))
	}
	t.Logf("Row file size: %d bytes", rowStat.Size())

	// Test 3: Verify metadata file (.info/count) size matches content
	countPath := mountpoint + "/users/.info/count"
	countContent, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatalf("Failed to read .info/count file: %v", err)
	}

	countStat, err := os.Stat(countPath)
	if err != nil {
		t.Fatalf("Failed to stat .info/count file: %v", err)
	}

	if countStat.Size() != int64(len(countContent)) {
		t.Errorf(".info/count file size mismatch: stat=%d, content=%d", countStat.Size(), len(countContent))
	}
	t.Logf(".info/count file size: %d bytes (content=%q)", countStat.Size(), strings.TrimSpace(string(countContent)))

	// Test 4: Verify all file sizes are non-zero for non-empty data
	if columnStat.Size() == 0 && len(columnContent) > 0 {
		t.Error("Column file reported size 0 but has content - Lookup not populating out.Attr")
	}
	if rowStat.Size() == 0 && len(rowContent) > 0 {
		t.Error("Row file reported size 0 but has content - Lookup not populating out.Attr")
	}
	if countStat.Size() == 0 && len(countContent) > 0 {
		t.Error(".count file reported size 0 but has content - Lookup not populating out.Attr")
	}
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
