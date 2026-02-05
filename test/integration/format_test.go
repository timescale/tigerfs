package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/nfs"
)

// Filesystem is an interface satisfied by both fuse.FS and nfs.FS.
// This allows tests to work with either mount method.
type Filesystem interface {
	Close() error
}

var (
	mountTestOnce   sync.Once
	mountTestWorks  bool
	mountTestReason string
	mountTestMethod MountMethod
)

// checkMountCapability performs a one-time check of filesystem mounting capability.
// On Linux, checks for FUSE. On macOS, checks based on TEST_MOUNT_METHOD env var.
// This avoids repeatedly timing out on systems where mounting doesn't work.
func checkMountCapability(t *testing.T) MountMethod {
	t.Helper()

	mountTestOnce.Do(func() {
		method, reason := getMountMethod(t)
		if reason != "" {
			mountTestWorks = false
			mountTestReason = reason
			return
		}
		mountTestWorks = true
		mountTestMethod = method
	})

	if !mountTestWorks {
		t.Skipf("Filesystem mounting not available: %s", mountTestReason)
	}

	return mountTestMethod
}

// checkFUSEMountCapability is deprecated - use checkMountCapability instead.
// Kept for backward compatibility with existing tests.
func checkFUSEMountCapability(t *testing.T) {
	t.Helper()
	checkMountCapability(t)
}

// mountWithTimeout attempts to mount a filesystem with a timeout.
// Uses FUSE on Linux, NFS on macOS (or Docker if TEST_MOUNT_METHOD=docker).
// Returns nil and skips the test if mount fails or times out.
// Also records failure so subsequent tests can skip immediately.
func mountWithTimeout(t *testing.T, cfg *config.Config, connStr, mountpoint string, timeout time.Duration) Filesystem {
	t.Helper()

	// Check mount capability and get method
	method := checkMountCapability(t)
	if method == "" {
		return nil
	}

	// Docker mount method not yet implemented
	if method == MountMethodDocker {
		t.Skip("TEST_MOUNT_METHOD=docker not yet implemented")
		return nil
	}

	mountCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type mountResult struct {
		fs  Filesystem
		err error
	}
	resultCh := make(chan mountResult, 1)

	go func() {
		var fs Filesystem
		var err error

		switch method {
		case MountMethodFUSE:
			fs, err = fuse.Mount(mountCtx, cfg, connStr, mountpoint)
		case MountMethodNFS:
			fs, err = nfs.Mount(mountCtx, cfg, connStr, mountpoint)
		default:
			err = fmt.Errorf("unknown mount method: %s", method)
		}

		resultCh <- mountResult{fs: fs, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			// Record failure so other tests skip immediately
			mountTestWorks = false
			mountTestReason = fmt.Sprintf("Mount failed (%s): %v", method, result.err)
			t.Skipf("Mount failed (%s may not be properly configured): %v", method, result.err)
			return nil
		}
		t.Logf("Mounted filesystem using %s", method)
		return result.fs
	case <-mountCtx.Done():
		// Record timeout so other tests skip immediately
		mountTestWorks = false
		mountTestReason = fmt.Sprintf("Mount timed out - %s may not be properly configured", method)
		t.Skip(mountTestReason)
		return nil
	}
}

// seedFormatTestData creates a test table with diverse data types and edge cases
func seedFormatTestData(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Create format_test table with diverse types
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS format_test (
			id serial PRIMARY KEY,
			text_col text,
			int_col integer,
			bool_col boolean,
			timestamp_col timestamp,
			numeric_col numeric(10,2)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create format_test table: %w", err)
	}

	// Insert test data with various edge cases
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	// Row 1: Normal values
	_, err = pool.Exec(ctx, `
		INSERT INTO format_test (id, text_col, int_col, bool_col, timestamp_col, numeric_col)
		VALUES (1, 'normal text', 42, true, $1, 19.99)
	`, testTime)
	if err != nil {
		return fmt.Errorf("failed to insert row 1: %w", err)
	}

	// Row 2: NULL values
	_, err = pool.Exec(ctx, `
		INSERT INTO format_test (id, text_col, int_col, bool_col, timestamp_col, numeric_col)
		VALUES (2, NULL, NULL, NULL, NULL, NULL)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert row 2: %w", err)
	}

	// Row 3: Special characters - tabs, newlines, quotes
	_, err = pool.Exec(ctx, `
		INSERT INTO format_test (id, text_col, int_col, bool_col, timestamp_col, numeric_col)
		VALUES (3, E'Text with:\ttab\nNewline\n"Quotes"', 100, false, $1, 0.01)
	`, testTime)
	if err != nil {
		return fmt.Errorf("failed to insert row 3: %w", err)
	}

	// Row 4: Commas (for CSV testing)
	_, err = pool.Exec(ctx, `
		INSERT INTO format_test (id, text_col, int_col, bool_col, timestamp_col, numeric_col)
		VALUES (4, 'Text, with, commas', -50, true, $1, 1234.56)
	`, testTime)
	if err != nil {
		return fmt.Errorf("failed to insert row 4: %w", err)
	}

	// Row 5: Mixed NULL and values
	_, err = pool.Exec(ctx, `
		INSERT INTO format_test (id, text_col, int_col, bool_col, timestamp_col, numeric_col)
		VALUES (5, 'some text', NULL, false, NULL, 99.99)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert row 5: %w", err)
	}

	return nil
}

func TestFormats_TSV_Default(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 1 as TSV
	// Note: Rows without extension are directories (row-as-directory), use .tsv for row-as-file
	rowFile := mountpoint + "/format_test/1.tsv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("TSV (default) content: %s", content)

	// Verify TSV format (tab-separated)
	if !strings.Contains(content, "\t") {
		t.Error("Expected tab-separated values in TSV format")
	}

	// Verify contains expected values
	if !strings.Contains(content, "1") {
		t.Error("Expected id=1 in output")
	}
	if !strings.Contains(content, "normal text") {
		t.Error("Expected 'normal text' in output")
	}
	if !strings.Contains(content, "42") {
		t.Error("Expected int_col=42 in output")
	}
	if !strings.Contains(content, "t") {
		t.Error("Expected bool_col=t in output")
	}
}

func TestFormats_TSV_Explicit(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 1 with explicit .tsv extension
	rowFile := mountpoint + "/format_test/1.tsv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("TSV (explicit) content: %s", content)

	// Verify TSV format
	if !strings.Contains(content, "\t") {
		t.Error("Expected tab-separated values in TSV format")
	}
}

func TestFormats_CSV(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 1 as CSV
	rowFile := mountpoint + "/format_test/1.csv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("CSV content: %s", content)

	// Verify CSV format (comma-separated)
	if !strings.Contains(content, ",") {
		t.Error("Expected comma-separated values in CSV format")
	}

	// Verify contains expected values
	if !strings.Contains(content, "1") {
		t.Error("Expected id=1 in output")
	}
	if !strings.Contains(content, "normal text") {
		t.Error("Expected 'normal text' in output")
	}
}

func TestFormats_CSV_WithCommas(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 4 (has commas in text) as CSV
	rowFile := mountpoint + "/format_test/4.csv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("CSV with commas content: %s", content)

	// Verify field with commas is quoted
	if !strings.Contains(content, "\"Text, with, commas\"") {
		t.Error("Expected field with commas to be quoted in CSV format")
	}
}

func TestFormats_JSON(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 1 as JSON
	rowFile := mountpoint + "/format_test/1.json"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("JSON content: %s", content)

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	// Verify contains expected fields
	if parsed["id"] == nil {
		t.Error("Expected 'id' field in JSON")
	}
	if parsed["text_col"] == nil {
		t.Error("Expected 'text_col' field in JSON")
	}
	if parsed["int_col"] == nil {
		t.Error("Expected 'int_col' field in JSON")
	}

	// Verify values
	if parsed["id"].(float64) != 1 {
		t.Errorf("Expected id=1, got %v", parsed["id"])
	}
	if parsed["text_col"].(string) != "normal text" {
		t.Errorf("Expected text_col='normal text', got %v", parsed["text_col"])
	}
	if parsed["int_col"].(float64) != 42 {
		t.Errorf("Expected int_col=42, got %v", parsed["int_col"])
	}
	if parsed["bool_col"].(bool) != true {
		t.Errorf("Expected bool_col=true, got %v", parsed["bool_col"])
	}
}

func TestFormats_NULL_TSV(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 2 (all NULLs) as TSV
	rowFile := mountpoint + "/format_test/2.tsv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("TSV NULL content: %s", content)

	// Verify NULL values represented as empty fields
	// Should have consecutive tabs for NULL values
	if !strings.Contains(content, "\t\t") {
		t.Error("Expected consecutive tabs for NULL values in TSV format")
	}
}

func TestFormats_NULL_CSV(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 2 (all NULLs) as CSV
	rowFile := mountpoint + "/format_test/2.csv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("CSV NULL content: %s", content)

	// Verify NULL values represented as empty fields
	// Should have consecutive commas for NULL values
	if !strings.Contains(content, ",,") {
		t.Error("Expected consecutive commas for NULL values in CSV format")
	}
}

func TestFormats_NULL_JSON(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 2 (all NULLs) as JSON
	rowFile := mountpoint + "/format_test/2.json"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("JSON NULL content: %s", content)

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	// Verify NULL values are JSON null
	if parsed["text_col"] != nil {
		t.Error("Expected text_col to be null in JSON")
	}
	if parsed["int_col"] != nil {
		t.Error("Expected int_col to be null in JSON")
	}
	if parsed["bool_col"] != nil {
		t.Error("Expected bool_col to be null in JSON")
	}
}

func TestFormats_SpecialCharacters_TSV(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 3 (special characters) as TSV
	rowFile := mountpoint + "/format_test/3.tsv"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("TSV special chars content: %s", content)

	// Verify special characters preserved in TSV
	// TSV does not escape tabs/newlines - they appear literally
	if !strings.Contains(content, "\t") {
		t.Error("Expected tab character in TSV format")
	}
	if !strings.Contains(content, "\n") {
		t.Error("Expected newline character in TSV format")
	}
}

func TestFormats_SpecialCharacters_JSON(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 3 (special characters) as JSON
	rowFile := mountpoint + "/format_test/3.json"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("JSON special chars content: %s", content)

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	// Verify special characters properly escaped and preserved
	textCol := parsed["text_col"].(string)
	if !strings.Contains(textCol, "\t") {
		t.Error("Expected tab character preserved in JSON")
	}
	if !strings.Contains(textCol, "\n") {
		t.Error("Expected newline character preserved in JSON")
	}
	if !strings.Contains(textCol, "\"") {
		t.Error("Expected quote character preserved in JSON")
	}
}

func TestFormats_YAML(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 1 as YAML
	rowFile := mountpoint + "/format_test/1.yaml"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("YAML content: %s", content)

	// Verify YAML format characteristics
	if !strings.Contains(content, "id:") {
		t.Error("Expected 'id:' key in YAML format")
	}
	if !strings.Contains(content, "text_col:") {
		t.Error("Expected 'text_col:' key in YAML format")
	}
	if !strings.Contains(content, "normal text") {
		t.Error("Expected 'normal text' value in YAML format")
	}
	if !strings.Contains(content, "int_col:") {
		t.Error("Expected 'int_col:' key in YAML format")
	}
	if !strings.Contains(content, "42") {
		t.Error("Expected '42' value in YAML format")
	}
}

func TestFormats_YAML_NULL(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test reading row 2 (all NULLs) as YAML
	rowFile := mountpoint + "/format_test/2.yaml"
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("YAML NULL content: %s", content)

	// Verify NULL values represented as null in YAML
	if !strings.Contains(content, "null") {
		t.Error("Expected 'null' for NULL values in YAML format")
	}
}

func TestMetadata_InfoSchema(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data (creates format_test table)
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test .info/schema
	schemaFile := mountpoint + "/format_test/.info/schema"
	data, err := os.ReadFile(schemaFile)
	if err != nil {
		t.Fatalf("Failed to read .info/schema: %v", err)
	}

	content := string(data)
	t.Logf(".info/schema content:\n%s", content)

	// Verify schema contains expected columns
	if !strings.Contains(content, "id") {
		t.Error("Expected 'id' column in schema")
	}
	if !strings.Contains(content, "text_col") {
		t.Error("Expected 'text_col' column in schema")
	}
	if !strings.Contains(content, "int_col") {
		t.Error("Expected 'int_col' column in schema")
	}
	if !strings.Contains(content, "bool_col") {
		t.Error("Expected 'bool_col' column in schema")
	}
}

func TestMetadata_InfoColumns(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test .info/columns
	columnsFile := mountpoint + "/format_test/.info/columns"
	data, err := os.ReadFile(columnsFile)
	if err != nil {
		t.Fatalf("Failed to read .info/columns: %v", err)
	}

	content := string(data)
	t.Logf(".info/columns content:\n%s", content)

	// Verify columns file contains column names (one per line)
	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) < 4 {
		t.Errorf("Expected at least 4 columns, got %d", len(lines))
	}

	// Check expected columns are present
	columnSet := make(map[string]bool)
	for _, line := range lines {
		columnSet[strings.TrimSpace(line)] = true
	}

	expectedColumns := []string{"id", "text_col", "int_col", "bool_col"}
	for _, col := range expectedColumns {
		if !columnSet[col] {
			t.Errorf("Expected column '%s' in .info/columns", col)
		}
	}
}

func TestMetadata_InfoCount(t *testing.T) {
	// Get empty test database to ensure clean state
	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data (creates 5 rows)
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test .info/count
	countFile := mountpoint + "/format_test/.info/count"
	data, err := os.ReadFile(countFile)
	if err != nil {
		t.Fatalf("Failed to read .info/count: %v", err)
	}

	content := strings.TrimSpace(string(data))
	t.Logf(".info/count content: %s", content)

	// Verify count is 5 (seedFormatTestData creates 5 rows)
	if content != "5" {
		t.Errorf("Expected count '5', got '%s'", content)
	}
}

func TestMetadata_InfoDDL(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test .info/ddl
	ddlFile := mountpoint + "/format_test/.info/ddl"
	data, err := os.ReadFile(ddlFile)
	if err != nil {
		t.Fatalf("Failed to read .info/ddl: %v", err)
	}

	content := string(data)
	t.Logf(".info/ddl content:\n%s", content)

	// Verify DDL contains CREATE TABLE
	if !strings.Contains(strings.ToUpper(content), "CREATE TABLE") {
		t.Error("Expected 'CREATE TABLE' in .info/ddl")
	}
	if !strings.Contains(content, "format_test") {
		t.Error("Expected table name 'format_test' in .info/ddl")
	}
}

func TestMetadata_InfoIndexes(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check FUSE capability once for all format tests
	checkFUSEMountCapability(t)

	// Seed format test data
	ctx := context.Background()
	if err := seedFormatTestData(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed format test data: %v", err)
	}

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

	// Mount filesystem with timeout
	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	// Give filesystem time to initialize
	time.Sleep(500 * time.Millisecond)

	// Test .info/indexes
	indexesFile := mountpoint + "/format_test/.info/indexes"
	data, err := os.ReadFile(indexesFile)
	if err != nil {
		t.Fatalf("Failed to read .info/indexes: %v", err)
	}

	content := string(data)
	t.Logf(".info/indexes content:\n%s", content)

	// format_test table has a primary key, so should have at least the PK index
	// The content format may vary, but it should not be empty
	if len(strings.TrimSpace(content)) == 0 {
		t.Error("Expected non-empty .info/indexes content (at least primary key)")
	}
}
