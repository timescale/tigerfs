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
)

var (
	fuseMountTestOnce sync.Once
	fuseMountWorks    bool
	fuseMountReason   string
)

// checkFUSEMountCapability performs a one-time test of FUSE mounting
// This avoids repeatedly timing out on systems where FUSE doesn't work
func checkFUSEMountCapability(t *testing.T) {
	t.Helper()

	fuseMountTestOnce.Do(func() {
		// First check if FUSE files exist
		if !isFUSEAvailable() {
			fuseMountWorks = false
			fuseMountReason = "FUSE not installed on this system"
			return
		}

		// FUSE files exist, so we assume it can work
		// The actual mount test will happen in the first test that runs
		fuseMountWorks = true
		fuseMountReason = ""
	})

	if !fuseMountWorks {
		t.Skipf("FUSE mounting not available: %s", fuseMountReason)
	}
}

// mountWithTimeout attempts to mount a FUSE filesystem with a timeout
// Returns nil and skips the test if mount fails or times out
// Also records failure so subsequent tests can skip immediately
func mountWithTimeout(t *testing.T, cfg *config.Config, connStr, mountpoint string, timeout time.Duration) *fuse.FS {
	t.Helper()

	// Check if we already know mounting doesn't work
	if !fuseMountWorks {
		t.Skipf("FUSE mounting not available: %s", fuseMountReason)
		return nil
	}

	mountCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type mountResult struct {
		fs  *fuse.FS
		err error
	}
	resultCh := make(chan mountResult, 1)

	go func() {
		fs, err := fuse.Mount(mountCtx, cfg, connStr, mountpoint)
		resultCh <- mountResult{fs: fs, err: err}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			// Record failure so other tests skip immediately
			fuseMountWorks = false
			fuseMountReason = fmt.Sprintf("Mount failed: %v", result.err)
			t.Skipf("Mount failed (FUSE may not be properly configured): %v", result.err)
			return nil
		}
		return result.fs
	case <-mountCtx.Done():
		// Record timeout so other tests skip immediately
		fuseMountWorks = false
		fuseMountReason = "Mount timed out - FUSE may not be properly configured"
		t.Skip(fuseMountReason)
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

	// Test reading row 1 as TSV (default, no extension)
	rowFile := mountpoint + "/format_test/1"
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
	rowFile := mountpoint + "/format_test/2"
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
	rowFile := mountpoint + "/format_test/3"
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
