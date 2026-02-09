package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// seedImportTestTable creates a test table for import operations.
func seedImportTestTable(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Drop and recreate the table to ensure clean state
	_, err = pool.Exec(ctx, `DROP TABLE IF EXISTS import_test`)
	if err != nil {
		return fmt.Errorf("failed to drop import_test table: %w", err)
	}

	// Create import_test table with various types including timestamps
	_, err = pool.Exec(ctx, `
		CREATE TABLE import_test (
			id uuid PRIMARY KEY,
			user_id integer,
			product_id integer,
			quantity integer,
			total numeric(10,2),
			status text,
			created_at timestamp
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create import_test table: %w", err)
	}

	return nil
}

// seedExportTestTable creates a test table with sample data for export tests.
func seedExportTestTable(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Drop and recreate the table to ensure clean state
	_, err = pool.Exec(ctx, `DROP TABLE IF EXISTS export_test`)
	if err != nil {
		return fmt.Errorf("failed to drop export_test table: %w", err)
	}

	// Create export_test table
	_, err = pool.Exec(ctx, `
		CREATE TABLE export_test (
			id serial PRIMARY KEY,
			name text,
			value integer
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create export_test table: %w", err)
	}

	// Insert sample data
	_, err = pool.Exec(ctx, `
		INSERT INTO export_test (id, name, value) VALUES
		(1, 'Alice', 100),
		(2, 'Bob', 200),
		(3, 'Charlie', 300)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert export_test data: %w", err)
	}

	return nil
}

// getRowCount returns the number of rows in a table.
func getRowCount(ctx context.Context, connStr, table string) (int, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	var count int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	return count, err
}

// TestImport_WithTimestamps tests importing data with ISO 8601 timestamps.
// Regression test: Binary COPY protocol couldn't handle timestamp strings.
func TestImport_WithTimestamps(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()
	if err := seedImportTestTable(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed import test table: %v", err)
	}

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		DirWritingLimit:         100000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// CSV data with ISO 8601 timestamps (the format that caused the error)
	csvData := `id,user_id,product_id,quantity,total,status,created_at
019c0cfd-1edd-7a1c-b51c-d09df4caae97,2,2,2,23.00,pending,2026-01-29T02:40:41.053167Z
019c0cfd-1edd-7b74-9ea6-0a08f27ff6af,3,3,3,39.00,shipped,2026-01-28T01:40:41.053167Z
`

	// Write to import file
	importFile := mountpoint + "/import_test/.import/.overwrite/csv"
	err := os.WriteFile(importFile, []byte(csvData), 0644)
	if err != nil {
		t.Fatalf("Failed to write import file: %v", err)
	}

	// Verify data was imported
	count, err := getRowCount(ctx, dbResult.ConnStr, "import_test")
	if err != nil {
		t.Fatalf("Failed to get row count: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows imported, got %d", count)
	}
}

// TestImport_OverwriteMode tests that .overwrite mode truncates existing data.
func TestImport_OverwriteMode(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()
	if err := seedImportTestTable(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed import test table: %v", err)
	}

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		DirWritingLimit:         100000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// First import
	csvData1 := `id,user_id,product_id,quantity,total,status,created_at
019c0cfd-1edd-7a1c-b51c-d09df4caae97,1,1,1,10.00,pending,2026-01-29T02:40:41Z
019c0cfd-1edd-7b74-9ea6-0a08f27ff6af,2,2,2,20.00,pending,2026-01-29T02:40:41Z
`
	importFile := mountpoint + "/import_test/.import/.overwrite/csv"
	if err := os.WriteFile(importFile, []byte(csvData1), 0644); err != nil {
		t.Fatalf("Failed to write first import: %v", err)
	}

	count1, _ := getRowCount(ctx, dbResult.ConnStr, "import_test")
	if count1 != 2 {
		t.Errorf("Expected 2 rows after first import, got %d", count1)
	}

	// Second import with different data - should replace all
	csvData2 := `id,user_id,product_id,quantity,total,status,created_at
019c0cfd-2222-7a1c-b51c-d09df4caae97,3,3,3,30.00,shipped,2026-01-29T02:40:41Z
`
	if err := os.WriteFile(importFile, []byte(csvData2), 0644); err != nil {
		t.Fatalf("Failed to write second import: %v", err)
	}

	count2, _ := getRowCount(ctx, dbResult.ConnStr, "import_test")
	if count2 != 1 {
		t.Errorf("Expected 1 row after overwrite, got %d (overwrite may not have truncated)", count2)
	}
}

// TestExport_WithHeaders tests that .with-headers includes column names.
func TestExport_WithHeaders(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()
	if err := seedExportTestTable(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed export test table: %v", err)
	}

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

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Export with headers
	exportFile := mountpoint + "/export_test/.export/.with-headers/csv"
	data, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("Failed to read export file: %v", err)
	}

	content := string(data)
	t.Logf("Export with headers:\n%s", content)

	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) < 1 {
		t.Fatal("Expected at least one line (header)")
	}

	// First line should be column names
	header := lines[0]
	if !strings.Contains(header, "id") || !strings.Contains(header, "name") || !strings.Contains(header, "value") {
		t.Errorf("Header line should contain column names, got: %s", header)
	}

	// Should have header + 3 data rows
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines (1 header + 3 data), got %d", len(lines))
	}
}

// TestExport_WithoutHeaders tests that default export has no header row.
func TestExport_WithoutHeaders(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()
	if err := seedExportTestTable(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed export test table: %v", err)
	}

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

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Export without headers (default)
	exportFile := mountpoint + "/export_test/.export/csv"
	data, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("Failed to read export file: %v", err)
	}

	content := string(data)
	t.Logf("Export without headers:\n%s", content)

	lines := strings.Split(strings.TrimSpace(content), "\n")

	// Should have 3 data rows only (no header)
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines (data only, no header), got %d", len(lines))
	}

	// First line should be data, not column names
	firstLine := lines[0]
	if strings.HasPrefix(firstLine, "id,name,value") {
		t.Error("First line appears to be a header row, expected data")
	}

	// First line should start with "1" (first ID)
	if !strings.HasPrefix(firstLine, "1,") {
		t.Errorf("First data line should start with '1,', got: %s", firstLine)
	}
}

// TestExportImportRoundTrip_WithHeaders tests export with headers can be imported.
func TestExportImportRoundTrip_WithHeaders(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()

	// Create source table with data
	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	// Create source table
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS roundtrip_src;
		CREATE TABLE roundtrip_src (id serial PRIMARY KEY, name text, score integer);
		INSERT INTO roundtrip_src (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 200);
	`)
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Create destination table
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS roundtrip_dst;
		CREATE TABLE roundtrip_dst (id serial PRIMARY KEY, name text, score integer);
	`)
	if err != nil {
		t.Fatalf("Failed to create destination table: %v", err)
	}

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		DirWritingLimit:         100000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Export with headers
	exportFile := mountpoint + "/roundtrip_src/.export/.with-headers/csv"
	exportData, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}
	t.Logf("Exported data:\n%s", string(exportData))

	// Import into destination (standard import expects headers)
	importFile := mountpoint + "/roundtrip_dst/.import/.overwrite/csv"
	if err := os.WriteFile(importFile, exportData, 0644); err != nil {
		t.Fatalf("Failed to write import: %v", err)
	}

	// Verify round-trip
	count, _ := getRowCount(ctx, dbResult.ConnStr, "roundtrip_dst")
	if count != 2 {
		t.Errorf("Expected 2 rows in destination, got %d", count)
	}
}

// TestExportImportRoundTrip_NoHeaders tests export without headers can be imported with .no-headers.
func TestExportImportRoundTrip_NoHeaders(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	// Create source table
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS noheader_src;
		CREATE TABLE noheader_src (id serial PRIMARY KEY, name text, score integer);
		INSERT INTO noheader_src (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 200);
	`)
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Create destination table with same schema
	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS noheader_dst;
		CREATE TABLE noheader_dst (id serial PRIMARY KEY, name text, score integer);
	`)
	if err != nil {
		t.Fatalf("Failed to create destination table: %v", err)
	}

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		DirWritingLimit:         100000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Export without headers (default)
	exportFile := mountpoint + "/noheader_src/.export/csv"
	exportData, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatalf("Failed to read export: %v", err)
	}
	t.Logf("Exported data (no headers):\n%s", string(exportData))

	// Import with .no-headers option
	importFile := mountpoint + "/noheader_dst/.import/.overwrite/.no-headers/csv"
	if err := os.WriteFile(importFile, exportData, 0644); err != nil {
		t.Fatalf("Failed to write import: %v", err)
	}

	// Verify round-trip
	count, _ := getRowCount(ctx, dbResult.ConnStr, "noheader_dst")
	if count != 2 {
		t.Errorf("Expected 2 rows in destination, got %d", count)
	}
}

// TestAccess_ViaSchemaPath tests that accessing tables via .schemas/<schema>/<table> works.
// Regression test: Schema was empty when GetDefaultSchema was called before cache refresh.
func TestAccess_ViaSchemaPath(t *testing.T) {
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	checkFUSEMountCapability(t)

	ctx := context.Background()
	if err := seedExportTestTable(ctx, dbResult.ConnStr); err != nil {
		t.Fatalf("Failed to seed export test table: %v", err)
	}

	schemaName := extractSchemaName(dbResult.ConnStr)

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           schemaName,
		DirListingLimit:         10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}

	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 5*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Access via .schemas path using the actual test schema
	schemaPath := mountpoint + "/.schemas/" + schemaName + "/export_test/.export/csv"
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to read via schema path: %v", err)
	}

	content := string(data)
	t.Logf("Content via schema path:\n%s", content)

	// Should have valid data
	if len(content) == 0 {
		t.Error("Expected non-empty content")
	}

	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 data rows, got %d", len(lines))
	}
}
