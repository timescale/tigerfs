package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// setupPipelineTestData creates test data specifically for pipeline tests.
// This includes larger datasets and indexes to test filtering, pagination, and ordering.
func setupPipelineTestData(t *testing.T, ctx context.Context, connStr, schemaName string) error {
	t.Helper()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer pool.Close()

	// Create orders table with indexed columns for pipeline testing
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.orders (
			id serial PRIMARY KEY,
			customer_id int NOT NULL,
			status text NOT NULL,
			amount numeric(10,2) NOT NULL,
			notes text,
			created_at timestamp DEFAULT NOW()
		)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to create orders table: %w", err)
	}

	// Create indexes for .by/ testing
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX idx_orders_status ON %s.orders(status)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to create status index: %w", err)
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE INDEX idx_orders_customer_id ON %s.orders(customer_id)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to create customer_id index: %w", err)
	}

	// Insert test orders - enough data for pagination testing
	// Customer 1: 5 orders (3 pending, 2 completed)
	// Customer 2: 5 orders (2 pending, 3 completed)
	// Customer 3: 5 orders (all pending)
	orders := []struct {
		customerID int
		status     string
		amount     float64
		notes      string
	}{
		// Customer 1
		{1, "pending", 100.00, "urgent"},
		{1, "pending", 150.00, "normal"},
		{1, "pending", 200.00, "urgent"},
		{1, "completed", 75.00, "normal"},
		{1, "completed", 125.00, "normal"},
		// Customer 2
		{2, "pending", 50.00, "normal"},
		{2, "pending", 80.00, "urgent"},
		{2, "completed", 120.00, "normal"},
		{2, "completed", 90.00, "normal"},
		{2, "completed", 110.00, "urgent"},
		// Customer 3
		{3, "pending", 200.00, "urgent"},
		{3, "pending", 250.00, "urgent"},
		{3, "pending", 300.00, "normal"},
		{3, "pending", 350.00, "normal"},
		{3, "pending", 400.00, "urgent"},
	}

	for _, o := range orders {
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.orders (customer_id, status, amount, notes)
			VALUES ($1, $2, $3, $4)
		`, schemaName), o.customerID, o.status, o.amount, o.notes)
		if err != nil {
			return fmt.Errorf("failed to insert order: %w", err)
		}
	}

	return nil
}

// TestPipeline_SimpleBy tests basic .by/ index filtering
func TestPipeline_SimpleBy(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Extract schema name from connection string for additional setup
	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	// Add pipeline-specific test data
	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("ByStatus", func(t *testing.T) {
		// Navigate to .by/status/pending/
		pendingDir := filepath.Join(mountpoint, "orders", ".by", "status", "pending")
		entries, err := os.ReadDir(pendingDir)
		if err != nil {
			t.Fatalf("Failed to read .by/status/pending/: %v", err)
		}

		// Should have 10 pending orders (3+2+5)
		rowCount := countNonDotEntries(entries)
		if rowCount != 10 {
			t.Errorf("Expected 10 pending orders, got %d", rowCount)
			logEntries(t, entries)
		}
	})

	t.Run("ByCustomerId", func(t *testing.T) {
		// Navigate to .by/customer_id/1/
		customer1Dir := filepath.Join(mountpoint, "orders", ".by", "customer_id", "1")
		entries, err := os.ReadDir(customer1Dir)
		if err != nil {
			t.Fatalf("Failed to read .by/customer_id/1/: %v", err)
		}

		// Customer 1 has 5 orders
		rowCount := countNonDotEntries(entries)
		if rowCount != 5 {
			t.Errorf("Expected 5 orders for customer 1, got %d", rowCount)
			logEntries(t, entries)
		}
	})
}

// TestPipeline_SimpleFilter tests basic .filter/ filtering
func TestPipeline_SimpleFilter(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("FilterNotes", func(t *testing.T) {
		// .filter/ works on any column (notes is not indexed)
		urgentDir := filepath.Join(mountpoint, "orders", ".filter", "notes", "urgent")
		entries, err := os.ReadDir(urgentDir)
		if err != nil {
			t.Fatalf("Failed to read .filter/notes/urgent/: %v", err)
		}

		// Should have urgent orders (1+1+1 from customer 1, 1+1 from customer 2, 3 from customer 3 = 8)
		rowCount := countNonDotEntries(entries)
		if rowCount != 8 {
			t.Errorf("Expected 8 urgent orders, got %d", rowCount)
			logEntries(t, entries)
		}
	})
}

// TestPipeline_ByWithFirst tests .by/<col>/<val>/.first/N/
func TestPipeline_ByWithFirst(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("First3Pending", func(t *testing.T) {
		// .by/status/pending/.first/3/
		first3Dir := filepath.Join(mountpoint, "orders", ".by", "status", "pending", ".first", "3")
		entries, err := os.ReadDir(first3Dir)
		if err != nil {
			t.Fatalf("Failed to read .by/status/pending/.first/3/: %v", err)
		}

		// Should have exactly 3 rows
		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
			logEntries(t, entries)
		}
	})
}

// TestPipeline_NestedPagination tests nested .first/.last combinations
func TestPipeline_NestedPagination(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("FirstThenLast", func(t *testing.T) {
		// .first/10/.last/3/ - first 10, then last 3 of those = rows 8-10
		lastDir := filepath.Join(mountpoint, "orders", ".first", "10", ".last", "3")
		entries, err := os.ReadDir(lastDir)
		if err != nil {
			t.Fatalf("Failed to read .first/10/.last/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
			logEntries(t, entries)
		}
	})

	t.Run("LastThenFirst", func(t *testing.T) {
		// .last/10/.first/3/ - last 10, then first 3 of those
		firstDir := filepath.Join(mountpoint, "orders", ".last", "10", ".first", "3")
		entries, err := os.ReadDir(firstDir)
		if err != nil {
			t.Fatalf("Failed to read .last/10/.first/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
			logEntries(t, entries)
		}
	})
}

// TestPipeline_MixedFilters tests combining .by/ and .filter/
func TestPipeline_MixedFilters(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("ByThenFilter", func(t *testing.T) {
		// .by/customer_id/1/.filter/notes/urgent/
		// Customer 1 has 5 orders, 2 are urgent
		dir := filepath.Join(mountpoint, "orders", ".by", "customer_id", "1", ".filter", "notes", "urgent")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .by/customer_id/1/.filter/notes/urgent/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 2 {
			t.Errorf("Expected 2 urgent orders for customer 1, got %d", rowCount)
			logEntries(t, entries)
		}
	})

	t.Run("FilterThenBy", func(t *testing.T) {
		// .filter/notes/urgent/.by/customer_id/3/
		// Customer 3 has 3 urgent orders
		dir := filepath.Join(mountpoint, "orders", ".filter", "notes", "urgent", ".by", "customer_id", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .filter/notes/urgent/.by/customer_id/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 urgent orders for customer 3, got %d", rowCount)
			logEntries(t, entries)
		}
	})
}

// TestPipeline_ConflictingFilters tests AND semantics for same-column filters
func TestPipeline_ConflictingFilters(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("SameColumnDifferentValues", func(t *testing.T) {
		// .by/status/pending/.by/status/completed/ should return empty
		// (status cannot be both pending AND completed)
		dir := filepath.Join(mountpoint, "orders", ".by", "status", "pending", ".by", "status", "completed")
		_, err := os.ReadDir(dir)

		// Should get ENOENT because no rows match
		if err == nil {
			t.Error("Expected error (ENOENT) for conflicting filters, got nil")
		} else if !os.IsNotExist(err) {
			t.Errorf("Expected IsNotExist error, got: %v", err)
		}
	})

	t.Run("SameColumnSameValue", func(t *testing.T) {
		// .by/status/pending/.by/status/pending/ should work (redundant but valid)
		dir := filepath.Join(mountpoint, "orders", ".by", "status", "pending", ".by", "status", "pending")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read redundant filter path: %v", err)
		}

		// Should have 10 pending orders
		rowCount := countNonDotEntries(entries)
		if rowCount != 10 {
			t.Errorf("Expected 10 pending orders, got %d", rowCount)
		}
	})
}

// TestPipeline_OrderAndExport tests .order/ and .export/ capabilities
func TestPipeline_OrderAndExport(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("OrderByAmount", func(t *testing.T) {
		// .by/customer_id/1/.order/amount/.first/3/
		dir := filepath.Join(mountpoint, "orders", ".by", "customer_id", "1", ".order", "amount", ".first", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read ordered path: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
		}
	})

	t.Run("ExportJSON", func(t *testing.T) {
		// .by/customer_id/1/.first/3/.export/json
		exportPath := filepath.Join(mountpoint, "orders", ".by", "customer_id", "1", ".first", "3", ".export", "json")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read export file: %v", err)
		}

		// Should be valid JSON array
		var rows []map[string]interface{}
		if err := json.Unmarshal(data, &rows); err != nil {
			t.Fatalf("Invalid JSON: %v\nContent: %s", err, string(data))
		}

		if len(rows) != 3 {
			t.Errorf("Expected 3 rows in export, got %d", len(rows))
		}
	})

	t.Run("ExportCSV", func(t *testing.T) {
		// .first/5/.export/csv
		exportPath := filepath.Join(mountpoint, "orders", ".first", "5", ".export", "csv")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read CSV export: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		// Should have header + 5 data rows = 6 lines
		if len(lines) != 6 {
			t.Errorf("Expected 6 lines (header + 5 rows), got %d", len(lines))
		}
	})
}

// TestPipeline_FullPipeline tests complex multi-step pipelines
func TestPipeline_FullPipeline(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("ByOrderLastExport", func(t *testing.T) {
		// .by/status/pending/.order/amount/.last/5/.export/json
		exportPath := filepath.Join(mountpoint, "orders", ".by", "status", "pending",
			".order", "amount", ".last", "5", ".export", "json")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read full pipeline export: %v", err)
		}

		var rows []map[string]interface{}
		if err := json.Unmarshal(data, &rows); err != nil {
			t.Fatalf("Invalid JSON: %v", err)
		}

		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}
	})
}

// TestPipeline_DisallowedCombinations tests paths that should return ENOENT
func TestPipeline_DisallowedCombinations(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	disallowedPaths := []struct {
		name string
		path string
	}{
		{"ExportThenFirst", filepath.Join(mountpoint, "orders", ".export", "csv", ".first", "10")},
		{"OrderThenOrder", filepath.Join(mountpoint, "orders", ".order", "amount", ".order", "status")},
		{"SampleThenSample", filepath.Join(mountpoint, "orders", ".sample", "5", ".sample", "3")},
		{"FilterAfterOrder", filepath.Join(mountpoint, "orders", ".order", "amount", ".filter", "status", "pending")},
	}

	for _, tc := range disallowedPaths {
		t.Run(tc.name, func(t *testing.T) {
			_, err := os.ReadDir(tc.path)
			if err == nil {
				t.Errorf("Expected error for disallowed path %s, got nil", tc.path)
			} else if !os.IsNotExist(err) {
				// Some paths may return permission denied or other errors
				t.Logf("Path %s returned: %v", tc.path, err)
			}
		})
	}
}

// TestPipeline_BackwardCompatibility tests that existing paths still work
func TestPipeline_BackwardCompatibility(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaName := extractSchemaName(dbResult.ConnStr)
	baseConnStr := stripSchemaFromConnStr(dbResult.ConnStr)

	if err := setupPipelineTestData(t, ctx, baseConnStr, schemaName); err != nil {
		t.Fatalf("Failed to setup pipeline test data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	t.Run("DirectExport", func(t *testing.T) {
		// Direct .export/csv at table level should work
		exportPath := filepath.Join(mountpoint, "orders", ".export", "csv")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read direct export: %v", err)
		}

		if len(data) == 0 {
			t.Error("Export file is empty")
		}
	})

	t.Run("DirectFirst", func(t *testing.T) {
		// Direct .first/5/ at table level should work
		dir := filepath.Join(mountpoint, "orders", ".first", "5")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .first/5/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 5 {
			t.Errorf("Expected 5 rows, got %d", rowCount)
		}
	})

	t.Run("DirectBy", func(t *testing.T) {
		// .by/<col>/<val>/ should work
		dir := filepath.Join(mountpoint, "orders", ".by", "status", "pending")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .by/status/pending/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount == 0 {
			t.Error("Expected rows for .by/status/pending/")
		}
	})
}

// Helper functions

func defaultTestConfig() *config.Config {
	return &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		DirFilterLimit:          100000,
		QueryTimeout:            30 * time.Second,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		Debug:                   false,
	}
}

func countNonDotEntries(entries []os.DirEntry) int {
	count := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			count++
		}
	}
	return count
}

func logEntries(t *testing.T, entries []os.DirEntry) {
	t.Helper()
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	t.Logf("Entries: %v", names)
}

func extractSchemaName(connStr string) string {
	// Extract schema name from search_path parameter
	if idx := strings.Index(connStr, "search_path="); idx != -1 {
		rest := connStr[idx+len("search_path="):]
		if endIdx := strings.Index(rest, "&"); endIdx != -1 {
			return rest[:endIdx]
		}
		return rest
	}
	return "public"
}

func stripSchemaFromConnStr(connStr string) string {
	// Remove search_path parameter to get base connection string
	if idx := strings.Index(connStr, "&search_path="); idx != -1 {
		return connStr[:idx]
	}
	if idx := strings.Index(connStr, "?search_path="); idx != -1 {
		return connStr[:idx]
	}
	return connStr
}
