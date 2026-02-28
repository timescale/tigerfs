package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
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

		// Should have urgent orders (2 from customer 1, 2 from customer 2, 3 from customer 3 = 7)
		rowCount := countNonDotEntries(entries)
		if rowCount != 7 {
			t.Errorf("Expected 7 urgent orders, got %d", rowCount)
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
		// .by/status/pending/.by/status/completed/ should return empty directory
		// (status cannot be both pending AND completed)
		// Note: Empty results return an empty listing, not ENOENT, consistent with
		// normal filesystem behavior where directories can be empty but still exist.
		dir := filepath.Join(mountpoint, "orders", ".by", "status", "pending", ".by", "status", "completed")
		entries, err := os.ReadDir(dir)

		if err != nil {
			t.Fatalf("Failed to read conflicting filter path: %v", err)
		}

		// Should have no rows (empty directory)
		rowCount := countNonDotEntries(entries)
		if rowCount != 0 {
			t.Errorf("Expected 0 rows for conflicting filters, got %d", rowCount)
			logEntries(t, entries)
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
		// .first/5/.export/csv (no header, use .with-headers/csv for headers)
		exportPath := filepath.Join(mountpoint, "orders", ".first", "5", ".export", "csv")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read CSV export: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		// Should have 5 data rows (no header - use .export/.with-headers/csv for headers)
		if len(lines) != 5 {
			t.Errorf("Expected 5 lines (5 data rows, no header), got %d", len(lines))
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

// TestMount_ColumnsExportJSON tests .columns/ with JSON export.
func TestMount_ColumnsExportJSON(t *testing.T) {
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

	// .columns/id,status/.export/json
	exportPath := filepath.Join(mountpoint, "orders", ".columns", "id,status", ".export", "json")
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read columns export: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("Invalid JSON: %v\nContent: %s", err, string(data))
	}

	if len(rows) == 0 {
		t.Fatal("Expected rows in export, got none")
	}

	// Each row should only have "id" and "status" keys
	for i, row := range rows {
		if _, ok := row["id"]; !ok {
			t.Errorf("Row %d missing 'id' key", i)
		}
		if _, ok := row["status"]; !ok {
			t.Errorf("Row %d missing 'status' key", i)
		}
		// Should NOT have other columns like amount, customer_id, etc.
		if _, ok := row["amount"]; ok {
			t.Errorf("Row %d should not have 'amount' key with .columns/id,status", i)
		}
		if _, ok := row["customer_id"]; ok {
			t.Errorf("Row %d should not have 'customer_id' key with .columns/id,status", i)
		}
	}
}

// TestMount_ColumnsExportCSV tests .columns/ with CSV export.
func TestMount_ColumnsExportCSV(t *testing.T) {
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

	// .columns/id,amount/.export/.with-headers/csv
	exportPath := filepath.Join(mountpoint, "orders", ".columns", "id,amount", ".export", ".with-headers", "csv")
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read columns CSV export: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		t.Fatalf("Expected at least 2 lines (header + data), got %d", len(lines))
	}

	// Header should be exactly "id,amount"
	header := lines[0]
	if header != "id,amount" {
		t.Errorf("CSV header = %q, want %q", header, "id,amount")
	}

	// Each data line should have exactly 2 fields
	for i := 1; i < len(lines); i++ {
		fields := strings.Split(lines[i], ",")
		if len(fields) != 2 {
			t.Errorf("Line %d has %d fields, want 2: %q", i, len(fields), lines[i])
		}
	}
}

// TestMount_ColumnsWithFilter tests .filter/ combined with .columns/.
func TestMount_ColumnsWithFilter(t *testing.T) {
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

	// .filter/status/pending/.columns/id,amount/.export/json
	exportPath := filepath.Join(mountpoint, "orders", ".filter", "status", "pending",
		".columns", "id,amount", ".export", "json")
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("Failed to read filtered columns export: %v", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("Invalid JSON: %v\nContent: %s", err, string(data))
	}

	// Should have 10 pending orders
	if len(rows) != 10 {
		t.Errorf("Expected 10 pending orders, got %d", len(rows))
	}

	// Each row should only have "id" and "amount"
	for i, row := range rows {
		if len(row) != 2 {
			t.Errorf("Row %d has %d keys, want 2 (id, amount): %v", i, len(row), row)
		}
	}
}

// TestMount_ColumnsInvalidColumn tests that nonexistent columns produce an error.
// After column validation fix, stat on .columns/nonexistent/ returns ENOENT,
// so the export path is unreachable.
func TestMount_ColumnsInvalidColumn(t *testing.T) {
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

	// .columns/nonexistent/ should fail at stat — the directory doesn't exist
	colDir := filepath.Join(mountpoint, "orders", ".columns", "nonexistent")
	_, err := os.Stat(colDir)
	if err == nil {
		t.Error("Expected error for .columns/nonexistent/, but stat succeeded")
	}

	// Export path should also fail since the parent doesn't exist
	exportPath := filepath.Join(mountpoint, "orders", ".columns", "nonexistent", ".export", "json")
	_, err = os.ReadFile(exportPath)
	if err == nil {
		t.Error("Expected error reading .columns/nonexistent/.export/json")
	}
}

// TestMount_ColumnsInvalidColumnDir tests that stat on .columns/nonexistent/ fails
// with a "not a directory" or "no such file" error because the invalid column name
// is caught during validation.
func TestMount_ColumnsInvalidColumnDir(t *testing.T) {
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

	// Valid columns should work
	validDir := filepath.Join(mountpoint, "orders", ".columns", "id,status")
	_, err := os.Stat(validDir)
	if err != nil {
		t.Errorf("Expected .columns/id,status/ to exist, got error: %v", err)
	}

	// Invalid column should fail
	invalidDir := filepath.Join(mountpoint, "orders", ".columns", "id,bogus")
	_, err = os.Stat(invalidDir)
	if err == nil {
		t.Error("Expected .columns/id,bogus/ to fail stat, but it succeeded")
	}

	// Mix of valid and invalid should also fail
	mixedDir := filepath.Join(mountpoint, "orders", ".columns", "status,nonexistent,amount")
	_, err = os.Stat(mixedDir)
	if err == nil {
		t.Error("Expected .columns/status,nonexistent,amount/ to fail stat, but it succeeded")
	}
}

// TestMount_ColumnsRowDirFiltered tests that ls on a row directory under
// .columns/id,status/ only shows the projected column files.
func TestMount_ColumnsRowDirFiltered(t *testing.T) {
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

	// First, get a row PK by listing the table
	tablePath := filepath.Join(mountpoint, "orders")
	tableEntries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to read orders table: %v", err)
	}

	// Find a numeric row PK (skip capability directories like .by, .export, etc.)
	var rowPK string
	for _, entry := range tableEntries {
		name := entry.Name()
		if len(name) > 0 && name[0] != '.' {
			rowPK = name
			break
		}
	}
	if rowPK == "" {
		t.Fatal("No row found in orders table")
	}

	// ls orders/.columns/id,status/<pk>/ should only show id and status column files
	projectedRowDir := filepath.Join(mountpoint, "orders", ".columns", "id,status", rowPK)
	entries, err := os.ReadDir(projectedRowDir)
	if err != nil {
		t.Fatalf("Failed to read projected row dir: %v", err)
	}

	names := make(map[string]bool, len(entries))
	for _, e := range entries {
		names[e.Name()] = true
	}

	// Should have the projected columns
	if !names["id"] {
		t.Error("Expected 'id' column file in projected row dir")
	}
	if !names["status.txt"] {
		t.Error("Expected 'status.txt' column file in projected row dir")
	}

	// Should NOT have non-projected columns
	for _, unwanted := range []string{"customer_id", "amount", "notes.txt", "created_at"} {
		if names[unwanted] {
			t.Errorf("Did not expect %q in projected row dir, but found it", unwanted)
		}
	}

	// Should still have row export format files
	for _, exportFile := range []string{".json", ".tsv", ".csv", ".yaml"} {
		if !names[exportFile] {
			t.Errorf("Expected %q export file in projected row dir", exportFile)
		}
	}
}

// TestMount_ColumnsLsDir tests listing available columns via ls .columns/.
func TestMount_ColumnsLsDir(t *testing.T) {
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

	// ls .columns/ should list all table columns
	columnsDir := filepath.Join(mountpoint, "orders", ".columns")
	entries, err := os.ReadDir(columnsDir)
	if err != nil {
		t.Fatalf("Failed to read .columns/: %v", err)
	}

	// orders table has: id, customer_id, status, amount, notes, created_at
	expectedCols := map[string]bool{
		"id": true, "customer_id": true, "status": true,
		"amount": true, "notes": true, "created_at": true,
	}

	for _, e := range entries {
		if !expectedCols[e.Name()] {
			t.Errorf("Unexpected column in .columns/ listing: %q", e.Name())
		}
		delete(expectedCols, e.Name())
	}

	for col := range expectedCols {
		t.Errorf("Missing column in .columns/ listing: %q", col)
	}
}

// TestPipeline_SampleWithUUIDs tests .sample/N with UUID PKs and 54 rows.
// This reproduces a user-reported bug where .sample/5 shows only 4 rows
// when the table has UUID PKs and many rows.
func TestPipeline_SampleWithUUIDs(t *testing.T) {
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

	pool, err := pgxpool.New(ctx, baseConnStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	// Create table with UUID PKs
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.uuid_items (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			name text NOT NULL,
			value int NOT NULL
		)
	`, schemaName))
	if err != nil {
		t.Fatalf("Failed to create uuid_items table: %v", err)
	}

	// Insert 54 rows (matching user scenario)
	for i := 0; i < 54; i++ {
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.uuid_items (name, value) VALUES ($1, $2)
		`, schemaName), fmt.Sprintf("item_%d", i), i)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	for _, n := range []int{1, 2, 3, 4, 5, 6, 10} {
		n := n
		t.Run(fmt.Sprintf("Sample%d", n), func(t *testing.T) {
			dir := filepath.Join(mountpoint, "uuid_items", ".sample", fmt.Sprintf("%d", n))
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("Failed to read .sample/%d/: %v", n, err)
			}

			rowCount := countNonDotEntries(entries)
			if rowCount != n {
				t.Errorf("Expected %d rows from .sample/%d, got %d", n, n, rowCount)
				logEntries(t, entries)
			}
		})
	}

	// Also test via ls command (macOS uses different NFS path than Go's os.ReadDir)
	t.Run("LsSample5", func(t *testing.T) {
		dir := filepath.Join(mountpoint, "uuid_items", ".sample", "5")
		out, err := exec.Command("ls", "-l", dir).Output()
		if err != nil {
			t.Fatalf("ls -l .sample/5 failed: %v", err)
		}
		// ls -l output has a "total" line first, then one line per entry
		// Dotfiles are hidden by default
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		// First line is "total N" - skip it
		rowLines := 0
		for _, line := range lines {
			if !strings.HasPrefix(line, "total ") {
				rowLines++
			}
		}
		if rowLines != 5 {
			t.Errorf("Expected 5 rows from ls -l .sample/5, got %d", rowLines)
			t.Logf("ls output:\n%s", string(out))
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
		LogLevel:                "warn",
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
	// Extract schema name from search_path parameter.
	// search_path may be comma-separated (e.g., "myschema,public") — return only the first.
	if idx := strings.Index(connStr, "search_path="); idx != -1 {
		rest := connStr[idx+len("search_path="):]
		if endIdx := strings.Index(rest, "&"); endIdx != -1 {
			rest = rest[:endIdx]
		}
		// Handle comma-separated search_path (e.g., "myschema,public")
		if commaIdx := strings.Index(rest, ","); commaIdx != -1 {
			return rest[:commaIdx]
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
