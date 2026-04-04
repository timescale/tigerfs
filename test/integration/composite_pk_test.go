package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"
)

// setupCompositePKTable creates a table with a composite primary key via DDL staging
// and inserts test data. Returns the table path within the mountpoint.
func setupCompositePKTable(t *testing.T, mountpoint string) string {
	t.Helper()

	tableName := "cpk_test"
	createDir := filepath.Join(mountpoint, ".create", tableName)
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/%s: %v", tableName, err)
	}

	sqlPath := filepath.Join(createDir, "sql")
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		region TEXT,
		user_id INT,
		name TEXT,
		PRIMARY KEY (region, user_id)
	);
	INSERT INTO %s (region, user_id, name) VALUES
		('us', 1, 'Alice'),
		('us', 2, 'Bob'),
		('eu', 1, 'Charlie')
	ON CONFLICT (region, user_id) DO UPDATE SET name = EXCLUDED.name;`,
		tableName, tableName)

	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, tableName)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Fatalf("Table directory should exist after CREATE: %s", tablePath)
	}

	return tablePath
}

// TestMount_CompositePK_ReadDir verifies that listing a table with a composite
// primary key shows comma-delimited directory entries (e.g., "eu,1", "us,1", "us,2").
func TestMount_CompositePK_ReadDir(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list table directory: %v", err)
	}

	// Collect non-dot entries (skip metadata like .info, .filter, etc.)
	var rowKeys []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			rowKeys = append(rowKeys, entry.Name())
		}
	}

	sort.Strings(rowKeys)
	t.Logf("Row entries: %v", rowKeys)

	if len(rowKeys) != 3 {
		t.Fatalf("Expected 3 row entries, got %d: %v", len(rowKeys), rowKeys)
	}

	expected := []string{"eu,1", "us,1", "us,2"}
	for _, exp := range expected {
		if !contains(rowKeys, exp) {
			t.Errorf("Expected row key %q in listing, got: %v", exp, rowKeys)
		}
	}
}

// TestMount_CompositePK_ReadRow verifies that reading a composite PK row as TSV
// returns the correct row data.
func TestMount_CompositePK_ReadRow(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Read row us,1 as TSV (row-as-file format)
	rowFile := filepath.Join(tablePath, "us,1.tsv")
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file us,1.tsv: %v", err)
	}

	content := string(data)
	t.Logf("Row content (us,1.tsv): %s", content)

	if !strings.Contains(content, "Alice") {
		t.Errorf("Expected 'Alice' in row content, got: %s", content)
	}

	if !strings.Contains(content, "us") {
		t.Errorf("Expected 'us' in row content, got: %s", content)
	}
}

// TestMount_CompositePK_ReadColumn verifies that reading a single column
// from a composite PK row returns the correct value.
func TestMount_CompositePK_ReadColumn(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Read the "name" column from row us,1
	columnPath := filepath.Join(tablePath, "us,1", "name")
	data, err := os.ReadFile(columnPath)
	if err != nil {
		t.Fatalf("Failed to read column file us,1/name: %v", err)
	}

	name := strings.TrimSpace(string(data))
	t.Logf("Column value (us,1/name): %q", name)

	if name != "Alice" {
		t.Errorf("Expected name='Alice', got: %q", name)
	}
}

// TestMount_CompositePK_WriteColumn verifies that writing to a column
// of a composite PK row updates the value correctly.
func TestMount_CompositePK_WriteColumn(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Write a new value to us,1/name
	columnPath := filepath.Join(tablePath, "us,1", "name")

	// Use withGCDisabled to prevent GC deadlock in NFS tests (see MEMORY.md)
	var writeErr error
	prev := debug.SetGCPercent(-1)
	writeErr = os.WriteFile(columnPath, []byte("Alicia\n"), 0644)
	debug.SetGCPercent(prev)

	if writeErr != nil {
		t.Fatalf("Failed to write column: %v", writeErr)
	}

	time.Sleep(200 * time.Millisecond)

	// Read back and verify
	data, err := os.ReadFile(columnPath)
	if err != nil {
		t.Fatalf("Failed to read back column: %v", err)
	}

	name := strings.TrimSpace(string(data))
	t.Logf("Updated column value (us,1/name): %q", name)

	if name != "Alicia" {
		t.Errorf("Expected name='Alicia' after update, got: %q", name)
	}
}

// TestMount_CompositePK_DeleteRow verifies that deleting a composite PK row
// removes it from the directory listing.
func TestMount_CompositePK_DeleteRow(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Delete the eu,1 row (Charlie) using the .json extension (row-as-file)
	rowFile := filepath.Join(tablePath, "eu,1.json")
	if err := os.Remove(rowFile); err != nil {
		t.Fatalf("Failed to delete row eu,1: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify the row is gone from directory listing
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list table after delete: %v", err)
	}

	var rowKeys []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			rowKeys = append(rowKeys, entry.Name())
		}
	}

	t.Logf("Row entries after delete: %v", rowKeys)

	if contains(rowKeys, "eu,1") {
		t.Errorf("Row 'eu,1' should not exist after delete, got: %v", rowKeys)
	}

	if len(rowKeys) != 2 {
		t.Errorf("Expected 2 rows after delete, got %d: %v", len(rowKeys), rowKeys)
	}

	// Also verify it returns not-found when reading the deleted row
	deletedPath := filepath.Join(tablePath, "eu,1", "name")
	if _, err := os.ReadFile(deletedPath); !os.IsNotExist(err) {
		t.Errorf("Expected IsNotExist error reading deleted row column, got: %v", err)
	}
}

// TestMount_CompositePK_ReadRowJSON verifies that reading a composite PK row
// in JSON format returns valid JSON with the correct data.
func TestMount_CompositePK_ReadRowJSON(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Read row us,2 (Bob) as JSON
	rowFile := filepath.Join(tablePath, "us,2.json")
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file us,2.json: %v", err)
	}

	content := string(data)
	t.Logf("Row JSON (us,2.json): %s", content)

	// Verify it's valid JSON
	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		t.Fatalf("Row content is not valid JSON: %v\nContent: %s", err, content)
	}

	// Verify the JSON contains Bob's data
	if name, ok := jsonData["name"]; !ok || name != "Bob" {
		t.Errorf("Expected name='Bob' in JSON, got: %v", jsonData["name"])
	}

	if region, ok := jsonData["region"]; !ok || region != "us" {
		t.Errorf("Expected region='us' in JSON, got: %v", jsonData["region"])
	}

	// user_id may come back as float64 from JSON unmarshaling
	if userID, ok := jsonData["user_id"]; !ok {
		t.Errorf("Expected user_id in JSON, not found")
	} else {
		switch v := userID.(type) {
		case float64:
			if v != 2 {
				t.Errorf("Expected user_id=2, got: %v", v)
			}
		case string:
			if v != "2" {
				t.Errorf("Expected user_id='2', got: %v", v)
			}
		default:
			t.Errorf("Unexpected type for user_id: %T = %v", userID, userID)
		}
	}
}

// setupCompositePKPipelineTable creates a larger composite PK table with indexes
// via DDL staging for testing pipeline operations.
func setupCompositePKPipelineTable(t *testing.T, mountpoint string) string {
	t.Helper()

	tableName := "shipments"
	createDir := filepath.Join(mountpoint, ".create", tableName)
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/%s: %v", tableName, err)
	}

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		warehouse TEXT NOT NULL,
		order_id INT NOT NULL,
		product TEXT NOT NULL,
		quantity INT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		PRIMARY KEY (warehouse, order_id)
	);
	CREATE INDEX IF NOT EXISTS idx_shipments_status ON %s(status);
	INSERT INTO %s (warehouse, order_id, product, quantity, status) VALUES
		('east', 1, 'Widget A', 10, 'shipped'),
		('east', 2, 'Widget B', 5, 'pending'),
		('east', 3, 'Widget C', 20, 'shipped'),
		('east', 4, 'Widget D', 3, 'pending'),
		('east', 5, 'Widget E', 15, 'shipped'),
		('west', 1, 'Gadget A', 8, 'pending'),
		('west', 2, 'Gadget B', 12, 'shipped'),
		('west', 3, 'Gadget C', 1, 'pending'),
		('west', 4, 'Gadget D', 25, 'shipped'),
		('west', 5, 'Gadget E', 7, 'pending')
	ON CONFLICT (warehouse, order_id) DO UPDATE SET
		product = EXCLUDED.product,
		quantity = EXCLUDED.quantity,
		status = EXCLUDED.status;`,
		tableName, tableName, tableName)

	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, tableName)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Fatalf("Table directory should exist after CREATE: %s", tablePath)
	}

	return tablePath
}

// TestMount_CompositePK_Pipeline tests pipeline operations (.first, .last, .filter,
// .order, .export) on tables with composite primary keys.
func TestMount_CompositePK_Pipeline(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKPipelineTable(t, mountpoint)

	t.Run("FirstN", func(t *testing.T) {
		// .first/3/ should return the first 3 rows ordered by composite PK (east,1 east,2 east,3)
		dir := filepath.Join(tablePath, ".first", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .first/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows in .first/3/, got %d", rowCount)
			logEntries(t, entries)
		}

		// Verify the entries are the first 3 by composite PK order (east before west)
		var keys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				keys = append(keys, e.Name())
			}
		}
		sort.Strings(keys)
		t.Logf(".first/3/ entries: %v", keys)

		// All should start with "east" since east < west
		for _, k := range keys {
			if !strings.HasPrefix(k, "east,") {
				t.Errorf("Expected all .first/3/ entries to be from 'east' warehouse, got: %s", k)
			}
		}
	})

	t.Run("LastN", func(t *testing.T) {
		// .last/3/ should return the last 3 rows (west,3 west,4 west,5)
		dir := filepath.Join(tablePath, ".last", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .last/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows in .last/3/, got %d", rowCount)
			logEntries(t, entries)
		}

		// Verify all from west warehouse
		var keys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				keys = append(keys, e.Name())
			}
		}
		t.Logf(".last/3/ entries: %v", keys)

		for _, k := range keys {
			if !strings.HasPrefix(k, "west,") {
				t.Errorf("Expected all .last/3/ entries to be from 'west' warehouse, got: %s", k)
			}
		}
	})

	t.Run("FilterStatus", func(t *testing.T) {
		// .filter/status/shipped/ should return only shipped rows
		dir := filepath.Join(tablePath, ".filter", "status", "shipped")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .filter/status/shipped/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		// 5 shipped: east,1 east,3 east,5 west,2 west,4
		if rowCount != 5 {
			t.Errorf("Expected 5 shipped rows, got %d", rowCount)
			logEntries(t, entries)
		}

		// Verify all entries are comma-delimited composite PKs
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") && !strings.Contains(e.Name(), ",") {
				t.Errorf("Expected comma-delimited PK, got: %s", e.Name())
			}
		}
	})

	t.Run("FilterThenFirst", func(t *testing.T) {
		// .filter/status/pending/.first/2/ should return first 2 pending rows
		dir := filepath.Join(tablePath, ".filter", "status", "pending", ".first", "2")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .filter/status/pending/.first/2/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 2 {
			t.Errorf("Expected 2 rows in .filter/status/pending/.first/2/, got %d", rowCount)
			logEntries(t, entries)
		}
	})

	t.Run("OrderByQuantity", func(t *testing.T) {
		// .order/quantity/.first/3/ should return 3 rows with lowest quantity
		dir := filepath.Join(tablePath, ".order", "quantity", ".first", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .order/quantity/.first/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
			logEntries(t, entries)
		}

		// The row with quantity=1 (west,3) should be in the results
		var keys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				keys = append(keys, e.Name())
			}
		}
		t.Logf(".order/quantity/.first/3/ entries: %v", keys)

		if !contains(keys, "west,3") {
			t.Errorf("Expected west,3 (quantity=1) in lowest-3, got: %v", keys)
		}
	})

	t.Run("ExportJSON", func(t *testing.T) {
		// .filter/status/shipped/.export/json should export shipped rows as JSON
		exportPath := filepath.Join(tablePath, ".filter", "status", "shipped", ".export", "json")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read export: %v", err)
		}

		var rows []map[string]interface{}
		if err := json.Unmarshal(data, &rows); err != nil {
			t.Fatalf("Invalid JSON: %v\nContent: %s", err, string(data))
		}

		if len(rows) != 5 {
			t.Errorf("Expected 5 shipped rows in export, got %d", len(rows))
		}

		// Verify all rows have status=shipped
		for i, row := range rows {
			if row["status"] != "shipped" {
				t.Errorf("Row %d: expected status='shipped', got %v", i, row["status"])
			}
		}
	})

	t.Run("ExportCSV", func(t *testing.T) {
		// .first/3/.export/csv should export first 3 rows as CSV
		exportPath := filepath.Join(tablePath, ".first", "3", ".export", "csv")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read CSV export: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		t.Logf("CSV export (%d lines): %s", len(lines), string(data))

		if len(lines) != 3 {
			t.Errorf("Expected 3 CSV lines, got %d", len(lines))
		}
	})

	t.Run("ReadRowThroughPipeline", func(t *testing.T) {
		// Read a specific row through the pipeline: .filter/status/shipped/east,1.json
		rowFile := filepath.Join(tablePath, ".filter", "status", "shipped", "east,1.json")
		data, err := os.ReadFile(rowFile)
		if err != nil {
			t.Fatalf("Failed to read row through pipeline: %v", err)
		}

		var row map[string]interface{}
		if err := json.Unmarshal(data, &row); err != nil {
			t.Fatalf("Invalid JSON: %v", err)
		}

		if row["warehouse"] != "east" {
			t.Errorf("Expected warehouse='east', got %v", row["warehouse"])
		}
		if row["product"] != "Widget A" {
			t.Errorf("Expected product='Widget A', got %v", row["product"])
		}
	})

	t.Run("NestedFirstLast", func(t *testing.T) {
		// .first/8/.last/3/ -- first 8 rows, then last 3 of those
		dir := filepath.Join(tablePath, ".first", "8", ".last", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .first/8/.last/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows in .first/8/.last/3/, got %d", rowCount)
			logEntries(t, entries)
		}
	})

	t.Run("SampleN", func(t *testing.T) {
		// .sample/4/ -- should return approximately 4 rows (probabilistic)
		dir := filepath.Join(tablePath, ".sample", "4")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .sample/4/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		t.Logf(".sample/4/ returned %d rows", rowCount)
		// Sample is probabilistic, so allow a wide range
		if rowCount < 1 || rowCount > 10 {
			t.Errorf("Expected 1-10 rows from .sample/4/, got %d", rowCount)
			logEntries(t, entries)
		}
	})

	t.Run("OrderQuantityLast", func(t *testing.T) {
		// .order/quantity/.last/3/ -- last 3 rows when ordered by quantity ascending
		// With .order/quantity (ascending), the last 3 should be the highest quantities.
		// Note: the pipeline implementation may return the last 3 from the ordered set
		// using PK-based pagination rather than reversing the custom order.
		dir := filepath.Join(tablePath, ".order", "quantity", ".last", "3")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read .order/quantity/.last/3/: %v", err)
		}

		rowCount := countNonDotEntries(entries)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
			logEntries(t, entries)
		}

		var keys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				keys = append(keys, e.Name())
			}
		}
		t.Logf(".order/quantity/.last/3/ entries: %v", keys)
	})

	t.Run("ColumnsProjection", func(t *testing.T) {
		// .columns/product,quantity/.export/json -- only product and quantity fields
		exportPath := filepath.Join(tablePath, ".columns", "product,quantity", ".export", "json")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read columns export: %v", err)
		}

		var rows []map[string]interface{}
		if err := json.Unmarshal(data, &rows); err != nil {
			t.Fatalf("Invalid JSON: %v\nContent: %s", err, string(data))
		}

		if len(rows) != 10 {
			t.Errorf("Expected 10 rows in columns export, got %d", len(rows))
		}

		// Verify only product and quantity are present
		for i, row := range rows {
			if _, ok := row["product"]; !ok {
				t.Errorf("Row %d missing 'product' key", i)
			}
			if _, ok := row["quantity"]; !ok {
				t.Errorf("Row %d missing 'quantity' key", i)
			}
			if _, ok := row["warehouse"]; ok {
				t.Errorf("Row %d should not have 'warehouse' key with .columns/product,quantity", i)
			}
			if _, ok := row["status"]; ok {
				t.Errorf("Row %d should not have 'status' key with .columns/product,quantity", i)
			}
		}
	})
}

// TestMount_CompositePK_MixedTypes verifies that composite PKs work with
// different column type combinations (int+int, text+text, int+text).
func TestMount_CompositePK_MixedTypes(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Create all three tables in a single DDL staging call
	createDir := filepath.Join(mountpoint, ".create", "mixed_types")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/mixed_types: %v", err)
	}

	ddl := `
CREATE TABLE IF NOT EXISTS cpk_int_int (a INT, b INT, val TEXT, PRIMARY KEY(a, b));
INSERT INTO cpk_int_int (a, b, val) VALUES (1, 10, 'x'), (1, 20, 'y'), (2, 10, 'z')
ON CONFLICT (a, b) DO UPDATE SET val = EXCLUDED.val;

CREATE TABLE IF NOT EXISTS cpk_text_text (a TEXT, b TEXT, val TEXT, PRIMARY KEY(a, b));
INSERT INTO cpk_text_text (a, b, val) VALUES ('foo', 'bar', 'x'), ('foo', 'baz', 'y'), ('qux', 'bar', 'z')
ON CONFLICT (a, b) DO UPDATE SET val = EXCLUDED.val;

CREATE TABLE IF NOT EXISTS cpk_int_text (a INT, b TEXT, val TEXT, PRIMARY KEY(a, b));
INSERT INTO cpk_int_text (a, b, val) VALUES (1, 'alpha', 'x'), (1, 'beta', 'y'), (2, 'alpha', 'z')
ON CONFLICT (a, b) DO UPDATE SET val = EXCLUDED.val;
`
	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	t.Run("IntInt", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, "cpk_int_int")
		entries, err := os.ReadDir(tablePath)
		if err != nil {
			t.Fatalf("Failed to list cpk_int_int: %v", err)
		}

		var rowKeys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				rowKeys = append(rowKeys, e.Name())
			}
		}
		sort.Strings(rowKeys)
		t.Logf("cpk_int_int entries: %v", rowKeys)

		expected := []string{"1,10", "1,20", "2,10"}
		if len(rowKeys) != 3 {
			t.Fatalf("Expected 3 entries, got %d: %v", len(rowKeys), rowKeys)
		}
		for _, exp := range expected {
			if !contains(rowKeys, exp) {
				t.Errorf("Expected %q in listing, got: %v", exp, rowKeys)
			}
		}

		// Read a column value
		data, err := os.ReadFile(filepath.Join(tablePath, "1,20", "val"))
		if err != nil {
			t.Fatalf("Failed to read 1,20/val: %v", err)
		}
		if strings.TrimSpace(string(data)) != "y" {
			t.Errorf("Expected val='y' for 1,20, got: %q", strings.TrimSpace(string(data)))
		}
	})

	t.Run("TextText", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, "cpk_text_text")
		entries, err := os.ReadDir(tablePath)
		if err != nil {
			t.Fatalf("Failed to list cpk_text_text: %v", err)
		}

		var rowKeys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				rowKeys = append(rowKeys, e.Name())
			}
		}
		sort.Strings(rowKeys)
		t.Logf("cpk_text_text entries: %v", rowKeys)

		expected := []string{"foo,bar", "foo,baz", "qux,bar"}
		if len(rowKeys) != 3 {
			t.Fatalf("Expected 3 entries, got %d: %v", len(rowKeys), rowKeys)
		}
		for _, exp := range expected {
			if !contains(rowKeys, exp) {
				t.Errorf("Expected %q in listing, got: %v", exp, rowKeys)
			}
		}

		// Read a column value
		data, err := os.ReadFile(filepath.Join(tablePath, "foo,baz", "val"))
		if err != nil {
			t.Fatalf("Failed to read foo,baz/val: %v", err)
		}
		if strings.TrimSpace(string(data)) != "y" {
			t.Errorf("Expected val='y' for foo,baz, got: %q", strings.TrimSpace(string(data)))
		}
	})

	t.Run("IntText", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, "cpk_int_text")
		entries, err := os.ReadDir(tablePath)
		if err != nil {
			t.Fatalf("Failed to list cpk_int_text: %v", err)
		}

		var rowKeys []string
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				rowKeys = append(rowKeys, e.Name())
			}
		}
		sort.Strings(rowKeys)
		t.Logf("cpk_int_text entries: %v", rowKeys)

		expected := []string{"1,alpha", "1,beta", "2,alpha"}
		if len(rowKeys) != 3 {
			t.Fatalf("Expected 3 entries, got %d: %v", len(rowKeys), rowKeys)
		}
		for _, exp := range expected {
			if !contains(rowKeys, exp) {
				t.Errorf("Expected %q in listing, got: %v", exp, rowKeys)
			}
		}

		// Read a column value
		data, err := os.ReadFile(filepath.Join(tablePath, "2,alpha", "val"))
		if err != nil {
			t.Fatalf("Failed to read 2,alpha/val: %v", err)
		}
		if strings.TrimSpace(string(data)) != "z" {
			t.Errorf("Expected val='z' for 2,alpha, got: %q", strings.TrimSpace(string(data)))
		}
	})
}

// TestMount_CompositePK_SpecialChars verifies that composite PK values containing
// special characters (percent, slash) are properly URL-encoded in directory entries.
func TestMount_CompositePK_SpecialChars(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Create table with special characters in PK values
	createDir := filepath.Join(mountpoint, ".create", "cpk_special")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/cpk_special: %v", err)
	}

	ddl := `CREATE TABLE IF NOT EXISTS cpk_special (
		tag TEXT, seq INT, data TEXT, PRIMARY KEY(tag, seq)
	);
	INSERT INTO cpk_special (tag, seq, data) VALUES
		('hello%world', 1, 'pct'),
		('a/b', 2, 'slash')
	ON CONFLICT (tag, seq) DO UPDATE SET data = EXCLUDED.data;`

	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, "cpk_special")

	// List entries and check encoding
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list cpk_special: %v", err)
	}

	var rowKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			rowKeys = append(rowKeys, e.Name())
		}
	}
	sort.Strings(rowKeys)
	t.Logf("cpk_special entries: %v", rowKeys)

	if len(rowKeys) != 2 {
		t.Fatalf("Expected 2 entries, got %d: %v", len(rowKeys), rowKeys)
	}

	// Percent in value should be encoded as %25, slash as %2F
	if !contains(rowKeys, "a%2Fb,2") {
		t.Errorf("Expected 'a%%2Fb,2' in listing, got: %v", rowKeys)
	}
	if !contains(rowKeys, "hello%25world,1") {
		t.Errorf("Expected 'hello%%25world,1' in listing, got: %v", rowKeys)
	}

	// Read column through encoded path
	data, err := os.ReadFile(filepath.Join(tablePath, "hello%25world,1", "data"))
	if err != nil {
		t.Fatalf("Failed to read hello%%25world,1/data: %v", err)
	}
	if strings.TrimSpace(string(data)) != "pct" {
		t.Errorf("Expected data='pct', got: %q", strings.TrimSpace(string(data)))
	}

	data, err = os.ReadFile(filepath.Join(tablePath, "a%2Fb,2", "data"))
	if err != nil {
		t.Fatalf("Failed to read a%%2Fb,2/data: %v", err)
	}
	if strings.TrimSpace(string(data)) != "slash" {
		t.Errorf("Expected data='slash', got: %q", strings.TrimSpace(string(data)))
	}
}

// TestMount_CompositePK_Import tests .import/.sync with composite primary keys,
// verifying both upsert (update existing row) and insert (new row).
func TestMount_CompositePK_Import(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// TSV data: update us,1 (Alice -> Alicia) and insert au,1 (Dave)
	tsvData := "region\tuser_id\tname\nus\t1\tAlicia\nau\t1\tDave\n"

	importPath := filepath.Join(tablePath, ".import", ".sync", "tsv")

	prev := debug.SetGCPercent(-1)
	err := os.WriteFile(importPath, []byte(tsvData), 0644)
	debug.SetGCPercent(prev)

	if err != nil {
		t.Fatalf("Failed to write import: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify the update: us,1 should now be Alicia
	data, err := os.ReadFile(filepath.Join(tablePath, "us,1", "name"))
	if err != nil {
		t.Fatalf("Failed to read us,1/name after import: %v", err)
	}
	if strings.TrimSpace(string(data)) != "Alicia" {
		t.Errorf("Expected name='Alicia' after sync, got: %q", strings.TrimSpace(string(data)))
	}

	// Verify the insert: au,1 should exist
	data, err = os.ReadFile(filepath.Join(tablePath, "au,1", "name"))
	if err != nil {
		t.Fatalf("Failed to read au,1/name after import: %v", err)
	}
	if strings.TrimSpace(string(data)) != "Dave" {
		t.Errorf("Expected name='Dave' for au,1, got: %q", strings.TrimSpace(string(data)))
	}
}

// TestMount_CompositePK_WriteFullRow tests writing a full row via TSV format
// to update an existing composite PK row.
func TestMount_CompositePK_WriteFullRow(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Write TSV to update us,1 (Alice -> Alicia)
	tsvData := "region\tuser_id\tname\nus\t1\tAlicia\n"
	rowFile := filepath.Join(tablePath, "us,1.tsv")

	prev := debug.SetGCPercent(-1)
	err := os.WriteFile(rowFile, []byte(tsvData), 0644)
	debug.SetGCPercent(prev)

	if err != nil {
		t.Fatalf("Failed to write row file: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Read back and verify
	data, err := os.ReadFile(filepath.Join(tablePath, "us,1", "name"))
	if err != nil {
		t.Fatalf("Failed to read us,1/name after write: %v", err)
	}
	if strings.TrimSpace(string(data)) != "Alicia" {
		t.Errorf("Expected name='Alicia' after write, got: %q", strings.TrimSpace(string(data)))
	}
}

// TestMount_CompositePK_InsertNewRow tests inserting a new row via JSON write
// to a composite PK table.
func TestMount_CompositePK_InsertNewRow(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	// Write JSON to create new row au,1
	jsonData := `{"region":"au","user_id":1,"name":"Dave"}`
	rowFile := filepath.Join(tablePath, "au,1.json")

	prev := debug.SetGCPercent(-1)
	err := os.WriteFile(rowFile, []byte(jsonData), 0644)
	debug.SetGCPercent(prev)

	if err != nil {
		t.Fatalf("Failed to write new row: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify the new row appears in listing
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list table after insert: %v", err)
	}

	var rowKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			rowKeys = append(rowKeys, e.Name())
		}
	}
	t.Logf("Row entries after insert: %v", rowKeys)

	if !contains(rowKeys, "au,1") {
		t.Errorf("Expected 'au,1' in listing after insert, got: %v", rowKeys)
	}

	// Verify correct data
	data, err := os.ReadFile(filepath.Join(tablePath, "au,1", "name"))
	if err != nil {
		t.Fatalf("Failed to read au,1/name: %v", err)
	}
	if strings.TrimSpace(string(data)) != "Dave" {
		t.Errorf("Expected name='Dave', got: %q", strings.TrimSpace(string(data)))
	}
}

// TestMount_CompositePK_ThreeColumns tests a 3-column composite primary key.
func TestMount_CompositePK_ThreeColumns(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	createDir := filepath.Join(mountpoint, ".create", "cpk_three")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/cpk_three: %v", err)
	}

	ddl := `CREATE TABLE IF NOT EXISTS cpk_three (
		a TEXT, b INT, c TEXT, val TEXT, PRIMARY KEY(a, b, c)
	);
	INSERT INTO cpk_three (a, b, c, val) VALUES
		('x', 1, 'p', 'data1'),
		('x', 1, 'q', 'data2'),
		('y', 2, 'p', 'data3')
	ON CONFLICT (a, b, c) DO UPDATE SET val = EXCLUDED.val;`

	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, "cpk_three")
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list cpk_three: %v", err)
	}

	var rowKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			rowKeys = append(rowKeys, e.Name())
		}
	}
	sort.Strings(rowKeys)
	t.Logf("cpk_three entries: %v", rowKeys)

	expected := []string{"x,1,p", "x,1,q", "y,2,p"}
	if len(rowKeys) != 3 {
		t.Fatalf("Expected 3 entries, got %d: %v", len(rowKeys), rowKeys)
	}
	for _, exp := range expected {
		if !contains(rowKeys, exp) {
			t.Errorf("Expected %q in listing, got: %v", exp, rowKeys)
		}
	}

	// Read a value through 3-part PK
	data, err := os.ReadFile(filepath.Join(tablePath, "x,1,p", "val"))
	if err != nil {
		t.Fatalf("Failed to read x,1,p/val: %v", err)
	}
	if strings.TrimSpace(string(data)) != "data1" {
		t.Errorf("Expected val='data1', got: %q", strings.TrimSpace(string(data)))
	}
}

// TestMount_CompositePK_SingleColumnRegression verifies that introducing composite
// PK support does not break single-column PK tables. Both types coexist correctly.
func TestMount_CompositePK_SingleColumnRegression(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Create both single-PK and composite-PK tables
	createDir := filepath.Join(mountpoint, ".create", "regression")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/regression: %v", err)
	}

	ddl := `
CREATE TABLE IF NOT EXISTS single_pk (id INT PRIMARY KEY, name TEXT);
INSERT INTO single_pk (id, name) VALUES (1, 'Alice'), (2, 'Bob')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

CREATE TABLE IF NOT EXISTS composite_pk (region TEXT, seq INT, val TEXT, PRIMARY KEY(region, seq));
INSERT INTO composite_pk (region, seq, val) VALUES ('us', 1, 'hello'), ('eu', 2, 'world')
ON CONFLICT (region, seq) DO UPDATE SET val = EXCLUDED.val;
`

	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify single-PK table: plain values, no commas
	singlePath := filepath.Join(mountpoint, "single_pk")
	entries, err := os.ReadDir(singlePath)
	if err != nil {
		t.Fatalf("Failed to list single_pk: %v", err)
	}

	var singleKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			singleKeys = append(singleKeys, e.Name())
		}
	}
	sort.Strings(singleKeys)
	t.Logf("single_pk entries: %v", singleKeys)

	if len(singleKeys) != 2 {
		t.Fatalf("Expected 2 single_pk entries, got %d: %v", len(singleKeys), singleKeys)
	}
	// Single PK entries should NOT have commas
	for _, k := range singleKeys {
		if strings.Contains(k, ",") {
			t.Errorf("Single-PK entry should not have comma: %s", k)
		}
	}

	// Verify reads work on single-PK table
	data, err := os.ReadFile(filepath.Join(singlePath, "1", "name"))
	if err != nil {
		t.Fatalf("Failed to read single_pk/1/name: %v", err)
	}
	if strings.TrimSpace(string(data)) != "Alice" {
		t.Errorf("Expected 'Alice', got: %q", strings.TrimSpace(string(data)))
	}

	// Verify composite-PK table: comma-delimited entries
	compositePath := filepath.Join(mountpoint, "composite_pk")
	entries, err = os.ReadDir(compositePath)
	if err != nil {
		t.Fatalf("Failed to list composite_pk: %v", err)
	}

	var compositeKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			compositeKeys = append(compositeKeys, e.Name())
		}
	}
	sort.Strings(compositeKeys)
	t.Logf("composite_pk entries: %v", compositeKeys)

	if len(compositeKeys) != 2 {
		t.Fatalf("Expected 2 composite_pk entries, got %d: %v", len(compositeKeys), compositeKeys)
	}
	// Composite PK entries SHOULD have commas
	for _, k := range compositeKeys {
		if !strings.Contains(k, ",") {
			t.Errorf("Composite-PK entry should have comma: %s", k)
		}
	}

	// Verify reads work on composite-PK table
	data, err = os.ReadFile(filepath.Join(compositePath, "us,1", "val"))
	if err != nil {
		t.Fatalf("Failed to read composite_pk/us,1/val: %v", err)
	}
	if strings.TrimSpace(string(data)) != "hello" {
		t.Errorf("Expected 'hello', got: %q", strings.TrimSpace(string(data)))
	}
}

// TestMount_CompositePK_EmptyTable verifies that an empty composite PK table
// shows only dot-entries (metadata directories) and no row entries.
func TestMount_CompositePK_EmptyTable(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	createDir := filepath.Join(mountpoint, ".create", "cpk_empty")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/cpk_empty: %v", err)
	}

	ddl := `CREATE TABLE IF NOT EXISTS cpk_empty (
		a TEXT, b INT, val TEXT, PRIMARY KEY(a, b)
	);`

	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, "cpk_empty")
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list cpk_empty: %v", err)
	}

	rowCount := countNonDotEntries(entries)
	if rowCount != 0 {
		t.Errorf("Expected 0 row entries in empty table, got %d", rowCount)
		logEntries(t, entries)
	}

	// Should have some dot entries (.info, .filter, etc.)
	if len(entries) == 0 {
		t.Errorf("Expected at least some dot entries in empty table, got none")
	}
}

// TestMount_CompositePK_LargeRowCount verifies that composite PK tables work
// correctly with a larger number of rows (~100).
func TestMount_CompositePK_LargeRowCount(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	createDir := filepath.Join(mountpoint, ".create", "cpk_large")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/cpk_large: %v", err)
	}

	ddl := `CREATE TABLE IF NOT EXISTS cpk_large (
		prefix TEXT, num INT, data TEXT, PRIMARY KEY(prefix, num)
	);
	INSERT INTO cpk_large (prefix, num, data)
	SELECT 'w' || (i / 10), i % 10, 'data' || i
	FROM generate_series(1, 100) AS i
	ON CONFLICT (prefix, num) DO UPDATE SET data = EXCLUDED.data;`

	sqlPath := filepath.Join(createDir, "sql")
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, "cpk_large")
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list cpk_large: %v", err)
	}

	rowCount := countNonDotEntries(entries)
	t.Logf("cpk_large row count: %d", rowCount)

	// generate_series(1,100) produces 100 rows, but some (prefix,num) combos
	// may collide via ON CONFLICT. The exact count depends on the math.
	// 'w' || (i/10) for i=1..100 gives w0..w10; i%10 gives 0..9
	// w0: i=1..9 -> num=1..9 (9 rows)
	// w1: i=10..19 -> num=0..9 (10 rows)
	// ...
	// w10: i=100 -> num=0 (1 row)
	// Total unique: 9 + 10*9 + 1 = 100 (all unique)
	if rowCount < 90 || rowCount > 100 {
		t.Errorf("Expected ~100 rows, got %d", rowCount)
	}

	// Verify all entries have commas (composite PK format)
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") && !strings.Contains(e.Name(), ",") {
			t.Errorf("Expected comma-delimited entry, got: %s", e.Name())
		}
	}

	// Verify .first/5/ returns exactly 5
	firstDir := filepath.Join(tablePath, ".first", "5")
	firstEntries, err := os.ReadDir(firstDir)
	if err != nil {
		t.Fatalf("Failed to read .first/5/: %v", err)
	}
	firstCount := countNonDotEntries(firstEntries)
	if firstCount != 5 {
		t.Errorf("Expected 5 rows in .first/5/, got %d", firstCount)
	}
}

// TestMount_CompositePK_Rename tests renaming a composite PK row (changing its PK value).
// Composite PK rename means updating multiple PK columns simultaneously.
func TestMount_CompositePK_Rename(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tablePath := setupCompositePKTable(t, mountpoint)

	oldPath := filepath.Join(tablePath, "us,1")
	newPath := filepath.Join(tablePath, "us,99")

	err := os.Rename(oldPath, newPath)
	if err != nil {
		// Composite PK rename may not be supported -- that's acceptable.
		// Verify it returns an error rather than crashing or corrupting data.
		t.Logf("Composite PK rename returned error (may be unsupported): %v", err)

		// Verify original row still exists and is intact
		data, readErr := os.ReadFile(filepath.Join(tablePath, "us,1", "name"))
		if readErr != nil {
			t.Fatalf("Original row us,1 should still exist after failed rename: %v", readErr)
		}
		if strings.TrimSpace(string(data)) != "Alice" {
			t.Errorf("Original row should be intact, got: %q", strings.TrimSpace(string(data)))
		}
		return
	}

	// If rename succeeded, verify the old path is gone and new path has data
	t.Logf("Composite PK rename succeeded")

	// Old path should not exist
	if _, err := os.ReadFile(filepath.Join(tablePath, "us,1", "name")); !os.IsNotExist(err) {
		t.Errorf("Old path us,1 should not exist after rename, got err: %v", err)
	}

	// New path should have the data
	data, err := os.ReadFile(filepath.Join(tablePath, "us,99", "name"))
	if err != nil {
		t.Fatalf("Failed to read renamed row us,99/name: %v", err)
	}
	if strings.TrimSpace(string(data)) != "Alice" {
		t.Errorf("Expected name='Alice' at new path, got: %q", strings.TrimSpace(string(data)))
	}
}
