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
}
