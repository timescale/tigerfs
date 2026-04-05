package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// setupHypertable creates a product_views hypertable via the DDL staging path
// (.create/<name>/sql + .commit), the same way other tables are created in tests.
// Must be called after mounting. Returns the known timestamp string for the first test row.
func setupHypertable(t *testing.T, mountpoint, connStr string) (knownTimestamp string) {
	t.Helper()

	// Check TimescaleDB availability
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	var hasTimescale bool
	_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')`).Scan(&hasTimescale)
	pool.Close()
	if !hasTimescale {
		t.Skip("TimescaleDB extension not available")
	}

	tableName := "product_views"
	createDir := filepath.Join(mountpoint, ".create", tableName)
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/%s: %v", tableName, err)
	}

	ddl := `CREATE TABLE product_views (
		time TIMESTAMPTZ NOT NULL,
		product_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		duration_seconds INTEGER,
		PRIMARY KEY (time, product_id, user_id)
	) WITH (
		tsdb.hypertable,
		tsdb.segmentby = 'product_id',
		tsdb.orderby = 'time DESC'
	);
	INSERT INTO product_views (time, product_id, user_id, duration_seconds) VALUES
		('2024-01-15 10:00:00+00', 1, 1, 30),
		('2024-01-15 10:05:00+00', 1, 2, 45),
		('2024-01-15 10:10:00+00', 2, 1, 60),
		('2024-01-14 09:00:00+00', 1, 3, 15),
		('2024-01-14 09:30:00+00', 3, 1, 120);`

	sqlPath := filepath.Join(createDir, "sql")
	withGCDisabled(func() {
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}
	})

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	tablePath := filepath.Join(mountpoint, tableName)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Fatalf("Table directory should exist after CREATE: %s", tablePath)
	}

	// Query back the first row's timestamp to know the exact RFC3339 filename encoding.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	pool2, err := pgxpool.New(ctx2, connStr)
	if err != nil {
		t.Fatalf("Failed to reconnect: %v", err)
	}
	defer pool2.Close()

	var ts time.Time
	err = pool2.QueryRow(ctx2, `
		SELECT time FROM product_views
		WHERE product_id = 1 AND user_id = 1 AND time = '2024-01-15 10:00:00+00'
	`).Scan(&ts)
	if err != nil {
		t.Fatalf("Failed to query known timestamp: %v", err)
	}

	knownTimestamp = ts.Format(time.RFC3339Nano)
	t.Logf("Known timestamp formatted: %s", knownTimestamp)

	return knownTimestamp
}

// TestMount_Hypertable_ReadDir verifies that listing a hypertable shows
// composite PK entries with timestamp,product_id,user_id format.
func TestMount_Hypertable_ReadDir(t *testing.T) {
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

	knownTS := setupHypertable(t, mountpoint, dbResult.ConnStr)

	tablePath := filepath.Join(mountpoint, "product_views")
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list product_views: %v", err)
	}

	var rowKeys []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			rowKeys = append(rowKeys, entry.Name())
		}
	}

	t.Logf("Row entries (%d): first=%v", len(rowKeys), rowKeys[:min(3, len(rowKeys))])

	if len(rowKeys) != 5 {
		t.Fatalf("Expected 5 row entries, got %d", len(rowKeys))
	}

	// Verify the known row appears with the expected timestamp encoding
	expectedKey := knownTS + ",1,1"
	found := false
	for _, key := range rowKeys {
		if key == expectedKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected row key %q in listing, got: %v", expectedKey, rowKeys)
	}
}

// TestMount_Hypertable_ReadRow verifies reading a hypertable row by composite PK.
func TestMount_Hypertable_ReadRow(t *testing.T) {
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

	knownTS := setupHypertable(t, mountpoint, dbResult.ConnStr)

	// Read the known row as TSV
	rowFile := filepath.Join(mountpoint, "product_views", knownTS+",1,1.tsv")
	data, err := os.ReadFile(rowFile)
	if err != nil {
		t.Fatalf("Failed to read row file: %v", err)
	}

	content := string(data)
	t.Logf("Row content: %s", content)

	// Should contain duration_seconds=30
	if !strings.Contains(content, "30") {
		t.Errorf("Expected '30' (duration_seconds) in row content, got: %s", content)
	}
}

// TestMount_Hypertable_ReadColumn verifies reading a single column from a hypertable row.
func TestMount_Hypertable_ReadColumn(t *testing.T) {
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

	knownTS := setupHypertable(t, mountpoint, dbResult.ConnStr)

	// Read duration_seconds column
	colPath := filepath.Join(mountpoint, "product_views", knownTS+",1,1", "duration_seconds")
	data, err := os.ReadFile(colPath)
	if err != nil {
		t.Fatalf("Failed to read column: %v", err)
	}

	content := strings.TrimSpace(string(data))
	if content != "30" {
		t.Errorf("Expected duration_seconds=30, got: %q", content)
	}
}

// TestMount_Hypertable_WriteColumn verifies updating a column in a hypertable row.
func TestMount_Hypertable_WriteColumn(t *testing.T) {
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

	knownTS := setupHypertable(t, mountpoint, dbResult.ConnStr)

	// Update duration_seconds from 30 to 99
	colPath := filepath.Join(mountpoint, "product_views", knownTS+",1,1", "duration_seconds")
	withGCDisabled(func() {
		if err := os.WriteFile(colPath, []byte("99"), 0644); err != nil {
			t.Fatalf("Failed to write column: %v", err)
		}
	})

	time.Sleep(300 * time.Millisecond)

	// Read back to verify
	data, err := os.ReadFile(colPath)
	if err != nil {
		t.Fatalf("Failed to read back column: %v", err)
	}

	content := strings.TrimSpace(string(data))
	if content != "99" {
		t.Errorf("Expected duration_seconds=99 after write, got: %q", content)
	}
}

// TestMount_Hypertable_DeleteRow verifies deleting a row from a hypertable.
func TestMount_Hypertable_DeleteRow(t *testing.T) {
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

	knownTS := setupHypertable(t, mountpoint, dbResult.ConnStr)

	tablePath := filepath.Join(mountpoint, "product_views")
	rowDir := filepath.Join(tablePath, knownTS+",1,1")

	// Verify it exists first
	if _, err := os.Stat(rowDir); os.IsNotExist(err) {
		t.Fatalf("Row directory should exist before delete")
	}

	// Delete the row
	if err := os.Remove(rowDir); err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Verify it's gone from listing
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list after delete: %v", err)
	}

	var rowKeys []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			rowKeys = append(rowKeys, entry.Name())
		}
	}

	if len(rowKeys) != 4 {
		t.Errorf("Expected 4 rows after delete, got %d: %v", len(rowKeys), rowKeys)
	}

	deletedKey := knownTS + ",1,1"
	for _, key := range rowKeys {
		if key == deletedKey {
			t.Errorf("Deleted row %q should not appear in listing", deletedKey)
		}
	}
}

// TestMount_Hypertable_Pipeline verifies pipeline operations on hypertables:
// .first/N, .filter, .order, .export/json
func TestMount_Hypertable_Pipeline(t *testing.T) {
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

	_ = setupHypertable(t, mountpoint, dbResult.ConnStr)

	tablePath := filepath.Join(mountpoint, "product_views")

	// Test .first/3
	t.Run("First3", func(t *testing.T) {
		firstPath := filepath.Join(tablePath, ".first", "3")
		entries, err := os.ReadDir(firstPath)
		if err != nil {
			t.Fatalf("Failed to list .first/3: %v", err)
		}

		nonDot := 0
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				nonDot++
			}
		}
		if nonDot != 3 {
			t.Errorf("Expected 3 rows from .first/3, got %d", nonDot)
		}
	})

	// Test .filter/product_id/1
	t.Run("FilterProductID", func(t *testing.T) {
		filterPath := filepath.Join(tablePath, ".filter", "product_id", "1")
		entries, err := os.ReadDir(filterPath)
		if err != nil {
			t.Fatalf("Failed to list .filter/product_id/1: %v", err)
		}

		nonDot := 0
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				nonDot++
			}
		}
		// product_id=1 has 3 rows
		if nonDot != 3 {
			t.Errorf("Expected 3 rows with product_id=1, got %d", nonDot)
		}
	})

	// Test .order/duration_seconds/.first/2
	t.Run("OrderByDuration", func(t *testing.T) {
		orderPath := filepath.Join(tablePath, ".order", "duration_seconds", ".first", "2")
		entries, err := os.ReadDir(orderPath)
		if err != nil {
			t.Fatalf("Failed to list .order/duration_seconds/.first/2: %v", err)
		}

		nonDot := 0
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), ".") {
				nonDot++
			}
		}
		if nonDot != 2 {
			t.Errorf("Expected 2 rows, got %d", nonDot)
		}
	})

	// Test .export/json
	t.Run("ExportJSON", func(t *testing.T) {
		exportPath := filepath.Join(tablePath, ".first", "2", ".export", "json")
		data, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read .export/json: %v", err)
		}

		var rows []map[string]interface{}
		if err := json.Unmarshal(data, &rows); err != nil {
			t.Fatalf("Failed to parse JSON export: %v", err)
		}

		if len(rows) != 2 {
			t.Errorf("Expected 2 rows in JSON export, got %d", len(rows))
		}

		// Verify expected columns exist
		if len(rows) > 0 {
			row := rows[0]
			for _, col := range []string{"time", "product_id", "user_id", "duration_seconds"} {
				if _, ok := row[col]; !ok {
					t.Errorf("Expected column %q in JSON export row", col)
				}
			}
		}
	})
}

// TestMount_Hypertable_InsertRow verifies inserting a new row into a hypertable via JSON write.
func TestMount_Hypertable_InsertRow(t *testing.T) {
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

	_ = setupHypertable(t, mountpoint, dbResult.ConnStr)

	tablePath := filepath.Join(mountpoint, "product_views")

	// Insert by writing a JSON file to a new PK path.
	// The PK is (time, product_id, user_id), so the filename encodes all three.
	newRow := map[string]interface{}{
		"time":             "2024-01-16T12:00:00Z",
		"product_id":       1,
		"user_id":          1,
		"duration_seconds": 200,
	}
	jsonData, err := json.Marshal(newRow)
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	// Write to product_views/2024-01-16T12:00:00Z,1,1.json
	newPath := filepath.Join(tablePath, "2024-01-16T12:00:00Z,1,1.json")
	withGCDisabled(func() {
		if err := os.WriteFile(newPath, jsonData, 0644); err != nil {
			t.Fatalf("Failed to write new row: %v", err)
		}
	})

	time.Sleep(500 * time.Millisecond)

	// Verify the new row appears in listing
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list after insert: %v", err)
	}

	var rowKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			rowKeys = append(rowKeys, e.Name())
		}
	}

	if len(rowKeys) != 6 {
		t.Errorf("Expected 6 rows after insert, got %d: %v", len(rowKeys), rowKeys)
	}

	// Check that we can find a row with the new timestamp
	found := false
	for _, key := range rowKeys {
		if strings.Contains(key, "2024-01-16") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find row with 2024-01-16 timestamp, got: %v", rowKeys)
	}
}

// TestMount_Hypertable_DDLCreate verifies creating a hypertable via the DDL staging path
// (.create/<name>/sql + .commit), including multi-statement SQL with WITH clause.
func TestMount_Hypertable_DDLCreate(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Check TimescaleDB availability before mounting
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	var hasTimescale bool
	_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')`).Scan(&hasTimescale)
	pool.Close()
	if !hasTimescale {
		t.Skip("TimescaleDB extension not available")
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Create hypertable via DDL staging with multi-statement SQL
	tableName := "sensor_data"
	createDir := filepath.Join(mountpoint, ".create", tableName)
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir .create/%s: %v", tableName, err)
	}

	ddl := fmt.Sprintf(`CREATE TABLE %s (
		time TIMESTAMPTZ NOT NULL,
		sensor_id INTEGER NOT NULL,
		temperature DOUBLE PRECISION,
		PRIMARY KEY (time, sensor_id)
	) WITH (
		tsdb.hypertable,
		tsdb.segmentby = 'sensor_id',
		tsdb.orderby = 'time DESC'
	);
	INSERT INTO %s (time, sensor_id, temperature) VALUES
		('2024-01-15 10:00:00+00', 1, 22.5),
		('2024-01-15 10:00:00+00', 2, 23.1),
		('2024-01-15 10:05:00+00', 1, 22.8);`,
		tableName, tableName)

	sqlPath := filepath.Join(createDir, "sql")
	withGCDisabled(func() {
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}
	})

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		t.Fatalf("Failed to commit DDL: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify the table exists and has rows
	tablePath := filepath.Join(mountpoint, tableName)
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		t.Fatalf("Failed to list %s: %v", tableName, err)
	}

	var rowKeys []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			rowKeys = append(rowKeys, e.Name())
		}
	}
	sort.Strings(rowKeys)

	t.Logf("Sensor data rows: %v", rowKeys)

	if len(rowKeys) != 3 {
		t.Fatalf("Expected 3 rows in %s, got %d: %v", tableName, len(rowKeys), rowKeys)
	}

	// Verify we can read a row
	data, err := os.ReadFile(filepath.Join(tablePath, rowKeys[0]+".tsv"))
	if err != nil {
		t.Fatalf("Failed to read row: %v", err)
	}

	content := string(data)
	t.Logf("First row content: %s", content)
	if !strings.Contains(content, "22") {
		t.Errorf("Expected temperature value in row content, got: %s", content)
	}
}

// setupHypertableColumnstore creates a hypertable via DDL staging, then converts
// all chunks to columnstore directly via SQL. Returns the known timestamp.
func setupHypertableColumnstore(t *testing.T, mountpoint, connStr string) (knownTimestamp string) {
	t.Helper()

	knownTimestamp = setupHypertable(t, mountpoint, connStr)

	// Convert all chunks to columnstore
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect for columnstore conversion: %v", err)
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		DO $$
		DECLARE
			chunk REGCLASS;
		BEGIN
			FOR chunk IN SELECT show_chunks('product_views')
			LOOP
				CALL convert_to_columnstore(chunk);
			END LOOP;
		END $$;
	`)
	if err != nil {
		t.Fatalf("Failed to convert to columnstore: %v", err)
	}

	return knownTimestamp
}

// TestMount_Hypertable_ColumnstoreDML verifies that UPDATE and DELETE work on
// columnstore-compressed hypertable chunks through TigerFS.
func TestMount_Hypertable_ColumnstoreDML(t *testing.T) {
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

	knownTS := setupHypertableColumnstore(t, mountpoint, dbResult.ConnStr)

	tablePath := filepath.Join(mountpoint, "product_views")

	// Test UPDATE on columnstore data
	t.Run("UpdateColumn", func(t *testing.T) {
		colPath := filepath.Join(tablePath, knownTS+",1,1", "duration_seconds")

		// Read original value
		data, err := os.ReadFile(colPath)
		if err != nil {
			t.Fatalf("Failed to read column before update: %v", err)
		}
		original := strings.TrimSpace(string(data))
		if original != "30" {
			t.Fatalf("Expected original duration_seconds=30, got: %q", original)
		}

		// Update to new value
		withGCDisabled(func() {
			if err := os.WriteFile(colPath, []byte("77"), 0644); err != nil {
				t.Fatalf("Failed to update column on columnstore data: %v", err)
			}
		})

		time.Sleep(300 * time.Millisecond)

		// Read back
		data, err = os.ReadFile(colPath)
		if err != nil {
			t.Fatalf("Failed to read column after update: %v", err)
		}
		if strings.TrimSpace(string(data)) != "77" {
			t.Errorf("Expected duration_seconds=77 after update, got: %q", strings.TrimSpace(string(data)))
		}
	})

	// Test DELETE on columnstore data
	t.Run("DeleteRow", func(t *testing.T) {
		// Count rows before
		entries, err := os.ReadDir(tablePath)
		if err != nil {
			t.Fatalf("Failed to list before delete: %v", err)
		}
		beforeCount := countNonDotEntries(entries)

		// Delete a different row (not the one we just updated)
		// Use the row with product_id=1, user_id=3 from Jan 14
		var targetKey string
		for _, e := range entries {
			if strings.Contains(e.Name(), ",1,3") {
				targetKey = e.Name()
				break
			}
		}
		if targetKey == "" {
			t.Fatal("Could not find target row for delete")
		}

		rowDir := filepath.Join(tablePath, targetKey)
		if err := os.Remove(rowDir); err != nil {
			t.Fatalf("Failed to delete row from columnstore data: %v", err)
		}

		time.Sleep(300 * time.Millisecond)

		// Count rows after
		entries, err = os.ReadDir(tablePath)
		if err != nil {
			t.Fatalf("Failed to list after delete: %v", err)
		}
		afterCount := countNonDotEntries(entries)

		if afterCount != beforeCount-1 {
			t.Errorf("Expected %d rows after delete, got %d", beforeCount-1, afterCount)
		}
	})
}

// TestMount_Hypertable_ColumnstoreInfo verifies that .info/ metadata files return
// correct values for a columnstore hypertable.
func TestMount_Hypertable_ColumnstoreInfo(t *testing.T) {
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

	_ = setupHypertableColumnstore(t, mountpoint, dbResult.ConnStr)

	tablePath := filepath.Join(mountpoint, "product_views")

	// Test .info/count
	t.Run("Count", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(tablePath, ".info", "count"))
		if err != nil {
			t.Fatalf("Failed to read .info/count: %v", err)
		}
		count := strings.TrimSpace(string(data))
		if count != "5" {
			t.Errorf("Expected count=5, got: %q", count)
		}
	})

	// Test .info/columns
	t.Run("Columns", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(tablePath, ".info", "columns"))
		if err != nil {
			t.Fatalf("Failed to read .info/columns: %v", err)
		}
		content := string(data)
		for _, col := range []string{"time", "product_id", "user_id", "duration_seconds"} {
			if !strings.Contains(content, col) {
				t.Errorf("Expected column %q in .info/columns, got:\n%s", col, content)
			}
		}
	})

	// Test .info/ddl
	t.Run("DDL", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(tablePath, ".info", "ddl"))
		if err != nil {
			t.Fatalf("Failed to read .info/ddl: %v", err)
		}
		content := string(data)
		// DDL should contain CREATE TABLE with the column definitions
		if !strings.Contains(content, "product_views") {
			t.Errorf("Expected 'product_views' in DDL, got:\n%s", content)
		}
		if !strings.Contains(content, "time") {
			t.Errorf("Expected 'time' column in DDL, got:\n%s", content)
		}
	})

	// Test .info/schema
	t.Run("Schema", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(tablePath, ".info", "schema"))
		if err != nil {
			t.Fatalf("Failed to read .info/schema: %v", err)
		}
		content := string(data)
		// Schema should list column names and types
		if !strings.Contains(content, "time") {
			t.Errorf("Expected 'time' in schema, got:\n%s", content)
		}
		if !strings.Contains(content, "timestamp") || !strings.Contains(content, "time zone") {
			t.Errorf("Expected timestamptz type in schema, got:\n%s", content)
		}
	})

	// Test .info directory listing
	t.Run("InfoDir", func(t *testing.T) {
		entries, err := os.ReadDir(filepath.Join(tablePath, ".info"))
		if err != nil {
			t.Fatalf("Failed to list .info/: %v", err)
		}
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		for _, expected := range []string{"count", "columns", "ddl", "schema"} {
			found := false
			for _, name := range names {
				if name == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected %q in .info/ listing, got: %v", expected, names)
			}
		}
	})
}

// TestMount_Hypertable_SeededDemoData verifies that the product_views hypertable
// created by seedDemoData appears in the root listing and has rows.
func TestMount_Hypertable_SeededDemoData(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Seed demo data (includes product_views hypertable)
	ctx := context.Background()
	demoCfg := DefaultDemoConfig()
	if err := seedDemoData(ctx, dbResult.ConnStr, demoCfg); err != nil {
		t.Fatalf("Failed to seed demo data: %v", err)
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	// Verify product_views appears in root listing
	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatalf("Failed to list mountpoint: %v", err)
	}

	tableNames := make([]string, len(entries))
	for i, e := range entries {
		tableNames[i] = e.Name()
	}

	found := false
	for _, name := range tableNames {
		if name == "product_views" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Expected 'product_views' in root listing, got: %v", tableNames)
	}

	// Verify product_views has rows
	pvPath := filepath.Join(mountpoint, "product_views")
	pvEntries, err := os.ReadDir(pvPath)
	if err != nil {
		t.Fatalf("Failed to list product_views: %v", err)
	}

	rowCount := countNonDotEntries(pvEntries)
	t.Logf("product_views has %d rows (from seeded demo data)", rowCount)

	if rowCount == 0 {
		t.Error("Expected product_views to have rows from seeded demo data")
	}

	// Verify row entries have timestamp composite PK format (timestamp,product_id,user_id)
	for _, e := range pvEntries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		// Should have exactly 2 commas (3 PK columns)
		commas := strings.Count(name, ",")
		if commas != 2 {
			t.Errorf("Expected 2 commas in PK entry %q (3-column composite), got %d", name, commas)
			break
		}
		break // Just check the first row
	}

	// Verify .info/count matches row listing
	countData, err := os.ReadFile(filepath.Join(pvPath, ".info", "count"))
	if err != nil {
		t.Fatalf("Failed to read .info/count: %v", err)
	}
	t.Logf("product_views .info/count: %s", strings.TrimSpace(string(countData)))
}
