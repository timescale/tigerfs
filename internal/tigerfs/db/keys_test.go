package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestGetPrimaryKey(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create test table with single-column primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_pk_single (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_pk_single")
	}()

	// Test single-column primary key
	pk, err := client.GetPrimaryKey(ctx, "public", "test_pk_single")
	if err != nil {
		t.Fatalf("GetPrimaryKey() failed: %v", err)
	}

	if len(pk.Columns) != 1 {
		t.Errorf("Expected 1 primary key column, got %d", len(pk.Columns))
	}

	if pk.Columns[0] != "id" {
		t.Errorf("Expected primary key column 'id', got '%s'", pk.Columns[0])
	}

	t.Logf("Found primary key: %v", pk.Columns)
}

func TestGetPrimaryKey_Composite(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create test table with composite primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_pk_composite (
			user_id int,
			order_id int,
			PRIMARY KEY (user_id, order_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_pk_composite")
	}()

	// Test composite primary key (should succeed)
	pk, err := client.GetPrimaryKey(ctx, "public", "test_pk_composite")
	if err != nil {
		t.Fatalf("GetPrimaryKey() failed for composite key: %v", err)
	}

	if len(pk.Columns) != 2 {
		t.Errorf("Expected 2 primary key columns, got %d", len(pk.Columns))
	}

	if pk.Columns[0] != "user_id" || pk.Columns[1] != "order_id" {
		t.Errorf("Expected columns [user_id, order_id], got %v", pk.Columns)
	}

	t.Logf("Found composite primary key: %v", pk.Columns)
}

func TestGetPrimaryKey_NoPrimaryKey(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create test table without primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_no_pk (
			id int,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_no_pk")
	}()

	// Test table without primary key (should return error)
	_, err = client.GetPrimaryKey(ctx, "public", "test_no_pk")
	if err == nil {
		t.Fatal("Expected error for table without primary key, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestListRows(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create test table and insert data
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_list_rows (
			id serial PRIMARY KEY,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_list_rows (email) VALUES
		('user1@example.com'),
		('user2@example.com'),
		('user3@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// List rows with limit
	rows, err := client.ListRows(ctx, "public", "test_list_rows", []string{"id"}, 10)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Check row values are strings
	for i, row := range rows {
		t.Logf("Row %d: %s", i, row)
		if row == "" {
			t.Errorf("Row %d has empty PK value", i)
		}
	}
}

func TestListRows_WithLimit(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create test table and insert data
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_list_rows_limit (
			id serial PRIMARY KEY,
			data text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows_limit")
	}()

	// Insert 5 rows
	for i := 1; i <= 5; i++ {
		_, err = client.pool.Exec(ctx, `INSERT INTO test_list_rows_limit (data) VALUES ($1)`, fmt.Sprintf("data%d", i))
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// List with limit=2
	rows, err := client.ListRows(ctx, "public", "test_list_rows_limit", []string{"id"}, 2)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	// Should only return 2 rows
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows (limit), got %d", len(rows))
	}

	t.Logf("Found %d rows with limit=2: %v", len(rows), rows)
}

func TestListRows_EmptyTable(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create empty test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_list_rows_empty (
			id serial PRIMARY KEY
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows_empty")
	}()

	// List rows from empty table
	rows, err := client.ListRows(ctx, "public", "test_list_rows_empty", []string{"id"}, 10)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	// Should return empty list, not error
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for empty table, got %d", len(rows))
	}
}

func TestClient_GetPrimaryKey_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetPrimaryKey(ctx, "public", "test_table")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

func TestListRows_CompositePK(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create table with composite primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_list_rows_composite (
			region text,
			user_id int,
			name text,
			PRIMARY KEY (region, user_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows_composite")
	}()

	// Insert rows in non-sorted order
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_list_rows_composite (region, user_id, name) VALUES
		('us', 2, 'Bob'),
		('eu', 1, 'Alice'),
		('us', 1, 'Charlie')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// List rows with composite PK columns
	rows, err := client.ListRows(ctx, "public", "test_list_rows_composite", []string{"region", "user_id"}, 10)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows))
	}

	// Should be ordered by region ASC, user_id ASC and comma-delimited
	expected := []string{"eu,1", "us,1", "us,2"}
	for i, want := range expected {
		if rows[i] != want {
			t.Errorf("Row %d = %q, want %q", i, rows[i], want)
		}
	}

	t.Logf("Composite PK rows: %v", rows)
}

// TestListRows_TimestampPK verifies that hypertable-style composite PKs with
// TIMESTAMPTZ columns work correctly through ListRows and GetRow.
// pgx must handle string-to-timestamptz casting in WHERE clauses.
func TestListRows_TimestampPK(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create table with timestamptz in composite PK (like a hypertable)
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_timestamp_pk (
			time TIMESTAMPTZ NOT NULL,
			device_id INTEGER NOT NULL,
			value DOUBLE PRECISION,
			PRIMARY KEY (time, device_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_timestamp_pk")
	}()

	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_timestamp_pk (time, device_id, value) VALUES
			('2024-01-15 10:00:00+00', 1, 22.5),
			('2024-01-15 10:00:00+00', 2, 23.1),
			('2024-01-15 10:05:00+00', 1, 22.8)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test ListRows returns properly encoded timestamp,device_id pairs
	rows, err := client.ListRows(ctx, "public", "test_timestamp_pk", []string{"time", "device_id"}, 10)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d: %v", len(rows), rows)
	}

	t.Logf("Timestamp PK rows: %v", rows)

	// Each row should contain a timestamp and device_id separated by comma
	for _, row := range rows {
		if !strings.Contains(row, ",") {
			t.Errorf("Row %q should be comma-delimited composite PK", row)
		}
	}

	// Test GetRow with a timestamp-based PKMatch
	// Decode the first listed PK, then use it to fetch the row
	pk := &PrimaryKey{Columns: []string{"time", "device_id"}}
	match, err := pk.Decode(rows[0])
	if err != nil {
		t.Fatalf("Failed to decode PK %q: %v", rows[0], err)
	}

	row, err := GetRow(ctx, client.pool, "public", "test_timestamp_pk", match)
	if err != nil {
		t.Fatalf("GetRow() with timestamp PKMatch failed: %v", err)
	}

	if row == nil {
		t.Fatal("GetRow() returned nil row")
	}

	t.Logf("GetRow columns: %v", row.Columns)
	t.Logf("GetRow values: %v", row.Values)

	// Should have time, device_id, value columns
	if len(row.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d: %v", len(row.Columns), row.Columns)
	}
}

func TestClient_ListRows_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.ListRows(ctx, "public", "test_table", []string{"id"}, 10)
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
