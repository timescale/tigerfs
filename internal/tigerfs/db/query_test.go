package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestIsUniqueViolation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "unique violation",
			err:  &pgconn.PgError{Code: "23505"},
			want: true,
		},
		{
			name: "wrapped unique violation",
			err:  fmt.Errorf("insert failed: %w", &pgconn.PgError{Code: "23505"}),
			want: true,
		},
		{
			name: "foreign key violation",
			err:  &pgconn.PgError{Code: "23503"},
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUniqueViolation(tt.err)
			if got != tt.want {
				t.Errorf("isUniqueViolation(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestGetRow(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_get_row (
			id serial PRIMARY KEY,
			name text,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_row (name, email) VALUES
		('Alice', 'alice@example.com'),
		('Bob', 'bob@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Get row by ID
	row, err := client.GetRow(ctx, "public", "test_get_row", SinglePKMatch("id", "1"))
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	// Verify columns
	if len(row.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(row.Columns))
	}

	expectedColumns := []string{"id", "name", "email"}
	for i, col := range expectedColumns {
		if i >= len(row.Columns) {
			t.Errorf("Missing column %s at index %d", col, i)
			continue
		}
		if row.Columns[i] != col {
			t.Errorf("Expected column %s at index %d, got %s", col, i, row.Columns[i])
		}
	}

	// Verify values
	if len(row.Values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(row.Values))
	}

	t.Logf("Row: columns=%v, values=%v", row.Columns, row.Values)
}

func TestGetRow_NotFound(t *testing.T) {
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

	// Create test table (empty)
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_row_notfound (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_notfound")
	}()

	// Try to get non-existent row
	_, err = client.GetRow(ctx, "public", "test_get_row_notfound", SinglePKMatch("id", "999"))
	if err == nil {
		t.Fatal("Expected error for non-existent row, got nil")
	}

	// Error should mention "row not found"
	t.Logf("Got expected error: %v", err)
}

func TestGetRow_WithNULL(t *testing.T) {
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

	// Create test table with nullable columns
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_row_null (
			id serial PRIMARY KEY,
			name text,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_null")
	}()

	// Insert row with NULL email
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_row_null (name, email) VALUES ('Alice', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Get row
	row, err := client.GetRow(ctx, "public", "test_get_row_null", SinglePKMatch("id", "1"))
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	// Check that email value is nil
	if len(row.Values) < 3 {
		t.Fatalf("Expected at least 3 values, got %d", len(row.Values))
	}

	emailValue := row.Values[2] // email is 3rd column
	if emailValue != nil {
		t.Errorf("Expected NULL email value, got %v", emailValue)
	}

	t.Logf("Row with NULL: columns=%v, values=%v", row.Columns, row.Values)
}

func TestClient_GetRow_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetRow(ctx, "public", "test_table", SinglePKMatch("id", "1"))
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

func TestGetColumn(t *testing.T) {
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

	// Create a test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_column (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Cleanup test table
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_column")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_column (id, name, email, age) VALUES
		(1, 'Alice', 'alice@example.com', 30),
		(2, 'Bob', NULL, 25)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test getting a text column
	value, err := client.GetColumn(ctx, "public", "test_get_column", SinglePKMatch("id", "1"), "name")
	if err != nil {
		t.Fatalf("GetColumn() failed: %v", err)
	}

	if value == nil {
		t.Fatal("Expected non-null value for name column")
	}

	nameStr, ok := value.(string)
	if !ok {
		t.Fatalf("Expected string value, got %T", value)
	}

	if nameStr != "Alice" {
		t.Errorf("Expected name='Alice', got '%s'", nameStr)
	}

	// Test getting an integer column
	value, err = client.GetColumn(ctx, "public", "test_get_column", SinglePKMatch("id", "1"), "age")
	if err != nil {
		t.Fatalf("GetColumn() failed for age: %v", err)
	}

	if value == nil {
		t.Fatal("Expected non-null value for age column")
	}

	// pgx returns int32 for integer columns
	ageInt, ok := value.(int32)
	if !ok {
		t.Fatalf("Expected int32 value, got %T", value)
	}

	if ageInt != 30 {
		t.Errorf("Expected age=30, got %d", ageInt)
	}

	// Test getting a NULL column
	value, err = client.GetColumn(ctx, "public", "test_get_column", SinglePKMatch("id", "2"), "email")
	if err != nil {
		t.Fatalf("GetColumn() failed for NULL email: %v", err)
	}

	if value != nil {
		t.Errorf("Expected NULL value for email, got %v", value)
	}

	t.Logf("GetColumn tests passed")
}

func TestGetColumn_NonExistentRow(t *testing.T) {
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

	// Create a test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_column_nonexistent (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Cleanup test table
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_column_nonexistent")
	}()

	// Test getting column from non-existent row
	_, err = client.GetColumn(ctx, "public", "test_column_nonexistent", SinglePKMatch("id", "999"), "name")
	if err == nil {
		t.Fatal("Expected error for non-existent row, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestGetColumn_NonExistentColumn(t *testing.T) {
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

	// Create a test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_column_invalid (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Cleanup test table
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_column_invalid")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_column_invalid (id, name) VALUES (1, 'Alice')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test getting non-existent column
	_, err = client.GetColumn(ctx, "public", "test_column_invalid", SinglePKMatch("id", "1"), "nonexistent_column")
	if err == nil {
		t.Fatal("Expected error for non-existent column, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestClient_GetColumn_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetColumn(ctx, "public", "test_table", SinglePKMatch("id", "1"), "name")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

func TestUpdateColumn(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_column (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_column")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_column (id, name, email, age) VALUES
		(1, 'Alice', 'alice@example.com', 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update email column
	err = client.UpdateColumn(ctx, "public", "test_update_column", SinglePKMatch("id", "1"), "email", "newemail@example.com")
	if err != nil {
		t.Fatalf("UpdateColumn() failed: %v", err)
	}

	// Verify update
	value, err := client.GetColumn(ctx, "public", "test_update_column", SinglePKMatch("id", "1"), "email")
	if err != nil {
		t.Fatalf("GetColumn() failed: %v", err)
	}

	emailStr, ok := value.(string)
	if !ok {
		t.Fatalf("Expected string value, got %T", value)
	}

	if emailStr != "newemail@example.com" {
		t.Errorf("Expected email='newemail@example.com', got '%s'", emailStr)
	}

	t.Logf("Column updated successfully")
}

func TestUpdateColumn_SetNull(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_null (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_null")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_null (id, name, email) VALUES
		(1, 'Alice', 'alice@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update email to NULL (empty string)
	err = client.UpdateColumn(ctx, "public", "test_update_null", SinglePKMatch("id", "1"), "email", "")
	if err != nil {
		t.Fatalf("UpdateColumn() failed: %v", err)
	}

	// Verify email is now NULL
	value, err := client.GetColumn(ctx, "public", "test_update_null", SinglePKMatch("id", "1"), "email")
	if err != nil {
		t.Fatalf("GetColumn() failed: %v", err)
	}

	if value != nil {
		t.Errorf("Expected NULL value, got %v", value)
	}

	t.Logf("Column set to NULL successfully")
}

func TestUpdateColumn_NonExistentRow(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_notfound (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_notfound")
	}()

	// Try to update non-existent row
	err = client.UpdateColumn(ctx, "public", "test_update_notfound", SinglePKMatch("id", "999"), "name", "New Name")
	if err == nil {
		t.Fatal("Expected error for non-existent row, got nil")
	}

	if err.Error() != "row not found" {
		t.Errorf("Expected 'row not found', got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

func TestClient_UpdateColumn_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	err := client.UpdateColumn(ctx, "public", "test_table", SinglePKMatch("id", "1"), "name", "New Name")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

// ============================================================================
// InsertRow Tests
// ============================================================================

func TestInsertRow_ExplicitPK(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_insert_explicit (
			id integer PRIMARY KEY,
			name text NOT NULL,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_explicit")
	}()

	// Insert row with explicit PK
	columns := []string{"id", "name", "email"}
	values := []interface{}{100, "Alice", "alice@example.com"}

	pkValue, err := client.InsertRow(ctx, "public", "test_insert_explicit", columns, values)
	if err != nil {
		t.Fatalf("InsertRow() failed: %v", err)
	}

	if pkValue != "100" {
		t.Errorf("Expected PK='100', got %q", pkValue)
	}

	// Verify row was inserted
	row, err := client.GetRow(ctx, "public", "test_insert_explicit", SinglePKMatch("id", "100"))
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	if len(row.Values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(row.Values))
	}

	t.Logf("Inserted row with explicit PK: %v", row.Values)
}

func TestInsertRow_AutoGeneratedPK(t *testing.T) {
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

	// Create test table with SERIAL PK
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_insert_serial (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_serial")
	}()

	// Insert row without PK (auto-generated)
	columns := []string{"name", "email"}
	values := []interface{}{"Bob", "bob@example.com"}

	pkValue, err := client.InsertRow(ctx, "public", "test_insert_serial", columns, values)
	if err != nil {
		t.Fatalf("InsertRow() failed: %v", err)
	}

	// PK should be auto-generated (typically "1" for first row)
	if pkValue == "" {
		t.Error("Expected non-empty auto-generated PK")
	}

	t.Logf("Inserted row with auto-generated PK: %s", pkValue)

	// Insert another row
	columns2 := []string{"name", "email"}
	values2 := []interface{}{"Charlie", "charlie@example.com"}

	pkValue2, err := client.InsertRow(ctx, "public", "test_insert_serial", columns2, values2)
	if err != nil {
		t.Fatalf("InsertRow() second insert failed: %v", err)
	}

	// Second PK should be different from first
	if pkValue2 == pkValue {
		t.Errorf("Expected different PKs, got same: %s", pkValue)
	}

	t.Logf("Second auto-generated PK: %s", pkValue2)
}

func TestInsertRow_NullValues(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_insert_null (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_null")
	}()

	// Insert row with NULL values
	columns := []string{"name", "email", "age"}
	values := []interface{}{"Dana", nil, nil}

	pkValue, err := client.InsertRow(ctx, "public", "test_insert_null", columns, values)
	if err != nil {
		t.Fatalf("InsertRow() failed: %v", err)
	}

	// Verify NULL values
	row, err := client.GetRow(ctx, "public", "test_insert_null", SinglePKMatch("id", pkValue))
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	// email (index 2) and age (index 3) should be nil
	if row.Values[2] != nil {
		t.Errorf("Expected NULL email, got %v", row.Values[2])
	}
	if row.Values[3] != nil {
		t.Errorf("Expected NULL age, got %v", row.Values[3])
	}

	t.Logf("Inserted row with NULL values: %v", row.Values)
}

func TestInsertRow_DuplicatePK(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_insert_dup (
			id integer PRIMARY KEY,
			name text NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_dup")
	}()

	// Insert first row
	columns := []string{"id", "name"}
	values := []interface{}{1, "First"}

	_, err = client.InsertRow(ctx, "public", "test_insert_dup", columns, values)
	if err != nil {
		t.Fatalf("First InsertRow() failed: %v", err)
	}

	// Try to insert duplicate PK
	values2 := []interface{}{1, "Second"} // Same ID
	_, err = client.InsertRow(ctx, "public", "test_insert_dup", columns, values2)
	if err == nil {
		t.Fatal("Expected error for duplicate PK, got nil")
	}

	t.Logf("Got expected error for duplicate PK: %v", err)
}

func TestInsertRow_NoColumns(t *testing.T) {
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

	// Try to insert with no columns
	_, err = client.InsertRow(ctx, "public", "test_table", []string{}, []interface{}{})
	if err == nil {
		t.Fatal("Expected error for no columns, got nil")
	}

	if err.Error() != "no columns provided for insert" {
		t.Errorf("Expected 'no columns provided for insert', got: %v", err)
	}
}

func TestInsertRow_ColumnValueMismatch(t *testing.T) {
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

	// Try to insert with mismatched columns/values
	columns := []string{"id", "name", "email"}
	values := []interface{}{1, "Alice"} // Missing one value

	_, err = client.InsertRow(ctx, "public", "test_table", columns, values)
	if err == nil {
		t.Fatal("Expected error for column/value mismatch, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestClient_InsertRow_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.InsertRow(ctx, "public", "test_table", []string{"name"}, []interface{}{"Alice"})
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

// ============================================================================
// UpdateRow Tests
// ============================================================================

func TestUpdateRow_PartialUpdate(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_partial (
			id integer PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_partial")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_partial (id, name, email, age) VALUES
		(1, 'Alice', 'alice@example.com', 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update only email (partial update)
	columns := []string{"email"}
	values := []interface{}{"newemail@example.com"}

	err = client.UpdateRow(ctx, "public", "test_update_partial", SinglePKMatch("id", "1"), columns, values)
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify only email changed
	row, err := client.GetRow(ctx, "public", "test_update_partial", SinglePKMatch("id", "1"))
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	// name should still be "Alice" (index 1)
	if row.Values[1] != "Alice" {
		t.Errorf("Expected name='Alice', got %v", row.Values[1])
	}

	// email should be updated (index 2)
	if row.Values[2] != "newemail@example.com" {
		t.Errorf("Expected email='newemail@example.com', got %v", row.Values[2])
	}

	t.Logf("Partial update successful: %v", row.Values)
}

func TestUpdateRow_AllColumns(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_all (
			id integer PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_all")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_all (id, name, email, age) VALUES
		(1, 'Alice', 'alice@example.com', 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update all columns
	columns := []string{"name", "email", "age"}
	values := []interface{}{"Bob", "bob@example.com", int32(25)}

	err = client.UpdateRow(ctx, "public", "test_update_all", SinglePKMatch("id", "1"), columns, values)
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify all columns changed
	row, err := client.GetRow(ctx, "public", "test_update_all", SinglePKMatch("id", "1"))
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	if row.Values[1] != "Bob" {
		t.Errorf("Expected name='Bob', got %v", row.Values[1])
	}
	if row.Values[2] != "bob@example.com" {
		t.Errorf("Expected email='bob@example.com', got %v", row.Values[2])
	}
	if row.Values[3] != int32(25) {
		t.Errorf("Expected age=25, got %v", row.Values[3])
	}

	t.Logf("Full update successful: %v", row.Values)
}

func TestUpdateRow_SetToNull(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_tonull (
			id integer PRIMARY KEY,
			name text NOT NULL,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_tonull")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_tonull (id, name, email) VALUES
		(1, 'Alice', 'alice@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update email to NULL
	columns := []string{"email"}
	values := []interface{}{nil}

	err = client.UpdateRow(ctx, "public", "test_update_tonull", SinglePKMatch("id", "1"), columns, values)
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify email is NULL
	value, err := client.GetColumn(ctx, "public", "test_update_tonull", SinglePKMatch("id", "1"), "email")
	if err != nil {
		t.Fatalf("GetColumn() failed: %v", err)
	}

	if value != nil {
		t.Errorf("Expected NULL email, got %v", value)
	}

	t.Logf("Update to NULL successful")
}

func TestUpdateRow_NotFound(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_notfound (
			id integer PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_notfound")
	}()

	// Try to update non-existent row
	columns := []string{"name"}
	values := []interface{}{"New Name"}

	err = client.UpdateRow(ctx, "public", "test_update_notfound", SinglePKMatch("id", "999"), columns, values)
	if err == nil {
		t.Fatal("Expected error for non-existent row, got nil")
	}

	if err.Error() != "row not found" {
		t.Errorf("Expected 'row not found', got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

func TestUpdateRow_NoColumns(t *testing.T) {
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

	// Try to update with no columns
	err = client.UpdateRow(ctx, "public", "test_table", SinglePKMatch("id", "1"), []string{}, []interface{}{})
	if err == nil {
		t.Fatal("Expected error for no columns, got nil")
	}

	if err.Error() != "no columns provided for update" {
		t.Errorf("Expected 'no columns provided for update', got: %v", err)
	}
}

func TestClient_UpdateRow_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	err := client.UpdateRow(ctx, "public", "test_table", SinglePKMatch("id", "1"), []string{"name"}, []interface{}{"Alice"})
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

// ============================================================================
// DeleteRow Tests
// ============================================================================

func TestDeleteRow_Simple(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_delete_simple (
			id integer PRIMARY KEY,
			name text NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_simple")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_delete_simple (id, name) VALUES (1, 'Alice'), (2, 'Bob')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Delete row with id=1
	err = client.DeleteRow(ctx, "public", "test_delete_simple", SinglePKMatch("id", "1"))
	if err != nil {
		t.Fatalf("DeleteRow() failed: %v", err)
	}

	// Verify row is deleted
	_, err = client.GetRow(ctx, "public", "test_delete_simple", SinglePKMatch("id", "1"))
	if err == nil {
		t.Fatal("Expected error for deleted row, got nil")
	}

	// Verify other row still exists
	row, err := client.GetRow(ctx, "public", "test_delete_simple", SinglePKMatch("id", "2"))
	if err != nil {
		t.Fatalf("Row 2 should still exist: %v", err)
	}

	t.Logf("Delete successful, remaining row: %v", row.Values)
}

func TestDeleteRow_NotFound(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_delete_notfound (
			id integer PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_notfound")
	}()

	// Try to delete non-existent row
	err = client.DeleteRow(ctx, "public", "test_delete_notfound", SinglePKMatch("id", "999"))
	if err == nil {
		t.Fatal("Expected error for non-existent row, got nil")
	}

	if err.Error() != "row not found" {
		t.Errorf("Expected 'row not found', got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

func TestDeleteRow_MultipleDeletes(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_delete_multi (
			id integer PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_multi")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_delete_multi (id, name) VALUES
		(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Delete all rows one by one
	for _, id := range []string{"1", "2", "3"} {
		err = client.DeleteRow(ctx, "public", "test_delete_multi", SinglePKMatch("id", id))
		if err != nil {
			t.Fatalf("DeleteRow(%s) failed: %v", id, err)
		}
	}

	// Verify table is empty
	var count int
	err = client.pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_delete_multi").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows, got %d", count)
	}

	t.Logf("All rows deleted successfully")
}

func TestGetRow_CompositePK(t *testing.T) {
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

	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_row_cpk (
			a int,
			b int,
			name text,
			PRIMARY KEY (a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_cpk")
	}()

	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_row_cpk (a, b, name) VALUES
		(1, 2, 'target'),
		(1, 3, 'other'),
		(2, 2, 'another')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Fetch using composite PKMatch
	pk := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "2"}}
	row, err := client.GetRow(ctx, "public", "test_get_row_cpk", pk)
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	if len(row.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(row.Columns))
	}

	// Find the name column value
	nameIdx := -1
	for i, col := range row.Columns {
		if col == "name" {
			nameIdx = i
			break
		}
	}
	if nameIdx == -1 {
		t.Fatal("Column 'name' not found in result")
	}

	nameVal := fmt.Sprintf("%v", row.Values[nameIdx])
	if nameVal != "target" {
		t.Errorf("Expected name='target', got %q", nameVal)
	}
}

func TestUpdateRow_CompositePK(t *testing.T) {
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

	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_row_cpk (
			a int,
			b int,
			name text,
			status text,
			PRIMARY KEY (a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_row_cpk")
	}()

	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_row_cpk (a, b, name, status) VALUES
		(1, 2, 'Alice', 'active'),
		(1, 3, 'Bob', 'active')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update row (1, 2) using composite PKMatch
	pk := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "2"}}
	err = client.UpdateRow(ctx, "public", "test_update_row_cpk", pk,
		[]string{"name", "status"}, []interface{}{"Alice Updated", "inactive"})
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify the updated row
	row, err := client.GetRow(ctx, "public", "test_update_row_cpk", pk)
	if err != nil {
		t.Fatalf("GetRow() after update failed: %v", err)
	}

	// Find name and status values
	vals := make(map[string]string)
	for i, col := range row.Columns {
		vals[col] = fmt.Sprintf("%v", row.Values[i])
	}

	if vals["name"] != "Alice Updated" {
		t.Errorf("Expected name='Alice Updated', got %q", vals["name"])
	}
	if vals["status"] != "inactive" {
		t.Errorf("Expected status='inactive', got %q", vals["status"])
	}

	// Verify the other row was NOT updated
	otherPK := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "3"}}
	otherRow, err := client.GetRow(ctx, "public", "test_update_row_cpk", otherPK)
	if err != nil {
		t.Fatalf("GetRow() for other row failed: %v", err)
	}

	otherVals := make(map[string]string)
	for i, col := range otherRow.Columns {
		otherVals[col] = fmt.Sprintf("%v", otherRow.Values[i])
	}

	if otherVals["name"] != "Bob" {
		t.Errorf("Other row name should be unchanged, got %q", otherVals["name"])
	}
}

func TestDeleteRow_CompositePK(t *testing.T) {
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

	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_delete_cpk (
			a int,
			b int,
			name text,
			PRIMARY KEY (a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_cpk")
	}()

	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_delete_cpk (a, b, name) VALUES
		(1, 2, 'delete-me'),
		(1, 3, 'keep-me')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Delete row (1, 2) using composite PKMatch
	pk := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "2"}}
	err = client.DeleteRow(ctx, "public", "test_delete_cpk", pk)
	if err != nil {
		t.Fatalf("DeleteRow() failed: %v", err)
	}

	// Verify it's gone
	_, err = client.GetRow(ctx, "public", "test_delete_cpk", pk)
	if err == nil {
		t.Fatal("Expected error for deleted row, got nil")
	}

	// Verify the other row still exists
	otherPK := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "3"}}
	row, err := client.GetRow(ctx, "public", "test_delete_cpk", otherPK)
	if err != nil {
		t.Fatalf("GetRow() for remaining row failed: %v", err)
	}

	nameIdx := -1
	for i, col := range row.Columns {
		if col == "name" {
			nameIdx = i
			break
		}
	}
	if nameIdx == -1 {
		t.Fatal("Column 'name' not found")
	}
	if fmt.Sprintf("%v", row.Values[nameIdx]) != "keep-me" {
		t.Errorf("Remaining row name = %q, want 'keep-me'", row.Values[nameIdx])
	}
}

func TestGetColumn_CompositePK(t *testing.T) {
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

	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_col_cpk (
			a int,
			b int,
			email text,
			PRIMARY KEY (a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_col_cpk")
	}()

	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_col_cpk (a, b, email) VALUES
		(1, 2, 'alice@example.com'),
		(1, 3, 'bob@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Fetch single column via composite PKMatch
	pk := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "2"}}
	value, err := client.GetColumn(ctx, "public", "test_get_col_cpk", pk, "email")
	if err != nil {
		t.Fatalf("GetColumn() failed: %v", err)
	}

	strVal := fmt.Sprintf("%v", value)
	if strVal != "alice@example.com" {
		t.Errorf("Expected 'alice@example.com', got %q", strVal)
	}

	// Verify different composite PK returns different value
	pk2 := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "3"}}
	value2, err := client.GetColumn(ctx, "public", "test_get_col_cpk", pk2, "email")
	if err != nil {
		t.Fatalf("GetColumn() for second row failed: %v", err)
	}

	strVal2 := fmt.Sprintf("%v", value2)
	if strVal2 != "bob@example.com" {
		t.Errorf("Expected 'bob@example.com', got %q", strVal2)
	}
}

func TestUpdateColumn_CompositePK(t *testing.T) {
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

	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_update_col_cpk (
			a int,
			b int,
			email text,
			PRIMARY KEY (a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_col_cpk")
	}()

	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_update_col_cpk (a, b, email) VALUES
		(1, 2, 'old@example.com'),
		(1, 3, 'other@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Update single column via composite PKMatch
	pk := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "2"}}
	err = client.UpdateColumn(ctx, "public", "test_update_col_cpk", pk, "email", "new@example.com")
	if err != nil {
		t.Fatalf("UpdateColumn() failed: %v", err)
	}

	// Verify the update
	value, err := client.GetColumn(ctx, "public", "test_update_col_cpk", pk, "email")
	if err != nil {
		t.Fatalf("GetColumn() after update failed: %v", err)
	}

	strVal := fmt.Sprintf("%v", value)
	if strVal != "new@example.com" {
		t.Errorf("Expected 'new@example.com', got %q", strVal)
	}

	// Verify the other row was NOT updated
	otherPK := &PKMatch{Columns: []string{"a", "b"}, Values: []string{"1", "3"}}
	otherValue, err := client.GetColumn(ctx, "public", "test_update_col_cpk", otherPK, "email")
	if err != nil {
		t.Fatalf("GetColumn() for other row failed: %v", err)
	}

	otherStr := fmt.Sprintf("%v", otherValue)
	if otherStr != "other@example.com" {
		t.Errorf("Other row should be unchanged, got %q", otherStr)
	}
}

func TestClient_DeleteRow_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	err := client.DeleteRow(ctx, "public", "test_table", SinglePKMatch("id", "1"))
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

// TestExecuteUndoTransaction_DefersFKConstraint verifies the deferral path
// documented in ADR-017: ExecuteUndoTransaction must call SET CONSTRAINTS ALL
// DEFERRED so rows can be UPSERTed in any order within the transaction. Without
// this, restoring a child row before its parent dir row fires parent_id_fkey
// (SQLSTATE 23503) immediately and aborts the undo.
//
// The test deliberately passes RestoreFileIDs in CHILD-FIRST order so the FK
// would violate without deferral. With the fix in place, the FK is checked at
// COMMIT and both rows survive.
func TestExecuteUndoTransaction_DefersFKConstraint(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := &config.Config{PoolSize: 5, PoolMaxIdle: 2}
	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Use unique table names so parallel runs / leftover state don't collide.
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	sourceTable := "tundo_src_" + suffix
	historyTable := "tundo_hist_" + suffix
	logTable := "tundo_log_" + suffix
	schema := "public"

	srcQT := QuoteTable(schema, sourceTable)
	histQT := QuoteTable(schema, historyTable)
	logQT := QuoteTable(schema, logTable)

	// Schema mirrors the relevant pieces of the synth-app shape from ADR-017:
	// parent_id FK and (parent_id, filename, filetype) UNIQUE both
	// DEFERRABLE INITIALLY IMMEDIATE. No archive trigger -- we'll seed history
	// directly so the test stays focused on ExecuteUndoTransaction's behavior.
	setupSQL := []string{
		fmt.Sprintf(`CREATE TABLE %s (
			id UUID PRIMARY KEY,
			parent_id UUID REFERENCES %s(id) DEFERRABLE INITIALLY IMMEDIATE,
			filename TEXT NOT NULL,
			filetype TEXT NOT NULL DEFAULT 'file',
			body TEXT,
			modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE
		)`, srcQT, srcQT),
		fmt.Sprintf(`CREATE TABLE %s (
			file_id UUID,
			parent_id UUID,
			filename TEXT NOT NULL,
			filetype TEXT,
			body TEXT,
			modified_at TIMESTAMPTZ,
			version_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			operation TEXT NOT NULL
		)`, histQT),
		fmt.Sprintf(`CREATE TABLE %s (
			log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			file_id UUID NOT NULL,
			type TEXT NOT NULL,
			user_id TEXT,
			filename TEXT NOT NULL,
			version_id UUID,
			description TEXT
		)`, logQT),
	}
	for _, sql := range setupSQL {
		if _, err := client.pool.Exec(ctx, sql); err != nil {
			t.Fatalf("setup failed (%s): %v", sql, err)
		}
	}
	defer func() {
		// Drop in reverse-dependency order; FK is on the source table itself
		// (self-reference), so a single DROP suffices, but we list all three.
		_, _ = client.pool.Exec(context.Background(),
			fmt.Sprintf(`DROP TABLE IF EXISTS %s, %s, %s`, srcQT, histQT, logQT))
	}()

	// Pre-generate UUIDs so we can refer to them by name and assert.
	var parentID, childID, parentVersion, childVersion string
	err = client.pool.QueryRow(ctx,
		`SELECT uuidv7()::text, uuidv7()::text, uuidv7()::text, uuidv7()::text`,
	).Scan(&parentID, &childID, &parentVersion, &childVersion)
	if err != nil {
		t.Fatalf("uuid generation failed: %v", err)
	}

	// Seed history with delete entries for both rows. The undo will UPSERT
	// from these history rows back into the source table.
	insertHistory := func(versionID, fileID, parentVal, filename, filetype, body string) {
		t.Helper()
		var parentArg interface{}
		if parentVal == "" {
			parentArg = nil
		} else {
			parentArg = parentVal
		}
		_, err := client.pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (version_id, file_id, parent_id, filename, filetype, body, modified_at, operation)
			 VALUES ($1, $2, $3, $4, $5, $6, now() - interval '1 hour', 'delete')`, histQT),
			versionID, fileID, parentArg, filename, filetype, body)
		if err != nil {
			t.Fatalf("seed history (%s): %v", filename, err)
		}
	}
	insertHistory(parentVersion, parentID, "", "parent", "directory", "")
	insertHistory(childVersion, childID, parentID, "child.md", "file", "child body")

	// Source table is empty -- both rows were "deleted" (we just simulated it
	// by populating history).

	// Call ExecuteUndoTransaction with CHILD-FIRST restore order. Without
	// SET CONSTRAINTS ALL DEFERRED this aborts with parent_id_fkey on the
	// first UPSERT.
	err = client.ExecuteUndoTransaction(ctx, &UndoTransactionParams{
		Schema:            schema,
		SourceTable:       sourceTable,
		HistoryTable:      historyTable,
		LogTable:          logTable,
		Description:       "test undo deferred FK",
		RestoreVersionIDs: []string{childVersion, parentVersion},
		RestoreFileIDs:    []string{childID, parentID},
		RestoreFilenames:  []string{"child.md", "parent"},
		UserID:            "test-user",
	})
	if err != nil {
		t.Fatalf("ExecuteUndoTransaction must succeed with deferred constraints (child-first restore): %v", err)
	}

	// Verify both rows are restored and the FK relationship is intact.
	var parentExists bool
	err = client.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1 AND filetype = 'directory')`, srcQT),
		parentID).Scan(&parentExists)
	if err != nil {
		t.Fatalf("query parent: %v", err)
	}
	if !parentExists {
		t.Fatal("parent row should be restored")
	}

	var childParent string
	err = client.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT parent_id::text FROM %s WHERE id = $1`, srcQT),
		childID).Scan(&childParent)
	if err != nil {
		t.Fatalf("query child: %v", err)
	}
	if childParent != parentID {
		t.Errorf("child.parent_id = %q, want %q", childParent, parentID)
	}

	// Verify undo log entries were written for both restored rows.
	var logCount int
	err = client.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE type = 'undo' AND file_id IN ($1, $2)`, logQT),
		parentID, childID).Scan(&logCount)
	if err != nil {
		t.Fatalf("query log: %v", err)
	}
	if logCount != 2 {
		t.Errorf("expected 2 undo log entries, got %d", logCount)
	}

	// Verify modified_at was bumped to roughly now() on both restored rows.
	// History rows have modified_at = now() - 1h; the bump-after-undo step
	// should override that. Without the bump, NFS clients with `noac` keep
	// serving cached readdir entries from before the undo (the file appears
	// in `ls`, but stat/open returns ENOENT).
	var ageSec float64
	err = client.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT EXTRACT(EPOCH FROM (now() - MIN(%s))) FROM %s WHERE %s IN ($1, $2)`,
			qi("modified_at"), srcQT, qi("id")),
		parentID, childID).Scan(&ageSec)
	if err != nil {
		t.Fatalf("query mtime: %v", err)
	}
	if ageSec > 60 {
		t.Errorf("modified_at not bumped: oldest restored row is %.0fs old (expected <60s)", ageSec)
	}
}

// undoTestEnv holds the per-test PostgreSQL fixture (client, schema, table
// names) used by the ExecuteUndoTransaction unit tests below.
type undoTestEnv struct {
	client                              *Client
	ctx                                 context.Context
	schema                              string
	sourceTable, historyTable, logTable string
	srcQT, histQT, logQT                string
}

// setupUndoTestEnv creates the parent_id-FK, UNIQUE, and history/log tables
// used by the ExecuteUndoTransaction tests, mirroring the synth-app shape from
// ADR-017. Returns the env and a cleanup func that drops the tables and
// closes the client. Skips the test if no test DB is available.
func setupUndoTestEnv(t *testing.T) (*undoTestEnv, func()) {
	t.Helper()
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	cfg := &config.Config{PoolSize: 5, PoolMaxIdle: 2}
	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		cancel()
		t.Fatalf("NewClient() failed: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	env := &undoTestEnv{
		client:       client,
		ctx:          ctx,
		schema:       "public",
		sourceTable:  "tundo_src_" + suffix,
		historyTable: "tundo_hist_" + suffix,
		logTable:     "tundo_log_" + suffix,
	}
	env.srcQT = QuoteTable(env.schema, env.sourceTable)
	env.histQT = QuoteTable(env.schema, env.historyTable)
	env.logQT = QuoteTable(env.schema, env.logTable)

	setupSQL := []string{
		fmt.Sprintf(`CREATE TABLE %s (
			id UUID PRIMARY KEY,
			parent_id UUID REFERENCES %s(id) DEFERRABLE INITIALLY IMMEDIATE,
			filename TEXT NOT NULL,
			filetype TEXT NOT NULL DEFAULT 'file',
			body TEXT,
			modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE
		)`, env.srcQT, env.srcQT),
		fmt.Sprintf(`CREATE TABLE %s (
			file_id UUID,
			parent_id UUID,
			filename TEXT NOT NULL,
			filetype TEXT,
			body TEXT,
			modified_at TIMESTAMPTZ,
			version_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			operation TEXT NOT NULL
		)`, env.histQT),
		fmt.Sprintf(`CREATE TABLE %s (
			log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
			file_id UUID NOT NULL,
			type TEXT NOT NULL,
			user_id TEXT,
			filename TEXT NOT NULL,
			version_id UUID,
			description TEXT
		)`, env.logQT),
	}
	for _, sql := range setupSQL {
		if _, err := client.pool.Exec(ctx, sql); err != nil {
			cancel()
			_ = client.Close()
			t.Fatalf("setup failed (%s): %v", sql, err)
		}
	}

	cleanup := func() {
		_, _ = client.pool.Exec(context.Background(),
			fmt.Sprintf(`DROP TABLE IF EXISTS %s, %s, %s`, env.srcQT, env.histQT, env.logQT))
		_ = client.Close()
		cancel()
	}
	return env, cleanup
}

// seedHistory inserts a history row with operation='delete' (the typical
// archive that an undo would restore from) and modified_at = now() - 1h, so
// the restored row's modified_at is observably old without the bump.
func (env *undoTestEnv) seedHistory(t *testing.T, versionID, fileID, parent, filename, filetype, body string) {
	t.Helper()
	var parentArg interface{}
	if parent != "" {
		parentArg = parent
	}
	_, err := env.client.pool.Exec(env.ctx, fmt.Sprintf(
		`INSERT INTO %s (version_id, file_id, parent_id, filename, filetype, body, modified_at, operation)
		 VALUES ($1, $2, $3, $4, $5, $6, now() - interval '1 hour', 'delete')`, env.histQT),
		versionID, fileID, parentArg, filename, filetype, body)
	if err != nil {
		t.Fatalf("seed history (%s): %v", filename, err)
	}
}

// genUUIDs returns n new UUIDv7 strings.
func (env *undoTestEnv) genUUIDs(t *testing.T, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := 0; i < n; i++ {
		var u string
		if err := env.client.pool.QueryRow(env.ctx, `SELECT uuidv7()::text`).Scan(&u); err != nil {
			t.Fatalf("uuidv7() failed: %v", err)
		}
		out[i] = u
	}
	return out
}

// TestExecuteUndoTransaction_DefersUniqueConstraint verifies the second half
// of the SET CONSTRAINTS ALL DEFERRED contract: the
// (parent_id, filename, filetype) UNIQUE constraint must also defer to COMMIT.
//
// Scenario: rename-as-replace. A and B both exist at root. The user does
// `mv A B` -- B's row is deleted, A's filename is changed to 'B'. Undoing
// this restoration must produce both rows with their original filenames.
//
// We force RestoreFileIDs in B-first order so B's INSERT runs while A still
// has filename='B' in the source table -- this is a transient UNIQUE
// violation that only resolves when A's UPSERT then reverts filename='A'.
// Without deferral, B's INSERT fails immediately. The pre-existing
// "filename already occupied (rename-as-replace)" error path at query.go is
// bypassed by the deferral path -- this test pins that down.
func TestExecuteUndoTransaction_DefersUniqueConstraint(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()

	ids := env.genUUIDs(t, 4)
	aID, bID := ids[0], ids[1]
	aVer, bVer := ids[2], ids[3]

	// History: A's pre-rename state (filename='A'), B's pre-delete state
	// (filename='B'). Both at root (parent=NULL).
	env.seedHistory(t, aVer, aID, "", "A", "file", "body A")
	env.seedHistory(t, bVer, bID, "", "B", "file", "body B")

	// Source post-rename-as-replace: A row exists with filename='B', B row gone.
	_, err := env.client.pool.Exec(env.ctx,
		fmt.Sprintf(`INSERT INTO %s (id, parent_id, filename, filetype, body) VALUES ($1, NULL, 'B', 'file', 'body A')`, env.srcQT),
		aID)
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}

	// B-first restore order forces the transient UNIQUE violation.
	err = env.client.ExecuteUndoTransaction(env.ctx, &UndoTransactionParams{
		Schema:            env.schema,
		SourceTable:       env.sourceTable,
		HistoryTable:      env.historyTable,
		LogTable:          env.logTable,
		Description:       "test undo deferred UNIQUE",
		RestoreVersionIDs: []string{bVer, aVer},
		RestoreFileIDs:    []string{bID, aID},
		RestoreFilenames:  []string{"B", "A"},
		UserID:            "test-user",
	})
	if err != nil {
		t.Fatalf("ExecuteUndoTransaction must succeed with deferred UNIQUE: %v", err)
	}

	// Verify both rows have their restored filenames.
	rows, err := env.client.pool.Query(env.ctx,
		fmt.Sprintf(`SELECT id::text, filename FROM %s WHERE id IN ($1, $2) ORDER BY filename`, env.srcQT),
		aID, bID)
	if err != nil {
		t.Fatalf("query source: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var id, filename string
		if err := rows.Scan(&id, &filename); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = filename
	}
	if got[aID] != "A" {
		t.Errorf("A.filename = %q, want %q", got[aID], "A")
	}
	if got[bID] != "B" {
		t.Errorf("B.filename = %q, want %q", got[bID], "B")
	}
}

// TestExecuteUndoTransaction_BumpsParentMtimeWhenOnlyChildRestored isolates
// the SELECT-DISTINCT-parent_id branch of the post-undo modified_at bump.
//
// The parent dir exists throughout (never in the restore set); only a
// child file is restored. The bump must still touch the parent so an NFS
// client with `noac` re-reads its directory listing.
func TestExecuteUndoTransaction_BumpsParentMtimeWhenOnlyChildRestored(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()

	ids := env.genUUIDs(t, 3)
	parentID, childID, childVer := ids[0], ids[1], ids[2]

	// Parent dir created an hour ago (set explicitly so we can detect
	// whether the bump touched it).
	_, err := env.client.pool.Exec(env.ctx, fmt.Sprintf(
		`INSERT INTO %s (id, parent_id, filename, filetype, modified_at)
		 VALUES ($1, NULL, 'parent', 'directory', now() - interval '1 hour')`, env.srcQT),
		parentID)
	if err != nil {
		t.Fatalf("seed parent: %v", err)
	}

	// Child existed under parent, then was deleted -- only history retains it.
	env.seedHistory(t, childVer, childID, parentID, "child.md", "file", "child body")

	// Restore the child only.
	err = env.client.ExecuteUndoTransaction(env.ctx, &UndoTransactionParams{
		Schema:            env.schema,
		SourceTable:       env.sourceTable,
		HistoryTable:      env.historyTable,
		LogTable:          env.logTable,
		Description:       "test undo bumps parent mtime",
		RestoreVersionIDs: []string{childVer},
		RestoreFileIDs:    []string{childID},
		RestoreFilenames:  []string{"child.md"},
		UserID:            "test-user",
	})
	if err != nil {
		t.Fatalf("ExecuteUndoTransaction: %v", err)
	}

	// Parent's modified_at should be fresh (within 60s of now).
	var ageSec float64
	err = env.client.pool.QueryRow(env.ctx,
		fmt.Sprintf(`SELECT EXTRACT(EPOCH FROM (now() - modified_at)) FROM %s WHERE id = $1`, env.srcQT),
		parentID).Scan(&ageSec)
	if err != nil {
		t.Fatalf("query parent mtime: %v", err)
	}
	if ageSec > 60 {
		t.Errorf("parent.modified_at not bumped: %.0fs old (expected <60s)", ageSec)
	}
}

// TestExecuteUndoTransaction_DefersFKMultiLevel extends the FK-deferral
// coverage to a 3-level hierarchy (grandparent -> parent -> child). The
// restore order is reversed (child, parent, grandparent), so each row's
// parent_id references a row that hasn't been UPSERTed yet. The whole
// chain must resolve at COMMIT.
//
// Catches a regression if someone narrows the deferral scope so that only
// the immediate FK is deferred; here the FK chain spans two hops.
func TestExecuteUndoTransaction_DefersFKMultiLevel(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()

	ids := env.genUUIDs(t, 6)
	gID, pID, cID := ids[0], ids[1], ids[2]
	gVer, pVer, cVer := ids[3], ids[4], ids[5]

	// History: all three exist as deletes (root grandparent, nested parent
	// under it, leaf child under parent).
	env.seedHistory(t, gVer, gID, "", "g", "directory", "")
	env.seedHistory(t, pVer, pID, gID, "p", "directory", "")
	env.seedHistory(t, cVer, cID, pID, "c.md", "file", "child")

	// Source is empty; everything restores via UPSERT-INSERT.
	// Reverse order forces the FK chain to be resolved at COMMIT only.
	err := env.client.ExecuteUndoTransaction(env.ctx, &UndoTransactionParams{
		Schema:            env.schema,
		SourceTable:       env.sourceTable,
		HistoryTable:      env.historyTable,
		LogTable:          env.logTable,
		Description:       "test undo 3-level FK",
		RestoreVersionIDs: []string{cVer, pVer, gVer},
		RestoreFileIDs:    []string{cID, pID, gID},
		RestoreFilenames:  []string{"c.md", "p", "g"},
		UserID:            "test-user",
	})
	if err != nil {
		t.Fatalf("ExecuteUndoTransaction must succeed with deferred FK chain: %v", err)
	}

	// Verify the full chain: g exists at root, p->g, c->p.
	var gParent, pParent, cParent *string
	row := env.client.pool.QueryRow(env.ctx,
		fmt.Sprintf(`SELECT (SELECT parent_id::text FROM %s WHERE id = $1),
			               (SELECT parent_id::text FROM %s WHERE id = $2),
			               (SELECT parent_id::text FROM %s WHERE id = $3)`,
			env.srcQT, env.srcQT, env.srcQT),
		gID, pID, cID)
	if err := row.Scan(&gParent, &pParent, &cParent); err != nil {
		t.Fatalf("query chain: %v", err)
	}
	if gParent != nil {
		t.Errorf("g.parent_id = %v, want NULL", *gParent)
	}
	if pParent == nil || *pParent != gID {
		t.Errorf("p.parent_id = %v, want %s", pParent, gID)
	}
	if cParent == nil || *cParent != pID {
		t.Errorf("c.parent_id = %v, want %s", cParent, pID)
	}
}

// TestExecuteUndoTransaction_DeleteOnlyUndo guards two distinct edge cases
// of a DELETE-only undo (single 'create' op being reversed):
//
//  1. The empty-RestoreFileIDs branch of the post-undo modified_at bump.
//     That SQL uses ANY($1::uuid[]) and would error on an empty array if
//     the branch wasn't gated.
//
//  2. The defensive NULL-version_id path in Step 3. The fixture deliberately
//     doesn't install the archive BEFORE-DELETE trigger (setupUndoTestEnv
//     keeps the schema trigger-free so other tests can seed history rows
//     directly without interference), which means Step 1's DELETE fires no
//     trigger and writes no history row. The inline version_id capture
//     finds nothing. Step 3 must record the resulting undo log entry with
//     version_id = NULL rather than failing with PG's "invalid input
//     syntax for type uuid" on the empty string. In a properly-configured
//     production workspace the archive trigger always fires and this
//     defensive path is dead code; the test holds it down regardless so a
//     future regression doesn't reintroduce the cryptic-error failure
//     mode. emptyVersionIDToNull (in query.go) handles the conversion and
//     emits a warning identifying the file_id and the branch.
func TestExecuteUndoTransaction_DeleteOnlyUndo(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()

	ids := env.genUUIDs(t, 1)
	rID := ids[0]

	// Source has one row; the undo will delete it.
	_, err := env.client.pool.Exec(env.ctx,
		fmt.Sprintf(`INSERT INTO %s (id, parent_id, filename, filetype, body) VALUES ($1, NULL, 'r.md', 'file', 'data')`, env.srcQT),
		rID)
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}

	err = env.client.ExecuteUndoTransaction(env.ctx, &UndoTransactionParams{
		Schema:          env.schema,
		SourceTable:     env.sourceTable,
		HistoryTable:    env.historyTable,
		LogTable:        env.logTable,
		Description:     "test undo delete-only",
		DeleteFileIDs:   []string{rID},
		DeleteFilenames: []string{"r.md"},
		UserID:          "test-user",
	})
	if err != nil {
		t.Fatalf("ExecuteUndoTransaction (delete-only) must succeed: %v", err)
	}

	// Row gone from source.
	var exists bool
	err = env.client.pool.QueryRow(env.ctx,
		fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)`, env.srcQT),
		rID).Scan(&exists)
	if err != nil {
		t.Fatalf("query source: %v", err)
	}
	if exists {
		t.Errorf("row should be deleted by undo")
	}

	// Undo log entry was written.
	var logCount int
	err = env.client.pool.QueryRow(env.ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE file_id = $1 AND type = 'undo'`, env.logQT),
		rID).Scan(&logCount)
	if err != nil {
		t.Fatalf("query log: %v", err)
	}
	if logCount != 1 {
		t.Errorf("expected 1 undo log entry, got %d", logCount)
	}
}

// TestExecuteUndoTransaction_FailsAtCommitOnUnrestorableFK verifies that
// the deferred FK doesn't silently allow orphan rows. If a restore
// targets a child whose parent_id references a row that's neither in
// the source table nor in the restore set, the deferred FK *must* fire
// at COMMIT and the whole undo transaction must roll back.
//
// We never expect this state from normal user operations -- the undo
// classifier always pulls in the parent's row when its children are
// affected -- but the test pins down what happens if a future refactor
// or log inconsistency produces an unrestorable child: a clean FK error
// at COMMIT, no orphan, no silent corruption. The whole point of using
// DEFERRABLE INITIALLY IMMEDIATE (vs just removing the FK) is that
// genuinely-broken state still surfaces; we exercise that here.
func TestExecuteUndoTransaction_FailsAtCommitOnUnrestorableFK(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()

	ids := env.genUUIDs(t, 3)
	childID, missingParentID, childVer := ids[0], ids[1], ids[2]

	// Seed history with a child whose parent_id points at a UUID that has
	// no history row and no source row -- the unrestorable case.
	env.seedHistory(t, childVer, childID, missingParentID, "orphan.md", "file", "body")

	err := env.client.ExecuteUndoTransaction(env.ctx, &UndoTransactionParams{
		Schema:            env.schema,
		SourceTable:       env.sourceTable,
		HistoryTable:      env.historyTable,
		LogTable:          env.logTable,
		Description:       "test undo unrestorable FK",
		RestoreVersionIDs: []string{childVer},
		RestoreFileIDs:    []string{childID},
		RestoreFilenames:  []string{"orphan.md"},
		UserID:            "test-user",
	})
	if err == nil {
		t.Fatal("ExecuteUndoTransaction must fail when restoring a child whose parent doesn't exist anywhere")
	}

	pgErr := asPgError(err)
	if pgErr == nil {
		t.Fatalf("expected pg error, got: %v", err)
	}
	if pgErr.Code != "23503" {
		t.Errorf("expected SQLSTATE 23503 (foreign_key_violation), got %q: %v", pgErr.Code, pgErr.Message)
	}

	// Source must be empty -- the UPSERT happened during the TX but the
	// COMMIT rolled back when the deferred FK check failed.
	var count int
	err = env.client.pool.QueryRow(env.ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id = $1`, env.srcQT),
		childID).Scan(&count)
	if err != nil {
		t.Fatalf("query source: %v", err)
	}
	if count != 0 {
		t.Errorf("expected source to be empty after rolled-back undo, got %d row(s)", count)
	}
}

// --- Metadata table tests ---

func TestQueryMetadata_MissingTable(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := &config.Config{PoolSize: 2, PoolMaxIdle: 1}
	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	entries, err := client.QueryMetadata(ctx, "public", "no_such_table_metadata")
	if err != nil {
		t.Fatalf("QueryMetadata on missing table should soft-fail to nil, got error: %v", err)
	}
	if entries != nil {
		t.Errorf("expected nil entries, got %v", entries)
	}
}

func TestQueryMetadata_EmptyAndRoundTrip(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := &config.Config{PoolSize: 2, PoolMaxIdle: 1}
	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := "tmeta_" + suffix
	schema := "public"
	qt := QuoteTable(schema, table)

	_, err = client.pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		entry_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
		subject TEXT NOT NULL,
		user_id TEXT,
		description TEXT,
		payload JSONB NOT NULL DEFAULT '{}'::jsonb
	)`, qt))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS %s`, qt))
	}()

	// Empty table
	entries, err := client.QueryMetadata(ctx, schema, table)
	if err != nil {
		t.Fatalf("QueryMetadata empty: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries from empty table, got %d", len(entries))
	}

	// Round-trip: insert, query
	payload1 := []byte(`{"v":1,"key":"value-a"}`)
	if err := client.InsertMetadata(ctx, schema, table, "first-subject", "first description", payload1, "user-a"); err != nil {
		t.Fatalf("InsertMetadata 1: %v", err)
	}
	payload2 := []byte(`{"v":1,"key":"value-b"}`)
	if err := client.InsertMetadata(ctx, schema, table, "second-subject", "", payload2, ""); err != nil {
		t.Fatalf("InsertMetadata 2 (empty userID, empty desc): %v", err)
	}

	entries, err = client.QueryMetadata(ctx, schema, table)
	if err != nil {
		t.Fatalf("QueryMetadata after insert: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// ASC order: UUIDv7 default means inserted-order
	if entries[0].Subject != "first-subject" {
		t.Errorf("expected first-subject at index 0, got %s", entries[0].Subject)
	}
	if entries[1].Subject != "second-subject" {
		t.Errorf("expected second-subject at index 1, got %s", entries[1].Subject)
	}

	// Fields preserved
	if entries[0].UserID != "user-a" {
		t.Errorf("first entry UserID = %q, want %q", entries[0].UserID, "user-a")
	}
	if entries[0].Description != "first description" {
		t.Errorf("first entry Description = %q, want %q", entries[0].Description, "first description")
	}
	if string(entries[0].Payload) == "" || !bytesContains(entries[0].Payload, []byte("value-a")) {
		t.Errorf("first entry Payload missing value-a: %s", entries[0].Payload)
	}

	// Empty userID stored as NULL → round-trips as ""
	if entries[1].UserID != "" {
		t.Errorf("second entry UserID = %q, want empty (NULL roundtrip)", entries[1].UserID)
	}
	if entries[1].Description != "" {
		t.Errorf("second entry Description = %q, want empty (NULL roundtrip)", entries[1].Description)
	}
}

func bytesContains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// installCascadeTriggers installs the archive BEFORE trigger and the
// bump_parent_mtime AFTER trigger on env.sourceTable, mirroring the
// production triggers in internal/tigerfs/fs/synth/build.go. Used by
// undo unit tests that need to exercise the cascade chain end-to-end
// (e.g., the inline-version-id-capture test below).
//
// The archive trigger writes a history row for every INSERT/UPDATE/DELETE
// on the source. bump_parent_mtime fires AFTER child changes and runs
// UPDATE parent SET modified_at=now() -- which itself re-fires the
// archive trigger on the parent, producing the cascade artifact that
// motivates Fix 1.
func (env *undoTestEnv) installCascadeTriggers(t *testing.T) {
	t.Helper()

	archiveFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION archive_%s_history()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO %s (file_id, parent_id, filename, filetype, body, modified_at, operation)
        VALUES (NEW.id, NEW.parent_id, NEW.filename, NEW.filetype, NEW.body, NEW.modified_at, 'create');
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO %s (file_id, parent_id, filename, filetype, body, modified_at, operation)
        VALUES (OLD.id, OLD.parent_id, OLD.filename, OLD.filetype, OLD.body, OLD.modified_at,
                CASE WHEN OLD.filename != NEW.filename OR OLD.parent_id IS DISTINCT FROM NEW.parent_id
                     THEN 'rename' ELSE 'edit' END);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO %s (file_id, parent_id, filename, filetype, body, modified_at, operation)
        VALUES (OLD.id, OLD.parent_id, OLD.filename, OLD.filetype, OLD.body, OLD.modified_at, 'delete');
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql`, env.sourceTable, env.histQT, env.histQT, env.histQT)

	archiveTrigger := fmt.Sprintf(`CREATE TRIGGER trg_archive_%s
    BEFORE INSERT OR UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE FUNCTION archive_%s_history()`,
		env.sourceTable, env.srcQT, env.sourceTable)

	bumpFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION bump_%s_parent_mtime()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.parent_id IS NOT NULL THEN
            UPDATE %s SET modified_at = now()
            WHERE id = NEW.parent_id AND filetype = 'directory';
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.parent_id IS NOT NULL THEN
            UPDATE %s SET modified_at = now()
            WHERE id = OLD.parent_id AND filetype = 'directory';
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.parent_id IS DISTINCT FROM NEW.parent_id
           OR OLD.filename IS DISTINCT FROM NEW.filename THEN
            IF OLD.parent_id IS NOT NULL
               AND OLD.parent_id IS DISTINCT FROM NEW.parent_id THEN
                UPDATE %s SET modified_at = now()
                WHERE id = OLD.parent_id AND filetype = 'directory';
            END IF;
            IF NEW.parent_id IS NOT NULL THEN
                UPDATE %s SET modified_at = now()
                WHERE id = NEW.parent_id AND filetype = 'directory';
            END IF;
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql`, env.sourceTable, env.srcQT, env.srcQT, env.srcQT, env.srcQT)

	bumpTrigger := fmt.Sprintf(`CREATE TRIGGER trg_bump_%s
    AFTER INSERT OR DELETE OR UPDATE OF parent_id, filename ON %s
    FOR EACH ROW EXECUTE FUNCTION bump_%s_parent_mtime()`,
		env.sourceTable, env.srcQT, env.sourceTable)

	for _, sql := range []string{archiveFunc, archiveTrigger, bumpFunc, bumpTrigger} {
		if _, err := env.client.pool.Exec(env.ctx, sql); err != nil {
			t.Fatalf("install cascade trigger: %v\nsql: %s", err, sql)
		}
	}
}

// TestExecuteUndoTransaction_CapturesVersionIDInline_NotPostHoc verifies
// the Fix-1 invariant: each affected file's undo-log version_id is the
// row that trigger A wrote during that file's own UPSERT, not whatever
// "newest history row for file_id" happened to be at end-of-transaction.
//
// Pre-Fix-1, Step 3 ran "SELECT version_id ... ORDER BY DESC LIMIT 1" per
// file AFTER all Step 2 UPSERTs were done. The bump_parent_mtime AFTER
// trigger cascades when a child is restored, re-firing the archive
// trigger on the parent dir with a NEW (newer) version_id pointing at a
// no-semantic-content edit row. That cascade artifact became "newest"
// and corrupted the parent's recorded undo-log version_id.
//
// Scenario: dir D (filename='e') with one child C (filename='b.md').
// History contains a "pre-demo" snapshot of D (filename='d', op='rename')
// and a "pre-demo" snapshot of C (filename='a.md', op='rename'). Call
// ExecuteUndoTransaction with parent-first iteration (D before C, the
// bug-exposing order matching file_id ASC). After the call:
//   - Without Fix 1: D's undo log entry's version_id resolves to the
//     cascade artifact -- a history row with operation='edit' and
//     filename='d' (D's post-iteration-1 state, archived when trigger B
//     bumped D's mtime during C's UPSERT).
//   - With Fix 1: D's undo log entry's version_id resolves to D's own
//     restore snapshot -- a history row with operation='rename' and
//     filename='e' (D's pre-iteration-1 state, captured inline right
//     after D's UPSERT completed).
//
// The test asserts the post-Fix-1 outcome.
func TestExecuteUndoTransaction_CapturesVersionIDInline_NotPostHoc(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()
	env.installCascadeTriggers(t)

	// Generate stable UUIDs upfront so we can refer to them by name.
	ids := env.genUUIDs(t, 4)
	dirID := ids[0]
	childID := ids[1]
	dirSnapshotVID := ids[2]
	childSnapshotVID := ids[3]

	// Seed source with post-demo state: D ("e", a directory) and C
	// ("b.md", inside D). Seeding INSERTs fire the archive trigger and
	// write 'create' rows to history; that's fine, just background noise.
	if _, err := env.client.pool.Exec(env.ctx, fmt.Sprintf(
		`INSERT INTO %s (id, parent_id, filename, filetype, body, modified_at)
		 VALUES ($1, NULL, 'e', 'directory', '', now()),
		        ($2, $1, 'b.md', 'file', 'body b', now())`, env.srcQT),
		dirID, childID); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	// Manually insert the "pre-demo" snapshots. These are what
	// ExecuteUndoTransaction will UPSERT from -- the rows whose
	// version_ids appear in RestoreVersionIDs.
	insertSnapshot := func(versionID, fileID, parent, filename, filetype string) {
		var parentArg interface{}
		if parent != "" {
			parentArg = parent
		}
		if _, err := env.client.pool.Exec(env.ctx, fmt.Sprintf(
			`INSERT INTO %s (version_id, file_id, parent_id, filename, filetype, body, modified_at, operation)
			 VALUES ($1, $2, $3, $4, $5, '', now() - interval '1 hour', 'rename')`, env.histQT),
			versionID, fileID, parentArg, filename, filetype); err != nil {
			t.Fatalf("seed snapshot for %s: %v", filename, err)
		}
	}
	insertSnapshot(dirSnapshotVID, dirID, "", "d", "directory")
	insertSnapshot(childSnapshotVID, childID, dirID, "a.md", "file")

	// Call ExecuteUndoTransaction with parent-first iteration order
	// (D before C). This matches the production file_id ASC ordering
	// when dirs are created before their children, and is the order
	// that exposes the cascade-artifact bug.
	if err := env.client.ExecuteUndoTransaction(env.ctx, &UndoTransactionParams{
		Schema:            env.schema,
		SourceTable:       env.sourceTable,
		HistoryTable:      env.historyTable,
		LogTable:          env.logTable,
		Description:       "cascade-artifact regression",
		RestoreVersionIDs: []string{dirSnapshotVID, childSnapshotVID},
		RestoreFileIDs:    []string{dirID, childID},
		RestoreFilenames:  []string{"d", "a.md"},
		UserID:            "test-user",
	}); err != nil {
		t.Fatalf("ExecuteUndoTransaction failed: %v", err)
	}

	// Look up the dir's new type='undo' log entry and follow its
	// version_id to the underlying history row.
	var dirUndoVID string
	if err := env.client.pool.QueryRow(env.ctx, fmt.Sprintf(
		`SELECT version_id::text FROM %s
		 WHERE file_id = $1 AND type = 'undo'
		 ORDER BY log_id DESC LIMIT 1`, env.logQT),
		dirID).Scan(&dirUndoVID); err != nil {
		t.Fatalf("query dir undo log entry: %v", err)
	}

	var op, filename string
	if err := env.client.pool.QueryRow(env.ctx, fmt.Sprintf(
		`SELECT operation, filename FROM %s WHERE version_id = $1`, env.histQT),
		dirUndoVID).Scan(&op, &filename); err != nil {
		t.Fatalf("query history row %s: %v", dirUndoVID, err)
	}

	// The dir's undo entry must point at the row trigger A wrote during
	// D's own UPSERT -- {operation='rename', filename='e'}. If Step 3
	// reads "newest" post-hoc instead (the pre-Fix-1 behavior), this
	// resolves to the cascade artifact {operation='edit', filename='d'}.
	if op != "rename" {
		t.Errorf("dir undo entry version_id resolves to operation=%q, want %q "+
			"(pre-Fix-1 would be 'edit' -- the cascade artifact)", op, "rename")
	}
	if filename != "e" {
		t.Errorf("dir undo entry version_id resolves to filename=%q, want %q "+
			"(pre-Fix-1 would be 'd' -- the cascade-artifact captures D's post-iteration-1 state)",
			filename, "e")
	}

	// Sanity: child's undo entry should also resolve to its own rename
	// snapshot. The child is a leaf -- no cascade lands on it -- so this
	// is correct pre-Fix-1 too; the assertion just guards against
	// regressions in the leaf path.
	var childUndoVID string
	if err := env.client.pool.QueryRow(env.ctx, fmt.Sprintf(
		`SELECT version_id::text FROM %s
		 WHERE file_id = $1 AND type = 'undo'
		 ORDER BY log_id DESC LIMIT 1`, env.logQT),
		childID).Scan(&childUndoVID); err != nil {
		t.Fatalf("query child undo log entry: %v", err)
	}
	var childOp, childFilename string
	if err := env.client.pool.QueryRow(env.ctx, fmt.Sprintf(
		`SELECT operation, filename FROM %s WHERE version_id = $1`, env.histQT),
		childUndoVID).Scan(&childOp, &childFilename); err != nil {
		t.Fatalf("query child history row %s: %v", childUndoVID, err)
	}
	if childOp != "rename" || childFilename != "b.md" {
		t.Errorf("child undo entry version_id resolves to (op=%q, filename=%q), want (rename, b.md)",
			childOp, childFilename)
	}
}

// TestQueryUndoAffectedFiles_TopologicalOrder verifies that the affected-files
// query returns rows sorted child-first: deepest by source.parent_id chain
// comes first, with file_id ASC as a stable tiebreaker among rows at the
// same depth. Files whose source row doesn't exist (deleted from source --
// would be recreated by undo) default to depth 0 via the COALESCE and sort
// to the end among the depth-0 group.
//
// Scenario: workspace with a 4-deep dir tree A->B->C->leaf.md, a sibling
// S.md at root, and a "ghost" file whose log entry exists but whose source
// row was deleted (simulating an affected file that the undo will INSERT
// back from history). Total 6 affected files, expected order:
//  1. leaf.md   (depth 3)
//  2. C         (depth 2)
//  3. B         (depth 1)
//     4-6. [A, S.md, ghost] sorted by file_id ASC (all depth 0)
func TestQueryUndoAffectedFiles_TopologicalOrder(t *testing.T) {
	env, cleanup := setupUndoTestEnv(t)
	defer cleanup()

	ids := env.genUUIDs(t, 6)
	aID := ids[0]     // depth 0 dir
	bID := ids[1]     // depth 1 (in A)
	cID := ids[2]     // depth 2 (in B)
	leafID := ids[3]  // depth 3 (in C)
	sID := ids[4]     // depth 0 sibling
	ghostID := ids[5] // depth 0 default (no source row)

	// Seed source in dependency order (parent before child) to satisfy the
	// self-FK at insertion time. The "ghost" file_id is intentionally NOT
	// inserted into source -- the affected-files query should still include
	// it (it has a log entry) and sort it to the end with depth=0.
	if _, err := env.client.pool.Exec(env.ctx, fmt.Sprintf(
		`INSERT INTO %s (id, parent_id, filename, filetype, body, modified_at) VALUES
		 ($1, NULL, 'A',       'directory', '', now()),
		 ($2, NULL, 'S.md',    'file',      '', now()),
		 ($3, $1,   'B',       'directory', '', now()),
		 ($4, $3,   'C',       'directory', '', now()),
		 ($5, $4,   'leaf.md', 'file',      '', now())`, env.srcQT),
		aID, sID, bID, cID, leafID); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	// Seed log: one 'edit' entry per file (including the ghost). log_id
	// auto-generated via DEFAULT uuidv7(), so all are > afterID below.
	seedLog := func(fileID, filename string) {
		if _, err := env.client.pool.Exec(env.ctx, fmt.Sprintf(
			`INSERT INTO %s (file_id, type, user_id, filename, version_id)
			 VALUES ($1, 'edit', 'u', $2, NULL)`, env.logQT),
			fileID, filename); err != nil {
			t.Fatalf("seed log (%s): %v", filename, err)
		}
	}
	seedLog(aID, "A")
	seedLog(bID, "B")
	seedLog(cID, "C")
	seedLog(leafID, "leaf.md")
	seedLog(sID, "S.md")
	seedLog(ghostID, "ghost.md")

	// afterID is the all-zeros UUID -- smaller than any UUIDv7 the log
	// inserts above generated, so the WHERE log_id > afterID picks them all.
	afterID := "00000000-0000-0000-0000-000000000000"

	result, err := env.client.QueryUndoAffectedFiles(env.ctx,
		env.schema, env.logTable, env.sourceTable, env.historyTable, afterID, "", nil)
	if err != nil {
		t.Fatalf("QueryUndoAffectedFiles: %v", err)
	}

	if len(result) != 6 {
		t.Fatalf("expected 6 affected files, got %d: %+v", len(result), result)
	}

	// Expected per-file depths.
	depthOf := map[string]int{
		aID:     0,
		sID:     0,
		ghostID: 0,
		bID:     1,
		cID:     2,
		leafID:  3,
	}

	// 1. Result must be monotonically non-increasing in depth.
	for i := 0; i < len(result)-1; i++ {
		curDepth := depthOf[result[i].FileID]
		nextDepth := depthOf[result[i+1].FileID]
		if curDepth < nextDepth {
			t.Errorf("result[%d] (file=%s, depth=%d) sorts before result[%d] (file=%s, depth=%d) -- depth must be DESC",
				i, result[i].FileID, curDepth, i+1, result[i+1].FileID, nextDepth)
		}
	}

	// 2. The deepest single file (leaf.md, depth 3) must be first.
	if result[0].FileID != leafID {
		t.Errorf("result[0] should be leaf.md (depth 3), got file_id=%s", result[0].FileID)
	}

	// 3. C (depth 2) must be second; B (depth 1) third.
	if result[1].FileID != cID {
		t.Errorf("result[1] should be C (depth 2), got file_id=%s", result[1].FileID)
	}
	if result[2].FileID != bID {
		t.Errorf("result[2] should be B (depth 1), got file_id=%s", result[2].FileID)
	}

	// 4. The last three (depth 0) must be the depth-0 file_ids sorted ASC
	//    by the file_id tiebreaker.
	depth0Want := []string{aID, sID, ghostID}
	sortStringsAsc(depth0Want)
	depth0Got := []string{result[3].FileID, result[4].FileID, result[5].FileID}
	for i := 0; i < 3; i++ {
		if depth0Got[i] != depth0Want[i] {
			t.Errorf("depth-0 tiebreaker order[%d]: got %s, want %s (full got=%v, want=%v)",
				i, depth0Got[i], depth0Want[i], depth0Got, depth0Want)
		}
	}
}

// sortStringsAsc sorts a slice of strings in ascending order in place.
// Helper for TestQueryUndoAffectedFiles_TopologicalOrder tiebreaker check.
func sortStringsAsc(s []string) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}
