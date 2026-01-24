package db

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row")
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
	row, err := client.GetRow(ctx, "public", "test_get_row", "id", "1")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_notfound")
	}()

	// Try to get non-existent row
	_, err = client.GetRow(ctx, "public", "test_get_row_notfound", "id", "999")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_null")
	}()

	// Insert row with NULL email
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_row_null (name, email) VALUES ('Alice', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Get row
	row, err := client.GetRow(ctx, "public", "test_get_row_null", "id", "1")
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

	_, err := client.GetRow(ctx, "public", "test_table", "id", "1")
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
	defer client.Close()

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
	value, err := client.GetColumn(ctx, "public", "test_get_column", "id", "1", "name")
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
	value, err = client.GetColumn(ctx, "public", "test_get_column", "id", "1", "age")
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
	value, err = client.GetColumn(ctx, "public", "test_get_column", "id", "2", "email")
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
	defer client.Close()

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
	_, err = client.GetColumn(ctx, "public", "test_column_nonexistent", "id", "999", "name")
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
	defer client.Close()

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
	_, err = client.GetColumn(ctx, "public", "test_column_invalid", "id", "1", "nonexistent_column")
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

	_, err := client.GetColumn(ctx, "public", "test_table", "id", "1", "name")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_column")
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
	err = client.UpdateColumn(ctx, "public", "test_update_column", "id", "1", "email", "newemail@example.com")
	if err != nil {
		t.Fatalf("UpdateColumn() failed: %v", err)
	}

	// Verify update
	value, err := client.GetColumn(ctx, "public", "test_update_column", "id", "1", "email")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_null")
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
	err = client.UpdateColumn(ctx, "public", "test_update_null", "id", "1", "email", "")
	if err != nil {
		t.Fatalf("UpdateColumn() failed: %v", err)
	}

	// Verify email is now NULL
	value, err := client.GetColumn(ctx, "public", "test_update_null", "id", "1", "email")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_notfound")
	}()

	// Try to update non-existent row
	err = client.UpdateColumn(ctx, "public", "test_update_notfound", "id", "999", "name", "New Name")
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

	err := client.UpdateColumn(ctx, "public", "test_table", "id", "1", "name", "New Name")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_explicit")
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
	row, err := client.GetRow(ctx, "public", "test_insert_explicit", "id", "100")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_serial")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_null")
	}()

	// Insert row with NULL values
	columns := []string{"name", "email", "age"}
	values := []interface{}{"Dana", nil, nil}

	pkValue, err := client.InsertRow(ctx, "public", "test_insert_null", columns, values)
	if err != nil {
		t.Fatalf("InsertRow() failed: %v", err)
	}

	// Verify NULL values
	row, err := client.GetRow(ctx, "public", "test_insert_null", "id", pkValue)
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_insert_dup")
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
	defer client.Close()

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
	defer client.Close()

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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_partial")
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

	err = client.UpdateRow(ctx, "public", "test_update_partial", "id", "1", columns, values)
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify only email changed
	row, err := client.GetRow(ctx, "public", "test_update_partial", "id", "1")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_all")
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

	err = client.UpdateRow(ctx, "public", "test_update_all", "id", "1", columns, values)
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify all columns changed
	row, err := client.GetRow(ctx, "public", "test_update_all", "id", "1")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_tonull")
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

	err = client.UpdateRow(ctx, "public", "test_update_tonull", "id", "1", columns, values)
	if err != nil {
		t.Fatalf("UpdateRow() failed: %v", err)
	}

	// Verify email is NULL
	value, err := client.GetColumn(ctx, "public", "test_update_tonull", "id", "1", "email")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_update_notfound")
	}()

	// Try to update non-existent row
	columns := []string{"name"}
	values := []interface{}{"New Name"}

	err = client.UpdateRow(ctx, "public", "test_update_notfound", "id", "999", columns, values)
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
	defer client.Close()

	// Try to update with no columns
	err = client.UpdateRow(ctx, "public", "test_table", "id", "1", []string{}, []interface{}{})
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

	err := client.UpdateRow(ctx, "public", "test_table", "id", "1", []string{"name"}, []interface{}{"Alice"})
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_simple")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_delete_simple (id, name) VALUES (1, 'Alice'), (2, 'Bob')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Delete row with id=1
	err = client.DeleteRow(ctx, "public", "test_delete_simple", "id", "1")
	if err != nil {
		t.Fatalf("DeleteRow() failed: %v", err)
	}

	// Verify row is deleted
	_, err = client.GetRow(ctx, "public", "test_delete_simple", "id", "1")
	if err == nil {
		t.Fatal("Expected error for deleted row, got nil")
	}

	// Verify other row still exists
	row, err := client.GetRow(ctx, "public", "test_delete_simple", "id", "2")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_notfound")
	}()

	// Try to delete non-existent row
	err = client.DeleteRow(ctx, "public", "test_delete_notfound", "id", "999")
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
	defer client.Close()

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
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_delete_multi")
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
		err = client.DeleteRow(ctx, "public", "test_delete_multi", "id", id)
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

func TestClient_DeleteRow_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	err := client.DeleteRow(ctx, "public", "test_table", "id", "1")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
