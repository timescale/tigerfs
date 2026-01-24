package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestValidateConstraints_NotNull(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to database
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Create test table
	_, err = pool.Exec(ctx, `
		CREATE TEMP TABLE test_constraints (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT NOT NULL,
			bio TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test 1: Valid values (all NOT NULL satisfied)
	values := map[string]interface{}{
		"email": "test@example.com",
		"name":  "Test User",
		"bio":   "Bio text",
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_constraints", values)
	if err != nil {
		t.Errorf("ValidateConstraints failed with valid values: %v", err)
	}

	// Test 2: Missing NOT NULL column (email)
	values = map[string]interface{}{
		"name": "Test User",
		"bio":  "Bio text",
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_constraints", values)
	if err == nil {
		t.Error("Expected NOT NULL constraint violation for missing email")
	}

	// Test 3: NULL value for NOT NULL column
	values = map[string]interface{}{
		"email": nil,
		"name":  "Test User",
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_constraints", values)
	if err == nil {
		t.Error("Expected NOT NULL constraint violation for NULL email")
	}

	// Test 4: Empty string for NOT NULL column
	values = map[string]interface{}{
		"email": "",
		"name":  "Test User",
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_constraints", values)
	if err == nil {
		t.Error("Expected NOT NULL constraint violation for empty email")
	}

	// Test 5: Nullable column can be NULL
	values = map[string]interface{}{
		"email": "test@example.com",
		"name":  "Test User",
		"bio":   nil,
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_constraints", values)
	if err != nil {
		t.Errorf("ValidateConstraints failed with NULL nullable column: %v", err)
	}
}

func TestValidateConstraints_Unique(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to database
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Create test table with unique constraint
	_, err = pool.Exec(ctx, `
		CREATE TEMP TABLE test_unique (
			id SERIAL PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			username TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert a test row
	_, err = pool.Exec(ctx, `INSERT INTO test_unique (email, username) VALUES ('existing@example.com', 'existing')`)
	if err != nil {
		t.Fatalf("Failed to insert test row: %v", err)
	}

	// Test 1: Unique value (should pass)
	values := map[string]interface{}{
		"email":    "new@example.com",
		"username": "newuser",
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_unique", values)
	if err != nil {
		t.Errorf("ValidateConstraints failed with unique value: %v", err)
	}

	// Test 2: Duplicate value (should fail)
	values = map[string]interface{}{
		"email":    "existing@example.com",
		"username": "newuser",
	}
	err = ValidateConstraints(ctx, pool, "pg_temp", "test_unique", values)
	if err == nil {
		t.Error("Expected UNIQUE constraint violation for duplicate email")
	}
}

func TestGetColumnsForConstraintCheck(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to database
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Create test table
	_, err = pool.Exec(ctx, `
		CREATE TEMP TABLE test_columns (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL,
			age INTEGER DEFAULT 0,
			bio TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Get columns
	columns, err := getColumnsForConstraintCheck(ctx, pool, "pg_temp", "test_columns")
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	if len(columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(columns))
	}

	// Check specific columns
	var emailCol, bioCol, ageCol *Column
	for i := range columns {
		switch columns[i].Name {
		case "email":
			emailCol = &columns[i]
		case "bio":
			bioCol = &columns[i]
		case "age":
			ageCol = &columns[i]
		}
	}

	if emailCol == nil {
		t.Fatal("email column not found")
	}
	if emailCol.IsNullable {
		t.Error("email should be NOT NULL")
	}

	if bioCol == nil {
		t.Fatal("bio column not found")
	}
	if !bioCol.IsNullable {
		t.Error("bio should be nullable")
	}

	if ageCol == nil {
		t.Fatal("age column not found")
	}
	if ageCol.Default == "" {
		t.Error("age should have a default value")
	}
}

func TestGetUniqueConstraints(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to database
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Create test table with unique constraints
	_, err = pool.Exec(ctx, `
		CREATE TEMP TABLE test_unique_constraints (
			id SERIAL PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			username TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Get unique constraints
	constraints, err := getUniqueConstraints(ctx, pool, "pg_temp", "test_unique_constraints")
	if err != nil {
		t.Fatalf("Failed to get unique constraints: %v", err)
	}

	// Should have at least 2 constraints (PRIMARY KEY + UNIQUE on email)
	if len(constraints) < 2 {
		t.Errorf("Expected at least 2 constraints, got %d", len(constraints))
	}

	// Check for email unique constraint
	foundEmailUnique := false
	for _, c := range constraints {
		if len(c.Columns) == 1 && c.Columns[0] == "email" {
			foundEmailUnique = true
			break
		}
	}

	if !foundEmailUnique {
		t.Error("Expected to find UNIQUE constraint on email column")
	}
}
