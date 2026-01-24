package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
)

// rowToMap converts a Row struct to a map for easier access
func rowToMap(row *db.Row) map[string]interface{} {
	result := make(map[string]interface{})
	for i, col := range row.Columns {
		result[col] = row.Values[i]
	}
	return result
}

// getTestConnectionString returns a PostgreSQL connection string for testing
func getTestConnectionString(t *testing.T) string {
	t.Helper()

	// Check for explicit test connection string
	if connStr := os.Getenv("TEST_DATABASE_URL"); connStr != "" {
		return connStr
	}

	// Build from PG environment variables
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("PGPORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("PGUSER")
	if user == "" {
		user = os.Getenv("USER")
	}

	database := os.Getenv("PGDATABASE")
	if database == "" {
		database = "postgres"
	}

	password := os.Getenv("PGPASSWORD")
	if password != "" {
		return "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + database
	}

	return "postgres://" + user + "@" + host + ":" + port + "/" + database
}

// setupTestTable creates a test table and returns cleanup function
func setupTestTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) func() {
	t.Helper()

	// Create test table
	_, err := pool.Exec(ctx, `
		CREATE TEMP TABLE test_users (
			id SERIAL PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			age INTEGER,
			bio TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Return cleanup function
	return func() {
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS test_users")
	}
}

// TestCRUDFullCycle tests the complete INSERT → SELECT → UPDATE → DELETE cycle
func TestCRUDFullCycle(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	// 1. INSERT - Create a new row
	t.Run("Insert", func(t *testing.T) {
		columns := []string{"email", "name", "age", "bio"}
		values := []interface{}{"alice@example.com", "Alice Smith", 30, "Software engineer"}

		pkValue, err := db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}

		if pkValue == "" {
			t.Error("Expected non-empty primary key value")
		}

		t.Logf("Inserted row with PK: %s", pkValue)
	})

	// 2. SELECT - Read the row back
	t.Run("Select", func(t *testing.T) {
		row, err := db.GetRow(ctx, pool, "pg_temp", "test_users", "id", "1")
		if err != nil {
			t.Fatalf("Failed to get row: %v", err)
		}

		rowMap := rowToMap(row)

		if rowMap["email"] != "alice@example.com" {
			t.Errorf("Expected email='alice@example.com', got %v", rowMap["email"])
		}

		if rowMap["name"] != "Alice Smith" {
			t.Errorf("Expected name='Alice Smith', got %v", rowMap["name"])
		}

		// Test NULL handling - bio should not be NULL
		if rowMap["bio"] == nil {
			t.Error("Expected bio to have value, got NULL")
		}
	})

	// 3. UPDATE - Modify column values
	t.Run("Update", func(t *testing.T) {
		err := db.UpdateColumn(ctx, pool, "pg_temp", "test_users", "id", "1", "age", "31")
		if err != nil {
			t.Fatalf("Failed to update column: %v", err)
		}

		// Verify update
		value, err := db.GetColumn(ctx, pool, "pg_temp", "test_users", "id", "1", "age")
		if err != nil {
			t.Fatalf("Failed to get updated column: %v", err)
		}

		// Convert to string for comparison
		strValue, err := format.ConvertValueToText(value)
		if err != nil {
			t.Fatalf("Failed to convert value: %v", err)
		}

		if strValue != "31" {
			t.Errorf("Expected age=31, got %s", strValue)
		}
	})

	// 4. UPDATE to NULL - Set nullable column to NULL
	t.Run("UpdateToNull", func(t *testing.T) {
		err := db.UpdateColumn(ctx, pool, "pg_temp", "test_users", "id", "1", "bio", "")
		if err != nil {
			t.Fatalf("Failed to update column to NULL: %v", err)
		}

		// Verify NULL
		value, err := db.GetColumn(ctx, pool, "pg_temp", "test_users", "id", "1", "bio")
		if err != nil {
			t.Fatalf("Failed to get column: %v", err)
		}

		if value != nil {
			t.Errorf("Expected NULL, got %v", value)
		}
	})

	// 5. DELETE - Remove the row
	t.Run("Delete", func(t *testing.T) {
		err := db.DeleteRow(ctx, pool, "pg_temp", "test_users", "id", "1")
		if err != nil {
			t.Fatalf("Failed to delete row: %v", err)
		}

		// Verify deletion
		_, err = db.GetRow(ctx, pool, "pg_temp", "test_users", "id", "1")
		if err == nil {
			t.Error("Expected error when getting deleted row")
		}
	})
}

// TestCRUDPartialUpdates tests updating only specific columns
func TestCRUDPartialUpdates(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	// Insert initial row
	columns := []string{"email", "name"}
	values := []interface{}{"bob@example.com", "Bob Jones"}
	pkValue, err := db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Test partial update - only update one column
	t.Run("UpdateSingleColumn", func(t *testing.T) {
		err := db.UpdateColumn(ctx, pool, "pg_temp", "test_users", "id", pkValue, "age", "25")
		if err != nil {
			t.Fatalf("Failed to update column: %v", err)
		}

		// Verify other columns unchanged
		row, err := db.GetRow(ctx, pool, "pg_temp", "test_users", "id", pkValue)
		if err != nil {
			t.Fatalf("Failed to get row: %v", err)
		}

		rowMap := rowToMap(row)

		if rowMap["email"] != "bob@example.com" {
			t.Errorf("Email should be unchanged, got %v", rowMap["email"])
		}

		if rowMap["name"] != "Bob Jones" {
			t.Errorf("Name should be unchanged, got %v", rowMap["name"])
		}
	})
}

// TestCRUDNullHandling tests NULL value operations
func TestCRUDNullHandling(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	// Test inserting with NULL values
	t.Run("InsertWithNull", func(t *testing.T) {
		columns := []string{"email", "name", "age", "bio"}
		values := []interface{}{"charlie@example.com", "Charlie Brown", nil, nil}

		pkValue, err := db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
		if err != nil {
			t.Fatalf("Failed to insert row with NULL: %v", err)
		}

		// Read back and verify NULLs
		row, err := db.GetRow(ctx, pool, "pg_temp", "test_users", "id", pkValue)
		if err != nil {
			t.Fatalf("Failed to get row: %v", err)
		}

		rowMap := rowToMap(row)

		if rowMap["age"] != nil {
			t.Errorf("Expected age=NULL, got %v", rowMap["age"])
		}

		if rowMap["bio"] != nil {
			t.Errorf("Expected bio=NULL, got %v", rowMap["bio"])
		}
	})

	// Test reading NULL as empty string
	t.Run("ReadNullAsEmpty", func(t *testing.T) {
		value, err := db.GetColumn(ctx, pool, "pg_temp", "test_users", "id", "1", "age")
		if err != nil {
			t.Fatalf("Failed to get column: %v", err)
		}

		// NULL should be returned as nil
		if value != nil {
			t.Errorf("Expected NULL (nil), got %v", value)
		}

		// Convert to text - should be empty string
		text, err := format.ConvertValueToText(value)
		if err != nil {
			t.Fatalf("Failed to convert NULL: %v", err)
		}

		if text != "" {
			t.Errorf("Expected empty string for NULL, got %q", text)
		}
	})
}

// TestCRUDConstraintViolations tests error scenarios
func TestCRUDConstraintViolations(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	// Insert test row
	columns := []string{"email", "name"}
	values := []interface{}{"test@example.com", "Test User"}
	_, err = db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Test UNIQUE constraint violation
	t.Run("UniqueViolation", func(t *testing.T) {
		columns := []string{"email", "name"}
		values := []interface{}{"test@example.com", "Another User"}

		_, err := db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
		if err == nil {
			t.Error("Expected error for UNIQUE constraint violation")
		}
	})

	// Test NOT NULL constraint violation via ValidateConstraints
	t.Run("NotNullViolation", func(t *testing.T) {
		valuesMap := map[string]interface{}{
			"email": "", // Empty string = NULL, should violate NOT NULL
			"name":  "User",
		}

		err := db.ValidateConstraints(ctx, pool, "pg_temp", "test_users", valuesMap)
		if err == nil {
			t.Error("Expected error for NOT NULL constraint violation")
		}
	})
}

// TestCRUDConcurrentOperations tests concurrent access
func TestCRUDConcurrentOperations(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	// Test concurrent inserts
	t.Run("ConcurrentInserts", func(t *testing.T) {
		const numGoroutines = 5

		errors := make(chan error, numGoroutines)
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				columns := []string{"email", "name"}
				values := []interface{}{
					"user" + string(rune('a'+id)) + "@example.com",
					"User " + string(rune('A'+id)),
				}

				_, err := db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
				if err != nil {
					errors <- err
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent insert failed: %v", err)
		}

		// Verify all rows inserted
		count, err := db.GetRowCount(ctx, pool, "pg_temp", "test_users")
		if err != nil {
			t.Fatalf("Failed to get row count: %v", err)
		}

		if count != int64(numGoroutines) {
			t.Errorf("Expected %d rows, got %d", numGoroutines, count)
		}
	})
}

// TestCRUDMultipleRows tests operations on multiple rows
func TestCRUDMultipleRows(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	// Insert multiple rows
	t.Run("InsertMultiple", func(t *testing.T) {
		for i := 1; i <= 5; i++ {
			columns := []string{"email", "name", "age"}
			values := []interface{}{
				"user" + string(rune('0'+i)) + "@example.com",
				"User " + string(rune('0'+i)),
				20 + i,
			}

			_, err := db.InsertRow(ctx, pool, "pg_temp", "test_users", columns, values)
			if err != nil {
				t.Fatalf("Failed to insert row %d: %v", i, err)
			}
		}

		// Verify count
		count, err := db.GetRowCount(ctx, pool, "pg_temp", "test_users")
		if err != nil {
			t.Fatalf("Failed to get row count: %v", err)
		}

		if count != 5 {
			t.Errorf("Expected 5 rows, got %d", count)
		}
	})

	// List rows
	t.Run("ListRows", func(t *testing.T) {
		rows, err := db.ListRows(ctx, pool, "pg_temp", "test_users", "id", 100)
		if err != nil {
			t.Fatalf("Failed to list rows: %v", err)
		}

		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}

		// Verify they're in order
		for i, row := range rows {
			expected := string(rune('1' + i))
			if row != expected {
				t.Errorf("Row %d: expected %s, got %s", i, expected, row)
			}
		}
	})
}
