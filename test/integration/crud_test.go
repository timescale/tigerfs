package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
)

// schemaFromDB returns the current_schema() for the given connection,
// which reflects the test schema set via search_path.
func schemaFromDB(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatalf("Failed to get current_schema: %v", err)
	}
	return schema
}

// rowToMap converts a Row struct to a map for easier access
func rowToMap(row *db.Row) map[string]interface{} {
	result := make(map[string]interface{})
	for i, col := range row.Columns {
		result[col] = row.Values[i]
	}
	return result
}

// setupTestTable creates a test table and returns the table name and cleanup function
// Uses a regular table (not temp) because:
// 1. information_schema doesn't work with temp tables (pg_temp vs pg_temp_N)
// 2. Temp tables are connection-specific, breaking concurrent tests
func setupTestTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, func()) {
	t.Helper()

	// Generate unique table name for this test
	tableName := fmt.Sprintf("test_users_%d", time.Now().UnixNano())

	// Create test table
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			age INTEGER,
			bio TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`, tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Return table name and cleanup function
	return tableName, func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}
}

// TestCRUDFullCycle tests the complete INSERT → SELECT → UPDATE → DELETE cycle
func TestCRUDFullCycle(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	tableName, cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	schema := schemaFromDB(t, ctx, pool)

	// 1. INSERT - Create a new row
	t.Run("Insert", func(t *testing.T) {
		columns := []string{"email", "name", "age", "bio"}
		values := []interface{}{"alice@example.com", "Alice Smith", 30, "Software engineer"}

		pkValue, err := db.InsertRow(ctx, pool, schema, tableName, columns, values)
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
		row, err := db.GetRow(ctx, pool, schema, tableName, "id", "1")
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
		err := db.UpdateColumn(ctx, pool, schema, tableName, "id", "1", "age", "31")
		if err != nil {
			t.Fatalf("Failed to update column: %v", err)
		}

		// Verify update
		value, err := db.GetColumn(ctx, pool, schema, tableName, "id", "1", "age")
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
		err := db.UpdateColumn(ctx, pool, schema, tableName, "id", "1", "bio", "")
		if err != nil {
			t.Fatalf("Failed to update column to NULL: %v", err)
		}

		// Verify NULL
		value, err := db.GetColumn(ctx, pool, schema, tableName, "id", "1", "bio")
		if err != nil {
			t.Fatalf("Failed to get column: %v", err)
		}

		if value != nil {
			t.Errorf("Expected NULL, got %v", value)
		}
	})

	// 5. DELETE - Remove the row
	t.Run("Delete", func(t *testing.T) {
		err := db.DeleteRow(ctx, pool, schema, tableName, "id", "1")
		if err != nil {
			t.Fatalf("Failed to delete row: %v", err)
		}

		// Verify deletion
		_, err = db.GetRow(ctx, pool, schema, tableName, "id", "1")
		if err == nil {
			t.Error("Expected error when getting deleted row")
		}
	})
}

// TestCRUDPartialUpdates tests updating only specific columns
func TestCRUDPartialUpdates(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	tableName, cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	schema := schemaFromDB(t, ctx, pool)

	// Insert initial row
	columns := []string{"email", "name"}
	values := []interface{}{"bob@example.com", "Bob Jones"}
	pkValue, err := db.InsertRow(ctx, pool, schema, tableName, columns, values)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Test partial update - only update one column
	t.Run("UpdateSingleColumn", func(t *testing.T) {
		err := db.UpdateColumn(ctx, pool, schema, tableName, "id", pkValue, "age", "25")
		if err != nil {
			t.Fatalf("Failed to update column: %v", err)
		}

		// Verify other columns unchanged
		row, err := db.GetRow(ctx, pool, schema, tableName, "id", pkValue)
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
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	tableName, cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	schema := schemaFromDB(t, ctx, pool)

	// Test inserting with NULL values
	t.Run("InsertWithNull", func(t *testing.T) {
		columns := []string{"email", "name", "age", "bio"}
		values := []interface{}{"charlie@example.com", "Charlie Brown", nil, nil}

		pkValue, err := db.InsertRow(ctx, pool, schema, tableName, columns, values)
		if err != nil {
			t.Fatalf("Failed to insert row with NULL: %v", err)
		}

		// Read back and verify NULLs
		row, err := db.GetRow(ctx, pool, schema, tableName, "id", pkValue)
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
		value, err := db.GetColumn(ctx, pool, schema, tableName, "id", "1", "age")
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
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	tableName, cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	schema := schemaFromDB(t, ctx, pool)

	// Insert test row
	columns := []string{"email", "name"}
	values := []interface{}{"test@example.com", "Test User"}
	_, err = db.InsertRow(ctx, pool, schema, tableName, columns, values)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Test UNIQUE constraint violation
	t.Run("UniqueViolation", func(t *testing.T) {
		columns := []string{"email", "name"}
		values := []interface{}{"test@example.com", "Another User"}

		_, err := db.InsertRow(ctx, pool, schema, tableName, columns, values)
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

		err := db.ValidateConstraints(ctx, pool, schema, tableName, valuesMap)
		if err == nil {
			t.Error("Expected error for NOT NULL constraint violation")
		}
	})
}

// TestCRUDConcurrentOperations tests concurrent access
func TestCRUDConcurrentOperations(t *testing.T) {
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	tableName, cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	schema := schemaFromDB(t, ctx, pool)

	// Test concurrent inserts
	t.Run("ConcurrentInserts", func(t *testing.T) {
		const numGoroutines = 5

		errors := make(chan error, numGoroutines)
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				columns := []string{"email", "name"}
				values := []interface{}{
					fmt.Sprintf("user%d@example.com", id),
					fmt.Sprintf("User %d", id),
				}

				_, err := db.InsertRow(ctx, pool, schema, tableName, columns, values)
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
		count, err := db.GetRowCount(ctx, pool, schema, tableName)
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
	// Get test database (tries local first, falls back to Docker)
	dbResult := GetTestDB(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbResult.ConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	tableName, cleanup := setupTestTable(t, ctx, pool)
	defer cleanup()

	schema := schemaFromDB(t, ctx, pool)

	// Insert multiple rows
	t.Run("InsertMultiple", func(t *testing.T) {
		for i := 1; i <= 5; i++ {
			columns := []string{"email", "name", "age"}
			values := []interface{}{
				fmt.Sprintf("user%d@example.com", i),
				fmt.Sprintf("User %d", i),
				20 + i,
			}

			_, err := db.InsertRow(ctx, pool, schema, tableName, columns, values)
			if err != nil {
				t.Fatalf("Failed to insert row %d: %v", i, err)
			}
		}

		// Verify count
		count, err := db.GetRowCount(ctx, pool, schema, tableName)
		if err != nil {
			t.Fatalf("Failed to get row count: %v", err)
		}

		if count != 5 {
			t.Errorf("Expected 5 rows, got %d", count)
		}
	})

	// List rows
	t.Run("ListRows", func(t *testing.T) {
		rows, err := db.ListRows(ctx, pool, schema, tableName, "id", 100)
		if err != nil {
			t.Fatalf("Failed to list rows: %v", err)
		}

		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}

		// Verify they're in order
		for i, row := range rows {
			expected := fmt.Sprintf("%d", i+1)
			if row != expected {
				t.Errorf("Row %d: expected %s, got %s", i, expected, row)
			}
		}
	})
}
