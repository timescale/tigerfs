package fuse

import (
	"sync"
	"testing"
)

// TestNewPartialRowTracker verifies that NewPartialRowTracker initializes correctly
func TestNewPartialRowTracker(t *testing.T) {
	tracker := NewPartialRowTracker(nil) // nil db is fine for non-commit operations

	if tracker == nil {
		t.Fatal("NewPartialRowTracker returned nil")
	}

	if tracker.rows == nil {
		t.Error("Expected rows map to be initialized, got nil")
	}

	if len(tracker.rows) != 0 {
		t.Errorf("Expected empty rows map, got %d entries", len(tracker.rows))
	}
}

// TestPartialRowTracker_GetOrCreate_New tests creating a new partial row
func TestPartialRowTracker_GetOrCreate_New(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	row := tracker.GetOrCreate("public", "users", "id", "123")

	if row == nil {
		t.Fatal("GetOrCreate returned nil")
	}

	// Verify row fields
	if row.Schema != "public" {
		t.Errorf("Expected Schema='public', got %q", row.Schema)
	}
	if row.Table != "users" {
		t.Errorf("Expected Table='users', got %q", row.Table)
	}
	if row.PkColumn != "id" {
		t.Errorf("Expected PkColumn='id', got %q", row.PkColumn)
	}
	if row.PkValue != "123" {
		t.Errorf("Expected PkValue='123', got %q", row.PkValue)
	}
	if row.Committed {
		t.Error("Expected Committed=false for new row")
	}
	if row.Columns == nil {
		t.Error("Expected Columns map to be initialized")
	}
	if len(row.Columns) != 0 {
		t.Errorf("Expected empty Columns map, got %d entries", len(row.Columns))
	}

	// Verify tracker now has the row
	if len(tracker.rows) != 1 {
		t.Errorf("Expected tracker to have 1 row, got %d", len(tracker.rows))
	}
}

// TestPartialRowTracker_GetOrCreate_Existing tests returning an existing partial row
func TestPartialRowTracker_GetOrCreate_Existing(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create first row
	row1 := tracker.GetOrCreate("public", "users", "id", "123")
	row1.Columns["email"] = "test@example.com"

	// Get the same row again
	row2 := tracker.GetOrCreate("public", "users", "id", "123")

	// Should be the exact same pointer
	if row1 != row2 {
		t.Error("GetOrCreate should return the same row instance")
	}

	// Verify the column data is preserved
	if row2.Columns["email"] != "test@example.com" {
		t.Errorf("Expected email='test@example.com', got %v", row2.Columns["email"])
	}

	// Verify tracker still has only 1 row
	if len(tracker.rows) != 1 {
		t.Errorf("Expected tracker to have 1 row, got %d", len(tracker.rows))
	}
}

// TestPartialRowTracker_GetOrCreate_DifferentKeys tests that different keys create different rows
func TestPartialRowTracker_GetOrCreate_DifferentKeys(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create rows with different schemas
	row1 := tracker.GetOrCreate("schema1", "users", "id", "123")
	row2 := tracker.GetOrCreate("schema2", "users", "id", "123")

	if row1 == row2 {
		t.Error("Different schemas should create different rows")
	}

	// Create rows with different tables
	row3 := tracker.GetOrCreate("public", "users", "id", "123")
	row4 := tracker.GetOrCreate("public", "orders", "id", "123")

	if row3 == row4 {
		t.Error("Different tables should create different rows")
	}

	// Create rows with different PK values
	row5 := tracker.GetOrCreate("public", "users", "id", "100")
	row6 := tracker.GetOrCreate("public", "users", "id", "200")

	if row5 == row6 {
		t.Error("Different PK values should create different rows")
	}

	// Verify tracker has all unique rows
	if len(tracker.rows) != 6 {
		t.Errorf("Expected tracker to have 6 rows, got %d", len(tracker.rows))
	}
}

// TestPartialRowTracker_Get_Existing tests retrieving an existing row
func TestPartialRowTracker_Get_Existing(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create a row first
	created := tracker.GetOrCreate("public", "users", "id", "123")
	created.Columns["name"] = "John"

	// Retrieve it
	retrieved := tracker.Get("public", "users", "123")

	if retrieved == nil {
		t.Fatal("Get returned nil for existing row")
	}

	// Should be the same pointer
	if created != retrieved {
		t.Error("Get should return the same row instance")
	}

	if retrieved.Columns["name"] != "John" {
		t.Errorf("Expected name='John', got %v", retrieved.Columns["name"])
	}
}

// TestPartialRowTracker_Get_NotFound tests retrieving a non-existent row
func TestPartialRowTracker_Get_NotFound(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Try to retrieve non-existent row
	row := tracker.Get("public", "users", "999")

	if row != nil {
		t.Errorf("Expected nil for non-existent row, got %+v", row)
	}
}

// TestPartialRowTracker_Get_WrongSchema tests that Get is schema-specific
func TestPartialRowTracker_Get_WrongSchema(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create a row in one schema
	tracker.GetOrCreate("schema1", "users", "id", "123")

	// Try to retrieve from different schema
	row := tracker.Get("schema2", "users", "123")

	if row != nil {
		t.Error("Expected nil when querying wrong schema")
	}
}

// TestPartialRowTracker_SetColumn_NewRow tests SetColumn creating a new row
func TestPartialRowTracker_SetColumn_NewRow(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// SetColumn should create the row if it doesn't exist
	tracker.SetColumn("public", "users", "id", "123", "email", "test@example.com")

	// Verify row was created
	row := tracker.Get("public", "users", "123")
	if row == nil {
		t.Fatal("SetColumn did not create row")
	}

	if row.Schema != "public" {
		t.Errorf("Expected Schema='public', got %q", row.Schema)
	}
	if row.Table != "users" {
		t.Errorf("Expected Table='users', got %q", row.Table)
	}
	if row.PkColumn != "id" {
		t.Errorf("Expected PkColumn='id', got %q", row.PkColumn)
	}
	if row.PkValue != "123" {
		t.Errorf("Expected PkValue='123', got %q", row.PkValue)
	}
	if row.Columns["email"] != "test@example.com" {
		t.Errorf("Expected email='test@example.com', got %v", row.Columns["email"])
	}
}

// TestPartialRowTracker_SetColumn_ExistingRow tests SetColumn updating an existing row
func TestPartialRowTracker_SetColumn_ExistingRow(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create row first
	tracker.GetOrCreate("public", "users", "id", "123")

	// Set column
	tracker.SetColumn("public", "users", "id", "123", "email", "test@example.com")

	row := tracker.Get("public", "users", "123")
	if row.Columns["email"] != "test@example.com" {
		t.Errorf("Expected email='test@example.com', got %v", row.Columns["email"])
	}
}

// TestPartialRowTracker_SetColumn_Multiple tests setting multiple columns
func TestPartialRowTracker_SetColumn_Multiple(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Set multiple columns on the same row
	tracker.SetColumn("public", "users", "id", "123", "email", "test@example.com")
	tracker.SetColumn("public", "users", "id", "123", "name", "John Doe")
	tracker.SetColumn("public", "users", "id", "123", "age", 30)

	row := tracker.Get("public", "users", "123")
	if row == nil {
		t.Fatal("Row not found")
	}

	if len(row.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(row.Columns))
	}

	if row.Columns["email"] != "test@example.com" {
		t.Errorf("Expected email='test@example.com', got %v", row.Columns["email"])
	}
	if row.Columns["name"] != "John Doe" {
		t.Errorf("Expected name='John Doe', got %v", row.Columns["name"])
	}
	if row.Columns["age"] != 30 {
		t.Errorf("Expected age=30, got %v", row.Columns["age"])
	}
}

// TestPartialRowTracker_SetColumn_Overwrite tests overwriting a column value
func TestPartialRowTracker_SetColumn_Overwrite(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Set column
	tracker.SetColumn("public", "users", "id", "123", "email", "old@example.com")

	// Overwrite
	tracker.SetColumn("public", "users", "id", "123", "email", "new@example.com")

	row := tracker.Get("public", "users", "123")
	if row.Columns["email"] != "new@example.com" {
		t.Errorf("Expected email='new@example.com', got %v", row.Columns["email"])
	}
}

// TestPartialRowTracker_SetColumn_NullValue tests setting a column to nil
func TestPartialRowTracker_SetColumn_NullValue(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Set column to nil (represents NULL)
	tracker.SetColumn("public", "users", "id", "123", "optional_field", nil)

	row := tracker.Get("public", "users", "123")
	if _, exists := row.Columns["optional_field"]; !exists {
		t.Error("Expected optional_field to exist in Columns")
	}
	if row.Columns["optional_field"] != nil {
		t.Errorf("Expected optional_field=nil, got %v", row.Columns["optional_field"])
	}
}

// TestPartialRowTracker_Remove tests removing a partial row
func TestPartialRowTracker_Remove(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create a row
	tracker.GetOrCreate("public", "users", "id", "123")

	// Verify it exists
	if tracker.Get("public", "users", "123") == nil {
		t.Fatal("Row should exist before removal")
	}

	// Remove it
	tracker.Remove("public", "users", "123")

	// Verify it's gone
	if tracker.Get("public", "users", "123") != nil {
		t.Error("Row should be nil after removal")
	}

	// Verify tracker is empty
	if len(tracker.rows) != 0 {
		t.Errorf("Expected empty tracker, got %d rows", len(tracker.rows))
	}
}

// TestPartialRowTracker_Remove_NonExistent tests removing a non-existent row (should be no-op)
func TestPartialRowTracker_Remove_NonExistent(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Remove non-existent row (should not panic)
	tracker.Remove("public", "users", "999")

	// Tracker should still be empty
	if len(tracker.rows) != 0 {
		t.Errorf("Expected empty tracker, got %d rows", len(tracker.rows))
	}
}

// TestPartialRowTracker_Remove_OnlyTargetRow tests that Remove only affects the target row
func TestPartialRowTracker_Remove_OnlyTargetRow(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create multiple rows
	tracker.GetOrCreate("public", "users", "id", "100")
	tracker.GetOrCreate("public", "users", "id", "200")
	tracker.GetOrCreate("public", "users", "id", "300")

	// Remove middle row
	tracker.Remove("public", "users", "200")

	// Verify other rows still exist
	if tracker.Get("public", "users", "100") == nil {
		t.Error("Row 100 should still exist")
	}
	if tracker.Get("public", "users", "200") != nil {
		t.Error("Row 200 should be removed")
	}
	if tracker.Get("public", "users", "300") == nil {
		t.Error("Row 300 should still exist")
	}

	if len(tracker.rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(tracker.rows))
	}
}

// TestPartialRowTracker_ConcurrentAccess tests thread-safety of the tracker
func TestPartialRowTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup

	// Concurrent GetOrCreate
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				tracker.GetOrCreate("public", "users", "id", "shared-key")
			}
		}(i)
	}

	wg.Wait()

	// Should have exactly one row for the shared key
	row := tracker.Get("public", "users", "shared-key")
	if row == nil {
		t.Fatal("Expected row to exist after concurrent GetOrCreate")
	}
}

// TestPartialRowTracker_ConcurrentSetColumn tests concurrent column updates
func TestPartialRowTracker_ConcurrentSetColumn(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	const numGoroutines = 50

	var wg sync.WaitGroup

	// Concurrent SetColumn on same row
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each goroutine sets a different column
			colName := string(rune('A' + (id % 26)))
			tracker.SetColumn("public", "users", "id", "123", colName, id)
		}(i)
	}

	wg.Wait()

	row := tracker.Get("public", "users", "123")
	if row == nil {
		t.Fatal("Expected row to exist after concurrent SetColumn")
	}

	// Should have multiple columns set
	if len(row.Columns) == 0 {
		t.Error("Expected columns to be set")
	}
}

// TestPartialRowTracker_ConcurrentGetAndSet tests concurrent Get and SetColumn operations
func TestPartialRowTracker_ConcurrentGetAndSet(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Pre-create a row
	tracker.GetOrCreate("public", "users", "id", "123")

	const numGoroutines = 50
	var wg sync.WaitGroup

	// Mix of Get and SetColumn
	for i := 0; i < numGoroutines; i++ {
		wg.Add(2)

		// Reader
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = tracker.Get("public", "users", "123")
			}
		}()

		// Writer
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tracker.SetColumn("public", "users", "id", "123", "counter", j)
			}
		}(i)
	}

	wg.Wait()

	row := tracker.Get("public", "users", "123")
	if row == nil {
		t.Fatal("Row should still exist after concurrent access")
	}
}

// TestPartialRowTracker_ConcurrentCreateAndRemove tests concurrent creation and removal
func TestPartialRowTracker_ConcurrentCreateAndRemove(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	const numGoroutines = 20
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('0' + (id % 10)))
			for j := 0; j < 100; j++ {
				tracker.GetOrCreate("public", "users", "id", key)
				if j%2 == 0 {
					tracker.Remove("public", "users", key)
				}
			}
		}(i)
	}

	wg.Wait()

	// No specific assertions - just verify no panics or deadlocks
}

// TestPartialRow_Committed tests the Committed flag behavior
func TestPartialRow_Committed(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	row := tracker.GetOrCreate("public", "users", "id", "123")

	// Initially not committed
	if row.Committed {
		t.Error("New row should not be committed")
	}

	// Manually set committed (simulating what TryCommit does)
	row.Committed = true

	// Retrieve and verify
	retrieved := tracker.Get("public", "users", "123")
	if !retrieved.Committed {
		t.Error("Committed flag should persist")
	}
}

// TestPartialRowTracker_KeyFormat tests the internal key format
func TestPartialRowTracker_KeyFormat(t *testing.T) {
	tracker := NewPartialRowTracker(nil)

	// Create rows with special characters in PK values
	testCases := []struct {
		schema  string
		table   string
		pkValue string
	}{
		{"public", "users", "simple"},
		{"my_schema", "my_table", "with.dots"},
		{"schema", "table", "with spaces"},
		{"SCHEMA", "TABLE", "UPPERCASE"},
		{"schema", "table", "123"},
		{"schema", "table", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.pkValue, func(t *testing.T) {
			row := tracker.GetOrCreate(tc.schema, tc.table, "id", tc.pkValue)
			if row == nil {
				t.Fatal("GetOrCreate returned nil")
			}

			// Verify we can retrieve it
			retrieved := tracker.Get(tc.schema, tc.table, tc.pkValue)
			if retrieved != row {
				t.Error("Get returned different row than GetOrCreate")
			}
		})
	}
}

// Note: TryCommit tests require database integration testing
// See test/integration/crud_test.go for TryCommit integration tests
// TryCommit depends on:
// - db.Client.GetColumns() for checking NOT NULL constraints
// - db.ValidateConstraints() for constraint validation
// - db.Client.InsertRow() for executing the INSERT
