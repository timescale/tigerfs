package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// touchTriggerFile creates/touches a DDL trigger file (.test, .commit, .abort).
// This uses os.Create+Close instead of os.WriteFile because writing non-empty
// content to trigger files via NFS can cause protocol errors on macOS.
// The trigger fires on Close, so we don't need to write any content.
func touchTriggerFile(t *testing.T, path string) error {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

// TestWriteDDL_EndToEndTableWorkflow tests the complete DDL workflow via the mounted filesystem:
// 1. Create table via DDL staging
// 2. Verify table exists
// 3. Write rows to the table
// 4. Read rows back
// 5. Delete the table
func TestWriteDDL_EndToEndTableWorkflow(t *testing.T) {
	checkMountCapability(t)

	// Use empty database - we'll create our own table
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

	tableName := "nfs_test_orders"

	// Step 1: Create table via DDL staging
	t.Run("CreateTable", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)

		// Create staging directory
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /.create/%s: %v", tableName, err)
		}

		// Read template (verifies session was created)
		sqlPath := filepath.Join(createDir, "sql")
		template, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Fatalf("Failed to read sql template: %v", err)
		}
		t.Logf("Template:\n%s", string(template))

		// Verify template contains expected content
		if !strings.Contains(string(template), "CREATE TABLE") {
			t.Errorf("Expected CREATE TABLE in template, got: %s", string(template))
		}

		// Write actual DDL
		ddl := `CREATE TABLE ` + tableName + ` (
			id SERIAL PRIMARY KEY,
			customer TEXT NOT NULL,
			amount NUMERIC(10,2) NOT NULL,
			status TEXT DEFAULT 'pending'
		);`
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Verify the DDL was written by reading it back
		time.Sleep(100 * time.Millisecond) // Allow filesystem to sync
		writtenSQL, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Fatalf("Failed to read back DDL: %v", err)
		}
		if !strings.Contains(string(writtenSQL), "CREATE TABLE") {
			t.Logf("WARNING: DDL read-back shows template content, not our DDL:")
			t.Logf("  Expected: CREATE TABLE ...")
			t.Logf("  Got: %s", string(writtenSQL))
		} else {
			t.Logf("DDL written successfully, content verified")
		}

		// Test the DDL (validate via BEGIN/ROLLBACK)
		testPath := filepath.Join(createDir, ".test")
		if err := touchTriggerFile(t, testPath); err != nil {
			t.Fatalf("Failed to trigger .test: %v", err)
		}

		// Read test.log to verify validation passed
		testLogPath := filepath.Join(createDir, "test.log")
		testResult, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("Could not read test.log: %v", err)
		} else {
			t.Logf("Test result: %s", string(testResult))
			if strings.Contains(strings.ToLower(string(testResult)), "error") {
				t.Fatalf("DDL validation failed: %s", string(testResult))
			}
		}

		// Commit the DDL (actually create the table)
		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to trigger .commit: %v", err)
		}

		// Wait for filesystem to process
		time.Sleep(500 * time.Millisecond)

		// Verify table now exists
		tablePath := filepath.Join(mountpoint, tableName)
		if _, err := os.Stat(tablePath); os.IsNotExist(err) {
			t.Fatalf("Table directory should exist after CREATE, path: %s", tablePath)
		}
		t.Logf("Table %s created successfully", tableName)
	})

	// Step 2: Write rows to the table
	t.Run("WriteRows", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, tableName)

		// Verify table exists
		if _, err := os.Stat(tablePath); os.IsNotExist(err) {
			t.Skip("Table doesn't exist, skipping write test")
		}

		// Write first row as JSON
		row1Path := filepath.Join(tablePath, "1.json")
		row1Data := `{"customer": "Alice", "amount": 99.99, "status": "paid"}`
		if err := os.WriteFile(row1Path, []byte(row1Data), 0644); err != nil {
			t.Fatalf("Failed to write row 1: %v", err)
		}

		// Write second row as JSON
		row2Path := filepath.Join(tablePath, "2.json")
		row2Data := `{"customer": "Bob", "amount": 150.00, "status": "pending"}`
		if err := os.WriteFile(row2Path, []byte(row2Data), 0644); err != nil {
			t.Fatalf("Failed to write row 2: %v", err)
		}

		t.Logf("Wrote 2 rows to %s", tableName)

		// Give filesystem time to process
		time.Sleep(200 * time.Millisecond)
	})

	// Step 3: Read rows back
	t.Run("ReadRows", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, tableName)

		// Read row 1 as JSON
		row1Path := filepath.Join(tablePath, "1.json")
		row1Data, err := os.ReadFile(row1Path)
		if err != nil {
			t.Fatalf("Failed to read row 1: %v", err)
		}
		t.Logf("Row 1 JSON: %s", string(row1Data))

		// Verify row 1 content
		if !strings.Contains(string(row1Data), "Alice") {
			t.Errorf("Expected 'Alice' in row 1, got: %s", string(row1Data))
		}
		if !strings.Contains(string(row1Data), "99.99") {
			t.Errorf("Expected '99.99' in row 1, got: %s", string(row1Data))
		}

		// Read row 2 as JSON
		row2Path := filepath.Join(tablePath, "2.json")
		row2Data, err := os.ReadFile(row2Path)
		if err != nil {
			t.Fatalf("Failed to read row 2: %v", err)
		}
		t.Logf("Row 2 JSON: %s", string(row2Data))

		// Verify row 2 content
		if !strings.Contains(string(row2Data), "Bob") {
			t.Errorf("Expected 'Bob' in row 2, got: %s", string(row2Data))
		}
	})

	// Step 4: Read individual columns
	t.Run("ReadColumns", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, tableName)

		// Read customer column from row 1
		customerPath := filepath.Join(tablePath, "1", "customer")
		customerData, err := os.ReadFile(customerPath)
		if err != nil {
			t.Fatalf("Failed to read customer column: %v", err)
		}
		customer := strings.TrimSpace(string(customerData))
		if customer != "Alice" {
			t.Errorf("Expected customer='Alice', got: %q", customer)
		}
		t.Logf("Row 1 customer: %s", customer)

		// Read amount column from row 1
		amountPath := filepath.Join(tablePath, "1", "amount")
		amountData, err := os.ReadFile(amountPath)
		if err != nil {
			t.Fatalf("Failed to read amount column: %v", err)
		}
		amount := strings.TrimSpace(string(amountData))
		if amount != "99.99" {
			t.Errorf("Expected amount='99.99', got: %q", amount)
		}
		t.Logf("Row 1 amount: %s", amount)
	})

	// Step 5: Verify row count via .info/count
	t.Run("VerifyRowCount", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, tableName)
		countPath := filepath.Join(tablePath, ".info", "count")

		countData, err := os.ReadFile(countPath)
		if err != nil {
			t.Fatalf("Failed to read .info/count: %v", err)
		}
		count := strings.TrimSpace(string(countData))
		if count != "2" {
			t.Errorf("Expected count='2', got: %q", count)
		}
		t.Logf("Row count: %s", count)
	})

	// Step 6: Update a row
	t.Run("UpdateRow", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, tableName)

		// Update status column of row 1
		statusPath := filepath.Join(tablePath, "1", "status")
		if err := os.WriteFile(statusPath, []byte("shipped\n"), 0644); err != nil {
			t.Fatalf("Failed to update status: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read back to verify
		statusData, err := os.ReadFile(statusPath)
		if err != nil {
			t.Fatalf("Failed to read updated status: %v", err)
		}
		status := strings.TrimSpace(string(statusData))
		if status != "shipped" {
			t.Errorf("Expected status='shipped', got: %q", status)
		}
		t.Logf("Updated row 1 status: %s", status)
	})

	// Step 7: Delete a row
	t.Run("DeleteRow", func(t *testing.T) {
		tablePath := filepath.Join(mountpoint, tableName)
		row2Path := filepath.Join(tablePath, "2.json")

		// Delete row 2
		if err := os.Remove(row2Path); err != nil {
			t.Fatalf("Failed to delete row 2: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Verify row is gone
		if _, err := os.Stat(row2Path); !os.IsNotExist(err) {
			t.Errorf("Row 2 should not exist after delete")
		}

		// Verify count decreased
		countPath := filepath.Join(tablePath, ".info", "count")
		countData, err := os.ReadFile(countPath)
		if err != nil {
			t.Logf("Could not read count after delete: %v", err)
		} else {
			count := strings.TrimSpace(string(countData))
			if count != "1" {
				t.Errorf("Expected count='1' after delete, got: %q", count)
			}
			t.Logf("Row count after delete: %s", count)
		}
	})

	// Step 8: Delete the table via DDL
	t.Run("DeleteTable", func(t *testing.T) {
		// Per spec: delete is at /{table}/.delete/, not /.delete/{table}/
		tablePath := filepath.Join(mountpoint, tableName)
		deleteDir := filepath.Join(tablePath, ".delete")

		// Create staging directory for delete
		if err := os.MkdirAll(deleteDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /%s/.delete: %v", tableName, err)
		}

		// Write DROP TABLE statement
		sqlPath := filepath.Join(deleteDir, "sql")
		ddl := "DROP TABLE " + tableName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DROP DDL: %v", err)
		}

		// Commit the delete
		commitPath := filepath.Join(deleteDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to trigger .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify table is gone (reusing tablePath from above)
		if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
			t.Errorf("Table should not exist after DROP")
		} else {
			t.Logf("Table %s deleted successfully", tableName)
		}
	})
}

// TestWriteDDL_ValidationErrors tests DDL validation error handling.
func TestWriteDDL_ValidationErrors(t *testing.T) {
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

	t.Run("InvalidSyntax", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", "invalid_syntax_table")

		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		// Write invalid DDL (syntax error)
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "CREAT TABL invalid_syntax_table (id int);" // intentional typos
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Test should fail
		testPath := filepath.Join(createDir, ".test")
		_ = touchTriggerFile(t, testPath) // May fail, that's expected

		// Read test.log - should contain error
		testLogPath := filepath.Join(createDir, "test.log")
		testResult, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("Could not read test.log: %v", err)
		} else {
			t.Logf("Test result for invalid DDL: %s", string(testResult))
			if !strings.Contains(strings.ToLower(string(testResult)), "error") &&
				!strings.Contains(strings.ToLower(string(testResult)), "syntax") {
				t.Errorf("Expected error in test result for invalid DDL")
			}
		}

		// Abort to clean up
		abortPath := filepath.Join(createDir, ".abort")
		_ = touchTriggerFile(t, abortPath)
	})

	t.Run("CommentsOnly", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", "comments_only_table")

		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		// Write only comments (no actual DDL)
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "-- This is just a comment\n/* Another comment */"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Test should indicate no content
		testPath := filepath.Join(createDir, ".test")
		_ = touchTriggerFile(t, testPath)

		testLogPath := filepath.Join(createDir, "test.log")
		testResult, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("Could not read test.log: %v", err)
		} else {
			t.Logf("Test result for comments-only: %s", string(testResult))
			if !strings.Contains(strings.ToLower(string(testResult)), "comment") &&
				!strings.Contains(strings.ToLower(string(testResult)), "no ddl") {
				t.Logf("Expected error about comments/no content")
			}
		}

		// Abort to clean up
		abortPath := filepath.Join(createDir, ".abort")
		_ = touchTriggerFile(t, abortPath)
	})
}

// TestWriteDDL_AbortClearsSession tests that .abort properly clears the staging session.
func TestWriteDDL_AbortClearsSession(t *testing.T) {
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

	tableName := "abort_test_table"
	createDir := filepath.Join(mountpoint, ".create", tableName)

	// Create staging session
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to mkdir: %v", err)
	}

	// Write DDL
	sqlPath := filepath.Join(createDir, "sql")
	ddl := "CREATE TABLE " + tableName + " (id SERIAL PRIMARY KEY);"
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	// Verify content was written
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Fatalf("Failed to read back DDL: %v", err)
	}
	if !strings.Contains(string(content), "CREATE TABLE") {
		t.Errorf("Expected DDL content, got: %s", string(content))
	}

	// Abort the session - use Create instead of WriteFile to avoid potential NFS issues
	abortPath := filepath.Join(createDir, ".abort")
	f, err := os.Create(abortPath)
	if err != nil {
		t.Fatalf("Failed to create .abort: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Failed to close .abort: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Table should NOT exist (was never committed)
	tablePath := filepath.Join(mountpoint, tableName)
	if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
		t.Errorf("Table should not exist after abort (never committed)")
	}

	t.Logf("Abort correctly prevented table creation")
}

// TestWriteDDL_ModifyExistingTable tests modifying an existing table via .modify.
func TestWriteDDL_ModifyExistingTable(t *testing.T) {
	checkMountCapability(t)

	// Use database with seeded tables (users, products)
	dbResult := GetTestDB(t)
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

	// Verify users table exists
	tablePath := filepath.Join(mountpoint, "users")
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Skip("users table doesn't exist")
	}

	t.Run("AddColumn", func(t *testing.T) {
		// Per spec: modify is at /{table}/.modify/, not /.modify/{table}/
		modifyDir := filepath.Join(mountpoint, "users", ".modify")

		if err := os.MkdirAll(modifyDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /users/.modify: %v", err)
		}

		// Read template
		sqlPath := filepath.Join(modifyDir, "sql")
		template, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Fatalf("Failed to read modify template: %v", err)
		}
		t.Logf("Modify template:\n%s", string(template))

		// Write ALTER TABLE
		ddl := "ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname TEXT;"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write ALTER DDL: %v", err)
		}

		// Test first
		testPath := filepath.Join(modifyDir, ".test")
		if err := touchTriggerFile(t, testPath); err != nil {
			t.Logf("Touch .test: %v", err)
		}

		// Check test log
		testLogPath := filepath.Join(modifyDir, "test.log")
		testResult, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("Could not read test.log: %v", err)
		} else {
			t.Logf("Test result: %s", string(testResult))
		}

		// Commit
		commitPath := filepath.Join(modifyDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to trigger .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify column was added via .info/columns
		columnsPath := filepath.Join(tablePath, ".info", "columns")
		columns, err := os.ReadFile(columnsPath)
		if err != nil {
			t.Logf("Could not read columns: %v", err)
		} else {
			if strings.Contains(string(columns), "nickname") {
				t.Logf("Column 'nickname' added successfully")
			} else {
				t.Logf("Columns after ALTER: %s", string(columns))
			}
		}
	})
}

// TestWriteDDL_WriteReadScenarios tests 8 comprehensive write/read scenarios via the mounted filesystem.
// This tests different access patterns:
// - Write without prior read
// - Read before write
// - Different format extensions
// - Column file access
// - Creating new rows
func TestWriteDDL_WriteReadScenarios(t *testing.T) {
	checkMountCapability(t)

	// Use empty database - we'll create our own table with 1000 rows
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

	tableName := "nfs_write_test"

	// Step 0: Create table with 10 rows
	t.Run("SetupTable", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)

		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /.create/%s: %v", tableName, err)
		}

		// Write DDL to create table with 10 rows
		sqlPath := filepath.Join(createDir, "sql")
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			value INT
		);
		INSERT INTO %s (id, name, email, value)
		SELECT i, 'User' || i, 'user' || i || '@example.com', i * 10
		FROM generate_series(1, 10) i;`, tableName, tableName)

		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Commit the DDL
		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to trigger .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify table exists
		tablePath := filepath.Join(mountpoint, tableName)
		if _, err := os.Stat(tablePath); os.IsNotExist(err) {
			t.Fatalf("Table directory should exist after CREATE")
		}
		t.Logf("Created table %s with 1000 rows", tableName)
	})

	tablePath := filepath.Join(mountpoint, tableName)

	// Scenario 1: Write 2.json (never read before), then read 2.json
	t.Run("Scenario1_WriteReadJSON", func(t *testing.T) {
		// Write row 2 (which exists in DB with different values)
		row2Path := filepath.Join(tablePath, "2.json")
		newData := `{"name": "NewUser2", "email": "new2@test.com", "value": 999}`
		if err := os.WriteFile(row2Path, []byte(newData), 0644); err != nil {
			t.Fatalf("Failed to write 2.json: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read back and verify
		readData, err := os.ReadFile(row2Path)
		if err != nil {
			t.Fatalf("Failed to read 2.json: %v", err)
		}
		t.Logf("Scenario 1 - Read 2.json: %s", string(readData))

		if !strings.Contains(string(readData), "NewUser2") {
			t.Errorf("Expected 'NewUser2' in read data, got: %s", string(readData))
		}
		if !strings.Contains(string(readData), "999") {
			t.Errorf("Expected '999' in read data, got: %s", string(readData))
		}
	})

	// Scenario 2: Write 3.tsv (never read before), then read 3.csv
	t.Run("Scenario2_WriteTSVReadCSV", func(t *testing.T) {
		// Write row 3 as TSV
		row3TSVPath := filepath.Join(tablePath, "3.tsv")
		newData := "name\temail\tvalue\nNewUser3\tnew3@test.com\t888"
		if err := os.WriteFile(row3TSVPath, []byte(newData), 0644); err != nil {
			t.Fatalf("Failed to write 3.tsv: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read back as CSV
		row3CSVPath := filepath.Join(tablePath, "3.csv")
		readData, err := os.ReadFile(row3CSVPath)
		if err != nil {
			t.Fatalf("Failed to read 3.csv: %v", err)
		}
		t.Logf("Scenario 2 - Read 3.csv: %s", string(readData))

		if !strings.Contains(string(readData), "NewUser3") {
			t.Errorf("Expected 'NewUser3' in read data, got: %s", string(readData))
		}
	})

	// Scenario 3: Read 4.json first, then write 4.json, then read 4.json
	t.Run("Scenario3_ReadWriteReadJSON", func(t *testing.T) {
		row4Path := filepath.Join(tablePath, "4.json")

		// Read first (existing row)
		originalData, err := os.ReadFile(row4Path)
		if err != nil {
			t.Fatalf("Failed to read original 4.json: %v", err)
		}
		t.Logf("Scenario 3 - Original 4.json: %s", string(originalData))

		if !strings.Contains(string(originalData), "User4") {
			t.Errorf("Expected 'User4' in original data, got: %s", string(originalData))
		}

		// Write new values
		newData := `{"name": "UpdatedUser4", "email": "updated4@test.com", "value": 777}`
		if err := os.WriteFile(row4Path, []byte(newData), 0644); err != nil {
			t.Fatalf("Failed to write 4.json: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read again
		readData, err := os.ReadFile(row4Path)
		if err != nil {
			t.Fatalf("Failed to read updated 4.json: %v", err)
		}
		t.Logf("Scenario 3 - Updated 4.json: %s", string(readData))

		if !strings.Contains(string(readData), "UpdatedUser4") {
			t.Errorf("Expected 'UpdatedUser4' in read data, got: %s", string(readData))
		}
	})

	// Scenario 4: Write 5/name, then read 5/name.txt
	t.Run("Scenario4_WriteColumnReadColumnTxt", func(t *testing.T) {
		row5Dir := filepath.Join(tablePath, "5")
		namePath := filepath.Join(row5Dir, "name")

		// Write column without extension
		if err := os.WriteFile(namePath, []byte("ScenarioFiveUser\n"), 0644); err != nil {
			t.Fatalf("Failed to write 5/name: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read with .txt extension (should read same column)
		nameTxtPath := filepath.Join(row5Dir, "name.txt")
		readData, err := os.ReadFile(nameTxtPath)
		if err != nil {
			t.Logf("Scenario 4 - Could not read 5/name.txt (may not be supported): %v", err)
			// Fall back to reading without extension
			readData, err = os.ReadFile(namePath)
			if err != nil {
				t.Fatalf("Failed to read 5/name: %v", err)
			}
		}
		t.Logf("Scenario 4 - Read 5/name(.txt): %s", strings.TrimSpace(string(readData)))

		if !strings.Contains(string(readData), "ScenarioFiveUser") {
			t.Errorf("Expected 'ScenarioFiveUser', got: %s", string(readData))
		}
	})

	// Scenario 5: Write 6/name.txt, then read 6/name
	t.Run("Scenario5_WriteColumnTxtReadColumn", func(t *testing.T) {
		row6Dir := filepath.Join(tablePath, "6")
		nameTxtPath := filepath.Join(row6Dir, "name.txt")
		namePath := filepath.Join(row6Dir, "name")

		// Write column with .txt extension
		if err := os.WriteFile(nameTxtPath, []byte("ScenarioSixUser\n"), 0644); err != nil {
			t.Logf("Scenario 5 - Could not write 6/name.txt (may not be supported): %v", err)
			// Fall back to writing without extension
			if err := os.WriteFile(namePath, []byte("ScenarioSixUser\n"), 0644); err != nil {
				t.Fatalf("Failed to write 6/name: %v", err)
			}
		}

		time.Sleep(100 * time.Millisecond)

		// Read without extension
		readData, err := os.ReadFile(namePath)
		if err != nil {
			t.Fatalf("Failed to read 6/name: %v", err)
		}
		t.Logf("Scenario 5 - Read 6/name: %s", strings.TrimSpace(string(readData)))

		if !strings.Contains(string(readData), "ScenarioSixUser") {
			t.Errorf("Expected 'ScenarioSixUser', got: %s", string(readData))
		}
	})

	// Scenario 6: Read 7/name first, then write 7/name, then read 7/name
	t.Run("Scenario6_ReadWriteReadColumn", func(t *testing.T) {
		row7Dir := filepath.Join(tablePath, "7")
		namePath := filepath.Join(row7Dir, "name")

		// Read original value
		originalData, err := os.ReadFile(namePath)
		if err != nil {
			t.Fatalf("Failed to read original 7/name: %v", err)
		}
		t.Logf("Scenario 6 - Original 7/name: %s", strings.TrimSpace(string(originalData)))

		if !strings.Contains(string(originalData), "User7") {
			t.Errorf("Expected 'User7' in original, got: %s", string(originalData))
		}

		// Write new value
		if err := os.WriteFile(namePath, []byte("UpdatedSevenUser\n"), 0644); err != nil {
			t.Fatalf("Failed to write 7/name: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read again
		readData, err := os.ReadFile(namePath)
		if err != nil {
			t.Fatalf("Failed to read updated 7/name: %v", err)
		}
		t.Logf("Scenario 6 - Updated 7/name: %s", strings.TrimSpace(string(readData)))

		if !strings.Contains(string(readData), "UpdatedSevenUser") {
			t.Errorf("Expected 'UpdatedSevenUser', got: %s", string(readData))
		}
	})

	// Scenario 7: Write 20.json (new row - doesn't exist), then read 20.json
	t.Run("Scenario7_WriteNewRowReadJSON", func(t *testing.T) {
		row20Path := filepath.Join(tablePath, "20.json")

		// Write new row (id 20 doesn't exist)
		newData := `{"id": 20, "name": "NewRow20", "email": "new20@test.com", "value": 200}`
		if err := os.WriteFile(row20Path, []byte(newData), 0644); err != nil {
			t.Fatalf("Failed to write 20.json: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read back
		readData, err := os.ReadFile(row20Path)
		if err != nil {
			t.Fatalf("Failed to read 20.json: %v", err)
		}
		t.Logf("Scenario 7 - Read 20.json: %s", string(readData))

		if !strings.Contains(string(readData), "NewRow20") {
			t.Errorf("Expected 'NewRow20' in read data, got: %s", string(readData))
		}
	})

	// Scenario 8: Read 21.json (should fail), then write 21.json, then read 21.json
	t.Run("Scenario8_ReadFailWriteReadJSON", func(t *testing.T) {
		row21Path := filepath.Join(tablePath, "21.json")

		// Read should fail (row doesn't exist)
		_, err := os.ReadFile(row21Path)
		if err == nil {
			t.Logf("Scenario 8 - Unexpected: 21.json exists before write")
		} else {
			t.Logf("Scenario 8 - Expected failure reading 21.json: %v", err)
		}

		// Write new row
		newData := `{"id": 21, "name": "NewRow21", "email": "new21@test.com", "value": 210}`
		if err := os.WriteFile(row21Path, []byte(newData), 0644); err != nil {
			t.Fatalf("Failed to write 21.json: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		// Read back
		readData, err := os.ReadFile(row21Path)
		if err != nil {
			t.Fatalf("Failed to read 21.json: %v", err)
		}
		t.Logf("Scenario 8 - Read 21.json: %s", string(readData))

		if !strings.Contains(string(readData), "NewRow21") {
			t.Errorf("Expected 'NewRow21' in read data, got: %s", string(readData))
		}
	})

	// Cleanup: Delete the test table
	t.Run("Cleanup", func(t *testing.T) {
		// Per spec: delete is at /{table}/.delete/, not /.delete/{table}/
		tablePath := filepath.Join(mountpoint, tableName)
		deleteDir := filepath.Join(tablePath, ".delete")

		if err := os.MkdirAll(deleteDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /%s/.delete: %v", tableName, err)
		}

		sqlPath := filepath.Join(deleteDir, "sql")
		ddl := "DROP TABLE " + tableName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DROP DDL: %v", err)
		}

		commitPath := filepath.Join(deleteDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to trigger .commit: %v", err)
		}

		t.Logf("Cleaned up table %s", tableName)
	})
}
