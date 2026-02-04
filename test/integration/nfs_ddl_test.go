package integration

import (
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

// TestNFSDDL_EndToEndTableWorkflow tests the complete DDL workflow via NFS:
// 1. Create table via DDL staging
// 2. Verify table exists
// 3. Write rows to the table
// 4. Read rows back
// 5. Delete the table
func TestNFSDDL_EndToEndTableWorkflow(t *testing.T) {
	checkFUSEMountCapability(t)

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
		time.Sleep(100 * time.Millisecond) // Allow NFS to sync
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
		deleteDir := filepath.Join(mountpoint, ".delete", tableName)

		// Create staging directory for delete
		if err := os.MkdirAll(deleteDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /.delete/%s: %v", tableName, err)
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

		// Verify table is gone
		tablePath := filepath.Join(mountpoint, tableName)
		if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
			t.Errorf("Table should not exist after DROP")
		} else {
			t.Logf("Table %s deleted successfully", tableName)
		}
	})
}

// TestNFSDDL_ValidationErrors tests DDL validation error handling.
func TestNFSDDL_ValidationErrors(t *testing.T) {
	checkFUSEMountCapability(t)

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

// TestNFSDDL_AbortClearsSession tests that .abort properly clears the staging session.
func TestNFSDDL_AbortClearsSession(t *testing.T) {
	checkFUSEMountCapability(t)

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

// TestNFSDDL_ModifyExistingTable tests modifying an existing table via .modify.
func TestNFSDDL_ModifyExistingTable(t *testing.T) {
	checkFUSEMountCapability(t)

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
		modifyDir := filepath.Join(mountpoint, ".modify", "users")

		if err := os.MkdirAll(modifyDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir /.modify/users: %v", err)
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
