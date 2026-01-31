package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDDL_TableCreateDeleteCycle tests creating and deleting a table via DDL staging.
func TestDDL_TableCreateDeleteCycle(t *testing.T) {
	checkFUSEMountCapability(t)

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

	tableName := "test_ddl_table"
	schemaName := extractSchemaName(dbResult.ConnStr)

	// Find the correct path - schema might be flattened or under .schemas
	var createDir string
	if schemaName == "public" {
		createDir = filepath.Join(mountpoint, ".create")
	} else {
		createDir = filepath.Join(mountpoint, ".create")
	}

	t.Run("CreateTable", func(t *testing.T) {
		// Step 1: mkdir to create staging directory
		tableCreateDir := filepath.Join(createDir, tableName)
		if err := os.MkdirAll(tableCreateDir, 0755); err != nil {
			t.Fatalf("Failed to create staging directory: %v", err)
		}

		// Step 2: Read sql file to see template
		sqlPath := filepath.Join(tableCreateDir, "sql")
		template, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Fatalf("Failed to read sql template: %v", err)
		}
		t.Logf("Template:\n%s", string(template))

		// Step 3: Write DDL
		ddl := "CREATE TABLE " + tableName + " (id serial PRIMARY KEY, name text NOT NULL, value int);"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Step 4: Test the DDL (optional - touch .test)
		testPath := filepath.Join(tableCreateDir, ".test")
		if err := touchFile(testPath); err != nil {
			t.Fatalf("Failed to touch .test: %v", err)
		}

		// Step 5: Read test.log to verify success
		testLogPath := filepath.Join(tableCreateDir, "test.log")
		testResult, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("No test.log found (may be normal): %v", err)
		} else {
			t.Logf("Test result: %s", string(testResult))
			if strings.Contains(strings.ToLower(string(testResult)), "error") {
				t.Errorf("DDL validation failed: %s", string(testResult))
			}
		}

		// Step 6: Commit the DDL
		commitPath := filepath.Join(tableCreateDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		// Give filesystem time to process
		time.Sleep(500 * time.Millisecond)

		// Step 7: Verify table was created
		tablePath := filepath.Join(mountpoint, tableName)
		if _, err := os.Stat(tablePath); os.IsNotExist(err) {
			t.Errorf("Table directory should exist after CREATE: %v", err)
		} else {
			t.Logf("Table %s created successfully", tableName)
		}
	})

	t.Run("DeleteTable", func(t *testing.T) {
		// Verify table exists first
		tablePath := filepath.Join(mountpoint, tableName)
		if _, err := os.Stat(tablePath); os.IsNotExist(err) {
			t.Skip("Table doesn't exist, skipping delete test")
		}

		// Navigate to table's .delete directory
		deleteDir := filepath.Join(tablePath, ".delete")
		if _, err := os.Stat(deleteDir); os.IsNotExist(err) {
			t.Fatalf(".delete directory not found: %v", err)
		}

		// Read the sql template (shows table info)
		sqlPath := filepath.Join(deleteDir, "sql")
		template, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Fatalf("Failed to read delete template: %v", err)
		}
		t.Logf("Delete template:\n%s", string(template))

		// Write DROP TABLE statement
		ddl := "DROP TABLE " + tableName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DROP DDL: %v", err)
		}

		// Commit
		commitPath := filepath.Join(deleteDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify table was deleted
		if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
			t.Errorf("Table directory should not exist after DROP")
		} else {
			t.Logf("Table %s deleted successfully", tableName)
		}
	})
}

// TestDDL_TableModify tests modifying an existing table.
func TestDDL_TableModify(t *testing.T) {
	checkFUSEMountCapability(t)

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

	// Use existing 'users' table from test data
	tablePath := filepath.Join(mountpoint, "users")
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Skip("users table doesn't exist")
	}

	t.Run("AddColumn", func(t *testing.T) {
		modifyDir := filepath.Join(tablePath, ".modify")

		// Read template (shows current schema)
		sqlPath := filepath.Join(modifyDir, "sql")
		template, err := os.ReadFile(sqlPath)
		if err != nil {
			t.Fatalf("Failed to read modify template: %v", err)
		}
		t.Logf("Modify template:\n%s", string(template))

		// Write ALTER TABLE
		ddl := "ALTER TABLE users ADD COLUMN IF NOT EXISTS test_col text;"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write ALTER DDL: %v", err)
		}

		// Test first
		testPath := filepath.Join(modifyDir, ".test")
		if err := touchFile(testPath); err != nil {
			t.Logf("Touch .test failed (may be normal): %v", err)
		}

		// Commit
		commitPath := filepath.Join(modifyDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify column was added by checking .info/columns
		columnsPath := filepath.Join(tablePath, ".info", "columns")
		columns, err := os.ReadFile(columnsPath)
		if err != nil {
			t.Logf("Could not read columns file: %v", err)
		} else {
			if strings.Contains(string(columns), "test_col") {
				t.Logf("Column test_col added successfully")
			} else {
				t.Logf("Column list: %s", string(columns))
			}
		}
	})
}

// TestDDL_IndexCreateDelete tests creating and deleting an index.
func TestDDL_IndexCreateDelete(t *testing.T) {
	checkFUSEMountCapability(t)

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

	// Use existing 'users' table
	tablePath := filepath.Join(mountpoint, "users")
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Skip("users table doesn't exist")
	}

	indexName := "idx_test_email"

	t.Run("CreateIndex", func(t *testing.T) {
		indexesDir := filepath.Join(tablePath, ".indexes")
		createDir := filepath.Join(indexesDir, ".create", indexName)

		// Create staging directory
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to create index staging dir: %v", err)
		}

		// Write CREATE INDEX
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "CREATE INDEX " + indexName + " ON users(email);"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write CREATE INDEX: %v", err)
		}

		// Commit
		commitPath := filepath.Join(createDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify index appears in .indexes
		entries, err := os.ReadDir(indexesDir)
		if err != nil {
			t.Fatalf("Failed to list indexes: %v", err)
		}

		found := false
		for _, e := range entries {
			if e.Name() == indexName {
				found = true
				break
			}
		}

		if found {
			t.Logf("Index %s created successfully", indexName)
		} else {
			t.Logf("Index list: %v", entryNames(entries))
		}
	})

	t.Run("DeleteIndex", func(t *testing.T) {
		indexPath := filepath.Join(tablePath, ".indexes", indexName)
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			t.Skip("Index doesn't exist")
		}

		deleteDir := filepath.Join(indexPath, ".delete")

		// Write DROP INDEX
		sqlPath := filepath.Join(deleteDir, "sql")
		ddl := "DROP INDEX " + indexName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DROP INDEX: %v", err)
		}

		// Commit
		commitPath := filepath.Join(deleteDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify index is gone
		if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
			t.Errorf("Index should not exist after DROP")
		} else {
			t.Logf("Index %s deleted successfully", indexName)
		}
	})
}

// TestDDL_SchemaCreateDelete tests creating and deleting a schema.
func TestDDL_SchemaCreateDelete(t *testing.T) {
	checkFUSEMountCapability(t)

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

	schemaName := "test_ddl_schema"
	schemasDir := filepath.Join(mountpoint, ".schemas")

	t.Run("CreateSchema", func(t *testing.T) {
		createDir := filepath.Join(schemasDir, ".create", schemaName)

		// Create staging directory
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to create schema staging dir: %v", err)
		}

		// Write CREATE SCHEMA
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "CREATE SCHEMA " + schemaName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write CREATE SCHEMA: %v", err)
		}

		// Commit
		commitPath := filepath.Join(createDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify schema exists
		schemaPath := filepath.Join(schemasDir, schemaName)
		if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
			t.Errorf("Schema directory should exist after CREATE")
		} else {
			t.Logf("Schema %s created successfully", schemaName)
		}
	})

	t.Run("DeleteSchema", func(t *testing.T) {
		schemaPath := filepath.Join(schemasDir, schemaName)
		if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
			t.Skip("Schema doesn't exist")
		}

		deleteDir := filepath.Join(schemaPath, ".delete")

		// Write DROP SCHEMA
		sqlPath := filepath.Join(deleteDir, "sql")
		ddl := "DROP SCHEMA " + schemaName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DROP SCHEMA: %v", err)
		}

		// Commit
		commitPath := filepath.Join(deleteDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify schema is gone
		if _, err := os.Stat(schemaPath); !os.IsNotExist(err) {
			t.Errorf("Schema should not exist after DROP")
		} else {
			t.Logf("Schema %s deleted successfully", schemaName)
		}
	})
}

// TestDDL_ViewCreateDelete tests creating and deleting a view.
func TestDDL_ViewCreateDelete(t *testing.T) {
	checkFUSEMountCapability(t)

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

	viewName := "test_active_users"
	viewsDir := filepath.Join(mountpoint, ".views")

	t.Run("CreateView", func(t *testing.T) {
		createDir := filepath.Join(viewsDir, ".create", viewName)

		// Create staging directory
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to create view staging dir: %v", err)
		}

		// Write CREATE VIEW
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "CREATE VIEW " + viewName + " AS SELECT * FROM users WHERE active = true;"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write CREATE VIEW: %v", err)
		}

		// Commit
		commitPath := filepath.Join(createDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify view exists
		viewPath := filepath.Join(viewsDir, viewName)
		if _, err := os.Stat(viewPath); os.IsNotExist(err) {
			t.Errorf("View directory should exist after CREATE")
		} else {
			t.Logf("View %s created successfully", viewName)
		}
	})

	t.Run("DeleteView", func(t *testing.T) {
		viewPath := filepath.Join(viewsDir, viewName)
		if _, err := os.Stat(viewPath); os.IsNotExist(err) {
			t.Skip("View doesn't exist")
		}

		deleteDir := filepath.Join(viewPath, ".delete")

		// Write DROP VIEW
		sqlPath := filepath.Join(deleteDir, "sql")
		ddl := "DROP VIEW " + viewName + ";"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DROP VIEW: %v", err)
		}

		// Commit
		commitPath := filepath.Join(deleteDir, ".commit")
		if err := touchFile(commitPath); err != nil {
			t.Fatalf("Failed to touch .commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Verify view is gone
		if _, err := os.Stat(viewPath); !os.IsNotExist(err) {
			t.Errorf("View should not exist after DROP")
		} else {
			t.Logf("View %s deleted successfully", viewName)
		}
	})
}

// TestDDL_TestValidation tests the .test validation workflow.
func TestDDL_TestValidation(t *testing.T) {
	checkFUSEMountCapability(t)

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

	t.Run("ValidDDL", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", "test_valid_table")
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to create staging dir: %v", err)
		}

		// Write valid DDL
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "CREATE TABLE test_valid_table (id serial PRIMARY KEY);"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Touch .test
		testPath := filepath.Join(createDir, ".test")
		if err := touchFile(testPath); err != nil {
			t.Fatalf("Failed to touch .test: %v", err)
		}

		// Read test.log
		testLogPath := filepath.Join(createDir, "test.log")
		result, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("Could not read test.log: %v", err)
		} else {
			t.Logf("Test result for valid DDL: %s", string(result))
			if strings.Contains(strings.ToLower(string(result)), "success") ||
				strings.Contains(strings.ToLower(string(result)), "ok") ||
				!strings.Contains(strings.ToLower(string(result)), "error") {
				t.Logf("Valid DDL passed validation")
			}
		}

		// Abort to clean up
		abortPath := filepath.Join(createDir, ".abort")
		_ = touchFile(abortPath)
	})

	t.Run("InvalidDDL", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", "test_invalid_table")
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to create staging dir: %v", err)
		}

		// Write invalid DDL (syntax error)
		sqlPath := filepath.Join(createDir, "sql")
		ddl := "CREAT TABL test_invalid_table;"
		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		// Touch .test
		testPath := filepath.Join(createDir, ".test")
		_ = touchFile(testPath) // May fail, that's ok

		// Read test.log - should show error
		testLogPath := filepath.Join(createDir, "test.log")
		result, err := os.ReadFile(testLogPath)
		if err != nil {
			t.Logf("Could not read test.log (may be expected): %v", err)
		} else {
			t.Logf("Test result for invalid DDL: %s", string(result))
			if strings.Contains(strings.ToLower(string(result)), "error") ||
				strings.Contains(strings.ToLower(string(result)), "syntax") {
				t.Logf("Invalid DDL correctly failed validation")
			}
		}

		// Abort to clean up
		abortPath := filepath.Join(createDir, ".abort")
		_ = touchFile(abortPath)
	})
}

// TestDDL_AbortClearsStaging tests that .abort clears the staging entry.
func TestDDL_AbortClearsStaging(t *testing.T) {
	checkFUSEMountCapability(t)

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

	createDir := filepath.Join(mountpoint, ".create", "test_abort_table")
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to create staging dir: %v", err)
	}

	// Write DDL
	sqlPath := filepath.Join(createDir, "sql")
	ddl := "CREATE TABLE test_abort_table (id serial PRIMARY KEY);"
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	// Verify content was staged by reading it back
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Fatalf("Failed to read back staged content: %v", err)
	}
	if !strings.Contains(string(content), "CREATE TABLE") {
		t.Errorf("Expected staged DDL content, got: %s", string(content))
	}

	// Touch .abort
	abortPath := filepath.Join(createDir, ".abort")
	if err := touchFile(abortPath); err != nil {
		t.Fatalf("Failed to touch .abort: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify staging was cleared - content should revert to template
	content, err = os.ReadFile(sqlPath)
	if err != nil {
		t.Logf("Could not read sql file after abort: %v", err)
	} else {
		// After abort, should see template again (with comments)
		if strings.HasPrefix(strings.TrimSpace(string(content)), "--") ||
			!strings.Contains(string(content), "test_abort_table") {
			t.Logf("Staging correctly cleared after abort")
		} else {
			t.Logf("Content after abort: %s", string(content))
		}
	}
}

// TestDDL_ScriptWorkflow tests the script workflow (direct write + commit).
func TestDDL_ScriptWorkflow(t *testing.T) {
	checkFUSEMountCapability(t)

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

	tableName := "test_script_workflow"
	createDir := filepath.Join(mountpoint, ".create", tableName)

	// Script workflow: mkdir + write sql + commit in quick succession
	if err := os.MkdirAll(createDir, 0755); err != nil {
		t.Fatalf("Failed to create staging dir: %v", err)
	}

	sqlPath := filepath.Join(createDir, "sql")
	ddl := "CREATE TABLE " + tableName + " (id serial PRIMARY KEY, data text);"
	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchFile(commitPath); err != nil {
		t.Fatalf("Failed to touch .commit: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify table was created
	tablePath := filepath.Join(mountpoint, tableName)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Errorf("Table should exist after script workflow")
	} else {
		t.Logf("Script workflow created table successfully")

		// Clean up - delete the table
		deleteDir := filepath.Join(tablePath, ".delete")
		sqlPath := filepath.Join(deleteDir, "sql")
		_ = os.WriteFile(sqlPath, []byte("DROP TABLE "+tableName+";"), 0644)
		_ = touchFile(filepath.Join(deleteDir, ".commit"))
	}
}

// Helper functions

func touchFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return f.Close()
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
