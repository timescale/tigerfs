package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// setupFSOperations creates fs.Operations with a real database connection.
func setupFSOperations(t *testing.T, connStr string) *fs.Operations {
	t.Helper()

	cfg := &config.Config{
		DirListingLimit: 10000,
		QueryTimeout:    30,
		PoolSize:        5,
		PoolMaxIdle:     2,
	}

	ctx := context.Background()
	dbClient, err := db.NewClient(ctx, cfg, connStr)
	require.NoError(t, err, "Failed to create db client")

	t.Cleanup(func() {
		dbClient.Close()
	})

	return fs.NewOperations(cfg, dbClient)
}

// findTablePath returns the path to the users table.
// In Docker mode with public schema, tables appear at root level: "/users"
// In local mode with unique schema, they're nested: "/tigerfs_test_xxx/users"
func findTablePath(t *testing.T, ops *fs.Operations) string {
	t.Helper()
	ctx := context.Background()

	entries, fsErr := ops.ReadDir(ctx, "/")
	require.Nil(t, fsErr, "ReadDir should succeed for root")

	// Check if tables are at root level (Docker/public schema mode)
	for _, e := range entries {
		if e.Name == "users" {
			return "/users"
		}
	}

	// Check if there's a tigerfs_test schema (local mode)
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "tigerfs_test") {
			return "/" + e.Name + "/users"
		}
	}

	// Fall back to public schema
	return "/public/users"
}

// TestFSOperations_ReadDir_Root tests reading the root directory.
func TestFSOperations_ReadDir_Root(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	entries, fsErr := ops.ReadDir(ctx, "/")
	require.Nil(t, fsErr, "ReadDir should succeed for root")
	require.NotEmpty(t, entries, "Root should have at least one entry")

	// Log what we found for debugging
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	t.Logf("Found at root: %v", names)

	// Root may show schemas or tables directly depending on setup
	var foundContent bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name, "tigerfs_test") || e.Name == "public" ||
			e.Name == "users" || e.Name == "products" || e.Name == ".schemas" {
			foundContent = true
			assert.True(t, e.IsDir, "%s should be a directory", e.Name)
			break
		}
	}
	assert.True(t, foundContent, "Should find schema or table at root")
}

// TestFSOperations_ReadDir_Table tests reading a table directory.
func TestFSOperations_ReadDir_Table(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - path handling differs
	if result.Source == SourceDocker {
		t.Skip("Skipping in Docker mode - path handling requires schema prefix")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	t.Logf("Using table path: %s", tablePath)

	entries, fsErr := ops.ReadDir(ctx, tablePath)
	require.Nil(t, fsErr, "ReadDir should succeed for table")

	var hasInfo, hasRows bool
	for _, e := range entries {
		if e.Name == ".info" {
			hasInfo = true
			assert.True(t, e.IsDir, ".info should be a directory")
		}
		if strings.HasSuffix(e.Name, ".json") || strings.HasSuffix(e.Name, ".csv") || strings.HasSuffix(e.Name, ".tsv") {
			hasRows = true
		}
	}

	assert.True(t, hasInfo, "Table should have .info directory")
	assert.True(t, hasRows, "Table should have row files")
}

// TestFSOperations_ReadDir_Info tests reading the .info metadata directory.
func TestFSOperations_ReadDir_Info(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - .info paths require schema prefix
	if result.Source == SourceDocker {
		t.Skip("Skipping in Docker mode - .info paths require schema prefix")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	infoPath := tablePath + "/.info"

	entries, fsErr := ops.ReadDir(ctx, infoPath)
	require.Nil(t, fsErr, "ReadDir should succeed for .info")

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
		assert.False(t, e.IsDir, "Metadata files should not be directories")
	}

	assert.True(t, names["count"], "Should have count file")
	assert.True(t, names["schema"], "Should have schema file")
	assert.True(t, names["columns"], "Should have columns file")
	assert.True(t, names["ddl"], "Should have ddl file")
}

// TestFSOperations_Stat_Root tests stat on root.
func TestFSOperations_Stat_Root(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	entry, fsErr := ops.Stat(ctx, "/")
	require.Nil(t, fsErr, "Stat should succeed for root")
	assert.True(t, entry.IsDir, "Root should be a directory")
	// Root entry has empty name - "/" is the path, not a name
	assert.Equal(t, "", entry.Name)
}

// TestFSOperations_Stat_Table tests stat on a table.
func TestFSOperations_Stat_Table(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	entry, fsErr := ops.Stat(ctx, tablePath)
	require.Nil(t, fsErr, "Stat should succeed for table")
	assert.True(t, entry.IsDir, "Table should be a directory")
	assert.Equal(t, "users", entry.Name)
}

// TestFSOperations_ReadFile_RowJSON tests reading a row as JSON.
func TestFSOperations_ReadFile_RowJSON(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	rowPath := tablePath + "/1.json"

	content, fsErr := ops.ReadFile(ctx, rowPath)
	require.Nil(t, fsErr, "ReadFile should succeed for row")
	require.NotEmpty(t, content.Data, "Row should have content")

	data := string(content.Data)
	assert.Contains(t, data, "Alice", "Should contain Alice")
	assert.Contains(t, data, "alice@example.com", "Should contain email")
}

// TestFSOperations_ReadFile_InfoCount tests reading the count file.
// Note: This test is skipped in Docker mode as .info file paths require
// schema-qualified table paths. Works correctly with FUSE mount.
func TestFSOperations_ReadFile_InfoCount(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode where tables appear at root without schema
	if result.Source == SourceDocker {
		t.Skip("Skipping .info test in Docker mode - requires schema-qualified path")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	countPath := tablePath + "/.info/count"

	content, fsErr := ops.ReadFile(ctx, countPath)
	require.Nil(t, fsErr, "ReadFile should succeed for count")

	count := strings.TrimSpace(string(content.Data))
	assert.Equal(t, "3", count, "Should have 3 users")
}

// TestFSOperations_WriteFile_RowUpdate tests updating a row.
func TestFSOperations_WriteFile_RowUpdate(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	rowPath := tablePath + "/1.json"

	newData := `{"id":1,"name":"Alice Updated","email":"alice@example.com","age":31,"active":true}`
	fsErr := ops.WriteFile(ctx, rowPath, []byte(newData))
	require.Nil(t, fsErr, "WriteFile should succeed for update")

	// Read back and verify
	content, fsErr := ops.ReadFile(ctx, rowPath)
	require.Nil(t, fsErr, "ReadFile should succeed after update")
	assert.Contains(t, string(content.Data), "Alice Updated", "Should contain updated name")
}

// TestFSOperations_Delete_Row tests deleting a row.
func TestFSOperations_Delete_Row(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	rowPath := tablePath + "/3.json"

	fsErr := ops.Delete(ctx, rowPath)
	require.Nil(t, fsErr, "Delete should succeed")

	// Verify it's gone
	_, fsErr = ops.ReadFile(ctx, rowPath)
	require.NotNil(t, fsErr, "ReadFile should fail for deleted row")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "Should be not exist error")
}

// TestFSOperations_NotExist tests error handling for non-existent paths.
func TestFSOperations_NotExist(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Non-existent row
	_, fsErr := ops.ReadFile(ctx, tablePath+"/99999.json")
	require.NotNil(t, fsErr, "Should fail for non-existent row")
}

// TestFSOperations_WriteFile_SimpleColumn tests writing to a simple column path.
// This tests: echo "value" > /table/pk/column
func TestFSOperations_WriteFile_SimpleColumn(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Write to column
	columnPath := tablePath + "/1/name"
	fsErr := ops.WriteFile(ctx, columnPath, []byte("Updated Name\n"))
	require.Nil(t, fsErr, "WriteFile should succeed for column")

	// Read back and verify
	content, fsErr := ops.ReadFile(ctx, columnPath)
	require.Nil(t, fsErr, "ReadFile should succeed after update")
	assert.Equal(t, "Updated Name\n", string(content.Data), "Should contain updated value")

	// Also verify via row read
	rowContent, fsErr := ops.ReadFile(ctx, tablePath+"/1.json")
	require.Nil(t, fsErr, "ReadFile should succeed for row")
	assert.Contains(t, string(rowContent.Data), "Updated Name", "Row should contain updated name")
}

// TestFSOperations_WriteFile_ColumnSizeConsistency tests that Stat and ReadFile
// return consistent sizes for column files. This is critical for NFS.
func TestFSOperations_WriteFile_ColumnSizeConsistency(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)
	columnPath := tablePath + "/1/name"

	// Get stat info
	entry, fsErr := ops.Stat(ctx, columnPath)
	require.Nil(t, fsErr, "Stat should succeed")

	// Get actual content
	content, fsErr := ops.ReadFile(ctx, columnPath)
	require.Nil(t, fsErr, "ReadFile should succeed")

	// Size should match
	assert.Equal(t, entry.Size, int64(len(content.Data)),
		"Stat size (%d) should match ReadFile size (%d)", entry.Size, len(content.Data))
}

// TestFSOperations_WriteFile_PipelineColumn tests writing to a column via pipeline path.
// This tests: cd /table/.by/col/val/pk; echo "value" > column
func TestFSOperations_WriteFile_PipelineColumn(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - pipeline paths need schema-qualified tables
	if result.Source == SourceDocker {
		t.Skip("Skipping pipeline test in Docker mode - requires schema-qualified path")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// First, let's verify there's an active=true filter option
	// Read a row to find the active column value
	content, fsErr := ops.ReadFile(ctx, tablePath+"/1.json")
	require.Nil(t, fsErr, "ReadFile should succeed")
	t.Logf("Row 1 data: %s", string(content.Data))

	// Write to column via pipeline path (filter by active=true, then update name)
	// Use .filter/active/true instead of .by to test filter paths
	pipelinePath := tablePath + "/.filter/active/true/1/name"
	fsErr = ops.WriteFile(ctx, pipelinePath, []byte("Pipeline Updated\n"))
	require.Nil(t, fsErr, "WriteFile via pipeline should succeed")

	// Verify the update via direct path
	directContent, fsErr := ops.ReadFile(ctx, tablePath+"/1/name")
	require.Nil(t, fsErr, "ReadFile should succeed after pipeline update")
	assert.Equal(t, "Pipeline Updated\n", string(directContent.Data), "Should contain updated value")
}

// TestFSOperations_Import_Sync tests the .import/.sync write operation.
func TestFSOperations_Import_Sync(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - import paths need specific setup
	if result.Source == SourceDocker {
		t.Skip("Skipping import test in Docker mode")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Prepare import data (CSV format)
	importData := `id,name,email,age,active
1,Sync Alice,sync.alice@example.com,25,true
2,Sync Bob,sync.bob@example.com,30,false
`

	// Write to sync import path
	importPath := tablePath + "/.import/.sync/data.csv"
	fsErr := ops.WriteFile(ctx, importPath, []byte(importData))
	require.Nil(t, fsErr, "Import sync should succeed")

	// Verify the data was imported
	row1, fsErr := ops.ReadFile(ctx, tablePath+"/1.json")
	require.Nil(t, fsErr, "ReadFile should succeed after import")
	assert.Contains(t, string(row1.Data), "Sync Alice", "Row 1 should have synced name")
}

// TestFSOperations_Import_Append tests the .import/.append write operation.
func TestFSOperations_Import_Append(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - import paths need specific setup
	if result.Source == SourceDocker {
		t.Skip("Skipping import test in Docker mode")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Get initial count
	countContent, fsErr := ops.ReadFile(ctx, tablePath+"/.info/count")
	require.Nil(t, fsErr, "ReadFile count should succeed")
	initialCount := strings.TrimSpace(string(countContent.Data))
	t.Logf("Initial count: %s", initialCount)

	// Prepare append data (new rows)
	appendData := `id,name,email,age,active
100,Appended User,appended@example.com,40,true
`

	// Write to append import path
	importPath := tablePath + "/.import/.append/data.csv"
	fsErr = ops.WriteFile(ctx, importPath, []byte(appendData))
	require.Nil(t, fsErr, "Import append should succeed")

	// Verify the new row exists
	newRow, fsErr := ops.ReadFile(ctx, tablePath+"/100.json")
	require.Nil(t, fsErr, "ReadFile should succeed for appended row")
	assert.Contains(t, string(newRow.Data), "Appended User", "Should contain appended user")
}

// TestFSOperations_WriteFile_NullColumn tests writing empty/NULL to a column.
func TestFSOperations_WriteFile_NullColumn(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Write empty to age column (should set to NULL)
	columnPath := tablePath + "/1/age"
	fsErr := ops.WriteFile(ctx, columnPath, []byte("\n"))
	require.Nil(t, fsErr, "WriteFile should succeed for empty value")

	// Verify stat and read are consistent after NULL
	entry, fsErr := ops.Stat(ctx, columnPath)
	require.Nil(t, fsErr, "Stat should succeed for NULL column")

	content, fsErr := ops.ReadFile(ctx, columnPath)
	require.Nil(t, fsErr, "ReadFile should succeed for NULL column")

	// Size should match (even for NULL)
	assert.Equal(t, entry.Size, int64(len(content.Data)),
		"Stat size (%d) should match ReadFile size (%d) for NULL column", entry.Size, len(content.Data))
}

// TestFSOperations_Import_Overwrite tests the .import/.overwrite write operation.
// Overwrite should truncate existing data before importing.
func TestFSOperations_Import_Overwrite(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - import paths need specific setup
	if result.Source == SourceDocker {
		t.Skip("Skipping import test in Docker mode")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Get initial count
	countContent, fsErr := ops.ReadFile(ctx, tablePath+"/.info/count")
	require.Nil(t, fsErr, "ReadFile count should succeed")
	t.Logf("Initial count: %s", strings.TrimSpace(string(countContent.Data)))

	// Prepare overwrite data (only 2 rows - should replace all existing)
	overwriteData := `id,name,email,age,active
1,Overwrite Alice,overwrite.alice@example.com,25,true
2,Overwrite Bob,overwrite.bob@example.com,30,false
`

	// Write to overwrite import path
	importPath := tablePath + "/.import/.overwrite/data.csv"
	fsErr = ops.WriteFile(ctx, importPath, []byte(overwriteData))
	require.Nil(t, fsErr, "Import overwrite should succeed")

	// Verify the data was replaced - should have exactly 2 rows now
	countContent, fsErr = ops.ReadFile(ctx, tablePath+"/.info/count")
	require.Nil(t, fsErr, "ReadFile count should succeed after overwrite")
	finalCount := strings.TrimSpace(string(countContent.Data))
	assert.Equal(t, "2", finalCount, "Overwrite should leave exactly 2 rows")

	// Verify the content
	row1, fsErr := ops.ReadFile(ctx, tablePath+"/1.json")
	require.Nil(t, fsErr, "ReadFile should succeed after overwrite")
	assert.Contains(t, string(row1.Data), "Overwrite Alice", "Row 1 should have overwritten name")
}

// TestFSOperations_Export_CSV tests the bulk export functionality.
// This tests reading from .export paths.
func TestFSOperations_Export_CSV(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - export paths need specific setup
	if result.Source == SourceDocker {
		t.Skip("Skipping export test in Docker mode")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Read export as CSV
	exportPath := tablePath + "/.export/csv"
	content, fsErr := ops.ReadFile(ctx, exportPath)
	require.Nil(t, fsErr, "ReadFile export should succeed")

	data := string(content.Data)
	assert.NotEmpty(t, data, "Export should have data")

	// CSV without headers should have data rows
	lines := strings.Split(strings.TrimSpace(data), "\n")
	assert.GreaterOrEqual(t, len(lines), 1, "Export should have at least 1 row")
	t.Logf("Exported %d rows", len(lines))
}

// TestFSOperations_Export_WithHeaders tests bulk export with headers.
func TestFSOperations_Export_WithHeaders(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - export paths need specific setup
	if result.Source == SourceDocker {
		t.Skip("Skipping export test in Docker mode")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Read export as CSV with headers
	exportPath := tablePath + "/.export/.with-headers/csv"
	content, fsErr := ops.ReadFile(ctx, exportPath)
	require.Nil(t, fsErr, "ReadFile export with headers should succeed")

	data := string(content.Data)
	assert.NotEmpty(t, data, "Export should have data")

	// CSV with headers should have header row
	lines := strings.Split(strings.TrimSpace(data), "\n")
	assert.GreaterOrEqual(t, len(lines), 2, "Export with headers should have header + data rows")

	// Header should contain column names
	header := lines[0]
	assert.Contains(t, header, "id", "Header should contain id column")
	assert.Contains(t, header, "name", "Header should contain name column")
	t.Logf("Export header: %s", header)
}

// TestFSOperations_Export_JSON tests bulk export as JSON.
func TestFSOperations_Export_JSON(t *testing.T) {
	result := GetTestDB(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	// Skip in Docker mode - export paths need specific setup
	if result.Source == SourceDocker {
		t.Skip("Skipping export test in Docker mode")
	}

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	tablePath := findTablePath(t, ops)

	// Read export as JSON
	exportPath := tablePath + "/.export/json"
	content, fsErr := ops.ReadFile(ctx, exportPath)
	require.Nil(t, fsErr, "ReadFile export JSON should succeed")

	data := string(content.Data)
	assert.NotEmpty(t, data, "Export should have data")

	// JSON export should be a valid JSON array
	assert.True(t, strings.HasPrefix(strings.TrimSpace(data), "["), "JSON export should start with [")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(data), "]"), "JSON export should end with ]")
}
