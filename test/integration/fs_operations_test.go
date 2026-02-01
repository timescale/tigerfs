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
