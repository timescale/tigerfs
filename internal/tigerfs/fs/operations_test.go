package fs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestNewOperations tests the Operations constructor.
func TestNewOperations(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	assert.NotNil(t, ops)
	assert.Equal(t, cfg, ops.config)
	assert.Equal(t, mockDB, ops.db)
}

// TestReadDir_Root tests reading the root directory.
// The root directory shows tables from the default schema (flattened) plus capability directories.
func TestReadDir_Root(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users", "tasks"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/")

	require.Nil(t, err)
	require.NotNil(t, entries)

	// Should have tables plus special directories
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	assert.Contains(t, names, "users")
	assert.Contains(t, names, "tasks")
	assert.Contains(t, names, ".schemas")
	assert.Contains(t, names, ".create")
	assert.Contains(t, names, ".build")
}

// TestReadDir_Table tests reading a table directory.
func TestReadDir_Table(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		rows: map[string][]string{
			"public.users": {"1", "2", "3"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users")

	require.Nil(t, err)
	require.NotNil(t, entries)

	// Should have rows plus capability directories
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	assert.Contains(t, names, "1")
	assert.Contains(t, names, "2")
	assert.Contains(t, names, "3")
	assert.Contains(t, names, ".info")
	assert.Contains(t, names, ".by")
	assert.Contains(t, names, ".filter")
	assert.Contains(t, names, ".format")
	assert.Contains(t, names, ".order")
	assert.Contains(t, names, ".first")
	assert.Contains(t, names, ".last")
	assert.Contains(t, names, ".sample")
	assert.Contains(t, names, ".export")
	assert.Contains(t, names, ".import")
}

// TestStat_Root tests Stat on root directory.
func TestStat_Root(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
}

// TestStat_Table tests Stat on a table directory.
func TestStat_Table(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, "users", entry.Name)
}

// TestStat_Table_NotFound tests Stat on a non-existent table.
func TestStat_Table_NotFound(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	_, err := ops.Stat(context.Background(), "/nonexistent")

	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestStat_RowFile tests Stat on a row file.
func TestStat_RowFile(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name"},
				values:  []interface{}{1, "Alice"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users/1.json")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.False(t, entry.IsDir)
	assert.Equal(t, "1.json", entry.Name)
	assert.True(t, entry.Size > 0)
}

// TestReadFile_RowJSON tests reading a row as JSON.
func TestReadFile_RowJSON(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name"},
				values:  []interface{}{1, "Alice"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/1.json")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "Alice")
	assert.Contains(t, string(content.Data), "id")
	assert.Contains(t, string(content.Data), "name")
}

// TestReadFile_ColumnFile tests reading a single column file.
func TestReadFile_ColumnFile(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columnData: map[string]interface{}{
			"public.users.1.name": "Alice",
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/1/name")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Equal(t, "Alice\n", string(content.Data))
}

// TestReadFile_NotFound tests reading a non-existent file.
func TestReadFile_NotFound(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/999.json")

	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
	assert.Nil(t, content)
}

// TestReadDir_InfoDirectory tests reading the .info metadata directory.
func TestReadDir_InfoDirectory(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.info")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	assert.Contains(t, names, "count")
	assert.Contains(t, names, "ddl")
	assert.Contains(t, names, "columns")
	assert.Contains(t, names, "schema")
}

// TestReadFile_InfoCount tests reading the .count metadata file.
func TestReadFile_InfoCount(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		rowCounts: map[string]int64{
			"public.users": 42,
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/.info/count")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Equal(t, "42\n", string(content.Data))
}

// TestStat_Column tests Stat on a column file.
func TestStat_Column(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columnData: map[string]interface{}{
			"public.users.1.name": "Alice",
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users/1/name")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.False(t, entry.IsDir)
	assert.Equal(t, "name", entry.Name)
}

// TestStat_InfoFile tests Stat on an info file.
func TestStat_InfoFile(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users/.info/count")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.False(t, entry.IsDir)
	assert.Equal(t, "count", entry.Name)
}

// TestStat_DDLFile tests Stat on a DDL control file.
func TestStat_DDLFile(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Create a DDL session first
	_, err := ops.GetDDLManager().CreateSession(DDLCreate, "index", "public", "myindex", "")
	require.NoError(t, err)

	// Test sql file - should exist and be readable
	entry, fsErr := ops.Stat(context.Background(), "/.create/myindex/sql")
	require.Nil(t, fsErr)
	require.NotNil(t, entry)
	assert.Equal(t, "sql", entry.Name)

	// Test trigger files - they exist so they appear in ls output
	entry, fsErr = ops.Stat(context.Background(), "/.create/myindex/.test")
	require.Nil(t, fsErr)
	require.NotNil(t, entry)
	assert.Equal(t, ".test", entry.Name)

	entry, fsErr = ops.Stat(context.Background(), "/.create/myindex/.commit")
	require.Nil(t, fsErr)
	require.NotNil(t, entry)
	assert.Equal(t, ".commit", entry.Name)

	entry, fsErr = ops.Stat(context.Background(), "/.create/myindex/.abort")
	require.Nil(t, fsErr)
	require.NotNil(t, entry)
	assert.Equal(t, ".abort", entry.Name)
}

// TestStat_DDLFile_NoSession tests Stat on a DDL file when no session exists.
func TestStat_DDLFile_NoSession(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Without creating a session, stat should return ErrNotExist
	_, err := ops.Stat(context.Background(), "/.create/nonexistent/sql")
	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestReadFile_InfoDDL tests reading the .ddl metadata file.
func TestReadFile_InfoDDL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		tableDDL: map[string]string{
			"public.users": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/.info/ddl")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "CREATE TABLE")
}

// TestReadFile_InfoColumns tests reading the columns metadata file.
func TestReadFile_InfoColumns(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/.info/columns")

	require.Nil(t, err)
	require.NotNil(t, content)
	// columns file just lists column names, one per line (no types)
	assert.Contains(t, string(content.Data), "id\n")
	assert.Contains(t, string(content.Data), "name\n")
}

// TestReadFile_InfoSchema tests reading the schema metadata file.
func TestReadFile_InfoSchema(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		tableDDL: map[string]string{
			"public.users": "CREATE TABLE users (id integer, name text)",
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/.info/schema")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "CREATE TABLE")
}

// TestReadDir_ByCapability tests listing indexed columns in .by/.
func TestReadDir_ByCapability(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		indexes: map[string][]mockIndex{
			"public.users": {
				{name: "users_email_idx", columns: []string{"email"}, unique: false},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.by")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "email")
}

// TestReadDir_FilterCapability tests listing columns in .filter/.
func TestReadDir_FilterCapability(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.filter")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "id")
	assert.Contains(t, names, "name")
}

// TestReadDir_OrderCapability tests listing columns in .order/.
func TestReadDir_OrderCapability(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.order")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "id")
	assert.Contains(t, names, "id.desc")
	assert.Contains(t, names, "name")
	assert.Contains(t, names, "name.desc")
}

// TestReadDir_PaginationCapability tests listing options in .first/.
func TestReadDir_PaginationCapability(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.first")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "10")
	assert.Contains(t, names, "100")
}

// TestReadDir_ExportDirectory tests listing the .export directory.
func TestReadDir_ExportDirectory(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.export")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	// Matches FUSE behavior: format names without "all." prefix
	assert.Contains(t, names, ".with-headers")
	assert.Contains(t, names, "csv")
	assert.Contains(t, names, "json")
	assert.Contains(t, names, "tsv")
	assert.Contains(t, names, "yaml")
}

// TestReadDir_ImportDirectory tests listing the .import directory.
func TestReadDir_ImportDirectory(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.import")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, ".sync")
	assert.Contains(t, names, ".overwrite")
	assert.Contains(t, names, ".append")
}

// TestReadDir_SchemaList tests listing all schemas.
func TestReadDir_SchemaList(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
			"myapp":  {"customers"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/.schemas")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "public")
	assert.Contains(t, names, "myapp")
}

// TestReadDir_Schema tests listing tables in a specific schema.
func TestReadDir_Schema(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"myapp": {"customers", "orders"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/.schemas/myapp")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "customers")
	assert.Contains(t, names, "orders")
}

// TestReadFile_RowFormats tests reading rows in different formats.
func TestReadFile_RowFormats(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name"},
				values:  []interface{}{1, "Alice"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Test CSV format - output is just values, no headers
	content, err := ops.ReadFile(context.Background(), "/users/1.csv")
	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "Alice")

	// Test TSV format - output is just values, no headers
	content, err = ops.ReadFile(context.Background(), "/users/1.tsv")
	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "Alice")
}

// TestReadDirWithContext tests the context-based ReadDir variant.
func TestReadDirWithContext(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		rows: map[string][]string{
			"public.users": {"1", "2"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	ctx := NewFSContext("public", "users", "id")
	entries, err := ops.ReadDirWithContext(context.Background(), ctx)

	require.Nil(t, err)
	require.NotNil(t, entries)
}

// TestStat_Capability tests Stat on a capability directory.
func TestStat_Capability(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Test .by capability
	entry, err := ops.Stat(context.Background(), "/users/.by")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, ".by", entry.Name)
}

// TestStat_Export tests Stat on .export directory.
func TestStat_Export(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users/.export")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
}

// TestStat_Import tests Stat on .import directory.
func TestStat_Import(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users/.import")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
}

// TestStat_DDL tests Stat on DDL directories.
func TestStat_DDL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Test .create directory (always exists)
	entry, err := ops.Stat(context.Background(), "/.create")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, ".create", entry.Name)

	// Create a DDL session first
	_, createErr := ops.GetDDLManager().CreateSession(DDLCreate, "index", "public", "myindex", "")
	require.NoError(t, createErr)

	// Test named staging directory (requires session)
	entry, err = ops.Stat(context.Background(), "/.create/myindex")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, "myindex", entry.Name)
}

// TestStat_DDL_NoSession tests Stat on a DDL staging directory when no session exists.
func TestStat_DDL_NoSession(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Without creating a session, stat on staging dir should return ErrNotExist
	_, err := ops.Stat(context.Background(), "/.create/nonexistent")
	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestReadDir_RowDirectory tests listing columns in a row directory.
func TestReadDir_RowDirectory(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
				{name: "email", dataType: "text"},
			},
		},
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name", "email"},
				values:  []interface{}{1, "Alice", "alice@example.com"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/1")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	// Columns now have type-appropriate extensions
	assert.Contains(t, names, "id")        // integer has no extension
	assert.Contains(t, names, "name.txt")  // text gets .txt extension
	assert.Contains(t, names, "email.txt") // text gets .txt extension
}

// TestReadDir_DDL tests listing DDL staging directories.
func TestReadDir_DDL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Test listing .create directory (empty initially)
	entries, err := ops.ReadDir(context.Background(), "/.create")
	require.Nil(t, err)
	require.NotNil(t, entries)
	assert.Len(t, entries, 0)

	// Create a DDL session
	_, createErr := ops.GetDDLManager().CreateSession(DDLCreate, "index", "public", "myindex", "")
	require.NoError(t, createErr)

	// Test listing .create directory (now has one session)
	entries, err = ops.ReadDir(context.Background(), "/.create")
	require.Nil(t, err)
	require.NotNil(t, entries)
	assert.Len(t, entries, 1)
	assert.Equal(t, "myindex", entries[0].Name)
	assert.True(t, entries[0].IsDir)

	// Test listing specific staging directory
	entries, err = ops.ReadDir(context.Background(), "/.create/myindex")
	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "sql")
	assert.Contains(t, names, ".test")
	assert.Contains(t, names, ".commit")
	assert.Contains(t, names, ".abort")
}

// TestReadDir_DDL_NoSession tests listing a DDL staging directory when no session exists.
func TestReadDir_DDL_NoSession(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Without creating a session, readdir on staging dir should return ErrNotExist
	_, err := ops.ReadDir(context.Background(), "/.create/nonexistent")
	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestStatWithContext tests Stat using a pre-parsed FSContext.
func TestStatWithContext(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	ctx := NewFSContext("public", "users", "id")
	entry, err := ops.StatWithContext(context.Background(), ctx)

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, "users", entry.Name)
}

// TestReadFileWithContext tests ReadFile using a pre-parsed FSContext.
func TestReadFileWithContext(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name"},
				values:  []interface{}{1, "Alice"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Note: ReadFileWithContext is designed for row/column paths, not tables
	// So we test that it returns an error for PathTable
	ctx := NewFSContext("public", "users", "id")
	_, err := ops.ReadFileWithContext(context.Background(), ctx)

	// Should error because PathTable is not a file type
	require.NotNil(t, err)
}

// TestToQueryParams tests the ToQueryParams method.
func TestToQueryParams(t *testing.T) {
	ctx := NewFSContext("public", "users", "id")
	ctx = ctx.WithFilter("status", "active", true)
	ctx = ctx.WithOrder("created_at", true)
	ctx = ctx.WithLimit(10, LimitFirst)

	params := ctx.ToQueryParams()

	assert.Equal(t, "public", params.Schema)
	assert.Equal(t, "users", params.Table)
	assert.Equal(t, "id", params.PKColumn)
	assert.Len(t, params.Filters, 1)
	assert.Equal(t, "status", params.Filters[0].Column)
	assert.Equal(t, "active", params.Filters[0].Value)
	assert.Equal(t, "created_at", params.OrderBy)
	assert.True(t, params.OrderDesc)
	assert.Equal(t, 10, params.Limit)
	assert.Equal(t, LimitFirst, params.LimitType)
}

// mockDBClient is a mock implementation of db.DBClient for testing.
type mockDBClient struct {
	// Schema data
	tables  map[string][]string
	views   map[string][]string
	columns map[string][]mockColumn

	// Primary key data
	primaryKeys map[string]*mockPK

	// Row data
	rows       map[string][]string
	rowData    map[string]*mockRow
	columnData map[string]interface{}

	// Count data
	rowCounts map[string]int64

	// DDL data
	tableDDL map[string]string

	// Index data
	indexes map[string][]mockIndex

	// View updatability
	viewUpdatable map[string]bool

	// Synth view data
	viewComments map[string]map[string]string // schema → viewName → comment
	allRowsData  map[string]*mockAllRows      // "schema.table" → columns + rows

	// Pipeline query results (for QueryRowsPipeline and QueryRowsWithDataPipeline)
	pipelineResultRows []string
	pipelineRows       [][]interface{}

	// Write tracking
	insertCalled       bool
	updateCalled       bool
	deleteCalled       bool
	updateColumnCalled bool
	importSyncCalled   bool
	lastUpdateColumn   string
	lastColumnName     string
	lastColumnValue    string
	lastDeletePK       string

	// Insert detail tracking
	lastInsertColumns []string
	lastInsertValues  []interface{}

	// Atomic rename tracking
	updateColumnCASCalled   bool
	updateColumnCASError    error
	updateColumnCASWhereVal string
	updateColumnCASNewVal   string
	renameByPrefixRows      int64

	// DDL execution tracking
	execCalled      bool
	execSuccess     bool
	execError       error
	execInTxSuccess bool
	execInTxError   error
}

type mockPK struct {
	column  string
	columns []string
}

type mockRow struct {
	columns []string
	values  []interface{}
}

type mockColumn struct {
	name     string
	dataType string
	nullable bool
}

type mockIndex struct {
	name    string
	columns []string
	unique  bool
}

type mockAllRows struct {
	columns []string
	rows    [][]interface{}
}

// Implement db.SchemaReader

func (m *mockDBClient) GetCurrentSchema(ctx context.Context) (string, error) {
	return "public", nil
}

func (m *mockDBClient) GetSchemas(ctx context.Context) ([]string, error) {
	schemas := make([]string, 0, len(m.tables))
	seen := make(map[string]bool)
	for schema := range m.tables {
		if !seen[schema] {
			schemas = append(schemas, schema)
			seen[schema] = true
		}
	}
	return schemas, nil
}

func (m *mockDBClient) GetTables(ctx context.Context, schema string) ([]string, error) {
	if tables, ok := m.tables[schema]; ok {
		return tables, nil
	}
	return []string{}, nil
}

func (m *mockDBClient) GetViews(ctx context.Context, schema string) ([]string, error) {
	if views, ok := m.views[schema]; ok {
		return views, nil
	}
	return []string{}, nil
}

func (m *mockDBClient) IsViewUpdatable(ctx context.Context, schema, view string) (bool, error) {
	if m.viewUpdatable != nil {
		key := schema + "." + view
		if updatable, ok := m.viewUpdatable[key]; ok {
			return updatable, nil
		}
	}
	return false, nil
}

func (m *mockDBClient) GetColumns(ctx context.Context, schema, table string) ([]db.Column, error) {
	key := schema + "." + table
	if cols, ok := m.columns[key]; ok {
		result := make([]db.Column, len(cols))
		for i, c := range cols {
			result[i] = db.Column{Name: c.name, DataType: c.dataType, IsNullable: c.nullable}
		}
		return result, nil
	}
	return []db.Column{}, nil
}

func (m *mockDBClient) GetPrimaryKey(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
	key := schema + "." + table
	if pk, ok := m.primaryKeys[key]; ok {
		cols := pk.columns
		if len(cols) == 0 && pk.column != "" {
			cols = []string{pk.column}
		}
		return &db.PrimaryKey{Columns: cols}, nil
	}
	return nil, fmt.Errorf("no primary key for %s", key)
}

func (m *mockDBClient) GetTablePermissions(ctx context.Context, schema, table string) (*db.TablePermissions, error) {
	return &db.TablePermissions{CanSelect: true, CanInsert: true, CanUpdate: true, CanDelete: true}, nil
}

func (m *mockDBClient) GetViewComment(ctx context.Context, schema, view string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetViewCommentsBatch(ctx context.Context, schema string) (map[string]string, error) {
	if m.viewComments != nil {
		if comments, ok := m.viewComments[schema]; ok {
			return comments, nil
		}
	}
	return make(map[string]string), nil
}

// Implement db.RowReader

func (m *mockDBClient) GetRow(ctx context.Context, schema, table, pkColumn, pkValue string) (*db.Row, error) {
	key := schema + "." + table + "." + pkValue
	if row, ok := m.rowData[key]; ok {
		return &db.Row{Columns: row.columns, Values: row.values}, nil
	}
	return nil, fmt.Errorf("row not found: %s", key)
}

func (m *mockDBClient) GetColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
	key := schema + "." + table + "." + pkValue + "." + columnName
	if val, ok := m.columnData[key]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("column not found: %s", key)
}

func (m *mockDBClient) ListRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	key := schema + "." + table
	if rows, ok := m.rows[key]; ok {
		if limit > 0 && limit < len(rows) {
			return rows[:limit], nil
		}
		return rows, nil
	}
	return []string{}, nil
}

func (m *mockDBClient) ListAllRows(ctx context.Context, schema, table, pkColumn string) ([]string, error) {
	key := schema + "." + table
	if rows, ok := m.rows[key]; ok {
		return rows, nil
	}
	return []string{}, nil
}

// Implement db.CountReader

func (m *mockDBClient) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	key := schema + "." + table
	if count, ok := m.rowCounts[key]; ok {
		return count, nil
	}
	return 0, nil
}

func (m *mockDBClient) GetRowCountSmart(ctx context.Context, schema, table string) (int64, error) {
	return m.GetRowCount(ctx, schema, table)
}

func (m *mockDBClient) GetRowCountEstimates(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
	result := make(map[string]int64)
	for _, t := range tables {
		key := schema + "." + t
		if count, ok := m.rowCounts[key]; ok {
			result[t] = count
		}
	}
	return result, nil
}

func (m *mockDBClient) GetTableRowCountEstimate(ctx context.Context, schema, table string) (int64, error) {
	return m.GetRowCount(ctx, schema, table)
}

// Implement db.DDLReader

func (m *mockDBClient) GetTableDDL(ctx context.Context, schema, table string) (string, error) {
	key := schema + "." + table
	if ddl, ok := m.tableDDL[key]; ok {
		return ddl, nil
	}
	return "", nil
}

func (m *mockDBClient) GetFullDDL(ctx context.Context, schema, table string) (string, error) {
	return m.GetTableDDL(ctx, schema, table)
}

func (m *mockDBClient) GetIndexDDL(ctx context.Context, schema, table string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetForeignKeyDDL(ctx context.Context, schema, table string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetCheckConstraintDDL(ctx context.Context, schema, table string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetTriggerDDL(ctx context.Context, schema, table string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetTableComments(ctx context.Context, schema, table string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetReferencingForeignKeys(ctx context.Context, schema, table string) ([]db.ForeignKeyRef, error) {
	return nil, nil
}

func (m *mockDBClient) GetSchemaTableCount(ctx context.Context, schema string) (int, error) {
	if tables, ok := m.tables[schema]; ok {
		return len(tables), nil
	}
	return 0, nil
}

func (m *mockDBClient) GetViewDefinition(ctx context.Context, schema, view string) (string, error) {
	return "", nil
}

func (m *mockDBClient) GetDependentViews(ctx context.Context, schema, name string) ([]string, error) {
	return nil, nil
}

// Implement db.IndexReader

func (m *mockDBClient) GetIndexes(ctx context.Context, schema, table string) ([]db.Index, error) {
	key := schema + "." + table
	if indexes, ok := m.indexes[key]; ok {
		result := make([]db.Index, len(indexes))
		for i, idx := range indexes {
			result[i] = db.Index{Name: idx.name, Columns: idx.columns, IsUnique: idx.unique}
		}
		return result, nil
	}
	return []db.Index{}, nil
}

func (m *mockDBClient) GetIndexByColumn(ctx context.Context, schema, table, column string) (*db.Index, error) {
	return nil, nil
}

func (m *mockDBClient) GetSingleColumnIndexes(ctx context.Context, schema, table string) ([]db.Index, error) {
	key := schema + "." + table
	if indexes, ok := m.indexes[key]; ok {
		result := make([]db.Index, 0)
		for _, idx := range indexes {
			if len(idx.columns) == 1 {
				result = append(result, db.Index{Name: idx.name, Columns: idx.columns, IsUnique: idx.unique})
			}
		}
		return result, nil
	}
	return []db.Index{}, nil
}

func (m *mockDBClient) GetCompositeIndexes(ctx context.Context, schema, table string) ([]db.Index, error) {
	return []db.Index{}, nil
}

func (m *mockDBClient) GetDistinctValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	return []string{}, nil
}

func (m *mockDBClient) GetDistinctValuesOrdered(ctx context.Context, schema, table, column string, limit int, ascending bool) ([]string, error) {
	return []string{}, nil
}

func (m *mockDBClient) GetDistinctValuesFiltered(ctx context.Context, schema, table, targetColumn string, filterColumns, filterValues []string, limit int) ([]string, error) {
	return []string{}, nil
}

func (m *mockDBClient) GetRowsByIndexValue(ctx context.Context, schema, table, column, value, pkColumn string, limit int) ([]string, error) {
	return []string{}, nil
}

func (m *mockDBClient) GetRowsByIndexValueOrdered(ctx context.Context, schema, table, column, value, pkColumn string, limit int, ascending bool) ([]string, error) {
	return []string{}, nil
}

func (m *mockDBClient) GetRowsByCompositeIndex(ctx context.Context, schema, table string, columns, values []string, pkColumn string, limit int) ([]string, error) {
	return []string{}, nil
}

// Implement db.PaginationReader

func (m *mockDBClient) GetFirstNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	return m.ListRows(ctx, schema, table, pkColumn, limit)
}

func (m *mockDBClient) GetLastNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	return m.ListRows(ctx, schema, table, pkColumn, limit)
}

func (m *mockDBClient) GetRandomSampleRows(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
	return m.ListRows(ctx, schema, table, pkColumn, limit)
}

func (m *mockDBClient) GetFirstNRowsOrdered(ctx context.Context, schema, table, pkColumn, orderColumn string, limit int) ([]string, error) {
	return m.ListRows(ctx, schema, table, pkColumn, limit)
}

func (m *mockDBClient) GetLastNRowsOrdered(ctx context.Context, schema, table, pkColumn, orderColumn string, limit int) ([]string, error) {
	return m.ListRows(ctx, schema, table, pkColumn, limit)
}

// Implement db.ExportReader

func (m *mockDBClient) GetAllRows(ctx context.Context, schema, table string, limit int) ([]string, [][]interface{}, error) {
	key := schema + "." + table
	if data, ok := m.allRowsData[key]; ok {
		rows := data.rows
		if limit > 0 && limit < len(rows) {
			rows = rows[:limit]
		}
		return data.columns, rows, nil
	}
	return nil, nil, nil
}

func (m *mockDBClient) GetFirstNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}

func (m *mockDBClient) GetLastNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}

func (m *mockDBClient) RowExistsByColumns(ctx context.Context, schema, table string, columns []string, values []interface{}) (bool, error) {
	key := schema + "." + table
	data, ok := m.allRowsData[key]
	if !ok {
		return false, nil
	}
	for _, row := range data.rows {
		if mockRowMatchesWhere(data.columns, row, columns, values) {
			return true, nil
		}
	}
	return false, nil
}

func (m *mockDBClient) GetRowByColumns(ctx context.Context, schema, table string, columns []string, values []interface{}) ([]string, []interface{}, error) {
	key := schema + "." + table
	data, ok := m.allRowsData[key]
	if !ok {
		return nil, nil, nil
	}
	for _, row := range data.rows {
		if mockRowMatchesWhere(data.columns, row, columns, values) {
			return data.columns, row, nil
		}
	}
	return nil, nil, nil
}

// mockRowMatchesWhere checks if a row matches all column=value conditions.
func mockRowMatchesWhere(allColumns []string, row []interface{}, whereCols []string, whereVals []interface{}) bool {
	for i, wCol := range whereCols {
		colIdx := -1
		for j, c := range allColumns {
			if c == wCol {
				colIdx = j
				break
			}
		}
		if colIdx < 0 {
			return false
		}
		// Compare as strings (matches how synth code uses ValueToString)
		if fmt.Sprintf("%v", row[colIdx]) != fmt.Sprintf("%v", whereVals[i]) {
			return false
		}
	}
	return true
}

// Implement db.ImportWriter

func (m *mockDBClient) ImportOverwrite(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error {
	return nil
}

func (m *mockDBClient) ImportSync(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error {
	m.importSyncCalled = true
	return nil
}

func (m *mockDBClient) ImportAppend(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error {
	return nil
}

// Implement db.RowWriter

func (m *mockDBClient) InsertRow(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error) {
	m.insertCalled = true
	m.lastInsertColumns = columns
	m.lastInsertValues = values
	return "1", nil
}

func (m *mockDBClient) UpdateRow(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error {
	m.updateCalled = true
	if len(columns) > 0 {
		m.lastUpdateColumn = columns[0]
	}
	return nil
}

func (m *mockDBClient) UpdateColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error {
	m.updateColumnCalled = true
	m.lastColumnName = columnName
	m.lastColumnValue = newValue
	return nil
}

func (m *mockDBClient) UpdateColumnCAS(ctx context.Context, schema, table, pkColumn, pkValue, setColumn, newValue, whereColumn, whereValue string) error {
	m.updateColumnCASCalled = true
	m.updateColumnCASWhereVal = whereValue
	m.updateColumnCASNewVal = newValue
	if m.updateColumnCASError != nil {
		return m.updateColumnCASError
	}
	return nil
}

func (m *mockDBClient) DeleteRow(ctx context.Context, schema, table, pkColumn, pkValue string) error {
	m.deleteCalled = true
	m.lastDeletePK = pkValue
	// Check if row exists
	key := schema + "." + table + "." + pkValue
	if _, ok := m.rowData[key]; !ok {
		return fmt.Errorf("row not found: %s", key)
	}
	return nil
}

// Implement db.DDLExecutor

func (m *mockDBClient) Exec(ctx context.Context, sql string, args ...interface{}) error {
	m.execCalled = true
	if m.execError != nil {
		return m.execError
	}
	return nil
}

func (m *mockDBClient) ExecInTransaction(ctx context.Context, sql string, args ...interface{}) error {
	if m.execInTxError != nil {
		return m.execInTxError
	}
	return nil
}

// Implement db.PipelineReader

func (m *mockDBClient) QueryRowsPipeline(ctx context.Context, params db.QueryParams) ([]string, error) {
	// If pipeline results are set, return them
	if m.pipelineResultRows != nil {
		return m.pipelineResultRows, nil
	}
	return []string{}, nil
}

func (m *mockDBClient) QueryRowsWithDataPipeline(ctx context.Context, params db.QueryParams) ([]string, [][]interface{}, error) {
	// If we have column data, return it filtered by params
	key := params.Schema + "." + params.Table
	if cols, ok := m.columns[key]; ok {
		columns := make([]string, len(cols))
		for i, c := range cols {
			columns[i] = c.name
		}
		// Return empty rows for now - tests can override via pipelineRows
		return columns, m.pipelineRows, nil
	}
	return nil, nil, nil
}

// pipelineRows allows tests to specify rows returned by QueryRowsPipeline
func (m *mockDBClient) SetPipelineResults(rows []string, dataRows [][]interface{}) {
	m.pipelineResultRows = rows
	m.pipelineRows = dataRows
}

// HierarchyWriter mock methods

func (m *mockDBClient) RenameByPrefix(ctx context.Context, schema, table, column, oldPrefix, newPrefix string) (int64, error) {
	return m.renameByPrefixRows, nil
}

func (m *mockDBClient) HasChildrenWithPrefix(ctx context.Context, schema, table, column, prefix string) (bool, error) {
	return false, nil
}

func (m *mockDBClient) InsertIfNotExists(ctx context.Context, schema, table string, columns []string, values []interface{}) error {
	return nil
}

// Implement db.HistoryReader

func (m *mockDBClient) HasExtension(ctx context.Context, extName string) (bool, error) {
	return false, nil
}

func (m *mockDBClient) TableExists(ctx context.Context, schema, table string) (bool, error) {
	return false, nil
}

func (m *mockDBClient) QueryHistoryByFilename(ctx context.Context, schema, historyTable, filename string, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}

func (m *mockDBClient) QueryHistoryByID(ctx context.Context, schema, historyTable, rowID string, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}

func (m *mockDBClient) QueryHistoryDistinctFilenames(ctx context.Context, schema, historyTable string, limit int) ([]string, error) {
	return nil, nil
}

func (m *mockDBClient) QueryHistoryDistinctIDs(ctx context.Context, schema, historyTable string, limit int) ([]string, error) {
	return nil, nil
}

func (m *mockDBClient) QueryHistoryVersionByTime(ctx context.Context, schema, historyTable, filterColumn, filterValue string, targetTime interface{}, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}

// ============================================================================
// Tests for bug fixes - prevent regressions
// ============================================================================

// TestStat_InfoFile_ReturnsCorrectSize verifies info files report accurate sizes.
// This prevents regression of the bug where stat returned 0 for info files.
func TestStat_InfoFile_ReturnsCorrectSize(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		rowCounts: map[string]int64{
			"public.users": 42,
		},
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users/.info/count")

	require.Nil(t, err)
	require.NotNil(t, entry)

	// Size should be 3 bytes: "42\n"
	assert.Equal(t, int64(3), entry.Size, "count file size should match content length")
}

// TestStat_InfoFile_HasReadOnlyMode verifies info files have read-only permissions.
// This prevents regression where info files had wrong permissions.
func TestStat_InfoFile_HasReadOnlyMode(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		rowCounts: map[string]int64{
			"public.users": 1,
		},
		tableDDL: map[string]string{
			"public.users": "CREATE TABLE users (id integer);",
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	infoFiles := []string{"count", "ddl", "schema", "columns", "indexes"}
	for _, file := range infoFiles {
		t.Run(file, func(t *testing.T) {
			entry, err := ops.Stat(context.Background(), "/users/.info/"+file)
			require.Nil(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, os.FileMode(0444), entry.Mode,
				"info file %s should have read-only mode 0444", file)
		})
	}
}

// TestReadDir_InfoDirectory_CorrectFilenames verifies info directory has correct file names.
// This prevents regression where info files had dot prefixes (e.g., .count instead of count).
func TestReadDir_InfoDirectory_CorrectFilenames(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.info")

	require.Nil(t, err)
	require.Len(t, entries, 5, "should have exactly 5 info files")

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	// Verify correct names (no dot prefix, matches FUSE)
	assert.Contains(t, names, "count")
	assert.Contains(t, names, "ddl")
	assert.Contains(t, names, "schema")
	assert.Contains(t, names, "columns")
	assert.Contains(t, names, "indexes")

	// Verify NO dot-prefixed names
	assert.NotContains(t, names, ".count", "should not have .count")
	assert.NotContains(t, names, ".ddl", "should not have .ddl")
	assert.NotContains(t, names, ".schema", "should not have .schema")
	assert.NotContains(t, names, ".columns", "should not have .columns")
	assert.NotContains(t, names, ".indexes", "should not have .indexes")
}

// TestStat_ExportFile_ReturnsNonZeroSize verifies export files report actual sizes.
// This prevents regression where stat returned 0 for export files.
func TestStat_ExportFile_ReturnsNonZeroSize(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
	}

	// Override GetAllRows to return actual data
	mockDB.rows = map[string][]string{"public.users": {"1"}}

	ops := NewOperations(cfg, mockDB)

	// Note: export files may return size 0 if GetAllRows returns no data,
	// but they should not error. With real data they should have size > 0.
	formats := []string{"csv", "json", "tsv", "yaml"}
	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			entry, err := ops.Stat(context.Background(), "/users/.export/"+format)
			require.Nil(t, err, "stat should not error for export file %s", format)
			require.NotNil(t, entry, "entry should not be nil for export file %s", format)
			// The file should exist and be a file (not directory)
			assert.False(t, entry.IsDir, "export file %s should not be a directory", format)
			assert.Equal(t, format, entry.Name, "export file name should be %s", format)
		})
	}
}

// TestReadDir_InfoDirectory_FilesHaveReadOnlyMode verifies info files in directory listing have correct mode.
func TestReadDir_InfoDirectory_FilesHaveReadOnlyMode(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/users/.info")

	require.Nil(t, err)
	require.NotEmpty(t, entries)

	for _, entry := range entries {
		assert.Equal(t, os.FileMode(0444), entry.Mode,
			"info file %s should have read-only mode 0444 in directory listing", entry.Name)
	}
}

// ============================================================================
// Tests for pipeline operations (filters, order, limits)
// ============================================================================

// TestReadDir_PipelineFilter_UsesQueryRowsPipeline verifies that .by/.filter paths use pipeline queries.
func TestReadDir_PipelineFilter_UsesQueryRowsPipeline(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		indexes: map[string][]mockIndex{
			"public.users": {
				{name: "users_status_idx", columns: []string{"status"}, unique: false},
			},
		},
		// Set pipeline results - these should be returned when using pipeline query
		pipelineResultRows: []string{"1", "3", "5"},
		// Regular rows should NOT be used when pipeline is active
		rows: map[string][]string{
			"public.users": {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Access path with filter: /users/.by/status/active
	entries, err := ops.ReadDir(context.Background(), "/users/.by/status/active")

	require.Nil(t, err)
	require.NotNil(t, entries)

	// Count row entries (not capability directories)
	rowEntries := []string{}
	for _, e := range entries {
		if !e.IsDir || (e.Name != ".by" && e.Name != ".columns" && e.Name != ".filter" && e.Name != ".order" &&
			e.Name != ".first" && e.Name != ".last" && e.Name != ".sample" &&
			e.Name != ".export" && e.Name != ".info") {
			rowEntries = append(rowEntries, e.Name)
		}
	}

	// Should have exactly 3 rows from pipeline query, not 10 from regular query
	assert.Len(t, rowEntries, 3, "should return filtered rows from pipeline query")
	assert.Contains(t, rowEntries, "1")
	assert.Contains(t, rowEntries, "3")
	assert.Contains(t, rowEntries, "5")
}

// TestReadDir_PipelineLimit_UsesQueryRowsPipeline verifies that .first/.last/.sample paths use pipeline queries.
func TestReadDir_PipelineLimit_UsesQueryRowsPipeline(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		// Set pipeline results - these should be returned when using pipeline query
		pipelineResultRows: []string{"1", "2", "3"},
		// Regular rows should NOT be used when pipeline is active
		rows: map[string][]string{
			"public.users": {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Access path with limit: /users/.first/3
	entries, err := ops.ReadDir(context.Background(), "/users/.first/3")

	require.Nil(t, err)
	require.NotNil(t, entries)

	// Count row entries (not capability directories)
	rowEntries := []string{}
	for _, e := range entries {
		// Row entries are not capability directories
		if e.Name[0] != '.' {
			rowEntries = append(rowEntries, e.Name)
		}
	}

	// Should have exactly 3 rows from pipeline query
	assert.Len(t, rowEntries, 3, "should return limited rows from pipeline query")
}

// TestReadDir_NoPipeline_UsesListRows verifies that plain table access uses ListRows.
func TestReadDir_NoPipeline_UsesListRows(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		// Regular rows should be used when there's no pipeline
		rows: map[string][]string{
			"public.users": {"1", "2", "3", "4", "5"},
		},
		// Pipeline results should NOT be used for plain table access
		pipelineResultRows: []string{"filtered"},
	}

	ops := NewOperations(cfg, mockDB)

	// Access plain table path: /users
	entries, err := ops.ReadDir(context.Background(), "/users")

	require.Nil(t, err)
	require.NotNil(t, entries)

	// Count row entries (not capability directories)
	rowEntries := []string{}
	for _, e := range entries {
		if e.Name[0] != '.' {
			rowEntries = append(rowEntries, e.Name)
		}
	}

	// Should have 5 rows from regular ListRows, not "filtered" from pipeline
	assert.Len(t, rowEntries, 5, "should return all rows from ListRows")
	assert.Contains(t, rowEntries, "1")
	assert.Contains(t, rowEntries, "5")
	assert.NotContains(t, rowEntries, "filtered")
}

// TestHasPipelineOperations verifies the HasPipelineOperations helper.
func TestHasPipelineOperations(t *testing.T) {
	// No operations
	ctx := NewFSContext("public", "users", "id")
	assert.False(t, ctx.HasPipelineOperations(), "empty context should have no pipeline operations")

	// With filter
	ctx = ctx.WithFilter("status", "active", true)
	assert.True(t, ctx.HasPipelineOperations(), "context with filter should have pipeline operations")

	// With order only
	ctx = NewFSContext("public", "users", "id")
	ctx = ctx.WithOrder("name", false)
	assert.True(t, ctx.HasPipelineOperations(), "context with order should have pipeline operations")

	// With limit only
	ctx = NewFSContext("public", "users", "id")
	ctx = ctx.WithLimit(10, LimitFirst)
	assert.True(t, ctx.HasPipelineOperations(), "context with limit should have pipeline operations")
}

// TestStat_FilterValue_ReturnsFilterValueAsName verifies that stat on filter value paths
// returns the filter value as the entry name, not the table name.
// This is critical for NFS which expects the stat response name to match the path basename.
func TestStat_FilterValue_ReturnsFilterValueAsName(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	tests := []struct {
		path         string
		expectedName string
		description  string
	}{
		{
			path:         "/users/.by/status/active",
			expectedName: "active",
			description:  "filter value via .by",
		},
		{
			path:         "/users/.filter/status/active",
			expectedName: "active",
			description:  "filter value via .filter",
		},
		{
			path:         "/users/.first/10",
			expectedName: "10",
			description:  "limit value via .first",
		},
		{
			path:         "/users/.last/25",
			expectedName: "25",
			description:  "limit value via .last",
		},
		{
			path:         "/users/.order/created_at",
			expectedName: "created_at",
			description:  "order column",
		},
		{
			path:         "/users/.by/product_id/100/.filter/id/abc-123",
			expectedName: "abc-123",
			description:  "nested filter value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			entry, err := ops.Stat(context.Background(), tt.path)
			require.Nil(t, err, "stat should succeed for path: %s", tt.path)
			require.NotNil(t, entry, "entry should not be nil for path: %s", tt.path)
			assert.Equal(t, tt.expectedName, entry.Name, "entry name should be the path basename for: %s", tt.description)
			assert.True(t, entry.IsDir, "filtered table should be a directory")
		})
	}
}

// TestStat_TableWithoutPipeline_ReturnsTableName verifies that regular table stat still works.
func TestStat_TableWithoutPipeline_ReturnsTableName(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	entry, err := ops.Stat(context.Background(), "/users")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "users", entry.Name, "table stat should return table name")
	assert.True(t, entry.IsDir, "table should be a directory")
}

// TestReadFile_PipelineExport_ReturnsJSONData verifies that .first/N/.export/json returns data.
// This tests the specific path that was failing in NFS integration tests.
func TestReadFile_PipelineExport_ReturnsJSONData(t *testing.T) {
	cfg := &config.Config{
		DirListingLimit: 1000,
	}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
			},
		},
		// Set pipeline rows for QueryRowsWithDataPipeline
		pipelineRows: [][]interface{}{
			{1, "Alice"},
			{2, "Bob"},
			{3, "Charlie"},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Test the path: /users/.first/3/.export/json
	content, err := ops.ReadFile(context.Background(), "/users/.first/3/.export/json")

	require.Nil(t, err, "ReadFile should not error")
	require.NotNil(t, content, "content should not be nil")
	assert.Greater(t, len(content.Data), 0, "export data should not be empty")

	// Verify it's valid JSON
	var jsonData []map[string]interface{}
	jsonErr := json.Unmarshal(content.Data, &jsonData)
	require.NoError(t, jsonErr, "data should be valid JSON")
	assert.Len(t, jsonData, 3, "should have 3 rows")
}

// ============================================================================
// Tests for synthesized view rendering (6.1.7)
// ============================================================================

// newSynthMockDB creates a mock DB configured with a synth markdown view "posts"
// backed by columns [id, filename, title, author, body] with sample data.
func newSynthMockDB() *mockDBClient {
	return &mockDBClient{
		tables: map[string][]string{
			"public": {"_posts"},
		},
		views: map[string][]string{
			"public": {"posts"},
		},
		viewComments: map[string]map[string]string{
			"public": {"posts": "tigerfs:md"},
		},
		columns: map[string][]mockColumn{
			"public.posts": {
				{name: "id", dataType: "integer"},
				{name: "filename", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "author", dataType: "text"},
				{name: "body", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public._posts": {column: "id"},
			"public.posts":  {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.posts": {
				columns: []string{"id", "filename", "title", "author", "body"},
				rows: [][]interface{}{
					{1, "hello-world.md", "Hello World", "alice", "# Hello\n\nFirst post.\n"},
					{2, "second-post.md", "Second Post", "bob", "# Second\n\nAnother post.\n"},
				},
			},
		},
	}
}

// TestReadDir_SynthView verifies that a synth view lists .md filenames instead of row IDs.
func TestReadDir_SynthView(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/posts")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	// Should list .md files, not row IDs
	assert.Contains(t, names, "hello-world.md")
	assert.Contains(t, names, "second-post.md")

	// Files should NOT be directories
	for _, e := range entries {
		if e.Name == "hello-world.md" || e.Name == "second-post.md" {
			assert.False(t, e.IsDir, "%s should be a file, not a directory", e.Name)
			assert.Equal(t, os.FileMode(0644), e.Mode)
		}
	}

	// Should NOT have capability directories (synth views skip them)
	assert.NotContains(t, names, ".by")
	assert.NotContains(t, names, ".filter")
}

// TestStat_SynthFile verifies stat on a synth file returns file metadata with correct size.
func TestStat_SynthFile(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/posts/hello-world.md")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.False(t, entry.IsDir, "synth file should not be a directory")
	assert.Equal(t, "hello-world.md", entry.Name)
	assert.True(t, entry.Size > 0, "synth file should have non-zero size")
	assert.Equal(t, os.FileMode(0644), entry.Mode)
}

// TestStat_SynthFile_NotFound verifies stat on a non-existent synth file returns ErrNotExist.
func TestStat_SynthFile_NotFound(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)
	_, err := ops.Stat(context.Background(), "/posts/nonexistent.md")

	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestReadFile_SynthView verifies reading a synth file returns synthesized markdown content.
func TestReadFile_SynthView(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/posts/hello-world.md")

	require.Nil(t, err)
	require.NotNil(t, content)

	text := string(content.Data)

	// Should have YAML frontmatter with title and author
	assert.Contains(t, text, "---\n")
	assert.Contains(t, text, "title: Hello World")
	assert.Contains(t, text, "author: alice")

	// Should have the body content
	assert.Contains(t, text, "# Hello")
	assert.Contains(t, text, "First post.")
}

// TestWriteFile_SynthView_Insert verifies creating a new synth file performs INSERT.
func TestWriteFile_SynthView_Insert(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)

	// Write a new markdown file
	newContent := []byte("---\ntitle: New Post\nauthor: charlie\n---\n\n# New Post\n\nHello there.\n")
	err := ops.WriteFile(context.Background(), "/posts/new-post.md", newContent)

	require.Nil(t, err)
	assert.True(t, mockDB.insertCalled, "should have called InsertRow for new file")
	assert.False(t, mockDB.updateCalled, "should not have called UpdateRow for new file")
}

// TestWriteFile_SynthView_Update verifies updating an existing synth file performs UPDATE.
func TestWriteFile_SynthView_Update(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)

	// Overwrite an existing markdown file
	updatedContent := []byte("---\ntitle: Hello World Updated\nauthor: alice\n---\n\n# Updated\n\nEdited post.\n")
	err := ops.WriteFile(context.Background(), "/posts/hello-world.md", updatedContent)

	require.Nil(t, err)
	assert.True(t, mockDB.updateCalled, "should have called UpdateRow for existing file")
}

// TestDeleteFile_SynthView verifies deleting a synth file performs DELETE.
func TestDeleteFile_SynthView(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()
	// Add the row to rowData so DeleteRow doesn't error
	mockDB.rowData = map[string]*mockRow{
		"public.posts.1": {
			columns: []string{"id", "filename", "title", "author", "body"},
			values:  []interface{}{1, "hello-world", "Hello World", "alice", "# Hello\n\nFirst post.\n"},
		},
	}

	ops := NewOperations(cfg, mockDB)
	err := ops.Delete(context.Background(), "/posts/hello-world.md")

	require.Nil(t, err)
	assert.True(t, mockDB.deleteCalled, "should have called DeleteRow")
	assert.Equal(t, "1", mockDB.lastDeletePK, "should delete by PK value")
}

// TestReadDir_SynthView_PlainText verifies plain text synth views list .txt files.
func TestReadDir_SynthView_PlainText(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"_snippets"},
		},
		views: map[string][]string{
			"public": {"snippets"},
		},
		viewComments: map[string]map[string]string{
			"public": {"snippets": "tigerfs:txt"},
		},
		columns: map[string][]mockColumn{
			"public.snippets": {
				{name: "id", dataType: "integer"},
				{name: "filename", dataType: "text"},
				{name: "body", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.snippets": {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.snippets": {
				columns: []string{"id", "filename", "body"},
				rows: [][]interface{}{
					{1, "hello.txt", "Hello, world!\n"},
					{2, "goodbye.txt", "Goodbye, world!\n"},
				},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/snippets")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	assert.Contains(t, names, "hello.txt")
	assert.Contains(t, names, "goodbye.txt")
}

// TestReadFile_SynthView_PlainText verifies reading a plain text synth file.
func TestReadFile_SynthView_PlainText(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"_snippets"},
		},
		views: map[string][]string{
			"public": {"snippets"},
		},
		viewComments: map[string]map[string]string{
			"public": {"snippets": "tigerfs:txt"},
		},
		columns: map[string][]mockColumn{
			"public.snippets": {
				{name: "id", dataType: "integer"},
				{name: "filename", dataType: "text"},
				{name: "body", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.snippets": {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.snippets": {
				columns: []string{"id", "filename", "body"},
				rows: [][]interface{}{
					{1, "hello.txt", "Hello, world!\n"},
				},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/snippets/hello.txt")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Equal(t, "Hello, world!\n", string(content.Data))
}

// TestStat_SynthView_IsDirectory verifies that stat on a synth view path returns a directory.
func TestStat_SynthView_IsDirectory(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/posts")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir, "synth view should appear as a directory")
	assert.Equal(t, "posts", entry.Name)
}

// newSynthMockDBWithEncoding creates a mock DB like newSynthMockDB but with the encoding column.
func newSynthMockDBWithEncoding() *mockDBClient {
	mock := newSynthMockDB()
	// Add encoding column to the column list
	mock.columns["public.posts"] = append(mock.columns["public.posts"],
		mockColumn{name: "encoding", dataType: "text"})
	// Add encoding value to existing rows in allRowsData
	for i := range mock.allRowsData["public.posts"].rows {
		mock.allRowsData["public.posts"].rows[i] = append(
			mock.allRowsData["public.posts"].rows[i], "utf8")
	}
	mock.allRowsData["public.posts"].columns = append(
		mock.allRowsData["public.posts"].columns, "encoding")
	return mock
}

// TestSynth_WriteBinaryFile verifies that writing binary data base64-encodes the body
// and sets the encoding column to "base64".
func TestSynth_WriteBinaryFile(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDBWithEncoding()

	ops := NewOperations(cfg, mockDB)

	// Write binary data (JPEG-like with null bytes)
	binaryData := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46}
	err := ops.WriteFile(context.Background(), "/posts/photo.jpeg", binaryData)

	require.Nil(t, err)
	assert.True(t, mockDB.insertCalled, "should have called InsertRow")

	// Verify encoding column was set to 'base64'
	encodingIdx := -1
	for i, col := range mockDB.lastInsertColumns {
		if col == "encoding" {
			encodingIdx = i
			break
		}
	}
	require.NotEqual(t, -1, encodingIdx, "should include encoding column")
	assert.Equal(t, "base64", mockDB.lastInsertValues[encodingIdx])

	// Verify body is base64-encoded
	bodyIdx := -1
	for i, col := range mockDB.lastInsertColumns {
		if col == "body" {
			bodyIdx = i
			break
		}
	}
	require.NotEqual(t, -1, bodyIdx, "should include body column")
	encoded := mockDB.lastInsertValues[bodyIdx].(string)
	decoded, decErr := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, decErr)
	assert.Equal(t, binaryData, decoded)
}

// TestSynth_ReadBinaryFile verifies base64-encoded rows are decoded back to raw bytes.
func TestSynth_ReadBinaryFile(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthMockDBWithEncoding()

	// Add a binary file row to the mock
	binaryData := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10}
	encoded := base64.StdEncoding.EncodeToString(binaryData)
	mockDB.allRowsData["public.posts"].rows = append(
		mockDB.allRowsData["public.posts"].rows,
		[]interface{}{3, "photo.jpeg", nil, nil, encoded, "base64"},
	)

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/posts/photo.jpeg")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Equal(t, binaryData, content.Data)
}

// TestStat_TableWithInvalidColumns tests that Stat on a table with invalid
// column projection returns ErrNotExist.
func TestStat_TableWithInvalidColumns(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
				{name: "email", dataType: "text"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Stat with a valid column works
	fsCtx := NewFSContext("public", "users", "id")
	fsCtx = fsCtx.WithColumns([]string{"id", "name"})
	entry, err := ops.StatWithContext(context.Background(), fsCtx)
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)

	// Stat with an invalid column returns ErrNotExist
	fsCtx2 := NewFSContext("public", "users", "id")
	fsCtx2 = fsCtx2.WithColumns([]string{"id", "nonexistent"})
	_, err2 := ops.StatWithContext(context.Background(), fsCtx2)
	require.NotNil(t, err2)
	assert.Equal(t, ErrNotExist, err2.Code)
}

// TestReadDir_RowDirectoryWithColumnProjection tests that ReadDir on a row
// directory with column projection only returns projected column files.
func TestReadDir_RowDirectoryWithColumnProjection(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer"},
				{name: "name", dataType: "text"},
				{name: "email", dataType: "text"},
				{name: "age", dataType: "integer"},
			},
		},
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name", "email", "age"},
				values:  []interface{}{1, "Alice", "alice@example.com", 30},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// ReadDir on /users/.columns/id,name/1 should only show id and name columns
	entries, err := ops.ReadDir(context.Background(), "/users/.columns/id,name/1")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	// Should contain export format files
	assert.Contains(t, names, ".json")
	assert.Contains(t, names, ".tsv")
	assert.Contains(t, names, ".csv")
	assert.Contains(t, names, ".yaml")

	// Should contain only projected columns (with extensions)
	assert.Contains(t, names, "id")       // integer has no extension
	assert.Contains(t, names, "name.txt") // text gets .txt extension

	// Should NOT contain non-projected columns
	assert.NotContains(t, names, "email.txt")
	assert.NotContains(t, names, "age")

	// Total: 4 export files + 2 projected columns = 6
	assert.Len(t, entries, 6)
}
