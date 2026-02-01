package fs

import (
	"context"
	"fmt"
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
	}

	ops := NewOperations(cfg, mockDB)
	entry, err := ops.Stat(context.Background(), "/users")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, "users", entry.Name)
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

	assert.Contains(t, names, ".count")
	assert.Contains(t, names, ".ddl")
	assert.Contains(t, names, ".columns")
	assert.Contains(t, names, ".indexes")
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
	content, err := ops.ReadFile(context.Background(), "/users/.info/.count")

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
	entry, err := ops.Stat(context.Background(), "/users/.info/.count")

	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.False(t, entry.IsDir)
	assert.Equal(t, ".count", entry.Name)
}

// TestStat_DDLFile tests Stat on a DDL control file.
func TestStat_DDLFile(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Test sql file
	entry, err := ops.Stat(context.Background(), "/.create/myindex/sql")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "sql", entry.Name)

	// Test .test file
	entry, err = ops.Stat(context.Background(), "/.create/myindex/.test")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, ".test", entry.Name)

	// Test .commit file
	entry, err = ops.Stat(context.Background(), "/.create/myindex/.commit")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, ".commit", entry.Name)
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
	content, err := ops.ReadFile(context.Background(), "/users/.info/.ddl")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "CREATE TABLE")
}

// TestReadFile_InfoColumns tests reading the .columns metadata file.
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
	content, err := ops.ReadFile(context.Background(), "/users/.info/.columns")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "id\tinteger")
	assert.Contains(t, string(content.Data), "name\ttext")
}

// TestReadFile_InfoIndexes tests reading the .indexes metadata file.
func TestReadFile_InfoIndexes(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		indexes: map[string][]mockIndex{
			"public.users": {
				{name: "users_pkey", columns: []string{"id"}, unique: true},
				{name: "users_name_idx", columns: []string{"name"}, unique: false},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/users/.info/.indexes")

	require.Nil(t, err)
	require.NotNil(t, content)
	assert.Contains(t, string(content.Data), "UNIQUE users_pkey")
	assert.Contains(t, string(content.Data), "users_name_idx")
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
	assert.Contains(t, names, "all.csv")
	assert.Contains(t, names, "all.json")
	assert.Contains(t, names, "all.tsv")
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

	// Test .create directory
	entry, err := ops.Stat(context.Background(), "/.create")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
	assert.Equal(t, ".create", entry.Name)

	// Test named staging directory
	entry, err = ops.Stat(context.Background(), "/.create/myindex")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir)
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

	assert.Contains(t, names, "id")
	assert.Contains(t, names, "name")
	assert.Contains(t, names, "email")
}

// TestReadDir_DDL tests listing DDL staging directories.
func TestReadDir_DDL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Test listing .create directory (empty)
	entries, err := ops.ReadDir(context.Background(), "/.create")
	require.Nil(t, err)
	require.NotNil(t, entries)
	assert.Len(t, entries, 0)

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

// TestStatWithContext tests Stat using a pre-parsed FSContext.
func TestStatWithContext(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
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
	return nil, nil, nil
}

func (m *mockDBClient) GetFirstNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}

func (m *mockDBClient) GetLastNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	return nil, nil, nil
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
	return []string{}, nil
}

func (m *mockDBClient) QueryRowsWithDataPipeline(ctx context.Context, params db.QueryParams) ([]string, [][]interface{}, error) {
	return nil, nil, nil
}
