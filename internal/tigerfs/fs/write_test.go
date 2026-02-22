package fs

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestWriteFile_UpdateRow tests updating an existing row.
func TestWriteFile_UpdateRow(t *testing.T) {
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

	// Write JSON data to update row
	data := []byte(`{"name": "Alice Updated"}`)
	err := ops.WriteFile(context.Background(), "/users/1.json", data)

	require.Nil(t, err)
	// Verify update was called (check mock state)
	assert.True(t, mockDB.updateCalled)
	assert.Equal(t, "name", mockDB.lastUpdateColumn)
}

// TestWriteFile_InsertRow tests inserting a new row.
func TestWriteFile_InsertRow(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		// No existing row data for new row
	}

	ops := NewOperations(cfg, mockDB)

	// Write JSON data to insert new row
	data := []byte(`{"id": 99, "name": "New User"}`)
	err := ops.WriteFile(context.Background(), "/users/99.json", data)

	require.Nil(t, err)
	assert.True(t, mockDB.insertCalled)
}

// TestWriteFile_UpdateColumn tests updating a single column.
func TestWriteFile_UpdateColumn(t *testing.T) {
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
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name"},
				values:  []interface{}{1, "Alice"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	// Write data to update single column
	data := []byte("Bob\n")
	err := ops.WriteFile(context.Background(), "/users/1/name", data)

	require.Nil(t, err)
	assert.True(t, mockDB.updateColumnCalled)
	assert.Equal(t, "name", mockDB.lastColumnName)
	assert.Equal(t, "Bob", mockDB.lastColumnValue)
}

// TestDelete_Row tests deleting a row.
func TestDelete_Row(t *testing.T) {
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

	err := ops.Delete(context.Background(), "/users/1.json")

	require.Nil(t, err)
	assert.True(t, mockDB.deleteCalled)
	assert.Equal(t, "1", mockDB.lastDeletePK)
}

// TestDelete_Column tests setting a column to NULL.
func TestDelete_Column(t *testing.T) {
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
		rowData: map[string]*mockRow{
			"public.users.1": {
				columns: []string{"id", "name"},
				values:  []interface{}{1, "Alice"},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	err := ops.Delete(context.Background(), "/users/1/name")

	require.Nil(t, err)
	assert.True(t, mockDB.updateColumnCalled)
	assert.Equal(t, "name", mockDB.lastColumnName)
	assert.Equal(t, "", mockDB.lastColumnValue) // NULL is represented as empty string
}

// TestMkdir_RowDirectory tests creating a row directory for incremental creation.
func TestMkdir_RowDirectory(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		// No existing row - this is a new row
	}

	ops := NewOperations(cfg, mockDB)

	err := ops.Mkdir(context.Background(), "/users/99")

	require.Nil(t, err)
	// Should create a staging entry for the new row
	assert.True(t, ops.staging != nil)
}

// TestCreate_RowFile tests creating a new row file.
func TestCreate_RowFile(t *testing.T) {
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

	handle, err := ops.Create(context.Background(), "/users/99.json")

	require.Nil(t, err)
	require.NotNil(t, handle)
	assert.Equal(t, "/users/99.json", handle.Path)
}

// TestWriteFile_CSV tests writing a row in CSV format.
func TestWriteFile_CSV(t *testing.T) {
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

	// CSV format with header
	data := []byte("id,name\n1,Bob\n")
	err := ops.WriteFile(context.Background(), "/users/1.csv", data)

	require.Nil(t, err)
	assert.True(t, mockDB.updateCalled)
}

// TestWriteFile_TSV tests writing a row in TSV format.
func TestWriteFile_TSV(t *testing.T) {
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

	// TSV format with header
	data := []byte("id\tname\n1\tBob\n")
	err := ops.WriteFile(context.Background(), "/users/1.tsv", data)

	require.Nil(t, err)
	assert.True(t, mockDB.updateCalled)
}

// TestWriteFile_ImportSync tests bulk sync import.
func TestWriteFile_ImportSync(t *testing.T) {
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

	// Write CSV data to sync import
	data := []byte("id,name\n1,Alice\n2,Bob\n")
	err := ops.WriteFile(context.Background(), "/users/.import/.sync/data.csv", data)

	require.Nil(t, err)
	assert.True(t, mockDB.importSyncCalled)
}

// TestWriteFile_PermissionDenied tests write to read-only view.
func TestWriteFile_PermissionDenied(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		views: map[string][]string{
			"public": {"readonly_view"},
		},
		primaryKeys: map[string]*mockPK{
			"public.readonly_view": {column: "id"},
		},
		viewUpdatable: map[string]bool{
			"public.readonly_view": false,
		},
	}

	ops := NewOperations(cfg, mockDB)

	data := []byte(`{"name": "test"}`)
	err := ops.WriteFile(context.Background(), "/readonly_view/1.json", data)

	require.NotNil(t, err)
	assert.Equal(t, ErrPermission, err.Code)
}

// TestWriteFile_InvalidPath tests writing to invalid path.
func TestWriteFile_InvalidPath(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	data := []byte("test")
	err := ops.WriteFile(context.Background(), "/", data)

	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
}

// TestDelete_NotFound tests deleting non-existent row.
func TestDelete_NotFound(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"users"},
		},
		primaryKeys: map[string]*mockPK{
			"public.users": {column: "id"},
		},
		// No row data - row doesn't exist
	}

	ops := NewOperations(cfg, mockDB)

	err := ops.Delete(context.Background(), "/users/999.json")

	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestMkdir_PathDDL tests creating a DDL staging session.
func TestMkdir_PathDDL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	err := ops.Mkdir(context.Background(), "/.create/test_orders")

	require.Nil(t, err)
	// Verify session was created
	sessionID := ops.ddl.FindSessionByName(DDLCreate, "test_orders")
	assert.NotEmpty(t, sessionID)
}

// TestMkdir_PathDDL_AlreadyExists tests that duplicate DDL mkdir returns ErrExists.
func TestMkdir_PathDDL_AlreadyExists(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// First mkdir should succeed
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)

	// Second mkdir should fail with ErrExists
	err = ops.Mkdir(context.Background(), "/.create/test_orders")
	require.NotNil(t, err)
	assert.Equal(t, ErrExists, err.Code)
}

// TestMkdir_PathDDL_EmptyName tests that empty DDL name returns error.
func TestMkdir_PathDDL_EmptyName(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	err := ops.Mkdir(context.Background(), "/.create/")

	require.NotNil(t, err)
	assert.Equal(t, ErrInvalidPath, err.Code)
}

// TestWriteDDLFile_SQL tests writing DDL content.
func TestWriteDDLFile_SQL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Create session first
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)

	// Write SQL content
	sql := "CREATE TABLE test_orders (id SERIAL PRIMARY KEY);"
	err = ops.WriteFile(context.Background(), "/.create/test_orders/sql", []byte(sql))
	require.Nil(t, err)

	// Verify content was written
	sessionID := ops.ddl.FindSessionByName(DDLCreate, "test_orders")
	assert.Equal(t, sql, ops.ddl.GetSQL(sessionID))
}

// TestWriteDDLFile_Test tests triggering DDL validation.
func TestWriteDDLFile_Test(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		execInTxSuccess: true,
	}

	ops := NewOperations(cfg, mockDB)

	// Create session and write SQL
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)
	err = ops.WriteFile(context.Background(), "/.create/test_orders/sql", []byte("CREATE TABLE test_orders;"))
	require.Nil(t, err)

	// Trigger validation
	err = ops.WriteFile(context.Background(), "/.create/test_orders/.test", []byte(""))
	require.Nil(t, err)

	// Verify test log was populated
	sessionID := ops.ddl.FindSessionByName(DDLCreate, "test_orders")
	log := ops.ddl.GetTestLog(sessionID)
	assert.Contains(t, log, "OK")
}

// TestWriteDDLFile_Commit tests executing DDL.
func TestWriteDDLFile_Commit(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		execSuccess: true,
	}

	ops := NewOperations(cfg, mockDB)

	// Create session and write SQL
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)
	err = ops.WriteFile(context.Background(), "/.create/test_orders/sql", []byte("CREATE TABLE test_orders;"))
	require.Nil(t, err)

	// Commit
	err = ops.WriteFile(context.Background(), "/.create/test_orders/.commit", []byte(""))
	require.Nil(t, err)

	// Verify session was marked completed (not removed — kept for grace period)
	sessionID := ops.ddl.FindSessionByName(DDLCreate, "test_orders")
	assert.NotEmpty(t, sessionID)
	session := ops.ddl.GetSession(sessionID)
	require.NotNil(t, session)
	assert.True(t, session.Completed)
	assert.True(t, mockDB.execCalled)
}

// TestWriteDDLFile_Abort tests canceling a DDL session.
func TestWriteDDLFile_Abort(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Create session
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)

	// Abort
	err = ops.WriteFile(context.Background(), "/.create/test_orders/.abort", []byte(""))
	require.Nil(t, err)

	// Verify session was marked completed (not removed — kept for grace period)
	sessionID := ops.ddl.FindSessionByName(DDLCreate, "test_orders")
	assert.NotEmpty(t, sessionID)
	session := ops.ddl.GetSession(sessionID)
	require.NotNil(t, session)
	assert.True(t, session.Completed)
}

// TestWriteDDLFile_NoSession tests error when session doesn't exist.
func TestWriteDDLFile_NoSession(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Try to write to non-existent session
	err := ops.WriteFile(context.Background(), "/.create/nonexistent/sql", []byte("test"))

	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestReadDDLFile_SQL tests reading DDL content.
func TestReadDDLFile_SQL(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Create session and write SQL
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)
	sql := "CREATE TABLE test_orders (id SERIAL PRIMARY KEY);"
	err = ops.WriteFile(context.Background(), "/.create/test_orders/sql", []byte(sql))
	require.Nil(t, err)

	// Read SQL content
	content, fsErr := ops.ReadFile(context.Background(), "/.create/test_orders/sql")
	require.Nil(t, fsErr)
	assert.Equal(t, sql, string(content.Data))
}

// TestReadDDLFile_SQLTemplate tests reading DDL template when no content written.
func TestReadDDLFile_SQLTemplate(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Create session without writing SQL
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)

	// Read SQL content - should return template
	content, fsErr := ops.ReadFile(context.Background(), "/.create/test_orders/sql")
	require.Nil(t, fsErr)
	assert.Contains(t, string(content.Data), "CREATE TABLE")
	assert.Contains(t, string(content.Data), "test_orders")
}

// TestReadDDLFile_TestLog tests reading test results.
func TestReadDDLFile_TestLog(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		execInTxSuccess: true,
	}

	ops := NewOperations(cfg, mockDB)

	// Create session, write SQL, and run test
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)
	err = ops.WriteFile(context.Background(), "/.create/test_orders/sql", []byte("CREATE TABLE test_orders;"))
	require.Nil(t, err)
	err = ops.WriteFile(context.Background(), "/.create/test_orders/.test", []byte(""))
	require.Nil(t, err)

	// Read test log
	content, fsErr := ops.ReadFile(context.Background(), "/.create/test_orders/test.log")
	require.Nil(t, fsErr)
	assert.Contains(t, string(content.Data), "OK")
}

// TestReadDDLFile_TestLogBeforeTest tests reading test.log before test was run.
func TestReadDDLFile_TestLogBeforeTest(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Create session without running test
	err := ops.Mkdir(context.Background(), "/.create/test_orders")
	require.Nil(t, err)

	// Read test log - should return ErrNotExist
	_, fsErr := ops.ReadFile(context.Background(), "/.create/test_orders/test.log")
	require.NotNil(t, fsErr)
	assert.Equal(t, ErrNotExist, fsErr.Code)
}

// TestSynth_Rename_AtomicCAS tests that synth file rename uses compare-and-swap
// so that concurrent renames of the same file have exactly one winner.
// The loser's UpdateColumnCAS should return "row not found" → ErrNotExist.
func TestSynth_Rename_AtomicCAS(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"notes"},
		},
		views: map[string][]string{
			"public": {"notes"},
		},
		viewUpdatable: map[string]bool{
			"public.notes": true,
		},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md"},
		},
		columns: map[string][]mockColumn{
			"public.notes": {
				{name: "id", dataType: "integer"},
				{name: "filename", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "body", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.notes": {column: "id", columns: []string{"id"}},
		},
		allRowsData: map[string]*mockAllRows{
			"public.notes": {
				columns: []string{"id", "filename", "title", "body"},
				rows: [][]interface{}{
					{1, "task1.md", "task1", "content"},
				},
			},
		},
	}

	// Simulate the second concurrent rename: UpdateColumnCAS returns "row not found"
	// because the first rename already changed the filename.
	mockDB.updateColumnCASError = fmt.Errorf("row not found")

	ops := NewOperations(cfg, mockDB)

	// Flat synth rename: task1.md → task2.md
	err := ops.Rename(context.Background(), "/notes/task1.md", "/notes/task2.md")

	require.NotNil(t, err, "expected error but got nil")
	assert.Equal(t, ErrNotExist, err.Code)
	assert.True(t, mockDB.updateColumnCASCalled, "should use UpdateColumnCAS, not UpdateColumn")
}

// TestSynth_Rename_DirAtomicCheck tests that synth directory rename returns
// ErrNotExist when RenameByPrefix affects 0 rows (concurrent loser).
func TestSynth_Rename_DirAtomicCheck(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"notes"},
		},
		views: map[string][]string{
			"public": {"notes"},
		},
		viewUpdatable: map[string]bool{
			"public.notes": true,
		},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md:filetype"},
		},
		columns: map[string][]mockColumn{
			"public.notes": {
				{name: "id", dataType: "integer"},
				{name: "title", dataType: "text"},
				{name: "body", dataType: "text"},
				{name: "filetype", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.notes": {column: "id", columns: []string{"id"}},
		},
		allRowsData: map[string]*mockAllRows{
			"public.notes": {
				columns: []string{"id", "title", "body", "filetype"},
				rows: [][]interface{}{
					{1, "todo", "", "directory"},
					{2, "todo/task1", "content", "file"},
				},
			},
		},
		// RenameByPrefix returns 0 rows affected — another agent already moved it
		renameByPrefixRows: 0,
	}

	ops := NewOperations(cfg, mockDB)

	err := ops.Rename(context.Background(), "/notes/todo", "/notes/doing")

	require.NotNil(t, err)
	assert.Equal(t, ErrNotExist, err.Code)
}

// TestSynth_Rename_FilenameWithExtensionInDB tests that CAS rename works when
// the DB stores filenames WITH the .md extension (e.g., "hello-world.md"),
// as the blog demo app does. The WHERE clause must use the actual DB value,
// not the extension-stripped version.
func TestSynth_Rename_FilenameWithExtensionInDB(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{
		tables: map[string][]string{
			"public": {"blog"},
		},
		views: map[string][]string{
			"public": {"blog"},
		},
		viewUpdatable: map[string]bool{
			"public.blog": true,
		},
		viewComments: map[string]map[string]string{
			"public": {"blog": "tigerfs:md"},
		},
		columns: map[string][]mockColumn{
			"public.blog": {
				{name: "id", dataType: "uuid"},
				{name: "title", dataType: "text"},
				{name: "body", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public.blog": {column: "id", columns: []string{"id"}},
		},
		allRowsData: map[string]*mockAllRows{
			"public.blog": {
				// DB stores "hello-world.md" WITH the .md extension
				columns: []string{"id", "title", "body"},
				rows: [][]interface{}{
					{"abc-123", "hello-world.md", "content"},
				},
			},
		},
	}

	ops := NewOperations(cfg, mockDB)

	err := ops.Rename(context.Background(), "/blog/hello-world.md", "/blog/hello.md")

	require.Nil(t, err, "rename should succeed")
	assert.True(t, mockDB.updateColumnCASCalled)
	// CAS WHERE value must be the actual DB value "hello-world.md", not "hello-world"
	assert.Equal(t, "hello-world.md", mockDB.updateColumnCASWhereVal,
		"CAS should use actual DB value (with .md), not stripped version")
	// New value should also preserve the .md extension since the DB convention includes it
	assert.Equal(t, "hello.md", mockDB.updateColumnCASNewVal,
		"new filename should preserve DB extension convention")
}

// TestReadDDLFile_NoSession tests reading DDL file without session.
func TestReadDDLFile_NoSession(t *testing.T) {
	cfg := &config.Config{}
	mockDB := &mockDBClient{}

	ops := NewOperations(cfg, mockDB)

	// Try to read from non-existent session
	_, fsErr := ops.ReadFile(context.Background(), "/.create/nonexistent/sql")

	require.NotNil(t, fsErr)
	assert.Equal(t, ErrNotExist, fsErr.Code)
}
