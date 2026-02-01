package fs

import (
	"context"
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
