package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// --- readDirSavepoint unit tests ---

func TestReadDirSavepoint_PassesAscendingForFirst(t *testing.T) {
	// Parse .savepoint/.first/3
	parsed, err := ParsePath("/notes/.savepoint/.first/3")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type)
	assert.Equal(t, LimitFirst, parsed.Context.LimitType)
	assert.Equal(t, 3, parsed.Context.Limit)
}

func TestReadDirSavepoint_PassesDescendingForLast(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.last/5")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type)
	assert.Equal(t, LimitLast, parsed.Context.LimitType)
	assert.Equal(t, 5, parsed.Context.Limit)
}

func TestReadDirSavepoint_PreservesTypeWithFilter(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.by/user_id/agent-7")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type, "Type should stay PathSavepoint through .by/ pipeline")
	require.Len(t, parsed.Context.Filters, 1)
	assert.Equal(t, "user_id", parsed.Context.Filters[0].Column)
	assert.Equal(t, "agent-7", parsed.Context.Filters[0].Value)
}

func TestReadDirSavepoint_PreservesTypeWithFilterAndLimit(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.by/user_id/agent-7/.last/3")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type, "Type should stay PathSavepoint through .by/.last chain")
	assert.Equal(t, LimitLast, parsed.Context.LimitType)
	assert.Equal(t, 3, parsed.Context.Limit)
}

// --- writeSavepoint unit tests ---

func TestWriteSavepoint_CreateWithDescription(t *testing.T) {
	mockDB := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "savepoint_id"},
		},
		lastInsertReturnPK: "sp-uuid-1",
	}
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ops.SetUserID("agent-7")

	parsed := &ParsedPath{
		Type:          PathRow,
		Context:       &FSContext{Schema: "tigerfs", TableName: "notes_savepoint"},
		PrimaryKey:    "my-checkpoint",
		OrigTableName: "notes",
	}

	fsErr := ops.writeSavepoint(context.Background(), parsed, []byte("Before refactoring\n"))
	require.Nil(t, fsErr)

	// Verify INSERT was called with name, description, user_id
	require.Len(t, mockDB.insertedRows, 1)
	row := mockDB.insertedRows[0]

	nameIdx := -1
	descIdx := -1
	userIdx := -1
	for i, col := range row.columns {
		switch col {
		case "name":
			nameIdx = i
		case "description":
			descIdx = i
		case "user_id":
			userIdx = i
		}
	}

	require.GreaterOrEqual(t, nameIdx, 0, "should have name column")
	assert.Equal(t, "my-checkpoint", row.values[nameIdx])

	require.GreaterOrEqual(t, descIdx, 0, "should have description column")
	assert.Equal(t, "Before refactoring", row.values[descIdx])

	require.GreaterOrEqual(t, userIdx, 0, "should have user_id column")
	assert.Equal(t, "agent-7", row.values[userIdx])
}

func TestWriteSavepoint_CreateWithoutDescription(t *testing.T) {
	mockDB := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "savepoint_id"},
		},
		lastInsertReturnPK: "sp-uuid-1",
	}
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	parsed := &ParsedPath{
		Type:          PathRow,
		Context:       &FSContext{Schema: "tigerfs", TableName: "notes_savepoint"},
		PrimaryKey:    "quick-mark",
		OrigTableName: "notes",
	}

	fsErr := ops.writeSavepoint(context.Background(), parsed, []byte(""))
	require.Nil(t, fsErr)

	require.Len(t, mockDB.insertedRows, 1)
	row := mockDB.insertedRows[0]

	// Should have name but NOT description (empty data = touch)
	hasDesc := false
	for _, col := range row.columns {
		if col == "description" {
			hasDesc = true
		}
	}
	assert.False(t, hasDesc, "touch (empty data) should not include description column")
}

func TestWriteSavepoint_CreateWithoutUserID(t *testing.T) {
	mockDB := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "savepoint_id"},
		},
		lastInsertReturnPK: "sp-uuid-1",
	}
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	// No SetUserID -- anonymous

	parsed := &ParsedPath{
		Type:          PathRow,
		Context:       &FSContext{Schema: "tigerfs", TableName: "notes_savepoint"},
		PrimaryKey:    "anon-save",
		OrigTableName: "notes",
	}

	fsErr := ops.writeSavepoint(context.Background(), parsed, []byte(""))
	require.Nil(t, fsErr)

	require.Len(t, mockDB.insertedRows, 1)
	row := mockDB.insertedRows[0]

	hasUserID := false
	for _, col := range row.columns {
		if col == "user_id" {
			hasUserID = true
		}
	}
	assert.False(t, hasUserID, "anonymous user should not include user_id column")
}

// --- deleteSavepoint unit tests ---

func TestDeleteSavepoint_ByName(t *testing.T) {
	mockDB := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "savepoint_id"},
		},
		// GetRowByColumns returns this row when looking up by name
		rowByColumnsData: map[string]*mockRowByColumns{
			"tigerfs.notes_savepoint.name=my-save": {
				columns: []string{"savepoint_id", "user_id", "name", "description"},
				values:  []interface{}{"sp-uuid-1", "agent-7", "my-save", "checkpoint"},
			},
		},
		// DeleteRow checks rowData for existence
		rowData: map[string]*mockRow{
			"tigerfs.notes_savepoint.sp-uuid-1": {
				columns: []string{"savepoint_id"},
				values:  []interface{}{"sp-uuid-1"},
			},
		},
	}
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	parsed := &ParsedPath{
		Type:          PathRow,
		Context:       &FSContext{Schema: "tigerfs", TableName: "notes_savepoint"},
		PrimaryKey:    "my-save",
		OrigTableName: "notes",
	}

	fsErr := ops.deleteSavepoint(context.Background(), parsed)
	require.Nil(t, fsErr)
	assert.True(t, mockDB.deleteCalled, "should call DeleteRow")
}

// --- getSavepointRowByName unit tests ---

func TestGetSavepointRowByName_Found(t *testing.T) {
	mockDB := &mockDBClient{
		rowByColumnsData: map[string]*mockRowByColumns{
			"tigerfs.notes_savepoint.name=before-exploration": {
				columns: []string{"savepoint_id", "name", "description"},
				values:  []interface{}{"sp-uuid-1", "before-exploration", "Starting exploration"},
			},
		},
	}
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	columns, row, err := ops.getSavepointRowByName(context.Background(), "tigerfs", "notes_savepoint", "before-exploration")
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "name", columns[1])
	assert.Equal(t, "before-exploration", row[1])
}

func TestGetSavepointRowByName_NotFound(t *testing.T) {
	mockDB := &mockDBClient{}
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	_, row, err := ops.getSavepointRowByName(context.Background(), "tigerfs", "notes_savepoint", "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, row)
}
