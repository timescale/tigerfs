package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewStagingManager tests the StagingManager constructor.
func TestNewStagingManager(t *testing.T) {
	sm := NewStagingManager()
	require.NotNil(t, sm)
	assert.NotNil(t, sm.rows)
}

// TestStagingManager_GetOrCreate tests creating and retrieving partial rows.
func TestStagingManager_GetOrCreate(t *testing.T) {
	sm := NewStagingManager()

	// Create first partial row
	row1 := sm.GetOrCreate("public", "users", "id", "1")
	require.NotNil(t, row1)
	assert.Equal(t, "public", row1.Schema)
	assert.Equal(t, "users", row1.Table)
	assert.Equal(t, "id", row1.PKColumn)
	assert.Equal(t, "1", row1.PKValue)
	assert.False(t, row1.Committed)

	// Get same row again
	row2 := sm.GetOrCreate("public", "users", "id", "1")
	assert.Same(t, row1, row2) // Same pointer

	// Create different row
	row3 := sm.GetOrCreate("public", "users", "id", "2")
	assert.NotSame(t, row1, row3)
}

// TestStagingManager_Get tests retrieving partial rows.
func TestStagingManager_Get(t *testing.T) {
	sm := NewStagingManager()

	// Get non-existent row
	row := sm.Get("public", "users", "1")
	assert.Nil(t, row)

	// Create row
	sm.GetOrCreate("public", "users", "id", "1")

	// Get existing row
	row = sm.Get("public", "users", "1")
	assert.NotNil(t, row)
}

// TestStagingManager_SetColumn tests setting column values.
func TestStagingManager_SetColumn(t *testing.T) {
	sm := NewStagingManager()

	// Set column on non-existent row (should create it)
	sm.SetColumn("public", "users", "id", "1", "name", "Alice")

	row := sm.Get("public", "users", "1")
	require.NotNil(t, row)
	assert.Equal(t, "Alice", row.Columns["name"])

	// Set another column
	sm.SetColumn("public", "users", "id", "1", "email", "alice@example.com")
	assert.Equal(t, "alice@example.com", row.Columns["email"])
}

// TestStagingManager_GetColumnValue tests getting column values.
func TestStagingManager_GetColumnValue(t *testing.T) {
	sm := NewStagingManager()

	// Get from non-existent row
	val := sm.GetColumnValue("public", "users", "1", "name")
	assert.Nil(t, val)

	// Set and get
	sm.SetColumn("public", "users", "id", "1", "name", "Alice")
	val = sm.GetColumnValue("public", "users", "1", "name")
	assert.Equal(t, "Alice", val)

	// Get non-existent column
	val = sm.GetColumnValue("public", "users", "1", "nonexistent")
	assert.Nil(t, val)
}

// TestStagingManager_Remove tests removing partial rows.
func TestStagingManager_Remove(t *testing.T) {
	sm := NewStagingManager()

	// Create row
	sm.GetOrCreate("public", "users", "id", "1")
	assert.NotNil(t, sm.Get("public", "users", "1"))

	// Remove row
	sm.Remove("public", "users", "1")
	assert.Nil(t, sm.Get("public", "users", "1"))
}

// TestStagingManager_IsCommitted tests checking commit status.
func TestStagingManager_IsCommitted(t *testing.T) {
	sm := NewStagingManager()

	// Check non-existent row
	assert.False(t, sm.IsCommitted("public", "users", "1"))

	// Create row
	row := sm.GetOrCreate("public", "users", "id", "1")
	assert.False(t, sm.IsCommitted("public", "users", "1"))

	// Mark as committed
	row.Committed = true
	assert.True(t, sm.IsCommitted("public", "users", "1"))
}

// TestStagingManager_TryCommit tests committing partial rows.
func TestStagingManager_TryCommit(t *testing.T) {
	sm := NewStagingManager()
	mockDB := &mockDBClient{
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer", nullable: false},
				{name: "name", dataType: "text", nullable: false},
				{name: "email", dataType: "text", nullable: true},
			},
		},
	}

	// Create partial row with only PK
	sm.SetColumn("public", "users", "id", "1", "name", "Alice")

	// Try commit - should succeed (name is provided, id from PK, email is nullable)
	committed, err := sm.TryCommit(context.Background(), "public", "users", "id", "1", mockDB)
	require.NoError(t, err)
	assert.True(t, committed)
	assert.True(t, sm.IsCommitted("public", "users", "1"))
}

// TestStagingManager_TryCommit_MissingColumn tests commit with missing NOT NULL column.
func TestStagingManager_TryCommit_MissingColumn(t *testing.T) {
	sm := NewStagingManager()
	mockDB := &mockDBClient{
		columns: map[string][]mockColumn{
			"public.users": {
				{name: "id", dataType: "integer", nullable: false},
				{name: "name", dataType: "text", nullable: false}, // NOT NULL and not provided
			},
		},
	}

	// Create partial row without required column
	sm.GetOrCreate("public", "users", "id", "1")
	// Don't set name column

	// Try commit - should fail (name is NOT NULL but not provided)
	committed, err := sm.TryCommit(context.Background(), "public", "users", "id", "1", mockDB)
	require.NoError(t, err)
	assert.False(t, committed)
	assert.False(t, sm.IsCommitted("public", "users", "1"))
}
