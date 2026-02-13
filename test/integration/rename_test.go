package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMount_Rename_Row verifies that os.Rename on a native row directory
// updates the primary key in the database. This exercises the FUSE
// NodeRenamer (Linux) or NFS Rename (macOS) wiring end-to-end.
func TestMount_Rename_Row(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	// Verify row 1 exists
	name := ctx.queryColumn("write_test", 1, "name")
	require.Equal(t, "initial_name", name)

	oldPath := filepath.Join(ctx.mountPoint, "write_test", "1")
	newPath := filepath.Join(ctx.mountPoint, "write_test", "99")

	// Rename row: mv write_test/1 write_test/99
	err := os.Rename(oldPath, newPath)
	require.NoError(t, err, "os.Rename should succeed for PK update")

	// Verify old row is gone, new row has the data
	var count int
	ctx.db.QueryRow("SELECT COUNT(*) FROM write_test WHERE id = 1").Scan(&count)
	assert.Equal(t, 0, count, "old row (id=1) should no longer exist")

	newName := ctx.queryColumn("write_test", 99, "name")
	assert.Equal(t, "initial_name", newName, "renamed row should preserve data")
}

// TestMount_Rename_Row_CrossTable verifies that renaming across tables
// is rejected with an appropriate error.
func TestMount_Rename_Row_CrossTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	// Create a second table
	ctx.execSQL(`CREATE TABLE IF NOT EXISTS write_test2 (
		id INT PRIMARY KEY,
		name TEXT,
		data TEXT
	)`)

	oldPath := filepath.Join(ctx.mountPoint, "write_test", "1")
	newPath := filepath.Join(ctx.mountPoint, "write_test2", "1")

	// Cross-table rename should fail
	err := os.Rename(oldPath, newPath)
	require.Error(t, err, "cross-table rename should be rejected")
	t.Logf("cross-table rename correctly rejected: %v", err)
}
