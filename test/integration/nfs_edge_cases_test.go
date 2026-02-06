package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Category 11: Edge Cases (Integration Tests)
//
// These tests verify error handling and boundary conditions for NFS write
// operations. Each test documents the specific edge case it covers.
// =============================================================================

// TestNFS_EdgeCase_WriteNonExistentRow verifies that writing to a non-existent
// row returns an appropriate error (or creates the row, depending on operation).
//
// EDGE CASE: Write to non-existent row
//
// When writing to a column file for a row that doesn't exist, the behavior
// depends on whether it's a create or update operation.
//
// Expected: Writing to /table/999/col.txt for non-existent row 999 should
// return ENOENT (file not found), since the row doesn't exist.
func TestNFS_EdgeCase_WriteNonExistentRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	// Ensure row 999 doesn't exist
	ctx.execSQL("DELETE FROM write_test WHERE id = 999")

	colPath := filepath.Join(ctx.mountPoint, "write_test", "999", "name.txt")

	// Attempt to write to non-existent row's column
	err := os.WriteFile(colPath, []byte("value"), 0644)

	// Should fail because row doesn't exist
	// The exact error may be ENOENT or EIO depending on implementation
	if err == nil {
		t.Log("write to non-existent row unexpectedly succeeded")
		// Verify if row was created
		var count int
		ctx.db.QueryRow("SELECT COUNT(*) FROM write_test WHERE id = 999").Scan(&count)
		if count > 0 {
			t.Log("row was created (upsert behavior)")
		} else {
			t.Fatal("write succeeded but row was not created")
		}
	} else {
		t.Logf("write to non-existent row correctly failed: %v", err)
		assert.True(t, os.IsNotExist(err),
			"error should indicate file/row not found")
	}
}

// TestNFS_EdgeCase_WriteNonExistentTable verifies that writing to a
// non-existent table returns an appropriate error.
//
// EDGE CASE: Write to non-existent table
//
// When writing to a table that doesn't exist, the operation should fail
// with ENOENT rather than creating a table or silently failing.
func TestNFS_EdgeCase_WriteNonExistentTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	// Path to non-existent table
	colPath := filepath.Join(ctx.mountPoint, "nonexistent_table_xyz", "1", "col.txt")

	// Attempt to write to non-existent table
	err := os.WriteFile(colPath, []byte("value"), 0644)

	// Should fail
	require.Error(t, err, "write to non-existent table should fail")
	assert.True(t, os.IsNotExist(err),
		"error should indicate table not found")
}

// TestNFS_EdgeCase_WriteNonExistentColumn verifies that writing to a
// non-existent column returns an appropriate error.
//
// EDGE CASE: Write to non-existent column
//
// When writing to a column that doesn't exist in the table schema,
// the operation should fail rather than creating the column.
func TestNFS_EdgeCase_WriteNonExistentColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	// Path to non-existent column in existing table/row
	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "nonexistent_column_xyz.txt")

	// Attempt to write to non-existent column
	err := os.WriteFile(colPath, []byte("value"), 0644)

	// Should fail
	require.Error(t, err, "write to non-existent column should fail")
	// NFS errors may manifest as I/O errors rather than "not exist"
	// The actual error ("column not found") is logged but not propagated through NFS
	errStr := err.Error()
	assert.True(t, strings.Contains(errStr, "not exist") ||
		strings.Contains(errStr, "no such file") ||
		strings.Contains(errStr, "input/output error") ||
		os.IsNotExist(err),
		"error should indicate column not found or I/O error, got: %v", err)
}

// TestNFS_EdgeCase_WriteEmptyContent verifies the behavior when writing
// empty content (zero bytes) to a column.
//
// EDGE CASE: Zero-byte write
//
// Writing empty content should either:
// - Set the column to empty string (if supported)
// - Leave column unchanged (current limitation for truncate-before-write)
// - Return an error (if NOT NULL constraint applies)
//
// This test documents the actual behavior.
func TestNFS_EdgeCase_WriteEmptyContent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	// Set known initial value
	ctx.execSQL("UPDATE write_test SET name = 'original' WHERE id = 1")

	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")

	// Write empty content (zero bytes)
	err := os.WriteFile(colPath, []byte{}, 0644)

	// Document actual behavior
	if err != nil {
		t.Logf("empty write failed: %v", err)
	} else {
		t.Log("empty write succeeded")
	}

	// Check current value
	name := ctx.queryColumn("write_test", 1, "name")
	t.Logf("after empty write, column value: %q (len=%d)", name, len(name))

	// Document behavior - column may be unchanged or set to empty
	// Current limitation: empty writes are skipped
}

// TestNFS_EdgeCase_VeryLongColumnValue verifies handling of very long
// text values (near or at column type limits).
//
// EDGE CASE: Very long content
//
// PostgreSQL TEXT columns have no practical length limit, but very long
// values may stress buffer handling, network chunking, or memory allocation.
//
// Test uses a 100KB value to verify handling of moderately large content.
func TestNFS_EdgeCase_VeryLongColumnValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "data.txt")

	// Generate 100KB of content
	content := make([]byte, 100*1024)
	for i := range content {
		content[i] = byte('A' + (i % 26))
	}

	// Write very long content
	err := os.WriteFile(colPath, content, 0644)
	require.NoError(t, err, "failed to write very long content")

	// Allow write to commit
	time.Sleep(200 * time.Millisecond)

	// Verify via database
	dbContent := ctx.queryColumn("write_test", 1, "data")
	assert.Equal(t, len(content), len(dbContent),
		"database should contain all %d bytes", len(content))

	// Verify content is correct (spot check)
	if len(dbContent) == len(content) {
		assert.Equal(t, content[0], dbContent[0], "first byte should match")
		assert.Equal(t, content[50000], dbContent[50000], "middle byte should match")
		assert.Equal(t, content[len(content)-1], dbContent[len(dbContent)-1], "last byte should match")
	}
}

// TestNFS_EdgeCase_WriteReadImmediately verifies that a value written
// can be read back immediately (close-to-open consistency).
//
// EDGE CASE: Immediate read after write
//
// NFS close-to-open consistency should ensure that after a file is closed,
// subsequent opens see the written data. This tests the basic case without
// any intentional delays.
func TestNFS_EdgeCase_WriteReadImmediately(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")

	// Write content
	writeContent := "immediate_read_test"
	err := os.WriteFile(colPath, []byte(writeContent), 0644)
	require.NoError(t, err, "failed to write")

	// Read immediately (no sleep)
	readContent, err := os.ReadFile(colPath)
	require.NoError(t, err, "failed to read immediately after write")

	assert.Equal(t, writeContent+"\n", string(readContent),
		"immediate read should return written content with trailing newline (text file semantics)")
}

// TestNFS_EdgeCase_OverwriteMultipleTimes verifies that a file can be
// overwritten multiple times with correct final value.
//
// EDGE CASE: Multiple sequential overwrites
//
// When a file is overwritten multiple times in sequence, each write
// should completely replace the previous content, and the final read
// should return the last written value.
func TestNFS_EdgeCase_OverwriteMultipleTimes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")

	// Overwrite multiple times
	values := []string{"first", "second", "third", "final_value"}
	for _, value := range values {
		err := os.WriteFile(colPath, []byte(value), 0644)
		require.NoError(t, err, "failed to write %q", value)
	}

	// Allow final write to commit
	time.Sleep(100 * time.Millisecond)

	// Verify final value
	content, err := os.ReadFile(colPath)
	require.NoError(t, err, "failed to read final value")

	assert.Equal(t, "final_value\n", string(content),
		"final read should return last written value with trailing newline")

	// Also verify via database
	dbContent := ctx.queryColumn("write_test", 1, "name")
	assert.Equal(t, "final_value", dbContent,
		"database should contain final value")
}

// TestNFS_EdgeCase_ConcurrentWriteDifferentColumns verifies that
// concurrent writes to different columns don't interfere.
//
// EDGE CASE: Concurrent writes to different columns
//
// When two columns are written concurrently, each write should succeed
// independently without corrupting the other.
//
// Note: This tests concurrent file operations, not true concurrent
// database transactions. The test verifies final state consistency.
func TestNFS_EdgeCase_ConcurrentWriteDifferentColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupNFSWriteTestContext(t)
	defer ctx.cleanup()

	namePath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")
	dataPath := filepath.Join(ctx.mountPoint, "write_test", "1", "data.txt")

	// Start two goroutines writing concurrently
	done := make(chan error, 2)

	go func() {
		done <- os.WriteFile(namePath, []byte("concurrent_name"), 0644)
	}()

	go func() {
		done <- os.WriteFile(dataPath, []byte("concurrent_data"), 0644)
	}()

	// Wait for both to complete
	err1 := <-done
	err2 := <-done

	require.NoError(t, err1, "first concurrent write failed")
	require.NoError(t, err2, "second concurrent write failed")

	// Allow writes to commit
	time.Sleep(100 * time.Millisecond)

	// Verify both columns have correct values
	name := ctx.queryColumn("write_test", 1, "name")
	data := ctx.queryColumn("write_test", 1, "data")

	assert.Equal(t, "concurrent_name", name,
		"name column should have concurrent write value")
	assert.Equal(t, "concurrent_data", data,
		"data column should have concurrent write value")
}

