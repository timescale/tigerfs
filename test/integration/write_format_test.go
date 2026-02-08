package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Category 7: File Format Handling (Integration Tests)
//
// These tests verify that different file formats (JSON, CSV, TSV, TXT) are
// parsed and stored correctly when written via the mounted filesystem.
//
// Each test documents the specific write issue it's designed to catch.
// =============================================================================

// TestWriteFormat_ValidJSON verifies that valid JSON files are parsed and
// stored correctly, updating the appropriate database columns.
//
// WRITE ISSUE CAPTURED: JSON parsing failure
//
// When writing to a .json row file, the content must be valid JSON and
// correctly parsed into individual column values.
//
// Example failure scenario:
//  1. Write '{"name":"test","age":30}' to /users/1.json
//  2. If JSON parsing fails or column mapping is wrong, row is corrupted
//  3. Database should have name="test", age=30
func TestWriteFormat_ValidJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Write valid JSON
	jsonContent := `{"name":"testuser","data":"testdata"}`
	err := os.WriteFile(jsonPath, []byte(jsonContent), 0644)
	require.NoError(t, err, "failed to write JSON file")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Verify via database query - individual columns
	name := ctx.queryColumn("write_test", 1, "name")
	data := ctx.queryColumn("write_test", 1, "data")

	assert.Equal(t, "testuser", name, "name column should be updated")
	assert.Equal(t, "testdata", data, "data column should be updated")
}

// TestWriteFormat_InvalidJSON verifies that invalid JSON is rejected with
// an appropriate error.
//
// WRITE ISSUE CAPTURED: Invalid JSON silently accepted
//
// When writing invalid JSON to a .json file, the operation should fail
// rather than corrupt the database with partial or malformed data.
//
// Example failure scenario:
//  1. Write 'not valid json' to /users/1.json
//  2. Without validation, raw text might be stored or row corrupted
//  3. Expected: Write returns error (EINVAL or EIO)
func TestWriteFormat_InvalidJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Store original values to verify they're unchanged
	origName := ctx.queryColumn("write_test", 1, "name")

	// Attempt to write invalid JSON
	invalidContent := `not valid json at all`
	err := os.WriteFile(jsonPath, []byte(invalidContent), 0644)

	// Should fail with an error (either EINVAL or EIO)
	if err == nil {
		// If write succeeded, verify the database wasn't corrupted
		newName := ctx.queryColumn("write_test", 1, "name")
		if newName == invalidContent {
			t.Fatal("invalid JSON was silently accepted and stored")
		}
		t.Log("write succeeded but database was not corrupted (unexpected behavior)")
	} else {
		t.Logf("invalid JSON correctly rejected: %v", err)
	}

	// Verify original data is preserved
	currentName := ctx.queryColumn("write_test", 1, "name")
	assert.Equal(t, origName, currentName,
		"original data should be preserved after invalid JSON write attempt")
}

// TestWriteFormat_ColumnText verifies that writing to individual column
// files (.txt extension) correctly updates just that column.
//
// WRITE ISSUE CAPTURED: Column file write corruption
//
// When writing to a column file like /table/1/name.txt, only that specific
// column should be updated, preserving all other columns.
//
// Example failure scenario:
//  1. Row has name="original", data="preserved"
//  2. Write "updated\n" to /table/1/name.txt
//  3. If column targeting is wrong, other columns may be corrupted
//  4. Expected: name="updated\n", data="preserved"
func TestWriteFormat_ColumnText(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	// Ensure row has known initial state
	ctx.execSQL("UPDATE write_test SET name = 'original', data = 'preserved' WHERE id = 1")

	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")

	// Write to column file (echo adds newline)
	err := os.WriteFile(colPath, []byte("updated\n"), 0644)
	require.NoError(t, err, "failed to write column file")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Verify column was updated (trailing newline stripped on write - text file semantics)
	name := ctx.queryColumn("write_test", 1, "name")
	assert.Equal(t, "updated", name, "column should be updated (trailing newline stripped on write)")

	// Verify other column is preserved
	data := ctx.queryColumn("write_test", 1, "data")
	assert.Equal(t, "preserved", data, "other columns should be preserved")
}

// TestWriteFormat_JSONNewline verifies that JSON with trailing newline is
// handled correctly.
//
// WRITE ISSUE CAPTURED: Trailing newline causes JSON parse error
//
// Many text editors add a trailing newline. JSON with a trailing newline
// should still parse correctly.
//
// Example failure scenario:
//  1. Write '{"name":"test"}\n' (with newline at end)
//  2. Strict JSON parser fails: "invalid character '\n' after top-level value"
//  3. Expected: Newline trimmed, JSON parsed correctly
func TestWriteFormat_JSONNewline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Write JSON with trailing newline (common with echo)
	jsonContent := `{"name":"withnewline","data":"test"}` + "\n"
	err := os.WriteFile(jsonPath, []byte(jsonContent), 0644)
	require.NoError(t, err, "failed to write JSON with newline")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Verify parsed correctly
	name := ctx.queryColumn("write_test", 1, "name")
	assert.Equal(t, "withnewline", name,
		"JSON with trailing newline should parse correctly")
}

// TestWriteFormat_JSONUnicode verifies that JSON with Unicode characters
// is handled correctly.
//
// WRITE ISSUE CAPTURED: Unicode encoding corruption
//
// JSON may contain Unicode characters, Unicode escape sequences, or
// multi-byte UTF-8 sequences that must be preserved correctly.
//
// Example failure scenario:
//  1. Write '{"name":"日本語","emoji":"🎉"}'
//  2. If encoding is wrong, characters become garbled or question marks
//  3. Expected: Unicode preserved exactly
func TestWriteFormat_JSONUnicode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Write JSON with Unicode
	jsonContent := `{"name":"日本語🎉","data":"テスト"}`
	err := os.WriteFile(jsonPath, []byte(jsonContent), 0644)
	require.NoError(t, err, "failed to write JSON with Unicode")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Verify Unicode preserved
	name := ctx.queryColumn("write_test", 1, "name")
	data := ctx.queryColumn("write_test", 1, "data")

	assert.Equal(t, "日本語🎉", name, "Unicode in name should be preserved")
	assert.Equal(t, "テスト", data, "Unicode in data should be preserved")
}

// =============================================================================
// Category 8: Cross-Format Consistency (Integration Tests)
//
// These tests verify that data written in one format can be read correctly
// in another format, ensuring consistent database representation.
// =============================================================================

// TestWriteFormat_Cross_WriteJSON_ReadColumn verifies that JSON writes can
// be read back as individual column files.
//
// WRITE ISSUE CAPTURED: Format-specific storage causing read inconsistency
//
// Data should be stored in the database in a canonical form, regardless
// of the format used to write it. Reading in a different format should
// return consistent data.
//
// Example scenario:
//  1. Write '{"name":"test"}' to /table/1.json
//  2. Read /table/1/name.txt
//  3. Should return "test" (the value written via JSON)
func TestWriteFormat_Cross_WriteJSON_ReadColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")
	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")

	// Write via JSON
	jsonContent := `{"name":"jsonvalue","data":"other"}`
	err := os.WriteFile(jsonPath, []byte(jsonContent), 0644)
	require.NoError(t, err, "failed to write JSON")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Read back via column file
	content, err := os.ReadFile(colPath)
	require.NoError(t, err, "failed to read column file")

	assert.Equal(t, "jsonvalue\n", string(content),
		"column read should return JSON-written value with trailing newline")
}

// TestWriteFormat_Cross_WriteColumn_ReadJSON verifies that column file writes
// can be read back as JSON.
//
// WRITE ISSUE CAPTURED: Column writes not reflected in JSON reads
//
// When a column is updated via its .txt file, reading the row as JSON
// should reflect the updated value.
//
// Example scenario:
//  1. Row has name="original"
//  2. Write "updated\n" to /table/1/name.txt
//  3. Read /table/1.json
//  4. JSON should contain "name":"updated\n"
func TestWriteFormat_Cross_WriteColumn_ReadJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	// Set known initial state
	ctx.execSQL("UPDATE write_test SET name = 'original', data = 'datavalue' WHERE id = 1")

	colPath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")
	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Write via column file
	err := os.WriteFile(colPath, []byte("columnvalue"), 0644)
	require.NoError(t, err, "failed to write column file")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Read back via JSON
	content, err := os.ReadFile(jsonPath)
	require.NoError(t, err, "failed to read JSON file")

	// Parse JSON and verify
	var row map[string]interface{}
	err = json.Unmarshal(content, &row)
	require.NoError(t, err, "failed to parse JSON")

	assert.Equal(t, "columnvalue", row["name"],
		"JSON read should reflect column-written value")
	assert.Equal(t, "datavalue", row["data"],
		"other columns should be preserved in JSON")
}

// TestWriteFormat_Cross_MultipleColumnWrites verifies that multiple column
// writes are all reflected when reading as JSON.
//
// WRITE ISSUE CAPTURED: Race condition between column writes
//
// When multiple columns are written separately, all writes should be
// committed and visible in subsequent JSON reads.
//
// Example scenario:
//  1. Write "name1" to /table/1/name.txt
//  2. Write "data1" to /table/1/data.txt
//  3. Read /table/1.json
//  4. JSON should have both updated values
func TestWriteFormat_Cross_MultipleColumnWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	namePath := filepath.Join(ctx.mountPoint, "write_test", "1", "name.txt")
	dataPath := filepath.Join(ctx.mountPoint, "write_test", "1", "data.txt")
	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Write to first column
	err := os.WriteFile(namePath, []byte("name1"), 0644)
	require.NoError(t, err, "failed to write name column")

	// Write to second column
	err = os.WriteFile(dataPath, []byte("data1"), 0644)
	require.NoError(t, err, "failed to write data column")

	// Allow writes to commit
	time.Sleep(100 * time.Millisecond)

	// Read back via JSON
	content, err := os.ReadFile(jsonPath)
	require.NoError(t, err, "failed to read JSON file")

	// Parse JSON and verify both columns updated
	var row map[string]interface{}
	err = json.Unmarshal(content, &row)
	require.NoError(t, err, "failed to parse JSON")

	assert.Equal(t, "name1", row["name"],
		"name column should be updated")
	assert.Equal(t, "data1", row["data"],
		"data column should be updated")
}

// TestWriteFormat_JSONSpecialCharacters verifies that JSON with special
// characters (quotes, backslashes, control chars) is handled correctly.
//
// WRITE ISSUE CAPTURED: JSON escaping corruption
//
// JSON with embedded quotes, backslashes, or control characters must be
// properly escaped/unescaped during write and read operations.
//
// Example failure scenario:
//  1. Write '{"name":"has \"quotes\" and \\backslash"}'
//  2. If escaping is wrong, stored value is corrupted
//  3. Expected: Stored as: has "quotes" and \backslash
func TestWriteFormat_JSONSpecialCharacters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := setupWriteTestContext(t)
	defer ctx.cleanup()

	jsonPath := filepath.Join(ctx.mountPoint, "write_test", "1.json")

	// Write JSON with special characters (properly escaped in JSON)
	jsonContent := `{"name":"has \"quotes\" and \\backslash","data":"tab\there"}`
	err := os.WriteFile(jsonPath, []byte(jsonContent), 0644)
	require.NoError(t, err, "failed to write JSON with special chars")

	// Allow write to commit
	time.Sleep(100 * time.Millisecond)

	// Verify via database - should have unescaped values
	name := ctx.queryColumn("write_test", 1, "name")
	data := ctx.queryColumn("write_test", 1, "data")

	assert.Equal(t, `has "quotes" and \backslash`, name,
		"quotes and backslashes should be unescaped")
	assert.True(t, strings.Contains(data, "\t"),
		"tab character should be preserved")
}
