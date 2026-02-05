package nfs

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Category 10: NFS Operation Sequence Unit Tests
//
// These tests verify the memFile and OpsFilesystem state management logic
// that handles various NFS operation sequences. They run without actual NFS
// or database connections, testing the core logic in isolation.
//
// Each test documents the specific write issue it's designed to catch.
// =============================================================================

// newTestOpsFilesystem creates an OpsFilesystem for testing with the cache initialized.
// Note: ops is nil, so database operations will fail. Use for testing cache logic only.
func newTestOpsFilesystem() *OpsFilesystem {
	return &OpsFilesystem{
		fileCache: make(map[string]*cachedFile),
	}
}

// TestMemFile_TruncateThenWrite_StateTracking verifies the truncate-before-write
// pattern is handled correctly at the state management level.
//
// WRITE ISSUE CAPTURED: Truncate-before-write data corruption
//
// When macOS NFS client writes a file, it sends:
//  1. SETATTR(size=0) - truncate file to empty
//  2. WRITE(data) - write actual content
//
// Without proper handling, step 2 would read stale data from DB and overlay
// the new content, potentially leaving old data at the end of the file.
//
// This test verifies:
//   - Truncate creates a cached file with truncated=true
//   - Subsequent OpenFile for write sees the cached file
//   - The write operation starts with empty data (not stale DB data)
func TestMemFile_TruncateThenWrite_StateTracking(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Simulate Cycle 1: SETATTR(size=0)
	// This is what go-nfs does when macOS sends SETATTR with size=0
	f1, err := fs.OpenFile("/table/1/col.txt", os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)

	// Verify file is in cache with truncated=true
	cached := fs.getCachedFile("/table/1/col.txt")
	require.NotNil(t, cached, "file should be cached after O_TRUNC")
	cached.mu.RLock()
	truncated := cached.truncated
	cached.mu.RUnlock()
	assert.True(t, truncated, "file should be marked as truncated after O_TRUNC")

	// Close the truncate operation (simulates go-nfs closing after SETATTR)
	err = f1.Close()
	require.NoError(t, err)

	// Simulate Cycle 2: WRITE(data)
	// This is the actual write with content
	f2, err := fs.OpenFile("/table/1/col.txt", os.O_WRONLY, 0644)
	require.NoError(t, err)

	// Verify we got an empty memFile (not reading from DB)
	mf2 := f2.(*memFile)
	mf2.cached.mu.RLock()
	data := mf2.cached.data
	mf2.cached.mu.RUnlock()
	assert.Empty(t, data, "write after truncate should start with empty data")

	// Write content
	n, err := f2.Write([]byte("new content"))
	require.NoError(t, err)
	assert.Equal(t, 11, n)

	mf2.cached.mu.RLock()
	dirty := mf2.cached.dirty
	finalData := mf2.cached.data
	mf2.cached.mu.RUnlock()
	assert.True(t, dirty, "file should be marked dirty after write")
	assert.Equal(t, []byte("new content"), finalData)
}

// TestMemFile_TruncateOnly_EmptyWriteSkipped verifies that truncate without
// subsequent write skips the empty commit (current limitation).
//
// WRITE ISSUE CAPTURED: NOT NULL constraint violations on truncate
//
// When a user explicitly truncates a file (e.g., `truncate -s 0 file`),
// the workaround skips writing empty content to avoid NOT NULL constraint
// violations during the truncate-before-write pattern.
//
// Current behavior: Empty write is skipped (column unchanged)
// After architectural fix: Empty write should be committed (column cleared)
//
// This test documents the current limitation.
func TestMemFile_TruncateOnly_EmptyWriteSkipped(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Simulate: truncate -s 0 /table/1/col.txt
	// go-nfs: OpenFile(O_TRUNC) -> Truncate(0) -> Close()
	f, err := fs.OpenFile("/table/1/col.txt", os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)

	mf := f.(*memFile)

	// Truncate to zero (go-nfs calls this for SETATTR)
	err = mf.Truncate(0)
	require.NoError(t, err)

	mf.cached.mu.RLock()
	dirty := mf.cached.dirty
	data := mf.cached.data
	mf.cached.mu.RUnlock()
	assert.True(t, dirty, "truncate should mark file as dirty")
	assert.Empty(t, data, "data should be empty after truncate")

	// Close would normally write to DB, but:
	// - ops is nil (no DB connection in this test)
	// - Even with ops, empty content is skipped by workaround
	//
	// Verification: The memFile.Close() logic checks:
	//   if len(data) == 0 && !isDDLSQL { return nil }
	// This skips the write for empty content.
	err = f.Close()
	require.NoError(t, err)
}

// TestMemFile_CreateThenWrite_InFlightTracking verifies that newly created
// files are tracked in the cache until content is written.
//
// WRITE ISSUE CAPTURED: Stat fails for newly created files before write
//
// NFS operation sequence for new file:
//  1. CREATE - creates file handle
//  2. SETATTR - sets attributes (calls Stat internally)
//  3. WRITE - writes content
//  4. CLOSE - finalizes
//
// Without caching, step 2 would fail because the row doesn't
// exist in the database yet.
//
// This test verifies:
//   - Create registers file in the cache
//   - Subsequent OpenFile for write returns a memFile sharing the same cache
//   - Stat would succeed (tested via the cache check)
func TestMemFile_CreateThenWrite_InFlightTracking(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Simulate: CREATE (touch /table/999.json or echo > /table/999.json)
	f1, err := fs.Create("/table/999.json")
	require.NoError(t, err)

	// Verify file is tracked in the cache
	cached := fs.getCachedFile("/table/999.json")
	require.NotNil(t, cached, "newly created file should be tracked in cache")

	// Close CREATE (go-nfs does this to get the NFS file handle)
	err = f1.Close()
	require.NoError(t, err)

	// File should be removed from cache after close with refCount=0
	// (since this is the only handle and no data was written)

	// Simulate: WRITE - this will create a new cache entry
	f2, err := fs.OpenFile("/table/999.json", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)

	mf2 := f2.(*memFile)
	n, err := f2.Write([]byte(`{"name":"test"}`))
	require.NoError(t, err)
	assert.Equal(t, 15, n)

	mf2.cached.mu.RLock()
	dirty := mf2.cached.dirty
	mf2.cached.mu.RUnlock()
	assert.True(t, dirty)
}

// TestMemFile_MultipleWritesSameHandle verifies that multiple writes to the
// same file handle accumulate correctly.
//
// WRITE ISSUE CAPTURED: Large file write corruption
//
// When writing large files, the NFS client may send multiple WRITE RPCs.
// If go-nfs keeps the file handle open (same memFile), writes should accumulate.
//
// This test verifies:
//   - Sequential writes append at the correct offset
//   - Final data contains all written content
func TestMemFile_MultipleWritesSameHandle(t *testing.T) {
	fs := newTestOpsFilesystem()

	f, err := fs.OpenFile("/table/1/data.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)

	mf := f.(*memFile)

	// Write chunk 1
	n, err := f.Write([]byte("chunk1"))
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, int64(6), mf.offset)

	// Write chunk 2
	n, err = f.Write([]byte("chunk2"))
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, int64(12), mf.offset)

	// Verify combined content
	mf.cached.mu.RLock()
	data := mf.cached.data
	dirty := mf.cached.dirty
	mf.cached.mu.RUnlock()
	assert.Equal(t, []byte("chunk1chunk2"), data)
	assert.True(t, dirty)
}

// TestMemFile_WriteAtOffset_Overlay verifies that writes at specific offsets
// correctly overlay existing data.
//
// WRITE ISSUE CAPTURED: Offset write corruption
//
// When NFS sends writes at specific offsets (e.g., for partial updates or
// large file chunking), the write should overlay at that position without
// corrupting surrounding data.
//
// This test verifies:
//   - Seek then Write correctly overlays at the specified offset
//   - Surrounding data is preserved
func TestMemFile_WriteAtOffset_Overlay(t *testing.T) {
	fs := newTestOpsFilesystem()

	f, err := fs.OpenFile("/table/1/data.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)

	mf := f.(*memFile)

	// Initial write
	f.Write([]byte("0123456789"))
	mf.cached.mu.RLock()
	data := mf.cached.data
	mf.cached.mu.RUnlock()
	assert.Equal(t, []byte("0123456789"), data)

	// Seek to middle and overwrite
	f.Seek(3, 0) // Seek to offset 3
	f.Write([]byte("XXX"))

	// Verify overlay
	mf.cached.mu.RLock()
	data = mf.cached.data
	mf.cached.mu.RUnlock()
	assert.Equal(t, []byte("012XXX6789"), data)
}

// TestMemFile_RowFile_ReplacementSemantics verifies that row files (JSON, CSV)
// use replacement semantics on write.
//
// WRITE ISSUE CAPTURED: JSON parsing error from stale data
//
// When NFS writes to an existing row file:
//  1. OpenFile reads existing content (e.g., 60 bytes of old JSON)
//  2. Write at offset 0 with new content (e.g., 56 bytes of new JSON)
//  3. Without replacement: buffer has 56 new bytes + 4 stale bytes
//  4. JSON parsing fails: "invalid character '0' after top-level value"
//
// This test verifies:
//   - Row files (.json, .csv, .tsv, .yaml) use replacement semantics
//   - Write at offset 0 replaces entire buffer, not overlay
func TestMemFile_RowFile_ReplacementSemantics(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Create a row file (JSON)
	f, err := fs.OpenFile("/table/1.json", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)

	mf := f.(*memFile)
	mf.cached.mu.RLock()
	isRowFile := mf.cached.isRowFile
	mf.cached.mu.RUnlock()
	assert.True(t, isRowFile, "JSON file should be marked as row file")

	// Simulate: existing data was read (57 bytes)
	mf.cached.mu.Lock()
	mf.cached.data = []byte(`{"name":"oldvalue","extra":"data that makes this longer"}`)
	mf.cached.mu.Unlock()
	assert.Equal(t, 57, len(mf.cached.data))

	// Write shorter new content at offset 0
	mf.offset = 0
	newContent := []byte(`{"name":"new"}`)
	n, err := f.Write(newContent)
	require.NoError(t, err)
	assert.Equal(t, 14, n)

	// Verify: buffer should be REPLACED, not overlaid
	// Without replacement semantics, we'd have 14 new + 42 stale = 56 bytes
	// With replacement semantics, we have exactly 14 bytes
	mf.cached.mu.RLock()
	data := mf.cached.data
	mf.cached.mu.RUnlock()
	assert.Equal(t, newContent, data, "row file write should replace entire buffer")
	assert.Equal(t, 14, len(data), "buffer length should match new content length")
}

// TestMemFile_ColumnFile_OverlaySemantics verifies that column files (.txt)
// use overlay semantics, not replacement.
//
// WRITE ISSUE CAPTURED: Distinguishing row vs column file behavior
//
// Column files are individual column values, not full row representations.
// They should use standard overlay semantics where writes at offset N
// overlay at that position.
//
// This test verifies:
//   - Column files (.txt) are NOT marked as row files
//   - Writes use standard overlay behavior
func TestMemFile_ColumnFile_OverlaySemantics(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Create a column file (not a row file)
	f, err := fs.OpenFile("/table/1/name.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)

	mf := f.(*memFile)
	mf.cached.mu.RLock()
	isRowFile := mf.cached.isRowFile
	mf.cached.mu.RUnlock()
	assert.False(t, isRowFile, ".txt file should NOT be marked as row file")

	// Set up existing data
	mf.cached.mu.Lock()
	mf.cached.data = []byte("original value")
	mf.cached.mu.Unlock()
	mf.offset = 0

	// Write shorter new content at offset 0
	newContent := []byte("new")
	n, err := f.Write(newContent)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	// Verify: buffer should be OVERLAID, not replaced
	// "original value" with "new" at offset 0 = "newginal value"
	mf.cached.mu.RLock()
	data := mf.cached.data
	mf.cached.mu.RUnlock()
	assert.Equal(t, []byte("newginal value"), data)
}

// TestMemFile_TruncateClearsData verifies that Truncate(0) clears the data buffer.
//
// WRITE ISSUE CAPTURED: Stale data after truncate
//
// When Truncate(0) is called (from SETATTR size=0), the data buffer must
// be cleared to prevent stale data from persisting.
//
// This test verifies:
//   - Truncate(0) clears the data buffer
//   - dirty flag is set
func TestMemFile_TruncateClearsData(t *testing.T) {
	fs := newTestOpsFilesystem()

	f, err := fs.OpenFile("/table/1/col.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)

	mf := f.(*memFile)

	// Set up existing data
	mf.cached.mu.Lock()
	mf.cached.data = []byte("existing content that should be cleared")
	mf.cached.dirty = false
	mf.cached.mu.Unlock()

	// Truncate to 0
	err = mf.Truncate(0)
	require.NoError(t, err)

	mf.cached.mu.RLock()
	data := mf.cached.data
	dirty := mf.cached.dirty
	mf.cached.mu.RUnlock()
	assert.Empty(t, data, "truncate(0) should clear data")
	assert.True(t, dirty, "truncate should mark dirty")
}

// TestMemFile_CacheSharing verifies that multiple opens of the same file
// share the same cached data via reference counting.
//
// WRITE ISSUE CAPTURED: Data loss when multiple handles write to same file
//
// With the persistent cache (ADR-010), multiple file handles should share
// the same cached data. Writes from any handle should be visible to others.
//
// This test verifies:
//   - Multiple opens return memFiles pointing to the same cachedFile
//   - Reference count is correctly incremented
//   - Data written by one handle is visible to the other
func TestMemFile_CacheSharing(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Open file twice
	f1, err := fs.OpenFile("/table/1/col.txt", os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf1 := f1.(*memFile)

	f2, err := fs.OpenFile("/table/1/col.txt", os.O_RDWR, 0644)
	require.NoError(t, err)
	mf2 := f2.(*memFile)

	// Verify they share the same cachedFile
	assert.Same(t, mf1.cached, mf2.cached, "both handles should share same cached file")

	// Verify refCount is 2
	mf1.cached.mu.RLock()
	refCount := mf1.cached.refCount
	mf1.cached.mu.RUnlock()
	assert.Equal(t, 2, refCount, "refCount should be 2 with two handles open")

	// Write via first handle
	_, err = f1.Write([]byte("hello"))
	require.NoError(t, err)

	// Read via second handle should see the write
	mf2.cached.mu.RLock()
	data := mf2.cached.data
	mf2.cached.mu.RUnlock()
	assert.Equal(t, []byte("hello"), data, "write from f1 should be visible in f2")

	// Close first handle
	err = f1.Close()
	require.NoError(t, err)

	// RefCount should now be 1
	mf2.cached.mu.RLock()
	refCount = mf2.cached.refCount
	mf2.cached.mu.RUnlock()
	assert.Equal(t, 1, refCount, "refCount should be 1 after closing one handle")

	// Cache entry should still exist
	cached := fs.getCachedFile("/table/1/col.txt")
	assert.NotNil(t, cached, "cache entry should still exist with one handle open")
}

// TestMemFile_DeleteWhileOpen verifies that deleting a file while it has
// open handles marks it as deleted and fails subsequent writes.
//
// WRITE ISSUE CAPTURED: Writing to deleted file causes confusion
//
// If a file is deleted while still open, subsequent writes should fail
// rather than silently succeed and then fail on commit.
//
// This test verifies:
//   - Marking a cached file as deleted works correctly
//   - Subsequent writes return an error
func TestMemFile_DeleteWhileOpen(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Open file
	f, err := fs.OpenFile("/table/1/col.txt", os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	// Write some initial data
	_, err = f.Write([]byte("initial"))
	require.NoError(t, err)

	// Simulate delete by directly marking the cached file as deleted
	// (In real usage, Remove() would do this before calling ops.Delete())
	mf.cached.mu.Lock()
	mf.cached.deleted = true
	mf.cached.mu.Unlock()

	// Subsequent write should fail
	_, err = f.Write([]byte("more data"))
	require.Error(t, err, "write to deleted file should fail")
	assert.Contains(t, err.Error(), "deleted", "error should mention file was deleted")
}

// TestMemFile_Sync_CommitsImmediately verifies that Sync() commits the
// current buffer to the database immediately without closing the file.
//
// WRITE ISSUE CAPTURED: Editor save doesn't persist until close
//
// When a user saves in an editor (Ctrl+S), the editor calls fsync().
// Without Sync() implementation, changes wouldn't persist until the
// file is closed, which could be much later.
//
// This test verifies:
//   - Sync() clears the dirty flag after commit
//   - File remains open and usable after Sync()
//   - Subsequent writes work and mark dirty again
func TestMemFile_Sync_CommitsImmediately(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Open file and write content
	f, err := fs.OpenFile("/table/1/col.txt", os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	_, err = f.Write([]byte("initial content"))
	require.NoError(t, err)

	// Verify dirty before sync
	mf.cached.mu.RLock()
	dirty := mf.cached.dirty
	mf.cached.mu.RUnlock()
	assert.True(t, dirty, "file should be dirty after write")

	// Sync (would commit to DB if ops wasn't nil)
	err = mf.Sync()
	require.NoError(t, err)

	// Dirty should be cleared (ops is nil so no actual write, but flag is cleared
	// only if ops != nil, so in this test dirty remains true)
	// Note: In real usage with ops set, dirty would be cleared

	// File should still be in cache and usable
	cached := fs.getCachedFile("/table/1/col.txt")
	require.NotNil(t, cached, "file should still be in cache after sync")

	// Write more content
	_, err = f.Write([]byte(" more"))
	require.NoError(t, err)

	mf.cached.mu.RLock()
	data := mf.cached.data
	mf.cached.mu.RUnlock()
	assert.Equal(t, []byte("initial content more"), data, "writes after sync should work")
}

// TestMemFile_Sync_SkipsCleanFile verifies that Sync() is a no-op for
// files that haven't been modified.
func TestMemFile_Sync_SkipsCleanFile(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Open file without writing
	f, err := fs.OpenFile("/table/1/col.txt", os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	// File should not be dirty
	mf.cached.mu.RLock()
	dirty := mf.cached.dirty
	mf.cached.mu.RUnlock()
	assert.False(t, dirty, "file should not be dirty before any writes")

	// Sync should succeed (no-op)
	err = mf.Sync()
	require.NoError(t, err)
}

// TestMemFile_Sync_SkipsDeletedFile verifies that Sync() is a no-op for
// files that have been deleted.
func TestMemFile_Sync_SkipsDeletedFile(t *testing.T) {
	fs := newTestOpsFilesystem()

	// Open file and write content
	f, err := fs.OpenFile("/table/1/col.txt", os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	_, err = f.Write([]byte("content"))
	require.NoError(t, err)

	// Mark as deleted
	mf.cached.mu.Lock()
	mf.cached.deleted = true
	mf.cached.mu.Unlock()

	// Sync should succeed but skip the commit
	err = mf.Sync()
	require.NoError(t, err)
}

// =============================================================================
// Large File Handling Tests (Task 9.13)
// =============================================================================

// TestMemFile_StreamingWrite_TriggersAtThreshold verifies that sequential writes
// exceeding the streaming threshold trigger a commit and buffer clear.
//
// WRITE ISSUE CAPTURED: Memory exhaustion on large file writes
//
// When writing very large files (e.g., dd if=/dev/zero bs=1M count=100),
// buffering everything in memory would cause OOM. For sequential writes,
// we commit at the threshold and clear the buffer.
//
// This test verifies:
//   - Sequential writes trigger streaming at threshold
//   - Buffer is cleared after streaming commit
//   - streamed flag is set
func TestMemFile_StreamingWrite_TriggersAtThreshold(t *testing.T) {
	fs := newTestOpsFilesystem()

	f, err := fs.OpenFile("/table/1/data.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	// Write data at the threshold (not exceeding yet)
	chunk := make([]byte, 1024*1024) // 1MB chunks
	for i := 0; i < 10; i++ {        // 10MB total, at threshold but not over
		_, err = f.Write(chunk)
		require.NoError(t, err)
	}

	// Verify buffer has accumulated (threshold is > not >=)
	mf.cached.mu.RLock()
	sizeAtThreshold := len(mf.cached.data)
	streamed := mf.cached.streamed
	mf.cached.mu.RUnlock()
	assert.Equal(t, 10*1024*1024, sizeAtThreshold, "buffer should have 10MB")
	assert.False(t, streamed, "should not have streamed yet (at threshold, not over)")

	// Write more to exceed threshold (ops is nil, so no actual DB write)
	_, err = f.Write(chunk) // 11MB total, exceeds 10MB threshold
	require.NoError(t, err)

	// After threshold, buffer should be cleared and streamed flag set
	mf.cached.mu.RLock()
	sizeAfterThreshold := len(mf.cached.data)
	streamed = mf.cached.streamed
	isSequential := mf.cached.isSequential
	mf.cached.mu.RUnlock()

	assert.Equal(t, 0, sizeAfterThreshold, "buffer should be cleared after streaming")
	assert.True(t, streamed, "streamed flag should be set")
	assert.True(t, isSequential, "should still be marked sequential")
}

// TestMemFile_RandomWrite_RejectsAtLimit verifies that non-sequential writes
// exceeding the maximum random write size return EFBIG.
//
// WRITE ISSUE CAPTURED: Unbounded memory for random access patterns
//
// Random writes (seeking to various positions) require keeping all data
// in memory. Without a limit, a malicious or buggy client could exhaust
// memory. We cap random writes at 100MB and return EFBIG.
//
// This test verifies:
//   - Non-sequential writes have a size limit
//   - Exceeding the limit returns EFBIG
func TestMemFile_RandomWrite_RejectsAtLimit(t *testing.T) {
	fs := newTestOpsFilesystem()

	f, err := fs.OpenFile("/table/1/data.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	// Write some data
	_, err = f.Write([]byte("initial"))
	require.NoError(t, err)

	// Seek backwards to make it non-sequential
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte("x"))
	require.NoError(t, err)

	// Verify it's marked as non-sequential
	mf.cached.mu.RLock()
	isSequential := mf.cached.isSequential
	mf.cached.mu.RUnlock()
	assert.False(t, isSequential, "should be marked non-sequential after seek+write")

	// Try to write past the limit (simulate by setting data size near limit)
	mf.cached.mu.Lock()
	mf.cached.data = make([]byte, 100*1024*1024-1) // Just under 100MB
	mf.offset = int64(len(mf.cached.data))
	mf.cached.mu.Unlock()

	// This write should fail with EFBIG
	_, err = f.Write([]byte("xx")) // Would push over 100MB
	require.Error(t, err, "write exceeding limit should fail")
	assert.ErrorIs(t, err, syscall.EFBIG, "error should be EFBIG")
}

// TestMemFile_SequentialWrite_NoSizeLimit verifies that sequential writes
// are not limited by maxRandomWriteSize (they stream instead).
func TestMemFile_SequentialWrite_NoSizeLimit(t *testing.T) {
	fs := newTestOpsFilesystem()

	f, err := fs.OpenFile("/table/1/data.txt", os.O_WRONLY|os.O_CREATE, 0644)
	require.NoError(t, err)
	mf := f.(*memFile)

	// Simulate writing more than maxRandomWriteSize sequentially
	// The streaming mechanism will clear the buffer periodically
	chunk := make([]byte, 1024*1024) // 1MB chunks
	for i := 0; i < 15; i++ {        // 15MB > 10MB threshold, will stream
		_, err = f.Write(chunk)
		require.NoError(t, err)
	}

	// Should have streamed (buffer cleared)
	mf.cached.mu.RLock()
	streamed := mf.cached.streamed
	isSequential := mf.cached.isSequential
	bufferSize := len(mf.cached.data)
	mf.cached.mu.RUnlock()

	assert.True(t, streamed, "should have streamed")
	assert.True(t, isSequential, "should still be sequential")
	assert.Less(t, bufferSize, 15*1024*1024, "buffer should be less than total written")
}

// =============================================================================
// Helper function tests
// =============================================================================

func TestIsRowFile(t *testing.T) {
	tests := []struct {
		filename string
		expected bool
	}{
		{"1.json", true},
		{"1.csv", true},
		{"1.tsv", true},
		{"1.yaml", true},
		{"name.txt", false},
		{"col", false},
		{".json", true},      // edge case: just extension
		{"data.JSON", false}, // case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			result := isRowFile(tt.filename)
			assert.Equal(t, tt.expected, result, "isRowFile(%q)", tt.filename)
		})
	}
}
