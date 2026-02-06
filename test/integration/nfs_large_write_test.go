package integration

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// Category 5: Large File Write Tests
//
// These tests verify that large file writes complete correctly despite NFS
// chunking behavior. NFS clients may split large writes into multiple WRITE
// RPCs based on the wsize mount option.
//
// ⚠️ FLAKINESS RISKS (documented in test plan):
//
// Risk 1: NFS client caching
//   - Read after write may return cached data, not DB content
//   - Mitigation: Use Sync(), add delay, verify via DB query
//
// Risk 2: Close-to-open consistency
//   - Read may race with final write commit
//   - Mitigation: Sync before close, delay before read
//
// Risk 3: Write reordering
//   - NFS client may send WRITE RPCs out of order
//   - Current arch handles this via read-overlay-write per chunk
//   - If overlay logic has bugs, certain orderings may fail
//
// Risk 4: Kernel coalescing
//   - Kernel may combine/split writes unpredictably
//   - Cannot control; accept that chunking varies by run
//
// Risk 5: Performance variability (not correctness)
//   - Current arch: O(n²) data transfer for n chunks
//   - 1MB file with 32KB wsize = ~32 DB round-trips
//   - Test may be slow; don't assert on timing
// =============================================================================

// TestNFS_LargeWrite_64KB tests writing and reading back 64KB of content.
//
// WRITE ISSUE CAPTURED: Large file write corruption from chunking
//
// When writing files larger than NFS wsize (typically 32KB-1MB), the NFS
// client splits the write into multiple WRITE RPCs. Each RPC must be
// handled correctly to produce the complete file.
func TestNFS_LargeWrite_64KB(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tableName := "large_write_test"

	// Create table with TEXT column for large content
	t.Run("Setup", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		sqlPath := filepath.Join(createDir, "sql")
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT
		);
		INSERT INTO %s (id, data) VALUES (1, 'placeholder');`, tableName, tableName)

		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("Write64KB", func(t *testing.T) {
		// Generate deterministic 64KB content
		// Using repeating pattern for easy verification
		content := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 4096) // 16 * 4096 = 65536 bytes
		if len(content) != 64*1024 {
			t.Fatalf("Expected 64KB, got %d bytes", len(content))
		}

		dataPath := filepath.Join(mountpoint, tableName, "1", "data")

		// Write via NFS
		f, err := os.Create(dataPath)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		n, err := f.Write(content)
		if err != nil {
			f.Close()
			t.Fatalf("Failed to write: %v", err)
		}
		if n != len(content) {
			f.Close()
			t.Fatalf("Short write: %d != %d", n, len(content))
		}

		// Force flush to server
		if err := f.Sync(); err != nil {
			t.Logf("Sync warning: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}

		// Mitigation: brief delay for NFS caches to settle
		time.Sleep(200 * time.Millisecond)

		// Read back via NFS
		result, err := os.ReadFile(dataPath)
		if err != nil {
			t.Fatalf("Failed to read back: %v", err)
		}

		// Verify content (read adds trailing newline - text file semantics)
		expectedResult := append(content, '\n')
		if len(result) != len(expectedResult) {
			t.Errorf("Length mismatch: got %d, expected %d (including trailing newline)", len(result), len(expectedResult))
		}

		if !bytes.Equal(result, expectedResult) {
			// Find first difference for debugging
			for i := 0; i < len(expectedResult) && i < len(result); i++ {
				if expectedResult[i] != result[i] {
					t.Errorf("First difference at byte %d: expected %02x, got %02x", i, expectedResult[i], result[i])
					break
				}
			}
			t.Errorf("Content mismatch in 64KB write")
		} else {
			t.Logf("64KB write/read successful")
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		deleteDir := filepath.Join(mountpoint, ".delete", tableName)
		os.MkdirAll(deleteDir, 0755)
		sqlPath := filepath.Join(deleteDir, "sql")
		os.WriteFile(sqlPath, []byte("DROP TABLE "+tableName+";"), 0644)
		commitPath := filepath.Join(deleteDir, ".commit")
		touchTriggerFile(t, commitPath)
	})
}

// TestNFS_LargeWrite_1MB tests writing and reading back 1MB of content.
//
// WRITE ISSUE CAPTURED: Very large file corruption with many chunks
//
// 1MB with typical 32KB wsize = ~32 WRITE RPCs. This exercises the
// overlay logic extensively and is more likely to expose ordering bugs.
//
// Note: This test may be slow with current architecture due to O(n²)
// data transfer pattern (each write reads existing data, overlays, commits).
func TestNFS_LargeWrite_1MB(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tableName := "large_write_1mb_test"

	// Create table
	t.Run("Setup", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		sqlPath := filepath.Join(createDir, "sql")
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT
		);
		INSERT INTO %s (id, data) VALUES (1, 'placeholder');`, tableName, tableName)

		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("Write1MB", func(t *testing.T) {
		// Generate 1MB content with identifiable pattern
		content := bytes.Repeat([]byte("0123456789ABCDEF"), 65536) // 16 * 65536 = 1MB
		if len(content) != 1024*1024 {
			t.Fatalf("Expected 1MB, got %d bytes", len(content))
		}

		dataPath := filepath.Join(mountpoint, tableName, "1", "data")

		// Write
		f, err := os.Create(dataPath)
		if err != nil {
			t.Fatalf("Failed to create: %v", err)
		}

		n, err := f.Write(content)
		if err != nil {
			f.Close()
			t.Fatalf("Failed to write: %v", err)
		}
		if n != len(content) {
			f.Close()
			t.Fatalf("Short write: %d != %d", n, len(content))
		}

		f.Sync()
		f.Close()

		time.Sleep(300 * time.Millisecond)

		// Read back (read adds trailing newline - text file semantics)
		result, err := os.ReadFile(dataPath)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		expectedResult := append(content, '\n')
		if len(result) != len(expectedResult) {
			t.Errorf("Length mismatch: got %d, expected %d (including trailing newline)", len(result), len(expectedResult))
		}

		if !bytes.Equal(result, expectedResult) {
			t.Errorf("Content mismatch in 1MB write")
		} else {
			t.Logf("1MB write/read successful")
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		deleteDir := filepath.Join(mountpoint, ".delete", tableName)
		os.MkdirAll(deleteDir, 0755)
		sqlPath := filepath.Join(deleteDir, "sql")
		os.WriteFile(sqlPath, []byte("DROP TABLE "+tableName+";"), 0644)
		commitPath := filepath.Join(deleteDir, ".commit")
		touchTriggerFile(t, commitPath)
	})
}

// TestNFS_LargeWrite_Checksum tests large write integrity using SHA-256.
//
// WRITE ISSUE CAPTURED: Byte-level corruption in large writes
//
// Uses cryptographic checksum to detect any byte-level corruption that
// might occur during chunked writes. More sensitive than pattern matching.
func TestNFS_LargeWrite_Checksum(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tableName := "checksum_test"

	// Create table
	t.Run("Setup", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		sqlPath := filepath.Join(createDir, "sql")
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT
		);
		INSERT INTO %s (id, data) VALUES (1, 'placeholder');`, tableName, tableName)

		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("WriteAndVerifyChecksum", func(t *testing.T) {
		// Generate pseudo-random content (deterministic for reproducibility)
		// Using a simple PRNG pattern
		content := make([]byte, 256*1024) // 256KB
		for i := range content {
			// Simple deterministic pattern based on position
			content[i] = byte((i*7 + i/256*13) % 256)
		}

		// Read adds trailing newline, so include it in expected checksum
		expectedResult := append(content, '\n')
		expectedChecksum := sha256.Sum256(expectedResult)

		dataPath := filepath.Join(mountpoint, tableName, "1", "data")

		// Write
		f, err := os.Create(dataPath)
		if err != nil {
			t.Fatalf("Failed to create: %v", err)
		}
		f.Write(content)
		f.Sync()
		f.Close()

		time.Sleep(200 * time.Millisecond)

		// Read and verify checksum (read adds trailing newline - text file semantics)
		result, err := os.ReadFile(dataPath)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		actualChecksum := sha256.Sum256(result)

		if expectedChecksum != actualChecksum {
			t.Errorf("Checksum mismatch!\n  Expected: %x\n  Actual:   %x", expectedChecksum, actualChecksum)
			t.Errorf("Length: expected %d, got %d", len(expectedResult), len(result))
		} else {
			t.Logf("Checksum verified: %x", actualChecksum[:8])
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		deleteDir := filepath.Join(mountpoint, ".delete", tableName)
		os.MkdirAll(deleteDir, 0755)
		sqlPath := filepath.Join(deleteDir, "sql")
		os.WriteFile(sqlPath, []byte("DROP TABLE "+tableName+";"), 0644)
		commitPath := filepath.Join(deleteDir, ".commit")
		touchTriggerFile(t, commitPath)
	})
}

// TestNFS_LargeWrite_BinaryContent tests writing binary content with all byte values.
//
// WRITE ISSUE CAPTURED: Encoding/escaping issues with binary data
//
// Ensures that all 256 byte values (0x00-0xFF) are correctly stored and
// retrieved. Catches issues with NULL bytes, control characters, or
// encoding transformations.
func TestNFS_LargeWrite_BinaryContent(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tableName := "binary_test"

	// Create table with BYTEA column for binary data
	t.Run("Setup", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		sqlPath := filepath.Join(createDir, "sql")
		// Note: Using TEXT for now as BYTEA handling may differ
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			id INT PRIMARY KEY,
			data TEXT
		);
		INSERT INTO %s (id, data) VALUES (1, 'placeholder');`, tableName, tableName)

		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("WriteAllByteValues", func(t *testing.T) {
		// Content with all byte values 0x00-0xFF, repeated
		content := make([]byte, 256*4) // 1KB with 4 repetitions
		for i := range content {
			content[i] = byte(i % 256)
		}

		dataPath := filepath.Join(mountpoint, tableName, "1", "data")

		// Write
		f, err := os.Create(dataPath)
		if err != nil {
			t.Fatalf("Failed to create: %v", err)
		}
		f.Write(content)
		f.Sync()
		f.Close()

		time.Sleep(200 * time.Millisecond)

		// Read back
		result, err := os.ReadFile(dataPath)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}

		if len(result) != len(content) {
			t.Errorf("Length mismatch: got %d, expected %d", len(result), len(content))
		}

		// Check each byte value is preserved
		mismatches := 0
		for i := 0; i < len(content) && i < len(result); i++ {
			if content[i] != result[i] {
				if mismatches < 5 {
					t.Errorf("Byte %d: expected 0x%02x, got 0x%02x", i, content[i], result[i])
				}
				mismatches++
			}
		}

		if mismatches > 0 {
			t.Errorf("Total byte mismatches: %d", mismatches)
		} else {
			t.Logf("All 256 byte values preserved correctly")
		}
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		deleteDir := filepath.Join(mountpoint, ".delete", tableName)
		os.MkdirAll(deleteDir, 0755)
		sqlPath := filepath.Join(deleteDir, "sql")
		os.WriteFile(sqlPath, []byte("DROP TABLE "+tableName+";"), 0644)
		commitPath := filepath.Join(deleteDir, ".commit")
		touchTriggerFile(t, commitPath)
	})
}

// TestNFS_LargeWrite_JSON tests writing large JSON content.
//
// WRITE ISSUE CAPTURED: JSON parsing fails with stale data at end
//
// When writing JSON, the entire content must be valid JSON. If old data
// remains at the end of the buffer, JSON parsing will fail with errors
// like "invalid character after top-level value".
func TestNFS_LargeWrite_JSON(t *testing.T) {
	checkFUSEMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	tableName := "large_json_test"

	// Create table
	t.Run("Setup", func(t *testing.T) {
		createDir := filepath.Join(mountpoint, ".create", tableName)
		if err := os.MkdirAll(createDir, 0755); err != nil {
			t.Fatalf("Failed to mkdir: %v", err)
		}

		sqlPath := filepath.Join(createDir, "sql")
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT,
			description TEXT
		);
		INSERT INTO %s (id, name, description) VALUES
			(1, 'InitialName', '%s');`,
			tableName, tableName, strings.Repeat("x", 1000))

		if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
			t.Fatalf("Failed to write DDL: %v", err)
		}

		commitPath := filepath.Join(createDir, ".commit")
		if err := touchTriggerFile(t, commitPath); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	})

	t.Run("WriteLargeJSON", func(t *testing.T) {
		// Create JSON with large description field
		largeDesc := strings.Repeat("This is a test description. ", 100) // ~2.9KB
		jsonContent := fmt.Sprintf(`{"name": "UpdatedName", "description": "%s"}`, largeDesc)

		jsonPath := filepath.Join(mountpoint, tableName, "1.json")

		// Write
		if err := os.WriteFile(jsonPath, []byte(jsonContent), 0644); err != nil {
			t.Fatalf("Failed to write JSON: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		// Read back
		result, err := os.ReadFile(jsonPath)
		if err != nil {
			t.Fatalf("Failed to read JSON: %v", err)
		}

		// Verify the JSON contains our updated values
		if !strings.Contains(string(result), "UpdatedName") {
			t.Errorf("Expected 'UpdatedName' in result, got: %s", string(result)[:min(100, len(result))])
		}

		// Verify no stale data (old description was longer)
		if strings.Contains(string(result), "InitialName") {
			t.Errorf("Found stale 'InitialName' in result - buffer not properly replaced")
		}

		t.Logf("Large JSON write successful, result length: %d", len(result))
	})

	// Cleanup
	t.Run("Cleanup", func(t *testing.T) {
		deleteDir := filepath.Join(mountpoint, ".delete", tableName)
		os.MkdirAll(deleteDir, 0755)
		sqlPath := filepath.Join(deleteDir, "sql")
		os.WriteFile(sqlPath, []byte("DROP TABLE "+tableName+";"), 0644)
		commitPath := filepath.Join(deleteDir, ".commit")
		touchTriggerFile(t, commitPath)
	})
}
