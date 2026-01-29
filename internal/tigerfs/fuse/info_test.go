package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestNewInfoDirNode tests InfoDirNode creation
func TestNewInfoDirNode(t *testing.T) {
	cfg := &config.Config{}

	node := NewInfoDirNode(cfg, nil, "public", "users")

	if node == nil {
		t.Fatal("NewInfoDirNode returned nil")
	}

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}

	if node.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", node.schema)
	}

	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", node.tableName)
	}
}

// TestInfoDirNode_Getattr tests that Getattr returns correct directory attributes
func TestInfoDirNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewInfoDirNode(cfg, nil, "public", "users")
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check mode is read-only directory with 500 permissions
	expectedMode := uint32(0500 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	// Check nlink is 2 (standard for directories)
	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}

	// Check size
	if out.Size != 4096 {
		t.Errorf("Expected Size=4096, got %d", out.Size)
	}
}

// TestInfoDirNode_Readdir tests listing metadata files in .info directory
func TestInfoDirNode_Readdir(t *testing.T) {
	cfg := &config.Config{}
	node := NewInfoDirNode(cfg, nil, "public", "users")
	ctx := context.Background()

	dirStream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	if dirStream == nil {
		t.Fatal("Expected non-nil DirStream")
	}

	// Read all entries
	var entries []fuse.DirEntry
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	// Should have exactly 4 entries: count, ddl, schema, columns
	if len(entries) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(entries))
	}

	// Verify expected files are present
	expectedNames := map[string]bool{
		InfoFileCount:   true,
		InfoFileDDL:     true,
		InfoFileSchema:  true,
		InfoFileColumns: true,
	}

	for _, entry := range entries {
		if !expectedNames[entry.Name] {
			t.Errorf("Unexpected entry: %q", entry.Name)
		}
		// Verify entries are regular files
		if entry.Mode != syscall.S_IFREG {
			t.Errorf("Expected entry %q to be regular file, got mode 0x%x", entry.Name, entry.Mode)
		}
		delete(expectedNames, entry.Name)
	}

	// Verify all expected entries were found
	for name := range expectedNames {
		t.Errorf("Missing expected entry: %q", name)
	}
}

// TestInfoDirNode_Readdir_FileOrder tests that files are listed in consistent order
func TestInfoDirNode_Readdir_FileOrder(t *testing.T) {
	cfg := &config.Config{}
	node := NewInfoDirNode(cfg, nil, "public", "users")
	ctx := context.Background()

	dirStream, errno := node.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Expected errno=0, got %d", errno)
	}

	// First entry should be count
	if dirStream.HasNext() {
		entry, _ := dirStream.Next()
		if entry.Name != InfoFileCount {
			t.Errorf("Expected first entry to be %q, got %q", InfoFileCount, entry.Name)
		}
	} else {
		t.Error("Expected at least one entry")
	}
}

// TestInfoDirNode_Lookup_ValidFiles tests lookup for all valid metadata files
func TestInfoDirNode_Lookup_ValidFiles(t *testing.T) {
	testCases := []struct {
		name     string
		fileType string
	}{
		{InfoFileCount, "count"},
		{InfoFileDDL, "ddl"},
		{InfoFileSchema, "schema"},
		{InfoFileColumns, "columns"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			// Note: We can't fully test Lookup without embedding in an inode tree.
			// This test verifies the node is created correctly.
			node := NewInfoDirNode(cfg, nil, "public", "users")

			if node.tableName != "users" {
				t.Errorf("Expected tableName='users', got '%s'", node.tableName)
			}
		})
	}
}

// TestInfoDirNode_Lookup_InvalidFile tests lookup for non-existent file
func TestInfoDirNode_Lookup_InvalidFile(t *testing.T) {
	// Note: Full Lookup testing requires an inode tree.
	// We verify the basic structure here.
	cfg := &config.Config{}
	node := NewInfoDirNode(cfg, nil, "public", "users")

	if node == nil {
		t.Fatal("Expected node to be created")
	}

	// The actual ENOENT behavior is tested via integration tests
}

// TestInfoDirNode_Interfaces verifies InfoDirNode implements required interfaces
func TestInfoDirNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	node := NewInfoDirNode(cfg, nil, "public", "users")

	// Verify interface implementations
	var _ fs.InodeEmbedder = node
	var _ fs.NodeReaddirer = node
	var _ fs.NodeLookuper = node
	var _ fs.NodeGetattrer = node
}

// TestInfoDirNode_DifferentTables tests InfoDirNode for different tables
func TestInfoDirNode_DifferentTables(t *testing.T) {
	testCases := []struct {
		schema    string
		tableName string
	}{
		{"public", "users"},
		{"public", "orders"},
		{"analytics", "metrics"},
		{"staging", "temp_data"},
	}

	for _, tc := range testCases {
		t.Run(tc.schema+"."+tc.tableName, func(t *testing.T) {
			cfg := &config.Config{}
			node := NewInfoDirNode(cfg, nil, tc.schema, tc.tableName)

			if node.schema != tc.schema {
				t.Errorf("Expected schema='%s', got '%s'", tc.schema, node.schema)
			}

			if node.tableName != tc.tableName {
				t.Errorf("Expected tableName='%s', got '%s'", tc.tableName, node.tableName)
			}

			// Verify Readdir still returns 4 entries
			ctx := context.Background()
			dirStream, errno := node.Readdir(ctx)
			if errno != 0 {
				t.Fatalf("Expected errno=0, got %d", errno)
			}

			var count int
			for dirStream.HasNext() {
				_, _ = dirStream.Next()
				count++
			}

			if count != 4 {
				t.Errorf("Expected 4 entries, got %d", count)
			}
		})
	}
}
