package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// TestNewByDirNode tests ByDirNode creation
func TestNewByDirNode(t *testing.T) {
	cfg := &config.Config{}

	node := NewByDirNode(cfg, nil, nil, "public", "users", nil)

	if node == nil {
		t.Fatal("NewByDirNode returned nil")
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

// TestByDirNode_Getattr tests that Getattr returns correct directory attributes
func TestByDirNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewByDirNode(cfg, nil, nil, "public", "users", nil)
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

// TestByDirNode_Interfaces verifies ByDirNode implements required interfaces
func TestByDirNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	node := NewByDirNode(cfg, nil, nil, "public", "users", nil)

	// Verify interface implementations
	var _ fs.InodeEmbedder = node
	var _ fs.NodeReaddirer = node
	var _ fs.NodeLookuper = node
	var _ fs.NodeGetattrer = node
}

// TestByDirNode_DifferentTables tests ByDirNode for different tables
func TestByDirNode_DifferentTables(t *testing.T) {
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
			node := NewByDirNode(cfg, nil, nil, tc.schema, tc.tableName, nil)

			if node.schema != tc.schema {
				t.Errorf("Expected schema='%s', got '%s'", tc.schema, node.schema)
			}

			if node.tableName != tc.tableName {
				t.Errorf("Expected tableName='%s', got '%s'", tc.tableName, node.tableName)
			}

			// Verify Getattr works
			ctx := context.Background()
			var out fuse.AttrOut
			errno := node.Getattr(ctx, nil, &out)
			if errno != 0 {
				t.Errorf("Expected errno=0, got %d", errno)
			}
		})
	}
}

// TestByDirNode_FieldsSet verifies all fields are set correctly
func TestByDirNode_FieldsSet(t *testing.T) {
	cfg := &config.Config{}
	cache := &tigerfs.MetadataCache{}
	partialRows := &PartialRowTracker{}

	node := NewByDirNode(cfg, nil, cache, "myschema", "mytable", partialRows)

	if node.cfg != cfg {
		t.Error("cfg not set correctly")
	}

	if node.cache != cache {
		t.Error("cache not set correctly")
	}

	if node.partialRows != partialRows {
		t.Error("partialRows not set correctly")
	}

	if node.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", node.schema)
	}

	if node.tableName != "mytable" {
		t.Errorf("Expected tableName='mytable', got '%s'", node.tableName)
	}
}
