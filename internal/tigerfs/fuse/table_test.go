package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewTableNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	partialRows := NewPartialRowTracker(nil)
	tableNode := NewTableNode(cfg, nil, nil, "myschema", "users", partialRows)

	if tableNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if tableNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", tableNode.tableName)
	}

	if tableNode.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", tableNode.schema)
	}

	if tableNode.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}

	if tableNode.db != nil {
		t.Error("Expected db to be nil")
	}
}

// TestTableNode_Interfaces verifies that TableNode implements required interfaces
func TestTableNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	tableNode := NewTableNode(cfg, nil, nil, "public", "users", nil)

	// Verify interface assertions compile (checked at compile time via var _ declarations)
	_ = tableNode
}

// TestTableNode_Getattr tests that Getattr returns correct directory attributes
func TestTableNode_Getattr(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	tableNode := NewTableNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := tableNode.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check mode is directory with 755 permissions
	expectedMode := uint32(0700 | syscall.S_IFDIR)
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

// TestTableNode_Getattr_DifferentTables tests Getattr for various table names
func TestTableNode_Getattr_DifferentTables(t *testing.T) {
	cfg := &config.Config{}
	ctx := context.Background()

	tables := []string{"users", "orders", "products", "inventory_items", "test_table_123"}

	for _, tableName := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableNode := NewTableNode(cfg, nil, nil, "public", tableName, nil)

			var out fuse.AttrOut
			errno := tableNode.Getattr(ctx, nil, &out)

			if errno != 0 {
				t.Errorf("Expected errno=0 for table %s, got %d", tableName, errno)
			}

			// All tables should return same attributes
			if out.Mode != uint32(0700|syscall.S_IFDIR) {
				t.Errorf("Expected directory mode for table %s", tableName)
			}
		})
	}
}

// TestTableNode_MultipleSchemas tests TableNode with different schemas
func TestTableNode_MultipleSchemas(t *testing.T) {
	cfg := &config.Config{}

	testCases := []struct {
		schema    string
		tableName string
	}{
		{"public", "users"},
		{"custom_schema", "orders"},
		{"analytics", "events"},
		{"", "test"}, // empty schema
	}

	for _, tc := range testCases {
		t.Run(tc.schema+"_"+tc.tableName, func(t *testing.T) {
			node := NewTableNode(cfg, nil, nil, tc.schema, tc.tableName, nil)

			if node.schema != tc.schema {
				t.Errorf("Expected schema=%q, got %q", tc.schema, node.schema)
			}
			if node.tableName != tc.tableName {
				t.Errorf("Expected tableName=%q, got %q", tc.tableName, node.tableName)
			}
		})
	}
}

// Note: Readdir, Lookup, Unlink, Rmdir, Mkdir tests require database integration
// See test/integration/ for full CRUD operation tests
