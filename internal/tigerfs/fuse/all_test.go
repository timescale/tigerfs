package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewAllRowsNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
		DirListingLimit:     10000,
	}

	partialRows := NewPartialRowTracker(nil)
	allNode := NewAllRowsNode(cfg, nil, nil, "myschema", "users", partialRows)

	if allNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if allNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", allNode.tableName)
	}

	if allNode.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", allNode.schema)
	}

	if allNode.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}

	if allNode.db != nil {
		t.Error("Expected db to be nil")
	}

	if allNode.cache != nil {
		t.Error("Expected cache to be nil")
	}
}

func TestAllRowsNode_Getattr(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
		DirListingLimit:     10000,
	}

	allNode := NewAllRowsNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := allNode.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Check mode is directory with 755 permissions
	expectedMode := uint32(0755 | syscall.S_IFDIR)
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

func TestAllRowsNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	allNode := NewAllRowsNode(cfg, nil, nil, "public", "users", nil)

	// Verify interface assertions compile (checked at compile time via var _ declarations)
	_ = allNode
}
