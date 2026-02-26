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

// TestNewOrderDirNode tests OrderDirNode creation
func TestNewOrderDirNode(t *testing.T) {
	cfg := &config.Config{}

	node := NewOrderDirNode(cfg, nil, nil, "public", "users", nil)

	if node == nil {
		t.Fatal("NewOrderDirNode returned nil")
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

// TestOrderDirNode_Getattr tests that Getattr returns correct directory attributes
func TestOrderDirNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewOrderDirNode(cfg, nil, nil, "public", "users", nil)
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

	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}
}

// TestOrderDirNode_Interfaces verifies OrderDirNode implements required interfaces
func TestOrderDirNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	node := NewOrderDirNode(cfg, nil, nil, "public", "users", nil)

	var _ fs.InodeEmbedder = node
	var _ fs.NodeReaddirer = node
	var _ fs.NodeLookuper = node
	var _ fs.NodeGetattrer = node
}

// TestNewOrderColumnNode tests OrderColumnNode creation
func TestNewOrderColumnNode(t *testing.T) {
	cfg := &config.Config{}

	node := NewOrderColumnNode(cfg, nil, nil, "public", "users", "created_at", nil)

	if node == nil {
		t.Fatal("NewOrderColumnNode returned nil")
	}

	if node.orderColumn != "created_at" {
		t.Errorf("Expected orderColumn='created_at', got '%s'", node.orderColumn)
	}
}

// TestOrderColumnNode_Getattr tests OrderColumnNode attributes
func TestOrderColumnNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewOrderColumnNode(cfg, nil, nil, "public", "users", "created_at", nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expectedMode := uint32(0500 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}
}

// TestOrderColumnNode_Readdir tests that Readdir returns capabilities per ADR-007.
// After .order/<col>/, should expose: .first/, .last/, .sample/, .export/
func TestOrderColumnNode_Readdir(t *testing.T) {
	cfg := &config.Config{}
	node := NewOrderColumnNode(cfg, nil, nil, "public", "users", "created_at", nil)
	ctx := context.Background()

	stream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Fatal("Expected non-nil stream")
	}

	// Collect entries
	var entries []fuse.DirEntry
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry)
	}

	// Per ADR-007: .first/, .last/, .sample/, .export/ are allowed after .order/<col>/
	if len(entries) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(entries))
	}

	// Check for all expected entries
	hasFirst := false
	hasLast := false
	hasSample := false
	hasExport := false
	for _, e := range entries {
		switch e.Name {
		case DirFirst:
			hasFirst = true
		case DirLast:
			hasLast = true
		case DirSample:
			hasSample = true
		case DirExport:
			hasExport = true
		}
	}

	if !hasFirst {
		t.Errorf("Expected %q entry", DirFirst)
	}
	if !hasLast {
		t.Errorf("Expected %q entry", DirLast)
	}
	if !hasSample {
		t.Errorf("Expected %q entry", DirSample)
	}
	if !hasExport {
		t.Errorf("Expected %q entry", DirExport)
	}
}

// TestNewOrderedPaginationNode tests OrderedPaginationNode creation
func TestNewOrderedPaginationNode(t *testing.T) {
	cfg := &config.Config{}

	node := NewOrderedPaginationNode(cfg, nil, nil, "public", "users", "created_at", PaginationFirst, nil)

	if node == nil {
		t.Fatal("NewOrderedPaginationNode returned nil")
	}

	if node.orderColumn != "created_at" {
		t.Errorf("Expected orderColumn='created_at', got '%s'", node.orderColumn)
	}

	if node.paginationType != PaginationFirst {
		t.Errorf("Expected paginationType=PaginationFirst, got '%s'", node.paginationType)
	}
}

// TestOrderedPaginationNode_Readdir tests empty readdir (user must specify N)
func TestOrderedPaginationNode_Readdir(t *testing.T) {
	cfg := &config.Config{}
	node := NewOrderedPaginationNode(cfg, nil, nil, "public", "users", "created_at", PaginationFirst, nil)
	ctx := context.Background()

	stream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Fatal("Expected non-nil stream")
	}

	// Should be empty
	if stream.HasNext() {
		t.Error("Expected empty directory")
	}
}

// TestNewOrderedPaginationLimitNode tests OrderedPaginationLimitNode creation
func TestNewOrderedPaginationLimitNode(t *testing.T) {
	cfg := &config.Config{}

	node := NewOrderedPaginationLimitNode(cfg, nil, nil, "public", "users", "created_at", PaginationFirst, 100, nil)

	if node == nil {
		t.Fatal("NewOrderedPaginationLimitNode returned nil")
	}

	if node.limit != 100 {
		t.Errorf("Expected limit=100, got %d", node.limit)
	}

	if node.orderColumn != "created_at" {
		t.Errorf("Expected orderColumn='created_at', got '%s'", node.orderColumn)
	}
}

// TestOrderedPaginationLimitNode_Getattr tests attributes
func TestOrderedPaginationLimitNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewOrderedPaginationLimitNode(cfg, nil, nil, "public", "users", "created_at", PaginationFirst, 100, nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	// Should be writable directory (mode 0700)
	expectedMode := uint32(0700 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}
}

// TestOrderDirNode_FieldsSet verifies all fields are set correctly
func TestOrderDirNode_FieldsSet(t *testing.T) {
	cfg := &config.Config{}
	cache := &tigerfs.MetadataCache{}
	partialRows := &PartialRowTracker{}

	node := NewOrderDirNode(cfg, nil, cache, "myschema", "mytable", partialRows)

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
