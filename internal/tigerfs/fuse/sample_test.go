package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewSampleNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:   "public",
		DirListingLimit: 10000,
	}

	partialRows := NewPartialRowTracker(nil)
	node := NewSampleNode(cfg, nil, nil, "myschema", "users", partialRows)

	if node.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", node.tableName)
	}

	if node.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", node.schema)
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}

	if node.db != nil {
		t.Error("Expected db to be nil")
	}

	if node.cache != nil {
		t.Error("Expected cache to be nil")
	}
}

func TestSampleNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewSampleNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expectedMode := uint32(0755 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}
}

func TestSampleNode_Readdir_Empty(t *testing.T) {
	cfg := &config.Config{}
	node := NewSampleNode(cfg, nil, nil, "public", "users", nil)
	ctx := context.Background()

	stream, errno := node.Readdir(ctx)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	if stream == nil {
		t.Error("Expected non-nil stream")
	}

	// Stream should be empty (user must specify N)
	if stream.HasNext() {
		t.Error("Expected empty directory stream")
	}
}

func TestNewSampleLimitNode(t *testing.T) {
	cfg := &config.Config{}
	partialRows := NewPartialRowTracker(nil)

	node := NewSampleLimitNode(cfg, nil, nil, "myschema", "users", 100, partialRows)

	if node.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", node.tableName)
	}

	if node.schema != "myschema" {
		t.Errorf("Expected schema='myschema', got '%s'", node.schema)
	}

	if node.sampleSize != 100 {
		t.Errorf("Expected sampleSize=100, got %d", node.sampleSize)
	}

	if node.partialRows != partialRows {
		t.Error("Expected partialRows to be set")
	}
}

func TestSampleLimitNode_Getattr(t *testing.T) {
	cfg := &config.Config{}
	node := NewSampleLimitNode(cfg, nil, nil, "public", "users", 50, nil)
	ctx := context.Background()

	var out fuse.AttrOut
	errno := node.Getattr(ctx, nil, &out)

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	expectedMode := uint32(0755 | syscall.S_IFDIR)
	if out.Mode != expectedMode {
		t.Errorf("Expected Mode=0x%x, got 0x%x", expectedMode, out.Mode)
	}

	if out.Nlink != 2 {
		t.Errorf("Expected Nlink=2, got %d", out.Nlink)
	}
}

func TestSampleNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	sampleNode := NewSampleNode(cfg, nil, nil, "public", "users", nil)
	limitNode := NewSampleLimitNode(cfg, nil, nil, "public", "users", 100, nil)

	// Verify interface assertions compile (checked at compile time via var _ declarations)
	_ = sampleNode
	_ = limitNode
}
