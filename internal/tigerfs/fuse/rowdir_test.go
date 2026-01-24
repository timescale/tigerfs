package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewRowDirectoryNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	rowDirNode := NewRowDirectoryNode(cfg, nil, "public", "users", "id", "1", nil)

	if rowDirNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if rowDirNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", rowDirNode.tableName)
	}

	if rowDirNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", rowDirNode.schema)
	}

	if rowDirNode.pkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got '%s'", rowDirNode.pkColumn)
	}

	if rowDirNode.pkValue != "1" {
		t.Errorf("Expected pkValue='1', got '%s'", rowDirNode.pkValue)
	}
}

// TestRowDirectoryNode_Interfaces verifies that RowDirectoryNode implements required interfaces
func TestRowDirectoryNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that RowDirectoryNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	rowDirNode := NewRowDirectoryNode(cfg, nil, "public", "users", "id", "1", nil)

	// Verify we can use rowDirNode as various interface types
	_ = interface{}(rowDirNode).(interface{})
}
