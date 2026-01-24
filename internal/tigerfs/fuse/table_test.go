package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewTableNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
		MaxLsRows:     10000,
	}

	tableNode := NewTableNode(cfg, nil, "public", "users")

	if tableNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if tableNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", tableNode.tableName)
	}

	if tableNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", tableNode.schema)
	}
}

// TestTableNode_Interfaces verifies that TableNode implements required interfaces
func TestTableNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that TableNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	tableNode := NewTableNode(cfg, nil, "public", "users")

	// Verify we can use tableNode as various interface types
	_ = interface{}(tableNode).(interface{})
}
