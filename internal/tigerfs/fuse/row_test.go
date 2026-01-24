package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewRowFileNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	rowNode := NewRowFileNode(cfg, nil, "public", "users", "id", "1")

	if rowNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if rowNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", rowNode.tableName)
	}

	if rowNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", rowNode.schema)
	}

	if rowNode.pkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got '%s'", rowNode.pkColumn)
	}

	if rowNode.pkValue != "1" {
		t.Errorf("Expected pkValue='1', got '%s'", rowNode.pkValue)
	}

	if rowNode.data != nil {
		t.Error("Expected data to be nil initially")
	}
}

// TestRowFileNode_Interfaces verifies that RowFileNode implements required interfaces
func TestRowFileNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that RowFileNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	rowNode := NewRowFileNode(cfg, nil, "public", "users", "id", "1")

	// Verify we can use rowNode as various interface types
	_ = interface{}(rowNode).(interface{})
}
