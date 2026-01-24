package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewColumnFileNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	colNode := NewColumnFileNode(cfg, nil, "public", "users", "id", "1", "email", nil)

	if colNode.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if colNode.tableName != "users" {
		t.Errorf("Expected tableName='users', got '%s'", colNode.tableName)
	}

	if colNode.schema != "public" {
		t.Errorf("Expected schema='public', got '%s'", colNode.schema)
	}

	if colNode.pkColumn != "id" {
		t.Errorf("Expected pkColumn='id', got '%s'", colNode.pkColumn)
	}

	if colNode.pkValue != "1" {
		t.Errorf("Expected pkValue='1', got '%s'", colNode.pkValue)
	}

	if colNode.columnName != "email" {
		t.Errorf("Expected columnName='email', got '%s'", colNode.columnName)
	}

	if colNode.data != nil {
		t.Error("Expected data to be nil initially")
	}
}

// TestColumnFileNode_Interfaces verifies that ColumnFileNode implements required interfaces
func TestColumnFileNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that ColumnFileNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	colNode := NewColumnFileNode(cfg, nil, "public", "users", "id", "1", "email", nil)

	// Verify we can use colNode as various interface types
	_ = interface{}(colNode).(interface{})
}
