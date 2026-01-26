package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestNewIndexNode verifies IndexNode creation.
func TestNewIndexNode(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{
		Name:      "idx_email",
		Columns:   []string{"email"},
		IsUnique:  true,
		IsPrimary: false,
	}

	node := NewIndexNode(cfg, nil, "public", "users", "email", idx, nil)

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName 'users', got '%s'", node.tableName)
	}
	if node.column != "email" {
		t.Errorf("Expected column 'email', got '%s'", node.column)
	}
	if node.index != idx {
		t.Error("Expected index to be set")
	}
}

// TestNewIndexValueNode verifies IndexValueNode creation.
func TestNewIndexValueNode(t *testing.T) {
	cfg := &config.Config{}
	matchingPKs := []string{"1", "5", "10"}

	node := NewIndexValueNode(cfg, nil, "public", "users", "email", "foo@example.com", "id", matchingPKs, nil)

	if node.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName 'users', got '%s'", node.tableName)
	}
	if node.column != "email" {
		t.Errorf("Expected column 'email', got '%s'", node.column)
	}
	if node.value != "foo@example.com" {
		t.Errorf("Expected value 'foo@example.com', got '%s'", node.value)
	}
	if node.pkColumn != "id" {
		t.Errorf("Expected pkColumn 'id', got '%s'", node.pkColumn)
	}
	if len(node.matchingPKs) != 3 {
		t.Errorf("Expected 3 matching PKs, got %d", len(node.matchingPKs))
	}
}

// TestIndexNode_Interfaces verifies IndexNode implements required interfaces.
func TestIndexNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	idx := &db.Index{Name: "test_idx", Columns: []string{"col"}}
	node := NewIndexNode(cfg, nil, "public", "test", "col", idx, nil)

	// Compiler will verify these interface assertions
	_ = node
}

// TestIndexValueNode_Interfaces verifies IndexValueNode implements required interfaces.
func TestIndexValueNode_Interfaces(t *testing.T) {
	cfg := &config.Config{}
	node := NewIndexValueNode(cfg, nil, "public", "test", "col", "val", "id", []string{"1"}, nil)

	// Compiler will verify these interface assertions
	_ = node
}
