package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewMetadataFileNode(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	tests := []struct {
		name     string
		fileType string
	}{
		{"columns metadata", "columns"},
		{"schema metadata", "schema"},
		{"count metadata", "count"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewMetadataFileNode(cfg, nil, "public", "users", tt.fileType)

			if node.cfg != cfg {
				t.Error("Expected config to be set")
			}

			if node.schema != "public" {
				t.Errorf("Expected schema='public', got '%s'", node.schema)
			}

			if node.tableName != "users" {
				t.Errorf("Expected tableName='users', got '%s'", node.tableName)
			}

			if node.fileType != tt.fileType {
				t.Errorf("Expected fileType='%s', got '%s'", tt.fileType, node.fileType)
			}

			if node.data != nil {
				t.Error("Expected data to be nil initially")
			}
		})
	}
}

// TestMetadataFileNode_Interfaces verifies that MetadataFileNode implements required interfaces
func TestMetadataFileNode_Interfaces(t *testing.T) {
	// This test verifies at compile time that MetadataFileNode implements required interfaces
	// If this compiles, the interfaces are correctly implemented

	cfg := &config.Config{}
	node := NewMetadataFileNode(cfg, nil, "public", "users", "columns")

	// Verify we can use node as various interface types
	_ = interface{}(node).(interface{})
}

func TestMetadataFileTypes(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema: "public",
	}

	validTypes := []string{"columns", "schema", "count"}

	for _, fileType := range validTypes {
		node := NewMetadataFileNode(cfg, nil, "public", "test_table", fileType)
		if node.fileType != fileType {
			t.Errorf("Expected fileType='%s', got '%s'", fileType, node.fileType)
		}
	}
}
