package db

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestGetTablePermissions(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_permissions (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_permissions")
	}()

	// Get permissions - superuser should have all permissions
	perms, err := client.GetTablePermissions(ctx, "public", "test_permissions")
	if err != nil {
		t.Fatalf("GetTablePermissions() failed: %v", err)
	}

	// Superuser (or table owner) should have all permissions
	if !perms.CanSelect {
		t.Error("Expected CanSelect=true for table owner")
	}
	if !perms.CanInsert {
		t.Error("Expected CanInsert=true for table owner")
	}
	if !perms.CanUpdate {
		t.Error("Expected CanUpdate=true for table owner")
	}
	if !perms.CanDelete {
		t.Error("Expected CanDelete=true for table owner")
	}

	t.Logf("Permissions: SELECT=%t, INSERT=%t, UPDATE=%t, DELETE=%t",
		perms.CanSelect, perms.CanInsert, perms.CanUpdate, perms.CanDelete)
}

func TestTablePermissions_IsReadOnly(t *testing.T) {
	tests := []struct {
		name     string
		perms    TablePermissions
		expected bool
	}{
		{
			name:     "select only is read-only",
			perms:    TablePermissions{CanSelect: true, CanInsert: false, CanUpdate: false, CanDelete: false},
			expected: true,
		},
		{
			name:     "select and insert is not read-only",
			perms:    TablePermissions{CanSelect: true, CanInsert: true, CanUpdate: false, CanDelete: false},
			expected: false,
		},
		{
			name:     "select and update is not read-only",
			perms:    TablePermissions{CanSelect: true, CanInsert: false, CanUpdate: true, CanDelete: false},
			expected: false,
		},
		{
			name:     "select and delete is not read-only",
			perms:    TablePermissions{CanSelect: true, CanInsert: false, CanUpdate: false, CanDelete: true},
			expected: false,
		},
		{
			name:     "all permissions is not read-only",
			perms:    TablePermissions{CanSelect: true, CanInsert: true, CanUpdate: true, CanDelete: true},
			expected: false,
		},
		{
			name:     "no permissions is not read-only",
			perms:    TablePermissions{CanSelect: false, CanInsert: false, CanUpdate: false, CanDelete: false},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.perms.IsReadOnly(); got != tt.expected {
				t.Errorf("IsReadOnly() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTablePermissions_HasAnyPermission(t *testing.T) {
	tests := []struct {
		name     string
		perms    TablePermissions
		expected bool
	}{
		{
			name:     "no permissions",
			perms:    TablePermissions{CanSelect: false, CanInsert: false, CanUpdate: false, CanDelete: false},
			expected: false,
		},
		{
			name:     "select only",
			perms:    TablePermissions{CanSelect: true, CanInsert: false, CanUpdate: false, CanDelete: false},
			expected: true,
		},
		{
			name:     "insert only",
			perms:    TablePermissions{CanSelect: false, CanInsert: true, CanUpdate: false, CanDelete: false},
			expected: true,
		},
		{
			name:     "update only",
			perms:    TablePermissions{CanSelect: false, CanInsert: false, CanUpdate: true, CanDelete: false},
			expected: true,
		},
		{
			name:     "delete only",
			perms:    TablePermissions{CanSelect: false, CanInsert: false, CanUpdate: false, CanDelete: true},
			expected: true,
		},
		{
			name:     "all permissions",
			perms:    TablePermissions{CanSelect: true, CanInsert: true, CanUpdate: true, CanDelete: true},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.perms.HasAnyPermission(); got != tt.expected {
				t.Errorf("HasAnyPermission() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestClient_GetTablePermissions_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetTablePermissions(ctx, "public", "test_table")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}
	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
