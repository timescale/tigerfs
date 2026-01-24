package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestNewClient_ValidConnection(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	if client.pool == nil {
		t.Error("Expected non-nil connection pool")
	}

	if client.cfg != cfg {
		t.Error("Expected config to be stored in client")
	}
}

func TestNewClient_InvalidConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	// Invalid connection string
	client, err := NewClient(ctx, cfg, "postgres://invalid:invalid@nonexistent:9999/invalid")
	if err == nil {
		if client != nil {
			client.Close()
		}
		t.Fatal("Expected error for invalid connection, got nil")
	}

	if client != nil {
		t.Error("Expected nil client on connection failure")
	}
}

func TestNewClient_InvalidConnectionString(t *testing.T) {
	ctx := context.Background()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	// Malformed connection string
	client, err := NewClient(ctx, cfg, "not-a-valid-connection-string")
	if err == nil {
		if client != nil {
			client.Close()
		}
		t.Fatal("Expected error for malformed connection string, got nil")
	}

	if client != nil {
		t.Error("Expected nil client on parse failure")
	}
}

func TestNewClient_PoolConfiguration(t *testing.T) {
	// Skip if no PostgreSQL available
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    10,
		PoolMaxIdle: 3,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	// Verify pool stats
	stats := client.pool.Stat()
	if stats.MaxConns() != 10 {
		t.Errorf("Expected MaxConns=10, got %d", stats.MaxConns())
	}
}

func TestClient_Close(t *testing.T) {
	// Test closing nil pool (should not panic)
	client := &Client{
		cfg: &config.Config{},
	}
	err := client.Close()
	if err != nil {
		t.Errorf("Close() with nil pool failed: %v", err)
	}

	// Test closing with real pool
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client2, err2 := NewClient(ctx, cfg, connStr)
	if err2 != nil {
		t.Fatalf("NewClient() failed: %v", err2)
	}

	closeErr := client2.Close()
	if closeErr != nil {
		t.Errorf("Close() failed: %v", closeErr)
	}

	// Pool should be closed, verify by checking if operations fail
	stats := client2.pool.Stat()
	if stats.TotalConns() != 0 {
		t.Error("Expected all connections to be closed")
	}
}

// getTestConnectionString returns a test PostgreSQL connection string
// from environment variables (PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD)
// Returns empty string if no database is available.
func getTestConnectionString(t *testing.T) string {
	t.Helper()

	// Only return a connection string if PGHOST is explicitly set
	// This allows CI/CD and developers with PostgreSQL to run integration tests
	// while skipping them in environments without a database

	// Check for explicit test database configuration
	if host := getEnv("PGHOST", ""); host != "" {
		// PGHOST is set, assume database is available
		// pgx will use PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD from environment
		return "postgres://localhost/postgres?sslmode=disable"
	}

	// Also check for TEST_DATABASE_URL for CI/CD environments
	if connStr := getEnv("TEST_DATABASE_URL", ""); connStr != "" {
		return connStr
	}

	// No database configuration found, skip tests
	return ""
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
