package db

import (
	"context"
	"fmt"
	"net/url"
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
	defer func() { _ = client.Close() }()

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
			_ = client.Close()
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
			_ = client.Close()
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
	defer func() { _ = client.Close() }()

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

	// Pool should be closed, verify by checking that operations fail. (Asserting on
	// pool.Stat().TotalConns() races with pgxpool's background createIdleResources
	// goroutine when MinConns > 0; Ping on a closed pool is deterministic.)
	if err := client2.pool.Ping(context.Background()); err == nil {
		t.Error("Expected Ping on closed pool to fail")
	}
}

// getTestConnectionString returns a test PostgreSQL connection string
// from environment variables (PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD).
// Returns empty string if no database is available.
//
// We build a fully-populated postgres://user:pass@host:port/db URL rather
// than relying on pgx's env-var fallback because NewClient's
// injectPasswordIntoConnStr requires user@host in the URL to inject the
// password; a bare URL errors out with "connection string has no
// user@host format" whenever PGPASSWORD is set in the environment.
func getTestConnectionString(t *testing.T) string {
	t.Helper()

	// Check for TEST_DATABASE_URL first (CI/CD environments may set it explicitly).
	if connStr := getEnv("TEST_DATABASE_URL", ""); connStr != "" {
		return connStr
	}

	// Only return a connection string if PGHOST is explicitly set. This allows
	// developers with a local PostgreSQL to run integration tests while
	// skipping them in environments without a database.
	host := getEnv("PGHOST", "")
	if host == "" {
		return ""
	}

	port := getEnv("PGPORT", "5432")
	user := getEnv("PGUSER", "postgres")
	password := getEnv("PGPASSWORD", "")
	dbname := getEnv("PGDATABASE", "postgres")

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host, port, dbname,
	)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// TestGetTestConnectionString_BuildsPopulatedURL locks in the contract that
// the helper returns a fully-populated postgres://user:pass@host:port/db URL
// when PG* env vars are set. Regressing to a bare URL (no user@host) would
// reintroduce the injectPasswordIntoConnStr failure that previously blocked
// most db unit tests when PGPASSWORD was set.
func TestGetTestConnectionString_BuildsPopulatedURL(t *testing.T) {
	// Clear TEST_DATABASE_URL so it doesn't preempt the PG* env path.
	t.Setenv("TEST_DATABASE_URL", "")
	t.Setenv("PGHOST", "10.0.0.1")
	t.Setenv("PGPORT", "5433")
	t.Setenv("PGUSER", "testuser")
	t.Setenv("PGPASSWORD", "testpass")
	t.Setenv("PGDATABASE", "testdb")

	got := getTestConnectionString(t)
	want := "postgres://testuser:testpass@10.0.0.1:5433/testdb?sslmode=disable"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestGetTestConnectionString_SkipsWhenNoPGHost ensures the helper returns ""
// (which causes tests to skip) when no PG* configuration is present.
func TestGetTestConnectionString_SkipsWhenNoPGHost(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "")
	t.Setenv("PGHOST", "")

	if got := getTestConnectionString(t); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

// TestGetTestConnectionString_PrefersTestDatabaseURL ensures that
// TEST_DATABASE_URL takes precedence over PG* env vars, preserving the
// existing CI/CD behavior.
func TestGetTestConnectionString_PrefersTestDatabaseURL(t *testing.T) {
	override := "postgres://ci-user:ci-pass@ci-host/ci-db?sslmode=require"
	t.Setenv("TEST_DATABASE_URL", override)
	t.Setenv("PGHOST", "should-be-ignored")

	if got := getTestConnectionString(t); got != override {
		t.Errorf("got %q, want %q", got, override)
	}
}

// TestGetTestConnectionString_EscapesSpecialChars ensures user/password
// containing URL-special characters are escaped, not interpolated raw into
// the URL.
func TestGetTestConnectionString_EscapesSpecialChars(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "")
	t.Setenv("PGHOST", "127.0.0.1")
	t.Setenv("PGUSER", "user@with/special:chars")
	t.Setenv("PGPASSWORD", "p@ss/w:rd?")
	t.Setenv("PGDATABASE", "db")

	got := getTestConnectionString(t)
	// The escaped form must roundtrip through url.Parse without losing the
	// user/password identity.
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("got URL %q is not parseable: %v", got, err)
	}
	if parsed.User.Username() != "user@with/special:chars" {
		t.Errorf("user roundtrip failed: got %q", parsed.User.Username())
	}
	pw, _ := parsed.User.Password()
	if pw != "p@ss/w:rd?" {
		t.Errorf("password roundtrip failed: got %q", pw)
	}
}
