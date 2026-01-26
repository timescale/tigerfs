package db

import (
	"context"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestResolveConnectionString_ExplicitConnStr tests that explicit connection
// strings take precedence over all other sources.
func TestResolveConnectionString_ExplicitConnStr(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		Host:                "should-not-use.example.com",
		TigerCloudServiceID: "should-not-use",
	}

	explicit := "postgres://user@explicit-host:5432/mydb"
	got, err := ResolveConnectionString(ctx, cfg, explicit)
	if err != nil {
		t.Fatalf("ResolveConnectionString() error = %v", err)
	}

	if got != explicit {
		t.Errorf("ResolveConnectionString() = %q, want %q", got, explicit)
	}
}

// TestResolveConnectionString_FromPGConfig tests building connection string
// from PostgreSQL config when no explicit string or Tiger Cloud is configured.
func TestResolveConnectionString_FromPGConfig(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		Host:     "localhost",
		Port:     5432,
		User:     "testuser",
		Database: "testdb",
	}

	got, err := ResolveConnectionString(ctx, cfg, "")
	if err != nil {
		t.Fatalf("ResolveConnectionString() error = %v", err)
	}

	want := "postgres://testuser@localhost:5432/testdb"
	if got != want {
		t.Errorf("ResolveConnectionString() = %q, want %q", got, want)
	}
}

// TestResolveConnectionString_NoConfig tests that an error is returned
// when no connection configuration is available.
func TestResolveConnectionString_NoConfig(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		// No host, no Tiger Cloud service ID
	}

	_, err := ResolveConnectionString(ctx, cfg, "")
	if err == nil {
		t.Error("ResolveConnectionString() expected error for empty config, got nil")
	}
}

// Note: Tiger Cloud integration tests are skipped as they require
// a valid Tiger Cloud service ID and authentication.
// To test Tiger Cloud integration manually:
//   1. Run: tiger auth login
//   2. Set: export TIGER_SERVICE_ID=<your-service-id>
//   3. Run: go test -run TestResolveConnectionString_TigerCloud -v
