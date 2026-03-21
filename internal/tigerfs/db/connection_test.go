package db

import (
	"context"
	"strings"
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

// TestSanitizeConnectionString verifies password removal from connection strings.
func TestSanitizeConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		want    string
	}{
		{
			name:    "empty string",
			connStr: "",
			want:    "(from environment)",
		},
		{
			name:    "simple URL",
			connStr: "postgres://localhost/mydb",
			want:    "postgres://localhost/mydb",
		},
		{
			name:    "URL with user",
			connStr: "postgres://user@localhost/mydb",
			want:    "postgres://user@localhost/mydb",
		},
		{
			name:    "URL with password stripped",
			connStr: "postgres://user:secret@localhost/mydb",
			want:    "postgres://user@localhost/mydb",
		},
		{
			name:    "URL with encoded password stripped",
			connStr: "postgres://user:p%40ss@localhost:5432/mydb?sslmode=require",
			want:    "postgres://user@localhost:5432/mydb?sslmode=require",
		},
		{
			name:    "key-value no password",
			connStr: "host=localhost dbname=mydb",
			want:    "host=localhost dbname=mydb",
		},
		{
			name:    "key-value with password",
			connStr: "host=localhost password=secret dbname=mydb",
			want:    "host=localhost password=*** dbname=mydb",
		},
		{
			name:    "key-value with password at end",
			connStr: "host=localhost dbname=mydb password=secret",
			want:    "host=localhost dbname=mydb password=***",
		},
		{
			name:    "key-value with password only",
			connStr: "password=secret",
			want:    "password=***",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeConnectionString(tt.connStr)
			if got != tt.want {
				t.Errorf("SanitizeConnectionString(%q) = %q, want %q", tt.connStr, got, tt.want)
			}
		})
	}
}

// TestSanitizeConnectionString_CredentialSafety verifies that passwords are
// never present in sanitized output, regardless of connection string format.
func TestSanitizeConnectionString_CredentialSafety(t *testing.T) {
	dangerous := []struct {
		name    string
		connStr string
	}{
		{"URL with password", "postgres://admin:hunter2@db.example.com/prod"},
		{"URL with encoded password", "postgres://admin:h%75nter2@db.example.com/prod"},
		{"key-value with password", "host=db.example.com password=hunter2 dbname=prod"},
		{"key-value password at end", "host=db.example.com dbname=prod password=hunter2"},
	}

	for _, tt := range dangerous {
		t.Run(tt.name, func(t *testing.T) {
			sanitized := SanitizeConnectionString(tt.connStr)
			if strings.Contains(sanitized, "hunter2") {
				t.Errorf("SanitizeConnectionString(%q) still contains password: %s", tt.connStr, sanitized)
			}
		})
	}
}

// Note: Tiger Cloud integration tests are skipped as they require
// a valid Tiger Cloud service ID and authentication.
// To test Tiger Cloud integration manually:
//   1. Run: tiger auth login
//   2. Set: export TIGER_SERVICE_ID=<your-service-id>
//   3. Run: go test -run TestResolveConnectionString_TigerCloud -v
