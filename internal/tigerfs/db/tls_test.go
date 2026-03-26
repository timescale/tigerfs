package db

import (
	"strings"
	"testing"
)

func TestIsLocalhost(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"127.0.0.1", true},
		{"::1", true},
		{"", true},
		{"/var/run/postgresql", true},
		{"/tmp/.s.PGSQL.5432", true},
		{"db.example.com", false},
		{"10.0.0.1", false},
		{"192.168.1.100", false},
		{"my-database.cloud.provider.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			got := isLocalhost(tt.host)
			if got != tt.want {
				t.Errorf("isLocalhost(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}

func TestExtractHost(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		want    string
	}{
		{"URL with host", "postgres://db.example.com/mydb", "db.example.com"},
		{"URL with user and port", "postgres://user:pass@db.example.com:5432/mydb", "db.example.com"},
		{"URL with localhost", "postgres://localhost/mydb", "localhost"},
		{"URL with 127.0.0.1", "postgres://127.0.0.1:5432/mydb", "127.0.0.1"},
		{"URL with sslmode", "postgres://remote.host/db?sslmode=require", "remote.host"},
		{"key-value with host", "host=db.example.com dbname=mydb", "db.example.com"},
		{"key-value with localhost", "host=localhost dbname=mydb", "localhost"},
		{"key-value host in middle", "user=admin host=10.0.0.1 dbname=prod", "10.0.0.1"},
		{"key-value no host", "dbname=mydb user=admin", ""},
		{"empty string", "", ""},
		{"postgresql scheme", "postgresql://db.example.com/mydb", "db.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractHost(tt.connStr)
			if got != tt.want {
				t.Errorf("extractHost(%q) = %q, want %q", tt.connStr, got, tt.want)
			}
		})
	}
}

func TestEnforceSSLMode(t *testing.T) {
	tests := []struct {
		name          string
		connStr       string
		insecureNoSSL bool
		wantContains  string // substring that must be present
		wantUnchanged bool   // expect connStr returned as-is
	}{
		// Non-localhost without sslmode -> adds require
		{
			name:         "URL remote no sslmode adds require",
			connStr:      "postgres://remote.host/db",
			wantContains: "sslmode=require",
		},
		{
			name:         "key-value remote no sslmode adds require",
			connStr:      "host=remote.host dbname=db",
			wantContains: "sslmode=require",
		},

		// Non-localhost with sslmode=disable -> replaces with require
		{
			name:         "URL remote sslmode=disable replaced",
			connStr:      "postgres://remote.host/db?sslmode=disable",
			wantContains: "sslmode=require",
		},
		{
			name:         "key-value remote sslmode=disable replaced",
			connStr:      "host=remote.host sslmode=disable dbname=db",
			wantContains: "sslmode=require",
		},

		// Non-localhost with sslmode=prefer -> replaces with require
		{
			name:         "URL remote sslmode=prefer replaced",
			connStr:      "postgres://remote.host/db?sslmode=prefer",
			wantContains: "sslmode=require",
		},
		{
			name:         "key-value remote sslmode=prefer replaced",
			connStr:      "host=remote.host sslmode=prefer dbname=db",
			wantContains: "sslmode=require",
		},

		// Non-localhost with sslmode=require -> unchanged
		{
			name:          "URL remote sslmode=require unchanged",
			connStr:       "postgres://remote.host/db?sslmode=require",
			wantUnchanged: true,
		},

		// Non-localhost with verify-ca -> unchanged (stricter)
		{
			name:          "URL remote sslmode=verify-ca unchanged",
			connStr:       "postgres://remote.host/db?sslmode=verify-ca",
			wantUnchanged: true,
		},

		// Non-localhost with verify-full -> unchanged (stricter)
		{
			name:          "URL remote sslmode=verify-full unchanged",
			connStr:       "postgres://remote.host/db?sslmode=verify-full",
			wantUnchanged: true,
		},

		// Localhost without sslmode -> adds sslmode=disable
		{
			name:         "localhost no sslmode adds disable",
			connStr:      "postgres://localhost/db",
			wantContains: "sslmode=disable",
		},
		{
			name:         "key-value localhost no sslmode adds disable",
			connStr:      "host=localhost dbname=db",
			wantContains: "sslmode=disable",
		},
		{
			name:         "empty host (unix socket) no sslmode adds disable",
			connStr:      "dbname=mydb",
			wantContains: "sslmode=disable",
		},

		// Localhost with explicit sslmode -> unchanged
		{
			name:          "127.0.0.1 sslmode=disable unchanged",
			connStr:       "postgres://127.0.0.1/db?sslmode=disable",
			wantUnchanged: true,
		},
		{
			name:          "localhost sslmode=require unchanged",
			connStr:       "postgres://localhost/db?sslmode=require",
			wantUnchanged: true,
		},

		// insecureNoSSL=true -> unchanged regardless of host
		{
			name:          "insecure flag skips enforcement",
			connStr:       "postgres://remote.host/db",
			insecureNoSSL: true,
			wantUnchanged: true,
		},
		{
			name:          "insecure flag with sslmode=disable",
			connStr:       "postgres://remote.host/db?sslmode=disable",
			insecureNoSSL: true,
			wantUnchanged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := enforceSSLMode(tt.connStr, tt.insecureNoSSL)

			if tt.wantUnchanged {
				if got != tt.connStr {
					t.Errorf("enforceSSLMode(%q) = %q, want unchanged", tt.connStr, got)
				}
				return
			}

			if !strings.Contains(got, tt.wantContains) {
				t.Errorf("enforceSSLMode(%q) = %q, want to contain %q", tt.connStr, got, tt.wantContains)
			}
		})
	}
}

// TestEnforceSSLMode_NoDisableOrPreferRemains verifies that after enforcement,
// no insecure sslmode value remains in the output for remote hosts.
func TestEnforceSSLMode_NoInsecureModeRemains(t *testing.T) {
	insecureCases := []string{
		"postgres://remote.host/db?sslmode=disable",
		"postgres://remote.host/db?sslmode=prefer",
		"host=remote.host sslmode=disable dbname=db",
		"host=remote.host sslmode=prefer dbname=db",
		"postgres://remote.host/db", // no sslmode at all
	}

	for _, connStr := range insecureCases {
		got := enforceSSLMode(connStr, false)
		if strings.Contains(got, "sslmode=disable") {
			t.Errorf("enforceSSLMode(%q) still contains sslmode=disable: %s", connStr, got)
		}
		if strings.Contains(got, "sslmode=prefer") {
			t.Errorf("enforceSSLMode(%q) still contains sslmode=prefer: %s", connStr, got)
		}
		if !strings.Contains(got, "sslmode=require") {
			t.Errorf("enforceSSLMode(%q) does not contain sslmode=require: %s", connStr, got)
		}
	}
}

// TestEnforceSSLMode_CredentialPreservation verifies that password and other
// parameters are preserved when sslmode is injected or replaced.
func TestEnforceSSLMode_CredentialPreservation(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		wantHas []string // substrings that must be present after enforcement
	}{
		{
			name:    "URL password preserved when adding sslmode",
			connStr: "postgres://user:secret@remote.host:5432/db",
			wantHas: []string{"user:secret@", "remote.host:5432", "/db", "sslmode=require"},
		},
		{
			name:    "URL other params preserved when replacing sslmode",
			connStr: "postgres://remote.host/db?sslmode=disable&application_name=tigerfs",
			wantHas: []string{"application_name=tigerfs", "sslmode=require"},
		},
		{
			name:    "key-value password preserved",
			connStr: "host=remote.host password=secret dbname=db",
			wantHas: []string{"password=secret", "dbname=db", "sslmode=require"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := enforceSSLMode(tt.connStr, false)
			for _, want := range tt.wantHas {
				if !strings.Contains(got, want) {
					t.Errorf("enforceSSLMode(%q) = %q, missing %q", tt.connStr, got, want)
				}
			}
		})
	}
}
