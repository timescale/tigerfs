package cmd

import (
	"path/filepath"
	"testing"
)

// TestResolveMountArgs verifies connection string and mountpoint resolution.
func TestResolveMountArgs(t *testing.T) {
	baseDir := "/tmp"

	tests := []struct {
		name      string
		args      []string
		wantConn  string
		wantMount string
		wantErr   bool
	}{
		{
			name:      "two args explicit",
			args:      []string{"ghost:mydb", "/mnt/db"},
			wantConn:  "ghost:mydb",
			wantMount: "/mnt/db",
		},
		{
			name:      "one arg ghost prefix",
			args:      []string{"ghost:mydb"},
			wantConn:  "ghost:mydb",
			wantMount: filepath.Join(baseDir, "mydb"),
		},
		{
			name:      "one arg tiger prefix",
			args:      []string{"tiger:abc123"},
			wantConn:  "tiger:abc123",
			wantMount: filepath.Join(baseDir, "abc123"),
		},
		{
			name:      "one arg absolute path",
			args:      []string{"/mnt/db"},
			wantConn:  "",
			wantMount: "/mnt/db",
		},
		{
			name:      "one arg relative path",
			args:      []string{"./db"},
			wantConn:  "",
			wantMount: "./db",
		},
		{
			name:      "one arg bare name treated as mountpoint",
			args:      []string{"mydb"},
			wantConn:  "",
			wantMount: "mydb",
		},
		{
			name:      "one arg postgres URL treated as mountpoint",
			args:      []string{"postgres://host/db"},
			wantConn:  "",
			wantMount: "postgres://host/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, mount, err := resolveMountArgs(tt.args, baseDir)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveMountArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if conn != tt.wantConn {
				t.Errorf("connStr = %q, want %q", conn, tt.wantConn)
			}
			if mount != tt.wantMount {
				t.Errorf("mountpoint = %q, want %q", mount, tt.wantMount)
			}
		})
	}
}

// TestSanitizeConnectionString verifies connection string sanitization.
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
			name:    "simple connection string",
			connStr: "postgres://localhost/mydb",
			want:    "postgres://localhost/mydb",
		},
		{
			name:    "connection string with user",
			connStr: "postgres://user@localhost/mydb",
			want:    "postgres://user@localhost/mydb",
		},
		{
			name:    "connection string with password stripped",
			connStr: "postgres://user:secret@localhost/mydb",
			want:    "postgres://user@localhost/mydb",
		},
		{
			name:    "connection string with encoded password stripped",
			connStr: "postgres://user:p%40ss@localhost:5432/mydb?sslmode=require",
			want:    "postgres://user@localhost:5432/mydb?sslmode=require",
		},
		{
			name:    "non-URL string returned as-is",
			connStr: "host=localhost dbname=mydb",
			want:    "host=localhost dbname=mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeConnectionString(tt.connStr)
			if got != tt.want {
				t.Errorf("sanitizeConnectionString(%q) = %q, want %q", tt.connStr, got, tt.want)
			}
		})
	}
}
