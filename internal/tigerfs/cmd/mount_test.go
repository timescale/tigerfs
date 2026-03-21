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

// Note: TestSanitizeConnectionString has been moved to db/connection_test.go
// where the SanitizeConnectionString function now lives.
