package cmd

import "testing"

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
