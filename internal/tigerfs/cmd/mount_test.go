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
		// TODO: Add tests for password redaction when implemented
		// {
		// 	name:    "connection string with password",
		// 	connStr: "postgres://user:secret@localhost/mydb",
		// 	want:    "postgres://user:***@localhost/mydb",
		// },
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
