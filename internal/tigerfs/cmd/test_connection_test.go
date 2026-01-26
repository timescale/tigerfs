package cmd

import "testing"

// TestExtractPgVersion verifies PostgreSQL version string extraction.
func TestExtractPgVersion(t *testing.T) {
	tests := []struct {
		name        string
		fullVersion string
		want        string
	}{
		{
			name:        "standard PostgreSQL version",
			fullVersion: "PostgreSQL 17.2 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 11.2.0, 64-bit",
			want:        "PostgreSQL 17.2 on x86_64-pc-linux-gnu",
		},
		{
			name:        "PostgreSQL with parentheses",
			fullVersion: "PostgreSQL 16.1 (Debian 16.1-1.pgdg120+1) on x86_64-pc-linux-gnu",
			want:        "PostgreSQL 16.1 ",
		},
		{
			name:        "TimescaleDB version",
			fullVersion: "PostgreSQL 15.4 on x86_64-pc-linux-gnu, compiled by gcc, a]TimescaleDB 2.11.2",
			want:        "PostgreSQL 15.4 on x86_64-pc-linux-gnu",
		},
		{
			name:        "short version string",
			fullVersion: "PostgreSQL 14.0",
			want:        "PostgreSQL 14.0",
		},
		{
			name:        "empty string",
			fullVersion: "",
			want:        "",
		},
		{
			name:        "very long string without delimiter",
			fullVersion: "PostgreSQL 17.2 on some very long platform description that goes on and on without any commas or parentheses at all",
			want:        "PostgreSQL 17.2 on some very long platform descrip...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPgVersion(tt.fullVersion)
			if got != tt.want {
				t.Errorf("extractPgVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}
