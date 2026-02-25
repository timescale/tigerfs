package backend

import (
	"errors"
	"testing"
)

func TestResolve(t *testing.T) {
	tests := []struct {
		name    string
		ref     string
		wantCLI string // expected CLIName(), or "" for nil backend
		wantID  string
		wantErr bool
	}{
		{
			name:    "tiger prefix with ID",
			ref:     "tiger:abc123",
			wantCLI: "tiger",
			wantID:  "abc123",
		},
		{
			name:    "ghost prefix with ID",
			ref:     "ghost:xyz789",
			wantCLI: "ghost",
			wantID:  "xyz789",
		},
		{
			name:    "tiger prefix empty ID (auto-generate)",
			ref:     "tiger:",
			wantCLI: "tiger",
			wantID:  "",
		},
		{
			name:    "ghost prefix empty ID (auto-generate)",
			ref:     "ghost:",
			wantCLI: "ghost",
			wantID:  "",
		},
		{
			name:   "postgres connection string",
			ref:    "postgres://user@host/db",
			wantID: "postgres://user@host/db",
		},
		{
			name:   "bare name (no prefix)",
			ref:    "my-database",
			wantID: "my-database",
		},
		{
			name:   "bare name with slash",
			ref:    "project/my-db",
			wantID: "project/my-db",
		},
		{
			name:   "empty string",
			ref:    "",
			wantID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, id, err := Resolve(tt.ref)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve(%q) error = %v, wantErr %v", tt.ref, err, tt.wantErr)
			}
			if id != tt.wantID {
				t.Errorf("Resolve(%q) id = %q, want %q", tt.ref, id, tt.wantID)
			}
			if tt.wantCLI == "" {
				if b != nil {
					t.Errorf("Resolve(%q) backend = %v, want nil", tt.ref, b)
				}
			} else {
				if b == nil {
					t.Fatalf("Resolve(%q) backend = nil, want %q", tt.ref, tt.wantCLI)
				}
				if b.CLIName() != tt.wantCLI {
					t.Errorf("Resolve(%q) backend.CLIName() = %q, want %q", tt.ref, b.CLIName(), tt.wantCLI)
				}
			}
		})
	}
}

func TestForName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantCLI string
		wantErr error
	}{
		{name: "tiger", input: "tiger", wantCLI: "tiger"},
		{name: "ghost", input: "ghost", wantCLI: "ghost"},
		{name: "empty", input: "", wantErr: ErrNoBackend},
		{name: "unknown", input: "neon", wantErr: nil}, // non-nil error but not ErrNoBackend
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := ForName(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("ForName(%q) error = %v, want %v", tt.input, err, tt.wantErr)
				}
				return
			}
			if tt.wantCLI != "" {
				if err != nil {
					t.Fatalf("ForName(%q) unexpected error: %v", tt.input, err)
				}
				if b.CLIName() != tt.wantCLI {
					t.Errorf("ForName(%q) CLIName() = %q, want %q", tt.input, b.CLIName(), tt.wantCLI)
				}
			} else if tt.input != "" && err == nil {
				t.Errorf("ForName(%q) expected error for unknown backend", tt.input)
			}
		})
	}
}
