package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestResolveCreateBackend(t *testing.T) {
	tests := []struct {
		name           string
		nameArg        string
		defaultBackend string
		wantCLI        string // expected CLIName(), or "" for error
		wantName       string
		wantErr        bool
	}{
		{
			name:     "tiger prefix with name",
			nameArg:  "tiger:my-db",
			wantCLI:  "tiger",
			wantName: "my-db",
		},
		{
			name:     "ghost prefix with name",
			nameArg:  "ghost:my-db",
			wantCLI:  "ghost",
			wantName: "my-db",
		},
		{
			name:     "tiger prefix auto-name",
			nameArg:  "tiger:",
			wantCLI:  "tiger",
			wantName: "",
		},
		{
			name:           "bare name with default backend",
			nameArg:        "my-db",
			defaultBackend: "tiger",
			wantCLI:        "tiger",
			wantName:       "my-db",
		},
		{
			name:    "bare name without default backend",
			nameArg: "my-db",
			wantErr: true,
		},
		{
			name:           "empty arg with default backend",
			nameArg:        "",
			defaultBackend: "ghost",
			wantCLI:        "ghost",
			wantName:       "",
		},
		{
			name:    "empty arg without default backend",
			nameArg: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{DefaultBackend: tt.defaultBackend}
			b, name, err := resolveCreateBackend(tt.nameArg, cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveCreateBackend(%q) expected error, got nil", tt.nameArg)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveCreateBackend(%q) unexpected error: %v", tt.nameArg, err)
			}
			if b.CLIName() != tt.wantCLI {
				t.Errorf("resolveCreateBackend(%q) backend = %q, want %q", tt.nameArg, b.CLIName(), tt.wantCLI)
			}
			if name != tt.wantName {
				t.Errorf("resolveCreateBackend(%q) name = %q, want %q", tt.nameArg, name, tt.wantName)
			}
		})
	}
}

func TestResolveCreateMountpoint(t *testing.T) {
	tmpDir := os.TempDir()

	tests := []struct {
		name          string
		mountpointArg string
		serviceName   string
		wantPath      string // exact match, or "" to check prefix
		wantPrefix    string // if wantPath is empty, check this prefix
		wantErr       bool
	}{
		{
			name:          "explicit absolute path",
			mountpointArg: "/mnt/data",
			serviceName:   "ignored",
			wantPath:      "/mnt/data",
		},
		{
			name:          "no mountpoint, name provided",
			mountpointArg: "",
			serviceName:   "my-service",
			wantPath:      filepath.Join(tmpDir, "my-service"),
		},
		{
			name:          "no mountpoint, no name",
			mountpointArg: "",
			serviceName:   "",
			wantErr:       true,
		},
		{
			name:          "relative path",
			mountpointArg: "./mydata",
			serviceName:   "ignored",
			wantPrefix:    "/", // resolved to absolute
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveCreateMountpoint(tt.mountpointArg, tt.serviceName)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantPath != "" && got != tt.wantPath {
				t.Errorf("got %q, want %q", got, tt.wantPath)
			}
			if tt.wantPrefix != "" && !filepath.IsAbs(got) {
				t.Errorf("got %q, want absolute path", got)
			}
		})
	}
}
