package cmd

import (
	"testing"
)

func TestIsPath(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{".", true},
		{"..", true},
		{"./foo", true},
		{"../foo", true},
		{".hidden", true},
		{"/", true},
		{"/tmp/foo", true},
		{"/absolute/path", true},
		{"tiger:abc123", false},
		{"ghost:xyz", false},
		{"my-db", false},
		{"postgres://host/db", false},
		{"foo/bar", false}, // names with / are NOT paths per ADR-013
		{"", false},
		{"~", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isPath(tt.input)
			if got != tt.want {
				t.Errorf("isPath(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveForkDest(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		nameFlag       string
		baseDir        string
		wantName       string
		wantMountpoint string // exact match; empty = check emptiness only
	}{
		{
			name:     "no DEST, no flag",
			args:     []string{"/tmp/prod"},
			nameFlag: "",
			baseDir:  "/tmp",
			wantName: "",
		},
		{
			name:     "no DEST, with name flag",
			args:     []string{"/tmp/prod"},
			nameFlag: "my-fork",
			baseDir:  "/tmp",
			wantName: "my-fork",
		},
		{
			name:           "DEST is bare name",
			args:           []string{"/tmp/prod", "my-fork"},
			nameFlag:       "",
			baseDir:        "/tmp",
			wantName:       "my-fork",
			wantMountpoint: "/tmp/my-fork",
		},
		{
			name:           "DEST is bare name, flag overrides",
			args:           []string{"/tmp/prod", "my-fork"},
			nameFlag:       "override",
			baseDir:        "/tmp",
			wantName:       "override",
			wantMountpoint: "/tmp/my-fork",
		},
		{
			name:           "DEST is absolute path",
			args:           []string{"/tmp/prod", "/mnt/data"},
			nameFlag:       "",
			baseDir:        "/tmp",
			wantName:       "data", // basename
			wantMountpoint: "/mnt/data",
		},
		{
			name:           "DEST is absolute path, flag overrides name",
			args:           []string{"/tmp/prod", "/mnt/data"},
			nameFlag:       "custom",
			baseDir:        "/tmp",
			wantName:       "custom",
			wantMountpoint: "/mnt/data",
		},
		{
			name:           "DEST is bare name, custom base dir",
			args:           []string{"/tmp/prod", "my-fork"},
			nameFlag:       "",
			baseDir:        "/mnt/data",
			wantName:       "my-fork",
			wantMountpoint: "/mnt/data/my-fork",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, mountpoint := resolveForkDest(tt.args, tt.nameFlag, tt.baseDir)
			if name != tt.wantName {
				t.Errorf("resolveForkDest() name = %q, want %q", name, tt.wantName)
			}
			if tt.wantMountpoint != "" {
				if mountpoint != tt.wantMountpoint {
					t.Errorf("resolveForkDest() mountpoint = %q, want %q", mountpoint, tt.wantMountpoint)
				}
			}
			// For no-DEST cases, mountpoint should be empty.
			if len(tt.args) < 2 && mountpoint != "" {
				t.Errorf("resolveForkDest() with no DEST should return empty mountpoint, got %q", mountpoint)
			}
		})
	}
}
