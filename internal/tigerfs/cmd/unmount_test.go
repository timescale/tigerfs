package cmd

import "testing"

// TestIsNotMountedError verifies detection of "not mounted" error messages.
func TestIsNotMountedError(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   bool
	}{
		{
			name:   "macOS not mounted",
			output: "umount: /mnt/db: not currently mounted",
			want:   true,
		},
		{
			name:   "Linux fusermount not in mtab",
			output: "fusermount: entry for /mnt/db not found in /etc/mtab",
			want:   true,
		},
		{
			name:   "generic not mounted",
			output: "/mnt/db is not mounted",
			want:   true,
		},
		{
			name:   "invalid argument",
			output: "umount: /mnt/db: Invalid argument",
			want:   true,
		},
		{
			name:   "device busy error",
			output: "umount: /mnt/db: device is busy",
			want:   false,
		},
		{
			name:   "permission denied",
			output: "umount: /mnt/db: permission denied",
			want:   false,
		},
		{
			name:   "empty output",
			output: "",
			want:   false,
		},
		{
			name:   "random error",
			output: "some unexpected error occurred",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNotMountedError(tt.output)
			if got != tt.want {
				t.Errorf("isNotMountedError(%q) = %v, want %v", tt.output, got, tt.want)
			}
		})
	}
}
