package cmd

import (
	"testing"
	"time"
)

// TestFormatDuration verifies human-readable duration formatting.
func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "zero seconds",
			duration: 0,
			want:     "0s",
		},
		{
			name:     "seconds only",
			duration: 45 * time.Second,
			want:     "45s",
		},
		{
			name:     "just under a minute",
			duration: 59 * time.Second,
			want:     "59s",
		},
		{
			name:     "exactly one minute",
			duration: 1 * time.Minute,
			want:     "1m",
		},
		{
			name:     "minutes and seconds",
			duration: 5*time.Minute + 30*time.Second,
			want:     "5m30s",
		},
		{
			name:     "minutes only (no seconds)",
			duration: 15 * time.Minute,
			want:     "15m",
		},
		{
			name:     "just under an hour",
			duration: 59*time.Minute + 59*time.Second,
			want:     "59m59s",
		},
		{
			name:     "exactly one hour",
			duration: 1 * time.Hour,
			want:     "1h",
		},
		{
			name:     "hours and minutes",
			duration: 2*time.Hour + 15*time.Minute,
			want:     "2h15m",
		},
		{
			name:     "hours only (no minutes)",
			duration: 5 * time.Hour,
			want:     "5h",
		},
		{
			name:     "just under a day",
			duration: 23*time.Hour + 59*time.Minute,
			want:     "23h59m",
		},
		{
			name:     "exactly one day",
			duration: 24 * time.Hour,
			want:     "1d",
		},
		{
			name:     "days and hours",
			duration: 3*24*time.Hour + 4*time.Hour,
			want:     "3d4h",
		},
		{
			name:     "days only (no hours)",
			duration: 7 * 24 * time.Hour,
			want:     "7d",
		},
		{
			name:     "many days",
			duration: 100 * 24 * time.Hour,
			want:     "100d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.duration)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.duration, got, tt.want)
			}
		})
	}
}

// TestTruncateString verifies string truncation with ellipsis.
func TestTruncateString(t *testing.T) {
	tests := []struct {
		name   string
		s      string
		maxLen int
		want   string
	}{
		{
			name:   "empty string",
			s:      "",
			maxLen: 10,
			want:   "",
		},
		{
			name:   "string shorter than max",
			s:      "hello",
			maxLen: 10,
			want:   "hello",
		},
		{
			name:   "string exactly at max",
			s:      "hello",
			maxLen: 5,
			want:   "hello",
		},
		{
			name:   "string longer than max",
			s:      "hello world",
			maxLen: 8,
			want:   "hello...",
		},
		{
			name:   "truncate to very short length",
			s:      "hello world",
			maxLen: 4,
			want:   "h...",
		},
		{
			name:   "maxLen equals 3 (edge case - just ellipsis)",
			s:      "hello",
			maxLen: 3,
			want:   "hel",
		},
		{
			name:   "maxLen less than 3",
			s:      "hello",
			maxLen: 2,
			want:   "he",
		},
		{
			name:   "long connection string truncation",
			s:      "postgres://user@very-long-hostname.example.com:5432/database",
			maxLen: 40,
			want:   "postgres://user@very-long-hostname.ex...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateString(tt.s, tt.maxLen)
			if got != tt.want {
				t.Errorf("truncateString(%q, %d) = %q, want %q", tt.s, tt.maxLen, got, tt.want)
			}
		})
	}
}

// TestGetMountStatus verifies mount status detection based on PID.
func TestGetMountStatus(t *testing.T) {
	tests := []struct {
		name string
		pid  int
		want string
	}{
		{
			name: "invalid PID returns stale",
			pid:  -1,
			want: "stale",
		},
		{
			name: "zero PID returns stale",
			pid:  0,
			want: "stale",
		},
		{
			name: "non-existent PID returns stale",
			pid:  999999,
			want: "stale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getMountStatus(tt.pid)
			if got != tt.want {
				t.Errorf("getMountStatus(%d) = %q, want %q", tt.pid, got, tt.want)
			}
		})
	}
}
