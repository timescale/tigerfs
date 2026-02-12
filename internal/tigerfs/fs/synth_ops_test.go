package fs

import (
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

func TestSynth_NormalizeSynthFilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		format   synth.SynthFormat
		want     string
	}{
		{
			name:     "plain text without extension",
			filename: "foo",
			format:   synth.FormatPlainText,
			want:     "foo.txt",
		},
		{
			name:     "plain text already has extension",
			filename: "foo.txt",
			format:   synth.FormatPlainText,
			want:     "foo.txt",
		},
		{
			name:     "markdown without extension",
			filename: "post",
			format:   synth.FormatMarkdown,
			want:     "post.md",
		},
		{
			name:     "markdown already has extension",
			filename: "post.md",
			format:   synth.FormatMarkdown,
			want:     "post.md",
		},
		{
			name:     "native format unchanged (no extension)",
			filename: "data",
			format:   synth.FormatNative,
			want:     "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &synth.ViewInfo{Format: tt.format}
			got := normalizeSynthFilename(tt.filename, info)
			if got != tt.want {
				t.Errorf("normalizeSynthFilename(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}

func TestSynth_ExtractModTime(t *testing.T) {
	fixedTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mountTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		columns []string
		values  []interface{}
		info    *synth.ViewInfo
		want    time.Time
	}{
		{
			name:    "modified_at present",
			columns: []string{"id", "filename", "body", "modified_at"},
			values:  []interface{}{1, "test", "body", fixedTime},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "modified_at", CreatedAt: "created_at"},
				CachedMountTime: mountTime,
			},
			want: fixedTime,
		},
		{
			name:    "only created_at present",
			columns: []string{"id", "filename", "body", "created_at"},
			values:  []interface{}{1, "test", "body", fixedTime},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "", CreatedAt: "created_at"},
				CachedMountTime: mountTime,
			},
			want: fixedTime,
		},
		{
			name:    "no timestamp columns in roles",
			columns: []string{"id", "filename", "body"},
			values:  []interface{}{1, "test", "body"},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "", CreatedAt: ""},
				CachedMountTime: mountTime,
			},
			want: mountTime,
		},
		{
			name:    "nil value in modified_at falls through to created_at",
			columns: []string{"id", "filename", "body", "modified_at", "created_at"},
			values:  []interface{}{1, "test", "body", nil, fixedTime},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "modified_at", CreatedAt: "created_at"},
				CachedMountTime: mountTime,
			},
			want: fixedTime,
		},
		{
			name:    "nil value in both timestamp columns falls to mount time",
			columns: []string{"id", "filename", "body", "modified_at", "created_at"},
			values:  []interface{}{1, "test", "body", nil, nil},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "modified_at", CreatedAt: "created_at"},
				CachedMountTime: mountTime,
			},
			want: mountTime,
		},
		{
			name:    "non-time value in timestamp column falls through",
			columns: []string{"id", "filename", "body", "modified_at"},
			values:  []interface{}{1, "test", "body", "not-a-time"},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "modified_at", CreatedAt: ""},
				CachedMountTime: mountTime,
			},
			want: mountTime,
		},
		{
			name:    "modified_at preferred over created_at",
			columns: []string{"id", "modified_at", "created_at"},
			values: []interface{}{
				1,
				time.Date(2025, 6, 20, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 6, 10, 0, 0, 0, 0, time.UTC),
			},
			info: &synth.ViewInfo{
				Roles:           &synth.ColumnRoles{ModifiedAt: "modified_at", CreatedAt: "created_at"},
				CachedMountTime: mountTime,
			},
			want: time.Date(2025, 6, 20, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractModTime(tt.columns, tt.values, tt.info)
			if !got.Equal(tt.want) {
				t.Errorf("extractModTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
