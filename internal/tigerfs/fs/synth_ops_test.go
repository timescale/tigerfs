package fs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

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

// newSynthHierarchicalMockDB creates a mock DB configured with a hierarchical synth markdown view.
// The "memory" view has columns [id, filename, filetype, title, author, body] with:
//   - "projects" (directory)
//   - "projects/web" (directory)
//   - "projects/web/todo" (file: "Todo List")
//   - "projects/web/notes" (file: "Notes")
//   - "readme" (file: "Readme")
func newSynthHierarchicalMockDB() *mockDBClient {
	return &mockDBClient{
		tables: map[string][]string{
			"public": {"_memory"},
		},
		views: map[string][]string{
			"public": {"memory"},
		},
		viewComments: map[string]map[string]string{
			"public": {"memory": "tigerfs:md"},
		},
		columns: map[string][]mockColumn{
			"public.memory": {
				{name: "id", dataType: "uuid"},
				{name: "filename", dataType: "text"},
				{name: "filetype", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "author", dataType: "text"},
				{name: "body", dataType: "text"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public._memory": {column: "id"},
			"public.memory":  {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.memory": {
				columns: []string{"id", "filename", "filetype", "title", "author", "body"},
				rows: [][]interface{}{
					{"uuid-1", "projects", "directory", nil, nil, nil},
					{"uuid-2", "projects/web", "directory", nil, nil, nil},
					{"uuid-3", "projects/web/todo.md", "file", "Todo List", "alice", "# Todo\n\nFix bugs.\n"},
					{"uuid-4", "projects/web/notes.md", "file", "Notes", "bob", "# Notes\n\nMeeting notes.\n"},
					{"uuid-5", "readme.md", "file", "Readme", "admin", "# Readme\n\nWelcome.\n"},
				},
			},
		},
	}
}

// TestSynth_ResolveSynthHierarchy verifies that PathColumn is converted to PathRow for hierarchical views.
func TestSynth_ResolveSynthHierarchy(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthHierarchicalMockDB()

	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	tests := []struct {
		name     string
		path     string
		wantType PathType
		wantPK   string
	}{
		{
			name:     "two-segment path converts to PathRow",
			path:     "/memory/projects/web",
			wantType: PathRow,
			wantPK:   "projects/web",
		},
		{
			name:     "three-segment path converts to PathRow",
			path:     "/memory/projects/web/todo.md",
			wantType: PathRow,
			wantPK:   "projects/web/todo.md",
		},
		{
			name:     "single segment stays PathRow",
			path:     "/memory/readme.md",
			wantType: PathRow,
			wantPK:   "readme.md",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, fsErr := ParsePath(tt.path)
			require.Nil(t, fsErr)

			// Resolve schema so getSynthViewInfo can find the view
			// (ParsePath leaves schema empty for root-level paths)
			if parsed.Context != nil && parsed.Context.Schema == "" {
				parsed.Context.Schema = "public"
			}

			ops.resolveSynthHierarchy(ctx, parsed)

			assert.Equal(t, tt.wantType, parsed.Type, "Type after resolve")
			assert.Equal(t, tt.wantPK, parsed.PrimaryKey, "PrimaryKey after resolve")
		})
	}
}

// TestSynth_ReadDirHierarchical_Root verifies that ReadDir on a hierarchical view root
// shows only top-level files and directories.
func TestSynth_ReadDirHierarchical_Root(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthHierarchicalMockDB()

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/memory")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}

	// Root should show: "projects" (directory) and "readme.md" (file)
	assert.Contains(t, names, "projects", "root should contain projects directory")
	assert.Contains(t, names, "readme.md", "root should contain readme.md file")
	// Should NOT show nested items at root level
	assert.NotContains(t, names, "projects/web")
	assert.NotContains(t, names, "todo.md")
	assert.NotContains(t, names, "notes.md")

	// Verify entry types
	for _, e := range entries {
		if e.Name == "projects" {
			assert.True(t, e.IsDir, "projects should be a directory")
		}
		if e.Name == "readme.md" {
			assert.False(t, e.IsDir, "readme.md should be a file")
		}
	}
}

// TestSynth_ReadDirHierarchical_Subdir verifies that ReadDir on a subdirectory
// shows only immediate children.
func TestSynth_ReadDirHierarchical_Subdir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthHierarchicalMockDB()

	ops := NewOperations(cfg, mockDB)

	// ReadDir on /memory/projects should show "web" directory
	entries, err := ops.ReadDir(context.Background(), "/memory/projects")
	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "web", "projects/ should contain web directory")
	assert.Len(t, entries, 1, "projects/ should have exactly 1 entry")

	// ReadDir on /memory/projects/web should show todo.md and notes.md
	entries, err = ops.ReadDir(context.Background(), "/memory/projects/web")
	require.Nil(t, err)
	require.NotNil(t, entries)

	names = make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "todo.md", "web/ should contain todo.md")
	assert.Contains(t, names, "notes.md", "web/ should contain notes.md")
	assert.Len(t, entries, 2, "web/ should have exactly 2 entries")
}

// TestSynth_StatDirectory verifies that Stat on a directory returns IsDir=true.
func TestSynth_StatDirectory(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthHierarchicalMockDB()

	ops := NewOperations(cfg, mockDB)

	// Stat a directory
	entry, err := ops.Stat(context.Background(), "/memory/projects")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir, "projects should be a directory")

	// Stat a nested directory
	entry, err = ops.Stat(context.Background(), "/memory/projects/web")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.True(t, entry.IsDir, "projects/web should be a directory")

	// Stat a file
	entry, err = ops.Stat(context.Background(), "/memory/projects/web/todo.md")
	require.Nil(t, err)
	require.NotNil(t, entry)
	assert.False(t, entry.IsDir, "todo.md should be a file")
	assert.True(t, entry.Size > 0, "file should have non-zero size")
}

// TestSynth_ReadFileHierarchical verifies reading a nested file in a hierarchical view.
func TestSynth_ReadFileHierarchical(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	mockDB := newSynthHierarchicalMockDB()

	ops := NewOperations(cfg, mockDB)
	content, err := ops.ReadFile(context.Background(), "/memory/projects/web/todo.md")

	require.Nil(t, err)
	require.NotNil(t, content)

	text := string(content.Data)
	assert.Contains(t, text, "title: Todo List")
	assert.Contains(t, text, "author: alice")
	assert.Contains(t, text, "# Todo")
}

// TestSynth_NonHierarchicalViewUnchanged verifies that synth views without filetype column
// still work as before (no hierarchy, no directory rows).
func TestSynth_NonHierarchicalViewUnchanged(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	// Use the standard (non-hierarchical) mock
	mockDB := newSynthMockDB()

	ops := NewOperations(cfg, mockDB)
	entries, err := ops.ReadDir(context.Background(), "/posts")

	require.Nil(t, err)
	require.NotNil(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	assert.Contains(t, names, "hello-world.md")
	assert.Contains(t, names, "second-post.md")
	assert.Len(t, entries, 2, "flat view should have exactly 2 entries")
}
