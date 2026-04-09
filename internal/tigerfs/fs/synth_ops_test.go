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

// TestSynth_LogEntries_WriteSynthFile verifies that write operations on
// history-enabled synth apps create log entries.
func TestSynth_LogEntries_WriteSynthFile(t *testing.T) {
	mockDB := &mockDBClient{
		tables: map[string][]string{"public": {"_notes"}},
		views:  map[string][]string{"public": {"notes"}},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md,history"},
		},
		columns: map[string][]mockColumn{
			"public.notes": {
				{name: "id", dataType: "uuid"},
				{name: "filename", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "body", dataType: "text"},
				{name: "encoding", dataType: "text"},
				{name: "created_at", dataType: "timestamptz"},
				{name: "modified_at", dataType: "timestamptz"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public._notes": {column: "id"},
			"public.notes":  {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.notes": {
				columns: []string{"id", "filename", "title", "body", "encoding", "created_at", "modified_at"},
				rows:    [][]interface{}{},
			},
		},
		// InsertRow returns a fake PK
		lastInsertReturnPK: "uuid-new-1",
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	// INSERT: write a new file
	content := "---\ntitle: Hello\n---\n# Hello\n"
	writeErr := ops.WriteFile(ctx, "/notes/hello.md", []byte(content))
	require.Nil(t, writeErr, "WriteFile should succeed for insert")

	// Verify a log entry was created for the insert
	require.Len(t, mockDB.logEntries, 1, "should have 1 log entry after insert")
	assert.Equal(t, "insert", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-new-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "hello.md", mockDB.logEntries[0].filename)
	assert.Empty(t, mockDB.logEntries[0].historyID, "insert should have no history_id")
}

// TestSynth_LogEntries_NoHistory verifies that write operations on synth apps
// WITHOUT history do NOT create log entries.
func TestSynth_LogEntries_NoHistory(t *testing.T) {
	mockDB := &mockDBClient{
		tables: map[string][]string{"public": {"_notes"}},
		views:  map[string][]string{"public": {"notes"}},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md"}, // no ",history"
		},
		columns: map[string][]mockColumn{
			"public.notes": {
				{name: "id", dataType: "uuid"},
				{name: "filename", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "body", dataType: "text"},
				{name: "encoding", dataType: "text"},
				{name: "created_at", dataType: "timestamptz"},
				{name: "modified_at", dataType: "timestamptz"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public._notes": {column: "id"},
			"public.notes":  {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.notes": {
				columns: []string{"id", "filename", "title", "body", "encoding", "created_at", "modified_at"},
				rows:    [][]interface{}{},
			},
		},
		lastInsertReturnPK: "uuid-new-1",
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	content := "---\ntitle: Hello\n---\n# Hello\n"
	writeErr := ops.WriteFile(ctx, "/notes/hello.md", []byte(content))
	require.Nil(t, writeErr)

	// No history → no log entries
	assert.Empty(t, mockDB.logEntries, "non-history app should not create log entries")
}

// newHistoryMockDB creates a mock DB with a history-enabled synth app that has
// one existing row (hello.md with PK uuid-1). Used for UPDATE/DELETE/RENAME tests.
func newHistoryMockDB() *mockDBClient {
	return &mockDBClient{
		tables: map[string][]string{"public": {"_notes"}},
		views:  map[string][]string{"public": {"notes"}},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md,history"},
		},
		columns: map[string][]mockColumn{
			"public.notes": {
				{name: "id", dataType: "uuid"},
				{name: "filename", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "body", dataType: "text"},
				{name: "encoding", dataType: "text"},
				{name: "created_at", dataType: "timestamptz"},
				{name: "modified_at", dataType: "timestamptz"},
			},
		},
		primaryKeys: map[string]*mockPK{
			"public._notes": {column: "id"},
			"public.notes":  {column: "id"},
		},
		allRowsData: map[string]*mockAllRows{
			"public.notes": {
				columns: []string{"id", "filename", "title", "body", "encoding", "created_at", "modified_at"},
				rows: [][]interface{}{
					{"uuid-1", "hello.md", "Hello", "# Hello\n", "utf8", nil, nil},
				},
			},
		},
		latestHistoryIDs: map[string]string{
			"uuid-1": "history-uuid-abc",
		},
		// rowData is checked by DeleteRow mock
		rowData: map[string]*mockRow{
			"public.notes.uuid-1": {
				columns: []string{"id", "filename", "title", "body"},
				values:  []interface{}{"uuid-1", "hello.md", "Hello", "# Hello\n"},
			},
		},
	}
}

// TestSynth_LogEntries_UpdateSynthFile verifies that updating an existing file
// creates a log entry with type=update and captures the history_id.
func TestSynth_LogEntries_UpdateSynthFile(t *testing.T) {
	mockDB := newHistoryMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	// UPDATE: write to an existing file
	content := "---\ntitle: Hello Updated\n---\n# Hello Updated\n"
	writeErr := ops.WriteFile(context.Background(), "/notes/hello.md", []byte(content))
	require.Nil(t, writeErr, "WriteFile should succeed for update")

	require.Len(t, mockDB.logEntries, 1, "should have 1 log entry after update")
	assert.Equal(t, "update", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "hello.md", mockDB.logEntries[0].filename)
	assert.Equal(t, "history-uuid-abc", mockDB.logEntries[0].historyID, "update should capture history_id")
}

// TestSynth_LogEntries_DeleteSynthFile verifies that deleting a file creates
// a log entry with type=delete and captures the history_id.
func TestSynth_LogEntries_DeleteSynthFile(t *testing.T) {
	mockDB := newHistoryMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	deleteErr := ops.Delete(context.Background(), "/notes/hello.md")
	require.Nil(t, deleteErr, "Delete should succeed")

	require.Len(t, mockDB.logEntries, 1, "should have 1 log entry after delete")
	assert.Equal(t, "delete", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "hello.md", mockDB.logEntries[0].filename)
	assert.Equal(t, "history-uuid-abc", mockDB.logEntries[0].historyID, "delete should capture history_id")
}

// TestSynth_LogEntries_RenameSynthFile verifies that renaming a file creates
// a log entry with type=update, the new filename, and captures the history_id.
func TestSynth_LogEntries_RenameSynthFile(t *testing.T) {
	mockDB := newHistoryMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	renameErr := ops.Rename(context.Background(), "/notes/hello.md", "/notes/goodbye.md")
	require.Nil(t, renameErr, "Rename should succeed")

	require.Len(t, mockDB.logEntries, 1, "should have 1 log entry after rename")
	assert.Equal(t, "update", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "goodbye.md", mockDB.logEntries[0].filename, "rename should log the NEW filename")
	assert.Equal(t, "history-uuid-abc", mockDB.logEntries[0].historyID, "rename should capture history_id")
}
