package fs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
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

func TestSynth_StatDirectory_UsesModifiedAt(t *testing.T) {
	dirModifiedAt := time.Date(2025, 3, 15, 10, 30, 0, 0, time.UTC)

	mockDB := &mockDBClient{
		tables: map[string][]string{"public": {"_notes"}},
		views:  map[string][]string{"public": {"notes"}},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md"},
		},
		columns: map[string][]mockColumn{
			"public.notes": {
				{name: "id", dataType: "uuid"},
				{name: "parent_id", dataType: "uuid"},
				{name: "filename", dataType: "text"},
				{name: "filetype", dataType: "text"},
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
		// resolveSynthPath returns the directory UUID
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-dir-1", Name: "docs"},
		},
		// GetRow returns the directory row with a known modified_at
		rowData: map[string]*mockRow{
			"public.notes.uuid-dir-1": {
				columns: []string{"id", "parent_id", "filename", "filetype", "title", "body", "encoding", "created_at", "modified_at"},
				values:  []interface{}{"uuid-dir-1", nil, "docs", "directory", nil, nil, "utf8", dirModifiedAt, dirModifiedAt},
			},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	entry, fsErr := ops.Stat(ctx, "/notes/docs")
	require.Nil(t, fsErr, "Stat should succeed for directory")
	require.NotNil(t, entry)

	assert.True(t, entry.IsDir, "should be a directory")
	assert.Equal(t, "docs", entry.Name)
	assert.True(t, entry.ModTime.Equal(dirModifiedAt),
		"directory ModTime should come from database modified_at, got %v, want %v", entry.ModTime, dirModifiedAt)
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
	assert.Equal(t, "create", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-new-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "hello.md", mockDB.logEntries[0].filename)
	assert.Empty(t, mockDB.logEntries[0].versionID, "create should have no version_id")
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
		latestVersionIDs: map[string]string{
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

// TestSynth_LogEntries_EditSynthFile verifies that updating an existing file
// creates a log entry with type=edit and captures the version_id.
func TestSynth_LogEntries_EditSynthFile(t *testing.T) {
	mockDB := newHistoryMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	// EDIT: write to an existing file
	content := "---\ntitle: Hello Updated\n---\n# Hello Updated\n"
	writeErr := ops.WriteFile(context.Background(), "/notes/hello.md", []byte(content))
	require.Nil(t, writeErr, "WriteFile should succeed for edit")

	require.Len(t, mockDB.logEntries, 1, "should have 1 log entry after edit")
	assert.Equal(t, "edit", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "hello.md", mockDB.logEntries[0].filename)
	assert.Equal(t, "history-uuid-abc", mockDB.logEntries[0].versionID, "edit should capture version_id")
}

// TestSynth_LogEntries_DeleteSynthFile verifies that deleting a file creates
// a log entry with type=delete and captures the version_id.
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
	assert.Equal(t, "history-uuid-abc", mockDB.logEntries[0].versionID, "delete should capture version_id")
}

// TestSynth_LogEntries_RenameSynthFile verifies that renaming a file creates
// a log entry with type=rename, the new filename, and captures the version_id.
func TestSynth_LogEntries_RenameSynthFile(t *testing.T) {
	mockDB := newHistoryMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	renameErr := ops.Rename(context.Background(), "/notes/hello.md", "/notes/goodbye.md")
	require.Nil(t, renameErr, "Rename should succeed")

	require.Len(t, mockDB.logEntries, 1, "should have 1 log entry after rename")
	assert.Equal(t, "rename", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-1", mockDB.logEntries[0].fileID)
	assert.Equal(t, "goodbye.md", mockDB.logEntries[0].filename, "rename should log the NEW filename")
	assert.Equal(t, "history-uuid-abc", mockDB.logEntries[0].versionID, "rename should capture version_id")
}

// --- resolveSynthPath tests ---

// TestSynth_ResolvePath_FullCacheMiss verifies that resolveSynthPath calls
// the DB resolve_path when the cache is empty and populates the cache.
func TestSynth_ResolvePath_FullCacheMiss(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-proj", Name: "projects"},
			{Depth: 2, ID: "uuid-web", Name: "web"},
			{Depth: 3, ID: "uuid-todo", Name: "todo.md"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "web", "todo.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-todo", id)
	assert.Equal(t, 1, mockDB.resolvePathCalls, "should call DB once")

	// Cache should now be populated -- second call should not hit DB
	id, ok, _ = ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "web", "todo.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-todo", id)
	assert.Equal(t, 1, mockDB.resolvePathCalls, "should still be 1 DB call (cache hit)")
}

// TestSynth_ResolvePath_PartialCacheHit verifies that resolveSynthPath
// only queries the DB for unresolved segments.
func TestSynth_ResolvePath_PartialCacheHit(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-notes", Name: "notes.md"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	// Pre-populate cache for "projects" and "web"
	ops.pathCache.put("public", "notes", "", "projects", "uuid-proj")
	ops.pathCache.put("public", "notes", "uuid-proj", "web", "uuid-web")

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "web", "notes.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-notes", id)
	assert.Equal(t, 1, mockDB.resolvePathCalls)

	// Verify DB was called with only the remaining segment and correct startParentID
	assert.Equal(t, "uuid-web", mockDB.lastResolveStartParent)
	assert.Equal(t, []string{"notes.md"}, mockDB.lastResolveSegments)
}

// TestSynth_ResolvePath_NonexistentPath verifies that resolveSynthPath
// returns false when a segment doesn't resolve.
func TestSynth_ResolvePath_NonexistentPath(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			// Only first segment resolves, "nonexistent" doesn't
			{Depth: 1, ID: "uuid-proj", Name: "projects"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "nonexistent", "file.md"})
	assert.False(t, ok)
	assert.Empty(t, id)
}

// TestSynth_ResolvePath_EmptySegments verifies root-level resolution.
func TestSynth_ResolvePath_EmptySegments(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})
	ctx := context.Background()

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{})
	assert.True(t, ok)
	assert.Empty(t, id)
}

// TestSynth_ResolvePath_SingleSegment verifies single-level resolution.
func TestSynth_ResolvePath_SingleSegment(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-file", Name: "hello.md"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{"hello.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-file", id)
	assert.Equal(t, "", mockDB.lastResolveStartParent, "root-level should pass empty start parent")
}

// TestSynth_ResolvePath_CacheInvalidation verifies that invalidation
// forces the next resolve to hit the DB.
func TestSynth_ResolvePath_CacheInvalidation(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-1", Name: "file.md"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	// First call populates cache
	ops.resolveSynthPath(ctx, "public", "notes", []string{"file.md"})
	assert.Equal(t, 1, mockDB.resolvePathCalls)

	// Invalidate cache
	ops.pathCache.invalidate("public", "notes")

	// Second call should hit DB again
	ops.resolveSynthPath(ctx, "public", "notes", []string{"file.md"})
	assert.Equal(t, 2, mockDB.resolvePathCalls)
}

// TestSynth_ResolvePath_DeeplyNested verifies resolution of a 5-level deep path
// (ADR-017 verification scenario #13).
func TestSynth_ResolvePath_DeeplyNested(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-a", Name: "a"},
			{Depth: 2, ID: "uuid-b", Name: "b"},
			{Depth: 3, ID: "uuid-c", Name: "c"},
			{Depth: 4, ID: "uuid-d", Name: "d"},
			{Depth: 5, ID: "uuid-file", Name: "file.md"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "app", []string{"a", "b", "c", "d", "file.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-file", id)

	// Verify all 5 levels are cached
	cached, ok := ops.pathCache.lookup("public", "app", "", "a")
	assert.True(t, ok)
	assert.Equal(t, "uuid-a", cached)

	cached, ok = ops.pathCache.lookup("public", "app", "uuid-a", "b")
	assert.True(t, ok)
	assert.Equal(t, "uuid-b", cached)

	cached, ok = ops.pathCache.lookup("public", "app", "uuid-b", "c")
	assert.True(t, ok)
	assert.Equal(t, "uuid-c", cached)

	cached, ok = ops.pathCache.lookup("public", "app", "uuid-c", "d")
	assert.True(t, ok)
	assert.Equal(t, "uuid-d", cached)

	cached, ok = ops.pathCache.lookup("public", "app", "uuid-d", "file.md")
	assert.True(t, ok)
	assert.Equal(t, "uuid-file", cached)
}

// TestSynth_ResolvePath_SiblingAccess verifies the sibling resolution pattern
// described in ADR-017: after resolving projects/web/todo.md, resolving
// projects/web/notes.md should only query the DB for the last segment.
func TestSynth_ResolvePath_SiblingAccess(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-proj", Name: "projects"},
			{Depth: 2, ID: "uuid-web", Name: "web"},
			{Depth: 3, ID: "uuid-todo", Name: "todo.md"},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	// First access: resolve full path (cold cache)
	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "web", "todo.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-todo", id)
	assert.Equal(t, 1, mockDB.resolvePathCalls)

	// Set up mock for sibling -- only returns the leaf
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-notes", Name: "notes.md"},
	}

	// Second access: sibling file in same directory
	id, ok, _ = ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "web", "notes.md"})
	assert.True(t, ok)
	assert.Equal(t, "uuid-notes", id)
	assert.Equal(t, 2, mockDB.resolvePathCalls)

	// DB should have been called with only the leaf segment, starting from uuid-web
	assert.Equal(t, "uuid-web", mockDB.lastResolveStartParent,
		"should start from cached parent (web directory)")
	assert.Equal(t, []string{"notes.md"}, mockDB.lastResolveSegments,
		"should only query the unresolved leaf segment")
}

// TestSynth_ResolvePath_DBError verifies that a DB error returns false
// without panicking.
func TestSynth_ResolvePath_DBError(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathErr: fmt.Errorf("connection refused"),
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	id, ok, _ := ops.resolveSynthPath(ctx, "public", "notes", []string{"projects", "file.md"})
	assert.False(t, ok)
	assert.Empty(t, id)
}

// --- Cache-load: ViewInfo.Metadata population ---
//
// The undo boundary check reads ViewInfo.Metadata, which is populated by
// loadSynthCache when HasHistory is true. These tests verify that:
//   1. populates when a history-enabled app has metadata entries
//   2. soft-fails to nil on QueryMetadata error (treats as fresh install)
//   3. skips entirely for non-history apps (zero QueryMetadata calls)

func newSynthCacheMock(viewName, comment string) *mockDBClient {
	return &mockDBClient{
		views:        map[string][]string{"public": {viewName}},
		viewComments: map[string]map[string]string{"public": {viewName: comment}},
		columns: map[string][]mockColumn{
			"public." + viewName: {
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
			"public." + viewName:                 {column: "id"},
			synth.TigerFSSchema + "." + viewName: {column: "id"},
		},
	}
}

func TestSynth_LoadSynthCache_PopulatesMetadata(t *testing.T) {
	mockDB := newSynthCacheMock("notes", "tigerfs:md,history")
	mockDB.metadataEntries = []db.MetadataEntry{
		{
			EntryID:     "019e0000-0000-7000-8000-000000000001",
			Subject:     synth.SubjectHistoryFormatMigration,
			Description: "boundary hint",
			Payload:     []byte(`{"from":"0.6","to":"0.7"}`),
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	info := ops.getSynthViewInfo(context.Background(), "public", "notes")
	require.NotNil(t, info, "ViewInfo for history-enabled app must be cached")
	require.True(t, info.HasHistory)
	require.Len(t, info.Metadata, 1)
	assert.Equal(t, synth.SubjectHistoryFormatMigration, info.Metadata[0].Subject)
	assert.Equal(t, "boundary hint", info.Metadata[0].Description)
	assert.Equal(t, 1, mockDB.metadataCalls, "exactly one QueryMetadata call expected")
}

func TestSynth_LoadSynthCache_SoftFailsOnMetadataError(t *testing.T) {
	mockDB := newSynthCacheMock("notes", "tigerfs:md,history")
	mockDB.metadataErr = fmt.Errorf("simulated transient query failure")

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	info := ops.getSynthViewInfo(context.Background(), "public", "notes")
	require.NotNil(t, info, "cache load must succeed even when metadata query errors")
	assert.True(t, info.HasHistory, "HasHistory detection still succeeds")
	assert.Nil(t, info.Metadata, "Metadata stays nil on soft-fail (treated as no boundary)")
	assert.Equal(t, 1, mockDB.metadataCalls, "QueryMetadata was attempted once")
}

func TestSynth_LoadSynthCache_SkipsMetadataForNonHistory(t *testing.T) {
	// View comment is "tigerfs:md" (no ",history") and there is no _history
	// companion table, so HasHistory=false. QueryMetadata must not be called.
	mockDB := newSynthCacheMock("notes", "tigerfs:md")

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	info := ops.getSynthViewInfo(context.Background(), "public", "notes")
	require.NotNil(t, info)
	assert.False(t, info.HasHistory)
	assert.Nil(t, info.Metadata)
	assert.Equal(t, 0, mockDB.metadataCalls, "QueryMetadata must not be called for non-history apps")
}
