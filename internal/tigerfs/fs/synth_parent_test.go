package fs

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

// --- Helper function tests ---

func TestJoinPathPrefix(t *testing.T) {
	tests := []struct {
		prefix string
		leaf   string
		want   string
	}{
		{"", "file.md", "file.md"},
		{"projects", "file.md", "projects/file.md"},
		{"projects/web", "todo.md", "projects/web/todo.md"},
		{"a/b/c/d", "file.txt", "a/b/c/d/file.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := joinPathPrefix(tt.prefix, tt.leaf)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHistoryDBFilename(t *testing.T) {
	withParent := &synth.ViewInfo{
		Format: synth.FormatMarkdown,
		Roles:  &synth.ColumnRoles{ParentID: "parent_id"},
	}
	withoutParent := &synth.ViewInfo{
		Format: synth.FormatMarkdown,
		Roles:  &synth.ColumnRoles{},
	}

	// Parent-pointer model: always returns localName (leaf), ignores prefix
	assert.Equal(t, "hello.md", historyDBFilename(withParent, "docs", "hello.md"))
	assert.Equal(t, "hello.md", historyDBFilename(withParent, "", "hello.md"))
	assert.Equal(t, "hello.md", historyDBFilename(withParent, "a/b/c", "hello.md"))

	// Old model: builds full path
	assert.Equal(t, "docs/hello.md", historyDBFilename(withoutParent, "docs", "hello.md"))
	assert.Equal(t, "hello.md", historyDBFilename(withoutParent, "", "hello.md"))
	assert.Equal(t, "a/b/c/hello.md", historyDBFilename(withoutParent, "a/b/c", "hello.md"))
}

// --- buildEntriesFromRows tests ---

func TestSynth_BuildEntriesFromRows_MixedFileAndDir(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	info := &synth.ViewInfo{
		Format: synth.FormatMarkdown,
		Roles: &synth.ColumnRoles{
			Filename: "filename",
			Filetype: "filetype",
			Body:     "body",
		},
		CachedMountTime: time.Now(),
	}

	columns := []string{"id", "parent_id", "filename", "filetype", "body"}
	rows := [][]interface{}{
		{"uuid-1", nil, "docs", "directory", nil},
		{"uuid-2", nil, "readme", "file", "# Hello\n"},
		{"uuid-3", nil, "notes", "directory", nil},
	}

	entries := ops.buildEntriesFromRows(columns, rows, info)
	require.Len(t, entries, 3)

	// Directory entries
	assert.Equal(t, "docs", entries[0].Name)
	assert.True(t, entries[0].IsDir)
	assert.Equal(t, "notes", entries[2].Name)
	assert.True(t, entries[2].IsDir)

	// File entry (markdown adds .md extension via GetMarkdownFilename)
	assert.Equal(t, "readme", entries[1].Name)
	assert.False(t, entries[1].IsDir)
}

func TestSynth_BuildEntriesFromRows_Empty(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	info := &synth.ViewInfo{
		Format: synth.FormatMarkdown,
		Roles: &synth.ColumnRoles{
			Filename: "filename",
			Filetype: "filetype",
			Body:     "body",
		},
	}

	entries := ops.buildEntriesFromRows([]string{"filename", "filetype", "body"}, nil, info)
	assert.Empty(t, entries)
}

func TestSynth_BuildEntriesFromRows_PlainText(t *testing.T) {
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, &mockDBClient{})

	info := &synth.ViewInfo{
		Format: synth.FormatPlainText,
		Roles: &synth.ColumnRoles{
			Filename: "filename",
			Filetype: "filetype",
			Body:     "body",
		},
	}

	columns := []string{"filename", "filetype", "body"}
	rows := [][]interface{}{
		{"notes", "file", "some content"},
	}

	entries := ops.buildEntriesFromRows(columns, rows, info)
	require.Len(t, entries, 1)
	assert.Equal(t, "notes", entries[0].Name)
	assert.False(t, entries[0].IsDir)
}

// --- resolveSynthRow tests ---

func TestSynth_ResolveSynthRow_Found(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{
			{Depth: 1, ID: "uuid-proj", Name: "projects"},
			{Depth: 2, ID: "uuid-file", Name: "todo.md"},
		},
		rowData: map[string]*mockRow{
			"public.notes.uuid-file": {
				columns: []string{"id", "filename", "body"},
				values:  []interface{}{"uuid-file", "todo.md", "content"},
			},
		},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	info := &synth.ViewInfo{
		Roles: &synth.ColumnRoles{PrimaryKey: "id", ParentID: "parent_id"},
	}

	columns, row, pkValue, fsErr := ops.resolveSynthRow(ctx, "public", "notes", info, "projects/todo.md")
	require.Nil(t, fsErr)
	assert.Equal(t, "uuid-file", pkValue)
	assert.Equal(t, []string{"id", "filename", "body"}, columns)
	assert.Equal(t, "todo.md", row[1])
}

func TestSynth_ResolveSynthRow_NotFound(t *testing.T) {
	mockDB := &mockDBClient{
		resolvePathResults: []db.PathSegment{}, // path doesn't resolve
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	info := &synth.ViewInfo{
		Roles: &synth.ColumnRoles{PrimaryKey: "id", ParentID: "parent_id"},
	}

	_, _, _, fsErr := ops.resolveSynthRow(ctx, "public", "notes", info, "nonexistent/file.md")
	require.NotNil(t, fsErr)
	assert.Equal(t, ErrNotExist, fsErr.Code)
}

func TestSynth_ResolveSynthRow_DBError(t *testing.T) {
	// A real DB error from db.ResolvePath should surface as ErrIO with the
	// cause preserved -- not as ErrNotExist (the old swallowing behavior
	// caused setNegative to poison statCache with false-negatives on
	// transient cancellations; see commit 1178aa4 and the resolveSynthPath
	// error-propagation fix).
	dbErr := fmt.Errorf("connection refused")
	mockDB := &mockDBClient{
		resolvePathErr: dbErr,
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	info := &synth.ViewInfo{
		Roles: &synth.ColumnRoles{PrimaryKey: "id", ParentID: "parent_id"},
	}

	_, _, _, fsErr := ops.resolveSynthRow(ctx, "public", "notes", info, "file.md")
	require.NotNil(t, fsErr)
	assert.Equal(t, ErrIO, fsErr.Code)
	require.True(t, errors.Is(fsErr.Cause, dbErr), "Cause chain should preserve the original DB error")
}

// --- Parent-pointer write operation tests ---

func newParentPointerMockDB() *mockDBClient {
	return &mockDBClient{
		tables: map[string][]string{"public": {"_notes"}},
		views:  map[string][]string{"public": {"notes"}},
		viewComments: map[string]map[string]string{
			"public": {"notes": "tigerfs:md,history"},
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
		allRowsData: map[string]*mockAllRows{
			"public.notes": {
				columns: []string{"id", "parent_id", "filename", "filetype", "title", "body", "encoding", "created_at", "modified_at"},
				rows:    [][]interface{}{},
			},
		},
		latestVersionIDs:   map[string]string{},
		lastInsertReturnPK: "uuid-new-1",
	}
}

// TestSynth_ParentPointer_CreateRootFile verifies creating a file at root level
// sets parent_id to nil (root) and uses the leaf filename.
func TestSynth_ParentPointer_CreateRootFile(t *testing.T) {
	mockDB := newParentPointerMockDB()
	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	content := "---\ntitle: Hello\n---\n# Hello\n"
	fsErr := ops.WriteFile(ctx, "/notes/hello.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile at root should succeed")

	// Verify the insert used leaf filename (not full path since it's root)
	require.Len(t, mockDB.insertedRows, 1)
	insertCols := mockDB.insertedRows[0].columns
	insertVals := mockDB.insertedRows[0].values

	// Find filename column value
	for i, col := range insertCols {
		if col == "filename" {
			assert.Equal(t, "hello.md", insertVals[i], "should store leaf filename")
		}
		// parent_id should NOT be in the insert for root level
		assert.NotEqual(t, "parent_id", col, "root-level file should not have parent_id in INSERT")
	}

	// Log entry should use "create" type
	require.Len(t, mockDB.logEntries, 1)
	assert.Equal(t, "create", mockDB.logEntries[0].opType)
}

// TestSynth_ParentPointer_EditExistingFile verifies that editing an existing file
// uses resolveSynthPath to find the file, then updates by UUID.
func TestSynth_ParentPointer_EditExistingFile(t *testing.T) {
	mockDB := newParentPointerMockDB()
	// File already exists: resolveSynthPath will find it
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-existing", Name: "hello.md"},
	}
	mockDB.latestVersionIDs = map[string]string{
		"uuid-existing": "version-abc",
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	content := "---\ntitle: Updated\n---\n# Updated\n"
	fsErr := ops.WriteFile(ctx, "/notes/hello.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile edit should succeed")

	// Should have updated by PK, not inserted
	assert.Empty(t, mockDB.insertedRows, "edit should UPDATE, not INSERT")
	assert.Len(t, mockDB.logEntries, 1)
	assert.Equal(t, "edit", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-existing", mockDB.logEntries[0].fileID)
}

// TestSynth_ParentPointer_MkdirAtRoot verifies mkdir creates a directory at root level.
func TestSynth_ParentPointer_MkdirAtRoot(t *testing.T) {
	mockDB := newParentPointerMockDB()
	// resolveSynthPath returns false (dir doesn't exist yet)
	mockDB.resolvePathResults = nil

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	fsErr := ops.Mkdir(ctx, "/notes/projects")
	require.Nil(t, fsErr, "Mkdir at root should succeed")

	// Should insert with leaf name and filetype=directory
	require.Len(t, mockDB.insertedRows, 1)
	insertCols := mockDB.insertedRows[0].columns
	insertVals := mockDB.insertedRows[0].values

	filenameIdx := -1
	filetypeIdx := -1
	for i, col := range insertCols {
		if col == "filename" {
			filenameIdx = i
		}
		if col == "filetype" {
			filetypeIdx = i
		}
	}
	require.GreaterOrEqual(t, filenameIdx, 0)
	require.GreaterOrEqual(t, filetypeIdx, 0)
	assert.Equal(t, "projects", insertVals[filenameIdx])
	assert.Equal(t, "directory", insertVals[filetypeIdx])
}

// TestSynth_ParentPointer_DeleteByUUID verifies delete resolves path and deletes by PK.
func TestSynth_ParentPointer_DeleteByUUID(t *testing.T) {
	mockDB := newParentPointerMockDB()
	// File exists
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-file", Name: "hello.md"},
	}
	mockDB.rowData = map[string]*mockRow{
		"public.notes.uuid-file": {
			columns: []string{"id", "parent_id", "filename", "filetype", "body"},
			values:  []interface{}{"uuid-file", nil, "hello.md", "file", "content"},
		},
	}
	mockDB.latestVersionIDs = map[string]string{
		"uuid-file": "version-xyz",
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	fsErr := ops.Delete(ctx, "/notes/hello.md")
	require.Nil(t, fsErr, "Delete should succeed")

	assert.Equal(t, "delete", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-file", mockDB.logEntries[0].fileID)
}

// TestSynth_ParentPointer_RenameSameDir verifies rename within same directory
// only changes the filename column.
func TestSynth_ParentPointer_RenameSameDir(t *testing.T) {
	mockDB := newParentPointerMockDB()
	// Old file exists
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-file", Name: "old.md"},
	}
	mockDB.rowData = map[string]*mockRow{
		"public.notes.uuid-file": {
			columns: []string{"id", "parent_id", "filename", "filetype", "body"},
			values:  []interface{}{"uuid-file", nil, "old.md", "file", "content"},
		},
	}
	mockDB.latestVersionIDs = map[string]string{
		"uuid-file": "version-abc",
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	fsErr := ops.Rename(ctx, "/notes/old.md", "/notes/new.md")
	require.Nil(t, fsErr, "Rename same dir should succeed")

	assert.Equal(t, "rename", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-file", mockDB.logEntries[0].fileID)
}

// --- Rename-as-replace tests ---

// TestSynth_ParentPointer_RenameReplace verifies that renaming to an existing
// file atomically deletes the target and renames the source (POSIX semantics).
func TestSynth_ParentPointer_RenameReplace(t *testing.T) {
	mockDB := newParentPointerMockDB()
	// Source file exists
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-lock", Name: "config.lock"},
	}
	mockDB.rowData = map[string]*mockRow{
		"public.notes.uuid-lock": {
			columns: []string{"id", "parent_id", "filename", "filetype", "body"},
			values:  []interface{}{"uuid-lock", nil, "config.lock", "file", "new content"},
		},
	}
	mockDB.latestVersionIDs = map[string]string{
		"uuid-lock":   "version-lock",
		"uuid-config": "version-config",
	}

	// Target "config" already exists -- resolveSynthPath for ["config"] returns uuid-config
	mockDB.resolvePathCallCount = 0
	originalResolveFn := mockDB.resolvePathFn
	mockDB.resolvePathFn = func(ctx context.Context, schema, table string, segments []string) ([]db.PathSegment, error) {
		// First call: resolve source ("config.lock")
		if mockDB.resolvePathCallCount == 0 {
			mockDB.resolvePathCallCount++
			return []db.PathSegment{{Depth: 1, ID: "uuid-lock", Name: "config.lock"}}, nil
		}
		// Second call: resolve target ("config") -- exists
		if mockDB.resolvePathCallCount == 1 {
			mockDB.resolvePathCallCount++
			return []db.PathSegment{{Depth: 1, ID: "uuid-config", Name: "config"}}, nil
		}
		if originalResolveFn != nil {
			return originalResolveFn(ctx, schema, table, segments)
		}
		return nil, nil
	}

	// Track DeleteAndUpdate calls
	deleteAndUpdateCalled := false
	mockDB.deleteAndUpdateFunc = func(ctx context.Context, schema, table string, deletePK *db.PKMatch, updatePK *db.PKMatch, updateCols []string, updateVals []interface{}) error {
		deleteAndUpdateCalled = true
		assert.Equal(t, "uuid-config", deletePK.Values[0], "should delete the target file")
		assert.Equal(t, "uuid-lock", updatePK.Values[0], "should update the source file")
		return nil
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	fsErr := ops.Rename(ctx, "/notes/config.lock", "/notes/config")
	require.Nil(t, fsErr, "Rename-as-replace should succeed")

	assert.True(t, deleteAndUpdateCalled, "should call DeleteAndUpdate for replace")

	// Should have two log entries: delete + rename
	require.Len(t, mockDB.logEntries, 2)
	assert.Equal(t, "delete", mockDB.logEntries[0].opType)
	assert.Equal(t, "uuid-config", mockDB.logEntries[0].fileID)
	assert.Equal(t, "rename", mockDB.logEntries[1].opType)
	assert.Equal(t, "uuid-lock", mockDB.logEntries[1].fileID)
}

// TestSynth_ParentPointer_RenameNoReplace verifies that simple rename (no target)
// uses the existing UpdateRow path, not DeleteAndUpdate.
func TestSynth_ParentPointer_RenameNoReplace(t *testing.T) {
	mockDB := newParentPointerMockDB()
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-file", Name: "old.md"},
	}
	mockDB.rowData = map[string]*mockRow{
		"public.notes.uuid-file": {
			columns: []string{"id", "parent_id", "filename", "filetype", "body"},
			values:  []interface{}{"uuid-file", nil, "old.md", "file", "content"},
		},
	}
	mockDB.latestVersionIDs = map[string]string{
		"uuid-file": "version-abc",
	}

	// Target doesn't exist -- resolveSynthPath for ["new.md"] returns empty
	mockDB.resolvePathCallCount = 0
	mockDB.resolvePathFn = func(ctx context.Context, schema, table string, segments []string) ([]db.PathSegment, error) {
		if mockDB.resolvePathCallCount == 0 {
			mockDB.resolvePathCallCount++
			return []db.PathSegment{{Depth: 1, ID: "uuid-file", Name: "old.md"}}, nil
		}
		// Second call: target doesn't exist
		mockDB.resolvePathCallCount++
		return nil, nil
	}

	deleteAndUpdateCalled := false
	mockDB.deleteAndUpdateFunc = func(ctx context.Context, schema, table string, deletePK *db.PKMatch, updatePK *db.PKMatch, updateCols []string, updateVals []interface{}) error {
		deleteAndUpdateCalled = true
		return nil
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	fsErr := ops.Rename(ctx, "/notes/old.md", "/notes/new.md")
	require.Nil(t, fsErr, "Simple rename should succeed")

	assert.False(t, deleteAndUpdateCalled, "should NOT call DeleteAndUpdate for simple rename")
	require.Len(t, mockDB.logEntries, 1)
	assert.Equal(t, "rename", mockDB.logEntries[0].opType)
}

// --- Reserved dotfile name tests ---

// TestSynth_WriteReservedName verifies that creating a file with a reserved name fails.
// Known capabilities are intercepted by the path parser (routed to their handlers, not synth).
// The checkReservedFilename helper provides defense-in-depth for direct API calls.
func TestSynth_WriteReservedName(t *testing.T) {
	reserved := []string{".history", ".log", ".savepoint", ".undo", ".info", ".by", ".filter", ".export"}
	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			mockDB := newParentPointerMockDB()
			mockDB.resolvePathResults = nil

			cfg := &config.Config{DirListingLimit: 1000}
			ops := NewOperations(cfg, mockDB)
			ctx := context.Background()

			fsErr := ops.WriteFile(ctx, fmt.Sprintf("/notes/%s", name), []byte("test"))
			require.NotNil(t, fsErr, "WriteFile(%q) should fail for reserved name", name)
		})
	}

	// Non-reserved dotfiles should not fail with ErrPermission
	allowed := []string{".gitignore", ".env", ".vscode", ".git"}
	for _, name := range allowed {
		t.Run(name, func(t *testing.T) {
			mockDB := newParentPointerMockDB()
			mockDB.resolvePathResults = nil

			cfg := &config.Config{DirListingLimit: 1000}
			ops := NewOperations(cfg, mockDB)
			ctx := context.Background()

			fsErr := ops.WriteFile(ctx, fmt.Sprintf("/notes/%s", name), []byte("test"))
			if fsErr != nil {
				assert.NotEqual(t, ErrPermission, fsErr.Code,
					"WriteFile(%q) should not fail with ErrPermission", name)
			}
		})
	}
}

// TestSynth_MkdirReservedName verifies that mkdir with a reserved name fails.
func TestSynth_MkdirReservedName(t *testing.T) {
	reserved := []string{".history", ".log", ".undo"}
	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			mockDB := newParentPointerMockDB()
			mockDB.resolvePathResults = nil

			cfg := &config.Config{DirListingLimit: 1000}
			ops := NewOperations(cfg, mockDB)
			ctx := context.Background()

			fsErr := ops.Mkdir(ctx, fmt.Sprintf("/notes/%s", name))
			require.NotNil(t, fsErr, "Mkdir(%q) should fail for reserved name", name)
		})
	}

	allowed := []string{".vscode", ".git"}
	for _, name := range allowed {
		t.Run(name, func(t *testing.T) {
			mockDB := newParentPointerMockDB()
			mockDB.resolvePathResults = nil

			cfg := &config.Config{DirListingLimit: 1000}
			ops := NewOperations(cfg, mockDB)
			ctx := context.Background()

			fsErr := ops.Mkdir(ctx, fmt.Sprintf("/notes/%s", name))
			if fsErr != nil {
				assert.NotEqual(t, ErrPermission, fsErr.Code,
					"Mkdir(%q) should not fail with ErrPermission", name)
			}
		})
	}
}

// TestSynth_RenameToReservedName verifies that renaming to a reserved name fails.
// Rename targets go through the path parser, so known capabilities are intercepted.
func TestSynth_RenameToReservedName(t *testing.T) {
	reserved := []string{".history", ".log", ".undo"}
	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			mockDB := newParentPointerMockDB()
			mockDB.resolvePathResults = []db.PathSegment{
				{Depth: 1, ID: "uuid-file", Name: "old.md"},
			}
			mockDB.rowData = map[string]*mockRow{
				"public.notes.uuid-file": {
					columns: []string{"id", "parent_id", "filename", "filetype", "body"},
					values:  []interface{}{"uuid-file", nil, "old.md", "file", "content"},
				},
			}
			mockDB.latestVersionIDs = map[string]string{
				"uuid-file": "version-xyz",
			}

			cfg := &config.Config{DirListingLimit: 1000}
			ops := NewOperations(cfg, mockDB)
			ctx := context.Background()

			fsErr := ops.Rename(ctx, "/notes/old.md", fmt.Sprintf("/notes/%s", name))
			require.NotNil(t, fsErr, "Rename to %q should fail for reserved name", name)
		})
	}

	// Rename to non-reserved dotfile should not fail with ErrPermission
	t.Run(".gitignore", func(t *testing.T) {
		mockDB := newParentPointerMockDB()
		mockDB.resolvePathResults = []db.PathSegment{
			{Depth: 1, ID: "uuid-file", Name: "old.md"},
		}
		mockDB.rowData = map[string]*mockRow{
			"public.notes.uuid-file": {
				columns: []string{"id", "parent_id", "filename", "filetype", "body"},
				values:  []interface{}{"uuid-file", nil, "old.md", "file", "content"},
			},
		}
		mockDB.latestVersionIDs = map[string]string{
			"uuid-file": "version-xyz",
		}

		cfg := &config.Config{DirListingLimit: 1000}
		ops := NewOperations(cfg, mockDB)
		ctx := context.Background()

		fsErr := ops.Rename(ctx, "/notes/old.md", "/notes/.gitignore")
		if fsErr != nil {
			assert.NotEqual(t, ErrPermission, fsErr.Code,
				"Rename to .gitignore should not fail with ErrPermission")
		}
	})
}

// TestSynth_RenameFromDotfile verifies that renaming FROM a dotfile works fine.
func TestSynth_RenameFromDotfile(t *testing.T) {
	mockDB := newParentPointerMockDB()
	mockDB.resolvePathResults = []db.PathSegment{
		{Depth: 1, ID: "uuid-file", Name: ".gitignore"},
	}
	mockDB.rowData = map[string]*mockRow{
		"public.notes.uuid-file": {
			columns: []string{"id", "parent_id", "filename", "filetype", "body"},
			values:  []interface{}{"uuid-file", nil, ".gitignore", "file", "content"},
		},
	}
	mockDB.latestVersionIDs = map[string]string{
		"uuid-file": "version-xyz",
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)
	ctx := context.Background()

	fsErr := ops.Rename(ctx, "/notes/.gitignore", "/notes/ignored.txt")
	require.Nil(t, fsErr, "Rename from dotfile should succeed")
}

// TestSynth_FilterReservedNames verifies that reserved names are filtered from ReadDir.
func TestSynth_FilterReservedNames(t *testing.T) {
	entries := []Entry{
		{Name: "hello.md"},
		{Name: ".gitignore"},
		{Name: ".history"}, // reserved -- should be filtered
		{Name: ".log"},     // reserved -- should be filtered
		{Name: ".env"},
		{Name: ".undo"}, // reserved -- should be filtered
	}

	filtered := filterReservedNames(entries)
	names := make([]string, len(filtered))
	for i, e := range filtered {
		names[i] = e.Name
	}

	assert.Contains(t, names, "hello.md")
	assert.Contains(t, names, ".gitignore")
	assert.Contains(t, names, ".env")
	assert.NotContains(t, names, ".history")
	assert.NotContains(t, names, ".log")
	assert.NotContains(t, names, ".undo")
}

// TestSynth_CheckReservedFilename verifies the reserved filename check helper.
func TestSynth_CheckReservedFilename(t *testing.T) {
	tests := []struct {
		filename string
		wantErr  bool
	}{
		{".history", true},
		{".log", true},
		{".info", true},
		{".gitignore", false},
		{".env", false},
		{"subdir/.history", true},    // leaf is reserved
		{"subdir/.gitignore", false}, // leaf is not reserved
		{"hello.md", false},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			err := checkReservedFilename(tt.filename)
			if tt.wantErr {
				require.NotNil(t, err)
				assert.Equal(t, ErrPermission, err.Code)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
