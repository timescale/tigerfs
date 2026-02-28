package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// TestSynth_BuildMarkdown tests the .build/ scaffolding for markdown apps.
// echo "markdown" > /.build/posts → creates _posts table + posts view with COMMENT 'tigerfs:md'
func TestSynth_BuildMarkdown(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build the markdown app
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "WriteFile to .build/posts should succeed: %v", fsErr)

	// Root should now list both _posts (backing table) and posts (synth view)
	entries, fsErr := ops.ReadDir(ctx, "/")
	require.Nil(t, fsErr, "ReadDir root should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "_posts", "root should contain backing table _posts")
	assert.Contains(t, names, "posts", "root should contain view posts")

	// The posts view should be a directory
	entry, fsErr := ops.Stat(ctx, "/posts")
	require.Nil(t, fsErr, "Stat /posts should succeed")
	assert.True(t, entry.IsDir, "posts view should be a directory")

	// The backing table should also be a directory (native access)
	entry, fsErr = ops.Stat(ctx, "/_posts")
	require.Nil(t, fsErr, "Stat /_posts should succeed")
	assert.True(t, entry.IsDir, "_posts backing table should be a directory")
}

// TestSynth_BuildPlainText tests the .build/ scaffolding for plain text apps.
func TestSynth_BuildPlainText(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build the plain text app
	fsErr := ops.WriteFile(ctx, "/.build/snippets", []byte("txt\n"))
	require.Nil(t, fsErr, "WriteFile to .build/snippets should succeed: %v", fsErr)

	// Root should now list both _snippets and snippets
	entries, fsErr := ops.ReadDir(ctx, "/")
	require.Nil(t, fsErr, "ReadDir root should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "_snippets", "root should contain backing table _snippets")
	assert.Contains(t, names, "snippets", "root should contain view snippets")
}

// TestSynth_FormatMarkdown tests the .format/ handler on an existing table.
// Creates a table manually, then: echo ok > /table/.format/markdown
func TestSynth_FormatMarkdown(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Create a table manually first (simulating an existing table)
	createSQL := `CREATE TABLE notes (
		id serial PRIMARY KEY,
		filename text NOT NULL,
		title text,
		author text,
		body text,
		created_at timestamp DEFAULT NOW()
	)`
	fsErr := execSQL(t, result.ConnStr, createSQL)
	require.NoError(t, fsErr, "Failed to create test table")

	// The table should appear at root
	entry, err := ops.Stat(ctx, "/notes")
	require.Nil(t, err, "Stat /notes should succeed")
	assert.True(t, entry.IsDir)

	// Apply markdown format to the table
	err = ops.WriteFile(ctx, "/notes/.format/markdown", []byte("ok\n"))
	require.Nil(t, err, "WriteFile to .format/markdown should succeed: %v", err)

	// Now a new view "notes_md" should be visible
	entries, err := ops.ReadDir(ctx, "/")
	require.Nil(t, err, "ReadDir root should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "notes", "root should still contain original table")
	assert.Contains(t, names, "notes_md", "root should contain synthesized view notes_md")
}

// TestSynth_ReadMarkdownFile tests reading a synthesized markdown file.
// cat posts/hello-world.md → YAML frontmatter + body
func TestSynth_ReadMarkdownFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app and insert test data
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', '# Hello World

This is my first post.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// List the synth view directory
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr, "ReadDir /posts should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "hello-world.md", "should list hello-world.md")

	// Read the markdown file
	content, fsErr := ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "ReadFile should succeed")

	text := string(content.Data)
	t.Logf("Markdown content:\n%s", text)

	// Verify frontmatter
	assert.True(t, strings.HasPrefix(text, "---\n"), "should start with YAML frontmatter delimiter")
	assert.Contains(t, text, "title: Hello World")
	assert.Contains(t, text, "author: alice")

	// Verify body
	assert.Contains(t, text, "# Hello World")
	assert.Contains(t, text, "This is my first post.")

	// Stat should return correct size
	entry, fsErr := ops.Stat(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "Stat should succeed")
	assert.Equal(t, int64(len(content.Data)), entry.Size, "Stat size should match content length")
	assert.False(t, entry.IsDir, "synth file should not be a directory")
}

// TestSynth_WriteMarkdownFile tests creating a new file via write.
// echo content > posts/new.md → INSERT into _posts
func TestSynth_WriteMarkdownFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Write a new markdown file
	newContent := "---\ntitle: My New Post\nauthor: bob\n---\n\n# My New Post\n\nWritten via TigerFS.\n"
	fsErr = ops.WriteFile(ctx, "/posts/my-new-post.md", []byte(newContent))
	require.Nil(t, fsErr, "WriteFile should succeed for new synth file")

	// Verify it appears in directory listing
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr, "ReadDir should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "my-new-post.md", "new file should appear in listing")

	// Read it back and verify round-trip
	content, fsErr := ops.ReadFile(ctx, "/posts/my-new-post.md")
	require.Nil(t, fsErr, "ReadFile should succeed for new file")

	text := string(content.Data)
	assert.Contains(t, text, "title: My New Post")
	assert.Contains(t, text, "author: bob")
	assert.Contains(t, text, "Written via TigerFS.")
}

// TestSynth_EditMarkdownFile tests overwriting an existing synthesized file.
// Overwriting hello-world.md should UPDATE the existing row.
func TestSynth_EditMarkdownFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app and insert test data
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', '# Hello

First version.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Verify initial content
	content, fsErr := ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "initial ReadFile should succeed")
	assert.Contains(t, string(content.Data), "First version.")

	// Overwrite with updated content
	updatedContent := "---\ntitle: Hello World (Updated)\nauthor: alice\n---\n\n# Hello\n\nSecond version.\n"
	fsErr = ops.WriteFile(ctx, "/posts/hello-world.md", []byte(updatedContent))
	require.Nil(t, fsErr, "overwrite should succeed")

	// Read back and verify update
	content, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "ReadFile after update should succeed")

	text := string(content.Data)
	assert.Contains(t, text, "Hello World (Updated)")
	assert.Contains(t, text, "Second version.")
	assert.NotContains(t, text, "First version.")

	// Verify directory listing still shows exactly one file (update, not duplicate)
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr, "ReadDir should succeed")

	mdFiles := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name, ".md") {
			mdFiles++
		}
	}
	assert.Equal(t, 1, mdFiles, "should still have exactly 1 .md file after update")
}

// TestSynth_DeleteMarkdownFile tests deleting a synthesized file.
// rm posts/hello-world.md → DELETE FROM _posts WHERE id = ?
func TestSynth_DeleteMarkdownFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app and insert test data
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', '# Hello

Content here.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Verify file exists
	_, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "file should exist before delete")

	// Delete the file
	fsErr = ops.Delete(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "Delete should succeed")

	// Verify file is gone
	_, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.NotNil(t, fsErr, "ReadFile should fail after delete")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should be not-exist error")

	// Directory should be empty
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr, "ReadDir should succeed")

	mdFiles := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name, ".md") {
			mdFiles++
		}
	}
	assert.Equal(t, 0, mdFiles, "should have 0 .md files after delete")
}

// TestSynth_ReadPlainTextFile tests reading a plain text synthesized file.
func TestSynth_ReadPlainTextFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build plain text app
	fsErr := ops.WriteFile(ctx, "/.build/snippets", []byte("txt\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _snippets (filename, body) VALUES
		('hello.txt', 'Hello, world!
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// List directory
	entries, fsErr := ops.ReadDir(ctx, "/snippets")
	require.Nil(t, fsErr, "ReadDir should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "hello.txt", "should list hello.txt")

	// Read the file
	content, fsErr := ops.ReadFile(ctx, "/snippets/hello.txt")
	require.Nil(t, fsErr, "ReadFile should succeed")
	assert.Equal(t, "Hello, world!\n", string(content.Data))
}

// TestSynth_NativeAccessStillWorks verifies that the native row/column access
// still works on the backing table alongside the synthesized view.
func TestSynth_NativeAccessStillWorks(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app and insert test data
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', '# Hello

Content here.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Access via synth view (markdown format)
	content, fsErr := ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "synth ReadFile should succeed")
	assert.Contains(t, string(content.Data), "title: Hello World")

	// Access via native table (_posts) — row directory format
	entries, fsErr := ops.ReadDir(ctx, "/_posts")
	require.Nil(t, fsErr, "ReadDir /_posts should succeed")

	// Should see row "1" (or whatever PK is assigned)
	var hasRow bool
	for _, e := range entries {
		if e.IsDir && e.Name != ".info" && e.Name != ".by" && e.Name != ".filter" &&
			e.Name != ".order" && e.Name != ".first" && e.Name != ".last" &&
			e.Name != ".sample" && e.Name != ".export" && e.Name != ".import" &&
			e.Name != ".format" && !strings.HasPrefix(e.Name, ".") {
			hasRow = true

			// Read a column from native path
			titleContent, fsErr := ops.ReadFile(ctx, "/_posts/"+e.Name+"/title")
			require.Nil(t, fsErr, "ReadFile column should succeed")
			assert.Equal(t, "Hello World\n", string(titleContent.Data))
			break
		}
	}
	assert.True(t, hasRow, "backing table should have at least one row entry")
}

// TestSynth_MultipleFiles tests a synth view with multiple files.
func TestSynth_MultipleFiles(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Insert multiple rows
	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('first-post.md', 'First Post', 'alice', 'Body of first post.
'),
		('second-post.md', 'Second Post', 'bob', 'Body of second post.
'),
		('third-post.md', 'Third Post', 'charlie', 'Body of third post.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// List should show all three files
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr, "ReadDir should succeed")

	names := fsEntryNames(entries)
	assert.Contains(t, names, "first-post.md")
	assert.Contains(t, names, "second-post.md")
	assert.Contains(t, names, "third-post.md")

	// Each file should have correct content
	for _, tc := range []struct {
		filename string
		title    string
		author   string
		body     string
	}{
		{"first-post.md", "First Post", "alice", "Body of first post."},
		{"second-post.md", "Second Post", "bob", "Body of second post."},
		{"third-post.md", "Third Post", "charlie", "Body of third post."},
	} {
		t.Run(tc.filename, func(t *testing.T) {
			content, fsErr := ops.ReadFile(ctx, "/posts/"+tc.filename)
			require.Nil(t, fsErr, "ReadFile %s should succeed", tc.filename)

			text := string(content.Data)
			assert.Contains(t, text, "title: "+tc.title)
			assert.Contains(t, text, "author: "+tc.author)
			assert.Contains(t, text, tc.body)
		})
	}
}

// TestSynth_RenameMarkdownFile tests renaming a synthesized markdown file.
// mv posts/hello-world.md posts/renamed-post.md → UPDATE filename column
func TestSynth_RenameMarkdownFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app and insert test data
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', '# Hello World

This is my first post.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Verify file exists at old path
	content, fsErr := ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "ReadFile should succeed before rename")
	assert.Contains(t, string(content.Data), "title: Hello World")

	// Rename the file
	fsErr = ops.Rename(ctx, "/posts/hello-world.md", "/posts/renamed-post.md")
	require.Nil(t, fsErr, "Rename should succeed")

	// Old path should no longer exist
	_, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.NotNil(t, fsErr, "ReadFile should fail at old path after rename")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should be not-exist error")

	// New path should be readable with correct content
	content, fsErr = ops.ReadFile(ctx, "/posts/renamed-post.md")
	require.Nil(t, fsErr, "ReadFile should succeed at new path")

	text := string(content.Data)
	assert.Contains(t, text, "title: Hello World", "frontmatter should be preserved")
	assert.Contains(t, text, "author: alice", "frontmatter should be preserved")
	assert.Contains(t, text, "This is my first post.", "body should be preserved")

	// Directory listing should show renamed file
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr, "ReadDir should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "renamed-post.md", "should list renamed-post.md")
	assert.NotContains(t, names, "hello-world.md", "should NOT list hello-world.md")
}

// TestSynth_RenameFilenameWithExtension tests renaming when the DB stores
// filenames with the .md extension (like the blog demo app does).
// This is a regression test: the CAS WHERE clause must use the actual DB
// value, not a stripped version.
func TestSynth_RenameFilenameWithExtension(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Insert data WITH .md extension in filename — matching real blog app convention
	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', '# Hello

This is a post.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Verify file exists (GetMarkdownFilename sees "hello-world.md",
	// recognizes it already has .md, returns "hello-world.md")
	content, fsErr := ops.ReadFile(ctx, "/posts/hello-world.md")
	require.Nil(t, fsErr, "ReadFile should succeed")
	assert.Contains(t, string(content.Data), "title: Hello World")

	// Rename — CAS must use "hello-world.md" (the DB value), not "hello-world"
	fsErr = ops.Rename(ctx, "/posts/hello-world.md", "/posts/renamed.md")
	require.Nil(t, fsErr, "Rename should succeed when DB stores filename with .md")

	// Old path gone
	_, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.NotNil(t, fsErr, "old path should not exist")

	// New path readable
	content, fsErr = ops.ReadFile(ctx, "/posts/renamed.md")
	require.Nil(t, fsErr, "ReadFile at new path should succeed")
	assert.Contains(t, string(content.Data), "title: Hello World")

	// Directory listing
	entries, fsErr := ops.ReadDir(ctx, "/posts")
	require.Nil(t, fsErr)
	names := fsEntryNames(entries)
	assert.Contains(t, names, "renamed.md")
	assert.NotContains(t, names, "hello-world.md")
}

// TestSynth_RenamePlainTextFile tests renaming a synthesized plain text file.
func TestSynth_RenamePlainTextFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build plain text app
	fsErr := ops.WriteFile(ctx, "/.build/snippets", []byte("txt\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _snippets (filename, body) VALUES ('scratch.txt', 'Some scratch notes.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Rename
	fsErr = ops.Rename(ctx, "/snippets/scratch.txt", "/snippets/ideas.txt")
	require.Nil(t, fsErr, "Rename should succeed")

	// Old path gone
	_, fsErr = ops.ReadFile(ctx, "/snippets/scratch.txt")
	require.NotNil(t, fsErr, "old path should not exist")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)

	// New path works
	content, fsErr := ops.ReadFile(ctx, "/snippets/ideas.txt")
	require.Nil(t, fsErr, "new path should be readable")
	assert.Equal(t, "Some scratch notes.\n", string(content.Data))
}

// TestSynth_RenamePreservesFilename tests that rename stores exactly the
// new filename in the DB (FS name == DB name, no extension manipulation).
func TestSynth_RenamePreservesFilename(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build app and insert test data
	fsErr := ops.WriteFile(ctx, "/.build/posts", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	insertSQL := `INSERT INTO _posts (filename, title, author, body) VALUES
		('hello-world.md', 'Hello World', 'alice', 'Body content.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Rename with extensions
	fsErr = ops.Rename(ctx, "/posts/hello-world.md", "/posts/new-name.md")
	require.Nil(t, fsErr, "Rename should succeed")

	// Old path gone
	_, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.NotNil(t, fsErr, "old path should not exist after rename")

	// New path works
	content, fsErr := ops.ReadFile(ctx, "/posts/new-name.md")
	require.Nil(t, fsErr, "new path should be readable")
	assert.Contains(t, string(content.Data), "title: Hello World")
}

// TestMount_BuildMarkdownViaNFS exercises the .build/ write through the actual
// NFS mount (os.WriteFile through the mount point), reproducing the user's
// `echo "markdown" > .build/memory` flow end-to-end.
//
// Prior to this test, .build/ writes were only tested via ops.WriteFile() which
// bypasses the NFS layer entirely. This catches EBADRPC and other NFS-level errors.
func TestMount_BuildMarkdownViaNFS(t *testing.T) {
	checkMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		return
	}
	defer func() { _ = filesystem.Close() }()

	time.Sleep(500 * time.Millisecond)

	buildPath := filepath.Join(mountpoint, ".build", "testapp")

	// Write "markdown\n" to .build/testapp — mimics: echo "markdown" > .build/testapp
	var writeErr error
	withGCDisabled(func() {
		writeErr = os.WriteFile(buildPath, []byte("markdown\n"), 0644)
	})
	require.NoError(t, writeErr, "os.WriteFile to .build/testapp should succeed")

	// Give NFS a moment to propagate
	time.Sleep(500 * time.Millisecond)

	// Verify the app was created by reading through the mount
	entries, err := os.ReadDir(mountpoint)
	require.NoError(t, err, "ReadDir mount root should succeed")

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	t.Logf("Mount root entries: %v", names)

	assert.Contains(t, names, "testapp", "root should contain synth view 'testapp'")
	assert.Contains(t, names, "_testapp", "root should contain backing table '_testapp'")
}

// ============================================================================
// Tests for hierarchical directories in synthesized apps (6.3)
// ============================================================================

// TestSynth_HierarchicalMkdir tests creating directories in a hierarchical synth view.
func TestSynth_HierarchicalMkdir(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build a markdown app (now includes filetype column)
	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed: %v", fsErr)

	// Create a directory
	fsErr = ops.Mkdir(ctx, "/memory/projects")
	require.Nil(t, fsErr, "Mkdir should succeed: %v", fsErr)

	// Verify it appears in listing
	entries, fsErr := ops.ReadDir(ctx, "/memory")
	require.Nil(t, fsErr, "ReadDir should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "projects", "root should show projects directory")

	// Verify it's a directory
	entry, fsErr := ops.Stat(ctx, "/memory/projects")
	require.Nil(t, fsErr, "Stat should succeed")
	assert.True(t, entry.IsDir, "projects should be a directory")
}

// TestSynth_HierarchicalMkdirNested tests creating nested directories with auto-created parents.
func TestSynth_HierarchicalMkdirNested(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a nested directory — should auto-create parent
	fsErr = ops.Mkdir(ctx, "/memory/projects/web")
	require.Nil(t, fsErr, "Mkdir nested should succeed: %v", fsErr)

	// Verify parent was auto-created
	entry, fsErr := ops.Stat(ctx, "/memory/projects")
	require.Nil(t, fsErr, "Stat parent should succeed")
	assert.True(t, entry.IsDir, "parent should be a directory")

	// Verify nested dir exists
	entry, fsErr = ops.Stat(ctx, "/memory/projects/web")
	require.Nil(t, fsErr, "Stat nested should succeed")
	assert.True(t, entry.IsDir, "nested should be a directory")

	// ReadDir on parent should show child
	entries, fsErr := ops.ReadDir(ctx, "/memory/projects")
	require.Nil(t, fsErr, "ReadDir parent should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "web", "projects/ should contain web")
}

// TestSynth_HierarchicalWriteFile tests writing files in subdirectories with auto-created parents.
func TestSynth_HierarchicalWriteFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Write a file in a subdirectory — should auto-create parent dirs
	content := "---\ntitle: Todo\nauthor: alice\n---\n\n# Todo\n\nFix bugs.\n"
	fsErr = ops.WriteFile(ctx, "/memory/projects/web/todo.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile nested should succeed: %v", fsErr)

	// Verify parent directories were auto-created
	entry, fsErr := ops.Stat(ctx, "/memory/projects")
	require.Nil(t, fsErr, "parent dir should exist")
	assert.True(t, entry.IsDir)

	entry, fsErr = ops.Stat(ctx, "/memory/projects/web")
	require.Nil(t, fsErr, "parent dir should exist")
	assert.True(t, entry.IsDir)

	// Verify the file exists and is readable
	readContent, fsErr := ops.ReadFile(ctx, "/memory/projects/web/todo.md")
	require.Nil(t, fsErr, "ReadFile should succeed")
	assert.Contains(t, string(readContent.Data), "title: Todo")
	assert.Contains(t, string(readContent.Data), "# Todo")
}

// TestSynth_HierarchicalReadDir tests listing directories at different levels.
func TestSynth_HierarchicalReadDir(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create structure: projects/web/todo.md, projects/web/notes.md, readme.md
	fsErr = ops.Mkdir(ctx, "/memory/projects/web")
	require.Nil(t, fsErr, "Mkdir should succeed")

	fsErr = ops.WriteFile(ctx, "/memory/projects/web/todo.md",
		[]byte("---\ntitle: Todo\n---\n\nContent.\n"))
	require.Nil(t, fsErr, "WriteFile todo should succeed")

	fsErr = ops.WriteFile(ctx, "/memory/projects/web/notes.md",
		[]byte("---\ntitle: Notes\n---\n\nNotes content.\n"))
	require.Nil(t, fsErr, "WriteFile notes should succeed")

	fsErr = ops.WriteFile(ctx, "/memory/readme.md",
		[]byte("---\ntitle: Readme\n---\n\nWelcome.\n"))
	require.Nil(t, fsErr, "WriteFile readme should succeed")

	// Root level: should show "projects" dir and "readme.md" file
	entries, fsErr := ops.ReadDir(ctx, "/memory")
	require.Nil(t, fsErr, "ReadDir root should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "projects")
	assert.Contains(t, names, "readme.md")
	assert.NotContains(t, names, "todo.md", "nested files should not appear at root")

	// projects/ should show "web" dir
	entries, fsErr = ops.ReadDir(ctx, "/memory/projects")
	require.Nil(t, fsErr, "ReadDir projects should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, "web")

	// projects/web/ should show todo.md and notes.md
	entries, fsErr = ops.ReadDir(ctx, "/memory/projects/web")
	require.Nil(t, fsErr, "ReadDir projects/web should succeed")
	names = fsEntryNames(entries)
	assert.Contains(t, names, "todo.md")
	assert.Contains(t, names, "notes.md")
}

// TestSynth_HierarchicalStatFile tests stat on nested files and directories.
func TestSynth_HierarchicalStatFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Write a nested file
	content := "---\ntitle: Deep File\n---\n\nDeep content.\n"
	fsErr = ops.WriteFile(ctx, "/memory/deep/nested/file.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile should succeed")

	// Stat the file
	entry, fsErr := ops.Stat(ctx, "/memory/deep/nested/file.md")
	require.Nil(t, fsErr, "Stat file should succeed")
	assert.False(t, entry.IsDir, "should be a file")
	assert.True(t, entry.Size > 0, "file should have content")

	// Stat the directory
	entry, fsErr = ops.Stat(ctx, "/memory/deep/nested")
	require.Nil(t, fsErr, "Stat dir should succeed")
	assert.True(t, entry.IsDir, "should be a directory")

	// Stat the parent directory
	entry, fsErr = ops.Stat(ctx, "/memory/deep")
	require.Nil(t, fsErr, "Stat parent dir should succeed")
	assert.True(t, entry.IsDir, "should be a directory")
}

// TestSynth_HierarchicalStatNotFound tests stat on non-existent paths in a hierarchical view.
// This covers the NFS stat amplification case (e.g., emacs probing foo.md~, .#foo.md, etc.)
// where negative caching prevents repeated DB queries for the same missing path.
func TestSynth_HierarchicalStatNotFound(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Write one real file so the view isn't empty
	content := "---\ntitle: Real File\n---\n\nReal content.\n"
	fsErr = ops.WriteFile(ctx, "/memory/real-file.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile should succeed")

	// Stat a non-existent file — should return not-found
	_, fsErr = ops.Stat(ctx, "/memory/nonexistent.md")
	require.NotNil(t, fsErr, "Stat non-existent file should fail")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should be not-found error")

	// Stat the same non-existent file again — should hit negative cache (no extra DB query)
	_, fsErr = ops.Stat(ctx, "/memory/nonexistent.md")
	require.NotNil(t, fsErr, "Second stat should also fail")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should still be not-found")

	// Stat different non-existent files (backup/probe names that don't start with '.',
	// since dot-prefix paths are parsed as control files by the path parser)
	for _, name := range []string{"real-file.md~", "real-file.bak", "nonexistent2.md"} {
		_, fsErr = ops.Stat(ctx, "/memory/"+name)
		require.NotNil(t, fsErr, "Stat %s should fail", name)
		assert.Equal(t, fs.ErrNotExist, fsErr.Code, "should be not-found for %s", name)
	}

	// The real file should still be stat-able
	entry, fsErr := ops.Stat(ctx, "/memory/real-file.md")
	require.Nil(t, fsErr, "Stat real file should succeed")
	assert.False(t, entry.IsDir, "should be a file")
	assert.True(t, entry.Size > 0, "file should have content")
}

// TestSynth_HierarchicalDeleteFile tests deleting a file in a subdirectory.
func TestSynth_HierarchicalDeleteFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a file in a directory
	fsErr = ops.WriteFile(ctx, "/memory/docs/readme.md",
		[]byte("---\ntitle: Readme\n---\n\nContent.\n"))
	require.Nil(t, fsErr, "WriteFile should succeed")

	// Delete the file
	fsErr = ops.Delete(ctx, "/memory/docs/readme.md")
	require.Nil(t, fsErr, "Delete should succeed")

	// File should be gone
	_, fsErr = ops.ReadFile(ctx, "/memory/docs/readme.md")
	require.NotNil(t, fsErr, "ReadFile should fail after delete")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)

	// Parent directory should still exist
	entry, fsErr := ops.Stat(ctx, "/memory/docs")
	require.Nil(t, fsErr, "parent dir should still exist")
	assert.True(t, entry.IsDir)
}

// TestSynth_HierarchicalRmdir tests deleting an empty directory.
func TestSynth_HierarchicalRmdir(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create an empty directory
	fsErr = ops.Mkdir(ctx, "/memory/empty-dir")
	require.Nil(t, fsErr, "Mkdir should succeed")

	// Delete the empty directory
	fsErr = ops.Delete(ctx, "/memory/empty-dir")
	require.Nil(t, fsErr, "Delete empty dir should succeed")

	// Directory should be gone
	_, fsErr = ops.Stat(ctx, "/memory/empty-dir")
	require.NotNil(t, fsErr, "Stat should fail after delete")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)
}

// TestSynth_HierarchicalRmdirNonEmpty tests that deleting a non-empty directory fails.
func TestSynth_HierarchicalRmdirNonEmpty(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a directory with a file in it
	fsErr = ops.WriteFile(ctx, "/memory/docs/readme.md",
		[]byte("---\ntitle: Readme\n---\n\nContent.\n"))
	require.Nil(t, fsErr, "WriteFile should succeed")

	// Attempt to delete non-empty directory should fail
	fsErr = ops.Delete(ctx, "/memory/docs")
	require.NotNil(t, fsErr, "Delete non-empty dir should fail")
	assert.Equal(t, fs.ErrNotEmpty, fsErr.Code, "should be ENOTEMPTY error")
}

// TestSynth_HierarchicalRenameFile tests renaming a file within a hierarchical view.
func TestSynth_HierarchicalRenameFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a file in a directory
	fsErr = ops.WriteFile(ctx, "/memory/docs/old-name.md",
		[]byte("---\ntitle: My Doc\n---\n\nContent here.\n"))
	require.Nil(t, fsErr, "WriteFile should succeed")

	// Rename the file
	fsErr = ops.Rename(ctx, "/memory/docs/old-name.md", "/memory/docs/new-name.md")
	require.Nil(t, fsErr, "Rename should succeed")

	// Old path should not exist
	_, fsErr = ops.ReadFile(ctx, "/memory/docs/old-name.md")
	require.NotNil(t, fsErr, "old path should not exist")
	assert.Equal(t, fs.ErrNotExist, fsErr.Code)

	// New path should be readable
	content, fsErr := ops.ReadFile(ctx, "/memory/docs/new-name.md")
	require.Nil(t, fsErr, "new path should be readable")
	assert.Contains(t, string(content.Data), "title: My Doc")
}

// TestSynth_HierarchicalRenameDir tests renaming a directory (atomic prefix swap).
func TestSynth_HierarchicalRenameDir(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a directory with files
	fsErr = ops.WriteFile(ctx, "/memory/old-dir/file1.md",
		[]byte("---\ntitle: File One\n---\n\nContent 1.\n"))
	require.Nil(t, fsErr, "WriteFile file1 should succeed")

	fsErr = ops.WriteFile(ctx, "/memory/old-dir/file2.md",
		[]byte("---\ntitle: File Two\n---\n\nContent 2.\n"))
	require.Nil(t, fsErr, "WriteFile file2 should succeed")

	// Rename the directory
	fsErr = ops.Rename(ctx, "/memory/old-dir", "/memory/new-dir")
	require.Nil(t, fsErr, "Rename dir should succeed: %v", fsErr)

	// Old path should not exist
	_, fsErr = ops.Stat(ctx, "/memory/old-dir")
	require.NotNil(t, fsErr, "old dir should not exist")

	// New path should be a directory
	entry, fsErr := ops.Stat(ctx, "/memory/new-dir")
	require.Nil(t, fsErr, "new dir should exist")
	assert.True(t, entry.IsDir)

	// Files should be accessible under new path
	content, fsErr := ops.ReadFile(ctx, "/memory/new-dir/file1.md")
	require.Nil(t, fsErr, "file1 should be readable under new dir")
	assert.Contains(t, string(content.Data), "title: File One")

	content, fsErr = ops.ReadFile(ctx, "/memory/new-dir/file2.md")
	require.Nil(t, fsErr, "file2 should be readable under new dir")
	assert.Contains(t, string(content.Data), "title: File Two")
}

// TestSynth_DeeplyNested tests 4+ level deep paths.
func TestSynth_DeeplyNested(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Write a deeply nested file — auto-creates all parent directories
	content := "---\ntitle: Deep File\n---\n\nVery deep content.\n"
	fsErr = ops.WriteFile(ctx, "/memory/a/b/c/d/deep.md", []byte(content))
	require.Nil(t, fsErr, "WriteFile deeply nested should succeed: %v", fsErr)

	// Verify all intermediate directories exist
	for _, dir := range []string{"/memory/a", "/memory/a/b", "/memory/a/b/c", "/memory/a/b/c/d"} {
		entry, fsErr := ops.Stat(ctx, dir)
		require.Nil(t, fsErr, "Stat %s should succeed", dir)
		assert.True(t, entry.IsDir, "%s should be a directory", dir)
	}

	// Verify the file is readable
	readContent, fsErr := ops.ReadFile(ctx, "/memory/a/b/c/d/deep.md")
	require.Nil(t, fsErr, "ReadFile deeply nested should succeed")
	assert.Contains(t, string(readContent.Data), "title: Deep File")

	// ReadDir at each level should show correct children
	entries, fsErr := ops.ReadDir(ctx, "/memory/a")
	require.Nil(t, fsErr)
	assert.Equal(t, 1, len(entries), "a/ should have 1 entry")
	assert.Equal(t, "b", entries[0].Name)

	entries, fsErr = ops.ReadDir(ctx, "/memory/a/b/c/d")
	require.Nil(t, fsErr)
	assert.Equal(t, 1, len(entries), "d/ should have 1 entry")
	assert.Equal(t, "deep.md", entries[0].Name)
}

// TestSynth_MixedFlatAndHierarchical tests that root level has both flat files and directories.
func TestSynth_MixedFlatAndHierarchical(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a flat file
	fsErr = ops.WriteFile(ctx, "/memory/flat-file.md",
		[]byte("---\ntitle: Flat\n---\n\nFlat content.\n"))
	require.Nil(t, fsErr, "WriteFile flat should succeed")

	// Create a nested file (creates directory)
	fsErr = ops.WriteFile(ctx, "/memory/subdir/nested.md",
		[]byte("---\ntitle: Nested\n---\n\nNested content.\n"))
	require.Nil(t, fsErr, "WriteFile nested should succeed")

	// Root should show both flat file and directory
	entries, fsErr := ops.ReadDir(ctx, "/memory")
	require.Nil(t, fsErr, "ReadDir should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "flat-file.md", "should have flat file")
	assert.Contains(t, names, "subdir", "should have subdir directory")

	// Verify types
	for _, e := range entries {
		if e.Name == "flat-file.md" {
			assert.False(t, e.IsDir, "flat-file.md should be a file")
		}
		if e.Name == "subdir" {
			assert.True(t, e.IsDir, "subdir should be a directory")
		}
	}
}

// TestSynth_HierarchicalPlainText tests hierarchy with plain text format.
func TestSynth_HierarchicalPlainText(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build a plain text app
	fsErr := ops.WriteFile(ctx, "/.build/notes", []byte("txt\n"))
	require.Nil(t, fsErr, "build should succeed: %v", fsErr)

	// Create a nested file
	fsErr = ops.WriteFile(ctx, "/notes/work/meeting.txt", []byte("Discuss quarterly goals.\n"))
	require.Nil(t, fsErr, "WriteFile nested txt should succeed: %v", fsErr)

	// Verify parent directory was auto-created
	entry, fsErr := ops.Stat(ctx, "/notes/work")
	require.Nil(t, fsErr, "Stat parent dir should succeed")
	assert.True(t, entry.IsDir)

	// Verify the file is readable
	content, fsErr := ops.ReadFile(ctx, "/notes/work/meeting.txt")
	require.Nil(t, fsErr, "ReadFile should succeed")
	assert.Equal(t, "Discuss quarterly goals.\n", string(content.Data))

	// ReadDir should show the file
	entries, fsErr := ops.ReadDir(ctx, "/notes/work")
	require.Nil(t, fsErr, "ReadDir should succeed")
	names := fsEntryNames(entries)
	assert.Contains(t, names, "meeting.txt")
}

// TestSynth_MkdirAlreadyExists tests that creating an existing directory fails.
func TestSynth_MkdirAlreadyExists(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/memory", []byte("markdown\n"))
	require.Nil(t, fsErr, "build should succeed")

	// Create a directory
	fsErr = ops.Mkdir(ctx, "/memory/projects")
	require.Nil(t, fsErr, "first Mkdir should succeed")

	// Creating the same directory again should fail
	fsErr = ops.Mkdir(ctx, "/memory/projects")
	require.NotNil(t, fsErr, "second Mkdir should fail")
	assert.Equal(t, fs.ErrExists, fsErr.Code, "should be EEXIST error")
}

// ============================================================================
// Helpers
// ============================================================================

// fsEntryNames extracts entry names from a slice of fs.Entry.
func fsEntryNames(entries []fs.Entry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// execSQL executes a SQL statement against the test database.
func execSQL(t *testing.T, connStr, sql string) error {
	t.Helper()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return err
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, sql)
	return err
}
