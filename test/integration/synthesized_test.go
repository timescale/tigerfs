package integration

import (
	"context"
	"strings"
	"testing"

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
		('hello-world', 'Hello World', 'alice', '# Hello World

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
		('hello-world', 'Hello World', 'alice', '# Hello

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
		('hello-world', 'Hello World', 'alice', '# Hello

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
		('hello', 'Hello, world!
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
		('hello-world', 'Hello World', 'alice', '# Hello

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
		('first-post', 'First Post', 'alice', 'Body of first post.
'),
		('second-post', 'Second Post', 'bob', 'Body of second post.
'),
		('third-post', 'Third Post', 'charlie', 'Body of third post.
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
		('hello-world', 'Hello World', 'alice', '# Hello World

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

	insertSQL := `INSERT INTO _snippets (filename, body) VALUES ('scratch', 'Some scratch notes.
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

// TestSynth_RenameWithoutExtension tests renaming using paths without extension.
// mv posts/hello-world posts/new-name → normalization handles missing extension
func TestSynth_RenameWithoutExtension(t *testing.T) {
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
		('hello-world', 'Hello World', 'alice', 'Body content.
')`
	require.NoError(t, execSQL(t, result.ConnStr, insertSQL))

	// Rename without extensions
	fsErr = ops.Rename(ctx, "/posts/hello-world", "/posts/new-name")
	require.Nil(t, fsErr, "Rename without extension should succeed")

	// Old path gone (with or without extension)
	_, fsErr = ops.ReadFile(ctx, "/posts/hello-world.md")
	require.NotNil(t, fsErr, "old path should not exist after rename")

	// New path works (with extension)
	content, fsErr := ops.ReadFile(ctx, "/posts/new-name.md")
	require.Nil(t, fsErr, "new path should be readable")
	assert.Contains(t, string(content.Data), "title: Hello World")
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
