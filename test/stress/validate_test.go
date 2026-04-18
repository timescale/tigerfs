package main

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTestDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for relPath, content := range files {
		fullPath := filepath.Join(dir, relPath)
		os.MkdirAll(filepath.Dir(fullPath), 0755)
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("setup file %s: %v", relPath, err)
		}
	}
	return dir
}

func TestValidateWorkspace_Passing(t *testing.T) {
	files := map[string]string{
		"hello.md":      "# Hello\n",
		"docs/intro.md": "# Intro\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	for relPath, content := range files {
		expected.SetFile(relPath, HashContent([]byte(content)))
	}

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("ValidateWorkspace should pass: %v", err)
	}
}

func TestValidateWorkspace_MissingFile(t *testing.T) {
	files := map[string]string{
		"hello.md": "# Hello\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello\n")))
	expected.SetFile("missing.md", HashContent([]byte("gone")))

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail for missing file")
	}
}

func TestValidateWorkspace_UnexpectedFile(t *testing.T) {
	files := map[string]string{
		"hello.md": "# Hello\n",
		"extra.md": "# Extra\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello\n")))
	// extra.md not in expected

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail for unexpected file")
	}
}

func TestValidateWorkspace_HashMismatch(t *testing.T) {
	files := map[string]string{
		"hello.md": "# Hello v2\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello v1\n"))) // wrong hash

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail for hash mismatch")
	}
}

func TestValidateWorkspace_SkipsDotfiles(t *testing.T) {
	dir := t.TempDir()

	// Create a regular file
	os.WriteFile(filepath.Join(dir, "hello.md"), []byte("# Hello\n"), 0644)

	// Create dotfile and dot-directory (should be skipped)
	os.WriteFile(filepath.Join(dir, ".hidden"), []byte("secret"), 0644)
	os.MkdirAll(filepath.Join(dir, ".git", "objects"), 0755)
	os.WriteFile(filepath.Join(dir, ".git", "config"), []byte("[core]"), 0644)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello\n")))

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("ValidateWorkspace should skip dotfiles: %v", err)
	}
}

func TestValidateWorkspace_EmptyWorkspace(t *testing.T) {
	dir := t.TempDir()
	expected := NewWorkspaceState()

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("empty workspace should validate: %v", err)
	}
}

func TestValidateWorkspace_NestedDirs(t *testing.T) {
	files := map[string]string{
		"a/b/c/deep.md": "deep content",
		"a/top.md":      "top content",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	for relPath, content := range files {
		expected.SetFile(relPath, HashContent([]byte(content)))
	}

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("nested dirs should validate: %v", err)
	}
}

func TestSnapshotHash_Deterministic(t *testing.T) {
	files := map[string]string{
		"a.md":     "content a",
		"b.md":     "content b",
		"dir/c.md": "content c",
	}
	dir := setupTestDir(t, files)

	h1, err := SnapshotHash(dir)
	if err != nil {
		t.Fatalf("SnapshotHash: %v", err)
	}

	h2, err := SnapshotHash(dir)
	if err != nil {
		t.Fatalf("SnapshotHash: %v", err)
	}

	if h1 != h2 {
		t.Error("same workspace should produce same snapshot hash")
	}
}

func TestSnapshotHash_DifferentContent(t *testing.T) {
	dir1 := setupTestDir(t, map[string]string{"a.md": "v1"})
	dir2 := setupTestDir(t, map[string]string{"a.md": "v2"})

	h1, _ := SnapshotHash(dir1)
	h2, _ := SnapshotHash(dir2)

	if h1 == h2 {
		t.Error("different content should produce different snapshot hash")
	}
}

func TestSnapshotHash_SkipsDotfiles(t *testing.T) {
	dir1 := setupTestDir(t, map[string]string{"a.md": "content"})

	// dir2 has same file plus a dotfile
	dir2 := setupTestDir(t, map[string]string{"a.md": "content"})
	os.WriteFile(filepath.Join(dir2, ".hidden"), []byte("secret"), 0644)

	h1, _ := SnapshotHash(dir1)
	h2, _ := SnapshotHash(dir2)

	if h1 != h2 {
		t.Error("dotfiles should be excluded from snapshot hash")
	}
}
