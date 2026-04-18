package main

import (
	"testing"
)

func TestDeepCopy_Independent(t *testing.T) {
	orig := NewWorkspaceState()
	orig.SetFile("a.md", "hash1")
	orig.AddDir("docs")

	clone := orig.DeepCopy()

	// Modify clone
	clone.SetFile("b.md", "hash2")
	clone.AddDir("notes")
	clone.RemoveFile("a.md")

	// Original should be unchanged
	if _, ok := orig.Files["a.md"]; !ok {
		t.Error("original lost a.md after modifying clone")
	}
	if _, ok := orig.Files["b.md"]; ok {
		t.Error("original gained b.md from clone modification")
	}
	if _, ok := orig.Dirs["notes"]; ok {
		t.Error("original gained notes/ from clone modification")
	}
}

func TestDeepCopy_Empty(t *testing.T) {
	orig := NewWorkspaceState()
	clone := orig.DeepCopy()

	clone.SetFile("x.md", "hash")
	if len(orig.Files) != 0 {
		t.Error("empty original affected by clone")
	}
}

func TestSetFile_RemoveFile(t *testing.T) {
	ws := NewWorkspaceState()
	ws.SetFile("docs/hello.md", "abc123")

	if ws.Files["docs/hello.md"] != "abc123" {
		t.Error("SetFile didn't store hash")
	}

	ws.RemoveFile("docs/hello.md")
	if _, ok := ws.Files["docs/hello.md"]; ok {
		t.Error("RemoveFile didn't remove file")
	}
}

func TestRenameFile(t *testing.T) {
	ws := NewWorkspaceState()
	ws.SetFile("old.md", "hash1")

	ws.RenameFile("old.md", "new.md")

	if _, ok := ws.Files["old.md"]; ok {
		t.Error("old path still exists after rename")
	}
	if ws.Files["new.md"] != "hash1" {
		t.Error("new path has wrong hash")
	}
}

func TestRenameDir(t *testing.T) {
	ws := NewWorkspaceState()
	ws.AddDir("docs")
	ws.AddDir("docs/sub")
	ws.SetFile("docs/a.md", "h1")
	ws.SetFile("docs/sub/b.md", "h2")
	ws.SetFile("other.md", "h3")

	ws.RenameDir("docs", "notes")

	if _, ok := ws.Dirs["docs"]; ok {
		t.Error("old dir still exists")
	}
	if !ws.Dirs["notes"] {
		t.Error("new dir not created")
	}
	if !ws.Dirs["notes/sub"] {
		t.Error("subdirectory not moved")
	}
	if ws.Files["notes/a.md"] != "h1" {
		t.Error("file not moved to new dir")
	}
	if ws.Files["notes/sub/b.md"] != "h2" {
		t.Error("nested file not moved")
	}
	if ws.Files["other.md"] != "h3" {
		t.Error("unrelated file affected")
	}
}

func TestRemoveDir_CascadesContents(t *testing.T) {
	ws := NewWorkspaceState()
	ws.AddDir("docs")
	ws.AddDir("docs/sub")
	ws.SetFile("docs/a.md", "h1")
	ws.SetFile("docs/sub/b.md", "h2")
	ws.SetFile("root.md", "h3")

	ws.RemoveDir("docs")

	if _, ok := ws.Dirs["docs"]; ok {
		t.Error("dir not removed")
	}
	if _, ok := ws.Dirs["docs/sub"]; ok {
		t.Error("subdir not removed")
	}
	if _, ok := ws.Files["docs/a.md"]; ok {
		t.Error("file in dir not removed")
	}
	if _, ok := ws.Files["docs/sub/b.md"]; ok {
		t.Error("file in subdir not removed")
	}
	if ws.Files["root.md"] != "h3" {
		t.Error("unrelated file affected")
	}
}

func TestFileCount(t *testing.T) {
	ws := NewWorkspaceState()
	ws.SetFile("a.md", "h1")
	ws.SetFile("b.md", "h2")
	ws.SetFile("docs/c.md", "h3")
	ws.AddDir("docs")

	if got := ws.FileCount(""); got != 2 {
		t.Errorf("root FileCount = %d, want 2", got)
	}
	if got := ws.FileCount("docs"); got != 1 {
		t.Errorf("docs FileCount = %d, want 1", got)
	}
}

func TestSubdirCount(t *testing.T) {
	ws := NewWorkspaceState()
	ws.AddDir("docs")
	ws.AddDir("notes")
	ws.AddDir("docs/sub")

	if got := ws.SubdirCount(""); got != 2 {
		t.Errorf("root SubdirCount = %d, want 2", got)
	}
	if got := ws.SubdirCount("docs"); got != 1 {
		t.Errorf("docs SubdirCount = %d, want 1", got)
	}
}

func TestStackPushPop(t *testing.T) {
	stack := NewStateStack()

	ws1 := NewWorkspaceState()
	ws1.SetFile("a.md", "h1")
	stack.Push(ws1, 0)

	ws2 := NewWorkspaceState()
	ws2.SetFile("a.md", "h1")
	ws2.SetFile("b.md", "h2")
	stack.Push(ws2, 1)

	if stack.Len() != 2 {
		t.Errorf("stack len = %d, want 2", stack.Len())
	}

	popped := stack.Pop()
	if popped == nil {
		t.Fatal("Pop returned nil")
	}
	if len(popped.Files) != 2 {
		t.Errorf("popped state has %d files, want 2", len(popped.Files))
	}

	if stack.Len() != 1 {
		t.Errorf("stack len after pop = %d, want 1", stack.Len())
	}

	popped2 := stack.Pop()
	if len(popped2.Files) != 1 {
		t.Errorf("second pop has %d files, want 1", len(popped2.Files))
	}

	if stack.Pop() != nil {
		t.Error("Pop on empty stack should return nil")
	}
}

func TestStackSavepoint(t *testing.T) {
	stack := NewStateStack()

	// Push 3 states, savepoint after state 1
	ws0 := NewWorkspaceState()
	stack.Push(ws0, 0)

	ws1 := NewWorkspaceState()
	ws1.SetFile("a.md", "h1")
	stack.Push(ws1, 1)
	stack.SaveSavepoint("sp1")

	ws2 := NewWorkspaceState()
	ws2.SetFile("a.md", "h1")
	ws2.SetFile("b.md", "h2")
	stack.Push(ws2, 2)

	ws3 := NewWorkspaceState()
	ws3.SetFile("a.md", "h1")
	ws3.SetFile("b.md", "h2")
	ws3.SetFile("c.md", "h3")
	stack.Push(ws3, 3)

	if stack.Len() != 4 {
		t.Fatalf("stack len = %d, want 4", stack.Len())
	}

	// Restore to savepoint (should go back to state after push index 1)
	restored := stack.RestoreToSavepoint("sp1")
	if restored == nil {
		t.Fatal("RestoreToSavepoint returned nil")
	}
	if len(restored.Files) != 1 {
		t.Errorf("restored state has %d files, want 1", len(restored.Files))
	}
	if restored.Files["a.md"] != "h1" {
		t.Error("restored state has wrong content")
	}
	if stack.Len() != 2 {
		t.Errorf("stack len after restore = %d, want 2", stack.Len())
	}
}

func TestStackSavepoint_NotFound(t *testing.T) {
	stack := NewStateStack()
	if stack.RestoreToSavepoint("nonexistent") != nil {
		t.Error("RestoreToSavepoint should return nil for unknown savepoint")
	}
}

func TestStackMostRecentSavepoint(t *testing.T) {
	stack := NewStateStack()

	if stack.MostRecentSavepoint() != "" {
		t.Error("MostRecentSavepoint should be empty for new stack")
	}

	ws := NewWorkspaceState()
	stack.Push(ws, 0)
	stack.SaveSavepoint("early")

	stack.Push(ws, 1)
	stack.Push(ws, 2)
	stack.SaveSavepoint("late")

	if got := stack.MostRecentSavepoint(); got != "late" {
		t.Errorf("MostRecentSavepoint = %q, want %q", got, "late")
	}
}

func TestStackRestoreToIndex(t *testing.T) {
	stack := NewStateStack()

	for i := 0; i < 5; i++ {
		ws := NewWorkspaceState()
		ws.SetFile("file.md", HashContent([]byte(string(rune('a'+i)))))
		stack.Push(ws, i)
	}

	restored := stack.RestoreToIndex(2)
	if restored == nil {
		t.Fatal("RestoreToIndex returned nil")
	}
	if stack.Len() != 3 {
		t.Errorf("stack len after RestoreToIndex(2) = %d, want 3", stack.Len())
	}
}

func TestHashContent(t *testing.T) {
	h1 := HashContent([]byte("hello"))
	h2 := HashContent([]byte("hello"))
	h3 := HashContent([]byte("world"))

	if h1 != h2 {
		t.Error("same content should produce same hash")
	}
	if h1 == h3 {
		t.Error("different content should produce different hash")
	}
	if len(h1) != 32 {
		t.Errorf("hash length = %d, want 32 (hex md5)", len(h1))
	}
}
