// Package fs tests for types.go
package fs

import (
	"os"
	"testing"
	"time"
)

// TestEntry verifies Entry struct fields and basic usage.
func TestEntry(t *testing.T) {
	now := time.Now()
	entry := Entry{
		Name:    "users",
		IsDir:   true,
		Size:    4096,
		Mode:    os.ModeDir | 0755,
		ModTime: now,
	}

	if entry.Name != "users" {
		t.Errorf("Name = %q, want %q", entry.Name, "users")
	}
	if !entry.IsDir {
		t.Error("IsDir = false, want true")
	}
	if entry.Size != 4096 {
		t.Errorf("Size = %d, want %d", entry.Size, 4096)
	}
	if entry.Mode != os.ModeDir|0755 {
		t.Errorf("Mode = %v, want %v", entry.Mode, os.ModeDir|0755)
	}
	if !entry.ModTime.Equal(now) {
		t.Errorf("ModTime = %v, want %v", entry.ModTime, now)
	}
}

// TestFileContent verifies FileContent struct fields.
func TestFileContent(t *testing.T) {
	data := []byte(`{"id": 1, "name": "test"}`)
	content := FileContent{
		Data: data,
		Size: int64(len(data)),
		Mode: 0644,
	}

	if string(content.Data) != `{"id": 1, "name": "test"}` {
		t.Errorf("Data = %q, want JSON content", string(content.Data))
	}
	if content.Size != 25 {
		t.Errorf("Size = %d, want %d", content.Size, 25)
	}
	if content.Mode != 0644 {
		t.Errorf("Mode = %v, want %v", content.Mode, os.FileMode(0644))
	}
}

// TestEntry_IsSymlink verifies symlink detection via Mode bits.
func TestEntry_IsSymlink(t *testing.T) {
	tests := []struct {
		name     string
		entry    Entry
		expected bool
	}{
		{
			name:     "regular file",
			entry:    Entry{Name: "file.txt", Mode: 0644},
			expected: false,
		},
		{
			name:     "directory",
			entry:    Entry{Name: "dir", IsDir: true, Mode: os.ModeDir | 0755},
			expected: false,
		},
		{
			name:     "symlink",
			entry:    Entry{Name: "link", Mode: os.ModeSymlink | 0777, Target: "/dev/null"},
			expected: true,
		},
		{
			name:     "symlink with zero perms",
			entry:    Entry{Name: "link", Mode: os.ModeSymlink},
			expected: true,
		},
		{
			name:     "zero mode (regular file)",
			entry:    Entry{Name: "file"},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.entry.IsSymlink()
			if got != tt.expected {
				t.Errorf("IsSymlink() = %v, want %v (Mode=%v)", got, tt.expected, tt.entry.Mode)
			}
		})
	}
}

// TestEntry_SymlinkFields verifies Target field on symlink entries.
func TestEntry_SymlinkFields(t *testing.T) {
	entry := Entry{
		Name:   "before",
		Mode:   os.ModeSymlink | 0777,
		Target: "../../.history/docs/hello.md/2026-04-07T143000.123Z-abc",
		Size:   53,
	}
	if !entry.IsSymlink() {
		t.Fatal("entry should be a symlink")
	}
	if entry.Target == "" {
		t.Fatal("symlink target should not be empty")
	}
	if entry.Size != 53 {
		t.Errorf("Size = %d, want %d (length of target path)", entry.Size, 53)
	}
}

// TestEntry_IsSymlink_IsDirTakesPrecedence verifies that IsDir=true takes
// precedence in EntryToAttr even if ModeSymlink is also set (invalid state).
func TestEntry_IsSymlink_IsDirTakesPrecedence(t *testing.T) {
	// This is an invalid combination -- IsDir and ModeSymlink should not both be set.
	// IsSymlink() still returns true (it only checks Mode bits), but code that
	// checks IsDir first (like EntryToAttr) will treat it as a directory.
	entry := Entry{
		Name:  "weird",
		IsDir: true,
		Mode:  os.ModeDir | os.ModeSymlink | 0755,
	}
	// IsSymlink checks Mode only -- it doesn't know about IsDir
	if !entry.IsSymlink() {
		t.Error("IsSymlink() should be true when ModeSymlink is set, regardless of IsDir")
	}
	// But IsDir is also true -- callers that check IsDir first will treat as directory
	if !entry.IsDir {
		t.Error("IsDir should be true")
	}
}

// TestEntry_Symlink_EmptyTarget verifies behavior when ModeSymlink is set but
// Target is empty.
func TestEntry_Symlink_EmptyTarget(t *testing.T) {
	entry := Entry{
		Name:   "dangling",
		Mode:   os.ModeSymlink | 0777,
		Target: "",
	}
	if !entry.IsSymlink() {
		t.Error("should be a symlink even with empty target")
	}
	if entry.Target != "" {
		t.Error("target should be empty")
	}
}

// TestEntry_Symlink_SizeMatchesTarget verifies the convention that symlink
// Size should equal len(Target).
func TestEntry_Symlink_SizeMatchesTarget(t *testing.T) {
	target := "../../.history/docs/hello.md/2026-04-07T143000.123Z-abc"
	entry := Entry{
		Name:   "before",
		Mode:   os.ModeSymlink | 0777,
		Target: target,
		Size:   int64(len(target)),
	}
	if entry.Size != int64(len(entry.Target)) {
		t.Errorf("Size=%d != len(Target)=%d; NFS clients may use Size for buffer allocation",
			entry.Size, len(entry.Target))
	}
}

// TestWriteHandle verifies WriteHandle struct and OnClose callback.
func TestWriteHandle(t *testing.T) {
	var closedWith []byte
	handle := WriteHandle{
		Path:   "/users/new.json",
		Buffer: []byte(`{"name": "new"}`),
		OnClose: func(data []byte) error {
			closedWith = data
			return nil
		},
	}

	if handle.Path != "/users/new.json" {
		t.Errorf("Path = %q, want %q", handle.Path, "/users/new.json")
	}
	if string(handle.Buffer) != `{"name": "new"}` {
		t.Errorf("Buffer = %q, want JSON content", string(handle.Buffer))
	}

	// Test OnClose callback
	err := handle.OnClose(handle.Buffer)
	if err != nil {
		t.Errorf("OnClose() error = %v, want nil", err)
	}
	if string(closedWith) != `{"name": "new"}` {
		t.Errorf("OnClose received %q, want %q", string(closedWith), `{"name": "new"}`)
	}
}
