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
