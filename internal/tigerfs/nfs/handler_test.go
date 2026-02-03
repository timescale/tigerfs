package nfs

import (
	"strings"
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nfs "github.com/willscott/go-nfs"
)

// splitPath converts a path string to path components.
func splitPath(path string) []string {
	if path == "/" || path == "" {
		return []string{}
	}
	// Remove leading slash and split
	cleanPath := strings.TrimPrefix(path, "/")
	if cleanPath == "" {
		return []string{}
	}
	return strings.Split(cleanPath, "/")
}

// TestToHandle_4ByteAlignment verifies all handles are 4-byte aligned.
// macOS NFS client requires 4-byte aligned handles.
func TestToHandle_4ByteAlignment(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	testPaths := []string{
		"/",                           // 1 char
		"/a",                          // 2 chars
		"/ab",                         // 3 chars
		"/abc",                        // 4 chars
		"/users",                      // 6 chars
		"/categories",                 // 11 chars
		"/categories/slug",            // 16 chars
		"/categories/automotive",      // 22 chars
		"/" + strings.Repeat("a", 62), // max direct path (63 bytes total with /)
		// Long paths that trigger hash fallback (version 2)
		"/" + strings.Repeat("a", 100),                                                     // 101 chars - uses hash
		"/orders/.by/product_id/100/.filter/id/019c2560-909a-7c65-b660-88037c7d86fb",       // 74 chars - uses hash
		"/very/long/path/that/exceeds/the/maximum/direct/encoding/limit/of/sixtyfour/bytes", // >64 chars - uses hash
	}

	for _, path := range testPaths {
		t.Run(path, func(t *testing.T) {
			parts := splitPath(path)
			handle := h.ToHandle(fs, parts)

			if len(handle)%4 != 0 {
				t.Errorf("handle for %q has length %d (not 4-byte aligned)", path, len(handle))
			}
			if len(handle) < minHandleSize {
				t.Errorf("handle for %q has length %d (less than min %d)", path, len(handle), minHandleSize)
			}
		})
	}
}

// TestToHandle_FromHandle_Roundtrip verifies handles can be decoded back to original paths.
func TestToHandle_FromHandle_Roundtrip(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	testPaths := []string{
		"/",
		"/users",
		"/users/1",
		"/categories/automotive",
		"/table/.info/count",
		"/table/.export/csv",
		"/public/users/row1/column",
	}

	for _, path := range testPaths {
		t.Run(path, func(t *testing.T) {
			parts := splitPath(path)
			handle := h.ToHandle(fs, parts)

			resultFS, decoded, err := h.FromHandle(handle)
			require.NoError(t, err)
			require.NotNil(t, resultFS)

			// Rejoin and compare
			var rejoined string
			if len(decoded) == 0 {
				rejoined = "/"
			} else {
				rejoined = "/" + strings.Join(decoded, "/")
			}
			assert.Equal(t, path, rejoined)
		})
	}
}

// TestToHandle_LongPath_UsesHashFallback verifies long paths use hash-based handles.
func TestToHandle_LongPath_UsesHashFallback(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	// Path longer than 63 bytes should use hash fallback
	longPath := "/" + strings.Repeat("a", 100)
	parts := splitPath(longPath)
	handle := h.ToHandle(fs, parts)

	assert.Equal(t, byte(handleVersionHash), handle[0], "long path should use hash version")
	// Hash handles: 1 version byte + 32 hash bytes + 3 padding = 36 bytes (4-byte aligned)
	// macOS NFS client requires handles to be 4-byte aligned
	assert.Equal(t, 36, len(handle), "hash handle should be 36 bytes (1 version + 32 hash + 3 padding)")
	assert.Equal(t, 0, len(handle)%4, "hash handle should be 4-byte aligned")
}

// TestFromHandle_InvalidHandle_ReturnsStale verifies invalid handles return NFSStatusStale.
func TestFromHandle_InvalidHandle_ReturnsStale(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	tests := []struct {
		name   string
		handle []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{1}},
		{"invalid version", []byte{99, 'a', 'b', 'c'}},
		{"hash not in cache", append([]byte{handleVersionHash}, make([]byte, 32)...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := h.FromHandle(tt.handle)
			require.Error(t, err)
			var nfsErr *nfs.NFSStatusError
			require.ErrorAs(t, err, &nfsErr)
			assert.Equal(t, nfs.NFSStatusStale, nfsErr.NFSStatus)
		})
	}
}

// TestToHandle_MinimumSize verifies handles meet minimum size requirements.
func TestToHandle_MinimumSize(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	// Even the smallest path should produce a handle >= minHandleSize
	handle := h.ToHandle(fs, []string{})
	assert.GreaterOrEqual(t, len(handle), minHandleSize, "root handle should be at least %d bytes", minHandleSize)

	handle = h.ToHandle(fs, []string{"a"})
	assert.GreaterOrEqual(t, len(handle), minHandleSize, "single-char path handle should be at least %d bytes", minHandleSize)
}

// TestToHandle_Deterministic verifies the same path always produces the same handle.
func TestToHandle_Deterministic(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	paths := []string{"/users", "/users/1", "/table/.info/count"}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			parts := splitPath(path)
			handle1 := h.ToHandle(fs, parts)
			handle2 := h.ToHandle(fs, parts)
			assert.Equal(t, handle1, handle2, "handles should be deterministic")
		})
	}
}

// TestInvalidateHandle verifies handles can be invalidated from the cache.
func TestInvalidateHandle(t *testing.T) {
	fs := memfs.New()
	h := NewStableHandler(fs)

	// Create a long path that uses hash fallback
	longPath := "/" + strings.Repeat("x", 100)
	parts := splitPath(longPath)
	handle := h.ToHandle(fs, parts)

	// Verify it works before invalidation
	_, decoded, err := h.FromHandle(handle)
	require.NoError(t, err)
	rejoined := "/" + strings.Join(decoded, "/")
	assert.Equal(t, longPath, rejoined)

	// Invalidate and verify it's gone
	err = h.InvalidateHandle(fs, handle)
	require.NoError(t, err)

	_, _, err = h.FromHandle(handle)
	require.Error(t, err)
	var nfsErr *nfs.NFSStatusError
	require.ErrorAs(t, err, &nfsErr)
	assert.Equal(t, nfs.NFSStatusStale, nfsErr.NFSStatus)
}
