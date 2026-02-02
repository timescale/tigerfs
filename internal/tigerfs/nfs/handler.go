package nfs

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
	"go.uber.org/zap"
)

// StableHandler implements nfs.Handler with stateless file handles.
// Instead of using an LRU cache with UUIDs (which causes stale handles when
// the cache overflows), this handler encodes the path directly into the handle.
//
// It embeds nfs.Handler (via NullAuthHandler) for proper delegation of
// Mount, Change, and FSStat methods, only overriding handle-related methods.
//
// Handle format:
//   - Byte 0: format version (1 = direct path, 2 = hash-based fallback)
//   - Version 1: path stored directly (for paths that fit in 63 bytes)
//   - Version 2: SHA-256 hash of path (requires cache lookup, but rare)
//
// This ensures handles are stable and don't depend on cache size for common cases.
type StableHandler struct {
	nfs.Handler                  // Embed NullAuthHandler for Mount, Change, FSStat delegation
	fs          billy.Filesystem // Keep reference for FromHandle

	// Fallback cache for paths too long to encode directly.
	// This should be rare - most paths are short enough.
	mu       sync.RWMutex
	fallback map[[32]byte]string
}

// NewStableHandler creates a handler with stateless file handles.
// It wraps a NullAuthHandler for proper delegation of base handler methods.
func NewStableHandler(fs billy.Filesystem) *StableHandler {
	return &StableHandler{
		Handler:  nfshelper.NewNullAuthHandler(fs),
		fs:       fs, // Store reference for FromHandle
		fallback: make(map[[32]byte]string),
	}
}

const (
	handleVersionCompressed = 1
	handleVersionHash       = 2
	maxHandleSize           = 64 // NFS v3 max handle size
	minHandleSize           = 16 // Minimum size for compatibility (macOS expects >= 16 bytes)
)

// Note: Mount, Change, and FSStat are delegated to the embedded NullAuthHandler

// ToHandle converts a path to an opaque file handle.
// The path is encoded directly into the handle when possible,
// making handles stateless and stable.
func (h *StableHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	joinedPath := f.Join(path...)
	if joinedPath == "" {
		joinedPath = "/"
	}
	logging.Debug("StableHandler.ToHandle", zap.Strings("path", path), zap.String("joined", joinedPath))

	// Simple approach: store path directly if it fits
	pathBytes := []byte(joinedPath)
	if len(pathBytes)+1 <= maxHandleSize {
		// Pad to minimum size for macOS NFS client compatibility
		handleSize := len(pathBytes) + 1
		if handleSize < minHandleSize {
			handleSize = minHandleSize
		}
		handle := make([]byte, handleSize)
		handle[0] = handleVersionCompressed // reuse version 1 for direct path storage
		copy(handle[1:], pathBytes)
		// Remaining bytes are zero (padding)
		return handle
	}

	// Path too long - use hash-based fallback (version 2)
	hash := sha256.Sum256([]byte(joinedPath))

	// Store in fallback cache
	h.mu.Lock()
	h.fallback[hash] = joinedPath
	h.mu.Unlock()

	handle := make([]byte, 33)
	handle[0] = handleVersionHash
	copy(handle[1:], hash[:])
	return handle
}

// FromHandle converts an opaque file handle back to a path.
func (h *StableHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	logging.Debug("StableHandler.FromHandle", zap.Int("handle_len", len(fh)))
	if len(fh) < 2 {
		logging.Debug("StableHandler.FromHandle: handle too short")
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	var joinedPath string

	switch fh[0] {
	case handleVersionCompressed:
		// Read path directly, trimming any null padding
		pathBytes := fh[1:]
		// Find the end of the path (first null byte or end of slice)
		for i, b := range pathBytes {
			if b == 0 {
				pathBytes = pathBytes[:i]
				break
			}
		}
		joinedPath = string(pathBytes)

	case handleVersionHash:
		if len(fh) < 33 {
			return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
		}
		var hash [32]byte
		copy(hash[:], fh[1:33])

		h.mu.RLock()
		path, ok := h.fallback[hash]
		h.mu.RUnlock()

		if !ok {
			// Hash not found - handle is truly stale
			// This can happen if server restarted or path was very long
			return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
		}
		joinedPath = path

	default:
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Split path back into components
	var pathParts []string
	if joinedPath == "/" || joinedPath == "" {
		pathParts = []string{}
	} else {
		// Remove leading slash and split
		cleanPath := strings.TrimPrefix(joinedPath, "/")
		if cleanPath != "" {
			pathParts = strings.Split(cleanPath, "/")
		}
	}

	logging.Debug("StableHandler.FromHandle: decoded", zap.String("joinedPath", joinedPath), zap.Strings("pathParts", pathParts))
	return h.fs, pathParts, nil
}

// InvalidateHandle removes a handle from any caches.
func (h *StableHandler) InvalidateHandle(fs billy.Filesystem, fh []byte) error {
	if len(fh) >= 33 && fh[0] == handleVersionHash {
		var hash [32]byte
		copy(hash[:], fh[1:33])

		h.mu.Lock()
		delete(h.fallback, hash)
		h.mu.Unlock()
	}
	return nil
}

// HandleLimit returns the max handles this handler supports.
// Since most handles are stateless, return a large number.
func (h *StableHandler) HandleLimit() int {
	return 1000000
}

// compressPath compresses a path string using zlib.
func compressPath(path string) []byte {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		return []byte(path) // Fallback to uncompressed
	}
	w.Write([]byte(path))
	w.Close()
	return buf.Bytes()
}

// decompressPath decompresses a zlib-compressed path.
func decompressPath(data []byte) (string, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("failed to create decompressor: %w", err)
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("failed to decompress: %w", err)
	}
	return string(result), nil
}

// Verifier support for READDIRPLUS
//
// Verifiers are deterministic hashes of path+contents. We intentionally do NOT
// cache directory listings here, even though it means re-querying the database
// on each pagination request.
//
// TODO: Consider adding a small MRU cache for directory listings to improve
// pagination performance on large directories. Trade-offs to consider:
//   - Pro: Avoids re-querying database on each READDIRPLUS continuation
//   - Pro: Better performance for tables with 1000s of rows
//   - Con: Cached listings become stale if concurrent writes occur
//   - Con: Users may see inconsistent data mid-pagination
//   - Con: Added complexity and memory usage
//
// For now, we prioritize correctness over performance. The verifier hash
// ensures clients detect when a directory has changed, triggering a re-read.

// VerifierFor creates a verifier for directory listing pagination.
// The verifier is a deterministic hash of path + contents, so it doesn't
// need to be cached - the same inputs always produce the same verifier.
func (h *StableHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	hash := sha256.New()
	binary.Write(hash, binary.BigEndian, uint64(len(path)))
	hash.Write([]byte(path))
	for _, c := range contents {
		hash.Write([]byte(c.Name()))
	}
	return binary.BigEndian.Uint64(hash.Sum(nil)[0:8])
}

// DataForVerifier retrieves cached directory listing for pagination.
// We don't cache listings - the verifier is just used to detect if the
// directory changed. Return nil to signal caller should re-read.
func (h *StableHandler) DataForVerifier(path string, id uint64) []fs.FileInfo {
	return nil
}
