// Package mount provides mount state tracking for TigerFS filesystem instances.
//
// TigerFS mounts run as long-lived processes. To support commands like `unmount`,
// `status`, and `list`, we need to track which mounts are active. This package
// provides a Registry that persists mount information to a JSON file in the
// user's config directory (typically ~/.config/tigerfs/mounts.json).
//
// The registry stores:
//   - Mountpoint path (the directory where the filesystem is mounted)
//   - Process ID (PID) of the TigerFS process serving the mount
//   - Database connection info (sanitized - no passwords)
//   - Start time of the mount
//
// Typical usage:
//
//	// During mount startup
//	registry, err := mount.NewRegistry("")
//	if err != nil {
//	    return err
//	}
//	err = registry.Register(mount.Entry{
//	    Mountpoint: "/mnt/db",
//	    PID:        os.Getpid(),
//	    Database:   "postgres://localhost/mydb",
//	    StartTime:  time.Now(),
//	})
//
//	// During unmount or listing
//	entries, err := registry.ListActive()
//
// The registry automatically handles stale entries (where the process has died
// but the entry remains) via the Cleanup() method and ListActive() filtering.
package mount

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Entry represents a single TigerFS mount in the registry.
//
// Each Entry captures the essential information needed to identify, monitor,
// and control a running TigerFS instance.
type Entry struct {
	// Mountpoint is the absolute path where the filesystem is mounted.
	// Example: "/mnt/db" or "/home/user/data"
	Mountpoint string `json:"mountpoint"`

	// PID is the process ID of the TigerFS process serving this mount.
	// Used to check if the mount is still active and to send signals for unmount.
	PID int `json:"pid"`

	// Database is the connection string (with password removed) for display purposes.
	// Example: "postgres://user@localhost:5432/mydb"
	Database string `json:"database"`

	// StartTime records when the mount was initiated.
	// Used to calculate uptime in status displays.
	StartTime time.Time `json:"start_time"`

	// ServiceID is the cloud service identifier (Tiger service ID or Ghost database ID).
	// Empty for direct postgres:// connections.
	ServiceID string `json:"service_id,omitempty"`

	// CLIBackend is the backend that manages this service ("tiger", "ghost", or "").
	// Empty for direct postgres:// connections. Used by `tigerfs info` and `tigerfs fork`
	// to resolve the backend without re-parsing the connection string.
	CLIBackend string `json:"cli_backend,omitempty"`

	// AutoCreated is true when the mountpoint directory was auto-created by TigerFS
	// (i.e., derived from a backend prefix like tiger:ID). Auto-created directories
	// are cleaned up on unmount; user-specified mountpoints are left alone.
	AutoCreated bool `json:"auto_created,omitempty"`
}

// Registry manages the collection of active TigerFS mounts.
//
// The registry persists mount information to a JSON file, enabling commands
// like `unmount`, `status`, and `list` to discover and interact with running
// TigerFS instances.
//
// Registry is safe for concurrent use. All operations acquire appropriate
// locks before reading or modifying the registry file.
type Registry struct {
	// path is the filesystem path to the JSON registry file.
	path string

	// mu protects concurrent access to the registry file.
	// RLock for read operations, Lock for write operations.
	mu sync.RWMutex
}

// DefaultRegistryPath returns the default path for the mount registry file.
//
// The path follows XDG conventions:
//   - Linux: ~/.config/tigerfs/mounts.json
//   - macOS: ~/Library/Application Support/tigerfs/mounts.json
//   - Windows: %APPDATA%\tigerfs\mounts.json
//
// Returns an error if the user's config directory cannot be determined.
func DefaultRegistryPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user config dir: %w", err)
	}
	return filepath.Join(configDir, "tigerfs", "mounts.json"), nil
}

// NewRegistry creates a new Registry instance.
//
// Parameters:
//   - path: The filesystem path for the registry file. If empty, uses DefaultRegistryPath().
//
// The function creates the parent directory if it doesn't exist (with mode 0700
// for privacy, since the registry may contain connection details).
//
// Returns an error if:
//   - path is empty and DefaultRegistryPath() fails
//   - The parent directory cannot be created
func NewRegistry(path string) (*Registry, error) {
	if path == "" {
		var err error
		path, err = DefaultRegistryPath()
		if err != nil {
			return nil, err
		}
	}

	// Ensure the parent directory exists with restricted permissions.
	// Mode 0700 = owner can read/write/execute, no access for group/others.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create registry directory: %w", err)
	}

	return &Registry{path: path}, nil
}

// Path returns the filesystem path to the registry file.
//
// This is useful for displaying the registry location to users
// or for debugging purposes.
func (r *Registry) Path() string {
	return r.path
}

// Register adds a new mount entry to the registry.
//
// If an entry with the same mountpoint already exists, it is replaced.
// This handles cases where a previous mount crashed without cleanup.
//
// Parameters:
//   - entry: The mount information to register. Mountpoint and PID are required.
//
// Returns an error if the registry file cannot be read or written.
func (r *Registry) Register(entry Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Load existing entries from the registry file.
	entries, err := r.loadLocked()
	if err != nil {
		return err
	}

	// Remove any existing entry for this mountpoint to avoid duplicates.
	// This handles the case where a previous mount died without cleanup.
	entries = filterMountpoint(entries, entry.Mountpoint)

	// Add the new entry and persist to disk.
	entries = append(entries, entry)

	return r.saveLocked(entries)
}

// Unregister removes a mount entry from the registry.
//
// This should be called when a mount is gracefully unmounted. The entry
// is identified by its mountpoint path.
//
// Parameters:
//   - mountpoint: The path of the mount to remove (will be normalized to absolute path).
//
// Returns an error if the registry file cannot be read or written.
// Does not return an error if the mountpoint is not found (idempotent).
func (r *Registry) Unregister(mountpoint string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	entries, err := r.loadLocked()
	if err != nil {
		return err
	}

	// Filter out the entry for this mountpoint.
	entries = filterMountpoint(entries, mountpoint)

	return r.saveLocked(entries)
}

// Get retrieves the entry for a specific mountpoint.
//
// Parameters:
//   - mountpoint: The path to look up (will be normalized to absolute path).
//
// Returns:
//   - The Entry pointer if found, nil if not found.
//   - An error if the registry file cannot be read.
//
// Note: This returns the entry regardless of whether the process is still running.
// Use ListActive() if you only want active mounts.
func (r *Registry) Get(mountpoint string) (*Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries, err := r.loadLocked()
	if err != nil {
		return nil, err
	}

	// Normalize the search path to absolute for consistent comparison.
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		// Fall back to the original path if Abs fails.
		absMountpoint = mountpoint
	}

	// Search for matching entry.
	for _, e := range entries {
		absEntry, err := filepath.Abs(e.Mountpoint)
		if err != nil {
			absEntry = e.Mountpoint
		}
		if absEntry == absMountpoint {
			return &e, nil
		}
	}

	return nil, nil
}

// List returns all entries in the registry.
//
// This includes both active mounts (process running) and stale entries
// (process died but entry remains). Use ListActive() if you only want
// mounts that are currently running.
//
// Returns an empty slice (not nil) if there are no entries.
// Returns an error if the registry file cannot be read.
func (r *Registry) List() ([]Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries, err := r.loadLocked()
	if err != nil {
		return nil, err
	}

	// Return empty slice instead of nil for consistent iteration.
	if entries == nil {
		return []Entry{}, nil
	}
	return entries, nil
}

// ListActive returns only entries where the TigerFS process is still running.
//
// This filters out stale entries by checking if each PID corresponds to
// a running process. Use this for user-facing commands like `tigerfs list`.
//
// Returns an empty slice (not nil) if there are no active mounts.
// Returns an error if the registry file cannot be read.
func (r *Registry) ListActive() ([]Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries, err := r.loadLocked()
	if err != nil {
		return nil, err
	}

	// Filter to only include entries with running processes.
	var active []Entry
	for _, e := range entries {
		if IsProcessRunning(e.PID) {
			active = append(active, e)
		}
	}

	// Return empty slice instead of nil for consistent iteration.
	if active == nil {
		return []Entry{}, nil
	}
	return active, nil
}

// Cleanup removes stale entries from the registry.
//
// An entry is considered stale if its PID no longer corresponds to a
// running process. This can happen if TigerFS crashes or is killed
// without graceful shutdown.
//
// Returns:
//   - The number of stale entries that were removed.
//   - An error if the registry file cannot be read or written.
//
// This operation is safe to call periodically or before listing mounts.
func (r *Registry) Cleanup() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entries, err := r.loadLocked()
	if err != nil {
		return 0, err
	}

	// Separate active entries from stale ones.
	var active []Entry
	removed := 0
	for _, e := range entries {
		if IsProcessRunning(e.PID) {
			active = append(active, e)
		} else {
			removed++
		}
	}

	// Only write to disk if we actually removed something.
	if removed > 0 {
		if err := r.saveLocked(active); err != nil {
			return 0, err
		}
	}

	return removed, nil
}

// loadLocked reads and parses the registry file.
//
// IMPORTANT: This method assumes the caller holds at least an RLock on r.mu.
// It does not acquire locks itself to allow callers to perform atomic
// read-modify-write operations.
//
// Returns:
//   - The list of entries (may be nil if file doesn't exist or is empty).
//   - An error if the file exists but cannot be read or parsed.
func (r *Registry) loadLocked() ([]Entry, error) {
	data, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet - this is normal for first use.
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read registry: %w", err)
	}

	// Handle empty file gracefully.
	if len(data) == 0 {
		return nil, nil
	}

	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse registry: %w", err)
	}

	return entries, nil
}

// saveLocked writes the entries to the registry file.
//
// IMPORTANT: This method assumes the caller holds a Lock (not RLock) on r.mu.
// It does not acquire locks itself to allow callers to perform atomic
// read-modify-write operations.
//
// The file is written with mode 0600 (owner read/write only) for privacy.
//
// Parameters:
//   - entries: The list of entries to persist. May be nil or empty.
//
// Returns an error if the file cannot be written.
func (r *Registry) saveLocked(entries []Entry) error {
	// Use indented JSON for human readability when debugging.
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal registry: %w", err)
	}

	// Mode 0600 = owner can read/write, no access for group/others.
	if err := os.WriteFile(r.path, data, 0600); err != nil {
		return fmt.Errorf("failed to write registry: %w", err)
	}

	return nil
}

// filterMountpoint returns a new slice with entries for the given mountpoint removed.
//
// The comparison is done using absolute paths to handle cases where the same
// directory is specified differently (e.g., "/mnt/db" vs "/mnt/db/").
//
// Parameters:
//   - entries: The slice to filter.
//   - mountpoint: The mountpoint path to exclude.
//
// Returns a new slice without modifying the original.
func filterMountpoint(entries []Entry, mountpoint string) []Entry {
	// Normalize the filter path to absolute.
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		absMountpoint = mountpoint
	}

	var result []Entry
	for _, e := range entries {
		// Normalize each entry's path for comparison.
		absEntry, err := filepath.Abs(e.Mountpoint)
		if err != nil {
			absEntry = e.Mountpoint
		}
		// Keep entries that don't match the filter.
		if absEntry != absMountpoint {
			result = append(result, e)
		}
	}
	return result
}

// IsProcessRunning checks if a process with the given PID is still alive.
//
// On Unix systems, this works by sending signal 0 to the process. Signal 0
// doesn't actually send a signal, but the kernel still checks if the process
// exists and if we have permission to signal it.
//
// Parameters:
//   - pid: The process ID to check. Must be positive.
//
// Returns:
//   - true if the process exists and we can signal it.
//   - false if the process doesn't exist, pid is invalid, or we lack permission.
func IsProcessRunning(pid int) bool {
	if pid <= 0 {
		return false
	}

	// os.FindProcess always succeeds on Unix - it doesn't actually check
	// if the process exists. We need to send a signal to verify.
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Signal 0 is a special "null signal" - it doesn't do anything to the
	// process but returns an error if the process doesn't exist or we
	// don't have permission to signal it.
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
