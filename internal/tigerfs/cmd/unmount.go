// Package cmd provides CLI commands for TigerFS.
//
// This file implements the unmount command which gracefully stops a running
// TigerFS instance and releases the mountpoint.
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
	"go.uber.org/zap"
)

// buildUnmountCmd creates the unmount command.
//
// The unmount command gracefully stops a TigerFS filesystem. It attempts
// to signal the TigerFS process first (if found in registry), then falls
// back to system unmount commands if needed.
//
// The command supports:
//   - Graceful unmount with configurable timeout
//   - Force unmount for busy filesystems
//   - Automatic cleanup of stale registry entries
func buildUnmountCmd() *cobra.Command {
	var force bool
	var timeout int

	cmd := &cobra.Command{
		Use:   "unmount MOUNTPOINT",
		Short: "Unmount a TigerFS filesystem",
		Long: `Gracefully unmount a TigerFS instance.

This command signals the TigerFS process to stop, which triggers a clean
unmount. Equivalent to 'tigerfs stop <pid>' if you know the process ID.

Examples:
  # Unmount filesystem
  tigerfs unmount /mnt/db

  # Force unmount
  tigerfs unmount --force /mnt/db

  # Unmount with custom timeout
  tigerfs unmount --timeout=60 /mnt/db`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			mountpoint := args[0]

			// Resolve to absolute path for consistent handling
			absMountpoint, err := filepath.Abs(mountpoint)
			if err != nil {
				return fmt.Errorf("failed to resolve mountpoint path: %w", err)
			}

			logging.Debug("Unmounting filesystem",
				zap.String("mountpoint", absMountpoint),
				zap.Bool("force", force),
				zap.Int("timeout", timeout),
			)

			// Create context with timeout for the entire unmount operation
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			// Try to unmount using the appropriate method
			if err := unmountFilesystem(ctx, absMountpoint, force); err != nil {
				return err
			}

			// Clean up registry entry if it exists
			if err := cleanupRegistryEntry(absMountpoint); err != nil {
				// Log but don't fail - the unmount itself succeeded
				logging.Warn("Failed to clean up registry entry", zap.Error(err))
			}

			fmt.Printf("Successfully unmounted %s\n", absMountpoint)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "force unmount even if busy")
	cmd.Flags().IntVarP(&timeout, "timeout", "t", 30, "wait timeout in seconds")

	return cmd
}

// unmountFilesystem attempts to unmount a FUSE filesystem.
//
// The function first tries to signal the TigerFS process (if found in the
// registry) to trigger graceful shutdown. If that fails or the process isn't
// found, it falls back to system unmount commands.
//
// Parameters:
//   - ctx: Context for timeout/cancellation
//   - mountpoint: Absolute path to the mountpoint
//   - force: If true, use force unmount options
//
// Returns an error if unmount fails after all attempts.
func unmountFilesystem(ctx context.Context, mountpoint string, force bool) error {
	// First, try to find the mount in our registry and signal the process
	registry, err := mount.NewRegistry("")
	if err != nil {
		logging.Debug("Could not open registry, will use system unmount", zap.Error(err))
	} else {
		entry, err := registry.Get(mountpoint)
		if err != nil {
			logging.Debug("Could not look up mount in registry", zap.Error(err))
		} else if entry != nil {
			// Found in registry - try to signal the process for graceful shutdown
			logging.Debug("Found mount in registry, signaling process",
				zap.Int("pid", entry.PID),
			)

			if err := signalProcess(entry.PID, syscall.SIGTERM); err == nil {
				// Wait for process to exit
				if waitForProcessExit(ctx, entry.PID) {
					logging.Info("TigerFS process exited gracefully")
					return nil
				}
				logging.Debug("Process did not exit in time, falling back to system unmount")
			} else {
				logging.Debug("Could not signal process", zap.Error(err))
			}
		}
	}

	// Fall back to system unmount command
	return systemUnmount(ctx, mountpoint, force)
}

// signalProcess sends a signal to a process.
//
// Parameters:
//   - pid: Process ID to signal
//   - sig: Signal to send (e.g., syscall.SIGTERM)
//
// Returns an error if the process doesn't exist or can't be signaled.
func signalProcess(pid int, sig syscall.Signal) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process not found: %w", err)
	}

	if err := process.Signal(sig); err != nil {
		return fmt.Errorf("failed to signal process: %w", err)
	}

	return nil
}

// waitForProcessExit waits for a process to exit.
//
// Parameters:
//   - ctx: Context for timeout/cancellation
//   - pid: Process ID to wait for
//
// Returns true if the process exited, false if context was cancelled/timed out.
func waitForProcessExit(ctx context.Context, pid int) bool {
	// Poll every 100ms to check if process has exited
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			// Check if process is still running by sending signal 0
			process, err := os.FindProcess(pid)
			if err != nil {
				return true // Process doesn't exist
			}
			if err := process.Signal(syscall.Signal(0)); err != nil {
				return true // Process has exited
			}
		}
	}
}

// systemUnmount uses OS-specific commands to unmount a filesystem.
//
// On macOS: uses `umount` (or `diskutil unmount force` for force unmount)
// On Linux: uses `fusermount -u` (or `fusermount -uz` for force/lazy unmount)
//
// Parameters:
//   - ctx: Context for timeout/cancellation
//   - mountpoint: Path to unmount
//   - force: If true, use force unmount options
//
// Returns an error if the system unmount command fails.
func systemUnmount(ctx context.Context, mountpoint string, force bool) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		// macOS: use umount or diskutil
		if force {
			cmd = exec.CommandContext(ctx, "diskutil", "unmount", "force", mountpoint)
		} else {
			cmd = exec.CommandContext(ctx, "umount", mountpoint)
		}

	case "linux":
		// Linux: use fusermount for FUSE filesystems
		if force {
			// -u = unmount, -z = lazy unmount (force)
			cmd = exec.CommandContext(ctx, "fusermount", "-uz", mountpoint)
		} else {
			cmd = exec.CommandContext(ctx, "fusermount", "-u", mountpoint)
		}

	default:
		// Fallback: try standard umount
		if force {
			cmd = exec.CommandContext(ctx, "umount", "-f", mountpoint)
		} else {
			cmd = exec.CommandContext(ctx, "umount", mountpoint)
		}
	}

	logging.Debug("Executing system unmount",
		zap.String("command", cmd.String()),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already unmounted (not a real error)
		if isNotMountedError(string(output)) {
			logging.Debug("Mountpoint was already unmounted")
			return nil
		}
		return fmt.Errorf("unmount failed: %w\nOutput: %s", err, string(output))
	}

	return nil
}

// isNotMountedError checks if the error message indicates the path is not mounted.
//
// Different systems return different error messages for this condition:
//   - macOS: "not currently mounted"
//   - Linux fusermount: "not found in /etc/mtab" or "not mounted"
//
// Parameters:
//   - output: The combined stdout/stderr from the unmount command
//
// Returns true if the error indicates the path is simply not mounted.
func isNotMountedError(output string) bool {
	notMountedPhrases := []string{
		"not currently mounted",
		"not mounted",
		"not found in /etc/mtab",
		"no mount point specified",
		"Invalid argument", // Sometimes returned for non-existent mounts
	}

	for _, phrase := range notMountedPhrases {
		if strings.Contains(output, phrase) {
			return true
		}
	}
	return false
}

// cleanupRegistryEntry removes a mount from the registry after successful unmount.
//
// Parameters:
//   - mountpoint: The mountpoint that was unmounted
//
// Returns an error if the registry cannot be accessed or modified.
// Note: This is best-effort cleanup - the unmount has already succeeded.
func cleanupRegistryEntry(mountpoint string) error {
	registry, err := mount.NewRegistry("")
	if err != nil {
		return fmt.Errorf("failed to open registry: %w", err)
	}

	if err := registry.Unregister(mountpoint); err != nil {
		return fmt.Errorf("failed to unregister mount: %w", err)
	}

	logging.Debug("Removed mount from registry", zap.String("mountpoint", mountpoint))
	return nil
}
