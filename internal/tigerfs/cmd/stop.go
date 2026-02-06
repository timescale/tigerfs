// Package cmd provides CLI commands for TigerFS.
//
// This file implements the stop command which stops a running TigerFS
// process by PID, triggering graceful unmount.
package cmd

import (
	"context"
	"fmt"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
	"go.uber.org/zap"
)

// buildStopCmd creates the stop command.
//
// The stop command gracefully stops a TigerFS process by sending SIGTERM.
// This is equivalent to `tigerfs unmount <mountpoint>` when you know the PID
// instead of the mountpoint.
//
// When a TigerFS process receives SIGTERM, it:
//  1. Cancels its context
//  2. Unmounts the filesystem
//  3. Closes database connections
//  4. Exits cleanly
func buildStopCmd() *cobra.Command {
	var timeout int

	cmd := &cobra.Command{
		Use:   "stop PID",
		Short: "Stop a TigerFS process by PID",
		Long: `Gracefully stop a TigerFS process by sending SIGTERM.

This is equivalent to 'tigerfs unmount <mountpoint>' but uses the process ID
instead of the mountpoint path. The process will unmount cleanly and exit.

You can find the PID using 'tigerfs list' or 'tigerfs status'.

Examples:
  # Stop TigerFS process
  tigerfs stop 12345

  # Stop with custom timeout
  tigerfs stop --timeout=60 12345`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			// Parse PID
			pid, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid PID: %s", args[0])
			}

			if pid <= 0 {
				return fmt.Errorf("PID must be a positive integer")
			}

			logging.Debug("Stopping TigerFS process",
				zap.Int("pid", pid),
				zap.Int("timeout", timeout),
			)

			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			// Try to find mountpoint in registry for cleanup
			mountpoint := findMountpointByPID(pid)

			// Send SIGTERM to process
			if err := signalProcess(pid, syscall.SIGTERM); err != nil {
				return fmt.Errorf("failed to signal process %d: %w", pid, err)
			}

			logging.Info("Sent SIGTERM to process", zap.Int("pid", pid))

			// Wait for process to exit
			if !waitForProcessExit(ctx, pid) {
				return fmt.Errorf("process %d did not exit within timeout", pid)
			}

			logging.Info("TigerFS process exited gracefully", zap.Int("pid", pid))

			// Clean up registry entry if we found the mountpoint
			if mountpoint != "" {
				if err := cleanupRegistryEntry(mountpoint); err != nil {
					logging.Warn("Failed to clean up registry entry", zap.Error(err))
				}
			}

			fmt.Printf("Successfully stopped TigerFS process %d\n", pid)
			return nil
		},
	}

	cmd.Flags().IntVarP(&timeout, "timeout", "t", 30, "wait timeout in seconds")

	return cmd
}

// findMountpointByPID looks up a mountpoint by PID in the registry.
//
// Parameters:
//   - pid: Process ID to look up
//
// Returns the mountpoint path if found, or empty string if not found.
func findMountpointByPID(pid int) string {
	registry, err := mount.NewRegistry("")
	if err != nil {
		logging.Debug("Could not open registry", zap.Error(err))
		return ""
	}

	entries, err := registry.List()
	if err != nil {
		logging.Debug("Could not list registry entries", zap.Error(err))
		return ""
	}

	for _, entry := range entries {
		if entry.PID == pid {
			return entry.Mountpoint
		}
	}

	return ""
}
