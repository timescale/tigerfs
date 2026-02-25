// Package cmd provides CLI commands for TigerFS.
//
// This file implements the status command which displays information about
// active TigerFS mounts including mountpoint, database, uptime, and status.
package cmd

import (
	"fmt"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
	"go.uber.org/zap"
)

// buildStatusCmd creates the status command.
//
// The status command displays information about TigerFS mounts:
//   - Without arguments: lists all active mounts in a table format
//   - With a mountpoint argument: shows detailed status for that mount
//
// Information displayed includes:
//   - Mountpoint path
//   - Database connection (sanitized)
//   - Process ID
//   - Status (active/stale)
//   - Uptime since mount started
func buildStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status [MOUNTPOINT]",
		Short: "Show status of mounted filesystems",
		Long: `Show status of mounted TigerFS instances.

Without MOUNTPOINT argument, lists all mounted instances.
With MOUNTPOINT, shows detailed status for that specific mount.

Examples:
  # List all mounts
  tigerfs status

  # Show detailed status for specific mount
  tigerfs status /mnt/db`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			registry, err := mount.NewRegistry("")
			if err != nil {
				return fmt.Errorf("failed to open registry: %w", err)
			}

			if len(args) == 0 {
				return showAllMounts(cmd, registry)
			}

			mountpoint := args[0]
			return showMountStatus(cmd, registry, mountpoint)
		},
	}
}

// showAllMounts displays a table of all active mounts.
//
// Parameters:
//   - cmd: The cobra command (used for output)
//   - registry: The mount registry to query
//
// Output format is a tab-aligned table with columns:
// MOUNTPOINT, DATABASE, PID, STATUS, UPTIME
func showAllMounts(cmd *cobra.Command, registry *mount.Registry) error {
	// Clean up stale entries first
	removed, err := registry.Cleanup()
	if err != nil {
		logging.Warn("Failed to clean up stale entries", zap.Error(err))
	} else if removed > 0 {
		logging.Debug("Cleaned up stale registry entries", zap.Int("count", removed))
	}

	entries, err := registry.ListActive()
	if err != nil {
		return fmt.Errorf("failed to list mounts: %w", err)
	}

	if len(entries) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No active TigerFS mounts")
		return nil
	}

	// Use tabwriter for aligned columns
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MOUNTPOINT\tDATABASE\tPID\tSTATUS\tUPTIME")

	for _, entry := range entries {
		status := getMountStatus(entry.PID)
		uptime := formatDuration(time.Since(entry.StartTime))
		database := truncateString(entry.Database, 40)

		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n",
			entry.Mountpoint,
			database,
			entry.PID,
			status,
			uptime,
		)
	}

	return w.Flush()
}

// showMountStatus displays detailed status for a specific mount.
//
// Parameters:
//   - cmd: The cobra command (used for output)
//   - registry: The mount registry to query
//   - mountpoint: The mountpoint to show status for
//
// Returns an error if the mountpoint is not found in the registry.
func showMountStatus(cmd *cobra.Command, registry *mount.Registry, mountpoint string) error {
	// Resolve to absolute path for consistent lookup
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return fmt.Errorf("failed to resolve mountpoint path: %w", err)
	}

	entry, err := registry.Get(absMountpoint)
	if err != nil {
		return fmt.Errorf("failed to look up mount: %w", err)
	}

	if entry == nil {
		return fmt.Errorf("no TigerFS mount found at %s", absMountpoint)
	}

	status := getMountStatus(entry.PID)
	uptime := formatDuration(time.Since(entry.StartTime))

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Mountpoint: %s\n", entry.Mountpoint)
	fmt.Fprintf(out, "Database:   %s\n", entry.Database)
	if entry.CLIBackend != "" {
		fmt.Fprintf(out, "Backend:    %s\n", entry.CLIBackend)
	}
	if entry.ServiceID != "" {
		fmt.Fprintf(out, "Service ID: %s\n", entry.ServiceID)
	}
	fmt.Fprintf(out, "PID:        %d\n", entry.PID)
	fmt.Fprintf(out, "Status:     %s\n", status)
	fmt.Fprintf(out, "Started:    %s\n", entry.StartTime.Format(time.RFC3339))
	fmt.Fprintf(out, "Uptime:     %s\n", uptime)

	return nil
}

// getMountStatus returns the status string for a mount based on whether
// its process is still running.
//
// Parameters:
//   - pid: The process ID to check
//
// Returns "active" if the process is running, "stale" otherwise.
func getMountStatus(pid int) string {
	if isProcessRunning(pid) {
		return "active"
	}
	return "stale"
}

// isProcessRunning checks if a process with the given PID exists.
// This is a local copy to avoid circular imports with the mount package.
func isProcessRunning(pid int) bool {
	return mount.IsProcessRunning(pid)
}

// formatDuration formats a duration in a human-readable way.
//
// Examples:
//   - 45s
//   - 5m30s
//   - 2h15m
//   - 3d4h
//
// Parameters:
//   - d: The duration to format
//
// Returns a compact string representation.
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		if secs == 0 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm%ds", mins, secs)
	}
	if d < 24*time.Hour {
		hours := int(d.Hours())
		mins := int(d.Minutes()) % 60
		if mins == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh%dm", hours, mins)
	}

	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	if hours == 0 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd%dh", days, hours)
}

// truncateString truncates a string to maxLen characters, adding "..." if truncated.
//
// Parameters:
//   - s: The string to truncate
//   - maxLen: Maximum length (must be > 3 for "..." to fit)
//
// Returns the original string if shorter than maxLen, otherwise truncated with "...".
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
