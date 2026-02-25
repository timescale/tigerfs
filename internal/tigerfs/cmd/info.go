package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/backend"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
	"go.uber.org/zap"

	"github.com/timescale/tigerfs/internal/tigerfs/logging"
)

// buildInfoCmd creates the info command.
//
// The info command displays detailed information about a mounted filesystem
// and its backing cloud service (if any).
//
// Without arguments, uses the sole active mount (errors if zero or multiple).
// With a MOUNTPOINT argument, shows info for that specific mount.
func buildInfoCmd() *cobra.Command {
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "info [MOUNTPOINT]",
		Short: "Show info about a mounted filesystem and its backing service",
		Long: `Show detailed information about a TigerFS mount.

Without MOUNTPOINT, uses the only active mount (errors if none or multiple).
For cloud-backed mounts, fetches live service info from the backend.

Examples:
  # Show info for a specific mount
  tigerfs info /tmp/mydb

  # Show info as JSON
  tigerfs info --json /tmp/mydb

  # Show info for the only active mount
  tigerfs info`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			registry, err := mount.NewRegistry("")
			if err != nil {
				return fmt.Errorf("failed to open registry: %w", err)
			}

			// Resolve the target mount entry.
			entry, err := resolveInfoTarget(registry, args)
			if err != nil {
				return err
			}

			// Build the display info from registry data.
			info := backend.MountInfo{
				Mountpoint: entry.Mountpoint,
				PID:        entry.PID,
				Database:   entry.Database,
				StartTime:  entry.StartTime,
				Uptime:     formatDuration(time.Since(entry.StartTime)),
				Status:     getMountStatus(entry.PID),
				Backend:    entry.CLIBackend,
			}

			// Fetch live service info from the backend if available.
			if entry.CLIBackend != "" && entry.ServiceID != "" {
				b, err := backend.ForName(entry.CLIBackend)
				if err != nil {
					logging.Warn("Failed to resolve backend",
						zap.String("backend", entry.CLIBackend),
						zap.Error(err))
				} else {
					svcInfo, err := b.GetInfo(cmd.Context(), entry.ServiceID)
					if err != nil {
						logging.Warn("Failed to fetch service info",
							zap.String("backend", entry.CLIBackend),
							zap.String("service_id", entry.ServiceID),
							zap.Error(err))
					} else {
						info.Service = svcInfo
					}
				}
			}

			if jsonOutput {
				return printInfoJSON(cmd, &info)
			}
			return printInfoHuman(cmd, entry, &info)
		},
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")

	return cmd
}

// resolveInfoTarget determines which mount entry the info command should display.
//
// With args: looks up the specified mountpoint.
// Without args: uses the sole active mount, or errors if zero or multiple.
func resolveInfoTarget(registry *mount.Registry, args []string) (*mount.Entry, error) {
	if len(args) == 1 {
		absMountpoint, err := filepath.Abs(args[0])
		if err != nil {
			return nil, fmt.Errorf("failed to resolve mountpoint path: %w", err)
		}
		entry, err := registry.Get(absMountpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to look up mount: %w", err)
		}
		if entry == nil {
			return nil, fmt.Errorf("no TigerFS mount found at %s", absMountpoint)
		}
		return entry, nil
	}

	// No argument — find the sole active mount.
	active, err := registry.ListActive()
	if err != nil {
		return nil, fmt.Errorf("failed to list mounts: %w", err)
	}
	switch len(active) {
	case 0:
		return nil, fmt.Errorf("no active TigerFS mounts")
	case 1:
		return &active[0], nil
	default:
		msg := "multiple active mounts — specify one:\n"
		for _, e := range active {
			msg += fmt.Sprintf("  tigerfs info %s\n", e.Mountpoint)
		}
		return nil, fmt.Errorf("%s", msg)
	}
}

// printInfoJSON outputs mount info as JSON.
func printInfoJSON(cmd *cobra.Command, info *backend.MountInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal info: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}

// printInfoHuman outputs mount info in human-readable format.
func printInfoHuman(cmd *cobra.Command, entry *mount.Entry, info *backend.MountInfo) error {
	out := cmd.OutOrStdout()

	fmt.Fprintf(out, "Mountpoint:   %s\n", info.Mountpoint)

	if info.Service != nil {
		svc := info.Service
		b, _ := backend.ForName(entry.CLIBackend)
		if b != nil {
			fmt.Fprintf(out, "Backend:      %s\n", b.Name())
		}
		fmt.Fprintf(out, "Service ID:   %s\n", svc.ID)
		if svc.Name != "" {
			fmt.Fprintf(out, "Service Name: %s\n", svc.Name)
		}
		fmt.Fprintf(out, "Status:       %s\n", svc.Status)
		if svc.Region != "" {
			fmt.Fprintf(out, "Region:       %s\n", svc.Region)
		}
		if svc.Host != "" {
			fmt.Fprintf(out, "Host:         %s\n", svc.Host)
		}
		if svc.Port != 0 {
			fmt.Fprintf(out, "Port:         %d\n", svc.Port)
		}
		if svc.DB != "" {
			fmt.Fprintf(out, "Database:     %s\n", svc.DB)
		}
		if svc.User != "" {
			fmt.Fprintf(out, "User:         %s\n", svc.User)
		}
		if svc.Type != "" {
			fmt.Fprintf(out, "Type:         %s\n", svc.Type)
		}
	} else {
		// Raw connection — show connection string.
		fmt.Fprintf(out, "Database:     %s\n", info.Database)
	}

	fmt.Fprintf(out, "PID:          %d\n", info.PID)
	fmt.Fprintf(out, "Uptime:       %s\n", info.Uptime)

	if info.Service == nil && entry.CLIBackend == "" {
		fmt.Fprintf(out, "\n(No cloud service — mounted via direct connection string)\n")
	}

	return nil
}
