package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/backend"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/mount"
)

// buildForkCmd creates the fork command.
//
// The fork command creates a copy of an existing database service and
// optionally mounts the fork. SOURCE can be a mountpoint path, a
// backend:id reference, or a bare name resolved via default_backend.
func buildForkCmd() *cobra.Command {
	var nameFlag string
	var noMount bool
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "fork SOURCE [DEST]",
		Short: "Fork an existing filesystem",
		Long: `Fork an existing database service and mount the copy.

SOURCE identifies what to fork:
  /path/to/mount   Mountpoint path (looked up in registry)
  tiger:ID         Tiger Cloud service by ID or name
  ghost:ID         Ghost database by ID or name
  NAME             Bare name on default_backend config

DEST specifies where to mount the fork:
  /path/to/mount   Mountpoint path (name inferred from basename)
  NAME             Service name (mounted at /tmp/NAME)
  (omitted)        Auto-name from CLI, mount at /tmp/<auto-name>

Examples:
  # Fork a mounted filesystem
  tigerfs fork /tmp/prod my-fork

  # Fork to a specific mountpoint
  tigerfs fork /tmp/prod /mnt/data

  # Fork with explicit name and mountpoint
  tigerfs fork /tmp/prod /mnt/data --name my-fork

  # Fork by service ID
  tigerfs fork tiger:abc123 my-fork

  # Fork without mounting
  tigerfs fork /tmp/prod --no-mount

  # Fork with auto-generated name
  tigerfs fork /tmp/prod

  # Fork with JSON output
  tigerfs fork /tmp/prod my-fork --json`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Resolve SOURCE to a backend + service ID.
			b, sourceID, sourceDesc, err := resolveForkSource(args[0], cfg)
			if err != nil {
				return err
			}

			// Resolve DEST to a service name and mountpoint.
			forkName, mountpoint := resolveForkDest(args, nameFlag)

			if !jsonOutput {
				fmt.Fprintf(cmd.OutOrStdout(), "Forking %s (%s)...\n", sourceDesc, b.Name())
			}

			// Fork the service.
			result, err := b.Fork(cmd.Context(), sourceID, backend.ForkOpts{Name: forkName})
			if err != nil {
				return fmt.Errorf("failed to fork service: %w", err)
			}

			// Determine the display name (from result, or from what we requested).
			displayName := result.Name
			if displayName == "" {
				displayName = forkName
			}

			if jsonOutput {
				return printForkJSON(cmd, b, result, noMount)
			}

			// Print human-readable result.
			out := cmd.OutOrStdout()
			fmt.Fprintln(out, "Fork created!")
			fmt.Fprintln(out)
			fmt.Fprintf(out, "Service ID:   %s\n", result.ID)
			if result.Name != "" {
				fmt.Fprintf(out, "Service Name: %s\n", result.Name)
			}
			fmt.Fprintf(out, "Status:       %s\n", result.Status)
			if result.Region != "" {
				fmt.Fprintf(out, "Region:       %s\n", result.Region)
			}
			if result.Type != "" {
				fmt.Fprintf(out, "Type:         %s\n", result.Type)
			}
			fmt.Fprintf(out, "Source:       %s\n", sourceID)

			if noMount {
				fmt.Fprintln(out)
				fmt.Fprintf(out, "To mount: tigerfs mount %s:%s /path/to/mountpoint\n", b.CLIName(), result.ID)
				return nil
			}

			// Resolve the mountpoint if we haven't yet (auto-name case).
			if mountpoint == "" {
				if displayName == "" {
					fmt.Fprintln(out)
					fmt.Fprintf(out, "To mount: tigerfs mount %s:%s /path/to/mountpoint\n", b.CLIName(), result.ID)
					return nil
				}
				mountpoint = filepath.Join(os.TempDir(), displayName)
			}

			fmt.Fprintln(out)
			fmt.Fprintf(out, "Mounting at %s...\n", mountpoint)

			if err := startMountProcess(b.CLIName(), result.ID, mountpoint); err != nil {
				fmt.Fprintf(out, "Mount failed: %v\n", err)
				fmt.Fprintf(out, "To mount manually: tigerfs mount %s:%s %s\n", b.CLIName(), result.ID, mountpoint)
				return nil
			}

			fmt.Fprintf(out, "Mounted. Run `tigerfs info %s` for details.\n", mountpoint)
			return nil
		},
	}

	cmd.Flags().StringVar(&nameFlag, "name", "", "override the service name for the fork")
	cmd.Flags().BoolVar(&noMount, "no-mount", false, "fork the service but don't mount it")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")

	return cmd
}

// resolveForkSource determines the backend and source service ID from
// the SOURCE argument.
//
// Returns (backend, serviceID, humanDescription, error).
func resolveForkSource(source string, cfg *config.Config) (backend.Backend, string, string, error) {
	// Path detection: starts with . or / → mountpoint lookup.
	if isPath(source) {
		return resolveForkSourceFromMount(source)
	}

	// Prefix detection: tiger:X or ghost:X → backend reference.
	b, id, err := backend.Resolve(source)
	if err != nil {
		return nil, "", "", err
	}
	if b != nil {
		return b, id, fmt.Sprintf("%s:%s", b.CLIName(), id), nil
	}

	// Bare name → default_backend config.
	b, err = backend.ForName(cfg.DefaultBackend)
	if err != nil {
		return nil, "", "", fmt.Errorf("no backend specified.\n"+
			"Set default_backend in ~/.config/tigerfs/config.yaml or use a prefix: tigerfs fork tiger:%s", source)
	}
	return b, source, source, nil
}

// resolveForkSourceFromMount looks up a mountpoint in the registry and
// returns its backend and service ID.
func resolveForkSourceFromMount(mountpoint string) (backend.Backend, string, string, error) {
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to resolve mountpoint path: %w", err)
	}

	registry, err := mount.NewRegistry("")
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to open registry: %w", err)
	}

	entry, err := registry.Get(absMountpoint)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to look up mount: %w", err)
	}
	if entry == nil {
		return nil, "", "", fmt.Errorf("no TigerFS mount found at %s", absMountpoint)
	}

	if entry.CLIBackend == "" || entry.ServiceID == "" {
		return nil, "", "", fmt.Errorf("cannot fork — filesystem at %s has no cloud service.\n"+
			"Fork requires a Tiger Cloud or Ghost backed filesystem.", absMountpoint)
	}

	b, err := backend.ForName(entry.CLIBackend)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to resolve backend %q: %w", entry.CLIBackend, err)
	}

	desc := fmt.Sprintf("filesystem at %s", absMountpoint)
	return b, entry.ServiceID, desc, nil
}

// resolveForkDest determines the fork service name and mountpoint from
// the DEST argument and --name flag.
//
// Returns (serviceName, mountpoint). Either may be empty:
//   - serviceName empty = auto-generate
//   - mountpoint empty = determine after fork from result name
func resolveForkDest(args []string, nameFlag string) (string, string) {
	if len(args) < 2 {
		// No DEST — auto-name, mountpoint determined after fork.
		return nameFlag, ""
	}

	dest := args[1]

	if isPath(dest) {
		// DEST is a path → mountpoint. Name from --name flag or basename.
		abs, err := filepath.Abs(dest)
		if err != nil {
			abs = dest
		}
		name := nameFlag
		if name == "" {
			name = filepath.Base(abs)
		}
		return name, abs
	}

	// DEST is a name → service name, mount at /tmp/NAME.
	name := dest
	if nameFlag != "" {
		name = nameFlag
	}
	return name, filepath.Join(os.TempDir(), dest)
}

// isPath returns true if s looks like a filesystem path (starts with . or /).
// Per ADR-013, only a leading . or / triggers path interpretation.
func isPath(s string) bool {
	return strings.HasPrefix(s, ".") || strings.HasPrefix(s, "/")
}

// printForkJSON outputs the fork result as JSON.
func printForkJSON(cmd *cobra.Command, b backend.Backend, result *backend.ForkResult, noMount bool) error {
	output := struct {
		backend.ForkResult
		Backend string `json:"backend"`
		NoMount bool   `json:"no_mount,omitempty"`
	}{
		ForkResult: *result,
		Backend:    b.CLIName(),
		NoMount:    noMount,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal fork result: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}
