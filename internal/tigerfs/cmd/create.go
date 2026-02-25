package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/timescale/tigerfs/internal/tigerfs/backend"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// buildCreateCmd creates the create command.
//
// The create command provisions a new database via a cloud backend and
// optionally mounts it. Backend is determined by prefix on the NAME
// argument (tiger:NAME, ghost:NAME) or the default_backend config.
func buildCreateCmd() *cobra.Command {
	var noMount bool
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "create [BACKEND:]NAME [MOUNTPOINT]",
		Short: "Create a new TigerFS filesystem",
		Long: `Create a new database service and mount it as a filesystem.

The backend is determined by prefix on NAME:
  tiger:NAME    Create on Tiger Cloud
  ghost:NAME    Create on Ghost
  NAME          Use default_backend config

If NAME is omitted or the prefix is bare (e.g., "tiger:"), the backend
auto-generates a name. MOUNTPOINT defaults to /tmp/NAME.

Examples:
  # Create on Tiger Cloud, mount at /tmp/my-db
  tigerfs create tiger:my-db

  # Create on Tiger Cloud, mount at current directory
  tigerfs create tiger:my-db .

  # Create with auto-generated name
  tigerfs create tiger:

  # Create using default backend
  tigerfs create my-db

  # Create without mounting
  tigerfs create tiger:my-db --no-mount

  # Create with JSON output
  tigerfs create tiger:my-db --json`,
		Args: cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Parse arguments.
			var nameArg, mountpointArg string
			if len(args) >= 1 {
				nameArg = args[0]
			}
			if len(args) == 2 {
				mountpointArg = args[1]
			}

			// Resolve backend from prefix or default config.
			b, serviceName, err := resolveCreateBackend(nameArg, cfg)
			if err != nil {
				return err
			}

			if !jsonOutput {
				fmt.Fprintf(cmd.OutOrStdout(), "Creating TigerFS filesystem (%s)...\n", b.Name())
			}

			// Create the service.
			result, err := b.Create(cmd.Context(), backend.CreateOpts{Name: serviceName})
			if err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}

			// Determine the display name (from result, or from what we requested).
			displayName := result.Name
			if displayName == "" {
				displayName = serviceName
			}

			if jsonOutput {
				return printCreateJSON(cmd, b, result, noMount)
			}

			// Print human-readable result.
			out := cmd.OutOrStdout()
			fmt.Fprintln(out, "Filesystem created!")
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

			if noMount {
				fmt.Fprintln(out)
				fmt.Fprintf(out, "To mount: tigerfs mount %s:%s /path/to/mountpoint\n", b.CLIName(), result.ID)
				return nil
			}

			// Auto-mount the new filesystem.
			mountpoint, err := resolveCreateMountpoint(mountpointArg, displayName, cfg.DefaultMountDir)
			if err != nil {
				return err
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

	cmd.Flags().BoolVar(&noMount, "no-mount", false, "create the service but don't mount it")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")

	return cmd
}

// resolveCreateBackend determines the backend and service name from the
// create command's first argument and config.
//
// Returns the backend, the service name (may be empty for auto-generate),
// and any error.
func resolveCreateBackend(nameArg string, cfg *config.Config) (backend.Backend, string, error) {
	if nameArg != "" {
		b, name, err := backend.Resolve(nameArg)
		if err != nil {
			return nil, "", err
		}
		if b != nil {
			return b, name, nil
		}
		// Bare name — use default backend.
		b, err = backend.ForName(cfg.DefaultBackend)
		if err != nil {
			return nil, "", fmt.Errorf("no backend specified.\n"+
				"Set default_backend in ~/.config/tigerfs/config.yaml or use a prefix: tigerfs create tiger:%s", nameArg)
		}
		return b, nameArg, nil
	}

	// No argument — default backend with auto-generated name.
	b, err := backend.ForName(cfg.DefaultBackend)
	if err != nil {
		return nil, "", fmt.Errorf("no backend specified.\n" +
			"Set default_backend in ~/.config/tigerfs/config.yaml or use a prefix: tigerfs create tiger:NAME")
	}
	return b, "", nil
}

// resolveCreateMountpoint determines where to mount after creation.
//
// If mountpointArg is given, it's used (resolved to absolute path).
// Otherwise, defaults to baseDir/<name>.
func resolveCreateMountpoint(mountpointArg, name, baseDir string) (string, error) {
	if mountpointArg != "" {
		abs, err := filepath.Abs(mountpointArg)
		if err != nil {
			return "", fmt.Errorf("failed to resolve mountpoint path: %w", err)
		}
		return abs, nil
	}

	if name == "" {
		return "", fmt.Errorf("cannot determine mountpoint: no name and no mountpoint specified")
	}
	return filepath.Join(baseDir, name), nil
}

// startMountProcess starts `tigerfs mount` as a background process.
//
// The mount command blocks (it serves the filesystem), so we start it
// detached. The new process inherits stderr for logging.
func startMountProcess(cliBackend, serviceID, mountpoint string) error {
	// Ensure mountpoint directory exists.
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		return fmt.Errorf("failed to create mountpoint directory: %w", err)
	}

	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to find tigerfs executable: %w", err)
	}

	ref := fmt.Sprintf("%s:%s", cliBackend, serviceID)
	cmd := exec.Command(selfPath, "mount", ref, mountpoint)
	cmd.Stderr = os.Stderr

	logging.Debug("Starting mount process",
		zap.String("ref", ref),
		zap.String("mountpoint", mountpoint))

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount process: %w", err)
	}

	// Detach — don't wait for the child process.
	if err := cmd.Process.Release(); err != nil {
		logging.Warn("Failed to release mount process", zap.Error(err))
	}

	return nil
}

// printCreateJSON outputs the create result as JSON.
func printCreateJSON(cmd *cobra.Command, b backend.Backend, result *backend.CreateResult, noMount bool) error {
	output := struct {
		backend.CreateResult
		Backend string `json:"backend"`
		NoMount bool   `json:"no_mount,omitempty"`
	}{
		CreateResult: *result,
		Backend:      b.CLIName(),
		NoMount:      noMount,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal create result: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(data))
	return nil
}
