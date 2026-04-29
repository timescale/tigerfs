// tigerfs-stress is a comprehensive stress test for TigerFS file-first workspaces.
// It exercises all filesystem operations (create, edit, rename, move, delete) and
// all undo operations (single, to-id, to-savepoint) with deterministic PRNG-seeded
// randomization and hash-based verification.
//
// Build: go build -o bin/tigerfs-stress ./test/stress
// Usage: bin/tigerfs-stress start [--seed N] [--iterations N] [--debug]
//
//	bin/tigerfs-stress stop
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all CLI options for the stress test.
//
// DumpAtSpec is the raw `--dump-at` flag value (kept for the startup
// banner / replay command). DumpAt is the parsed lookup set used during
// the run: a key per iteration where a manual snapshot should fire.
type Config struct {
	Seed          int64
	Iterations    int
	Debug         bool
	Keep          bool
	Workspace     string
	ValidateEvery int
	LargeFiles    bool
	ManyFiles     bool
	DumpAtSpec    string
	DumpAt        map[int]bool
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "start":
		cfg := parseStartFlags(os.Args[2:])
		os.Exit(runStart(cfg))
	case "stop":
		os.Exit(runStop())
	case "-h", "--help", "help":
		printUsage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func parseStartFlags(args []string) *Config {
	cfg := &Config{}
	fs := flag.NewFlagSet("start", flag.ExitOnError)

	fs.Int64Var(&cfg.Seed, "seed", 0, "PRNG seed (0 = generate from time)")
	fs.IntVar(&cfg.Iterations, "iterations", 20, "number of operation rounds")
	fs.BoolVar(&cfg.Debug, "debug", false, "pass --log-level debug to tigerfs")
	fs.BoolVar(&cfg.Keep, "keep", false, "don't tear down Docker/mount on exit")
	fs.StringVar(&cfg.Workspace, "workspace", "testws", "workspace name")
	fs.IntVar(&cfg.ValidateEvery, "validate-every", 1, "validate workspace every N ops (undo always validates)")
	fs.BoolVar(&cfg.LargeFiles, "large-files", false, "enable large file generation (up to 10MB)")
	fs.BoolVar(&cfg.ManyFiles, "many-files", false, "enable dense directories (up to 1000 files/dir)")
	fs.StringVar(&cfg.DumpAtSpec, "dump-at", "", "comma-separated iteration numbers to write a snapshot dump after (e.g., 100,250)")

	fs.Parse(args)

	// Generate seed if not provided
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}

	cfg.DumpAt = parseDumpAtSpec(cfg.DumpAtSpec, cfg.Iterations)

	return cfg
}

// parseDumpAtSpec splits a comma-separated iteration list into a
// deduplicated lookup set. Empty input returns nil (no snapshots).
// Invalid entries (non-integer, <=0, or > max) are warned to stderr and
// skipped so a typo doesn't silently neutralize the flag.
func parseDumpAtSpec(spec string, maxIter int) map[int]bool {
	if spec == "" {
		return nil
	}
	out := map[int]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n <= 0 {
			fmt.Fprintf(os.Stderr, "[WARN] --dump-at: ignoring invalid iteration %q\n", part)
			continue
		}
		if n > maxIter {
			fmt.Fprintf(os.Stderr, "[WARN] --dump-at %d: past --iterations %d, will never fire\n", n, maxIter)
			continue
		}
		out[n] = true
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func runStart(cfg *Config) int {
	fmt.Printf("=== tigerfs-stress ===\n")
	fmt.Printf("Seed:       %d\n", cfg.Seed)
	fmt.Printf("Iterations: %d\n", cfg.Iterations)
	fmt.Printf("Workspace:  %s\n", cfg.Workspace)
	fmt.Printf("Debug:      %v\n", cfg.Debug)
	fmt.Printf("LargeFiles: %v\n", cfg.LargeFiles)
	fmt.Printf("ManyFiles:  %v\n", cfg.ManyFiles)
	fmt.Printf("Validate:   every %d ops\n", cfg.ValidateEvery)
	if len(cfg.DumpAt) > 0 {
		fmt.Printf("DumpAt:     %s\n", cfg.DumpAtSpec)
	}
	fmt.Println()

	// Set up infrastructure
	infra, err := SetupInfra(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Infrastructure setup failed: %v\n", err)
		return 2
	}

	fmt.Printf("Infrastructure ready. Mountpoint: %s\n", infra.Mountpoint)
	fmt.Printf("Workspace path: %s/%s\n", infra.Mountpoint, cfg.Workspace)
	fmt.Println()

	// Run test iterations. RunAndExit returns KeepInfra=true on a
	// validation failure (so the user can inspect live state alongside
	// the failure dump); we honour both that signal and the explicit
	// --keep flag.
	res := RunAndExit(cfg, infra)
	if cfg.Keep || res.KeepInfra {
		fmt.Printf("\nInfrastructure left running at %s (use 'bin/tigerfs-stress stop' to tear down)\n", infra.Mountpoint)
	} else {
		infra.Teardown()
	}
	return res.ExitCode
}

func runStop() int {
	err := StopInfra()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Stop failed: %v\n", err)
		return 2
	}
	fmt.Println("Stopped.")
	return 0
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `tigerfs-stress - stress test for TigerFS file-first workspaces

Usage:
  tigerfs-stress start [OPTIONS]    Start infrastructure, run test, teardown
  tigerfs-stress stop               Kill running test and teardown infrastructure

Options:
  --seed N              PRNG seed for reproducibility (0 = random)
  --iterations N        Number of operation rounds (default: 20)
  --debug               Pass --log-level debug to tigerfs
  --keep                Don't tear down on exit
  --workspace NAME      Workspace name (default: testws)
  --validate-every N    Validate every N ops (default: 1; undo always validates)
  --large-files         Enable large files up to 10MB (default max: 100KB)
  --many-files          Enable dense directories up to 1000 files/dir (default: 10)
  --dump-at LIST        Write a snapshot dump after these iterations (e.g., 100,250)

Examples:
  tigerfs-stress start
  tigerfs-stress start --seed 42 --iterations 50
  tigerfs-stress start --large-files --many-files --iterations 100 --validate-every 5
  tigerfs-stress start --debug --keep --seed 42
  tigerfs-stress start --seed 42 --iterations 1000 --dump-at 100,500,778
  tigerfs-stress stop
`)
}
