// tigerfs-stress is a comprehensive stress test for TigerFS file-first workspaces.
// It exercises all filesystem operations (create, edit, rename, move, delete) and
// all undo operations (single, to-id, to-savepoint) with deterministic PRNG-seeded
// randomization and hash-based verification.
//
// Build: go build -o bin/tigerfs-stress ./test/stress
// Usage: bin/tigerfs-stress start [--seed N] [--iterations N] [--debug]
//        bin/tigerfs-stress stop
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// Config holds all CLI options for the stress test.
type Config struct {
	Seed          int64
	Iterations    int
	Debug         bool
	Keep          bool
	Workspace     string
	ValidateEvery int
	LargeFiles    bool
	ManyFiles     bool
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

	fs.Parse(args)

	// Generate seed if not provided
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}

	return cfg
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
	fmt.Println()

	// Set up infrastructure
	infra, err := SetupInfra(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Infrastructure setup failed: %v\n", err)
		return 2
	}

	// Ensure teardown on exit (unless --keep)
	if !cfg.Keep {
		defer infra.Teardown()
	} else {
		defer fmt.Printf("\n--keep: infrastructure left running at %s\n", infra.Mountpoint)
	}

	fmt.Printf("Infrastructure ready. Mountpoint: %s\n", infra.Mountpoint)
	fmt.Printf("Workspace path: %s/%s\n", infra.Mountpoint, cfg.Workspace)
	fmt.Println()

	// Run test iterations
	return RunAndExit(cfg, infra)
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

Examples:
  tigerfs-stress start
  tigerfs-stress start --seed 42 --iterations 50
  tigerfs-stress start --large-files --many-files --iterations 100 --validate-every 5
  tigerfs-stress start --debug --keep --seed 42
  tigerfs-stress stop
`)
}
