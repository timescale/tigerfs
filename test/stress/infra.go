package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultInfoFileName  = "tigerfs-stress.info"
	defaultDumpDir       = "/tmp"
	defaultMountpointDir = "/tmp"
	pgPort               = "5433"
	pgUser               = "testundo"
	pgPassword           = "testundo"
	pgDatabase           = "testundo"
	pgReadyTimeout       = 30 * time.Second
	mountWaitTimeout     = 15 * time.Second
)

// envInfoFilePath is the env var that overrides the info-file location.
// scripts/stress-docker.sh sets this so `tigerfs-stress stop` inside the
// container reads the same path that `start` wrote on the bind-mounted /out.
const envInfoFilePath = "TIGERFS_STRESS_INFO_FILE"

// resolveInfoFilePath returns the info file path, honoring (in order):
// the env var override, the supplied dumpDir, then the legacy /tmp default.
func resolveInfoFilePath(dumpDir string) string {
	if v := os.Getenv(envInfoFilePath); v != "" {
		return v
	}
	if dumpDir == "" {
		dumpDir = defaultDumpDir
	}
	return filepath.Join(dumpDir, defaultInfoFileName)
}

// Infra holds the state of the running infrastructure.
type Infra struct {
	Mountpoint  string
	TigerFSPid  int
	ComposePath string
	RepoRoot    string
	ConnStr     string // postgres URL used by the mounted tigerfs (also reusable for diagnostics)
	// External is true when --external-conn-str was supplied: the runner
	// neither owns the postgres lifecycle nor will it run `go build`. The
	// launcher (scripts/stress-docker.sh or whoever set the flag) is
	// responsible for those concerns.
	External     bool
	DumpDir      string // resolved dump directory (defaults to /tmp).
	InfoFilePath string // resolved info-file path (under DumpDir or env override).
	tigerfsCmd   *exec.Cmd
	sigChan      chan os.Signal
}

// RunInfo is serialized to the info file for the stop command.
// External determines whether `stop` should also run `docker compose down`.
type RunInfo struct {
	TigerFSPid  int    `json:"tigerfs_pid"`
	Mountpoint  string `json:"mountpoint"`
	ComposePath string `json:"compose_path"`
	RepoRoot    string `json:"repo_root"`
	External    bool   `json:"external,omitempty"`
}

// SetupInfra builds tigerfs, starts Docker PostgreSQL, mounts the filesystem,
// and creates the workspace.
func SetupInfra(cfg *Config) (*Infra, error) {
	external := cfg.ExternalConnStr != ""

	// repoRoot is only required when we actually need to `go build` or
	// `docker compose up` from the repo. In external mode (the docker-FUSE
	// launcher path) neither happens, so a missing go.mod is not fatal --
	// we fall back to the current working directory.
	repoRoot, err := findRepoRoot()
	if err != nil {
		if !external && cfg.TigerFSBinary == "" {
			return nil, fmt.Errorf("find repo root: %w", err)
		}
		if cwd, cwdErr := os.Getwd(); cwdErr == nil {
			repoRoot = cwd
		}
	}

	composePath := filepath.Join(repoRoot, "test", "stress", "docker-compose.yml")

	mountBase := cfg.MountpointDir
	if mountBase == "" {
		mountBase = defaultMountpointDir
	}
	mountpoint := filepath.Join(mountBase, fmt.Sprintf("tigerfs-stress-%d", time.Now().Unix()))

	dumpDir := cfg.DumpDir
	if dumpDir == "" {
		dumpDir = defaultDumpDir
	}

	infra := &Infra{
		Mountpoint:   mountpoint,
		ComposePath:  composePath,
		RepoRoot:     repoRoot,
		External:     external,
		DumpDir:      dumpDir,
		InfoFilePath: resolveInfoFilePath(dumpDir),
		sigChan:      make(chan os.Signal, 1),
	}

	// Step 1: Build tigerfs (skipped when --tigerfs-binary is supplied).
	var tigerBinary string
	if cfg.TigerFSBinary != "" {
		if _, err := os.Stat(cfg.TigerFSBinary); err != nil {
			return nil, fmt.Errorf("--tigerfs-binary %q not found: %w", cfg.TigerFSBinary, err)
		}
		tigerBinary = cfg.TigerFSBinary
		fmt.Printf("Using tigerfs binary: %s\n", tigerBinary)
	} else {
		tigerBinary = filepath.Join(repoRoot, "bin", "tigerfs")
		fmt.Print("Building tigerfs... ")
		if err := runCmd(repoRoot, "go", "build", "-o", tigerBinary, "./cmd/tigerfs"); err != nil {
			return nil, fmt.Errorf("build tigerfs: %w", err)
		}
		fmt.Println("done")
	}

	// Steps 2 & 3: Start Docker PostgreSQL and wait for it (skipped when
	// --external-conn-str is supplied; the launcher owns postgres in that case).
	var connStr string
	if external {
		connStr = cfg.ExternalConnStr
		fmt.Printf("Using external postgres: %s\n", redactConnStr(connStr))
		fmt.Print("Verifying postgres reachable... ")
		if err := pingExternal(connStr); err != nil {
			return nil, fmt.Errorf("external postgres not reachable: %w", err)
		}
		fmt.Println("ok")
	} else {
		fmt.Print("Starting PostgreSQL... ")
		if err := runCmd(repoRoot, "docker", "compose", "-f", composePath, "up", "-d"); err != nil {
			return nil, fmt.Errorf("docker compose up: %w", err)
		}
		fmt.Println("done")

		fmt.Print("Waiting for PostgreSQL... ")
		if err := waitForPostgres(); err != nil {
			infra.teardownDocker()
			return nil, fmt.Errorf("postgres not ready: %w", err)
		}
		fmt.Println("ready")

		connStr = fmt.Sprintf("postgres://%s:%s@localhost:%s/%s", pgUser, pgPassword, pgPort, pgDatabase)
	}

	// Step 4: Create mountpoint
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		infra.teardownDocker()
		return nil, fmt.Errorf("create mountpoint: %w", err)
	}

	// Step 5: Mount TigerFS
	fmt.Print("Mounting TigerFS... ")
	infra.ConnStr = connStr

	args := []string{"mount", "--insecure-no-ssl", "--user-id", "stress-test"}
	if cfg.Debug {
		args = append(args, "--log-level", "debug")
	}
	args = append(args, connStr, mountpoint)

	logFile, err := os.Create("tigerfs.log")
	if err != nil {
		infra.teardownDocker()
		return nil, fmt.Errorf("create tigerfs.log: %w", err)
	}

	infra.tigerfsCmd = exec.Command(tigerBinary, args...)
	infra.tigerfsCmd.Stdout = logFile
	infra.tigerfsCmd.Stderr = logFile
	infra.tigerfsCmd.Dir = repoRoot

	if err := infra.tigerfsCmd.Start(); err != nil {
		logFile.Close()
		infra.teardownDocker()
		return nil, fmt.Errorf("start tigerfs: %w", err)
	}
	infra.TigerFSPid = infra.tigerfsCmd.Process.Pid
	fmt.Printf("pid=%d ", infra.TigerFSPid)

	// Wait for mount to appear
	if err := waitForMount(mountpoint); err != nil {
		infra.killTigerFS()
		infra.teardownDocker()
		return nil, fmt.Errorf("mount not ready: %w", err)
	}
	fmt.Println("mounted")

	// Step 6: Create workspace
	fmt.Printf("Creating workspace '%s'... ", cfg.Workspace)
	buildPath := filepath.Join(mountpoint, ".build", cfg.Workspace)
	if err := os.WriteFile(buildPath, []byte("markdown,history\n"), 0644); err != nil {
		infra.killTigerFS()
		infra.unmount()
		infra.teardownDocker()
		return nil, fmt.Errorf("create workspace: %w", err)
	}

	// Verify workspace exists
	wsPath := filepath.Join(mountpoint, cfg.Workspace)
	if _, err := os.Stat(wsPath); err != nil {
		infra.killTigerFS()
		infra.unmount()
		infra.teardownDocker()
		return nil, fmt.Errorf("workspace not found after creation: %w", err)
	}
	fmt.Println("done")

	// Step 7: Write run info for stop command
	if err := infra.writeInfo(); err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] Failed to write info file: %v\n", err)
	}

	// Step 8: Set up signal handling
	signal.Notify(infra.sigChan, syscall.SIGINT, syscall.SIGTERM)

	return infra, nil
}

// Teardown shuts down all infrastructure in order.
func (infra *Infra) Teardown() {
	fmt.Println("\nTearing down...")
	infra.killTigerFS()
	infra.unmount()
	infra.removeMountpoint()
	infra.teardownDocker()
	infra.removeInfo()
	fmt.Println("Teardown complete.")
}

// WaitForSignal blocks until SIGINT or SIGTERM is received.
func (infra *Infra) WaitForSignal() {
	sig := <-infra.sigChan
	fmt.Printf("\nReceived %s\n", sig)
}

func (infra *Infra) killTigerFS() {
	if infra.tigerfsCmd == nil || infra.tigerfsCmd.Process == nil {
		return
	}

	pid := infra.tigerfsCmd.Process.Pid
	fmt.Printf("  Stopping tigerfs (pid %d)... ", pid)

	// Try graceful shutdown first
	infra.tigerfsCmd.Process.Signal(syscall.SIGTERM)

	// Wait up to 5 seconds
	done := make(chan error, 1)
	go func() { done <- infra.tigerfsCmd.Wait() }()

	select {
	case <-done:
		fmt.Println("stopped")
	case <-time.After(5 * time.Second):
		fmt.Print("force killing... ")
		infra.tigerfsCmd.Process.Kill()
		<-done
		fmt.Println("killed")
	}
}

func (infra *Infra) unmount() {
	fmt.Printf("  Unmounting %s... ", infra.Mountpoint)

	var err error
	if runtime.GOOS == "darwin" {
		err = exec.Command("diskutil", "unmount", "force", infra.Mountpoint).Run()
	} else {
		err = exec.Command("umount", infra.Mountpoint).Run()
	}

	if err != nil {
		fmt.Printf("warning: %v\n", err)
	} else {
		fmt.Println("done")
	}
}

func (infra *Infra) removeMountpoint() {
	os.RemoveAll(infra.Mountpoint)
}

// teardownDocker stops the postgres compose stack. No-op in external mode --
// the caller (e.g. scripts/stress-docker.sh) owns postgres lifecycle there.
func (infra *Infra) teardownDocker() {
	if infra.External {
		return
	}
	fmt.Print("  Stopping Docker... ")
	if err := runCmd(infra.RepoRoot, "docker", "compose", "-f", infra.ComposePath, "down", "-v"); err != nil {
		fmt.Printf("warning: %v\n", err)
	} else {
		fmt.Println("done")
	}
}

func (infra *Infra) writeInfo() error {
	info := RunInfo{
		TigerFSPid:  infra.TigerFSPid,
		Mountpoint:  infra.Mountpoint,
		ComposePath: infra.ComposePath,
		RepoRoot:    infra.RepoRoot,
		External:    infra.External,
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return os.WriteFile(infra.InfoFilePath, data, 0644)
}

func (infra *Infra) removeInfo() {
	os.Remove(infra.InfoFilePath)
}

// StopInfra reads the info file and tears down a running stress test. The
// info-file path follows resolveInfoFilePath: TIGERFS_STRESS_INFO_FILE env
// var wins; otherwise the legacy /tmp/tigerfs-stress.info default is used.
func StopInfra() error {
	infoPath := resolveInfoFilePath("")
	data, err := os.ReadFile(infoPath)
	if err != nil {
		return fmt.Errorf("no running test found (cannot read %s): %w", infoPath, err)
	}

	var info RunInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("parse info file: %w", err)
	}

	fmt.Printf("Found running test: pid=%d mountpoint=%s external=%v\n",
		info.TigerFSPid, info.Mountpoint, info.External)

	// Kill tigerfs process
	fmt.Printf("  Killing tigerfs (pid %d)... ", info.TigerFSPid)
	proc, err := os.FindProcess(info.TigerFSPid)
	if err == nil {
		proc.Signal(syscall.SIGTERM)
		time.Sleep(2 * time.Second)
		proc.Kill()
		fmt.Println("done")
	} else {
		fmt.Printf("not found: %v\n", err)
	}

	// Unmount
	fmt.Printf("  Unmounting %s... ", info.Mountpoint)
	if runtime.GOOS == "darwin" {
		exec.Command("diskutil", "unmount", "force", info.Mountpoint).Run()
	} else {
		exec.Command("umount", info.Mountpoint).Run()
	}
	fmt.Println("done")

	// Remove mountpoint
	os.RemoveAll(info.Mountpoint)

	// Docker down (skipped when the launcher owns postgres lifecycle).
	if !info.External {
		fmt.Print("  Stopping Docker... ")
		if err := runCmd(info.RepoRoot, "docker", "compose", "-f", info.ComposePath, "down", "-v"); err != nil {
			fmt.Printf("warning: %v\n", err)
		} else {
			fmt.Println("done")
		}
	}

	// Remove info file
	os.Remove(infoPath)

	return nil
}

// Helper functions

func findRepoRoot() (string, error) {
	// Walk up from current dir looking for go.mod
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find go.mod in any parent directory")
		}
		dir = parent
	}
}

func runCmd(dir string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// pingExternal opens a short-lived pgxpool against the supplied connection
// string and Pings it. Used in --external-conn-str mode so a misconfigured
// URL fails immediately with a clear error instead of wedging tigerfs.
func pingExternal(connStr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("parse/connect: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

// redactConnStr replaces the password component (if present) with "***" so
// the startup banner doesn't echo credentials.
func redactConnStr(s string) string {
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s
	}
	colon := strings.LastIndex(s[:at], ":")
	scheme := strings.Index(s, "://")
	if colon < 0 || scheme < 0 || colon <= scheme+2 {
		return s
	}
	return s[:colon+1] + "***" + s[at:]
}

func waitForPostgres() error {
	deadline := time.Now().Add(pgReadyTimeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("pg_isready", "-h", "localhost", "-p", pgPort, "-U", pgUser, "-d", pgDatabase)
		if cmd.Run() == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("PostgreSQL not ready after %s", pgReadyTimeout)
}

func waitForMount(mountpoint string) error {
	deadline := time.Now().Add(mountWaitTimeout)
	for time.Now().Before(deadline) {
		// Check if mount appears in mount table
		out, err := exec.Command("mount").Output()
		if err == nil && strings.Contains(string(out), mountpoint) {
			// Also verify we can stat a path inside the mount
			if _, err := os.Stat(filepath.Join(mountpoint, ".info")); err == nil {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("mount not ready after %s", mountWaitTimeout)
}

// processExists checks if a process with the given PID is running.
func processExists(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, FindProcess always succeeds. Send signal 0 to check.
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}

// formatPID returns a string representation of a PID for display.
func formatPID(pid int) string {
	return strconv.Itoa(pid)
}
