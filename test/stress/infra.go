package main

import (
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
)

const (
	infoFilePath     = "/tmp/tigerfs-stress.info"
	pgPort           = "5433"
	pgUser           = "testundo"
	pgPassword       = "testundo"
	pgDatabase       = "testundo"
	pgReadyTimeout   = 30 * time.Second
	mountWaitTimeout = 15 * time.Second
)

// Infra holds the state of the running infrastructure.
type Infra struct {
	Mountpoint  string
	TigerFSPid  int
	ComposePath string
	RepoRoot    string
	tigerfsCmd  *exec.Cmd
	sigChan     chan os.Signal
}

// RunInfo is serialized to the info file for the stop command.
type RunInfo struct {
	TigerFSPid  int    `json:"tigerfs_pid"`
	Mountpoint  string `json:"mountpoint"`
	ComposePath string `json:"compose_path"`
	RepoRoot    string `json:"repo_root"`
}

// SetupInfra builds tigerfs, starts Docker PostgreSQL, mounts the filesystem,
// and creates the workspace.
func SetupInfra(cfg *Config) (*Infra, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return nil, fmt.Errorf("find repo root: %w", err)
	}

	composePath := filepath.Join(repoRoot, "test", "stress", "docker-compose.yml")
	mountpoint := fmt.Sprintf("/tmp/tigerfs-stress-%d", time.Now().Unix())
	tigerBinary := filepath.Join(repoRoot, "bin", "tigerfs")

	infra := &Infra{
		Mountpoint:  mountpoint,
		ComposePath: composePath,
		RepoRoot:    repoRoot,
		sigChan:     make(chan os.Signal, 1),
	}

	// Step 1: Build tigerfs
	fmt.Print("Building tigerfs... ")
	if err := runCmd(repoRoot, "go", "build", "-o", tigerBinary, "./cmd/tigerfs"); err != nil {
		return nil, fmt.Errorf("build tigerfs: %w", err)
	}
	fmt.Println("done")

	// Step 2: Start Docker PostgreSQL
	fmt.Print("Starting PostgreSQL... ")
	if err := runCmd(repoRoot, "docker", "compose", "-f", composePath, "up", "-d"); err != nil {
		return nil, fmt.Errorf("docker compose up: %w", err)
	}
	fmt.Println("done")

	// Step 3: Wait for PostgreSQL
	fmt.Print("Waiting for PostgreSQL... ")
	if err := waitForPostgres(); err != nil {
		infra.teardownDocker()
		return nil, fmt.Errorf("postgres not ready: %w", err)
	}
	fmt.Println("ready")

	// Step 4: Create mountpoint
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		infra.teardownDocker()
		return nil, fmt.Errorf("create mountpoint: %w", err)
	}

	// Step 5: Mount TigerFS
	fmt.Print("Mounting TigerFS... ")
	connStr := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s", pgUser, pgPassword, pgPort, pgDatabase)

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

func (infra *Infra) teardownDocker() {
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
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return os.WriteFile(infoFilePath, data, 0644)
}

func (infra *Infra) removeInfo() {
	os.Remove(infoFilePath)
}

// StopInfra reads the info file and tears down a running stress test.
func StopInfra() error {
	data, err := os.ReadFile(infoFilePath)
	if err != nil {
		return fmt.Errorf("no running test found (cannot read %s): %w", infoFilePath, err)
	}

	var info RunInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("parse info file: %w", err)
	}

	fmt.Printf("Found running test: pid=%d mountpoint=%s\n", info.TigerFSPid, info.Mountpoint)

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

	// Docker down
	fmt.Print("  Stopping Docker... ")
	if err := runCmd(info.RepoRoot, "docker", "compose", "-f", info.ComposePath, "down", "-v"); err != nil {
		fmt.Printf("warning: %v\n", err)
	} else {
		fmt.Println("done")
	}

	// Remove info file
	os.Remove(infoFilePath)

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
