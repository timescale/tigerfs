package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// GhostBackend implements Backend for Ghost databases via the ghost CLI.
//
// CLI commands used:
//   - GetConnectionString: ghost connect <id> (password from ~/.pgpass)
//   - GetInfo: ghost list --json (filter by ID — ghost has no get command)
//   - Create: ghost create --name <n> --wait --json
//   - Fork: ghost fork <id> --name <n> --wait --json
//
// Ghost's create and fork require explicit --wait (unlike Tiger which waits by default).
// Ghost connect includes passwords automatically from ~/.pgpass.
type GhostBackend struct{}

func (g *GhostBackend) Name() string    { return "Ghost" }
func (g *GhostBackend) CLIName() string { return "ghost" }

// GetConnectionString retrieves a PostgreSQL connection string for a Ghost database.
//
// Calls: ghost connect <id>
// Ghost includes the password from ~/.pgpass automatically — no --with-password flag.
func (g *GhostBackend) GetConnectionString(ctx context.Context, id string) (string, error) {
	if err := checkCLIInstalled("ghost"); err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, "ghost", "connect", id)
	output, err := cmd.Output()
	if err != nil {
		return "", formatCLIError("ghost", err)
	}

	connStr := strings.TrimSpace(string(output))
	if connStr == "" {
		return "", errors.New("ghost CLI returned empty connection string")
	}
	return connStr, nil
}

// ghostDatabaseJSON is the JSON structure returned by ghost list/create/fork.
type ghostDatabaseJSON struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Region string `json:"region"`
	Host   string `json:"hostname"`
	Port   int    `json:"port"`
}

func (d *ghostDatabaseJSON) toServiceInfo() *ServiceInfo {
	return &ServiceInfo{
		ID:     d.ID,
		Name:   d.Name,
		Status: d.Status,
		Region: d.Region,
		Host:   d.Host,
		Port:   d.Port,
	}
}

// GetInfo fetches live database info from Ghost.
//
// Calls: ghost list --json
// Ghost has no single-database get command, so we list all and filter by ID.
func (g *GhostBackend) GetInfo(ctx context.Context, id string) (*ServiceInfo, error) {
	if err := checkCLIInstalled("ghost"); err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, "ghost", "list", "--json")
	output, err := cmd.Output()
	if err != nil {
		return nil, formatCLIError("ghost", err)
	}

	var databases []ghostDatabaseJSON
	if err := json.Unmarshal(output, &databases); err != nil {
		return nil, fmt.Errorf("failed to parse ghost list response: %w", err)
	}

	for _, db := range databases {
		if db.ID == id {
			return db.toServiceInfo(), nil
		}
	}

	return nil, fmt.Errorf("service not found: no ghost database with ID %q", id)
}

// Create provisions a new Ghost database.
//
// Calls: ghost create [--name <n>] --wait --json
// Ghost requires explicit --wait (does not wait by default).
// Connection string is fetched separately via ghost connect after creation.
func (g *GhostBackend) Create(ctx context.Context, opts CreateOpts) (*CreateResult, error) {
	if err := checkCLIInstalled("ghost"); err != nil {
		return nil, err
	}

	args := []string{"create", "--wait", "--json"}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}

	cmd := exec.CommandContext(ctx, "ghost", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, formatCLIError("ghost", err)
	}

	var db ghostDatabaseJSON
	if err := json.Unmarshal(output, &db); err != nil {
		return nil, fmt.Errorf("failed to parse ghost create response: %w", err)
	}

	// Fetch connection string separately — ghost create doesn't include it.
	connStr, err := g.GetConnectionString(ctx, db.ID)
	if err != nil {
		return nil, fmt.Errorf("database created but failed to get connection string: %w", err)
	}

	return &CreateResult{
		ServiceInfo: *db.toServiceInfo(),
		ConnStr:     connStr,
	}, nil
}

// Fork creates a copy of an existing Ghost database.
//
// Calls: ghost fork <sourceID> [--name <n>] --wait --json
// Ghost requires explicit --wait. No fork strategy flag (always forks current state).
// Connection string is fetched separately via ghost connect after fork.
func (g *GhostBackend) Fork(ctx context.Context, sourceID string, opts ForkOpts) (*ForkResult, error) {
	if err := checkCLIInstalled("ghost"); err != nil {
		return nil, err
	}

	args := []string{"fork", sourceID, "--wait", "--json"}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}

	cmd := exec.CommandContext(ctx, "ghost", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, formatCLIError("ghost", err)
	}

	var db ghostDatabaseJSON
	if err := json.Unmarshal(output, &db); err != nil {
		return nil, fmt.Errorf("failed to parse ghost fork response: %w", err)
	}

	// Fetch connection string separately — ghost fork doesn't include it.
	connStr, err := g.GetConnectionString(ctx, db.ID)
	if err != nil {
		return nil, fmt.Errorf("fork created but failed to get connection string: %w", err)
	}

	return &ForkResult{
		ServiceInfo: *db.toServiceInfo(),
		ConnStr:     connStr,
		SourceID:    sourceID,
	}, nil
}
