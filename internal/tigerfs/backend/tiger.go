package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// TigerBackend implements Backend for Tiger Cloud services via the tiger CLI.
//
// CLI commands used:
//   - GetConnectionString: tiger db connection-string <id> --with-password
//   - GetInfo: tiger service get <id> --output json
//   - Create: tiger service create --name <n> --output json --with-password
//   - Fork: tiger service fork <id> --now --name <n> --output json --with-password
//
// Tiger's create and fork commands wait by default (30m timeout).
// Use --no-wait to skip waiting.
type TigerBackend struct{}

func (t *TigerBackend) Name() string    { return "Tiger Cloud" }
func (t *TigerBackend) CLIName() string { return "tiger" }

// GetConnectionString retrieves a PostgreSQL connection string for a Tiger Cloud service.
//
// Calls: tiger db connection-string <id> --with-password
// The --with-password flag is required because tiger excludes passwords by default.
func (t *TigerBackend) GetConnectionString(ctx context.Context, id string) (string, error) {
	if err := checkCLIInstalled("tiger"); err != nil {
		return "", err
	}

	cmd := exec.CommandContext(ctx, "tiger", "db", "connection-string",
		id, "--with-password")
	output, err := cmd.Output()
	if err != nil {
		return "", formatCLIError("tiger", err)
	}

	connStr := strings.TrimSpace(string(output))
	if connStr == "" {
		return "", errors.New("tiger CLI returned empty connection string")
	}
	return connStr, nil
}

// tigerServiceJSON is the JSON structure returned by tiger service get/create/fork.
type tigerServiceJSON struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Region string `json:"region_code"`
	Type   string `json:"service_type"`

	// Connection info from spec
	Spec struct {
		Hostname string `json:"hostname"`
		Port     int    `json:"port"`
		DBName   string `json:"dbname"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"spec"`

	// Connection string (when --with-password is used)
	ConnectionString string `json:"connection_string"`
}

func (s *tigerServiceJSON) toServiceInfo() *ServiceInfo {
	return &ServiceInfo{
		ID:     s.ID,
		Name:   s.Name,
		Status: s.Status,
		Region: s.Region,
		Host:   s.Spec.Hostname,
		Port:   s.Spec.Port,
		DB:     s.Spec.DBName,
		User:   s.Spec.Username,
		Type:   s.Type,
	}
}

// GetInfo fetches live service info from Tiger Cloud.
//
// Calls: tiger service get <id> --output json
func (t *TigerBackend) GetInfo(ctx context.Context, id string) (*ServiceInfo, error) {
	if err := checkCLIInstalled("tiger"); err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, "tiger", "service", "get", id, "--output", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, formatCLIError("tiger", err)
	}

	var svc tigerServiceJSON
	if err := json.Unmarshal(output, &svc); err != nil {
		return nil, fmt.Errorf("failed to parse tiger service info: %w", err)
	}

	return svc.toServiceInfo(), nil
}

// Create provisions a new Tiger Cloud database service.
//
// Calls: tiger service create [--name <n>] --output json --with-password
// Tiger's create waits by default (30m timeout).
func (t *TigerBackend) Create(ctx context.Context, opts CreateOpts) (*CreateResult, error) {
	if err := checkCLIInstalled("tiger"); err != nil {
		return nil, err
	}

	args := []string{"service", "create", "--output", "json", "--with-password"}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}

	cmd := exec.CommandContext(ctx, "tiger", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, formatCLIError("tiger", err)
	}

	var svc tigerServiceJSON
	if err := json.Unmarshal(output, &svc); err != nil {
		return nil, fmt.Errorf("failed to parse tiger create response: %w", err)
	}

	return &CreateResult{
		ServiceInfo: *svc.toServiceInfo(),
		ConnStr:     svc.ConnectionString,
	}, nil
}

// Fork creates a copy of an existing Tiger Cloud database service.
//
// Calls: tiger service fork <sourceID> --now [--name <n>] --output json --with-password
// Tiger's fork waits by default (30m timeout). Always uses --now strategy.
func (t *TigerBackend) Fork(ctx context.Context, sourceID string, opts ForkOpts) (*ForkResult, error) {
	if err := checkCLIInstalled("tiger"); err != nil {
		return nil, err
	}

	args := []string{"service", "fork", sourceID, "--now", "--output", "json", "--with-password"}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}

	cmd := exec.CommandContext(ctx, "tiger", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, formatCLIError("tiger", err)
	}

	var svc tigerServiceJSON
	if err := json.Unmarshal(output, &svc); err != nil {
		return nil, fmt.Errorf("failed to parse tiger fork response: %w", err)
	}

	return &ForkResult{
		ServiceInfo: *svc.toServiceInfo(),
		ConnStr:     svc.ConnectionString,
		SourceID:    sourceID,
	}, nil
}
