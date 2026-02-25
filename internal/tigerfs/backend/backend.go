// Package backend provides a unified interface for cloud database backends
// (Tiger Cloud, Ghost) used by TigerFS to provision, inspect, and connect
// to database services.
//
// Each backend wraps a CLI tool (tiger, ghost) via exec.CommandContext.
// The Backend interface abstracts the differences so commands like mount,
// info, create, and fork work identically regardless of backend.
//
// Use Resolve() to parse a user-provided reference (e.g., "tiger:abc123")
// into a Backend and identifier. Use ForName() to look up a backend by
// name (e.g., from a registry entry's CLIBackend field).
package backend

import (
	"context"
	"time"
)

// Backend is the interface implemented by cloud database backends.
//
// Each method shells out to the backend's CLI tool. Errors from the CLI
// are classified into sentinel errors (ErrCLINotFound, ErrNotAuthenticated)
// or returned as formatted messages.
type Backend interface {
	// Name returns a human-readable backend name (e.g., "Tiger Cloud", "Ghost").
	Name() string

	// CLIName returns the short backend identifier used in prefixes and config
	// (e.g., "tiger", "ghost").
	CLIName() string

	// GetConnectionString resolves a service/database ID to a postgres:// connection string.
	// The returned string includes credentials suitable for direct database connection.
	GetConnectionString(ctx context.Context, id string) (string, error)

	// GetInfo fetches live service/database info from the cloud provider.
	GetInfo(ctx context.Context, id string) (*ServiceInfo, error)

	// Create provisions a new database service.
	Create(ctx context.Context, opts CreateOpts) (*CreateResult, error)

	// Fork creates a copy of an existing database service.
	Fork(ctx context.Context, sourceID string, opts ForkOpts) (*ForkResult, error)
}

// ServiceInfo contains metadata about a cloud database service.
// Fields are populated on a best-effort basis — not all backends provide all fields.
type ServiceInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Region string `json:"region,omitempty"`
	Host   string `json:"host,omitempty"`
	Port   int    `json:"port,omitempty"`
	DB     string `json:"database,omitempty"`
	User   string `json:"user,omitempty"`
	Type   string `json:"type,omitempty"` // e.g., "TIMESCALEDB", "POSTGRESQL"
}

// CreateOpts specifies options for creating a new database service.
type CreateOpts struct {
	// Name for the new service. Empty means auto-generate.
	Name string
}

// CreateResult contains the result of a successful Create operation.
type CreateResult struct {
	ServiceInfo
	ConnStr string `json:"connection_string,omitempty"`
}

// ForkOpts specifies options for forking a database service.
type ForkOpts struct {
	// Name for the forked service. Empty means auto-generate.
	Name string
}

// ForkResult contains the result of a successful Fork operation.
type ForkResult struct {
	ServiceInfo
	ConnStr  string `json:"connection_string,omitempty"`
	SourceID string `json:"source_id,omitempty"`
}

// MountInfo combines registry data with live service info for display.
// Used by the info command to present a unified view.
type MountInfo struct {
	Mountpoint string       `json:"mountpoint"`
	PID        int          `json:"pid"`
	Database   string       `json:"database"`
	StartTime  time.Time    `json:"start_time"`
	Uptime     string       `json:"uptime,omitempty"`
	Status     string       `json:"status"`
	Backend    string       `json:"backend,omitempty"`
	Service    *ServiceInfo `json:"service,omitempty"`
}
