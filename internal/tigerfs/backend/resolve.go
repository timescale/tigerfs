package backend

import (
	"fmt"
	"strings"
)

// Resolve parses a user-provided reference and returns the appropriate backend.
//
// The ref parameter uses a prefix scheme:
//   - "tiger:ID" → (TigerBackend, "ID", nil)
//   - "ghost:ID" → (GhostBackend, "ID", nil)
//   - "postgres://..." → (nil, "postgres://...", nil) — direct connection, no backend
//   - "my-db" → (nil, "my-db", nil) — bare name, caller must resolve via default_backend
//
// When Resolve returns a nil backend with a non-postgres:// string, the caller
// is responsible for resolving the backend via default_backend config or returning
// an error.
func Resolve(ref string) (Backend, string, error) {
	if strings.HasPrefix(ref, "tiger:") {
		id := strings.TrimPrefix(ref, "tiger:")
		return &TigerBackend{}, id, nil
	}
	if strings.HasPrefix(ref, "ghost:") {
		id := strings.TrimPrefix(ref, "ghost:")
		return &GhostBackend{}, id, nil
	}

	// Direct connection string or bare name — no backend.
	return nil, ref, nil
}

// ForName returns the backend for a given name.
//
// Valid names are "tiger" and "ghost". These correspond to the CLIBackend
// field stored in the mount registry and the default_backend config value.
//
// Returns ErrNoBackend if name is empty.
// Returns an error if the name is not recognized.
func ForName(name string) (Backend, error) {
	switch name {
	case "tiger":
		return &TigerBackend{}, nil
	case "ghost":
		return &GhostBackend{}, nil
	case "":
		return nil, ErrNoBackend
	default:
		return nil, fmt.Errorf("unknown backend: %q", name)
	}
}

// TODO: ResolveName performs name→ID lookup via the backend CLI.
// Enable when tiger/ghost CLIs fully support name-based lookups.
//
// func ResolveName(ctx context.Context, b Backend, name string) (string, error) {
// 	 // For tiger: tiger service list --output json, filter by name
// 	 // For ghost: ghost list --json, filter by name
// 	 return "", fmt.Errorf("name resolution not yet implemented")
// }
