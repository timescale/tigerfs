package db

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// kvSSLModeRegexp matches sslmode=<value> in key-value connection strings.
var kvSSLModeRegexp = regexp.MustCompile(`sslmode=\S+`)

// secureModes are sslmode values that already enforce TLS encryption.
// These values are left unchanged by enforceSSLMode.
var secureModes = map[string]bool{
	"require":     true,
	"verify-ca":   true,
	"verify-full": true,
}

// enforceSSLMode ensures sslmode=require for non-localhost connections.
//
// For remote hosts, any existing sslmode=disable or sslmode=prefer is replaced
// with sslmode=require. If no sslmode is present, sslmode=require is added.
// Existing sslmode=require, verify-ca, or verify-full are left unchanged.
//
// Localhost connections (127.0.0.1, ::1, Unix sockets) are exempt from
// enforcement since they don't traverse a network.
//
// When insecureNoSSL is true, enforcement is skipped and a warning is logged.
func enforceSSLMode(connStr string, insecureNoSSL bool) string {
	host := extractHost(connStr)
	if isLocalhost(host) {
		// For localhost, default to sslmode=disable if not specified.
		// pgx defaults to sslmode=prefer which attempts TLS first; some
		// PostgreSQL configurations (e.g. ssl=off) reset the connection
		// during the TLS handshake instead of cleanly rejecting it,
		// causing pgx's prefer fallback to fail. TLS on localhost adds
		// no security value, so we skip it. Users can still override
		// with an explicit sslmode in their connection string.
		if extractSSLMode(connStr) == "" {
			return addSSLMode(connStr, "disable")
		}
		return connStr
	}

	if insecureNoSSL {
		logging.Warn("TLS enforcement disabled for remote database connection",
			zap.String("host", host),
			zap.String("hint", "remove --insecure-no-ssl for encrypted connections"))
		return connStr
	}

	// Check current sslmode
	currentMode := extractSSLMode(connStr)
	if secureModes[currentMode] {
		return connStr
	}

	// Replace or add sslmode=require
	if currentMode != "" {
		// Replace existing insecure sslmode
		return replaceSSLMode(connStr, "require")
	}

	// No sslmode present -- add it
	return addSSLMode(connStr, "require")
}

// isLocalhost returns true if the host is a localhost address or Unix socket.
func isLocalhost(host string) bool {
	switch host {
	case "", "localhost", "127.0.0.1", "::1":
		return true
	}
	// Unix socket paths start with /
	return strings.HasPrefix(host, "/")
}

// extractHost extracts the host from a connection string.
// Handles both URL format (postgres://host/db) and key-value format (host=value).
func extractHost(connStr string) string {
	// URL format
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err == nil {
			return u.Hostname()
		}
	}

	// Key-value format: extract host=<value>
	for _, part := range strings.Fields(connStr) {
		if strings.HasPrefix(part, "host=") {
			return strings.TrimPrefix(part, "host=")
		}
	}

	// No host found (Unix socket or empty)
	return ""
}

// extractSSLMode extracts the current sslmode value from a connection string.
// Returns empty string if no sslmode is present.
func extractSSLMode(connStr string) string {
	// URL format
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err == nil {
			return u.Query().Get("sslmode")
		}
	}

	// Key-value format
	for _, part := range strings.Fields(connStr) {
		if strings.HasPrefix(part, "sslmode=") {
			return strings.TrimPrefix(part, "sslmode=")
		}
	}

	return ""
}

// replaceSSLMode replaces the existing sslmode value in a connection string.
func replaceSSLMode(connStr string, mode string) string {
	// URL format
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err == nil {
			q := u.Query()
			q.Set("sslmode", mode)
			u.RawQuery = q.Encode()
			return u.String()
		}
	}

	// Key-value format
	return kvSSLModeRegexp.ReplaceAllString(connStr, "sslmode="+mode)
}

// addSSLMode adds sslmode to a connection string that doesn't have one.
func addSSLMode(connStr string, mode string) string {
	// URL format
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err == nil {
			q := u.Query()
			q.Set("sslmode", mode)
			u.RawQuery = q.Encode()
			return u.String()
		}
	}

	// Key-value format
	return connStr + " sslmode=" + mode
}
