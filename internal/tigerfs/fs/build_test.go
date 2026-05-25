package fs

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestBuild_HistoryRequiresTimescaleDB verifies that the .build/<app>
// handler refuses a `,history` build with a clear FSError when the
// TimescaleDB extension is absent. This is the user-facing failure for
// vanilla-Postgres deployments and must surface a hint, not a raw SQL
// error from a downstream CREATE TABLE WITH (tsdb.hypertable, ...).
func TestBuild_HistoryRequiresTimescaleDB(t *testing.T) {
	mockDB := &mockDBClient{
		// hasExtensions left nil → HasExtension returns false for any name.
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	parsed := &ParsedPath{BuildName: "notes"}
	err := ops.writeBuildFile(context.Background(), parsed, []byte("markdown,history"))

	require.NotNil(t, err, "history build must fail when TimescaleDB is absent")
	assert.Equal(t, ErrInvalidPath, err.Code)
	assert.Contains(t, err.Message, "TimescaleDB",
		"message must name TimescaleDB so users know what's missing")
	assert.NotEmpty(t, err.Hint, "FSError must carry a Hint for adapter logging")
	assert.True(t,
		strings.Contains(err.Hint, "TimescaleDB") ||
			strings.Contains(err.Hint, "CREATE EXTENSION"),
		"Hint must give an actionable recovery direction")
}

// TestBuild_HistoryAllowedWhenTimescaleDBPresent verifies the boundary
// path -- when TimescaleDB IS available, the build proceeds past the
// guard. The mock DDLExecutor will execute the generated SQL into a
// no-op, so we don't assert on the final result; we only assert that
// the early TimescaleDB guard did NOT fire.
func TestBuild_HistoryAllowedWhenTimescaleDBPresent(t *testing.T) {
	mockDB := &mockDBClient{
		hasExtensions: map[string]bool{"timescaledb": true},
	}

	cfg := &config.Config{DirListingLimit: 1000}
	ops := NewOperations(cfg, mockDB)

	parsed := &ParsedPath{BuildName: "notes"}
	err := ops.writeBuildFile(context.Background(), parsed, []byte("markdown,history"))

	// The mock's Exec returns nil for any statement, so the build should
	// succeed end-to-end. Any error means a non-TimescaleDB failure.
	if err != nil {
		assert.NotContains(t, err.Message, "TimescaleDB",
			"TimescaleDB guard fired even though the extension is present (err: %v)", err)
	}
}
