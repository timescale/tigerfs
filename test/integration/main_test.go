package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestMain(m *testing.M) {
	probeLocalPostgreSQL()

	if !localPGAvailable && isDockerAvailable() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		connStr, cleanup, err := startSharedContainer(ctx)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start shared PostgreSQL container: %v\n", err)
			os.Exit(1)
		}
		sharedContainerConnStr = connStr
		sharedContainerCleanup = cleanup
	}

	code := m.Run()

	if sharedContainerCleanup != nil {
		sharedContainerCleanup()
	}

	os.Exit(code)
}

// startSharedContainer starts a single PostgreSQL container shared by all tests.
// Each test gets isolation via unique schemas (created by setupLocalTestDB/setupLocalTestDBEmpty).
func startSharedContainer(ctx context.Context) (string, func(), error) {
	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb-ha:pg18",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start container: %w", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		return "", nil, fmt.Errorf("get connection string: %w", err)
	}

	cleanup := func() {
		_ = pgContainer.Terminate(context.Background())
	}

	return connStr, cleanup, nil
}
