# Release Process

## 1. Run all tests

Start a Postgres 18 + TimescaleDB container so that DB-requiring unit tests actually run (they silently skip without `PGHOST`/`TEST_DATABASE_URL`):

```bash
docker run --rm -d --name tigerfs-release-pg -e POSTGRES_PASSWORD=test \
    -p 15432:5432 timescale/timescaledb-ha:pg18
sleep 5  # wait for PG to accept connections
```

Then run the full test suite in one command (works because config-package tests isolate themselves from PG env vars):

```bash
go fmt ./... && go vet ./... && go mod tidy

PGHOST=127.0.0.1 PGPORT=15432 PGUSER=postgres PGPASSWORD=test PGDATABASE=postgres \
TEST_DATABASE_URL='postgres://postgres:test@127.0.0.1:15432/postgres?sslmode=disable' \
  go test -count=1 -timeout 300s ./internal/tigerfs/...   # Unit tests
go test -count=1 -timeout 600s ./test/integration/...     # Integration tests (uses testcontainers)
./scripts/test-docker.sh -v -timeout 300s                 # Docker FUSE tests
```

When done:

```bash
docker stop tigerfs-release-pg
```

`-count=1` disables Go's test cache so an unchanged-since-last-run test doesn't silently re-skip. The PG env vars are needed for unit tests in `internal/tigerfs/db/` (and a few others) that connect directly; integration tests spin up their own testcontainers.

## 1b. Test migration paths (when a release adds tigerfs migrate migrations)

If the release introduces or modifies `tigerfs migrate` migrations, verify them end-to-end against a pre-release-state workspace:

```bash
docker run --rm -d --name migrate-test -e POSTGRES_PASSWORD=test \
    -p 15433:5432 timescale/timescaledb-ha:pg18
sleep 5
# ... seed with previous-release fixture data ...

CONN='postgres://postgres:test@127.0.0.1:15433/postgres?sslmode=disable'

tigerfs migrate "$CONN" --describe   # list pending migrations
tigerfs migrate "$CONN" --dry-run    # preview the SQL
tigerfs migrate "$CONN"              # apply

# Re-run integration tests against the migrated workspace
TEST_DATABASE_URL="$CONN" go test -count=1 -timeout 600s ./test/integration/... -run TestMigrate

docker stop migrate-test
```

For the v0.7 release specifically: confirm the v0.6→v0.7 history-format migration boundary is recorded and that `.undo/` on pre-boundary log entries returns `EPERM` with a hint.

## 2. Update CHANGELOG.md

Add a new version section to `CHANGELOG.md`. Follow the existing format:

- **One-line bold tagline** describing the release theme
- **Bullet list** of user-facing changes -- each bullet has a **bold short name**, one-sentence description
- Focus on what users can now do, not implementation details
- Omit internal changes (refactors, test infrastructure) unless they affect users
- Match the tone of previous releases (see v0.5.0 and v0.6.0 in CHANGELOG.md for current examples)
- **Replace the `YYYY-MM-DD` placeholder with today's date** before tagging

The CHANGELOG entry is also used as the GitHub release body -- write it for that audience.

## 3. Update implementation checklist

Mark the release task complete in `docs/implementation/implementation-tasks-checklist.md` and update the Summary table.

## 4. Snapshot build

```bash
goreleaser release --snapshot --clean
./dist/tigerfs_darwin_arm64*/tigerfs version    # path suffix varies by Go version
```

## 5. Commit, tag, and push

Stage all docs changes for the release and review what's included before committing:

```bash
git add CHANGELOG.md README.md docs/
git status                                       # confirm the staged set
git commit -m "docs: prepare vX.Y.Z release"
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

The `v*.*.*` tag triggers `.github/workflows/release.yml` which runs GoReleaser to build and publish binaries. GoReleaser auto-generates a changelog from commit messages, but **you should edit the release on GitHub** to replace it with the CHANGELOG.md entry for a clean, curated summary.

## 6. Edit release notes on GitHub

After the release workflow completes, edit the release at `https://github.com/timescale/tigerfs/releases/tag/vX.Y.Z` and replace the auto-generated changelog with the CHANGELOG.md entry.
