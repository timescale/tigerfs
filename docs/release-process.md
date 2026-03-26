# Release Process

## 1. Run all tests

```bash
go fmt ./... && go vet ./... && go mod tidy
go test ./internal/tigerfs/...                        # Unit tests
go test -v -timeout 300s ./test/integration/...       # Integration tests
./scripts/test-docker.sh -v -timeout 300s             # Docker FUSE tests
```

## 2. Update CHANGELOG.md

Add a new version section to `CHANGELOG.md`. Follow the existing format:

- **One-line bold tagline** describing the release theme
- **Bullet list** of user-facing changes -- each bullet has a **bold short name**, one-sentence description
- Focus on what users can now do, not implementation details
- Omit internal changes (refactors, test infrastructure) unless they affect users
- Match the tone of previous releases (see v0.1.0 and v0.2.0 in CHANGELOG.md for examples)

The CHANGELOG entry is also used as the GitHub release body -- write it for that audience.

## 3. Update implementation checklist

Mark the release task complete in `docs/implementation/implementation-tasks-checklist.md` and update the Summary table.

## 4. Snapshot build

```bash
goreleaser release --snapshot --clean
./dist/tigerfs_darwin_arm64_v8.0/tigerfs version
```

## 5. Commit, tag, and push

```bash
git add CHANGELOG.md README.md docs/implementation/implementation-tasks-checklist.md
git commit -m "docs: prepare vX.Y.Z release"
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

The `v*.*.*` tag triggers `.github/workflows/release.yml` which runs GoReleaser to build and publish binaries. GoReleaser auto-generates a changelog from commit messages, but **you should edit the release on GitHub** to replace it with the CHANGELOG.md entry for a clean, curated summary.

## 6. Edit release notes on GitHub

After the release workflow completes, edit the release at `https://github.com/timescale/tigerfs/releases/tag/vX.Y.Z` and replace the auto-generated changelog with the CHANGELOG.md entry.
