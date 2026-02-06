# NFS Integration TODO

Outstanding issues from NFS integration work (2026-02-04).

## 1. Views lack primary key support

**Status:** Tests skipped
**Priority:** Medium
**Files:** `test/integration/command_cases.go` (4 tests skipped)

Views like `active_users` and `order_summary` fail because `GetPrimaryKey` returns an error for views. Need to either:
- Add synthetic primary key support for views (e.g., use `ctid` or row number)
- Query view definitions to find underlying table's primary key
- Support views that have unique constraints

**Skipped tests:**
- `ListActiveUsersView`
- `ActiveUsersViewCount`
- `ReadActiveUserFromView`
- `OrderSummaryViewColumns`

## 2. Truncate-before-write architectural fix

**Status:** Fixed
**Priority:** Done
**Files:** `internal/tigerfs/nfs/ops_filesystem.go`, `internal/tigerfs/nfs/memfile_test.go`

The macOS NFS client sends SETATTR with size=0 before writing content. This is now properly handled:

1. **Truncated file tracking**: When O_TRUNC is set, the `cachedFile.truncated` flag is set to true. This prevents reading stale data from DB on subsequent opens.

2. **Cache persistence**: Truncated files are kept in cache (not removed on Close) so subsequent WRITE operations can find them.

3. **Empty write skip**: Empty content is not written to DB during the truncate phase, avoiding NOT NULL constraint violations. The actual content arrives in subsequent WRITEs.

4. **Proper state management**: After content is written, `truncated` flag is cleared. The reaper cleans up stale truncated entries after 5 minutes if no write arrives.

**Tests:**
- `TestMemFile_TruncateThenWrite_StateTracking` - verifies truncate-then-write pattern
- `TestMemFile_TruncateOnly_EmptyWriteSkipped` - verifies empty writes are skipped
- `TestMemFile_TruncateClearsData` - verifies Truncate(0) clears buffer

**Minor limitation:** Users cannot clear a column to empty by truncating. They must use `rm` (Delete) to set columns to NULL. This is documented behavior.

## 3. DDL operations

**Status:** Fixed
**Priority:** Done
**Files:** `internal/tigerfs/fs/ddl.go`, `internal/tigerfs/nfs/ops_filesystem.go`

DDL operations are now fully implemented via the staging pattern:
- `mkdir /.create/<name>` - create DDL staging session
- Edit `/.create/<name>/sql` - write DDL content
- `touch /.create/<name>/.test` - validate DDL
- `touch /.create/<name>/.commit` - execute DDL
- `touch /.create/<name>/.abort` - cancel session

**Tests:** `TestCommandCases_DDL` (18 test cases), `TestNFSDDL_*` tests all pass.

## 4. Pre-existing pipeline test failures

**Status:** Fixed
**Priority:** Done
**Files:** `internal/tigerfs/fs/path.go`, `test/integration/pipeline_test.go`

Fixed in two parts:

1. **Path validation** - Added validation to `path.go` to reject disallowed combinations:
   - `processOrder` checks `CanAddOrder()` before adding (rejects `.order/.order`)
   - `processBy` and `processFilter` check `CanAddFilter()` (rejects filter after `.order/`)
   - `processLimit` checks `CanAddLimit(limitType)` (rejects `.sample/.sample`, `.first/.first`, `.last/.last`)

2. **Test expectations** - Corrected two test expectations:
   - `ExportCSV`: Changed from 6 lines (header + 5 rows) to 5 lines (no header - use `.with-headers` for headers)
   - `ConflictingFilters`: Changed from ENOENT to empty directory (consistent with filesystem semantics)

**Added tests:** `TestParsePathDisallowedCombinations` and `TestParsePathAllowedCombinations` in `path_test.go`

## 5. Pipeline export PKColumn fix verification

**Status:** Fixed
**Priority:** Done (verify only)
**Files:** `internal/tigerfs/fs/operations.go:1131-1145`

Fixed in commit `ea826f5`. The issue was that `readExportFile` wasn't looking up PKColumn before calling `QueryRowsWithDataPipeline`, causing empty JSON exports for paths like `/users/.first/5/.export/json`.

**Verification:** Unit test `TestReadFile_PipelineExport_ReturnsJSONData` covers this case.
