package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupTestDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for relPath, content := range files {
		fullPath := filepath.Join(dir, relPath)
		os.MkdirAll(filepath.Dir(fullPath), 0755)
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("setup file %s: %v", relPath, err)
		}
	}
	return dir
}

func TestValidateWorkspace_Passing(t *testing.T) {
	files := map[string]string{
		"hello.md":      "# Hello\n",
		"docs/intro.md": "# Intro\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	for relPath, content := range files {
		expected.SetFile(relPath, HashContent([]byte(content)))
	}
	// docs/ exists on disk because setupTestDir creates parents implicitly;
	// expected state must declare it (validation now checks dirs too).
	expected.AddDir("docs")

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("ValidateWorkspace should pass: %v", err)
	}
}

func TestValidateWorkspace_MissingFile(t *testing.T) {
	files := map[string]string{
		"hello.md": "# Hello\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello\n")))
	expected.SetFile("missing.md", HashContent([]byte("gone")))

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail for missing file")
	}
}

func TestValidateWorkspace_UnexpectedFile(t *testing.T) {
	files := map[string]string{
		"hello.md": "# Hello\n",
		"extra.md": "# Extra\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello\n")))
	// extra.md not in expected

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail for unexpected file")
	}
}

func TestValidateWorkspace_HashMismatch(t *testing.T) {
	files := map[string]string{
		"hello.md": "# Hello v2\n",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello v1\n"))) // wrong hash

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail for hash mismatch")
	}
}

func TestValidateWorkspace_SkipsDotfiles(t *testing.T) {
	dir := t.TempDir()

	// Create a regular file
	os.WriteFile(filepath.Join(dir, "hello.md"), []byte("# Hello\n"), 0644)

	// Create dotfile and dot-directory (should be skipped)
	os.WriteFile(filepath.Join(dir, ".hidden"), []byte("secret"), 0644)
	os.MkdirAll(filepath.Join(dir, ".git", "objects"), 0755)
	os.WriteFile(filepath.Join(dir, ".git", "config"), []byte("[core]"), 0644)

	expected := NewWorkspaceState()
	expected.SetFile("hello.md", HashContent([]byte("# Hello\n")))

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("ValidateWorkspace should skip dotfiles: %v", err)
	}
}

func TestValidateWorkspace_EmptyWorkspace(t *testing.T) {
	dir := t.TempDir()
	expected := NewWorkspaceState()

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("empty workspace should validate: %v", err)
	}
}

func TestValidateWorkspace_NestedDirs(t *testing.T) {
	files := map[string]string{
		"a/b/c/deep.md": "deep content",
		"a/top.md":      "top content",
	}
	dir := setupTestDir(t, files)

	expected := NewWorkspaceState()
	for relPath, content := range files {
		expected.SetFile(relPath, HashContent([]byte(content)))
	}
	// Declare every dir setupTestDir's MkdirAll created.
	for _, d := range []string{"a", "a/b", "a/b/c"} {
		expected.AddDir(d)
	}

	err := ValidateWorkspace(dir, expected)
	if err != nil {
		t.Errorf("nested dirs should validate: %v", err)
	}
}

func TestValidateWorkspace_MissingDir(t *testing.T) {
	dir := t.TempDir() // empty workspace

	expected := NewWorkspaceState()
	expected.AddDir("expected-dir")

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail when an expected dir is missing")
	}
}

func TestValidateWorkspace_UnexpectedDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "rogue-dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	expected := NewWorkspaceState() // no dirs declared

	err := ValidateWorkspace(dir, expected)
	if err == nil {
		t.Error("ValidateWorkspace should fail when an unexpected dir is on disk")
	}
}

func TestValidateWorkspace_EmptyExpectedDir(t *testing.T) {
	// An empty dir on disk that's also declared in expected.Dirs is fine.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	expected := NewWorkspaceState()
	expected.AddDir("empty")

	if err := ValidateWorkspace(dir, expected); err != nil {
		t.Errorf("declared empty dir should validate: %v", err)
	}
}

func TestSnapshotHash_Deterministic(t *testing.T) {
	files := map[string]string{
		"a.md":     "content a",
		"b.md":     "content b",
		"dir/c.md": "content c",
	}
	dir := setupTestDir(t, files)

	h1, err := SnapshotHash(dir)
	if err != nil {
		t.Fatalf("SnapshotHash: %v", err)
	}

	h2, err := SnapshotHash(dir)
	if err != nil {
		t.Fatalf("SnapshotHash: %v", err)
	}

	if h1 != h2 {
		t.Error("same workspace should produce same snapshot hash")
	}
}

func TestSnapshotHash_DifferentContent(t *testing.T) {
	dir1 := setupTestDir(t, map[string]string{"a.md": "v1"})
	dir2 := setupTestDir(t, map[string]string{"a.md": "v2"})

	h1, _ := SnapshotHash(dir1)
	h2, _ := SnapshotHash(dir2)

	if h1 == h2 {
		t.Error("different content should produce different snapshot hash")
	}
}

func TestSnapshotHash_SkipsDotfiles(t *testing.T) {
	dir1 := setupTestDir(t, map[string]string{"a.md": "content"})

	// dir2 has same file plus a dotfile
	dir2 := setupTestDir(t, map[string]string{"a.md": "content"})
	os.WriteFile(filepath.Join(dir2, ".hidden"), []byte("secret"), 0644)

	h1, _ := SnapshotHash(dir1)
	h2, _ := SnapshotHash(dir2)

	if h1 != h2 {
		t.Error("dotfiles should be excluded from snapshot hash")
	}
}

// TestDiffWorkspace_AllIssueKinds verifies that diffWorkspace surfaces
// every kind of divergence the dump consumes, with stable sort order.
func TestDiffWorkspace_AllIssueKinds(t *testing.T) {
	expected := NewWorkspaceState()
	expected.SetFile("keep.md", "h1")
	expected.SetFile("missing.md", "h2")
	expected.SetFile("changed.md", "h3-expected")
	expected.AddDir("keepdir")
	expected.AddDir("missingdir")

	actualFiles := map[string]string{
		"keep.md":       "h1",
		"changed.md":    "h3-actual",
		"unexpected.md": "h4",
	}
	actualDirs := map[string]bool{
		"keepdir":      true,
		"unexpectedir": true,
	}

	issues := diffWorkspace(expected, actualFiles, actualDirs)
	if len(issues) != 5 {
		t.Fatalf("want 5 issues, got %d: %+v", len(issues), issues)
	}

	// Sorted by kind alphabetically. hash_mismatch < missing_dir <
	// missing_file < unexpected_dir < unexpected_file.
	wantKinds := []ValidationIssueKind{
		IssueHashMismatch, IssueMissingDir, IssueMissingFile,
		IssueUnexpectedDir, IssueUnexpectedFile,
	}
	for i, k := range wantKinds {
		if issues[i].Kind != k {
			t.Errorf("issue %d: kind=%s, want %s", i, issues[i].Kind, k)
		}
	}
}

// TestWriteDump_FailureKind exercises the dump writer end-to-end against
// a real local filesystem (no DB, no infra) for the failure case. DB
// capture failure is expected and surfaces as db_error.txt; everything
// else must materialize.
func TestWriteDump_FailureKind(t *testing.T) {
	mountDir, cfg, infra, state, stack, opLog := setupDumpScenario(t)

	dumpDir, err := WriteDump(DumpKindFailure, "validation", cfg, infra, state, stack, opLog,
		strErr("validation failed (1 issues): missing file: expected-but-missing.md"),
		"create_file foo", 1)
	if err != nil {
		t.Fatalf("WriteDump: %v", err)
	}
	if dumpDir == "" {
		t.Fatal("dumpDir empty")
	}
	if !strings.Contains(dumpDir, "tigerfs-stress-failure-") {
		t.Errorf("failure dump should use 'failure' prefix in dir name, got %s", dumpDir)
	}

	// Every dump file (except the optional db_state.json which is
	// expected to fail in this test) must be present and non-empty.
	for _, name := range []string{"summary.txt", "summary.json", "expected_state.json", "actual_state.json", "diff.txt", "diff.json", "stack.json", "operations.log", "operations.json", "db_error.txt"} {
		info, err := os.Stat(filepath.Join(dumpDir, name))
		if err != nil {
			t.Errorf("%s missing: %v", name, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("%s empty", name)
		}
	}

	// summary.txt must contain the dump dir, replay command, the issue
	// summary, and the kind heading -- those are the user-facing
	// signposts.
	body, err := os.ReadFile(filepath.Join(dumpDir, "summary.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--seed 999", "--large-files", "--validate-every 1", "missing_file", dumpDir, "failure dump"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("summary.txt missing %q", want)
		}
	}

	// summary.json should round-trip cleanly so downstream tools can
	// read it without ad-hoc parsing.
	jb, _ := os.ReadFile(filepath.Join(dumpDir, "summary.json"))
	var s dumpSummary
	if err := json.Unmarshal(jb, &s); err != nil {
		t.Fatalf("summary.json: %v", err)
	}
	if s.Kind != DumpKindFailure || s.FailureKind != "validation" || s.Seed != 999 || s.IssueCount == 0 || s.DumpDir != dumpDir {
		t.Errorf("summary.json fields: %+v", s)
	}
	if s.ErrorMessage == "" {
		t.Error("failure summary should populate ErrorMessage")
	}

	keepOrCleanup(t, dumpDir)
	_ = mountDir
}

// TestWriteDump_SnapshotKind exercises the same machinery via the manual
// --dump-at path: no validation error, "snapshot" prefix in the dir name,
// empty ValidationMessage.
func TestWriteDump_SnapshotKind(t *testing.T) {
	mountDir, cfg, infra, state, stack, opLog := setupDumpScenario(t)

	dumpDir, err := WriteDump(DumpKindSnapshot, "", cfg, infra, state, stack, opLog,
		nil, "create_file foo", 1)
	if err != nil {
		t.Fatalf("WriteDump: %v", err)
	}
	if !strings.Contains(dumpDir, "tigerfs-stress-snapshot-") {
		t.Errorf("snapshot dump should use 'snapshot' prefix in dir name, got %s", dumpDir)
	}

	body, err := os.ReadFile(filepath.Join(dumpDir, "summary.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "snapshot dump") {
		t.Errorf("snapshot summary.txt should say 'snapshot dump', got:\n%s", body)
	}
	if strings.Contains(string(body), "Validation error:") {
		t.Errorf("snapshot summary.txt should not include 'Validation error:' (no error to report)")
	}

	jb, _ := os.ReadFile(filepath.Join(dumpDir, "summary.json"))
	var s dumpSummary
	if err := json.Unmarshal(jb, &s); err != nil {
		t.Fatalf("summary.json: %v", err)
	}
	if s.Kind != DumpKindSnapshot || s.FailureKind != "" {
		t.Errorf("summary.json kind = %q failure_kind = %q, want snapshot/empty", s.Kind, s.FailureKind)
	}
	if s.ErrorMessage != "" {
		t.Errorf("snapshot summary.json should have empty ErrorMessage, got %q", s.ErrorMessage)
	}

	keepOrCleanup(t, dumpDir)
	_ = mountDir
}

// TestWriteDump_OperationFailureKind verifies the third trigger path:
// an op-level error (e.g. EIO) should also produce a dump with
// failure_kind="operation". The dump format and machinery are identical
// to validation failures; only the FailureKind tag and the summary.txt
// heading differ. Without this path, op failures tear down infra
// without leaving diagnostic data -- the gap that motivated this test.
func TestWriteDump_OperationFailureKind(t *testing.T) {
	mountDir, cfg, infra, state, stack, opLog := setupDumpScenario(t)

	dumpDir, err := WriteDump(DumpKindFailure, "operation", cfg, infra, state, stack, opLog,
		strErr("write doc-1681.md: open /tmp/.../doc-1681.md: input/output error"),
		"create_file foo [FAILED: input/output error]", 1)
	if err != nil {
		t.Fatalf("WriteDump: %v", err)
	}
	if !strings.Contains(dumpDir, "tigerfs-stress-failure-") {
		t.Errorf("op-failure dump should still use 'failure' prefix, got %s", dumpDir)
	}

	body, err := os.ReadFile(filepath.Join(dumpDir, "summary.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// summary.txt should label this as an operation error, not a
	// validation error -- they're different categories of failure.
	if !strings.Contains(string(body), "Operation error:") {
		t.Errorf("op-failure summary.txt should say 'Operation error:', got:\n%s", body)
	}
	if strings.Contains(string(body), "Validation error:") {
		t.Errorf("op-failure summary.txt must not say 'Validation error:'")
	}
	if !strings.Contains(string(body), "input/output error") {
		t.Errorf("op-failure summary.txt should include the underlying error message, got:\n%s", body)
	}

	jb, _ := os.ReadFile(filepath.Join(dumpDir, "summary.json"))
	var s dumpSummary
	if err := json.Unmarshal(jb, &s); err != nil {
		t.Fatalf("summary.json: %v", err)
	}
	if s.Kind != DumpKindFailure || s.FailureKind != "operation" {
		t.Errorf("summary.json kind=%q failure_kind=%q, want failure/operation", s.Kind, s.FailureKind)
	}
	if s.ErrorMessage == "" || !strings.Contains(s.ErrorMessage, "input/output error") {
		t.Errorf("summary.json error_message must carry the op error, got %q", s.ErrorMessage)
	}

	keepOrCleanup(t, dumpDir)
	_ = mountDir
}

// setupDumpScenario builds the common test fixture for dump tests: a
// staged workspace, a Config, an Infra (with intentionally invalid DB
// conn so the DB capture fails cleanly), an expected state that diverges
// from the workspace, a stack with one entry, and an op log.
//
// Shortens fsProbeOffsets so dump tests with op-failure errors don't
// trigger the full 5s walk-failure probe sleep.
func setupDumpScenario(t *testing.T) (string, *Config, *Infra, *WorkspaceState, *StateStack, []OpRecord) {
	t.Helper()
	prev := fsProbeOffsets
	fsProbeOffsets = []time.Duration{0, 1 * time.Millisecond}
	t.Cleanup(func() { fsProbeOffsets = prev })
	mountDir := t.TempDir()
	wsDir := filepath.Join(mountDir, "testws")
	if err := os.MkdirAll(wsDir, 0755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(wsDir, "actual.md"), []byte("on-disk"), 0644)

	cfg := &Config{
		Seed: 999, Iterations: 10, ValidateEvery: 1,
		LargeFiles: true, ManyFiles: false, Workspace: "testws",
	}
	infra := &Infra{
		Mountpoint: mountDir,
		ConnStr:    "postgres://invalid:invalid@127.0.0.1:1/none",
	}
	state := NewWorkspaceState()
	state.SetFile("expected-but-missing.md", HashContent([]byte("v")))
	stack := NewStateStack()
	stack.Push(NewWorkspaceState(), 1)
	stack.SetLastLogID("019dcca8-0000-7000-8000-000000000001")
	opLog := []OpRecord{
		{Iteration: 1, OpName: "create_file", Desc: "create_file foo (1KB)", NewLogIDs: []string{"019dcca8-0000-7000-8000-000000000001"}, Validated: true},
	}
	return mountDir, cfg, infra, state, stack, opLog
}

func keepOrCleanup(t *testing.T, dumpDir string) {
	t.Helper()
	if os.Getenv("STRESS_KEEP_DUMP") == "" {
		os.RemoveAll(dumpDir)
	} else {
		t.Logf("dump kept at %s (STRESS_KEEP_DUMP set)", dumpDir)
	}
}

// TestParseDumpAtSpec covers the small parser used by --dump-at: empty
// input, comma+space mix, dedup, and rejection of garbage / OOB values.
func TestParseDumpAtSpec(t *testing.T) {
	cases := []struct {
		name string
		spec string
		max  int
		want map[int]bool
	}{
		{"empty", "", 100, nil},
		{"single", "5", 100, map[int]bool{5: true}},
		{"multiple", "10,20,30", 100, map[int]bool{10: true, 20: true, 30: true}},
		{"with spaces", " 10 , 20 ", 100, map[int]bool{10: true, 20: true}},
		{"dedup", "5,5,5", 100, map[int]bool{5: true}},
		{"reject zero/neg", "0,-3,5", 100, map[int]bool{5: true}},
		{"reject non-int", "abc,5", 100, map[int]bool{5: true}},
		{"reject past max", "5,500", 100, map[int]bool{5: true}},
		{"all rejected -> nil", "abc,0,-1", 100, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseDumpAtSpec(tc.spec, tc.max)
			if len(got) != len(tc.want) {
				t.Fatalf("parseDumpAtSpec(%q, %d) = %v, want %v", tc.spec, tc.max, got, tc.want)
			}
			for k := range tc.want {
				if !got[k] {
					t.Errorf("missing key %d", k)
				}
			}
		})
	}
}

// strErr is a tiny helper for tests that need a non-nil error with a
// specific message.
func strErr(msg string) error { return &simpleErr{msg} }

type simpleErr struct{ msg string }

func (e *simpleErr) Error() string { return e.msg }

// TestParseSizeBytes covers the trailing "(N.NUNIT)" parser.
func TestParseSizeBytes(t *testing.T) {
	cases := []struct {
		desc      string
		wantBytes int
		wantOk    bool
	}{
		{"create_file foo (1.1KB)", 1126, true}, // 1.1*1024
		{"create_file foo (56.2KB)", 57548, true},
		{"edit_file bar (10.0MB)", 10 * 1024 * 1024, true},
		{"create_file baz (134.5KB)", 137728, true},
		{"create_file q (134B)", 134, true},
		{"delete_dir foo (3 files, 1 subdirs)", 0, false},
		{"create_savepoint sp-1", 0, false},
		{"undo_to_savepoint sp-1", 0, false},
		{"unbalanced parens (oops", 0, false},
		{"empty parens ()", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			b, ok := parseSizeBytes(tc.desc)
			if ok != tc.wantOk {
				t.Errorf("ok = %v, want %v (bytes=%d)", ok, tc.wantOk, b)
			}
			if ok && b != tc.wantBytes {
				t.Errorf("bytes = %d, want %d", b, tc.wantBytes)
			}
		})
	}
}

// TestStackIslands covers the iteration-clustering helper used to render
// "stack: [1] [107..131] [220..227]" in analysis.txt.
func TestStackIslands(t *testing.T) {
	mk := func(iters ...int) *StateStack {
		s := NewStateStack()
		for _, it := range iters {
			s.entries = append(s.entries, StackEntry{Iteration: it, State: NewWorkspaceState()})
		}
		return s
	}
	cases := []struct {
		name    string
		stack   *StateStack
		wantStr string
	}{
		{"empty", mk(), ""},
		{"single", mk(5), "[5,5]"},
		{"contiguous", mk(1, 2, 3, 4), "[1,4]"},
		{"two islands", mk(1, 2, 5, 6, 7), "[1,2] [5,7]"},
		{"three islands", mk(1, 107, 108, 220, 221, 222), "[1,1] [107,108] [220,222]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stackIslands(tc.stack)
			parts := make([]string, len(got))
			for i, isl := range got {
				parts[i] = fmt.Sprintf("[%d,%d]", isl.start, isl.end)
			}
			if joined := strings.Join(parts, " "); joined != tc.wantStr {
				t.Errorf("got %q, want %q", joined, tc.wantStr)
			}
		})
	}
}

// TestDetectAnomalies_Iter107Style covers the headline case the analyzer
// is built for: a tiny create with log_count blown up by a prior undo's
// lastLogID regression.
func TestDetectAnomalies_Iter107Style(t *testing.T) {
	opLog := []OpRecord{
		{Iteration: 107, OpName: "create_file", Desc: "create_file log-5022.md (1.1KB)",
			NewLogIDs: makeFakeLogIDs(61)}, // pathological
	}
	got := detectAnomalies(opLog, NewStateStack(), nil)
	if len(got) != 1 {
		t.Fatalf("want 1 anomaly, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "iter 107") || !strings.Contains(got[0], "log_count=61") {
		t.Errorf("anomaly text doesn't mention iter and count: %q", got[0])
	}
}

// TestDetectAnomalies_LargeWriteIsNotAnomaly verifies a legitimate
// multi-chunk write is NOT flagged. 10MB / 128KB = 80 entries is the
// expected fan-out, not a regression.
func TestDetectAnomalies_LargeWriteIsNotAnomaly(t *testing.T) {
	opLog := []OpRecord{
		{Iteration: 471, OpName: "edit_file", Desc: "edit_file note-9832.md (10.0MB)",
			NewLogIDs: makeFakeLogIDs(80)},
		{Iteration: 432, OpName: "create_file", Desc: "create_file memo-0047.md (307.8KB)",
			NewLogIDs: makeFakeLogIDs(3)},
	}
	got := detectAnomalies(opLog, NewStateStack(), nil)
	if len(got) != 0 {
		t.Errorf("expected no anomalies for legitimate fan-out, got: %v", got)
	}
}

// TestDetectAnomalies_SavepointWithLogEntries verifies that
// create_savepoint logging anything is flagged (savepoints are recorded
// in a separate table and must not appear in testws_log).
func TestDetectAnomalies_SavepointWithLogEntries(t *testing.T) {
	opLog := []OpRecord{
		{Iteration: 1, OpName: "create_savepoint", Desc: "create_savepoint sp-1",
			NewLogIDs: makeFakeLogIDs(2)},
	}
	got := detectAnomalies(opLog, NewStateStack(), nil)
	if len(got) != 1 || !strings.Contains(got[0], "create_savepoint") {
		t.Errorf("expected savepoint anomaly, got: %v", got)
	}
}

// TestDetectAnomalies_UUIDv7Regression catches a stack entry whose
// log_id is older than its predecessor -- a sign of bookkeeping
// crossing iteration boundaries in the wrong order.
func TestDetectAnomalies_UUIDv7Regression(t *testing.T) {
	stack := NewStateStack()
	stack.entries = []StackEntry{
		{Iteration: 5, State: NewWorkspaceState(), LogID: "019dccdd-d800-...", LogCount: 1},
		{Iteration: 6, State: NewWorkspaceState(), LogID: "019dccdd-d700-...", LogCount: 1}, // regression
	}
	got := detectAnomalies(nil, stack, nil)
	if len(got) != 1 || !strings.Contains(got[0], "UUIDv7 regression") {
		t.Errorf("expected UUIDv7 regression anomaly, got: %v", got)
	}
}

// TestDetectAnomalies_RenameArtifact validates the failure-dump
// heuristic that pairs MissingFile + UnexpectedFile by content hash to
// identify TigerFS/stress-test rename divergences.
func TestDetectAnomalies_RenameArtifact(t *testing.T) {
	issues := []ValidationIssue{
		{Kind: IssueMissingFile, Path: "old/file.md", ExpectedHash: "deadbeef00000000"},
		{Kind: IssueUnexpectedFile, Path: "new/file.md", ActualHash: "deadbeef00000000"},
		{Kind: IssueMissingFile, Path: "unrelated.md", ExpectedHash: "abc123"},
	}
	got := detectAnomalies(nil, NewStateStack(), issues)
	if len(got) != 1 || !strings.Contains(got[0], "rename artifact") {
		t.Errorf("expected single rename artifact, got: %v", got)
	}
	if !strings.Contains(got[0], "old/file.md") || !strings.Contains(got[0], "new/file.md") {
		t.Errorf("anomaly text missing path pair: %q", got[0])
	}
}

func makeFakeLogIDs(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("019dccdd-fake-%04d", i)
	}
	return out
}
