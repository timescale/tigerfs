package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// OpRecord captures one iteration's operation for the failure dump's
// operations.log -- a structured replay of the run that reproduces what
// `[STEP N/M] ...` printed at runtime, plus log_id metadata that wouldn't
// otherwise survive process exit.
type OpRecord struct {
	Iteration  int      `json:"iteration"`
	OpName     string   `json:"op_name"`
	Desc       string   `json:"desc"`        // human-readable, matches stdout
	NewLogIDs  []string `json:"new_log_ids"` // log_ids produced by this op (>=2 indicates fan-out)
	Validated  bool     `json:"validated"`
	UndoTarget string   `json:"undo_target,omitempty"` // for undo_* ops: the targeted log_id / savepoint
}

// stackDump is the JSON shape for stack.json. Mirrors StackEntry/StateStack
// but with exported field names suitable for offline analysis.
type stackDumpEntry struct {
	Iteration int               `json:"iteration"`
	LogID     string            `json:"log_id,omitempty"`
	LogCount  int               `json:"log_count"`
	Files     map[string]string `json:"files"`
	Dirs      map[string]bool   `json:"dirs"`
}

type stackDump struct {
	Entries    []stackDumpEntry `json:"entries"`
	Savepoints map[string]int   `json:"savepoints"`
}

// dbDump is the JSON shape for db_state.json. We snapshot the four tables
// the undo system uses; *_history is capped because it can grow without
// bound and we only need recent versions for diagnosis.
type dbDump struct {
	Workspace    string                   `json:"workspace"`
	Schema       string                   `json:"schema"`
	Rows         []map[string]interface{} `json:"rows"`
	Log          []map[string]interface{} `json:"log"`
	Savepoints   []map[string]interface{} `json:"savepoints"`
	HistoryLastN []map[string]interface{} `json:"history_last_n"`
	HistoryLimit int                      `json:"history_limit"`
}

// DumpKind tags a dump as either a validation failure (auto-fired by the
// runner) or a manual snapshot (--dump-at). Distinguishing them in the dump
// directory prefix and summary lets `find /tmp -name 'tigerfs-stress-failure-*'`
// keep returning real failures only.
type DumpKind string

const (
	DumpKindFailure  DumpKind = "failure"
	DumpKindSnapshot DumpKind = "snapshot"
)

// dumpSummary is the JSON shape for summary.json -- one-stop overview
// describing the dump for downstream tooling and humans alike. Keep it
// flat: anything that fits here doesn't need to be cross-referenced from
// the other dump files.
//
// FailureKind is "" for snapshots, "validation" when ValidateWorkspace
// returned mismatches, "operation" when an op (create_file, edit_file,
// etc.) returned an error. Provided so downstream tooling can grep
// failures by category without parsing free-form text.
//
// ErrorMessage carries the abort reason -- the validation issue list
// for kind=validation, the underlying op error (e.g. EIO) for
// kind=operation. Empty for snapshots.
type dumpSummary struct {
	Kind            DumpKind `json:"kind"`
	FailureKind     string   `json:"failure_kind,omitempty"`
	Seed            int64    `json:"seed"`
	Iteration       int      `json:"iteration"`
	TotalIterations int      `json:"total_iterations"`
	Workspace       string   `json:"workspace"`
	LargeFiles      bool     `json:"large_files"`
	ManyFiles       bool     `json:"many_files"`
	ValidateEvery   int      `json:"validate_every"`
	Op              string   `json:"op"`
	IssueCount      int      `json:"issue_count"`
	ReplayCommand   string   `json:"replay_command"`
	Mountpoint      string   `json:"mountpoint"`
	ConnStr         string   `json:"conn_str"`
	DumpDir         string   `json:"dump_dir"`
	GeneratedAt     string   `json:"generated_at"`
	ErrorMessage    string   `json:"error_message,omitempty"`
}

// historyDumpLimit caps how many *_history rows we serialize. Versions can
// pile up in a long run; the most-recent ones are what's diagnostic.
const historyDumpLimit = 200

// WriteDump captures everything a future investigator might want about a
// stress-test moment: expected vs actual workspace state, the full state
// stack, the live DB tables, the structured op trace, and a sorted diff
// of any divergence. Used both for auto-fired failure dumps (kind =
// DumpKindFailure, valErr non-nil) and manual --dump-at snapshots (kind =
// DumpKindSnapshot, valErr nil).
//
// Returns the dump directory path and any I/O error encountered along the
// way. Best-effort: we keep going past individual file write errors so
// the caller gets as much data as possible.
//
// The dump dir is
//
//	<dump-dir>/tigerfs-stress-<kind>-<seed>-<iteration>-<unix>/
//
// where <dump-dir> is `cfg.DumpDir` (default `/tmp`). The disambiguating
// suffix lets users keep multiple dumps around without collision. Caller
// is expected to print the returned path.
func WriteDump(kind DumpKind, failureKind string, cfg *Config, infra *Infra, state *WorkspaceState, stack *StateStack, opLog []OpRecord, runErr error, op string, iteration int) (string, error) {
	dumpBase := cfg.DumpDir
	if dumpBase == "" {
		dumpBase = defaultDumpDir
	}
	dumpDir := filepath.Join(dumpBase, fmt.Sprintf("tigerfs-stress-%s-%d-%d-%d", kind, cfg.Seed, iteration, time.Now().Unix()))
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return "", fmt.Errorf("create dump dir: %w", err)
	}

	// Walk-failure probe (FS visibility timeline). Run FIRST, before
	// re-snapshotting the workspace, so the first sample is as close to the
	// original failure moment as possible. The DB half runs later, after
	// db_state is captured. See WalkFailureProbe doc comment for context.
	wsPath := filepath.Join(infra.Mountpoint, cfg.Workspace)
	var probe *WalkFailureProbe
	if missingAbs, ok := extractMissingPath(runErr); ok {
		// Compute relative path (relative to the workspace mount root).
		rel, relErr := filepath.Rel(wsPath, missingAbs)
		if relErr != nil {
			rel = missingAbs // fall back to absolute if Rel fails
		}
		probe = &WalkFailureProbe{
			MissingRelPath: rel,
			WorkspacePath:  wsPath,
			WalkErrorRaw:   runErr.Error(),
			FS:             probeFSVisibility(missingAbs),
		}
	}

	// Snapshot the live filesystem and compute the structured diff. Both
	// feed the actual_state.json and diff.txt files.
	actualFiles, actualDirs, snapErr := snapshotWorkspace(wsPath)
	if snapErr != nil {
		// Don't bail -- record the error and continue with the data we
		// already have. A failed snapshot is a useful symptom, not a
		// reason to abort dumping the rest.
		writeText(dumpDir, "snapshot_error.txt", snapErr.Error())
	}
	issues := diffWorkspace(state, actualFiles, actualDirs)

	// summary.json + summary.txt -- the at-a-glance view
	summary := dumpSummary{
		Kind:            kind,
		FailureKind:     failureKind,
		Seed:            cfg.Seed,
		Iteration:       iteration,
		TotalIterations: cfg.Iterations,
		Workspace:       cfg.Workspace,
		LargeFiles:      cfg.LargeFiles,
		ManyFiles:       cfg.ManyFiles,
		ValidateEvery:   cfg.ValidateEvery,
		Op:              op,
		IssueCount:      len(issues),
		ReplayCommand:   replayCommand(cfg),
		Mountpoint:      infra.Mountpoint,
		ConnStr:         infra.ConnStr,
		DumpDir:         dumpDir,
		GeneratedAt:     time.Now().Format(time.RFC3339),
		ErrorMessage:    shortErrorMessage(runErr),
	}
	writeJSON(dumpDir, "summary.json", summary)
	writeText(dumpDir, "summary.txt", renderSummaryText(summary, issues))

	// expected_state.json -- WorkspaceState at the moment of failure
	writeJSON(dumpDir, "expected_state.json", map[string]interface{}{
		"files": state.Files,
		"dirs":  state.Dirs,
	})

	// actual_state.json -- live filesystem snapshot
	writeJSON(dumpDir, "actual_state.json", map[string]interface{}{
		"files": actualFiles,
		"dirs":  actualDirs,
	})

	// diff.txt + diff.json -- structured divergence
	writeJSON(dumpDir, "diff.json", issues)
	writeText(dumpDir, "diff.txt", renderDiffText(issues))

	// stack.json -- every StackEntry and savepoint (in-package access).
	writeJSON(dumpDir, "stack.json", buildStackDump(stack))

	// operations.log + operations.json -- full op trace
	writeJSON(dumpDir, "operations.json", opLog)
	writeText(dumpDir, "operations.log", renderOpLogText(opLog))

	// db_state.json -- live PostgreSQL view of the four undo-related
	// tables. Best-effort: a DB error here doesn't fail the whole dump.
	dbDumpResult, dbDumpErr := captureDBState(cfg, infra)
	if dbDumpErr != nil {
		writeText(dumpDir, "db_error.txt", dbDumpErr.Error())
	} else {
		writeJSON(dumpDir, "db_state.json", dbDumpResult)
	}

	// Walk-failure probe (DB half). Now that we have rows captured, resolve
	// the missing path against them. Combined conclusion encoded with the
	// FS timeline collected earlier.
	if probe != nil {
		if dbDumpResult != nil {
			probe.DB = probeDBForRelPath(dbDumpResult.Rows, probe.MissingRelPath)
		}
		probe.Conclusion = renderProbeConclusion(probe.FS, probe.DB)
		writeJSON(dumpDir, "walk_failure_probe.json", probe)
	}

	// analysis.txt -- pre-computed cross-reference and anomaly checks.
	// Last so it's based on the same in-memory state captured above.
	writeText(dumpDir, "analysis.txt", analyzeDump(state, stack, opLog, issues, probe))

	return dumpDir, nil
}

// renderSummaryText produces a human-friendly summary.txt. Designed to be
// the first file someone opens after a dump -- everything else can be
// looked up via the paths and command listed here.
func renderSummaryText(s dumpSummary, issues []ValidationIssue) string {
	var b strings.Builder
	title := "tigerfs-stress " + string(s.Kind) + " dump"
	fmt.Fprintf(&b, "%s\n", title)
	fmt.Fprintf(&b, "%s\n\n", strings.Repeat("=", len(title)))
	fmt.Fprintf(&b, "Generated:        %s\n", s.GeneratedAt)
	fmt.Fprintf(&b, "Seed:             %d\n", s.Seed)
	fmt.Fprintf(&b, "Iteration:        %d / %d\n", s.Iteration, s.TotalIterations)
	fmt.Fprintf(&b, "Op:               %s\n", s.Op)
	fmt.Fprintf(&b, "Issues:           %d\n", s.IssueCount)
	fmt.Fprintf(&b, "Workspace:        %s\n", s.Workspace)
	fmt.Fprintf(&b, "LargeFiles:       %v\n", s.LargeFiles)
	fmt.Fprintf(&b, "ManyFiles:        %v\n", s.ManyFiles)
	fmt.Fprintf(&b, "ValidateEvery:    %d\n", s.ValidateEvery)
	fmt.Fprintf(&b, "\nOpen analysis.txt first -- it lists anomalies and the run shape at a glance.\n")
	fmt.Fprintf(&b, "\nDump directory:   %s\n", s.DumpDir)
	fmt.Fprintf(&b, "Mountpoint:       %s\n", s.Mountpoint)
	fmt.Fprintf(&b, "Postgres:         %s\n", s.ConnStr)
	fmt.Fprintf(&b, "\nReplay:           %s\n", s.ReplayCommand)
	if len(issues) > 0 {
		fmt.Fprintf(&b, "\nIssue summary:\n%s\n", renderDiffText(issues))
	}
	if s.ErrorMessage != "" {
		// Heading adapts to whether this was a validation diff or an
		// op-level error; both flow through the same field.
		heading := "Error"
		switch s.FailureKind {
		case "validation":
			heading = "Validation error"
		case "operation":
			heading = "Operation error"
		}
		fmt.Fprintf(&b, "\n%s:\n  %s\n", heading, s.ErrorMessage)
	}
	return b.String()
}

// renderDiffText groups issues by kind for quick scanning. Within each
// group, paths are already sorted by diffWorkspace, so a side-by-side
// rename ("missing X" / "unexpected Y at different path") usually appears
// near each other.
func renderDiffText(issues []ValidationIssue) string {
	if len(issues) == 0 {
		return "(no issues)"
	}
	groups := map[ValidationIssueKind][]ValidationIssue{}
	for _, iss := range issues {
		groups[iss.Kind] = append(groups[iss.Kind], iss)
	}
	order := []ValidationIssueKind{
		IssueMissingFile, IssueUnexpectedFile, IssueHashMismatch,
		IssueMissingDir, IssueUnexpectedDir,
	}
	var b strings.Builder
	for _, k := range order {
		group := groups[k]
		if len(group) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n  [%s]  (%d)\n", k, len(group))
		for _, iss := range group {
			fmt.Fprintf(&b, "    %s\n", formatIssue(iss))
		}
	}
	return b.String()
}

// renderOpLogText produces operations.log -- the line-per-step trace
// that's easy to grep. JSON form (operations.json) is also written for
// programmatic analysis.
func renderOpLogText(opLog []OpRecord) string {
	var b strings.Builder
	for _, r := range opLog {
		marker := ""
		if !r.Validated {
			marker = " [no-validate]"
		}
		ids := ""
		if len(r.NewLogIDs) > 1 {
			ids = fmt.Sprintf(" [%d log entries]", len(r.NewLogIDs))
		}
		fmt.Fprintf(&b, "[STEP %d] %s%s%s\n", r.Iteration, r.Desc, marker, ids)
	}
	return b.String()
}

// shortErrorMessage extracts just the human summary from a wrapped run
// error -- the goroutine stack inside `%w(...)` is noise here. Used for
// both validation-diff messages and op-level error chains.
func shortErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	// Cap at first 4 KB; the structured diff has the full data.
	if len(msg) > 4096 {
		msg = msg[:4096] + "... (truncated; see diff.txt)"
	}
	return msg
}

// buildStackDump copies StateStack internals into the JSON-friendly
// stackDump shape. Same package so we can read unexported fields directly.
func buildStackDump(stack *StateStack) stackDump {
	out := stackDump{
		Entries:    make([]stackDumpEntry, 0, len(stack.entries)),
		Savepoints: map[string]int{},
	}
	for _, e := range stack.entries {
		out.Entries = append(out.Entries, stackDumpEntry{
			Iteration: e.Iteration,
			LogID:     e.LogID,
			LogCount:  e.LogCount,
			Files:     e.State.Files,
			Dirs:      e.State.Dirs,
		})
	}
	for k, v := range stack.savepoints {
		out.Savepoints[k] = v
	}
	return out
}

// captureDBState opens a fresh pgx connection and snapshots the four
// tables that participate in undo. Lives in its own connection (not the
// pool tigerfs uses) so this is safe even mid-failure.
func captureDBState(cfg *Config, infra *Infra) (*dbDump, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, infra.ConnStr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	out := &dbDump{
		Workspace:    cfg.Workspace,
		Schema:       "tigerfs",
		HistoryLimit: historyDumpLimit,
	}

	// Live rows (current workspace contents). Body length only -- full
	// bodies can be megabytes per row and the diff doesn't need them.
	rows, err := queryAsMaps(ctx, conn,
		fmt.Sprintf(`SELECT id::text, parent_id::text AS parent_id, filename, filetype, length(body) AS body_len, modified_at::text AS modified_at FROM tigerfs.%s ORDER BY filename, id`, cfg.Workspace))
	if err != nil {
		return out, fmt.Errorf("query rows: %w", err)
	}
	out.Rows = rows

	logRows, err := queryAsMaps(ctx, conn,
		fmt.Sprintf(`SELECT log_id::text, file_id::text, type, filename, version_id::text AS version_id, description FROM tigerfs.%s_log ORDER BY log_id`, cfg.Workspace))
	if err != nil {
		return out, fmt.Errorf("query log: %w", err)
	}
	out.Log = logRows

	spRows, err := queryAsMaps(ctx, conn,
		fmt.Sprintf(`SELECT name, log_id::text, created_at::text AS created_at FROM tigerfs.%s_savepoint ORDER BY created_at`, cfg.Workspace))
	if err != nil {
		// _savepoint table may not always exist; degrade gracefully.
		out.Savepoints = nil
	} else {
		out.Savepoints = spRows
	}

	histRows, err := queryAsMaps(ctx, conn,
		fmt.Sprintf(`SELECT version_id::text, file_id::text, filename, length(body) AS body_len, modified_at::text AS modified_at, operation FROM tigerfs.%s_history ORDER BY version_id DESC LIMIT %d`, cfg.Workspace, historyDumpLimit))
	if err != nil {
		return out, fmt.Errorf("query history: %w", err)
	}
	out.HistoryLastN = histRows

	return out, nil
}

// queryAsMaps executes a query and returns each row as a column->value
// map. Field names come from the result description so the JSON output
// matches the SQL projection exactly.
func queryAsMaps(ctx context.Context, conn *pgx.Conn, query string) ([]map[string]interface{}, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = string(f.Name)
	}

	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		m := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			m[c] = vals[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Stable ordering for the cols list -- map ranges are random and a
	// stable JSON output makes diffing dumps easier.
	sort.Strings(cols)
	return out, nil
}

// writeJSON marshals v indented and writes it to dumpDir/name. Errors
// are logged to stderr but do not propagate -- a single missing file
// shouldn't take the whole dump down.
func writeJSON(dumpDir, name string, v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] dump: marshal %s: %v\n", name, err)
		return
	}
	if err := os.WriteFile(filepath.Join(dumpDir, name), data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] dump: write %s: %v\n", name, err)
	}
}

func writeText(dumpDir, name, content string) {
	if err := os.WriteFile(filepath.Join(dumpDir, name), []byte(content), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] dump: write %s: %v\n", name, err)
	}
}

// nfsChunkBytes is the NFS write chunk size (matches mount option
// wsize=131072). A user-level write of N bytes fans out into
// ceil(N/nfsChunkBytes) WRITE+CLOSE RPCs, each producing one log entry,
// so this is the upper bound on a single op's log_count.
const nfsChunkBytes = 128 * 1024

// analyzeDump renders analysis.txt -- a pre-computed cross-reference of
// the most useful dump observations and a battery of anomaly checks.
// Called from WriteDump so every dump is self-explanatory without ad-hoc
// queries: at a glance the reader sees workspace status, stack island
// structure, log_count distribution, op counts, and any flagged
// anomalies (false positives tolerated; false negatives are not).
func analyzeDump(state *WorkspaceState, stack *StateStack, opLog []OpRecord, issues []ValidationIssue, probe *WalkFailureProbe) string {
	var b strings.Builder

	fmt.Fprintf(&b, "=== Workspace ===\n")
	if len(issues) == 0 {
		fmt.Fprintf(&b, "Validation: PASSED\n")
	} else {
		fmt.Fprintf(&b, "Validation: FAILED (%d issues; see diff.txt)\n", len(issues))
	}
	fmt.Fprintf(&b, "Files: %d  Dirs: %d\n\n", len(state.Files), len(state.Dirs))

	if probe != nil {
		fmt.Fprintf(&b, "=== Walk-failure probe ===\n")
		fmt.Fprintf(&b, "Missing path:    %s\n", probe.MissingRelPath)
		fmt.Fprintf(&b, "FS visibility timeline:\n")
		for _, p := range probe.FS {
			status := "ENOENT"
			if p.Exists {
				if p.IsDir {
					status = "exists (dir)"
				} else {
					status = fmt.Sprintf("exists (file, %d bytes)", p.SizeBytes)
				}
			} else if p.ErrMessage != "" && !strings.Contains(p.ErrMessage, "no such file") {
				status = "err: " + p.ErrMessage
			}
			fmt.Fprintf(&b, "  %4dms: %s\n", p.OffsetMs, status)
		}
		if probe.DB != nil {
			if probe.DB.Resolved {
				fmt.Fprintf(&b, "DB probe:        RESOLVED (%d-deep parent chain)\n", probe.DB.ParentChainLength)
			} else {
				fmt.Fprintf(&b, "DB probe:        UNRESOLVED at %q -- %s\n", probe.DB.UnresolvedAt, probe.DB.UnresolvedReason)
			}
		}
		fmt.Fprintf(&b, "Conclusion:      %s\n\n", probe.Conclusion)
	}

	fmt.Fprintf(&b, "=== Stack islands ===\n")
	islands := stackIslands(stack)
	parts := make([]string, 0, len(islands))
	for _, isl := range islands {
		if isl.start == isl.end {
			parts = append(parts, fmt.Sprintf("[%d]", isl.start))
		} else {
			parts = append(parts, fmt.Sprintf("[%d..%d]", isl.start, isl.end))
		}
	}
	fmt.Fprintf(&b, "%d entries: %s\n", len(stack.entries), strings.Join(parts, " "))
	if len(stack.savepoints) > 0 {
		spNames := make([]string, 0, len(stack.savepoints))
		for n := range stack.savepoints {
			spNames = append(spNames, n)
		}
		sort.Strings(spNames)
		fmt.Fprintf(&b, "Savepoints (%d): %s\n", len(spNames), strings.Join(spNames, ", "))
	}
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "=== log_count distribution ===\n")
	dist := map[int]int{}
	for _, e := range stack.entries {
		dist[e.LogCount]++
	}
	keys := make([]int, 0, len(dist))
	for k := range dist {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "  log_count=%-3d  %d entries\n", k, dist[k])
	}
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "=== Anomalies ===\n")
	anomalies := detectAnomalies(opLog, stack, issues)
	if len(anomalies) == 0 {
		fmt.Fprintf(&b, "(none detected)\n")
	} else {
		for _, a := range anomalies {
			fmt.Fprintf(&b, "WARNING  %s\n", a)
		}
	}
	fmt.Fprintln(&b)

	fmt.Fprintf(&b, "=== Op counts ===\n")
	opCounts := map[string]int{}
	for _, op := range opLog {
		opCounts[op.OpName]++
	}
	opNames := make([]string, 0, len(opCounts))
	for n := range opCounts {
		opNames = append(opNames, n)
	}
	sort.Strings(opNames)
	for _, n := range opNames {
		fmt.Fprintf(&b, "  %-20s %d\n", n, opCounts[n])
	}

	return b.String()
}

// island groups consecutive iterations from the stack. Gaps between
// islands correspond to undo operations that trimmed the stack -- their
// pattern is one of the most useful "what's the run shape" signals.
type island struct{ start, end int }

func stackIslands(stack *StateStack) []island {
	if len(stack.entries) == 0 {
		return nil
	}
	out := []island{{stack.entries[0].Iteration, stack.entries[0].Iteration}}
	for i := 1; i < len(stack.entries); i++ {
		iter := stack.entries[i].Iteration
		if iter == out[len(out)-1].end+1 {
			out[len(out)-1].end = iter
		} else {
			out = append(out, island{iter, iter})
		}
	}
	return out
}

// detectAnomalies runs heuristics over the op trace, stack, and issues.
// Each finding produces a short "what looks wrong" string for analysis.txt.
// Bias toward false positives: a noisy flag is a cheap way to surface
// something the reader will quickly dismiss; a missed regression is
// invisible until the next 1000-iter run also passes silently.
func detectAnomalies(opLog []OpRecord, stack *StateStack, issues []ValidationIssue) []string {
	var out []string

	// 1. log_count vs op-type expectation. NFS chunks at nfsChunkBytes;
	//    a small create can produce at most 1 log entry, a large one
	//    ceil(size/chunk). Anything wildly higher implies lastLogID
	//    regressed during a prior undo and the next op picked up old
	//    entries as if they were new.
	for _, op := range opLog {
		actual := len(op.NewLogIDs)
		if actual <= 1 {
			continue
		}
		if op.OpName == "create_savepoint" {
			out = append(out, fmt.Sprintf("iter %d: create_savepoint logged %d entries (expected 0)", op.Iteration, actual))
			continue
		}
		if strings.HasPrefix(op.OpName, "undo_") {
			continue // undo bookkeeping is handled separately
		}
		expected := 1
		switch op.OpName {
		case "create_file", "edit_file":
			if size, ok := parseSizeBytes(op.Desc); ok {
				expected = (size + nfsChunkBytes - 1) / nfsChunkBytes
				if expected < 1 {
					expected = 1
				}
			}
		}
		// Tolerance of +2 absorbs off-by-one chunk-boundary cases (e.g.,
		// a 128KB file can land as 1 or 2 chunks depending on NFS client
		// framing) without burying real regressions.
		if actual > expected+2 {
			out = append(out, fmt.Sprintf(
				"iter %d: log_count=%d for %q (expected ~%d, off by %d) -- likely lastLogID regression in a prior undo",
				op.Iteration, actual, op.Desc, expected, actual-expected))
		}
	}

	// 2. UUIDv7 monotonicity in the stack. Each subsequent stack entry
	//    captures a later state, so its LogID (when set) must be >
	//    the previous logged entry's LogID. A regression means stack
	//    bookkeeping crossed an iteration boundary in the wrong order.
	var prevID string
	var prevIter int
	for _, e := range stack.entries {
		if e.LogID == "" {
			continue
		}
		if prevID != "" && e.LogID < prevID {
			out = append(out, fmt.Sprintf(
				"stack iter %d: log_id %s < prior iter %d's %s (UUIDv7 regression)",
				e.Iteration, e.LogID, prevIter, prevID))
		}
		prevID = e.LogID
		prevIter = e.Iteration
	}

	// 3. Rename / move artifacts in failure dumps. Pair MissingFile +
	//    UnexpectedFile by content hash -- a match means the file is
	//    really still on disk, just at a different path. The likely
	//    cause is TigerFS and the stress-test diverging on a rename
	//    or move (one applied it, the other didn't).
	if len(issues) > 0 {
		unexpectedByHash := map[string]string{}
		for _, iss := range issues {
			if iss.Kind == IssueUnexpectedFile {
				unexpectedByHash[iss.ActualHash] = iss.Path
			}
		}
		for _, iss := range issues {
			if iss.Kind != IssueMissingFile {
				continue
			}
			if foundAt, ok := unexpectedByHash[iss.ExpectedHash]; ok {
				out = append(out, fmt.Sprintf(
					"rename artifact: %q expected, found at %q (hash %s match) -- TigerFS and stress-test diverged on a rename/move",
					iss.Path, foundAt, shortHash(iss.ExpectedHash)))
			}
		}
	}

	return out
}

// parseSizeBytes extracts the trailing "(NNN.NB)" / "(NN.NKB)" /
// "(N.NMB)" / "(N.NGB)" from an op description like
// "create_file foo (134.5KB)". Returns (bytes, true) on success;
// (0, false) when there's no parseable size suffix (e.g., for a
// dir-content summary like "(3 files, 0 subdirs)").
func parseSizeBytes(desc string) (int, bool) {
	open := strings.LastIndex(desc, "(")
	closeIdx := strings.LastIndex(desc, ")")
	if open == -1 || closeIdx <= open {
		return 0, false
	}
	inner := desc[open+1 : closeIdx]
	var num float64
	var unit string
	n, err := fmt.Sscanf(inner, "%f%s", &num, &unit)
	if err != nil || n != 2 {
		return 0, false
	}
	mult := 0
	switch unit {
	case "B":
		mult = 1
	case "KB":
		mult = 1024
	case "MB":
		mult = 1024 * 1024
	case "GB":
		mult = 1024 * 1024 * 1024
	default:
		return 0, false
	}
	return int(num * float64(mult)), true
}

// === Walk-failure probes ===
//
// A validation walk failure of the form "open <abs-path>: no such file or
// directory" or "readdirent <abs-path>: no such file or directory" is
// suspicious because postgres usually still has the row -- this is the
// FUSE-side cache window investigation. The probes below run automatically
// when WriteDump sees such an error and produce evidence that pins which
// layer lied:
//
//   * FS visibility probe: os.Stat the missing path at increasing offsets,
//     measuring how long until it becomes visible again (or "never" within
//     the probe window).
//   * DB row probe: walk the captured workspace rows by parent_id chain to
//     check whether the missing path is actually present in postgres at
//     this moment. If yes -- userspace tigerfs lied. If no -- we have a
//     real correctness / state-tracking issue.

// FSVisibilityProbe is one os.Stat attempt at a given offset from
// dump-start. Records whether the path was visible and any errno.
type FSVisibilityProbe struct {
	OffsetMs   int64  `json:"offset_ms"`
	Exists     bool   `json:"exists"`
	IsDir      bool   `json:"is_dir,omitempty"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	ErrMessage string `json:"err_message,omitempty"`
}

// DBProbeResult records what postgres knows about the missing relative path.
// Resolves the path component-by-component through the parent_id chain.
type DBProbeResult struct {
	Resolved          bool                   `json:"resolved"`
	UnresolvedAt      string                 `json:"unresolved_at,omitempty"`
	UnresolvedReason  string                 `json:"unresolved_reason,omitempty"`
	Row               map[string]interface{} `json:"row,omitempty"`
	ParentChainLength int                    `json:"parent_chain_length,omitempty"`
}

// WalkFailureProbe is the full walk_failure_probe.json payload.
type WalkFailureProbe struct {
	MissingRelPath string              `json:"missing_rel_path"`
	WorkspacePath  string              `json:"workspace_path"`
	WalkErrorRaw   string              `json:"walk_error_raw"`
	FS             []FSVisibilityProbe `json:"fs_visibility_timeline"`
	DB             *DBProbeResult      `json:"db_probe,omitempty"`
	Conclusion     string              `json:"conclusion"`
}

// walkFailurePathRe matches the absolute path embedded in a walk-failure
// error message produced by snapshotWorkspace. Captures the path component
// after the syscall name. Examples we want to match:
//
//	"... open <abs>: no such file or directory"        (ENOENT on file)
//	"... readdirent <abs>: no such file or directory"  (ENOENT on dir)
//	"... open <abs>: input/output error"               (EIO on file)
//	"... read <abs>: input/output error"               (EIO during read)
//	"... stat <abs>: <any errno>"                      (any Stat-time errno)
//
// We deliberately match a broad family of "this path misbehaved" errors so
// the probe fires on any FS-side anomaly worth investigating, not just
// ENOENT.
var walkFailurePathRe = regexp.MustCompile(`(?:open|readdirent|read|stat|lstat|fstat)\s+(/\S+):\s+\S`)

// extractMissingPath pulls the absolute path of the failing entry from a
// walk-failure error. Returns ("", false) if the error doesn't match the
// expected shape (e.g. it's a non-walk failure like a validation diff).
func extractMissingPath(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	m := walkFailurePathRe.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}

// fsProbeOffsets is the schedule of os.Stat attempts in probeFSVisibility,
// measured from probe start. Set wide enough to span the kernel FUSE entry
// cache TTL (1s default) and tigerfs's own 2s pathCache TTL. Overridable
// in tests so they don't sleep the full 5s on every dump.
var fsProbeOffsets = []time.Duration{
	0,
	100 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
}

// probeFSVisibility os.Stat's the absolute path at fsProbeOffsets. Total
// wallclock is bounded by the largest offset (~5s) by default, which is
// acceptable inside dump generation -- the bigger cost is collecting the
// evidence.
func probeFSVisibility(absPath string) []FSVisibilityProbe {
	offsets := fsProbeOffsets
	out := make([]FSVisibilityProbe, 0, len(offsets))
	start := time.Now()
	for _, off := range offsets {
		// Sleep until we hit this offset from start (compensating for the
		// time the previous Stat took).
		elapsed := time.Since(start)
		if off > elapsed {
			time.Sleep(off - elapsed)
		}
		info, err := os.Stat(absPath)
		probe := FSVisibilityProbe{OffsetMs: off.Milliseconds()}
		if err != nil {
			probe.ErrMessage = err.Error()
		} else {
			probe.Exists = true
			probe.IsDir = info.IsDir()
			if !info.IsDir() {
				probe.SizeBytes = info.Size()
			}
		}
		out = append(out, probe)
	}
	return out
}

// probeDBForRelPath walks the captured workspace rows by parent_id chain to
// resolve the missing path. Returns a structured result distinguishing
// "resolved" from "stuck at <component>" so we can tell which directory
// segment is missing if any.
func probeDBForRelPath(rows []map[string]interface{}, relPath string) *DBProbeResult {
	relPath = strings.Trim(relPath, "/")
	if relPath == "" {
		return &DBProbeResult{
			Resolved:          true,
			Row:               map[string]interface{}{"workspace_root": true},
			ParentChainLength: 0,
		}
	}
	parts := strings.Split(relPath, "/")
	var parentID interface{} // nil at root
	var lastRow map[string]interface{}
	for i, name := range parts {
		match := findRowByParentAndName(rows, parentID, name)
		if match == nil {
			return &DBProbeResult{
				Resolved:         false,
				UnresolvedAt:     strings.Join(parts[:i+1], "/"),
				UnresolvedReason: fmt.Sprintf("no row with parent_id=%v and filename=%q", parentID, name),
			}
		}
		lastRow = match
		parentID = match["id"]
	}
	return &DBProbeResult{
		Resolved:          true,
		Row:               lastRow,
		ParentChainLength: len(parts),
	}
}

// findRowByParentAndName scans rows for a child of the given parent. Used
// to walk the parent_id chain when resolving a path. parentID==nil matches
// roots (parent_id IS NULL in the DB; pgx surfaces that as a Go nil here).
func findRowByParentAndName(rows []map[string]interface{}, parentID interface{}, name string) map[string]interface{} {
	for _, r := range rows {
		fname, _ := r["filename"].(string)
		if fname != name {
			continue
		}
		rowParent := r["parent_id"]
		if parentMatches(rowParent, parentID) {
			return r
		}
	}
	return nil
}

// parentMatches handles the (nil-or-string) comparison for parent_id
// without tripping on Go's interface-nil quirks.
func parentMatches(rowParent, want interface{}) bool {
	if want == nil {
		return rowParent == nil
	}
	if rowParent == nil {
		return false
	}
	rs, _ := rowParent.(string)
	ws, _ := want.(string)
	return rs == ws && rs != ""
}

// renderProbeConclusion builds a one-line summary that pins which layer is
// at fault, derived from the FS timeline + DB result.
func renderProbeConclusion(fs []FSVisibilityProbe, db *DBProbeResult) string {
	// Find the first offset at which the FS started seeing the path.
	recoverIdx := -1
	for i, p := range fs {
		if p.Exists {
			recoverIdx = i
			break
		}
	}
	dbHas := db != nil && db.Resolved
	switch {
	case dbHas && recoverIdx == 0:
		return "DB has row; FS visible immediately on first re-probe (race window closed before probe started)"
	case dbHas && recoverIdx > 0:
		return fmt.Sprintf("DB has row; FS recovered visibility at offset %dms -- userspace lied during walk, recovered within probe window", fs[recoverIdx].OffsetMs)
	case dbHas && recoverIdx < 0:
		return "DB has row; FS still ENOENT at probe end (>5s) -- userspace persistently lying"
	case !dbHas && recoverIdx >= 0:
		return "DB does NOT have row; FS sees path -- DB capture missed it (race), or stress-runner expected something the DB never had"
	case !dbHas && recoverIdx < 0:
		return "DB does NOT have row; FS still ENOENT -- consistent absence (stress-runner expected something not in DB)"
	default:
		return "inconclusive"
	}
}
