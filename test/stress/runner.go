package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

// Operation types with weights for random selection.
type opType int

const (
	opCreateFile opType = iota
	opEditFile
	opRenameFile
	opMoveFile
	opDeleteFile
	opCreateDir
	opRenameDir
	opMoveDir
	opDeleteDir
	opCreateSavepoint
	opUndoSingle
	opUndoToID
	opUndoToSavepoint
)

type weightedOp struct {
	op     opType
	weight int
	name   string
}

var operationTable = []weightedOp{
	{opCreateFile, 25, "create_file"},
	{opEditFile, 25, "edit_file"},
	{opRenameFile, 10, "rename_file"},
	{opMoveFile, 10, "move_file"},
	{opDeleteFile, 10, "delete_file"},
	{opCreateDir, 5, "create_dir"},
	{opRenameDir, 5, "rename_dir"},
	{opMoveDir, 5, "move_dir"},
	{opDeleteDir, 3, "delete_dir"},
	{opCreateSavepoint, 5, "create_savepoint"},
	{opUndoSingle, 3, "undo_single"},
	{opUndoToID, 2, "undo_to_id"},
	{opUndoToSavepoint, 2, "undo_to_savepoint"},
}

// totalWeight is the sum of all operation weights.
var totalWeight int

func init() {
	for _, wo := range operationTable {
		totalWeight += wo.weight
	}
}

// selectOperation picks a random operation based on weights.
func selectOperation(rng *rand.Rand) opType {
	r := rng.Intn(totalWeight)
	cumulative := 0
	for _, wo := range operationTable {
		cumulative += wo.weight
		if r < cumulative {
			return wo.op
		}
	}
	return opCreateFile // fallback
}

// opName returns the human-readable name of an operation.
func opName(op opType) string {
	for _, wo := range operationTable {
		if wo.op == op {
			return wo.name
		}
	}
	return "unknown"
}

// canExecute checks if an operation's preconditions are met.
func canExecute(op opType, pools *Pools, state *WorkspaceState, stack *StateStack) bool {
	switch op {
	case opCreateFile:
		return len(pools.Dirs) > 0
	case opEditFile, opRenameFile, opDeleteFile:
		return len(pools.Files) > 0
	case opMoveFile:
		return len(pools.Files) > 0 && len(pools.Dirs) >= 2
	case opCreateDir:
		return len(pools.Dirs) > 0
	case opRenameDir:
		return len(pools.NonRootDirs()) > 0
	case opMoveDir:
		// Need at least one feasible (src, dest) pair. Checking pool counts
		// alone is insufficient -- e.g., a single root-child dir has no valid
		// destination because root is its current parent.
		return canMoveDir(pools, state)
	case opDeleteDir:
		return len(pools.NonRootDirs()) > 0
	case opCreateSavepoint:
		return true
	case opUndoSingle:
		// undo_single only undoes the most recent log entry. If the most
		// recent op produced multiple log entries (e.g., a multi-chunk NFS
		// write of a large file), undoing just one leaves the file in a
		// partial-content state that WorkspaceState (md5-keyed) can't track.
		// Require atomic (1-log-entry) ops only.
		return stack.MostRecentLogIsAtomic()
	case opUndoToID:
		return stack.LoggedCount() >= 2
	case opUndoToSavepoint:
		return stack.HasSavepoints()
	}
	return false
}

// Run-failure kinds. Kept as small string constants so they're stable in
// summary.json (downstream tooling can switch on them) and self-explanatory
// when read back from a dump.
const (
	runFailureValidation = "validation" // ValidateWorkspace returned mismatches
	runFailureOperation  = "operation"  // executeOperation returned an error (EIO, etc.)
)

// RunFailure is returned from RunIterations when the run cannot continue.
// Carries the dump directory path so RunAndExit can surface it in the
// failure message and main.go can skip teardown to leave infrastructure
// live for inspection.
//
// Kind distinguishes the cause:
//   - "validation": the live filesystem diverged from the expected state
//   - "operation":  an op (create_file, edit_file, etc.) returned an error
//
// Both cases produce the same dump format -- the differences live in the
// summary.txt heading and the dump's `failure_kind` field.
type RunFailure struct {
	Kind      string
	DumpDir   string
	Iteration int
	Seed      int64
	Desc      string
	Err       error
}

func (v *RunFailure) Error() string {
	return fmt.Sprintf("[STEP %d] Seed=%d %s failure after %s:\n%v",
		v.Iteration, v.Seed, v.Kind, v.Desc, v.Err)
}

func (v *RunFailure) Unwrap() error { return v.Err }

// RunIterations executes the main test loop.
func RunIterations(cfg *Config, infra *Infra) error {
	rng := rand.New(rand.NewSource(cfg.Seed))
	opCfg := NewOpConfig(cfg.LargeFiles, cfg.ManyFiles)
	wsPath := filepath.Join(infra.Mountpoint, cfg.Workspace)

	state := NewWorkspaceState()
	stack := NewStateStack()
	pools := NewPools()
	stats := NewStats()
	opLog := make([]OpRecord, 0, cfg.Iterations)

	var lastLogID string

	for i := 1; i <= cfg.Iterations; i++ {
		// Select operation (with re-roll for unmet preconditions)
		op := selectValidOperation(rng, pools, state, stack)

		// Determine if this is an undo operation
		isUndo := op == opUndoSingle || op == opUndoToID || op == opUndoToSavepoint

		// Push state before non-undo operations
		preStackLen := stack.Len()
		if !isUndo {
			stack.Push(state, i)
		}

		// Execute operation
		desc, restoredState, err := executeOperation(op, wsPath, rng, pools, state, opCfg, stack, i, stats)
		if err != nil {
			// Operational failure (EIO, precondition mismatch, etc.).
			// Append a marker OpRecord so the dump's op trace shows
			// what failed and why; otherwise the trace would just end
			// abruptly at iter N-1 with no indication of what went
			// wrong at iter N.
			failingDesc := fmt.Sprintf("%s [FAILED: %v]", opName(op), err)
			opLog = append(opLog, OpRecord{
				Iteration: i,
				OpName:    opName(op),
				Desc:      failingDesc,
				Validated: false,
			})
			dumpDir, dumpErr := WriteDump(DumpKindFailure, runFailureOperation, cfg, infra, state, stack, opLog, err, failingDesc, i)
			if dumpErr != nil {
				fmt.Fprintf(os.Stderr, "[WARN] failed to write diagnostics dump: %v\n", dumpErr)
			}
			return &RunFailure{
				Kind:      runFailureOperation,
				DumpDir:   dumpDir,
				Iteration: i,
				Seed:      cfg.Seed,
				Desc:      failingDesc,
				Err:       fmt.Errorf("%s failed: %w", opName(op), err),
			}
		}
		stats.RecordOp(opName(op))

		fmt.Printf("[STEP %d/%d] %s\n", i, cfg.Iterations, desc)

		// Record log_ids for non-undo operations. A single op may produce
		// multiple log entries (e.g., a multi-chunk NFS write of a large
		// file fans out into 1 create + N edits). Capture all of them so
		// undo_single can be gated to atomic (1-log-entry) ops.
		//
		// Skip when the op grew the stack itself (OpDeleteDir pushes one
		// entry per deletion via SetLastLogID, each tagged LogCount=1).
		// Overwriting the final entry with total-deletions LogCount would
		// incorrectly mark it non-atomic.
		var newIDs []string
		if !isUndo && stack.Len() == preStackLen+1 {
			newIDs = readLogIDsSince(wsPath, lastLogID)
			if len(newIDs) > 0 {
				stack.SetLogIDsForLastEntry(newIDs)
				lastLogID = newIDs[len(newIDs)-1]
			}
			// If newIDs is empty, this op didn't log (e.g., create_savepoint).
			// LogID stays empty on the stack entry.
		} else if !isUndo {
			// Op managed its own stack growth (OpDeleteDir). Refresh
			// lastLogID to the latest log entry so subsequent ops see
			// all of the op's log entries as "old".
			//
			// Same staleness window as the post-undo read -- the dump
			// at iter 472 (log_count=65) was caused by *this* call
			// returning a stale value, cascading into the next op's
			// readLogIDsSince. Wrap with the monotonic helper so a
			// regressed read keeps the prior known-good lastLogID
			// instead of cascading old entries forward.
			lastLogID = readLatestLogIDMonotonic(wsPath, lastLogID, i, desc, stats)
		}

		// For undo operations, restore state and rebuild pools.
		//
		// Defensive: readLatestLogID over NFS has been observed to
		// return stale results after a heavy undo_to_savepoint -- the
		// TigerFS-side `.log/.last/N/.export/json` virtual file
		// occasionally yields a snapshot from before the undo's log
		// rows were visible, despite noac mounts and unique paths.
		// When the returned id is *older* than what we already saw,
		// retry briefly. If it never recovers, keep the old lastLogID
		// (we know it's at least correct as a lower bound) and warn.
		// Without this guard, a regressed lastLogID makes the next
		// non-undo op's readLogIDsSince attribute every previously
		// observed-but-newer log entry to that op (the iter-107
		// log_count=61 anomaly was a 60-entry regression of this kind).
		if isUndo && restoredState != nil {
			state = restoredState
			pools = RebuildPools(state)
			lastLogID = readLatestLogIDMonotonic(wsPath, lastLogID, i, desc, stats)
		}

		// Validate
		shouldValidate := isUndo || (cfg.ValidateEvery > 0 && i%cfg.ValidateEvery == 0)
		opLog = append(opLog, OpRecord{
			Iteration: i,
			OpName:    opName(op),
			Desc:      desc,
			NewLogIDs: newIDs,
			Validated: shouldValidate,
		})
		if shouldValidate {
			if err := ValidateWorkspace(wsPath, state); err != nil {
				dumpDir, dumpErr := WriteDump(DumpKindFailure, runFailureValidation, cfg, infra, state, stack, opLog, err, desc, i)
				if dumpErr != nil {
					fmt.Fprintf(os.Stderr, "[WARN] failed to write diagnostics dump: %v\n", dumpErr)
				}
				return &RunFailure{
					Kind:      runFailureValidation,
					DumpDir:   dumpDir,
					Iteration: i,
					Seed:      cfg.Seed,
					Desc:      desc,
					Err:       err,
				}
			}
		}

		// --dump-at: capture a manual snapshot at this iteration.
		// Fires after validation so the dump reflects post-op state.
		// Doesn't stop the run; multiple --dump-at iterations produce
		// multiple snapshots in one run.
		if cfg.DumpAt[i] {
			snapDir, snapErr := WriteDump(DumpKindSnapshot, "", cfg, infra, state, stack, opLog, nil, desc, i)
			if snapErr != nil {
				fmt.Fprintf(os.Stderr, "[WARN] --dump-at %d: snapshot dump failed: %v\n", i, snapErr)
			} else {
				fmt.Fprintf(os.Stderr, "  [dump-at %d] snapshot written to %s\n", i, snapDir)
			}
		}
	}

	// Final validation
	fmt.Println()
	fmt.Print("Final validation... ")
	if err := ValidateWorkspace(wsPath, state); err != nil {
		dumpDir, dumpErr := WriteDump(DumpKindFailure, runFailureValidation, cfg, infra, state, stack, opLog, err, "final-validation", cfg.Iterations)
		if dumpErr != nil {
			fmt.Fprintf(os.Stderr, "[WARN] failed to write diagnostics dump: %v\n", dumpErr)
		}
		return &RunFailure{
			Kind:      runFailureValidation,
			DumpDir:   dumpDir,
			Iteration: cfg.Iterations,
			Seed:      cfg.Seed,
			Desc:      "final-validation",
			Err:       err,
		}
	}
	fmt.Println("PASSED")

	fmt.Printf("\nCompleted %d iterations with seed %d. All validations passed.\n", cfg.Iterations, cfg.Seed)

	stats.Print()
	return nil
}

// selectValidOperation picks an operation that can be executed.
// Falls back to create_file if nothing else works.
func selectValidOperation(rng *rand.Rand, pools *Pools, state *WorkspaceState, stack *StateStack) opType {
	// Try up to 50 times to find a valid operation
	for attempt := 0; attempt < 50; attempt++ {
		op := selectOperation(rng)
		if canExecute(op, pools, state, stack) {
			return op
		}
	}
	// Fallback: create_file is almost always valid (dirs always exist)
	if canExecute(opCreateFile, pools, state, stack) {
		return opCreateFile
	}
	// Ultimate fallback: create_dir
	return opCreateDir
}

// executeOperation dispatches to the appropriate operation function.
// Returns (description, restoredState, error). restoredState is non-nil only
// for undo operations -- the caller should use it to replace the current state.
// stats is updated with per-op metadata (currently just created-file sizes).
func executeOperation(op opType, wsPath string, rng *rand.Rand, pools *Pools,
	state *WorkspaceState, cfg *OpConfig, stack *StateStack, iteration int, stats *Stats) (string, *WorkspaceState, error) {

	switch op {
	case opCreateFile:
		desc, size, err := OpCreateFile(wsPath, rng, pools, state, cfg)
		if err == nil {
			stats.RecordCreatedFileSize(size)
		}
		return desc, nil, err
	case opEditFile:
		desc, err := OpEditFile(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opRenameFile:
		desc, err := OpRenameFile(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opMoveFile:
		desc, err := OpMoveFile(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opDeleteFile:
		desc, err := OpDeleteFile(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opCreateDir:
		desc, err := OpCreateDir(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opRenameDir:
		desc, err := OpRenameDir(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opMoveDir:
		desc, err := OpMoveDir(wsPath, rng, pools, state, cfg)
		return desc, nil, err
	case opDeleteDir:
		desc, err := OpDeleteDir(wsPath, rng, pools, state, cfg, stack, iteration, stats)
		return desc, nil, err
	case opCreateSavepoint:
		desc, err := OpCreateSavepoint(wsPath, rng, pools, state, cfg, iteration, stack)
		return desc, nil, err
	case opUndoSingle:
		return OpUndoSingle(wsPath, stack)
	case opUndoToID:
		return OpUndoToID(wsPath, rng, stack)
	case opUndoToSavepoint:
		return OpUndoToSavepoint(wsPath, stack)
	default:
		return "", nil, fmt.Errorf("unknown operation: %d", op)
	}
}

// RunResult is the outcome of a stress run. KeepInfra is true when the
// caller should skip teardown so the user can inspect the live state
// (DB rows, mounted workspace, FS-level traces) -- currently set on
// validation failure where the dump directory references the live infra.
type RunResult struct {
	ExitCode  int
	KeepInfra bool
}

// RunAndExit runs the test iterations and returns the result. The caller
// (main) decides whether to skip teardown based on KeepInfra.
func RunAndExit(cfg *Config, infra *Infra) RunResult {
	err := RunIterations(cfg, infra)
	if err == nil {
		return RunResult{ExitCode: 0}
	}

	fmt.Fprintf(os.Stderr, "\n[ERROR] %v\n", err)

	var rf *RunFailure
	if errors.As(err, &rf) && rf.DumpDir != "" {
		// Highlight the dump dir prominently -- it's the first place the
		// user should look. Surrounding boxes draw the eye in dense
		// terminal output where the trace can be 1000+ lines.
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, strings.Repeat("=", 70))
		fmt.Fprintf(os.Stderr, "  Failure dump directory: %s\n", rf.DumpDir)
		fmt.Fprintf(os.Stderr, "  Failure kind:           %s\n", rf.Kind)
		fmt.Fprintf(os.Stderr, "  Open %s/summary.txt first.\n", rf.DumpDir)
		fmt.Fprintln(os.Stderr, strings.Repeat("=", 70))
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Infrastructure left running for inspection.\n")
		fmt.Fprintf(os.Stderr, "Mountpoint: %s\n", infra.Mountpoint)
		fmt.Fprintf(os.Stderr, "Postgres:   %s\n", infra.ConnStr)
		fmt.Fprintf(os.Stderr, "Tear down with: bin/tigerfs-stress stop\n")
		fmt.Fprintf(os.Stderr, "\nReplay with: %s\n", replayCommand(cfg))
		return RunResult{ExitCode: 1, KeepInfra: true}
	}

	fmt.Fprintf(os.Stderr, "Replay with: %s\n", replayCommand(cfg))
	return RunResult{ExitCode: 1}
}

// replayCommand reconstructs the full CLI invocation needed to re-run a
// failing seed. Must include every flag that affects the workload (seed,
// iterations, validate-every, large-files, many-files, workspace) plus
// the infrastructure-override flags so a docker-FUSE failure can be
// replayed by re-running the same launcher (or directly with the listed
// flags) and reproducing the same conditions.
func replayCommand(cfg *Config) string {
	parts := []string{
		"bin/tigerfs-stress start",
		fmt.Sprintf("--seed %d", cfg.Seed),
		fmt.Sprintf("--iterations %d", cfg.Iterations),
		fmt.Sprintf("--validate-every %d", cfg.ValidateEvery),
	}
	if cfg.LargeFiles {
		parts = append(parts, "--large-files")
	}
	if cfg.ManyFiles {
		parts = append(parts, "--many-files")
	}
	if cfg.Workspace != "testws" {
		parts = append(parts, fmt.Sprintf("--workspace %s", cfg.Workspace))
	}
	if cfg.DumpAtSpec != "" {
		parts = append(parts, fmt.Sprintf("--dump-at %s", cfg.DumpAtSpec))
	}
	if cfg.ExternalConnStr != "" {
		parts = append(parts, fmt.Sprintf("--external-conn-str %q", cfg.ExternalConnStr))
	}
	if cfg.TigerFSBinary != "" {
		parts = append(parts, fmt.Sprintf("--tigerfs-binary %s", cfg.TigerFSBinary))
	}
	if cfg.MountpointDir != "" {
		parts = append(parts, fmt.Sprintf("--mountpoint-dir %s", cfg.MountpointDir))
	}
	if cfg.DumpDir != "" {
		parts = append(parts, fmt.Sprintf("--dump-dir %s", cfg.DumpDir))
	}
	return strings.Join(parts, " ")
}
