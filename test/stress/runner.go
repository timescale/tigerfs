package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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

// RunIterations executes the main test loop.
func RunIterations(cfg *Config, infra *Infra) error {
	rng := rand.New(rand.NewSource(cfg.Seed))
	opCfg := NewOpConfig(cfg.LargeFiles, cfg.ManyFiles)
	wsPath := filepath.Join(infra.Mountpoint, cfg.Workspace)

	state := NewWorkspaceState()
	stack := NewStateStack()
	pools := NewPools()
	stats := NewStats()

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
			return fmt.Errorf("[STEP %d/%d] %s failed: %w", i, cfg.Iterations, opName(op), err)
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
		if !isUndo && stack.Len() == preStackLen+1 {
			newIDs := readLogIDsSince(wsPath, lastLogID)
			if len(newIDs) > 0 {
				stack.SetLogIDsForLastEntry(newIDs)
				lastLogID = newIDs[len(newIDs)-1]
			}
			// If newIDs is empty, this op didn't log (e.g., create_savepoint).
			// LogID stays empty on the stack entry.
		} else if !isUndo {
			// Op managed its own stack growth (OpDeleteDir). Refresh
			// lastLogID to the latest log entry so subsequent ops see all
			// of the op's log entries as "old".
			if latest := readLatestLogID(wsPath); latest != "" {
				lastLogID = latest
			}
		}

		// For undo operations, restore state and rebuild pools
		if isUndo && restoredState != nil {
			state = restoredState
			pools = RebuildPools(state)
			lastLogID = readLatestLogID(wsPath)
		}

		// Validate
		shouldValidate := isUndo || (cfg.ValidateEvery > 0 && i%cfg.ValidateEvery == 0)
		if shouldValidate {
			if err := ValidateWorkspace(wsPath, state); err != nil {
				return fmt.Errorf("[STEP %d/%d] Seed=%d validation failed after %s:\n%w",
					i, cfg.Iterations, cfg.Seed, desc, err)
			}
		}
	}

	// Final validation
	fmt.Println()
	fmt.Print("Final validation... ")
	if err := ValidateWorkspace(wsPath, state); err != nil {
		return fmt.Errorf("Seed=%d final validation failed:\n%w", cfg.Seed, err)
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
		desc, err := OpDeleteDir(wsPath, rng, pools, state, cfg, stack, iteration)
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

// RunAndExit runs the test iterations and returns the exit code.
func RunAndExit(cfg *Config, infra *Infra) int {
	err := RunIterations(cfg, infra)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[ERROR] %v\n", err)
		fmt.Fprintf(os.Stderr, "Replay with: bin/tigerfs-stress start --seed %d --iterations %d\n", cfg.Seed, cfg.Iterations)
		return 1
	}
	return 0
}
