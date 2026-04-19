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
func canExecute(op opType, pools *Pools, stack *StateStack) bool {
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
	case opCreateSavepoint:
		return true
	case opUndoSingle:
		return stack.LoggedCount() > 0
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

	var lastLogID string

	for i := 1; i <= cfg.Iterations; i++ {
		// Select operation (with re-roll for unmet preconditions)
		op := selectValidOperation(rng, pools, stack)

		// Determine if this is an undo operation
		isUndo := op == opUndoSingle || op == opUndoToID || op == opUndoToSavepoint

		// Push state before non-undo operations
		if !isUndo {
			stack.Push(state, i)
		}

		// Execute operation
		desc, restoredState, err := executeOperation(op, wsPath, rng, pools, state, opCfg, stack, i)
		if err != nil {
			return fmt.Errorf("[STEP %d/%d] %s failed: %w", i, cfg.Iterations, opName(op), err)
		}

		fmt.Printf("[STEP %d/%d] %s\n", i, cfg.Iterations, desc)

		// Record log_id for non-undo operations
		if !isUndo {
			currentLogID := readLatestLogID(wsPath)
			if currentLogID != "" && currentLogID != lastLogID {
				stack.SetLastLogID(currentLogID)
				lastLogID = currentLogID
			}
			// If currentLogID == lastLogID, this operation didn't create a log entry
			// (e.g., create_dir, create_savepoint). LogID stays empty on the stack entry.
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
	return nil
}

// selectValidOperation picks an operation that can be executed.
// Falls back to create_file if nothing else works.
func selectValidOperation(rng *rand.Rand, pools *Pools, stack *StateStack) opType {
	// Try up to 50 times to find a valid operation
	for attempt := 0; attempt < 50; attempt++ {
		op := selectOperation(rng)
		if canExecute(op, pools, stack) {
			return op
		}
	}
	// Fallback: create_file is almost always valid (dirs always exist)
	if canExecute(opCreateFile, pools, stack) {
		return opCreateFile
	}
	// Ultimate fallback: create_dir
	return opCreateDir
}

// executeOperation dispatches to the appropriate operation function.
// Returns (description, restoredState, error). restoredState is non-nil only
// for undo operations -- the caller should use it to replace the current state.
func executeOperation(op opType, wsPath string, rng *rand.Rand, pools *Pools,
	state *WorkspaceState, cfg *OpConfig, stack *StateStack, iteration int) (string, *WorkspaceState, error) {

	switch op {
	case opCreateFile:
		desc, err := OpCreateFile(wsPath, rng, pools, state, cfg)
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
