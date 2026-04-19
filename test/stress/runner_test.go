package main

import (
	"math/rand"
	"testing"
)

func TestSelectOperation_Deterministic(t *testing.T) {
	rng1 := rand.New(rand.NewSource(42))
	rng2 := rand.New(rand.NewSource(42))

	ops1 := make([]opType, 100)
	ops2 := make([]opType, 100)

	for i := range ops1 {
		ops1[i] = selectOperation(rng1)
		ops2[i] = selectOperation(rng2)
	}

	for i := range ops1 {
		if ops1[i] != ops2[i] {
			t.Errorf("iteration %d: ops differ with same seed (%s vs %s)", i, opName(ops1[i]), opName(ops2[i]))
		}
	}
}

func TestSelectOperation_AllTypesAppear(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	seen := make(map[opType]bool)

	// With enough iterations, all types should appear
	for i := 0; i < 10000; i++ {
		seen[selectOperation(rng)] = true
	}

	for _, wo := range operationTable {
		if !seen[wo.op] {
			t.Errorf("operation %s never selected in 10000 iterations", wo.name)
		}
	}
}

func TestCanExecute_EmptyState(t *testing.T) {
	pools := NewPools() // only root dir
	stack := NewStateStack()

	// Should be executable with empty state
	if !canExecute(opCreateFile, pools, stack) {
		t.Error("create_file should be valid with root dir")
	}
	if !canExecute(opCreateDir, pools, stack) {
		t.Error("create_dir should be valid with root dir")
	}
	if !canExecute(opCreateSavepoint, pools, stack) {
		t.Error("create_savepoint should always be valid")
	}

	// Should NOT be executable
	if canExecute(opEditFile, pools, stack) {
		t.Error("edit_file should be invalid with no files")
	}
	if canExecute(opDeleteFile, pools, stack) {
		t.Error("delete_file should be invalid with no files")
	}
	if canExecute(opRenameFile, pools, stack) {
		t.Error("rename_file should be invalid with no files")
	}
	if canExecute(opMoveFile, pools, stack) {
		t.Error("move_file should be invalid with no files")
	}
	if canExecute(opRenameDir, pools, stack) {
		t.Error("rename_dir should be invalid with no non-root dirs")
	}
	if canExecute(opUndoSingle, pools, stack) {
		t.Error("undo_single should be invalid with empty stack")
	}
	if canExecute(opUndoToID, pools, stack) {
		t.Error("undo_to_id should be invalid with empty stack")
	}
	if canExecute(opUndoToSavepoint, pools, stack) {
		t.Error("undo_to_savepoint should be invalid with no savepoints")
	}
}

func TestCanExecute_WithFiles(t *testing.T) {
	pools := NewPools()
	pools.AddFile("test.md")
	pools.AddDir("docs")
	stack := NewStateStack()
	stack.Push(NewWorkspaceState(), 0)
	stack.SetLastLogID("log-0")
	stack.Push(NewWorkspaceState(), 1)
	stack.SetLastLogID("log-1")
	stack.SaveSavepoint("sp1")

	if !canExecute(opEditFile, pools, stack) {
		t.Error("edit_file should be valid with files")
	}
	if !canExecute(opMoveFile, pools, stack) {
		t.Error("move_file should be valid with files + 2 dirs")
	}
	if !canExecute(opRenameDir, pools, stack) {
		t.Error("rename_dir should be valid with non-root dirs")
	}
	if !canExecute(opUndoSingle, pools, stack) {
		t.Error("undo_single should be valid with stack entries")
	}
	if !canExecute(opUndoToID, pools, stack) {
		t.Error("undo_to_id should be valid with 2+ stack entries")
	}
	if !canExecute(opUndoToSavepoint, pools, stack) {
		t.Error("undo_to_savepoint should be valid with savepoints")
	}
}

func TestSelectValidOperation_FallsBack(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	pools := NewPools() // only root dir, no files
	stack := NewStateStack()

	// Should always return something valid (create_file or create_dir)
	for i := 0; i < 100; i++ {
		op := selectValidOperation(rng, pools, stack)
		if !canExecute(op, pools, stack) {
			t.Errorf("iteration %d: selectValidOperation returned %s which can't execute", i, opName(op))
		}
	}
}

func TestOpName(t *testing.T) {
	if opName(opCreateFile) != "create_file" {
		t.Error("wrong name for opCreateFile")
	}
	if opName(opUndoToSavepoint) != "undo_to_savepoint" {
		t.Error("wrong name for opUndoToSavepoint")
	}
}
