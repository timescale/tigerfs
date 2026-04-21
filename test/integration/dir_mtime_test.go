package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getDirMtime reads a directory row's modified_at directly from PostgreSQL.
func getDirMtime(t *testing.T, pool *pgxpool.Pool, tableName, dirname string) time.Time {
	t.Helper()
	var mtime time.Time
	err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT modified_at FROM tigerfs.%q WHERE filename = $1 AND filetype = 'directory'`, tableName),
		dirname,
	).Scan(&mtime)
	require.NoError(t, err, "failed to read mtime for dir %s", dirname)
	return mtime
}

// getRootDirMtime reads the root directory's modified_at (parent_id IS NULL).
func getRootDirMtime(t *testing.T, pool *pgxpool.Pool, tableName string) time.Time {
	t.Helper()
	var mtime time.Time
	// Root entries have parent_id IS NULL. There may be multiple (files + dirs at root),
	// but for mtime we want a directory that serves as the root container.
	// Root-level files have parent_id IS NULL but we want the implicit "root" --
	// use the max modified_at of rows where parent_id IS NULL as a proxy,
	// or query a specific root-level directory if one exists.
	// For these tests we create explicit subdirectories and query those.
	err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT MAX(modified_at) FROM tigerfs.%q WHERE parent_id IS NULL`, tableName),
	).Scan(&mtime)
	require.NoError(t, err, "failed to read root mtime")
	return mtime
}

func TestDirMtime_CreateFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime1")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime1", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create a directory
	fsErr = ops.Mkdir(ctx, "/dirmtime1/docs")
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime1", "docs")

	// Create a file in the directory
	time.Sleep(10 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/dirmtime1/docs/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	require.Nil(t, fsErr)

	mtimeAfter := getDirMtime(t, pool, "dirmtime1", "docs")
	assert.True(t, mtimeAfter.After(mtimeBefore),
		"parent dir mtime should increase after file creation (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}

func TestDirMtime_DeleteFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime2")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime2", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime2/docs")
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime2/docs/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime2", "docs")

	// Delete the file
	time.Sleep(10 * time.Millisecond)
	fsErr = ops.Delete(ctx, "/dirmtime2/docs/hello.md")
	require.Nil(t, fsErr)

	mtimeAfter := getDirMtime(t, pool, "dirmtime2", "docs")
	assert.True(t, mtimeAfter.After(mtimeBefore),
		"parent dir mtime should increase after file deletion (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}

func TestDirMtime_MoveFile(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime3")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime3", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime3/dirA")
	require.Nil(t, fsErr)
	fsErr = ops.Mkdir(ctx, "/dirmtime3/dirB")
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime3/dirA/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeA := getDirMtime(t, pool, "dirmtime3", "dirA")
	mtimeB := getDirMtime(t, pool, "dirmtime3", "dirB")

	// Move file from dirA to dirB
	time.Sleep(10 * time.Millisecond)
	fsErr = ops.Rename(ctx, "/dirmtime3/dirA/hello.md", "/dirmtime3/dirB/hello.md")
	require.Nil(t, fsErr)

	mtimeAAfter := getDirMtime(t, pool, "dirmtime3", "dirA")
	mtimeBAfter := getDirMtime(t, pool, "dirmtime3", "dirB")
	assert.True(t, mtimeAAfter.After(mtimeA),
		"source dir mtime should increase after move (before=%v, after=%v)", mtimeA, mtimeAAfter)
	assert.True(t, mtimeBAfter.After(mtimeB),
		"target dir mtime should increase after move (before=%v, after=%v)", mtimeB, mtimeBAfter)
}

func TestDirMtime_RenameInPlace(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime4")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime4", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime4/docs")
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime4/docs/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime4", "docs")

	// Rename file within same directory
	time.Sleep(10 * time.Millisecond)
	fsErr = ops.Rename(ctx, "/dirmtime4/docs/hello.md", "/dirmtime4/docs/world.md")
	require.Nil(t, fsErr)

	mtimeAfter := getDirMtime(t, pool, "dirmtime4", "docs")
	assert.True(t, mtimeAfter.After(mtimeBefore),
		"parent dir mtime should increase after rename (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}

func TestDirMtime_EditContent(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime5")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime5", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime5/docs")
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime5/docs/hello.md", []byte("---\ntitle: Hello\n---\nOriginal\n"))
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime5", "docs")

	// Edit file content (not filename or parent)
	time.Sleep(10 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/dirmtime5/docs/hello.md", []byte("---\ntitle: Hello\n---\nUpdated content\n"))
	require.Nil(t, fsErr)

	mtimeAfter := getDirMtime(t, pool, "dirmtime5", "docs")
	assert.Equal(t, mtimeBefore, mtimeAfter,
		"parent dir mtime should NOT change on content edit (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}

func TestDirMtime_UndoCreate(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime6")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime6", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime6/docs")
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime6/docs/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime6", "docs")

	// Find the most recent create log entry
	entries, fsErr := ops.ReadDir(ctx, "/dirmtime6/.log/.by/type/create")
	require.Nil(t, fsErr)
	var logID string
	for _, e := range entries {
		if e.Name[0] != '.' {
			logID = e.Name
		}
	}
	require.NotEmpty(t, logID, "should find a create log entry")

	// Undo the file creation (DELETE fires trigger)
	time.Sleep(10 * time.Millisecond)
	_, err = ops.ExecuteUndoSingle(ctx, "public", "dirmtime6", logID)
	require.NoError(t, err)

	mtimeAfter := getDirMtime(t, pool, "dirmtime6", "docs")
	assert.True(t, mtimeAfter.After(mtimeBefore),
		"parent dir mtime should increase after undo of creation (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}

func TestDirMtime_UndoDelete(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime7")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime7", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime7/docs")
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime7/docs/hello.md", []byte("---\ntitle: Hello\n---\nContent\n"))
	require.Nil(t, fsErr)

	// Delete the file
	fsErr = ops.Delete(ctx, "/dirmtime7/docs/hello.md")
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime7", "docs")

	// Undo the deletion (INSERT fires trigger -- row is restored)
	time.Sleep(10 * time.Millisecond)
	entries, fsErr := ops.ReadDir(ctx, "/dirmtime7/.log/.by/type/delete")
	require.Nil(t, fsErr)
	var logID string
	for _, e := range entries {
		if e.Name[0] != '.' {
			logID = e.Name
		}
	}
	require.NotEmpty(t, logID, "should find a delete log entry")
	_, err = ops.ExecuteUndoSingle(ctx, "public", "dirmtime7", logID)
	require.NoError(t, err)

	mtimeAfter := getDirMtime(t, pool, "dirmtime7", "docs")
	assert.True(t, mtimeAfter.After(mtimeBefore),
		"parent dir mtime should increase after undo of deletion (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}

func TestDirMtime_UndoToSavepoint(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "dirmtime8")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	fsErr := ops.WriteFile(ctx, "/.build/dirmtime8", []byte("markdown,history\n"))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	fsErr = ops.Mkdir(ctx, "/dirmtime8/docs")
	require.Nil(t, fsErr)

	// Create savepoint
	fsErr = ops.WriteFile(ctx, "/dirmtime8/.savepoint/sp1.json", []byte(`{"description":"before adds"}`))
	require.Nil(t, fsErr)
	time.Sleep(100 * time.Millisecond)

	// Create files after savepoint
	fsErr = ops.WriteFile(ctx, "/dirmtime8/docs/a.md", []byte("---\ntitle: A\n---\nContent A\n"))
	require.Nil(t, fsErr)
	fsErr = ops.WriteFile(ctx, "/dirmtime8/docs/b.md", []byte("---\ntitle: B\n---\nContent B\n"))
	require.Nil(t, fsErr)

	pool, err := pgxpool.New(ctx, result.ConnStr)
	require.NoError(t, err)
	defer pool.Close()

	time.Sleep(10 * time.Millisecond)
	mtimeBefore := getDirMtime(t, pool, "dirmtime8", "docs")

	// Undo to savepoint (deletes both files)
	time.Sleep(10 * time.Millisecond)
	fsErr = ops.WriteFile(ctx, "/dirmtime8/.undo/to-savepoint/sp1/.apply", []byte("apply"))
	require.Nil(t, fsErr)

	mtimeAfter := getDirMtime(t, pool, "dirmtime8", "docs")
	assert.True(t, mtimeAfter.After(mtimeBefore),
		"parent dir mtime should increase after undo to savepoint (before=%v, after=%v)", mtimeBefore, mtimeAfter)
}
