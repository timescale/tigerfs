package main

import (
	"math/rand"
	"strings"
	"testing"
)

func TestGenerateFileSize_DefaultBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cfg := defaultSizeConfig

	for i := 0; i < 1000; i++ {
		size := generateFileSize(rng, cfg)
		if size < 64 {
			t.Errorf("iteration %d: size %d below minimum 64", i, size)
		}
		if size > cfg.MaxBytes {
			t.Errorf("iteration %d: size %d above max %d", i, size, cfg.MaxBytes)
		}
	}
}

func TestGenerateFileSize_LargeBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cfg := largeSizeConfig

	for i := 0; i < 1000; i++ {
		size := generateFileSize(rng, cfg)
		if size < 64 {
			t.Errorf("iteration %d: size %d below minimum 64", i, size)
		}
		if size > cfg.MaxBytes {
			t.Errorf("iteration %d: size %d above max %d", i, size, cfg.MaxBytes)
		}
	}
}

func TestGenerateFileSize_Deterministic(t *testing.T) {
	sizes1 := make([]int, 20)
	sizes2 := make([]int, 20)

	rng1 := rand.New(rand.NewSource(123))
	rng2 := rand.New(rand.NewSource(123))

	for i := range sizes1 {
		sizes1[i] = generateFileSize(rng1, defaultSizeConfig)
		sizes2[i] = generateFileSize(rng2, defaultSizeConfig)
	}

	for i := range sizes1 {
		if sizes1[i] != sizes2[i] {
			t.Errorf("iteration %d: sizes differ (%d vs %d) with same seed", i, sizes1[i], sizes2[i])
		}
	}
}

func TestGenerateContent_Deterministic(t *testing.T) {
	rng1 := rand.New(rand.NewSource(42))
	rng2 := rand.New(rand.NewSource(42))

	c1 := generateContent(rng1, "test", 500)
	c2 := generateContent(rng2, "test", 500)

	if c1 != c2 {
		t.Error("same seed should produce identical content")
	}
}

func TestGenerateContent_ApproximateSize(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for _, targetSize := range []int{64, 500, 5000, 50000} {
		content := generateContent(rng, "test", targetSize)
		// Content ends at a natural line boundary, so it will be at least targetSize
		// but may overshoot by up to one line (~200 bytes max).
		if len(content) < targetSize {
			t.Errorf("content length %d shorter than target %d", len(content), targetSize)
		}
		maxOvershoot := 300
		if len(content) > targetSize+maxOvershoot {
			t.Errorf("content length %d overshoots target %d by more than %d", len(content), targetSize, maxOvershoot)
		}
	}
}

func TestRandomName_Valid(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 100; i++ {
		name := randomName(rng)
		if name == "" {
			t.Errorf("iteration %d: empty name", i)
		}
		if len(name) > 20 {
			t.Errorf("iteration %d: name too long: %s", i, name)
		}
	}
}

func TestRandomName_Deterministic(t *testing.T) {
	rng1 := rand.New(rand.NewSource(42))
	rng2 := rand.New(rand.NewSource(42))

	for i := 0; i < 20; i++ {
		n1 := randomName(rng1)
		n2 := randomName(rng2)
		if n1 != n2 {
			t.Errorf("iteration %d: names differ with same seed: %s vs %s", i, n1, n2)
		}
	}
}

func TestRandomWords(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	words := randomWords(rng, 5)
	if words == "" {
		t.Error("randomWords returned empty string")
	}
	// Should have spaces
	parts := len(words) - len(strings.ReplaceAll(words, " ", ""))
	if parts != 4 { // 5 words = 4 spaces
		t.Errorf("expected 4 spaces in 5 words, got %d", parts)
	}
}

func TestPools_Basic(t *testing.T) {
	pools := NewPools()

	if len(pools.Dirs) != 1 || pools.Dirs[0] != "" {
		t.Error("new pools should have root dir")
	}

	pools.AddFile("a.md")
	pools.AddFile("b.md")
	pools.AddDir("docs")

	if len(pools.Files) != 2 {
		t.Errorf("expected 2 files, got %d", len(pools.Files))
	}

	pools.RemoveFile("a.md")
	if len(pools.Files) != 1 || pools.Files[0] != "b.md" {
		t.Error("RemoveFile didn't work correctly")
	}

	nonRoot := pools.NonRootDirs()
	if len(nonRoot) != 1 || nonRoot[0] != "docs" {
		t.Error("NonRootDirs should return docs")
	}
}

func TestPools_RenameDir(t *testing.T) {
	pools := NewPools()
	pools.AddDir("docs")
	pools.AddDir("docs/sub")
	pools.AddFile("docs/a.md")
	pools.AddFile("docs/sub/b.md")
	pools.AddFile("root.md")

	pools.RenameDir("docs", "notes")

	found := false
	for _, d := range pools.Dirs {
		if d == "docs" {
			t.Error("old dir still in pool")
		}
		if d == "notes" {
			found = true
		}
	}
	if !found {
		t.Error("new dir not in pool")
	}

	for _, f := range pools.Files {
		if strings.HasPrefix(f, "docs/") {
			t.Errorf("file %s still has old prefix", f)
		}
	}
}

func TestDirHasContents(t *testing.T) {
	state := NewWorkspaceState()
	state.AddDir("docs")
	state.AddDir("empty")
	state.AddDir("docs/sub")
	state.SetFile("docs/sub/a.md", "h1")

	if !dirHasContents("docs", state) {
		t.Error("docs should have contents (subdir + file)")
	}
	if !dirHasContents("docs/sub", state) {
		t.Error("docs/sub should have contents (a.md)")
	}
	if dirHasContents("empty", state) {
		t.Error("empty should have no contents")
	}
}

func TestCountDirContents(t *testing.T) {
	state := NewWorkspaceState()
	state.AddDir("docs")
	state.AddDir("docs/sub")
	state.AddDir("docs/sub/deep")
	state.SetFile("docs/a.md", "h1")
	state.SetFile("docs/sub/b.md", "h2")
	state.SetFile("docs/sub/deep/c.md", "h3")
	state.SetFile("other.md", "h4") // outside docs

	files, subdirs := countDirContents("docs", state)
	if files != 3 {
		t.Errorf("docs files: got %d, want 3", files)
	}
	if subdirs != 2 {
		t.Errorf("docs subdirs: got %d, want 2", subdirs)
	}

	files, subdirs = countDirContents("docs/sub", state)
	if files != 2 {
		t.Errorf("docs/sub files: got %d, want 2", files)
	}
	if subdirs != 1 {
		t.Errorf("docs/sub subdirs: got %d, want 1", subdirs)
	}
}

func TestPickNonRootDirBiased_Empty(t *testing.T) {
	pools := NewPools()
	state := NewWorkspaceState()
	rng := rand.New(rand.NewSource(1))
	if got := pickNonRootDirBiased(rng, pools, state); got != "" {
		t.Errorf("no non-root dirs: got %q, want empty", got)
	}
}

func TestPickNonRootDirBiased_OnlyEmpty(t *testing.T) {
	pools := NewPools()
	pools.AddDir("a")
	pools.AddDir("b")
	state := NewWorkspaceState()
	state.AddDir("a")
	state.AddDir("b")
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 10; i++ {
		got := pickNonRootDirBiased(rng, pools, state)
		if got != "a" && got != "b" {
			t.Errorf("iter %d: got %q, want a or b", i, got)
		}
	}
}

func TestPickNonRootDirBiased_PrefersNonEmpty(t *testing.T) {
	// Set up: one dir with contents, three empty dirs.
	// Expect the non-empty dir to be chosen ~70% of the time.
	pools := NewPools()
	pools.AddDir("full")
	pools.AddDir("empty1")
	pools.AddDir("empty2")
	pools.AddDir("empty3")
	pools.AddFile("full/a.md")

	state := NewWorkspaceState()
	state.AddDir("full")
	state.AddDir("empty1")
	state.AddDir("empty2")
	state.AddDir("empty3")
	state.SetFile("full/a.md", "h")

	rng := rand.New(rand.NewSource(42))
	fullCount := 0
	const iters = 1000
	for i := 0; i < iters; i++ {
		if pickNonRootDirBiased(rng, pools, state) == "full" {
			fullCount++
		}
	}
	// Target 70% +/- 5% (bias is 70/30 between groups, but within the empty
	// group each dir gets 10% so "full" alone vs each "empty_i" is 70 vs 10).
	if fullCount < 650 || fullCount > 750 {
		t.Errorf("expected ~700/1000 for non-empty dir, got %d", fullCount)
	}
}

func TestCanMoveDir_SingleRootChild(t *testing.T) {
	// Regression: a single non-root dir whose parent is root has no valid
	// destination -- root is its current parent, and the dir itself is
	// excluded. canMoveDir must return false in this case.
	pools := NewPools()
	pools.AddDir("docs")
	state := NewWorkspaceState()
	state.AddDir("docs")
	if canMoveDir(pools, state) {
		t.Error("canMoveDir should return false when only source is a root-child with no sibling")
	}
}

func TestCanMoveDir_TwoRootChildren(t *testing.T) {
	pools := NewPools()
	pools.AddDir("a")
	pools.AddDir("b")
	state := NewWorkspaceState()
	state.AddDir("a")
	state.AddDir("b")
	if !canMoveDir(pools, state) {
		t.Error("canMoveDir should return true with two sibling root-child dirs")
	}
}

func TestCanMoveDir_NestedHasRootDest(t *testing.T) {
	// Nested dir can always move to root.
	pools := NewPools()
	pools.AddDir("a")
	pools.AddDir("a/b")
	state := NewWorkspaceState()
	state.AddDir("a")
	state.AddDir("a/b")
	if !canMoveDir(pools, state) {
		t.Error("canMoveDir should return true when a nested dir can move to root")
	}
}

func TestValidMoveDirDests_ExcludesSelfParentAndDescendants(t *testing.T) {
	pools := NewPools()
	pools.AddDir("a")
	pools.AddDir("a/b")
	pools.AddDir("a/b/c")
	pools.AddDir("other")
	state := NewWorkspaceState()
	state.AddDir("a")
	state.AddDir("a/b")
	state.AddDir("a/b/c")
	state.AddDir("other")

	// Moving "a/b": current parent is "a", self is "a/b", descendants are "a/b/c".
	// Valid dests: "" (root), "other".
	dests := validMoveDirDests("a/b", pools, state)
	got := map[string]bool{}
	for _, d := range dests {
		got[d] = true
	}
	if !got[""] || !got["other"] || len(got) != 2 {
		t.Errorf("validMoveDirDests(a/b): got %v, want [\"\" other]", dests)
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes int
		want  string
	}{
		{100, "100B"},
		{1024, "1.0KB"},
		{5120, "5.0KB"},
		{1048576, "1.0MB"},
	}
	for _, tt := range tests {
		got := formatSize(tt.bytes)
		if got != tt.want {
			t.Errorf("formatSize(%d) = %s, want %s", tt.bytes, got, tt.want)
		}
	}
}
