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
		if len(content) > targetSize {
			t.Errorf("content length %d exceeds target %d", len(content), targetSize)
		}
		// Allow some slack for the frontmatter
		if len(content) < targetSize-100 && targetSize > 100 {
			t.Errorf("content length %d too short for target %d", len(content), targetSize)
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
