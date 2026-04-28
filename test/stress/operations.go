package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SizeConfig controls the file size distribution.
type SizeConfig struct {
	MaxBytes  int
	MeanLog   float64 // mu of underlying normal (log-space)
	StdDevLog float64 // sigma of underlying normal (log-space)
}

// DensityConfig controls directory density limits.
type DensityConfig struct {
	MaxFilesPerDir   int
	MaxSubdirsPerDir int
}

var (
	defaultSizeConfig = SizeConfig{
		MaxBytes:  100 * 1024, // 100KB
		MeanLog:   8.5,        // ~5KB typical
		StdDevLog: 1.5,        // spread: 500B - 50KB
	}
	largeSizeConfig = SizeConfig{
		MaxBytes:  10 * 1024 * 1024, // 10MB
		MeanLog:   10.0,             // ~22KB typical
		StdDevLog: 2.5,              // spread: 1KB - 1MB common, occasional 10MB
	}
	defaultDensityConfig = DensityConfig{
		MaxFilesPerDir:   10,
		MaxSubdirsPerDir: 3,
	}
	manyFilesDensityConfig = DensityConfig{
		MaxFilesPerDir:   1000,
		MaxSubdirsPerDir: 20,
	}
)

// OpConfig bundles all configuration for operations.
type OpConfig struct {
	Size    SizeConfig
	Density DensityConfig
}

// NewOpConfig creates an OpConfig from CLI flags.
func NewOpConfig(largeFiles, manyFiles bool) *OpConfig {
	cfg := &OpConfig{
		Size:    defaultSizeConfig,
		Density: defaultDensityConfig,
	}
	if largeFiles {
		cfg.Size = largeSizeConfig
	}
	if manyFiles {
		cfg.Density = manyFilesDensityConfig
	}
	return cfg
}

// Pools tracks available files and directories for operation targeting.
type Pools struct {
	Files []string // relative paths of existing files
	Dirs  []string // relative paths of existing directories (empty string = root)
}

// NewPools creates pools with just the root directory.
func NewPools() *Pools {
	return &Pools{
		Files: nil,
		Dirs:  []string{""}, // root
	}
}

// AddFile adds a file to the pool.
func (p *Pools) AddFile(relPath string) {
	p.Files = append(p.Files, relPath)
}

// RemoveFile removes a file from the pool.
func (p *Pools) RemoveFile(relPath string) {
	for i, f := range p.Files {
		if f == relPath {
			p.Files = append(p.Files[:i], p.Files[i+1:]...)
			return
		}
	}
}

// AddDir adds a directory to the pool.
func (p *Pools) AddDir(relPath string) {
	p.Dirs = append(p.Dirs, relPath)
}

// RemoveDir removes a directory and all files/subdirs under it from pools.
func (p *Pools) RemoveDir(relPath string) {
	prefix := relPath + "/"

	// Remove the dir itself
	for i, d := range p.Dirs {
		if d == relPath {
			p.Dirs = append(p.Dirs[:i], p.Dirs[i+1:]...)
			break
		}
	}

	// Remove subdirs
	filtered := p.Dirs[:0]
	for _, d := range p.Dirs {
		if !strings.HasPrefix(d, prefix) {
			filtered = append(filtered, d)
		}
	}
	p.Dirs = filtered

	// Remove files
	filteredFiles := p.Files[:0]
	for _, f := range p.Files {
		if !strings.HasPrefix(f, prefix) && filepath.Dir(f) != relPath {
			filteredFiles = append(filteredFiles, f)
		}
	}
	// Also remove files directly in the dir
	var finalFiles []string
	for _, f := range filteredFiles {
		dir := filepath.Dir(f)
		if dir == "." {
			dir = ""
		}
		if dir == relPath {
			continue
		}
		finalFiles = append(finalFiles, f)
	}
	p.Files = finalFiles
}

// RenameFile renames a file in the pool.
func (p *Pools) RenameFile(oldPath, newPath string) {
	for i, f := range p.Files {
		if f == oldPath {
			p.Files[i] = newPath
			return
		}
	}
}

// RenameDir renames a directory and updates all nested paths.
func (p *Pools) RenameDir(oldPath, newPath string) {
	oldPrefix := oldPath + "/"
	newPrefix := newPath + "/"

	for i, d := range p.Dirs {
		if d == oldPath {
			p.Dirs[i] = newPath
		} else if strings.HasPrefix(d, oldPrefix) {
			p.Dirs[i] = newPrefix + strings.TrimPrefix(d, oldPrefix)
		}
	}

	for i, f := range p.Files {
		if strings.HasPrefix(f, oldPrefix) {
			p.Files[i] = newPrefix + strings.TrimPrefix(f, oldPrefix)
		}
	}
}

// NonRootDirs returns directories excluding the root.
func (p *Pools) NonRootDirs() []string {
	var result []string
	for _, d := range p.Dirs {
		if d != "" {
			result = append(result, d)
		}
	}
	return result
}

// generateFileSize returns a random file size using log-normal distribution.
func generateFileSize(rng *rand.Rand, cfg SizeConfig) int {
	logSize := cfg.MeanLog + cfg.StdDevLog*rng.NormFloat64()
	size := int(math.Exp(logSize))
	if size < 64 {
		size = 64
	}
	if size > cfg.MaxBytes {
		size = cfg.MaxBytes
	}
	return size
}

// generateContent creates deterministic markdown content of approximately targetSize bytes.
// Content always ends at a newline boundary to avoid truncation artifacts (null bytes
// from mid-string slicing that PostgreSQL TEXT columns can't store).
func generateContent(rng *rand.Rand, title string, targetSize int) string {
	var buf strings.Builder
	buf.Grow(targetSize + 200)
	fmt.Fprintf(&buf, "---\ntitle: %s\n---\n\n", title)
	lineNum := 0
	for buf.Len() < targetSize {
		nWords := 5 + rng.Intn(15)
		fmt.Fprintf(&buf, "Line %d: %s\n", lineNum, randomWords(rng, nWords))
		lineNum++
	}
	// Don't truncate mid-line -- return at the last complete line.
	// Content may slightly exceed targetSize (by up to one line ~200 chars).
	return buf.String()
}

// Word pool for content generation (deterministic, no external dependencies).
var wordPool = []string{
	"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
	"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
	"data", "query", "index", "table", "schema", "column", "row", "view",
	"create", "update", "delete", "insert", "select", "filter", "order",
	"first", "last", "sample", "export", "import", "mount", "build",
	"file", "directory", "path", "name", "hash", "content", "version",
	"undo", "redo", "save", "restore", "checkpoint", "rollback", "commit",
}

// randomWords returns n random words from the word pool.
func randomWords(rng *rand.Rand, n int) string {
	words := make([]string, n)
	for i := range words {
		words[i] = wordPool[rng.Intn(len(wordPool))]
	}
	return strings.Join(words, " ")
}

// randomName generates a random filename-safe name.
func randomName(rng *rand.Rand) string {
	prefixes := []string{
		"doc", "note", "memo", "report", "guide", "spec",
		"draft", "plan", "log", "ref", "brief", "summary",
	}
	prefix := prefixes[rng.Intn(len(prefixes))]
	suffix := rng.Intn(10000)
	return fmt.Sprintf("%s-%04d", prefix, suffix)
}

// randomDirName generates a random directory name.
func randomDirName(rng *rand.Rand) string {
	names := []string{
		"docs", "notes", "drafts", "specs", "guides", "refs",
		"archive", "inbox", "review", "staging", "projects", "topics",
	}
	name := names[rng.Intn(len(names))]
	suffix := rng.Intn(1000)
	return fmt.Sprintf("%s-%03d", name, suffix)
}

// readBackHash reads a file back from TigerFS and returns the hash of
// what TigerFS returns. This is necessary because TigerFS synth views
// re-synthesize content (parse frontmatter into columns, reconstruct
// on read), so the returned content may differ from what was written.
//
// Uses explicit open/close to ensure the NFS client fetches fresh data
// from the server rather than serving from the write cache.
func readBackHash(fullPath string) (string, error) {
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", fmt.Errorf("read back %s: %w", fullPath, err)
	}
	return HashContent(data), nil
}

// --- Filesystem Operations ---
// Each operation takes the workspace path, rng, pools, state, and config.
// It performs the real filesystem operation and updates the expected state.
// Returns a description string for logging, or an error.

// OpCreateFile creates a new markdown file in a random directory.
// Returns the description, the number of bytes written, and any error.
func OpCreateFile(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, cfg *OpConfig) (string, int, error) {
	// Pick a directory with capacity
	dir := pickDirWithCapacity(rng, pools, state, cfg.Density.MaxFilesPerDir)
	name := randomName(rng) + ".md"
	relPath := name
	if dir != "" {
		relPath = dir + "/" + name
	}

	size := generateFileSize(rng, cfg.Size)
	content := generateContent(rng, name, size)
	written := len(content)

	fullPath := filepath.Join(wsPath, relPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return "", 0, fmt.Errorf("mkdir for %s: %w", relPath, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return "", 0, fmt.Errorf("write %s: %w", relPath, err)
	}

	// Read back from TigerFS to get the synthesized content hash
	// (TigerFS re-synthesizes markdown from structured columns)
	hash, err := readBackHash(fullPath)
	if err != nil {
		return "", 0, err
	}

	state.SetFile(relPath, hash)
	pools.AddFile(relPath)

	return fmt.Sprintf("create_file %s (%s)", relPath, formatSize(size)), written, nil
}

// OpEditFile modifies an existing file.
func OpEditFile(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, cfg *OpConfig) (string, error) {
	if len(pools.Files) == 0 {
		return "", fmt.Errorf("no files to edit")
	}

	idx := rng.Intn(len(pools.Files))
	relPath := pools.Files[idx]
	size := generateFileSize(rng, cfg.Size)
	content := generateContent(rng, filepath.Base(relPath), size)

	fullPath := filepath.Join(wsPath, relPath)
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write %s: %w", relPath, err)
	}

	// Read back synthesized content
	hash, err := readBackHash(fullPath)
	if err != nil {
		return "", err
	}

	state.SetFile(relPath, hash)

	return fmt.Sprintf("edit_file %s (%s)", relPath, formatSize(size)), nil
}

// OpRenameFile renames a file within the same directory.
func OpRenameFile(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, _ *OpConfig) (string, error) {
	if len(pools.Files) == 0 {
		return "", fmt.Errorf("no files to rename")
	}

	idx := rng.Intn(len(pools.Files))
	oldRelPath := pools.Files[idx]
	dir := filepath.Dir(oldRelPath)
	if dir == "." {
		dir = ""
	}

	newName := randomName(rng) + ".md"
	newRelPath := newName
	if dir != "" {
		newRelPath = dir + "/" + newName
	}

	oldFull := filepath.Join(wsPath, oldRelPath)
	newFull := filepath.Join(wsPath, newRelPath)
	if err := os.Rename(oldFull, newFull); err != nil {
		return "", fmt.Errorf("rename %s -> %s: %w", oldRelPath, newRelPath, err)
	}

	state.RenameFile(oldRelPath, newRelPath)
	pools.RenameFile(oldRelPath, newRelPath)

	return fmt.Sprintf("rename_file %s -> %s", oldRelPath, newRelPath), nil
}

// OpMoveFile moves a file to a different directory.
func OpMoveFile(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, _ *OpConfig) (string, error) {
	if len(pools.Files) == 0 || len(pools.Dirs) < 2 {
		return "", fmt.Errorf("need files and multiple dirs to move")
	}

	fileIdx := rng.Intn(len(pools.Files))
	oldRelPath := pools.Files[fileIdx]
	oldDir := filepath.Dir(oldRelPath)
	if oldDir == "." {
		oldDir = ""
	}

	// Enumerate dirs other than the file's current parent and pick one
	// uniformly. Random-with-retries can flake here: with only 2 dirs
	// (root + one), each attempt has a 50% chance of picking oldDir, so
	// 10 attempts fail ~0.1% of the time. canExecute already guarantees
	// len(pools.Dirs) >= 2, and oldDir is one of those entries, so the
	// candidate slice is always non-empty.
	candidates := make([]string, 0, len(pools.Dirs)-1)
	for _, d := range pools.Dirs {
		if d != oldDir {
			candidates = append(candidates, d)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no different directory available for move_file %s", oldRelPath)
	}
	newDir := candidates[rng.Intn(len(candidates))]

	baseName := filepath.Base(oldRelPath)
	newRelPath := baseName
	if newDir != "" {
		newRelPath = newDir + "/" + baseName
	}

	oldFull := filepath.Join(wsPath, oldRelPath)
	newFull := filepath.Join(wsPath, newRelPath)
	if err := os.MkdirAll(filepath.Dir(newFull), 0755); err != nil {
		return "", fmt.Errorf("mkdir for move: %w", err)
	}
	if err := os.Rename(oldFull, newFull); err != nil {
		return "", fmt.Errorf("move %s -> %s: %w", oldRelPath, newRelPath, err)
	}

	state.RenameFile(oldRelPath, newRelPath)
	pools.RenameFile(oldRelPath, newRelPath)

	return fmt.Sprintf("move_file %s -> %s", oldRelPath, newRelPath), nil
}

// OpDeleteFile deletes an existing file.
func OpDeleteFile(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, _ *OpConfig) (string, error) {
	if len(pools.Files) == 0 {
		return "", fmt.Errorf("no files to delete")
	}

	idx := rng.Intn(len(pools.Files))
	relPath := pools.Files[idx]

	fullPath := filepath.Join(wsPath, relPath)
	if err := os.Remove(fullPath); err != nil {
		return "", fmt.Errorf("delete %s: %w", relPath, err)
	}

	state.RemoveFile(relPath)
	pools.RemoveFile(relPath)

	return fmt.Sprintf("delete_file %s", relPath), nil
}

// OpCreateDir creates a new subdirectory.
func OpCreateDir(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, cfg *OpConfig) (string, error) {
	// Pick a parent directory with subdir capacity
	parent := pickDirWithSubdirCapacity(rng, pools, state, cfg.Density.MaxSubdirsPerDir)
	name := randomDirName(rng)
	relPath := name
	if parent != "" {
		relPath = parent + "/" + name
	}

	fullPath := filepath.Join(wsPath, relPath)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", relPath, err)
	}

	state.AddDir(relPath)
	pools.AddDir(relPath)

	return fmt.Sprintf("create_dir %s", relPath), nil
}

// OpRenameDir renames a non-root directory.
func OpRenameDir(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, _ *OpConfig) (string, error) {
	nonRoot := pools.NonRootDirs()
	if len(nonRoot) == 0 {
		return "", fmt.Errorf("no non-root dirs to rename")
	}

	idx := rng.Intn(len(nonRoot))
	oldRelPath := nonRoot[idx]
	parent := filepath.Dir(oldRelPath)
	if parent == "." {
		parent = ""
	}
	oldName := filepath.Base(oldRelPath)

	// randomDirName has 12 prefixes * 1000 suffixes = 12k options; collisions
	// with the existing name are rare but possible. Re-roll up to a few
	// times to avoid `os.Rename(A, A)` which fails with EEXIST.
	var newName string
	for attempt := 0; attempt < 5; attempt++ {
		newName = randomDirName(rng)
		if newName != oldName {
			break
		}
	}
	if newName == oldName {
		return "", fmt.Errorf("rename_dir: could not generate a different name for %s after retries", oldRelPath)
	}

	newRelPath := newName
	if parent != "" {
		newRelPath = parent + "/" + newName
	}

	oldFull := filepath.Join(wsPath, oldRelPath)
	newFull := filepath.Join(wsPath, newRelPath)
	if err := os.Rename(oldFull, newFull); err != nil {
		return "", fmt.Errorf("rename dir %s -> %s: %w", oldRelPath, newRelPath, err)
	}

	state.RenameDir(oldRelPath, newRelPath)
	pools.RenameDir(oldRelPath, newRelPath)

	return fmt.Sprintf("rename_dir %s -> %s", oldRelPath, newRelPath), nil
}

// OpMoveDir moves a non-root directory (which may contain files/subdirs)
// into a different parent directory. Sources are biased toward dirs with
// contents so the recursive case is regularly exercised. The destination
// must not be the source's current parent, the source itself, or a
// descendant of the source. If the biased source has no valid destination,
// other non-root sources are tried in random order before giving up.
func OpMoveDir(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, _ *OpConfig) (string, error) {
	srcs := orderedMoveDirSources(rng, pools, state)
	if len(srcs) == 0 {
		return "", fmt.Errorf("no non-root dirs to move")
	}

	var src, newRelPath string
	for _, candidate := range srcs {
		dests := validMoveDirDests(candidate, pools, state)
		if len(dests) == 0 {
			continue
		}
		src = candidate
		dest := dests[rng.Intn(len(dests))]
		baseName := filepath.Base(src)
		newRelPath = baseName
		if dest != "" {
			newRelPath = dest + "/" + baseName
		}
		break
	}
	if newRelPath == "" {
		return "", fmt.Errorf("no valid (src, dest) pair for move_dir")
	}

	fileCount, subdirCount := countDirContents(src, state)

	oldFull := filepath.Join(wsPath, src)
	newFull := filepath.Join(wsPath, newRelPath)
	if err := os.MkdirAll(filepath.Dir(newFull), 0755); err != nil {
		return "", fmt.Errorf("mkdir for move_dir: %w", err)
	}
	if err := os.Rename(oldFull, newFull); err != nil {
		return "", fmt.Errorf("move_dir %s -> %s: %w", src, newRelPath, err)
	}

	state.RenameDir(src, newRelPath)
	pools.RenameDir(src, newRelPath)

	return fmt.Sprintf("move_dir %s -> %s (%d files, %d subdirs)", src, newRelPath, fileCount, subdirCount), nil
}

// OpDeleteDir recursively deletes a non-root directory and everything inside.
//
// TigerFS records one log entry per delete (parent_id_fkey forces leaves to
// go first), so a delete_dir produces N entries in total. Each one is an
// independent undo target -- ExecuteUndoSingle restores exactly one row,
// not "the whole delete_dir." If we push a single stack entry covering the
// whole op, undo_single's pop returns "state before delete_dir" while
// TigerFS only restores one row, and validation fails with state mismatches
// (cannot recover, since deferred-FK requires the parent dir to exist by
// COMMIT time -- restoring just a child of a deleted dir would orphan it).
//
// Instead, walk the tree explicitly and push one stack entry per deletion.
// The first deletion uses the entry the runner already pushed before this
// op; each subsequent deletion gets a fresh entry capturing the state right
// before that particular row was removed. undo_single can then target any
// individual deletion safely.
func OpDeleteDir(wsPath string, rng *rand.Rand, pools *Pools, state *WorkspaceState, _ *OpConfig, stack *StateStack, iteration int, stats *Stats) (string, error) {
	src := pickNonRootDirBiased(rng, pools, state)
	if src == "" {
		return "", fmt.Errorf("no non-root dirs to delete")
	}

	fileCount, subdirCount := countDirContents(src, state)
	deletions, err := collectDeletionOrder(wsPath, src)
	if err != nil {
		return "", fmt.Errorf("walk %s for deletion: %w", src, err)
	}

	// lastSeenLogID enforces monotonicity across the per-row reads below.
	// Empirically the same staleness window that affects post-undo reads
	// also affects these per-row reads in tight succession (snapshot dump
	// at iter 471 caught three reads stale by 1.5s, with each subsequent
	// read advancing slightly but still trailing the actual newest).
	// On regression the helper returns the prior; we treat that as "do
	// not tag this entry" so a stale id never lands on a newer stack
	// entry (which would point undo_single at the wrong log row).
	var lastSeenLogID string

	for i, item := range deletions {
		// First deletion uses the runner-supplied stack entry; subsequent
		// deletions get a fresh entry with the in-progress state.
		if i > 0 {
			stack.Push(state, iteration)
		}

		fullPath := filepath.Join(wsPath, item.path)
		if err := os.Remove(fullPath); err != nil {
			return "", fmt.Errorf("delete %s: %w", item.path, err)
		}
		if item.isDir {
			state.RemoveDir(item.path)
		} else {
			state.RemoveFile(item.path)
		}

		// Pin the freshly-produced log_id on the entry that just
		// captured the pre-deletion state, but only if it advances past
		// the prior tagged id. If the read regresses (stale snapshot)
		// or stays equal (helper kept prior on retry exhaustion), we
		// leave this entry unlogged so undo_single will skip it -- it's
		// safer to lose targetability for one deletion than to misroute
		// undo_single onto a completely unrelated log row.
		logID := readLatestLogIDMonotonic(wsPath, lastSeenLogID, iteration,
			fmt.Sprintf("delete_dir per-row %s", item.path), stats)
		if logID != "" && logID > lastSeenLogID {
			stack.SetLastLogID(logID)
			lastSeenLogID = logID
		}
	}

	pools.RemoveDir(src)

	return fmt.Sprintf("delete_dir %s (%d files, %d subdirs)", src, fileCount, subdirCount), nil
}

// deletionItem is one entry in collectDeletionOrder's output.
type deletionItem struct {
	path  string
	isDir bool
}

// collectDeletionOrder walks the *actual* filesystem under src and returns
// every entry in post-order (children before their parent), with src itself
// last. Hidden entries (those starting with ".") are skipped -- TigerFS
// virtual paths such as .log/ aren't real children to remove.
//
// We deliberately walk the FS rather than reading WorkspaceState. After the
// mkdirSynth-logging fix, the test's tracked state should stay in sync with
// TigerFS across undos, so a state-based traversal would also work in the
// common case. Walking the FS is kept as defensive coding: any future state
// drift (a new unlogged op, a tracking bug in move_dir/rename_dir, etc.)
// would otherwise surface as cryptic ENOTEMPTY/EIO failures from os.Remove
// hitting unexpected children. The cost is a few NFS-mediated readdirs per
// delete_dir, which is negligible next to the N+1 deletes the op already
// makes.
func collectDeletionOrder(wsPath, src string) ([]deletionItem, error) {
	var out []deletionItem
	var walk func(rel string) error
	walk = func(rel string) error {
		entries, err := os.ReadDir(filepath.Join(wsPath, rel))
		if err != nil {
			return err
		}
		// Sort for determinism within a directory.
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, ".") {
				continue
			}
			childRel := filepath.Join(rel, name)
			if e.IsDir() {
				if err := walk(childRel); err != nil {
					return err
				}
				out = append(out, deletionItem{path: childRel, isDir: true})
			} else {
				out = append(out, deletionItem{path: childRel, isDir: false})
			}
		}
		return nil
	}

	if err := walk(src); err != nil {
		return nil, err
	}
	out = append(out, deletionItem{path: src, isDir: true})
	return out, nil
}

// OpCreateSavepoint creates a named savepoint.
func OpCreateSavepoint(wsPath string, rng *rand.Rand, _ *Pools, _ *WorkspaceState, _ *OpConfig, iteration int, stack *StateStack) (string, error) {
	name := fmt.Sprintf("sp-%d-%04d", iteration, rng.Intn(10000))
	spPath := filepath.Join(wsPath, ".savepoint", name+".json")
	content := fmt.Sprintf(`{"description":"Savepoint at iteration %d"}`, iteration)

	if err := os.WriteFile(spPath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("create savepoint %s: %w", name, err)
	}

	stack.SaveSavepoint(name)

	return fmt.Sprintf("create_savepoint %s", name), nil
}

// RebuildPools reconstructs pools from the workspace state.
// Used after undo operations to sync pools with the restored state.
func RebuildPools(state *WorkspaceState) *Pools {
	pools := &Pools{
		Dirs: []string{""}, // root always exists
	}
	for relPath := range state.Files {
		pools.Files = append(pools.Files, relPath)
	}
	for relPath := range state.Dirs {
		pools.Dirs = append(pools.Dirs, relPath)
	}
	return pools
}

// --- Helpers ---

func pickDirWithCapacity(rng *rand.Rand, pools *Pools, state *WorkspaceState, maxFiles int) string {
	// Try random directories, find one with capacity
	for attempts := 0; attempts < 20; attempts++ {
		dir := pools.Dirs[rng.Intn(len(pools.Dirs))]
		if state.FileCount(dir) < maxFiles {
			return dir
		}
	}
	// Fallback: root always works (or first dir with space)
	return pools.Dirs[0]
}

func pickDirWithSubdirCapacity(rng *rand.Rand, pools *Pools, state *WorkspaceState, maxSubdirs int) string {
	for attempts := 0; attempts < 20; attempts++ {
		dir := pools.Dirs[rng.Intn(len(pools.Dirs))]
		if state.SubdirCount(dir) < maxSubdirs {
			return dir
		}
	}
	return pools.Dirs[0]
}

// orderedMoveDirSources returns non-root dirs in the order the picker should
// try them for move_dir: the biased preferred candidate first (prefers dirs
// with nested contents), then the remaining non-root dirs in random order.
// This lets OpMoveDir keep the non-empty bias while still finding a valid
// (src, dest) pair whenever one exists.
func orderedMoveDirSources(rng *rand.Rand, pools *Pools, state *WorkspaceState) []string {
	first := pickNonRootDirBiased(rng, pools, state)
	if first == "" {
		return nil
	}
	ordered := []string{first}
	rest := make([]string, 0, len(pools.NonRootDirs())-1)
	for _, d := range pools.NonRootDirs() {
		if d != first {
			rest = append(rest, d)
		}
	}
	rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
	return append(ordered, rest...)
}

// validMoveDirDests returns all pool directories that are legal destinations
// for moving src: not src's current parent, not src itself, not a descendant
// of src, and not already occupied by a dir of the same basename.
func validMoveDirDests(src string, pools *Pools, state *WorkspaceState) []string {
	oldParent := filepath.Dir(src)
	if oldParent == "." {
		oldParent = ""
	}
	baseName := filepath.Base(src)
	descendantPrefix := src + "/"

	var out []string
	for _, dest := range pools.Dirs {
		if dest == oldParent || dest == src {
			continue
		}
		if strings.HasPrefix(dest, descendantPrefix) {
			continue
		}
		proposed := baseName
		if dest != "" {
			proposed = dest + "/" + baseName
		}
		if _, exists := state.Dirs[proposed]; exists {
			continue
		}
		out = append(out, dest)
	}
	return out
}

// canMoveDir reports whether at least one valid (src, dest) pair exists.
// Used by canExecute so opMoveDir is only selected when feasible.
func canMoveDir(pools *Pools, state *WorkspaceState) bool {
	for _, src := range pools.NonRootDirs() {
		if len(validMoveDirDests(src, pools, state)) > 0 {
			return true
		}
	}
	return false
}

// pickNonRootDirBiased chooses a non-root directory, preferring ones that
// contain files or subdirectories so move_dir / delete_dir exercise recursive
// behavior. Empty dirs are still reachable as a secondary bucket. Returns ""
// if no non-root dirs exist.
func pickNonRootDirBiased(rng *rand.Rand, pools *Pools, state *WorkspaceState) string {
	nonRoot := pools.NonRootDirs()
	if len(nonRoot) == 0 {
		return ""
	}

	var withContent, empty []string
	for _, d := range nonRoot {
		if dirHasContents(d, state) {
			withContent = append(withContent, d)
		} else {
			empty = append(empty, d)
		}
	}

	// When both groups exist, prefer non-empty dirs 70% of the time.
	if len(withContent) > 0 && len(empty) > 0 {
		if rng.Intn(10) < 7 {
			return withContent[rng.Intn(len(withContent))]
		}
		return empty[rng.Intn(len(empty))]
	}
	if len(withContent) > 0 {
		return withContent[rng.Intn(len(withContent))]
	}
	return empty[rng.Intn(len(empty))]
}

// dirHasContents reports whether the given directory has any nested files
// or subdirectories in the expected state.
func dirHasContents(dir string, state *WorkspaceState) bool {
	prefix := dir + "/"
	for f := range state.Files {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	for d := range state.Dirs {
		if strings.HasPrefix(d, prefix) {
			return true
		}
	}
	return false
}

// countDirContents returns the number of files and subdirectories nested
// anywhere beneath dir (recursive).
func countDirContents(dir string, state *WorkspaceState) (files, subdirs int) {
	prefix := dir + "/"
	for f := range state.Files {
		if strings.HasPrefix(f, prefix) {
			files++
		}
	}
	for d := range state.Dirs {
		if strings.HasPrefix(d, prefix) {
			subdirs++
		}
	}
	return
}

func formatSize(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(bytes)/1024)
	}
	return fmt.Sprintf("%.1fMB", float64(bytes)/(1024*1024))
}
