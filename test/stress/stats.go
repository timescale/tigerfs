package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// MonotonicWarning records a single readLatestLogIDMonotonic regression
// event so the runner can emit a per-run summary at the end. The
// warnings stream individually to stderr as they happen; the summary is
// the at-a-glance view of "is the NFS-layer staleness behavior stable?"
type MonotonicWarning struct {
	Iteration int
	OpDesc    string        // op that just ran when the regression was detected
	Retries   int           // number of retries until recovery (or hit the cap)
	Recovered bool          // false = retry budget exhausted, prior was kept
	Elapsed   time.Duration // wall-clock time spent in retries (Retries * monotonicRetryDelay)
}

// Stats tracks operation counts, created-file sizes, and monotonicity
// warnings over a stress-test run. Printed after final validation to
// give a summary of what the run exercised.
type Stats struct {
	ops         map[string]int
	createSizes []int
	warnings    []MonotonicWarning
}

// NewStats returns an empty Stats.
func NewStats() *Stats {
	return &Stats{ops: make(map[string]int)}
}

// RecordOp increments the counter for the given operation name (see opName).
func (s *Stats) RecordOp(name string) {
	s.ops[name]++
}

// RecordCreatedFileSize records the size in bytes of a newly created file.
// Only called for create_file; edits and other writes are not tracked.
func (s *Stats) RecordCreatedFileSize(bytes int) {
	s.createSizes = append(s.createSizes, bytes)
}

// RecordMonotonicWarning appends a regression event for end-of-run
// summary. Called from readLatestLogIDMonotonic each time a regressed
// read is observed (whether or not it later recovers).
func (s *Stats) RecordMonotonicWarning(w MonotonicWarning) {
	s.warnings = append(s.warnings, w)
}

// Print writes operation counts/percentages and a file-size histogram to stdout.
func (s *Stats) Print() {
	total := 0
	for _, n := range s.ops {
		total += n
	}

	fmt.Println()
	fmt.Println("=== Operation Statistics ===")
	fmt.Println()

	s.printGroup("File Operations:", []string{
		"create_file", "edit_file", "rename_file", "move_file", "delete_file",
	}, nil, total)
	fmt.Println()
	s.printGroup("Directory Operations:", []string{
		"create_dir", "rename_dir", "move_dir", "delete_dir",
	}, nil, total)
	fmt.Println()
	s.printGroup("Other Operations:", []string{
		"create_savepoint", "undo_single", "undo_to_id", "undo_to_savepoint",
	}, nil, total)
	fmt.Println()
	fmt.Printf("  %-20s %5d   %5.1f%%\n", "Total:", total, 100.0)

	s.printHistogram()
	s.printMonotonicWarnings(total)
}

// printMonotonicWarnings renders the per-run monotonicity-warning
// summary -- count, recovery rate, retry distribution, and which op
// categories triggered the regressions. Surfaces whether NFS-layer
// staleness is stable across runs without requiring the reader to
// scroll through the trace looking for `[warn iter ...]` lines.
//
// The "total" param is the total op count from Print(), used to
// compute the regression rate as a percentage of all ops.
func (s *Stats) printMonotonicWarnings(total int) {
	fmt.Println()
	fmt.Println("=== Monotonicity Warnings ===")
	if len(s.warnings) == 0 {
		fmt.Println("(none -- no readLatestLogID regressions observed)")
		return
	}

	recovered := 0
	for _, w := range s.warnings {
		if w.Recovered {
			recovered++
		}
	}
	rate := 0.0
	if total > 0 {
		rate = 100.0 * float64(len(s.warnings)) / float64(total)
	}
	fmt.Printf("Total:     %d regressions across %d ops (%.2f%%)\n",
		len(s.warnings), total, rate)
	fmt.Printf("Recovered: %d / %d (%d kept prior lastLogID after retry exhaustion)\n",
		recovered, len(s.warnings), len(s.warnings)-recovered)

	// Recovery-time distribution: bucket by retry count so the reader
	// can spot whether most recoveries are quick (1-3 retries) or
	// approaching the cap (a sign the budget needs to grow).
	buckets := map[string]int{}
	for _, w := range s.warnings {
		switch {
		case !w.Recovered:
			buckets["stuck (>cap)"]++
		case w.Retries <= 3:
			buckets["fast (≤3 retries, ~300ms)"]++
		case w.Retries <= 10:
			buckets["medium (4-10 retries, ~1s)"]++
		default:
			buckets["slow (>10 retries, >1s)"]++
		}
	}
	for _, label := range []string{
		"fast (≤3 retries, ~300ms)",
		"medium (4-10 retries, ~1s)",
		"slow (>10 retries, >1s)",
		"stuck (>cap)",
	} {
		if c := buckets[label]; c > 0 {
			fmt.Printf("  %-30s %d\n", label, c)
		}
	}

	// Which op kind preceded each regression -- helps spot whether the
	// staleness only follows particular op types (e.g., undo_to_savepoint
	// was the original suspect; deletion-loop reads also contribute).
	opKindCounts := map[string]int{}
	for _, w := range s.warnings {
		// w.OpDesc is the full description like "undo_to_savepoint sp-..."
		// or "create_file foo.md (1KB)"; first token is the op kind.
		kind := w.OpDesc
		if idx := strings.IndexByte(kind, ' '); idx > 0 {
			kind = kind[:idx]
		}
		opKindCounts[kind]++
	}
	if len(opKindCounts) > 0 {
		fmt.Println("Op kinds preceding regressions:")
		var kinds []string
		for k := range opKindCounts {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			fmt.Printf("  %-22s %d\n", k, opKindCounts[k])
		}
	}
}

func (s *Stats) printGroup(title string, names []string, notes map[string]string, total int) {
	fmt.Println(title)
	for _, name := range names {
		count := s.ops[name]
		pct := 0.0
		if total > 0 {
			pct = 100.0 * float64(count) / float64(total)
		}
		note := ""
		if n, ok := notes[name]; ok {
			note = "  " + n
		}
		fmt.Printf("  %-20s %5d   %5.1f%%%s\n", name, count, pct, note)
	}
}

func (s *Stats) printHistogram() {
	n := len(s.createSizes)
	fmt.Println()
	fmt.Println("=== File Size Histogram ===")
	fmt.Printf("Files created: %d\n\n", n)
	if n == 0 {
		return
	}

	type bucket struct {
		lo, hi int64
		label  string
		count  int
	}
	buckets := []bucket{
		{0, 1024, "   64 B  -   1 KB", 0},
		{1024, 4 * 1024, "    1 KB -   4 KB", 0},
		{4 * 1024, 16 * 1024, "    4 KB -  16 KB", 0},
		{16 * 1024, 64 * 1024, "   16 KB -  64 KB", 0},
		{64 * 1024, 256 * 1024, "   64 KB - 256 KB", 0},
		{256 * 1024, 1024 * 1024, "  256 KB -   1 MB", 0},
		{1024 * 1024, 10 * 1024 * 1024, "    1 MB -  10 MB", 0},
		{10 * 1024 * 1024, 100 * 1024 * 1024, "   10 MB - 100 MB", 0},
	}

	for _, sz := range s.createSizes {
		v := int64(sz)
		for i := range buckets {
			if v >= buckets[i].lo && v < buckets[i].hi {
				buckets[i].count++
				break
			}
		}
	}

	last := -1
	maxCount := 0
	for i, b := range buckets {
		if b.count > 0 {
			last = i
			if b.count > maxCount {
				maxCount = b.count
			}
		}
	}
	if last < 0 {
		return
	}

	const maxBar = 30
	fmt.Println("Range                 Count   Distribution")
	for i := 0; i <= last; i++ {
		barLen := 0
		if maxCount > 0 {
			barLen = (buckets[i].count * maxBar) / maxCount
		}
		bar := strings.Repeat("█", barLen)
		fmt.Printf("  %-17s %5d   %s\n", buckets[i].label, buckets[i].count, bar)
	}

	sorted := make([]int, n)
	copy(sorted, s.createSizes)
	sort.Ints(sorted)
	var sum int64
	for _, v := range sorted {
		sum += int64(v)
	}
	mean := int(float64(sum) / float64(n))

	fmt.Println()
	fmt.Println("Size Statistics:")
	fmt.Printf("  Count:  %d\n", n)
	fmt.Printf("  Min:    %s\n", formatSize(sorted[0]))
	fmt.Printf("  Max:    %s\n", formatSize(sorted[n-1]))
	fmt.Printf("  Mean:   %s\n", formatSize(mean))
	fmt.Printf("  P50:    %s\n", formatSize(percentile(sorted, 50)))
	fmt.Printf("  P90:    %s\n", formatSize(percentile(sorted, 90)))
	fmt.Printf("  P99:    %s\n", formatSize(percentile(sorted, 99)))
}

// percentile returns the p-th percentile (nearest-rank) of a sorted slice.
func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(p)/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
