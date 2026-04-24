package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Stats tracks operation counts and created-file sizes over a stress-test run.
// Printed after final validation to give a summary of what the run exercised.
type Stats struct {
	ops         map[string]int
	createSizes []int
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

// Print writes operation counts/percentages and a file-size histogram to stdout.
func (s *Stats) Print() {
	total := 0
	for _, n := range s.ops {
		total += n
	}

	fmt.Println()
	fmt.Println("=== Operation Statistics ===")
	fmt.Println()

	notImpl := map[string]string{
		"move_dir":   "(not implemented in test)",
		"delete_dir": "(not implemented in test)",
	}

	s.printGroup("File Operations:", []string{
		"create_file", "edit_file", "rename_file", "move_file", "delete_file",
	}, nil, total)
	fmt.Println()
	s.printGroup("Directory Operations:", []string{
		"create_dir", "rename_dir", "move_dir", "delete_dir",
	}, notImpl, total)
	fmt.Println()
	s.printGroup("Other Operations:", []string{
		"create_savepoint", "undo_single", "undo_to_id", "undo_to_savepoint",
	}, nil, total)
	fmt.Println()
	fmt.Printf("  %-20s %5d   %5.1f%%\n", "Total:", total, 100.0)

	s.printHistogram()
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
