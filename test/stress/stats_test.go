package main

import "testing"

func TestStats_RecordOp(t *testing.T) {
	s := NewStats()
	s.RecordOp("create_file")
	s.RecordOp("create_file")
	s.RecordOp("edit_file")
	if s.ops["create_file"] != 2 {
		t.Errorf("create_file count: got %d, want 2", s.ops["create_file"])
	}
	if s.ops["edit_file"] != 1 {
		t.Errorf("edit_file count: got %d, want 1", s.ops["edit_file"])
	}
	if s.ops["rename_file"] != 0 {
		t.Errorf("rename_file count: got %d, want 0", s.ops["rename_file"])
	}
}

func TestStats_RecordCreatedFileSize(t *testing.T) {
	s := NewStats()
	s.RecordCreatedFileSize(100)
	s.RecordCreatedFileSize(200)
	s.RecordCreatedFileSize(300)
	if len(s.createSizes) != 3 {
		t.Fatalf("createSizes length: got %d, want 3", len(s.createSizes))
	}
	if s.createSizes[0] != 100 || s.createSizes[1] != 200 || s.createSizes[2] != 300 {
		t.Errorf("createSizes: got %v, want [100 200 300]", s.createSizes)
	}
}

func TestPercentile(t *testing.T) {
	sorted := []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	cases := []struct {
		p, want int
	}{
		{10, 10},
		{50, 50},
		{90, 90},
		{99, 100},
		{100, 100},
	}
	for _, c := range cases {
		if got := percentile(sorted, c.p); got != c.want {
			t.Errorf("p%d: got %d, want %d", c.p, got, c.want)
		}
	}
}

func TestPercentile_Empty(t *testing.T) {
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("empty p50: got %d, want 0", got)
	}
}

func TestPercentile_Single(t *testing.T) {
	sorted := []int{42}
	for _, p := range []int{1, 50, 99, 100} {
		if got := percentile(sorted, p); got != 42 {
			t.Errorf("single p%d: got %d, want 42", p, got)
		}
	}
}
