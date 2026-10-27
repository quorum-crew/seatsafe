package seed

import (
	"path/filepath"
	"strings"
	"testing"
)

func sample(n int) File {
	f := File{FormatVersion: 1, ShardID: 2, ChunkSize: 3}
	for i := 0; i < n; i++ {
		f.Seats = append(f.Seats, Seat{"evt-1", "sec-101", string(rune('A' + i))})
	}
	return f
}

func TestRoundTripAndVersion(t *testing.T) {
	b, err := Marshal(sample(7))
	if err != nil {
		t.Fatal(err)
	}
	l, err := Parse(b, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Seats) != 7 || len(l.Version) != 64 {
		t.Fatalf("seats=%d version=%q", len(l.Seats), l.Version)
	}
	b2, _ := Marshal(sample(7))
	l2, _ := Parse(b2, 2)
	if l.Version != l2.Version {
		t.Fatal("same content must give the same version")
	}
	b3, _ := Marshal(sample(8))
	l3, _ := Parse(b3, 2)
	if l.Version == l3.Version {
		t.Fatal("different content must give a different version")
	}
}

func TestChunks(t *testing.T) {
	b, _ := Marshal(sample(7))
	l, _ := Parse(b, 2)
	if got := l.TotalChunks(); got != 3 {
		t.Fatalf("TotalChunks = %d, want 3", got)
	}
	sizes := []int{3, 3, 1}
	var all []Seat
	for i, want := range sizes {
		c := l.Chunk(i)
		if len(c) != want {
			t.Fatalf("chunk %d has %d seats, want %d", i, len(c), want)
		}
		all = append(all, c...)
	}
	if l.Chunk(3) != nil || l.Chunk(-1) != nil {
		t.Fatal("out-of-range chunk should be nil")
	}
	for i := range all {
		if all[i] != l.Seats[i] {
			t.Fatal("chunks must cover the seats in file order")
		}
	}
	empty, _ := Marshal(sample(0))
	le, _ := Parse(empty, 2)
	if le.TotalChunks() != 0 {
		t.Fatal("empty file has no chunks")
	}
}

func TestValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*File)
		shard  uint32
		want   string
	}{
		{"version", func(f *File) { f.FormatVersion = 2 }, 2, "format_version"},
		{"wrong shard", func(f *File) {}, 3, "is for shard 2"},
		{"chunk size", func(f *File) { f.ChunkSize = 0 }, 2, "chunk_size"},
		{"dup seat", func(f *File) { f.Seats = append(f.Seats, f.Seats[0]) }, 2, "twice"},
		{"empty field", func(f *File) { f.Seats[0].SeatID = "" }, 2, "empty"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := sample(3)
			c.mutate(&f)
			err := Validate(f, c.shard)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want containing %q", err, c.want)
			}
		})
	}
	if _, err := Parse([]byte(`{"format_version":1,"shard_id":2,"chunk_size":1,"seats":[],"extra":1}`), 2); err == nil {
		t.Fatal("unknown field should be rejected")
	}
}

func TestPathAndEnv(t *testing.T) {
	if got := Path("/seed", 3); got != filepath.Join("/seed", "shard3.json") {
		t.Fatalf("Path = %s", got)
	}
	t.Setenv("SEED_DIR", "")
	if DirFromEnv() != DefaultDir {
		t.Fatal("default dir")
	}
	t.Setenv("SEED_DIR", "/x")
	if DirFromEnv() != "/x" {
		t.Fatal("env dir")
	}
}
