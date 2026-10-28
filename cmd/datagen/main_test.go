package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

func defaults() Config {
	return Config{
		Shards: 3, NodesPerGroup: 3, Port: 7000,
		ShardHostFmt: "shard%d-node%d", CoordHostFmt: "coord-%d", ChunkSize: seed.DefaultChunkSize,
		FlashSections: 50, FlashRows: 20, FlashSeatsPerRow: 50,
		Events: 4, Sections: 12, Rows: 10, SeatsPerRow: 40,
	}
}

func TestDefaultDataset(t *testing.T) {
	o, err := Generate(defaults())
	if err != nil {
		t.Fatal(err)
	}
	if o.Stats.Seats != 50_000+4*4_800 {
		t.Fatalf("total seats = %d", o.Stats.Seats)
	}

	m, err := shardmap.Parse(o.ShardMap)
	if err != nil {
		t.Fatalf("router can't load generated shardmap: %v", err)
	}
	if got := m.EventShards("evt-flash"); len(got) != 3 {
		t.Fatalf("flash event should span all 3 shards, got %v", got)
	}
	// Starting shard rotates per event.
	for i, want := range []uint32{1, 2, 3, 1, 2} {
		ev := []string{"evt-flash", "evt-001", "evt-002", "evt-003", "evt-004"}[i]
		if got, _ := m.Lookup(ev, "sec-101"); got != want {
			t.Fatalf("%s/sec-101 on shard %d, want %d", ev, got, want)
		}
	}

	// Every seat appears in exactly one seed file, on the shard the map says.
	seen := map[seed.Seat]bool{}
	total := 0
	for id, b := range o.SeedFiles {
		l, err := seed.Parse(b, id)
		if err != nil {
			t.Fatalf("shard %d seed invalid: %v", id, err)
		}
		if o.Stats.Chunks[id] != l.TotalChunks() || l.TotalChunks() < 20 {
			t.Fatalf("shard %d chunks = %d", id, l.TotalChunks())
		}
		for _, s := range l.Seats {
			if seen[s] {
				t.Fatalf("seat %+v in two seed files", s)
			}
			seen[s] = true
			if owner, ok := m.Lookup(s.EventID, s.SectionID); !ok || owner != id {
				t.Fatalf("seat %+v in shard %d file but map says %d", s, id, owner)
			}
		}
		total += len(l.Seats)
	}
	if total != o.Stats.Seats {
		t.Fatalf("seed files hold %d seats, want %d", total, o.Stats.Seats)
	}
}

func TestDeterministic(t *testing.T) {
	a, _ := Generate(defaults())
	b, _ := Generate(defaults())
	if !bytes.Equal(a.ShardMap, b.ShardMap) {
		t.Fatal("shardmap differs between runs")
	}
	for id := range a.SeedFiles {
		if !bytes.Equal(a.SeedFiles[id], b.SeedFiles[id]) || a.Stats.Versions[id] != b.Stats.Versions[id] {
			t.Fatalf("shard %d seed differs between runs", id)
		}
	}
}

func TestWrite(t *testing.T) {
	c := defaults()
	c.FlashSections, c.Events = 3, 1
	o, err := Generate(c)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := Write(dir, o); err != nil {
		t.Fatal(err)
	}
	if _, err := shardmap.Load(filepath.Join(dir, "shardmap.json")); err != nil {
		t.Fatal(err)
	}
	for id := uint32(1); id <= 3; id++ {
		if _, err := seed.Load(seed.Path(filepath.Join(dir, "seed"), id), id); err != nil {
			t.Fatal(err)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*", "*.tmp")); len(left) > 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
	_ = os.RemoveAll(dir)
}

func TestRejectsBadConfig(t *testing.T) {
	c := defaults()
	c.Shards = 0
	if _, err := Generate(c); err == nil {
		t.Fatal("0 shards should fail")
	}
	c = defaults()
	c.FlashSections, c.Events = 0, 0
	if _, err := Generate(c); err == nil {
		t.Fatal("no events should fail")
	}
}
