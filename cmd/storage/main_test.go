package main

import (
	"os"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/seed"
)

func TestParsePeers(t *testing.T) {
	self, ids, addrs, err := parsePeers("shard2-node3", "shard2-node1:7000,shard2-node2:7000,shard2-node3:7000")
	if err != nil {
		t.Fatal(err)
	}
	if self != 3 || len(ids) != 3 || addrs[1] != "shard2-node1:7000" {
		t.Fatalf("self=%d ids=%v addrs=%v", self, ids, addrs)
	}
	if _, _, _, err := parsePeers("shard9-node1", "shard2-node1:7000"); err == nil {
		t.Fatal("NODE_ID missing from PEERS was accepted")
	}
	if _, _, _, err := parsePeers("x", "no-port"); err == nil {
		t.Fatal("PEERS entry without a port was accepted")
	}
}

func TestSeedFileAdapter(t *testing.T) {
	dir := t.TempDir()
	f := seed.File{FormatVersion: seed.FormatVersion, ShardID: 2, ChunkSize: 2, Seats: []seed.Seat{
		{EventID: "evt-001", SectionID: "sec-001", SeatID: "R01-S001"},
		{EventID: "evt-001", SectionID: "sec-001", SeatID: "R01-S002"},
		{EventID: "evt-001", SectionID: "sec-001", SeatID: "R01-S003"},
	}}
	data, err := seed.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seed.Path(dir, 2), data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SEED_DIR", dir)
	src, err := seedLoader(2)()
	if err != nil {
		t.Fatal(err)
	}
	if src.TotalChunks() != 2 || len(src.Version()) != 64 {
		t.Fatalf("chunks=%d version=%q", src.TotalChunks(), src.Version())
	}
	if got := src.Chunk(1); len(got) != 1 || got[0].SeatId != "R01-S003" || got[0].EventId != "evt-001" {
		t.Fatalf("chunk 1 = %v", got)
	}
	if _, err := seedLoader(3)(); err == nil {
		t.Fatal("loading a missing shard file succeeded")
	}
}

func TestRaftTimingFromEnv(t *testing.T) {
	got, err := raftTimingFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got != (raftTiming{tick: 100 * time.Millisecond, election: 10, heartbeat: 1}) {
		t.Fatalf("defaults: %+v", got)
	}

	t.Setenv("RAFT_TICK_MS", "50")
	t.Setenv("RAFT_ELECTION_TICKS", "6")
	t.Setenv("RAFT_HEARTBEAT_TICKS", "2")
	got, err = raftTimingFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got != (raftTiming{tick: 50 * time.Millisecond, election: 6, heartbeat: 2}) {
		t.Fatalf("from env: %+v", got)
	}

	for _, bad := range []struct{ key, val string }{
		{"RAFT_TICK_MS", "0"},
		{"RAFT_TICK_MS", "fast"},
		{"RAFT_ELECTION_TICKS", "-3"},
		{"RAFT_ELECTION_TICKS", "2"}, // not greater than the heartbeat (2)
	} {
		t.Run(bad.key+"="+bad.val, func(t *testing.T) {
			t.Setenv(bad.key, bad.val)
			if _, err := raftTimingFromEnv(); err == nil {
				t.Fatalf("%s=%q was accepted", bad.key, bad.val)
			}
		})
	}
}
