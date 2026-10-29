package raftstore

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func entries(term uint64, from, to uint64) []raftpb.Entry {
	var es []raftpb.Entry
	for i := from; i <= to; i++ {
		es = append(es, raftpb.Entry{Term: term, Index: i, Data: []byte{byte(i)}})
	}
	return es
}

func indexes(es []raftpb.Entry) []uint64 {
	var out []uint64
	for _, e := range es {
		out = append(out, e.Index)
	}
	return out
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs := raftpb.HardState{Term: 2, Vote: 1, Commit: 3}
	if err := p.Save(hs, entries(1, 1, 5), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	// A new leader overwrites a conflicting tail: entry 4 in term 2 replaces
	// 4 and 5 from term 1.
	if err := p.Save(raftpb.HardState{}, entries(2, 4, 4), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p, err = Open(dir) // reopen, as after a restart
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	gotHS, snap, ents, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if gotHS != hs {
		t.Errorf("hard state = %+v, want %+v", gotHS, hs)
	}
	if snap.Metadata.Index != 0 {
		t.Errorf("unexpected snapshot at index %d", snap.Metadata.Index)
	}
	if got := indexes(ents); len(got) != 4 || got[3] != 4 || ents[3].Term != 2 {
		t.Errorf("entries = %v (last term %d), want 1..4 with entry 4 in term 2", got, ents[len(ents)-1].Term)
	}
}

func TestSnapshotDropsCoveredEntries(t *testing.T) {
	p, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 6}, entries(1, 1, 6), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	snap := raftpb.Snapshot{
		Data:     []byte("state"),
		Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}},
	}
	if err := p.Save(raftpb.HardState{}, nil, snap); err != nil {
		t.Fatal(err)
	}
	_, gotSnap, ents, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if gotSnap.Metadata.Index != 4 || string(gotSnap.Data) != "state" {
		t.Errorf("snapshot = index %d data %q", gotSnap.Metadata.Index, gotSnap.Data)
	}
	if got := indexes(ents); len(got) != 2 || got[0] != 5 || got[1] != 6 {
		t.Errorf("entries after snapshot = %v, want [5 6]", got)
	}
}

func TestSaveNothingIsNoop(t *testing.T) {
	p, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Save(raftpb.HardState{}, nil, raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	hs, _, ents, err := p.Load()
	if err != nil || hs.Term != 0 || len(ents) != 0 {
		t.Fatalf("fresh store loaded hs=%+v ents=%d err=%v", hs, len(ents), err)
	}
}
