package raftstore

import (
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
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
	if err := p.Save(hs, entries(1, 1, 5), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	// A new leader overwrites a conflicting tail: entry 4 in term 2 replaces
	// 4 and 5 from term 1.
	if err := p.Save(raftpb.HardState{}, entries(2, 4, 4), raftpb.Snapshot{}, true); err != nil {
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
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 6}, entries(1, 1, 6), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	snap := raftpb.Snapshot{
		Data:     []byte("state"),
		Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}},
	}
	if err := p.Save(raftpb.HardState{}, nil, snap, false); err != nil {
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
	if err := p.Save(raftpb.HardState{}, nil, raftpb.Snapshot{}, false); err != nil {
		t.Fatal(err)
	}
	hs, _, ents, err := p.Load()
	if err != nil || hs.Term != 0 || len(ents) != 0 {
		t.Fatalf("fresh store loaded hs=%+v ents=%d err=%v", hs, len(ents), err)
	}
}

// syncCountingFS counts fsyncs on every file Pebble creates.
type syncCountingFS struct {
	vfs.FS
	syncs *atomic.Int64
}

func (fs syncCountingFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	return syncCountingFile{f, fs.syncs}, err
}

func (fs syncCountingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	return syncCountingFile{f, fs.syncs}, err
}

type syncCountingFile struct {
	vfs.File
	syncs *atomic.Int64
}

func (f syncCountingFile) Sync() error     { f.syncs.Add(1); return f.File.Sync() }
func (f syncCountingFile) SyncData() error { f.syncs.Add(1); return f.File.SyncData() }
func (f syncCountingFile) SyncTo(n int64) (bool, error) {
	f.syncs.Add(1)
	return f.File.SyncTo(n)
}

// Only batches Raft needs durable are fsynced (#15): commit-index-only
// HardState updates go out without an fsync, but are still saved.
func TestSyncsOnlyWhenRequired(t *testing.T) {
	dir := t.TempDir()
	var syncs atomic.Int64
	p, err := open(dir, &pebble.Options{Logger: quietLogger{}, FS: syncCountingFS{vfs.Default, &syncs}})
	if err != nil {
		t.Fatal(err)
	}

	before := syncs.Load()
	if err := p.Save(raftpb.HardState{Term: 1, Vote: 1, Commit: 0}, entries(1, 1, 3), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() == before {
		t.Fatal("a Save with mustSync=true did not fsync")
	}

	before = syncs.Load()
	for c := uint64(1); c <= 3; c++ { // the commit index advancing, nothing else
		if err := p.Save(raftpb.HardState{Term: 1, Vote: 1, Commit: c}, nil, raftpb.Snapshot{}, false); err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load() - before; got != 0 {
		t.Fatalf("commit-index-only Saves did %d fsyncs, want 0", got)
	}

	before = syncs.Load()
	snap := raftpb.Snapshot{Data: []byte("s"), Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
	if err := p.Save(raftpb.HardState{}, nil, snap, false); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() == before {
		t.Fatal("a Save with a snapshot did not fsync")
	}

	// The unsynced writes are still there after a clean close and reopen.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	hs, _, _, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Commit != 3 {
		t.Fatalf("commit index after reopen = %d, want 3", hs.Commit)
	}
}
