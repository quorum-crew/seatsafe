package raftstore

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

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

func snapAt(index uint64, data string) raftpb.Snapshot {
	return raftpb.Snapshot{
		Data:     []byte(data),
		Metadata: raftpb.SnapshotMetadata{Index: index, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}},
	}
}

// rawEntries counts entry keys on disk, including ones Load would skip.
func rawEntries(t *testing.T, p *Pebble) []uint64 {
	t.Helper()
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: entryKey(0), UpperBound: entriesEnd})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []uint64
	for it.First(); it.Valid(); it.Next() {
		out = append(out, binary.BigEndian.Uint64(it.Key()[1:]))
	}
	return out
}

// A snapshot received from the leader (in a Ready) replaces the whole log,
// like MemoryStorage.ApplySnapshot: a stale tail after its index must not
// come back on restart (#11).
func TestReceivedSnapshotReplacesWholeLog(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 6}, entries(1, 1, 10), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(raftpb.HardState{Term: 2, Commit: 4}, nil, snapAt(4, "state"), true); err != nil {
		t.Fatal(err)
	}
	if got := rawEntries(t, p); len(got) != 0 {
		t.Fatalf("entries left on disk after a received snapshot: %v", got)
	}
	// Entries in the same Ready come after the snapshot and are kept.
	if err := p.Save(raftpb.HardState{}, entries(2, 5, 6), snapAt(4, "state"), true); err != nil {
		t.Fatal(err)
	}
	p.Close()

	p, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, gotSnap, ents, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if gotSnap.Metadata.Index != 4 || string(gotSnap.Data) != "state" {
		t.Errorf("snapshot = index %d data %q", gotSnap.Metadata.Index, gotSnap.Data)
	}
	if got := indexes(ents); len(got) != 2 || got[0] != 5 || got[1] != 6 || ents[0].Term != 2 {
		t.Errorf("entries after reload = %v, want [5 6] from the new leader", got)
	}
}

// A snapshot the node takes of itself keeps its own log after the snapshot
// and drops only entries up to compactTo.
func TestSaveSnapshotCompactsUpTo(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 10}, entries(1, 1, 10), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	if err := p.SaveSnapshot(snapAt(8, "s8"), 5); err != nil {
		t.Fatal(err)
	}
	if got := rawEntries(t, p); len(got) != 5 || got[0] != 6 || got[4] != 10 {
		t.Fatalf("entries on disk = %v, want 6..10 (catch-up entries kept)", got)
	}
	p.Close()

	p, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, snap, ents, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Metadata.Index != 8 || string(snap.Data) != "s8" {
		t.Errorf("snapshot = index %d data %q", snap.Metadata.Index, snap.Data)
	}
	if got := indexes(ents); len(got) != 2 || got[0] != 9 || got[1] != 10 {
		t.Errorf("Load returned entries %v, want [9 10] (only those after the snapshot)", got)
	}
	// compactTo = 0 keeps everything.
	if err := p.SaveSnapshot(snapAt(9, "s9"), 0); err != nil {
		t.Fatal(err)
	}
	if got := rawEntries(t, p); len(got) != 5 {
		t.Errorf("compactTo=0 dropped entries: %v", got)
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

// Plain appends must not write a range deletion each (#30's load test found
// Save stalling for seconds and the heap exploding: Pebble flushes nested
// range tombstones in quadratic time). 30,000 one-entry appends, the shape of
// a leader under load, must stay fast. Before the fix one Save blocked for
// seconds at 20,000 and the process ran out of memory soon after.
func TestAppendsStayFast(t *testing.T) {
	if testing.Short() {
		t.Skip("slow when it regresses")
	}
	p, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	data := make([]byte, 200)
	var slowest time.Duration
	start := time.Now()
	for i := uint64(1); i <= 30000; i++ {
		s := time.Now()
		if err := p.Save(raftpb.HardState{Term: 1, Commit: i}, []raftpb.Entry{{Term: 1, Index: i, Data: data}}, raftpb.Snapshot{}, false); err != nil {
			t.Fatal(err)
		}
		slowest = max(slowest, time.Since(s))
		if time.Since(start) > 20*time.Second {
			t.Fatalf("only %d appends in 20s (slowest Save %v): appends are writing range deletions again", i, slowest)
		}
	}
	if slowest > time.Second {
		t.Fatalf("a single append took %v", slowest)
	}
	t.Logf("30,000 appends in %v, slowest Save %v", time.Since(start).Round(time.Millisecond), slowest.Round(time.Microsecond))
}

// The last index is loaded on open, so a conflicting tail written after a
// restart is still dropped: a new leader's shorter log replaces ours.
func TestConflictingTailDroppedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 6}, entries(1, 1, 10), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	p.Close()

	p, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// Term 2's leader has entries 7 and 8 only: 9 and 10 must go.
	if err := p.Save(raftpb.HardState{Term: 2, Commit: 6}, entries(2, 7, 8), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	// A later plain append extends the new log normally.
	if err := p.Save(raftpb.HardState{}, entries(2, 9, 9), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	_, _, ents, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := indexes(ents); len(got) != 9 || got[8] != 9 {
		t.Fatalf("entries = %v, want 1..9", got)
	}
	for _, e := range ents[6:] {
		if e.Term != 2 {
			t.Fatalf("entry %d has term %d: the old tail survived the reopen", e.Index, e.Term)
		}
	}
}

// The node takes its own snapshots on the apply goroutine while the Raft loop
// keeps saving. If the loop stores a newer snapshot from the leader first,
// the node's older one must not roll it back, before or after a reopen.
func TestOlderOwnSnapshotIgnored(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 10}, entries(1, 1, 10), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(raftpb.HardState{Term: 2, Commit: 50}, nil, snapAt(50, "leader"), true); err != nil {
		t.Fatal(err)
	}
	if err := p.SaveSnapshot(snapAt(8, "own"), 5); err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		t.Helper()
		_, snap, _, err := p.Load()
		if err != nil {
			t.Fatal(err)
		}
		if snap.Metadata.Index != 50 || string(snap.Data) != "leader" {
			t.Fatalf("%s: snapshot = index %d data %q, want the leader's at 50", when, snap.Metadata.Index, snap.Data)
		}
	}
	check("before reopen")
	p.Close()

	if p, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.SaveSnapshot(snapAt(40, "own"), 0); err != nil {
		t.Fatal(err)
	}
	check("after reopen")
}

// slowSyncFS makes every fsync take delay while slow is set, and signals
// entered when one starts.
type slowSyncFS struct {
	vfs.FS
	slow    *atomic.Bool
	delay   time.Duration
	entered chan struct{}
}

func (fs slowSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	return slowSyncFile{f, fs}, err
}

func (fs slowSyncFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	return slowSyncFile{f, fs}, err
}

type slowSyncFile struct {
	vfs.File
	fs slowSyncFS
}

func (f slowSyncFile) wait() {
	if f.fs.slow.Load() {
		select {
		case f.fs.entered <- struct{}{}:
		default:
		}
		time.Sleep(f.fs.delay)
	}
}

func (f slowSyncFile) Sync() error     { f.wait(); return f.File.Sync() }
func (f slowSyncFile) SyncData() error { f.wait(); return f.File.SyncData() }
func (f slowSyncFile) SyncTo(n int64) (bool, error) {
	f.wait()
	return f.File.SyncTo(n)
}

// The node's snapshot goroutine stores snapshots while the Raft loop keeps
// saving. A Save that needs no fsync must not wait for the snapshot's fsync:
// holding one lock across that commit stalled the Raft loop for 1.4 s on
// every replica of a shard at once.
func TestSaveNotBlockedBySnapshotSync(t *testing.T) {
	var slow atomic.Bool
	fs := slowSyncFS{FS: vfs.Default, slow: &slow, delay: 500 * time.Millisecond, entered: make(chan struct{}, 1)}
	p, err := open(t.TempDir(), &pebble.Options{Logger: quietLogger{}, FS: fs})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 10}, entries(1, 1, 10), raftpb.Snapshot{}, true); err != nil {
		t.Fatal(err)
	}

	slow.Store(true)
	errc := make(chan error, 1)
	go func() { errc <- p.SaveSnapshot(snapAt(8, "own"), 5) }()
	<-fs.entered // the snapshot's commit is fsyncing

	start := time.Now()
	if err := p.Save(raftpb.HardState{Term: 1, Commit: 12}, entries(1, 11, 12), raftpb.Snapshot{}, false); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Fatalf("Save waited %v for the snapshot's fsync", d)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	slow.Store(false)

	_, snap, ents, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Metadata.Index != 8 || len(ents) != 4 || ents[0].Index != 9 || ents[3].Index != 12 {
		t.Fatalf("after both: snapshot %d, %d entries after it, on disk %v", snap.Metadata.Index, len(ents), rawEntries(t, p))
	}
}
