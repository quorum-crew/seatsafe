package raftnode

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// orderLog records, in order, what handleReady did with a Ready: which
// messages it sent and when it saved.
type orderLog struct{ events []string }

func (o *orderLog) Send(msgs []raftpb.Message) {
	var types []string
	for _, m := range msgs {
		types = append(types, m.Type.String())
	}
	o.events = append(o.events, "send "+strings.Join(types, ","))
}

func (o *orderLog) Save(raftpb.HardState, []raftpb.Entry, raftpb.Snapshot, bool) error {
	o.events = append(o.events, "save")
	return nil
}

func (o *orderLog) SaveSnapshot(raftpb.Snapshot, uint64) error { return nil }

func (o *orderLog) Load() (raftpb.HardState, raftpb.Snapshot, []raftpb.Entry, error) {
	return raftpb.HardState{}, raftpb.Snapshot{}, nil, nil
}

// readyNode is a Node with only what handleReady uses: entries 1..last
// persisted at term 1, HardState {Term 1, Vote 1} persisted.
func readyNode(t *testing.T, last uint64) (*Node, *orderLog) {
	t.Helper()
	o := &orderLog{}
	n := &Node{cfg: Config{ID: 1, Transport: o, Store: o}, log: quiet, storage: raft.NewMemoryStorage()}
	if err := n.storage.Append(entries(1, 1, last)); err != nil {
		t.Fatal(err)
	}
	n.hardState = raftpb.HardState{Term: 1, Vote: 1, Commit: 1}
	return n, o
}

// entries returns entries from..to of one term.
func entries(term, from, to uint64) []raftpb.Entry {
	var out []raftpb.Entry
	for i := from; i <= to; i++ {
		out = append(out, raftpb.Entry{Term: term, Index: i, Data: encode(i, []byte("x"))})
	}
	return out
}

func indexes(ents []raftpb.Entry) []uint64 {
	out := []uint64{}
	for _, e := range ents {
		out = append(out, e.Index)
	}
	return out
}

// queued drains the apply queue: for every batch, "early" or "late" and the
// indexes of its entries.
func queued(n *Node) []string {
	var out []string
	for _, b := range n.applyq.items {
		kind := "late"
		if b.early {
			kind = "early"
		}
		out = append(out, fmt.Sprint(kind, indexes(b.entries)))
	}
	n.applyq.items = nil
	return out
}

func msgs(types ...raftpb.MessageType) []raftpb.Message {
	var out []raftpb.Message
	for _, t := range types {
		out = append(out, raftpb.Message{Type: t, From: 1, To: 2})
	}
	return out
}

// Which messages go out before the Save, and which after it.
func TestHandleReadySendOrder(t *testing.T) {
	leader := &raft.SoftState{Lead: 1, RaftState: raft.StateLeader}
	follower := &raft.SoftState{Lead: 2, RaftState: raft.StateFollower}
	all := msgs(raftpb.MsgApp, raftpb.MsgHeartbeat, raftpb.MsgSnap, raftpb.MsgAppResp, raftpb.MsgVoteResp, raftpb.MsgPreVoteResp)
	for _, c := range []struct {
		name    string
		leading bool // leader as of the Ready before
		rd      raft.Ready
		want    []string
	}{
		{"leader: appends and heartbeats first; snapshots and answers after the Save", true,
			raft.Ready{Messages: all, Entries: entries(1, 11, 12)},
			[]string{"send MsgApp,MsgHeartbeat", "save", "send MsgSnap,MsgAppResp,MsgVoteResp,MsgPreVoteResp"}},
		{"leader by this Ready's SoftState", false,
			raft.Ready{SoftState: leader, Messages: msgs(raftpb.MsgApp), Entries: entries(1, 11, 11)},
			[]string{"send MsgApp", "save"}},
		{"leader, HardState with a new commit index only", true,
			raft.Ready{HardState: raftpb.HardState{Term: 1, Vote: 1, Commit: 9}, Messages: msgs(raftpb.MsgApp)},
			[]string{"send MsgApp", "save"}},
		{"leader, but the term changes in this Ready", true,
			raft.Ready{HardState: raftpb.HardState{Term: 2, Vote: 1, Commit: 9}, Messages: msgs(raftpb.MsgApp)},
			[]string{"save", "send MsgApp"}},
		{"leader, but the vote changes in this Ready", true,
			raft.Ready{HardState: raftpb.HardState{Term: 1, Vote: 3, Commit: 9}, Messages: msgs(raftpb.MsgApp)},
			[]string{"save", "send MsgApp"}},
		{"follower", false,
			raft.Ready{Messages: msgs(raftpb.MsgAppResp, raftpb.MsgHeartbeatResp), Entries: entries(1, 11, 11)},
			[]string{"save", "send MsgAppResp,MsgHeartbeatResp"}},
		{"candidate", false,
			raft.Ready{SoftState: &raft.SoftState{RaftState: raft.StateCandidate}, HardState: raftpb.HardState{Term: 2, Vote: 1}, Messages: msgs(raftpb.MsgVote)},
			[]string{"save", "send MsgVote"}},
		{"was leader, steps down in this Ready", true,
			raft.Ready{SoftState: follower, Messages: msgs(raftpb.MsgAppResp, raftpb.MsgApp)},
			[]string{"save", "send MsgAppResp,MsgApp"}},
		{"leader with nothing to send", true, raft.Ready{Entries: entries(1, 11, 11)}, []string{"save"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, o := readyNode(t, 10)
			n.leading = c.leading
			if _, err := n.handleReady(c.rd); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(o.events, c.want) {
				t.Fatalf("order %q, want %q", o.events, c.want)
			}
		})
	}

	// A Ready with a snapshot to install: persist, then send, leader or not.
	n, o := readyNode(t, 10)
	n.leading = true
	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 1}}
	if _, err := n.handleReady(raft.Ready{Snapshot: snap, Messages: msgs(raftpb.MsgApp)}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"save", "send MsgApp"}; !slices.Equal(o.events, want) {
		t.Fatalf("with a snapshot: order %q, want %q", o.events, want)
	}
}

// Which committed entries are handed to the apply goroutine before the Save.
// The node has entries 1..10 on disk.
func TestHandleReadyApplyOrder(t *testing.T) {
	confChange := entries(1, 9, 10)
	confChange[1].Type = raftpb.EntryConfChange
	for _, c := range []struct {
		name string
		rd   raft.Ready
		want []string // batches in the order pushed; the late one is returned, not pushed
	}{
		{"all persisted: early, though this Ready writes more",
			raft.Ready{CommittedEntries: entries(1, 8, 10), Entries: entries(1, 11, 12)}, []string{"early[8 9 10]", "late[]"}},
		{"all persisted, nothing to write",
			raft.Ready{CommittedEntries: entries(1, 8, 10)}, []string{"early[8 9 10]", "late[]"}},
		{"committed past the persisted log: the rest waits for the Save",
			raft.Ready{CommittedEntries: entries(1, 9, 12), Entries: entries(1, 11, 12)}, []string{"early[9 10]", "late[11 12]"}},
		{"only entries this Ready writes",
			raft.Ready{CommittedEntries: entries(1, 11, 12), Entries: entries(1, 11, 12)}, []string{"late[11 12]"}},
		{"this Ready rewrites entries 9 and 10: the new 9 is not on disk",
			raft.Ready{CommittedEntries: entries(2, 8, 9), Entries: entries(2, 9, 10)}, []string{"early[8]", "late[9]"}},
		{"a configuration change: nothing early",
			raft.Ready{CommittedEntries: confChange, Entries: entries(1, 11, 11)}, []string{"late[9 10]"}},
		{"a snapshot to install: nothing early",
			raft.Ready{Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 1}}, CommittedEntries: entries(1, 21, 21), Entries: entries(1, 21, 21)},
			[]string{"late[21]"}},
		{"nothing committed", raft.Ready{Entries: entries(1, 11, 11)}, []string{"late[]"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, o := readyNode(t, 10)
			// The early batch must be in the queue before Save is called.
			var beforeSave []string
			n.cfg.Store = saveFunc(func() { beforeSave = queued(n) })
			late, err := n.handleReady(c.rd)
			if err != nil {
				t.Fatal(err)
			}
			if got := queued(n); len(got) != 0 {
				t.Fatalf("pushed after the Save: %v", got)
			}
			if late.early || (late.done != nil) != slices.ContainsFunc(c.rd.CommittedEntries, isConfChange) {
				t.Fatalf("late batch: early=%v, waits=%v", late.early, late.done != nil)
			}
			got := append(beforeSave, fmt.Sprint("late", indexes(late.entries)))
			if !slices.Equal(got, c.want) {
				t.Fatalf("batches %v, want %v", got, c.want)
			}
			_ = o
		})
	}
}

// saveFunc is a LogStore whose Save calls a function.
type saveFunc func()

func (f saveFunc) Save(raftpb.HardState, []raftpb.Entry, raftpb.Snapshot, bool) error {
	f()
	return nil
}
func (saveFunc) SaveSnapshot(raftpb.Snapshot, uint64) error { return nil }
func (saveFunc) Load() (raftpb.HardState, raftpb.Snapshot, []raftpb.Entry, error) {
	return raftpb.HardState{}, raftpb.Snapshot{}, nil, nil
}

// An early batch never starts a snapshot, however many entries it applies,
// and a snapshot is never taken at an applied index beyond the persisted log.
func TestSnapshotOnlyOfPersistedState(t *testing.T) {
	n, _ := readyNode(t, 10)
	n.cfg.SM = &logSM{}
	n.cfg.SnapshotEntries = 1
	n.waiters = map[uint64]waiter{}
	if err := n.apply(applyBatch{entries: entries(1, 1, 10), early: true}); err != nil {
		t.Fatal(err)
	}
	if n.snapshotting.Load() || n.applied.Load() != 10 {
		t.Fatalf("after an early batch: snapshotting=%v, applied=%d", n.snapshotting.Load(), n.applied.Load())
	}
	// Applied beyond the persisted log (which the loop never lets happen):
	// no snapshot either.
	n.applied.Store(11)
	if err := n.maybeSnapshot(); err != nil || n.snapshotting.Load() {
		t.Fatalf("snapshot at an index that is not persisted: %v, snapshotting=%v", err, n.snapshotting.Load())
	}
	// The batch after the Save takes it.
	n.applied.Store(10)
	if err := n.apply(applyBatch{}); err != nil {
		t.Fatal(err)
	}
	n.snapshots.Wait()
	if got := n.snapIndex.Load(); got != 10 {
		t.Fatalf("snapshot index %d, want 10", got)
	}
}
