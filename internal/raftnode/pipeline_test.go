package raftnode

import (
	"context"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// These tests hold the leader's Raft loop at a chosen point (inside Save, or
// right after a Send) and check what the group does meanwhile: what the
// leader has sent, what counts towards a commit, and what is applied.

// gateStore is a LogStore that can block one Save. It also keeps track of
// what its Saves have made durable, and of every snapshot stored.
type gateStore struct {
	inner LogStore // nil: nothing is kept, as for a node without a Store

	mu     sync.Mutex
	gate   *gate
	last   uint64 // index of the last entry (or snapshot) a Save has returned for
	commit uint64 // commit index of the last HardState a Save has returned for
	snaps  []snapRecord
}

// snapRecord is one SaveSnapshot call and what was durable when it started.
type snapRecord struct{ index, last, commit uint64 }

// gate blocks the first Save (or Send) that carries the entry with payload.
type gate struct {
	payload string
	entered chan struct{} // closed when the call blocks
	release chan struct{}
	once    sync.Once
	killed  atomic.Bool
}

func newGate(payload string) *gate {
	return &gate{payload: payload, entered: make(chan struct{}), release: make(chan struct{})}
}

// open lets the blocked call go on. Safe to call more than once.
func (g *gate) open() { g.once.Do(func() { close(g.release) }) }

// crash ends the blocked Save's goroutine, the node's Raft loop, without
// letting the Save write anything: the node dies in the middle of it.
func (g *gate) crash() {
	g.killed.Store(true)
	g.open()
}

// carries reports whether ents holds the proposal with this payload.
func carries(ents []raftpb.Entry, payload string) bool {
	return slices.ContainsFunc(ents, func(e raftpb.Entry) bool {
		return e.Type == raftpb.EntryNormal && len(e.Data) > idLen && string(e.Data[idLen:]) == payload
	})
}

// arm blocks the next Save that writes the entry with payload. The gate is
// opened when the test ends, so a failed test still stops its nodes.
func (s *gateStore) arm(t *testing.T, payload string) *gate {
	g := newGate(payload)
	t.Cleanup(g.open)
	s.mu.Lock()
	s.gate = g
	s.mu.Unlock()
	return g
}

func (s *gateStore) Save(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot, mustSync bool) error {
	s.mu.Lock()
	g := s.gate
	if g != nil && carries(ents, g.payload) {
		s.gate = nil
	} else {
		g = nil
	}
	s.mu.Unlock()
	if g != nil {
		close(g.entered)
		<-g.release
		if g.killed.Load() {
			runtime.Goexit()
		}
	}
	if s.inner != nil {
		if err := s.inner.Save(hs, ents, snap, mustSync); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !raft.IsEmptySnap(snap) {
		s.last = snap.Metadata.Index
	}
	if len(ents) > 0 {
		s.last = ents[len(ents)-1].Index
	}
	if !raft.IsEmptyHardState(hs) {
		s.commit = hs.Commit
	}
	return nil
}

func (s *gateStore) SaveSnapshot(snap raftpb.Snapshot, compactTo uint64) error {
	s.mu.Lock()
	s.snaps = append(s.snaps, snapRecord{snap.Metadata.Index, s.last, s.commit})
	s.mu.Unlock()
	if s.inner == nil {
		return nil
	}
	return s.inner.SaveSnapshot(snap, compactTo)
}

func (s *gateStore) Load() (raftpb.HardState, raftpb.Snapshot, []raftpb.Entry, error) {
	if s.inner == nil {
		return raftpb.HardState{}, raftpb.Snapshot{}, nil, nil
	}
	return s.inner.Load()
}

// lastIndex is the index of the last entry a finished Save wrote.
func (s *gateStore) lastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *gateStore) snapshots() []snapRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.snaps)
}

// stallTransport is a Transport that can hold its caller, the Raft loop,
// right after it has sent the message carrying one entry.
type stallTransport struct {
	Transport
	mu    sync.Mutex
	stall *gate
}

func (tr *stallTransport) arm(t *testing.T, payload string) *gate {
	g := newGate(payload)
	t.Cleanup(g.open)
	tr.mu.Lock()
	tr.stall = g
	tr.mu.Unlock()
	return g
}

func (tr *stallTransport) Send(msgs []raftpb.Message) {
	tr.Transport.Send(msgs)
	tr.mu.Lock()
	g := tr.stall
	if g != nil && slices.ContainsFunc(msgs, func(m raftpb.Message) bool { return carries(m.Entries, g.payload) }) {
		tr.stall = nil
	} else {
		g = nil
	}
	tr.mu.Unlock()
	if g != nil {
		close(g.entered)
		<-g.release
	}
}

// pipeline is a three-node cluster whose stores and transports can be held,
// with node 1 as leader. Election timeouts are 10 s, so no follower starts an
// election while a test holds the leader's loop.
type pipeline struct {
	*cluster
	stores map[uint64]*gateStore
	links  map[uint64]*stallTransport
	lead   *Node
}

const pipelineElectionTicks = 1000

func newPipeline(t *testing.T, durable bool, snapEvery uint64) *pipeline {
	t.Helper()
	p := &pipeline{stores: map[uint64]*gateStore{}, links: map[uint64]*stallTransport{}}
	p.cluster = startClusterOpts(t, 3, func(c *cluster) {
		c.durable, c.snapEvery, c.electionTicks = durable, snapEvery, pipelineElectionTicks
		c.wrapStore = func(id uint64, st LogStore) LogStore {
			p.stores[id] = &gateStore{inner: st}
			return p.stores[id]
		}
		c.wrapTransport = func(id uint64, tr Transport) Transport {
			p.links[id] = &stallTransport{Transport: tr}
			return p.links[id]
		}
	})
	// These tests are of the two Ready loop changes: both must be on, as
	// they are by default (RAFT_LEADER_SEND_FIRST and RAFT_EARLY_APPLY).
	for id, n := range p.nodes {
		if n.cfg.DisableLeaderSendFirst || n.cfg.DisableEarlyApply {
			t.Fatalf("node %d runs with a pipeline change off: %+v", id, n.cfg)
		}
	}
	// Nobody's election timer fires for 10 s: make node 1 campaign now. It
	// ignores the request until it has applied the initial configuration.
	p.lead = p.nodes[1]
	// A second request while the first election is still running would
	// start it over, so each one gets time to finish.
	eventually(t, "node 1 to become leader", func() bool {
		_ = p.lead.raft.Campaign(context.Background())
		return holdsWithin(500*time.Millisecond, func() bool { return p.lead.Status().IsLeader() })
	})
	if got := p.waitLeader(); got != 1 {
		t.Fatalf("leader is node %d, want node 1", got)
	}
	return p
}

// settle proposes one entry, waits until every node has applied it and knows
// it is committed, and returns its index: the next entry proposed gets the
// index after it.
func (p *pipeline) settle() uint64 {
	p.t.Helper()
	propose(p.t, p.lead, "warm")
	p.waitApplied(1)
	base := p.lead.Status().Commit
	eventually(p.t, "the followers to learn the commit index", func() bool {
		for _, n := range p.nodes {
			if st := n.Status(); st.Commit != base || st.Applied != base {
				return false
			}
		}
		return true
	})
	return base
}

// proposeAsync proposes data on the leader and returns where the result
// arrives.
func (p *pipeline) proposeAsync(data string) <-chan outcome {
	ch := make(chan outcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		val, err := p.lead.Propose(ctx, []byte(data))
		ch <- outcome{val, err}
	}()
	return ch
}

// proposeNoWait hands data to the leader's Raft and returns once Raft has
// taken it, without waiting for it to be applied.
func (p *pipeline) proposeNoWait(data string) {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.lead.raft.Propose(ctx, encode(1, []byte(data))); err != nil {
		p.t.Fatalf("propose %q: %v", data, err)
	}
}

const patience = 10 * time.Second

// eventually waits for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !holdsWithin(patience, cond) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func holdsWithin(d time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(d); ; time.Sleep(2 * time.Millisecond) {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// entered waits until the gate's Save or Send has blocked.
func entered(t *testing.T, g *gate, what string) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(patience):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// result waits for a proposal's result and returns its position in the log.
func result(t *testing.T, ch <-chan outcome, what string) int {
	t.Helper()
	select {
	case out := <-ch:
		if out.err != nil {
			t.Fatalf("%s: %v", what, out.err)
		}
		return out.val.(int)
	case <-time.After(patience):
		t.Fatalf("timed out: %s", what)
		return 0
	}
}

// never fails the test if cond holds at any time in the next 300 ms.
func never(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if holdsWithin(300*time.Millisecond, cond) {
		t.Fatal(what)
	}
}

// Fix 1. The leader sends an entry to its followers before its own Save of
// it: with that Save blocked, the entry still commits once both followers
// have it on disk.
func TestLeaderReplicatesWhileItPersists(t *testing.T) {
	p := newPipeline(t, false, 0)
	base := p.settle()

	save := p.stores[1].arm(t, "x")
	res := p.proposeAsync("x")
	entered(t, save, "the leader's Save of x")

	if !holdsWithin(patience, func() bool { return p.lead.Status().Commit > base }) {
		t.Fatalf("x did not commit while the leader's Save of it was blocked: the followers hold entries through %d and %d, x is entry %d",
			p.stores[2].lastIndex(), p.stores[3].lastIndex(), base+1)
	}
	for _, id := range []uint64{2, 3} {
		if got := p.stores[id].lastIndex(); got != base+1 {
			t.Fatalf("follower %d has persisted through %d, want %d: x committed without it", id, got, base+1)
		}
	}
	if got := p.stores[1].lastIndex(); got != base {
		t.Fatalf("the leader has persisted through %d with its Save blocked, want %d", got, base)
	}
	// Committed, but not on the leader's disk: the leader does not apply it.
	never(t, "the leader applied x before it had persisted it", func() bool {
		return p.lead.Status().Applied > base || len(res) > 0
	})

	save.open()
	if got := result(t, res, "x was not applied after the leader's Save returned"); got != 2 {
		t.Fatalf("x applied at position %d, want 2", got)
	}
	p.waitApplied(2)
}

// The other half of fix 1: the leader counts its own copy of an entry only
// once it has persisted it. With its Save blocked and one follower stopped,
// the entry is on one disk of three and must not commit.
func TestLeaderCountsItselfOnlyAfterPersisting(t *testing.T) {
	p := newPipeline(t, false, 0)
	base := p.settle()
	p.stop(3)

	save := p.stores[1].arm(t, "x")
	res := p.proposeAsync("x")
	entered(t, save, "the leader's Save of x")

	// The follower that is left persists x and the leader knows it.
	eventually(t, "follower 2 to persist x and the leader to hear of it", func() bool {
		if p.stores[2].lastIndex() != base+1 {
			return false
		}
		for _, f := range p.lead.Status().Followers {
			if f.ID == 2 && f.Match == base+1 {
				return true
			}
		}
		return false
	})
	never(t, "x committed with one durable copy: the leader counted its own, which is not on disk", func() bool {
		return p.lead.Status().Commit > base || p.nodes[2].Status().Commit > base || len(res) > 0
	})

	save.open()
	if got := result(t, res, "x did not commit once the leader had persisted it"); got != 2 {
		t.Fatalf("x applied at position %d, want 2", got)
	}
	p.waitApplied(2)
}

// aheadOfSave brings the leader to the point fix 3 is about: its loop is
// blocked in the Save of b, in a Ready that also carries a, which is
// committed and was persisted by the Ready before. It returns a's index, the
// gate holding the Save and where a's result arrives.
//
// To get a's commit and b's append into one Ready, the loop is held right
// after it has sent a, until the followers have acknowledged a and b has
// been proposed.
func (p *pipeline) aheadOfSave(t *testing.T) (uint64, *gate, <-chan outcome) {
	t.Helper()
	base := p.settle()

	stall := p.links[1].arm(t, "a")
	resA := p.proposeAsync("a")
	entered(t, stall, "the leader to send a")
	eventually(t, "a to commit", func() bool { return p.lead.Status().Commit > base })

	save := p.stores[1].arm(t, "b")
	p.proposeNoWait("b")
	stall.open()
	entered(t, save, "the leader's Save of b")
	if got := p.stores[1].lastIndex(); got != base+1 {
		t.Fatalf("the leader has persisted through %d, want %d (a, not b)", got, base+1)
	}
	return base + 1, save, resA
}

// Fix 3. A committed entry that is already on the leader's disk is applied
// while the Save of the next Ready is blocked. An entry that is committed but
// not on the leader's disk yet is not.
func TestAppliesPersistedEntriesBeforeSave(t *testing.T) {
	p := newPipeline(t, false, 0)
	a, save, resA := p.aheadOfSave(t)

	if got := result(t, resA, "a (committed, persisted) was not applied while the Save of b was blocked"); got != 2 {
		t.Fatalf("a applied at position %d, want 2", got)
	}
	if got := p.lead.Status().Applied; got != a {
		t.Fatalf("applied index %d, want %d", got, a)
	}

	// b reaches the followers (fix 1) and commits, but the leader's Save of
	// it is still blocked.
	eventually(t, "b to commit", func() bool { return p.lead.Status().Commit > a })
	never(t, "the leader applied b before it had persisted it", func() bool {
		return p.lead.Status().Applied > a || slices.Contains(p.sms[1].snapshot(), "b")
	})

	save.open()
	p.waitApplied(3)
	if got := p.sms[1].snapshot(); !slices.Equal(got, []string{"warm", "a", "b"}) {
		t.Fatalf("applied %v", got)
	}
}

// A snapshot never records an applied index that the disk does not cover: not
// above the last persisted entry, and not above the persisted commit index (a
// restart would refuse a snapshot ahead of it). Entries applied ahead of a
// Save must wait for that Save before they are in a snapshot.
func TestNoSnapshotAheadOfPersistedState(t *testing.T) {
	p := newPipeline(t, false, 1) // a snapshot is due after every entry
	a, save, resA := p.aheadOfSave(t)
	result(t, resA, "a was not applied while the Save of b was blocked")

	check := func() {
		t.Helper()
		for _, s := range p.stores[1].snapshots() {
			if s.index > s.last || s.index > s.commit {
				t.Fatalf("snapshot at index %d stored when the log was persisted through %d and the commit index through %d", s.index, s.last, s.commit)
			}
		}
	}
	// a is applied, and the commit index that covers it is in the blocked
	// Save.
	never(t, "a snapshot that includes a was stored before the Save that commits a", func() bool {
		return slices.ContainsFunc(p.stores[1].snapshots(), func(s snapRecord) bool { return s.index >= a })
	})
	check()

	save.open()
	eventually(t, "a snapshot that includes a", func() bool {
		return slices.ContainsFunc(p.stores[1].snapshots(), func(s snapRecord) bool { return s.index >= a })
	})
	p.waitApplied(3)
	check()
}

// The leader crashes in the middle of a Save, after it applied an entry ahead
// of that Save and answered the client. After a restart from disk nothing
// that was applied is lost and nothing is applied twice.
func TestCrashDuringSave(t *testing.T) {
	for name, snapEvery := range map[string]uint64{"log only": 0, "snapshot after every entry": 1} {
		t.Run(name, func(t *testing.T) {
			p := newPipeline(t, true, snapEvery)
			a, save, resA := p.aheadOfSave(t)
			posA := result(t, resA, "a was not applied while the Save of b was blocked")
			// b went out before the Save: both followers get it.
			eventually(t, "the followers to persist b", func() bool {
				return p.stores[2].lastIndex() == a+1 && p.stores[3].lastIndex() == a+1
			})
			if snapEvery > 0 {
				// Let any snapshot the leader has started reach its disk.
				time.Sleep(50 * time.Millisecond)
			}

			// Crash: the Save of b never happens, and nothing after it.
			old := p.lead
			save.crash()
			<-old.done
			old.raft.Stop()
			if err := p.cluster.stores[1].Close(); err != nil {
				t.Fatal(err)
			}
			delete(p.cluster.stores, 1)
			delete(p.nodes, 1)
			delete(p.sms, 1)

			lead := p.failover(2, 3)
			p.start(1) // from disk, with an empty state machine
			if got := propose(t, p.nodes[lead], "after"); got != 4 {
				t.Fatalf("the first entry after the restart applied at position %d, want 4", got)
			}
			p.waitApplied(4)

			want := []string{"warm", "a", "b", "after"}
			for id, sm := range p.sms {
				got := sm.snapshot()
				if !slices.Equal(got, want) {
					t.Fatalf("node %d applied %v, want %v", id, got, want)
				}
				if got[posA-1] != "a" {
					t.Fatalf("node %d: a was answered at position %d, which now holds %q", id, posA, got[posA-1])
				}
			}
			// Every entry reached the restarted state machine exactly once:
			// through Apply, or inside the one snapshot it was restored from.
			applies, restores := p.sms[1].counts()
			if snapEvery == 0 && (applies != len(want) || restores != 0) {
				t.Fatalf("restarted node: %d applies and %d restores for %d entries", applies, restores, len(want))
			}
			if applies > len(want) || restores > 1 {
				t.Fatalf("restarted node: %d applies and %d restores for %d entries", applies, restores, len(want))
			}
			for _, id := range []uint64{2, 3} {
				if applies, _ := p.sms[id].counts(); applies != len(want) {
					t.Fatalf("node %d: %d applies for %d entries", id, applies, len(want))
				}
			}
		})
	}
}

// failover makes the given nodes' election timers run out, by ticking them,
// until one of them is leader, and returns it.
func (p *pipeline) failover(ids ...uint64) uint64 {
	p.t.Helper()
	eventually(p.t, "a new leader", func() bool {
		for _, id := range ids {
			if p.nodes[id].Status().IsLeader() {
				return true
			}
		}
		for _, id := range ids {
			for range 50 {
				p.nodes[id].raft.Tick()
			}
		}
		return false
	})
	return p.waitLeader(1) // node 1 is down
}
