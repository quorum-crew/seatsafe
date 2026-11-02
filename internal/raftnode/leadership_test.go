package raftnode

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// proposeAsync proposes with no deadline, as an RPC from a client without one
// would, and returns a channel with the error.
func proposeAsync(n *Node, data string) <-chan error {
	errc := make(chan error, 1)
	go func() {
		_, err := n.Propose(context.Background(), []byte(data))
		errc <- err
	}()
	return errc
}

func waitErr(t *testing.T, errc <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(within):
		t.Fatalf("Propose still blocked after %v: a pending proposal hangs when leadership changes", within)
		return nil
	}
}

// A leader cut off from the group steps down (CheckQuorum). A proposal it
// accepted just before must fail with ErrLeadershipLost, not wait forever.
func TestLeaderLossFailsPendingProposal(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	propose(t, c.nodes[old], "warm-up")

	c.net.Isolate(old)
	errc := proposeAsync(c.nodes[old], "never-commits")

	if err := waitErr(t, errc, 3*time.Second); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("pending proposal on the deposed leader returned %v, want ErrLeadershipLost", err)
	}
	if c.nodes[old].Status().IsLeader() {
		t.Fatal("isolated node still claims to be leader")
	}

	// The rest of the group carries on, and after healing everyone agrees.
	lead := c.waitLeader(old)
	propose(t, c.nodes[lead], "after")
	c.net.Heal()
	c.waitApplied(2)
}

// A follower forwards proposals to the leader. If that leader is gone, the
// forwarded proposal is lost; once the follower learns of a new leader (or of
// no leader), its waiting Propose must return instead of hanging.
func TestFollowerProposalFailsWhenLeaderChanges(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	follower := old%3 + 1

	c.net.Isolate(old)
	errc := proposeAsync(c.nodes[follower], "forwarded-to-a-dead-leader")

	err := waitErr(t, errc, 3*time.Second)
	switch {
	case err == nil:
		// Possible if the follower already became leader itself: fine.
	case errors.Is(err, ErrLeadershipLost), errors.Is(err, raft.ErrProposalDropped):
		// Lost leadership, or the follower already knew there was no leader.
	default:
		t.Fatalf("follower proposal returned %v", err)
	}
}

// Results committed before the change are still delivered: failing pending
// proposals must not throw away answers that are ready.
func TestCommittedResultsSurviveLeaderChange(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.waitLeader()
	for i := range 20 {
		if got := propose(t, c.nodes[lead], string(rune('a'+i))); got != i+1 {
			t.Fatalf("entry %d applied at %d", i+1, got)
		}
	}
	c.net.Isolate(lead)
	newLead := c.waitLeader(lead)
	if got := propose(t, c.nodes[newLead], "z"); got != 21 {
		t.Fatalf("first entry under the new leader applied at %d, want 21", got)
	}
	c.net.Heal()
	c.waitApplied(21)
}

// electionGate is a LogStore that stands in for a slow fsync. Once armed, it
// holds the Save of the Ready that makes its node leader until a proposal is
// waiting on that node: Status already says leader there, so this is what a
// client that proposes the moment it sees a new leader runs into.
type electionGate struct {
	recordingStore
	node   atomic.Pointer[Node]
	armed  *atomic.Bool
	result chan error
}

func (g *electionGate) Save(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot, mustSync bool) error {
	n := g.node.Load()
	if n != nil && len(ents) > 0 && n.Status().IsLeader() && g.armed.CompareAndSwap(true, false) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := n.Propose(ctx, []byte("first-proposal-to-the-new-leader"))
			g.result <- err
		}()
		for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			n.mu.Lock()
			waiting := len(n.waiters)
			n.mu.Unlock()
			if waiting > 0 {
				break
			}
		}
	}
	return g.recordingStore.Save(hs, ents, snap, mustSync)
}

// A node that wins an election has lost nothing. A proposal that reaches it
// while the Ready carrying its leadership is still being persisted must commit,
// not fail with ErrLeadershipLost.
func TestProposalDuringElectionIsNotFailed(t *testing.T) {
	net := NewNetwork()
	t.Cleanup(net.Close)
	peers := []uint64{1, 2, 3}
	armed := &atomic.Bool{}
	result := make(chan error, 1)
	nodes := map[uint64]*Node{}
	for _, id := range peers {
		g := &electionGate{armed: armed, result: result}
		n, err := Start(Config{ID: id, Peers: peers, SM: &logSM{}, Transport: net, Store: g,
			TickInterval: 10 * time.Millisecond, Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		g.node.Store(n)
		net.Add(id, n)
		nodes[id] = n
		t.Cleanup(n.Stop)
	}
	c := &cluster{t: t, nodes: nodes}

	for round := range 10 {
		old := c.waitLeader()
		net.Isolate(old)
		armed.Store(true) // the isolated leader appends nothing, so only a new leader can trip this
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("election %d: proposal to the new leader failed: %v", round, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("election %d: no new leader saved its first entry", round)
		}
		net.Heal()
	}
}

// What a SoftState update does to a proposal waiting on the node, for every
// transition. Only winning an election with no known leader leaves it waiting.
func TestLeadershipTransitions(t *testing.T) {
	const self, other, third = 1, 2, 3
	for _, tc := range []struct {
		name     string
		lead     uint64
		isLeader bool
		ss       *raft.SoftState
		fails    bool
	}{
		{name: "no SoftState", lead: other},
		{name: "candidate with no leader", ss: &raft.SoftState{RaftState: raft.StateCandidate}},
		{name: "none to self", ss: &raft.SoftState{Lead: self, RaftState: raft.StateLeader}},
		{name: "old leader to self", lead: other, ss: &raft.SoftState{Lead: self, RaftState: raft.StateLeader}, fails: true},
		{name: "none to other", ss: &raft.SoftState{Lead: other, RaftState: raft.StateFollower}, fails: true},
		{name: "known leader to none", lead: other, ss: &raft.SoftState{RaftState: raft.StateCandidate}, fails: true},
		{name: "known leader to another", lead: other, ss: &raft.SoftState{Lead: third, RaftState: raft.StateFollower}, fails: true},
		{name: "self steps down to none", lead: self, isLeader: true, ss: &raft.SoftState{RaftState: raft.StateFollower}, fails: true},
		{name: "self deposed by other", lead: self, isLeader: true, ss: &raft.SoftState{Lead: other, RaftState: raft.StateFollower}, fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := &Node{cfg: Config{ID: self}, storage: raft.NewMemoryStorage(), waiters: map[uint64]chan outcome{},
				lead: tc.lead, isLeader: tc.isLeader}
			_, ch := n.register()
			if err := n.apply(applyBatch{softState: tc.ss}); err != nil {
				t.Fatal(err)
			}
			select {
			case out := <-ch:
				if !tc.fails {
					t.Fatalf("waiter failed with %v, want it left waiting", out.err)
				}
				if !errors.Is(out.err, ErrLeadershipLost) {
					t.Fatalf("waiter got %v, want ErrLeadershipLost", out.err)
				}
			default:
				if tc.fails {
					t.Fatal("waiter left waiting, want ErrLeadershipLost")
				}
			}
		})
	}
}
