package raftnode

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
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
