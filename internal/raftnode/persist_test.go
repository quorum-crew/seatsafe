package raftnode

import (
	"fmt"
	"testing"
)

// A follower that crashes misses writes, then restarts from its Pebble log and
// catches up, ending with exactly the same entries as everyone else.
func TestRestartedNodeKeepsLog(t *testing.T) {
	c := newDurableCluster(t, 3)
	lead := c.waitLeader()
	for i := range 50 {
		propose(t, c.nodes[lead], fmt.Sprintf("before-%d", i))
	}

	follower := lead%3 + 1
	c.stop(follower)
	for i := range 50 {
		propose(t, c.nodes[lead], fmt.Sprintf("while-down-%d", i))
	}

	c.start(follower) // empty state machine: rebuilt from disk + catch-up
	c.waitApplied(100)
}

// All three nodes stop at once (say, the whole cluster loses power). On
// restart they rebuild their state from disk, elect a leader, and continue.
func TestWholeClusterRestart(t *testing.T) {
	c := newDurableCluster(t, 3)
	lead := c.waitLeader()
	for i := range 30 {
		propose(t, c.nodes[lead], fmt.Sprintf("e-%d", i))
	}
	c.waitApplied(30)

	for _, id := range c.peers {
		c.stop(id)
	}
	for _, id := range c.peers {
		c.start(id)
	}
	c.waitApplied(30) // replayed from disk

	lead = c.waitLeader()
	if got := propose(t, c.nodes[lead], "after-restart"); got != 31 {
		t.Fatalf("first entry after restart applied at position %d, want 31", got)
	}
	c.waitApplied(31)
}
