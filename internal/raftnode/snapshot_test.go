package raftnode

import (
	"fmt"
	"testing"
	"time"
)

func proposeN(t *testing.T, n *Node, prefix string, count int) {
	t.Helper()
	for i := range count {
		propose(t, n, fmt.Sprintf("%s-%d", prefix, i))
	}
}

// Nodes snapshot every SnapshotEntries applied entries and compact their log,
// keeping CatchUpEntries entries before the snapshot (#11).
func TestSnapshotsCompactTheLog(t *testing.T) {
	c := startClusterWith(t, 3, false, 50, 10)
	lead := c.waitLeader()
	proposeN(t, c.nodes[lead], "e", 200)
	c.waitApplied(200)

	for id, n := range c.nodes {
		st := n.Status()
		first, err := n.storage.FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if st.SnapshotIndex < 150 {
			t.Errorf("node %d: newest snapshot at %d, want >= 150 after 200 entries with SnapshotEntries=50", id, st.SnapshotIndex)
		}
		// Compacted up to snapshot-10, so the log starts just after that.
		if want := st.SnapshotIndex - 10 + 1; first != want {
			t.Errorf("node %d: log starts at %d, want %d (snapshot %d minus 10 catch-up entries)", id, first, want, st.SnapshotIndex)
		}
	}
}

// A follower that falls behind the compacted log can only catch up from a
// snapshot: the leader sends one, the follower restores it and ends up with
// exactly the same state as everyone else.
func TestLaggingFollowerCatchesUpFromSnapshot(t *testing.T) {
	c := startClusterWith(t, 3, false, 20, 5)
	lead := c.waitLeader()
	follower := lead%3 + 1
	proposeN(t, c.nodes[lead], "before", 10)
	c.waitApplied(10)

	c.net.Isolate(follower)
	proposeN(t, c.nodes[lead], "while-cut-off", 100) // leader compacts far past the follower
	c.net.Heal()
	c.waitApplied(110)

	if _, restores := c.sms[follower].counts(); restores == 0 {
		t.Fatal("the lagging follower caught up without restoring a snapshot; the test did not exercise the snapshot path")
	}
}

// A restarted node restores its newest snapshot and replays only the entries
// after it, instead of the whole log.
func TestRestartReplaysOnlyAfterSnapshot(t *testing.T) {
	c := startClusterWith(t, 3, true, 50, 10)
	lead := c.waitLeader()
	proposeN(t, c.nodes[lead], "e", 205)
	c.waitApplied(205)

	victim := lead%3 + 1
	snapAt := c.nodes[victim].Status().SnapshotIndex
	c.stop(victim)
	c.start(victim) // fresh, empty state machine
	c.waitApplied(205)

	applies, restores := c.sms[victim].counts()
	if restores != 1 {
		t.Fatalf("restarted node restored %d snapshots, want 1", restores)
	}
	if applies >= 205 || applies > 60 {
		t.Fatalf("restarted node re-applied %d entries (snapshot was at index %d); want only those after the snapshot", applies, snapAt)
	}
	t.Logf("restart: restored snapshot at %d, then applied %d entries", snapAt, applies)
}

// A follower that received a snapshot from the leader, persisted it, and then
// restarts must come back to the same state: a stale log tail must not be
// reloaded after the snapshot.
func TestRestartAfterReceivingSnapshot(t *testing.T) {
	c := startClusterWith(t, 3, true, 20, 5)
	lead := c.waitLeader()
	follower := lead%3 + 1
	proposeN(t, c.nodes[lead], "a", 10)
	c.waitApplied(10)

	c.net.Isolate(follower)
	proposeN(t, c.nodes[lead], "b", 100)
	c.net.Heal()
	c.waitApplied(110)
	if _, restores := c.sms[follower].counts(); restores == 0 {
		t.Fatal("follower did not receive a snapshot")
	}

	c.stop(follower)
	c.start(follower)
	c.waitApplied(110)

	// And the group keeps going with the restarted follower in it.
	lead = c.waitLeader()
	propose(t, c.nodes[lead], "after-restart")
	deadline := time.Now().Add(5 * time.Second)
	for len(c.sms[follower].snapshot()) < 111 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	c.waitApplied(111)
}
