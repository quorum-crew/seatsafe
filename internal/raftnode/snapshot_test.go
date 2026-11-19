package raftnode

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
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

// slowSnapSM stands in for a big state machine without SnapshotCapturer: its
// whole Snapshot runs on the apply goroutine (seatsm took 1 to 2 s at the
// 1M-seat benchmark).
type slowSnapSM struct {
	logSM
	slow atomic.Bool
}

func (s *slowSnapSM) Snapshot() ([]byte, error) {
	if s.slow.Load() {
		time.Sleep(time.Second)
	}
	return s.logSM.Snapshot()
}

// Taking a snapshot must not stop the Raft loop. The leader's snapshots here
// take ten election timeouts; if they ran on the loop, it would send no
// heartbeats meanwhile and the followers would elect someone else.
func TestSlowSnapshotKeepsLeader(t *testing.T) {
	net := NewNetwork()
	t.Cleanup(net.Close)
	peers := []uint64{1, 2, 3}
	nodes := map[uint64]*Node{}
	sms := map[uint64]*slowSnapSM{}
	for _, id := range peers {
		sms[id] = &slowSnapSM{}
		n, err := Start(Config{ID: id, Peers: peers, SM: sms[id], Transport: net,
			TickInterval: 10 * time.Millisecond, Logger: quiet, SnapshotEntries: 20, CatchUpEntries: 5})
		if err != nil {
			t.Fatal(err)
		}
		net.Add(id, n)
		nodes[id] = n
		t.Cleanup(n.Stop)
	}
	c := &cluster{t: t, nodes: nodes}
	lead := c.waitLeader()
	term := nodes[lead].Status().Term
	sms[lead].slow.Store(true)

	// Entry 20 or so triggers a snapshot on every node. Proposals keep
	// committing while it runs; their results wait for the apply goroutine.
	errc := make(chan error, 40)
	for i := range 40 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := nodes[lead].Propose(ctx, []byte(fmt.Sprintf("e-%d", i)))
			errc <- err
		}()
	}
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		for id, n := range nodes {
			st := n.Status()
			if st.Term != term || st.Leader != lead {
				t.Fatalf("node %d: term %d leader %d during a snapshot, want term %d leader %d", id, st.Term, st.Leader, term, lead)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for range 40 {
		if err := <-errc; err != nil {
			t.Fatalf("proposal failed: %v", err)
		}
	}
	if st := nodes[lead].Status(); st.SnapshotIndex == 0 {
		t.Fatal("no snapshot was taken")
	}
}

// capSM copies its entries to capture and encodes slowly, like seatsm at
// benchmark scale (a 175 ms copy, then a 1 s encode).
type capSM struct {
	logSM
	encodeDelay time.Duration
	encodes     atomic.Int32
}

func (s *capSM) CaptureSnapshot() func() ([]byte, error) {
	s.mu.Lock()
	entries := append([]string(nil), s.entries...)
	s.mu.Unlock()
	return func() ([]byte, error) {
		time.Sleep(s.encodeDelay)
		s.encodes.Add(1)
		return json.Marshal(entries)
	}
}

// Encoding a snapshot must not hold up applying: with a 1 s encode every 20
// entries, proposals must keep completing quickly. Encoding on the apply
// goroutine made every Propose behind it wait, which cut benchmark throughput
// by more than half.
func TestSlowSnapshotEncodeDoesNotStallApply(t *testing.T) {
	net := NewNetwork()
	t.Cleanup(net.Close)
	peers := []uint64{1, 2, 3}
	nodes := map[uint64]*Node{}
	sms := map[uint64]*capSM{}
	for _, id := range peers {
		sms[id] = &capSM{encodeDelay: time.Second}
		n, err := Start(Config{ID: id, Peers: peers, SM: sms[id], Transport: net,
			TickInterval: 10 * time.Millisecond, Logger: quiet, SnapshotEntries: 20, CatchUpEntries: 5})
		if err != nil {
			t.Fatal(err)
		}
		net.Add(id, n)
		nodes[id] = n
		t.Cleanup(n.Stop)
	}
	c := &cluster{t: t, nodes: nodes}
	lead := c.waitLeader()

	var slowest time.Duration
	for i := range 100 {
		start := time.Now()
		propose(t, nodes[lead], fmt.Sprintf("e-%d", i))
		slowest = max(slowest, time.Since(start))
	}
	if slowest > 400*time.Millisecond {
		t.Fatalf("slowest proposal took %v while snapshots encoded for 1 s each", slowest)
	}
	deadline := time.Now().Add(5 * time.Second)
	for sms[lead].encodes.Load() == 0 || nodes[lead].Status().SnapshotIndex == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no snapshot was finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("slowest proposal %v; snapshot at %d", slowest, nodes[lead].Status().SnapshotIndex)
}
