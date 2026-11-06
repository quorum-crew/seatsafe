package raftnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/raftstore"
	"go.etcd.io/raft/v3"
)

// logSM is a fake state machine that records every applied payload in order.
// Apply returns the entry's position in that order (1-based).
type logSM struct {
	mu       sync.Mutex
	entries  []string
	applies  int // Apply calls since this SM was created
	restores int // Restore calls since this SM was created
}

// Apply appends data and returns its position. Data already in the log is
// not added again, like seatsm's dedup by request ID, so the propose helper
// can retry after a leader change without double-counting.
func (s *logSM) Apply(_ uint64, data []byte) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applies++
	if i := slices.Index(s.entries, string(data)); i >= 0 {
		return i + 1
	}
	s.entries = append(s.entries, string(data))
	return len(s.entries)
}

func (s *logSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.entries)
}

func (s *logSM) Restore(data []byte) error {
	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
	s.restores++
	return nil
}

func (s *logSM) counts() (applies, restores int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applies, s.restores
}

func (s *logSM) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.entries)
}

type cluster struct {
	t       *testing.T
	net     *Network
	peers   []uint64
	durable bool
	// Snapshot settings for every node (0 = raftnode's defaults).
	snapEvery, catchUp uint64
	nodes              map[uint64]*Node
	sms                map[uint64]*logSM
	dirs               map[uint64]string
	stores             map[uint64]*raftstore.Pebble
}

// newCluster starts size nodes that keep their logs in memory only.
func newCluster(t *testing.T, size int) *cluster { return startCluster(t, size, false) }

// newDurableCluster starts size nodes that persist their logs in Pebble, so
// they can be stopped and restarted.
func newDurableCluster(t *testing.T, size int) *cluster { return startCluster(t, size, true) }

func startCluster(t *testing.T, size int, durable bool) *cluster {
	return startClusterWith(t, size, durable, 0, 0)
}

// startClusterWith also sets how often nodes snapshot and how many entries
// they keep before a snapshot.
func startClusterWith(t *testing.T, size int, durable bool, snapEvery, catchUp uint64) *cluster {
	t.Helper()
	c := &cluster{
		t: t, net: NewNetwork(), durable: durable, snapEvery: snapEvery, catchUp: catchUp,
		nodes: map[uint64]*Node{}, sms: map[uint64]*logSM{},
		dirs: map[uint64]string{}, stores: map[uint64]*raftstore.Pebble{},
	}
	for id := uint64(1); id <= uint64(size); id++ {
		c.peers = append(c.peers, id)
		c.dirs[id] = t.TempDir()
	}
	for _, id := range c.peers {
		c.start(id)
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
		c.net.Close()
	})
	return c
}

var quiet = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

// start starts (or restarts) node id with an empty state machine.
func (c *cluster) start(id uint64) {
	c.t.Helper()
	cfg := Config{
		ID: id, Peers: c.peers, SM: &logSM{}, Transport: c.net,
		TickInterval: 10 * time.Millisecond, Logger: quiet,
		SnapshotEntries: c.snapEvery, CatchUpEntries: c.catchUp,
	}
	if c.durable {
		st, err := raftstore.Open(c.dirs[id])
		if err != nil {
			c.t.Fatal(err)
		}
		cfg.Store = st
		c.stores[id] = st
	}
	n, err := Start(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.Add(id, n)
	c.nodes[id], c.sms[id] = n, cfg.SM.(*logSM)
}

// stop stops node id, as a crash or restart would, and closes its store.
func (c *cluster) stop(id uint64) {
	c.t.Helper()
	c.nodes[id].Stop()
	if st := c.stores[id]; st != nil {
		if err := st.Close(); err != nil {
			c.t.Fatal(err)
		}
		delete(c.stores, id)
	}
	delete(c.nodes, id)
	delete(c.sms, id)
}

// waitLeader waits until every node not in skip agrees on one leader, and that
// leader is not in skip either.
func (c *cluster) waitLeader(skip ...uint64) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var lead uint64
		agreed := true
		for id, n := range c.nodes {
			if slices.Contains(skip, id) {
				continue
			}
			st := n.Status()
			if st.Leader == 0 || (lead != 0 && st.Leader != lead) {
				agreed = false
				break
			}
			lead = st.Leader
		}
		if agreed && lead != 0 && !slices.Contains(skip, lead) && c.nodes[lead].Status().IsLeader() {
			return lead
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected within 5s")
	return 0
}

// waitApplied waits until every node's state machine holds want entries, then
// checks that all of them applied the same entries in the same order.
func (c *cluster) waitApplied(want int) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		done := true
		for _, sm := range c.sms {
			if len(sm.snapshot()) < want {
				done = false
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			for id, sm := range c.sms {
				c.t.Logf("node %d applied %d entries", id, len(sm.snapshot()))
			}
			c.t.Fatalf("not every node applied %d entries within 10s", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var ref []string
	for id, sm := range c.sms {
		got := sm.snapshot()
		if ref == nil {
			ref = got
		} else if !slices.Equal(got, ref) {
			c.t.Fatalf("node %d's applied entries differ from another node's", id)
		}
	}
}

// propose proposes data through n and returns its position in the log. When
// the leader changes under it (slow fsyncs under -race with 10 ms ticks make
// that happen now and then) it retries, as a real client does; logSM ignores
// data it already holds, so a retry of an entry that did commit is harmless.
func propose(t *testing.T, n *Node, data string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		res, err := n.Propose(ctx, []byte(data))
		if err == nil {
			return res.(int)
		}
		if ctx.Err() == nil && (errors.Is(err, ErrLeadershipLost) || errors.Is(err, raft.ErrProposalDropped)) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		t.Fatalf("propose %q: %v", data, err)
	}
}

func TestElectsLeaderAndReplicates(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.nodes[c.waitLeader()]

	// 1,000 concurrent proposals, as a burst of bookings would arrive.
	const workers, each = 10, 100
	results := make(chan int, workers*each)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := leader.Propose(ctx, []byte(fmt.Sprintf("w%d-%d", w, i)))
				cancel()
				if err != nil {
					t.Errorf("propose: %v", err) // t.Fatal is not allowed off the test goroutine
					return
				}
				results <- res.(int)
			}
		}()
	}
	wg.Wait()
	close(results)
	if t.Failed() {
		return
	}

	// Each proposal must get back its own apply result: positions 1..1000, once each.
	var got []int
	for r := range results {
		got = append(got, r)
	}
	slices.Sort(got)
	for i, r := range got {
		if r != i+1 {
			t.Fatalf("results are not exactly 1..%d (position %d holds %d)", workers*each, i, r)
		}
	}
	c.waitApplied(workers * each)
}

func TestFollowerForwardsProposal(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.waitLeader()
	follower := lead%3 + 1
	if got := propose(t, c.nodes[follower], "via-follower"); got != 1 {
		t.Fatalf("result = %d, want 1", got)
	}
	c.waitApplied(1)
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader()
	for i := range 10 {
		propose(t, c.nodes[old], fmt.Sprintf("before-%d", i))
	}

	// Cut the leader off. The other two elect a new leader and keep going.
	c.net.Isolate(old)
	lead := c.waitLeader(old)
	for i := range 10 {
		propose(t, c.nodes[lead], fmt.Sprintf("during-%d", i))
	}

	// Heal: the old leader rejoins, steps down, and catches up.
	c.net.Heal()
	c.waitApplied(20)
	if st := c.nodes[old].Status(); st.IsLeader() && st.Term <= c.nodes[lead].Status().Term {
		t.Fatalf("old leader %d still thinks it leads in term %d", old, st.Term)
	}
}

func TestStartValidatesConfig(t *testing.T) {
	net := NewNetwork()
	defer net.Close()
	for name, cfg := range map[string]Config{
		"zero ID":         {ID: 0, Peers: []uint64{1}, SM: &logSM{}, Transport: net},
		"ID not in peers": {ID: 4, Peers: []uint64{1, 2, 3}, SM: &logSM{}, Transport: net},
		"no SM":           {ID: 1, Peers: []uint64{1}, Transport: net},
		"no transport":    {ID: 1, Peers: []uint64{1}, SM: &logSM{}},
	} {
		if _, err := Start(cfg); err == nil {
			t.Errorf("%s: Start succeeded, want an error", name)
		}
	}
}
