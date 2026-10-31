package raftmetrics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

type fixed raftnode.Status

func (f fixed) Status() raftnode.Status { return raftnode.Status(f) }

func scrape(src Source, names map[uint64]string) string {
	var b bytes.Buffer
	Writer("1", "shard1-node1", src, names)(&b)
	return b.String()
}

func TestLeaderOutput(t *testing.T) {
	out := scrape(fixed{
		ID: 1, Leader: 1, Term: 4, Role: raft.StateLeader, LastIndex: 120, Commit: 118, Applied: 118,
		Followers: []raftnode.FollowerProgress{
			{ID: 2, Match: 120, RecentActive: true},
			{ID: 3, Match: 95, RecentActive: false},
		},
	}, NamesFromAddrs(map[uint64]string{1: "shard1-node1:7000", 2: "shard1-node2:7000", 3: "shard1-node3:7000"}))

	for _, want := range []string{
		"# TYPE seatsafe_raft_is_leader gauge",
		`seatsafe_raft_is_leader{group="1",node="shard1-node1"} 1`,
		`seatsafe_raft_leader_id{group="1",node="shard1-node1"} 1`,
		`seatsafe_raft_term{group="1",node="shard1-node1"} 4`,
		`seatsafe_raft_last_index{group="1",node="shard1-node1"} 120`,
		`seatsafe_raft_commit_index{group="1",node="shard1-node1"} 118`,
		`seatsafe_raft_applied_index{group="1",node="shard1-node1"} 118`,
		`seatsafe_raft_follower_match_index{group="1",node="shard1-node1",follower="shard1-node3"} 95`,
		`seatsafe_raft_follower_lag_entries{group="1",node="shard1-node1",follower="shard1-node2"} 0`,
		`seatsafe_raft_follower_lag_entries{group="1",node="shard1-node1",follower="shard1-node3"} 25`,
		`seatsafe_raft_follower_recent_active{group="1",node="shard1-node1",follower="shard1-node3"} 0`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing line %q in:\n%s", want, out)
		}
	}
}

func TestFollowerOutput(t *testing.T) {
	out := scrape(fixed{ID: 2, Leader: 1, Term: 4, Role: raft.StateFollower, LastIndex: 120, Commit: 118, Applied: 117}, nil)
	if !strings.Contains(out, `seatsafe_raft_is_leader{group="1",node="shard1-node1"} 0`+"\n") {
		t.Errorf("follower not reported as non-leader:\n%s", out)
	}
	if strings.Contains(out, "seatsafe_raft_follower_") {
		t.Errorf("follower exported per-follower metrics:\n%s", out)
	}
}

func TestUnknownFollowerFallsBackToID(t *testing.T) {
	out := scrape(fixed{Role: raft.StateLeader, LastIndex: 5, Followers: []raftnode.FollowerProgress{{ID: 7, Match: 5}}}, nil)
	if !strings.Contains(out, `follower="7"} 5`) {
		t.Errorf("follower without a name should be labelled by Raft ID:\n%s", out)
	}
}

// value returns a metric line's value for the given name and follower.
func value(t *testing.T, out, name, follower string) uint64 {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, name+"{") && strings.Contains(line, `follower="`+follower+`"`) {
			var v uint64
			fmt.Sscan(line[strings.LastIndex(line, " ")+1:], &v)
			return v
		}
	}
	t.Fatalf("no %s for follower %s in:\n%s", name, follower, out)
	return 0
}

type countSM struct {
	mu sync.Mutex
	n  int
}

func (s *countSM) Apply(uint64, []byte) any  { s.mu.Lock(); s.n++; s.mu.Unlock(); return nil }
func (s *countSM) Snapshot() ([]byte, error) { return nil, nil }
func (s *countSM) Restore([]byte) error      { return nil }

// On a real 3-node group: an isolated follower's lag grows by the number of
// entries it misses, and drops back to 0 once it catches up.
func TestLagOnRealCluster(t *testing.T) {
	nw := raftnode.NewNetwork()
	peers := []uint64{1, 2, 3}
	quiet := &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	nodes := map[uint64]*raftnode.Node{}
	for _, id := range peers {
		n, err := raftnode.Start(raftnode.Config{ID: id, Peers: peers, SM: &countSM{}, Transport: nw,
			TickInterval: 10 * time.Millisecond, Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		nw.Add(id, n)
		nodes[id] = n
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		nw.Close()
	}()

	var lead uint64
	for deadline := time.Now().Add(5 * time.Second); lead == 0; time.Sleep(10 * time.Millisecond) {
		for id, n := range nodes {
			if n.Status().IsLeader() {
				lead = id
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
	}
	propose := func(k int) {
		for i := range k {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if _, err := nodes[lead].Propose(ctx, []byte(fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
			cancel()
		}
	}
	names := map[uint64]string{1: "n1", 2: "n2", 3: "n3"}
	lagged := lead%3 + 1
	name := names[lagged]
	waitLag := func(want func(uint64) bool, what string) uint64 {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			v := value(t, scrape(nodes[lead], names), "seatsafe_raft_follower_lag_entries", name)
			if want(v) {
				return v
			}
			if time.Now().After(deadline) {
				t.Fatalf("lag for %s stayed at %d, want %s", name, v, what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	propose(5)
	waitLag(func(v uint64) bool { return v == 0 }, "0 while connected")

	nw.Isolate(lagged)
	defer nw.Heal()
	propose(30) // the other two still form a majority
	got := waitLag(func(v uint64) bool { return v >= 30 }, ">= 30 while isolated")
	t.Logf("isolated follower lag: %d entries", got)

	nw.Heal()
	waitLag(func(v uint64) bool { return v == 0 }, "0 after catching up")
}
