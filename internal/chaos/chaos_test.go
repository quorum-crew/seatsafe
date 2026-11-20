package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDocker answers /metrics for a cluster where node `leader` of every
// shard leads, and records every other command.
type fakeDocker struct {
	mu      sync.Mutex
	leader  int
	applied map[string]uint64 // per service; default 100
	down    map[string]bool   // containers whose /metrics fails
	cmds    []string
}

func (d *fakeDocker) Run(_ context.Context, args ...string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(args) >= 4 && args[0] == "exec" && args[2] == "wget" {
		c := args[1]
		if d.down[c] {
			return "", fmt.Errorf("container %s is not running", c)
		}
		svc := strings.TrimSuffix(strings.TrimPrefix(c, "seatsafe-"), "-1")
		var shard, node int
		fmt.Sscanf(svc, "shard%d-node%d", &shard, &node)
		applied := uint64(100)
		if a, ok := d.applied[svc]; ok {
			applied = a
		}
		isLeader := 0
		if node == d.leader {
			isLeader = 1
		}
		return fmt.Sprintf("# HELP x\nseatsafe_raft_is_leader{shard=\"%d\"} %d\nseatsafe_raft_leader_id{shard=\"%d\"} %d\n"+
			"seatsafe_raft_applied_index{shard=\"%d\"} %d\nseatsafe_raft_follower_lag_entries{follower=\"x\"} 3\n",
			shard, isLeader, shard, d.leader, shard, applied), nil
	}
	d.cmds = append(d.cmds, strings.Join(args, " "))
	return "", nil
}

func (d *fakeDocker) commands() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.cmds)
}

func newInjector(d *fakeDocker) *Injector {
	return &Injector{C: &Cluster{Docker: d, Project: "seatsafe", Files: []string{"docker-compose.yml"}}, Rand: rand.New(rand.NewPCG(1, 2))}
}

func TestMetricsParsing(t *testing.T) {
	d := &fakeDocker{leader: 2}
	c := &Cluster{Docker: d, Project: "seatsafe"}
	m, err := c.Metrics(context.Background(), "shard1-node3")
	if err != nil {
		t.Fatal(err)
	}
	if m["seatsafe_raft_leader_id"] != 2 || m["seatsafe_raft_applied_index"] != 100 || m["seatsafe_raft_is_leader"] != 0 {
		t.Fatalf("parsed %v", m)
	}
	if _, ok := m["seatsafe_raft_follower_lag_entries"]; ok {
		t.Fatal("per-follower series should be skipped")
	}
	if l := c.Leader(context.Background(), 1); l != 2 {
		t.Fatalf("leader %d, want 2", l)
	}
}

// Each fault kind issues the right docker commands, on the right targets,
// and Heal undoes it.
func TestInjectAndHeal(t *testing.T) {
	cases := []struct {
		kind         Kind
		inject, heal []string // prefixes of the recorded commands
		targets      func(f *Fault) bool
	}{
		{KillLeader, []string{"kill -s KILL seatsafe-shard"}, []string{"start seatsafe-shard"},
			func(f *Fault) bool { return len(f.Targets) == 1 && strings.HasSuffix(f.Targets[0], "-node2") }},
		{KillFollower, []string{"kill -s KILL"}, []string{"start"},
			func(f *Fault) bool { return len(f.Targets) == 1 && !strings.HasSuffix(f.Targets[0], "-node2") }},
		{PauseLeader, []string{"pause seatsafe-shard"}, []string{"unpause seatsafe-shard"},
			func(f *Fault) bool { return strings.HasSuffix(f.Targets[0], "-node2") }},
		{IsolateLeader, []string{"network disconnect seatsafe_seatsafe seatsafe-shard"}, []string{"network connect --alias shard"},
			func(f *Fault) bool { return strings.HasSuffix(f.Targets[0], "-node2") }},
		{IsolateFollower, []string{"network disconnect"}, []string{"network connect --alias"},
			func(f *Fault) bool { return !strings.HasSuffix(f.Targets[0], "-node2") }},
		{QuorumLoss, []string{"kill -s KILL"}, []string{"start"},
			func(f *Fault) bool { return len(f.Targets) == 2 && strings.HasSuffix(f.Targets[0], "-node2") }},
		{DelayShard, []string{"exec --privileged", "exec --privileged", "exec --privileged"},
			[]string{"exec --privileged", "exec --privileged", "exec --privileged"},
			func(f *Fault) bool { return len(f.Targets) == 3 && strings.HasPrefix(f.Detail, "delay ") }},
		{KillRouter, []string{"kill -s KILL seatsafe-router-1"}, []string{"start seatsafe-router-1"},
			func(f *Fault) bool { return f.Shard == 0 && f.Targets[0] == "router" }},
		{RollingRestart, []string{"restart -t 0", "restart -t 0", "restart -t 0"}, nil,
			func(f *Fault) bool { return len(f.Targets) == 3 }},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			d := &fakeDocker{leader: 2}
			f, err := newInjector(d).Inject(context.Background(), tc.kind)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.targets(f) {
				t.Fatalf("targets %v (%s)", f.Targets, f)
			}
			checkPrefixes(t, "inject", d.commands(), tc.inject)
			before := len(d.commands())
			if err := f.Heal(context.Background()); err != nil {
				t.Fatal(err)
			}
			checkPrefixes(t, "heal", d.commands()[before:], tc.heal)
		})
	}
}

func checkPrefixes(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: commands %q, want %d", what, got, len(want))
	}
	for i := range want {
		if !strings.HasPrefix(got[i], want[i]) {
			t.Fatalf("%s: command %q, want prefix %q", what, got[i], want[i])
		}
	}
}

// With no leader anywhere, a leader-targeted fault is not injected.
func TestNoLeaderNoFault(t *testing.T) {
	d := &fakeDocker{leader: 0}
	f, err := newInjector(d).Inject(context.Background(), KillLeader)
	if err == nil || f != nil || len(d.commands()) != 0 {
		t.Fatalf("fault %v, err %v, commands %v", f, err, d.commands())
	}
}

func TestWaitHealed(t *testing.T) {
	d := &fakeDocker{leader: 1}
	c := &Cluster{Docker: d, Project: "seatsafe"}
	if _, bad, err := c.WaitHealed(context.Background(), time.Second); err != nil {
		t.Fatalf("healthy cluster: %v %v", err, bad)
	}
	d.applied = map[string]uint64{"shard2-node3": 90}
	_, bad, err := c.WaitHealed(context.Background(), 600*time.Millisecond)
	if err == nil || len(bad) != 1 || bad[0].Shard != 2 || !strings.Contains(bad[0].Err, "applied indexes differ") {
		t.Fatalf("lagging replica: %v %v", err, bad)
	}
	d.applied = nil
	d.down = map[string]bool{"seatsafe-shard3-node1-1": true}
	if _, bad, err := c.WaitHealed(context.Background(), 600*time.Millisecond); err == nil || bad[0].Shard != 3 {
		t.Fatalf("down replica: %v %v", err, bad)
	}
}

// agreedLeader ignores what the faulted nodes say: an isolated old leader
// still believes it leads.
func TestAgreedLeader(t *testing.T) {
	d := &fakeDocker{leader: 2}
	c := &Cluster{Docker: d, Project: "seatsafe"}
	if l := c.agreedLeader(context.Background(), 1, nil); l != 2 {
		t.Fatalf("no faults: leader %d", l)
	}
	// Node 2 is the faulted one, and the others still name it: no failover yet.
	if l := c.agreedLeader(context.Background(), 1, []string{"shard1-node2"}); l != 0 {
		t.Fatalf("faulted leader counted as the new one: %d", l)
	}
	d.leader = 3
	if l := c.agreedLeader(context.Background(), 1, []string{"shard1-node2"}); l != 3 {
		t.Fatalf("after failover: leader %d", l)
	}
	if !c.caughtUp(context.Background(), 1, 100) || c.caughtUp(context.Background(), 1, 101) {
		t.Fatal("caughtUp wrong around the applied index")
	}
}

func TestParseKinds(t *testing.T) {
	if k, err := ParseKinds("all"); err != nil || len(k) != len(AllKinds) {
		t.Fatal(k, err)
	}
	if k, err := ParseKinds("kill-leader, pause-leader"); err != nil || len(k) != 2 {
		t.Fatal(k, err)
	}
	if _, err := ParseKinds("kill-everything"); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestSummarize(t *testing.T) {
	s := Summarize([]Result{
		{Run: 1, Kind: KillLeader, Verdict: "PASS", Failover: 2 * time.Second, Recovery: time.Second, Elections: 1, P99: 50 * time.Millisecond, Max: 800 * time.Millisecond},
		{Run: 2, Kind: KillLeader, Verdict: "PASS", Failover: 4 * time.Second, Recovery: 3 * time.Second, Elections: 2, P99: 100 * time.Millisecond, Max: 900 * time.Millisecond},
		{Run: 3, Kind: KillLeader, Verdict: "FAIL", Fault: "kill-leader shard 1", Reason: "correctness audit failed"},
		{Run: 4, Kind: PauseLeader, Verdict: "SKIP", Failover: -1, Recovery: -1},
		{Run: 5, Kind: None, Verdict: "PASS", Failover: -1, Recovery: -1, Elections: 0, P99: 15 * time.Millisecond, Max: 42 * time.Millisecond},
	})
	for _, want := range []string{"5 runs: 3 PASS, 1 FAIL, 1 SKIP", "| none | 1 | 1 | 0 | 0 | - | - | - | - | 0.0 | 15 ms | 42 ms |", "| kill-leader | 3 | 2 | 1 | 0 | 4.0 s | 4.0 s | 3.0 s | 3.0 s | 1.5 | 100 ms | 900 ms |", "| pause-leader | 1 | 0 | 0 | 1 | - | - | - | - | - | - | - |", "run 3: kill-leader shard 1: correctness audit failed"} {
		if !strings.Contains(s, want) {
			t.Fatalf("summary missing %q:\n%s", want, s)
		}
	}
}

func TestClientLatencyParsing(t *testing.T) {
	out := "closed flash, 30s window\n  latency (ms)            count       p50       p90       p99     p99.9       max\n" +
		"  all                     88899   4587.52   5091.33   6176.77   6946.81   7766.02\n  CONFIRMED  18115 573.95 4243.45 4886.53 5861.38 6549.50\n"
	p99, max := clientLatency(out)
	if p99 != 6176770*time.Microsecond || max != 7766020*time.Microsecond {
		t.Fatalf("p99 %v max %v", p99, max)
	}
	if p99, _ := clientLatency("no table"); p99 != -1 {
		t.Fatal("missing table should give -1")
	}
}

func TestNoneInjectsNothing(t *testing.T) {
	d := &fakeDocker{leader: 2}
	f, err := newInjector(d).Inject(context.Background(), None)
	if err != nil || f == nil || len(d.commands()) != 0 || f.Heal(context.Background()) != nil {
		t.Fatalf("none: %v %v %v", f, err, d.commands())
	}
	if k, err := ParseKinds("none,kill-leader"); err != nil || len(k) != 2 {
		t.Fatal(k, err)
	}
	if k, _ := ParseKinds("all"); slices.Contains(k, None) {
		t.Fatal("all should not include none")
	}
}
