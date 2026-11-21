package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/linearize"
)

// fakeRemote is a remote cluster of three zones with one node of every group
// in each, and a router in zone a. Node 2 leads every group until it is
// killed, paused or cut off; then node 3 does.
type fakeRemote struct {
	mu     sync.Mutex
	calls  []string
	gone   map[string]bool // services killed, paused or partitioned
	load   func(dir string, args []string) (string, error)
	resets int
}

func (r *fakeRemote) note(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf(format, args...))
}

func (r *fakeRemote) set(gone bool, services ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gone == nil {
		r.gone = map[string]bool{}
	}
	for _, s := range services {
		r.gone[s] = gone
	}
}

func (r *fakeRemote) Metrics(_ context.Context, service string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gone[service] {
		return "", fmt.Errorf("%s does not answer", service)
	}
	var shard, node int
	if _, err := fmt.Sscanf(service, "coord-%d", &node); err != nil {
		fmt.Sscanf(service, "shard%d-node%d", &shard, &node)
	}
	leader := 2
	if r.gone[Service(shard, 2)] {
		leader = 3
	}
	isLeader := 0
	if node == leader {
		isLeader = 1
	}
	return fmt.Sprintf("seatsafe_raft_is_leader %d\nseatsafe_raft_leader_id %d\nseatsafe_raft_applied_index 100\nseatsafe_raft_term 4\n", isLeader, leader), nil
}

func (r *fakeRemote) Kill(_ context.Context, s []string) error {
	r.note("kill %s", strings.Join(s, ","))
	r.set(true, s...)
	return nil
}

func (r *fakeRemote) Start(_ context.Context, s []string) error {
	r.note("start %s", strings.Join(s, ","))
	r.set(false, s...)
	return nil
}

func (r *fakeRemote) Pause(_ context.Context, s string) error {
	r.note("pause %s", s)
	r.set(true, s)
	return nil
}

func (r *fakeRemote) Unpause(_ context.Context, s string) error {
	r.note("unpause %s", s)
	r.set(false, s)
	return nil
}

func (r *fakeRemote) Partition(_ context.Context, s string) error {
	r.note("partition %s", s)
	r.set(true, s)
	return nil
}

func (r *fakeRemote) Rejoin(_ context.Context, s string) error {
	r.note("rejoin %s", s)
	r.set(false, s)
	return nil
}

func (r *fakeRemote) Reset(context.Context) error {
	r.note("reset")
	r.mu.Lock()
	r.resets++
	r.gone = nil
	r.mu.Unlock()
	return nil
}

func (r *fakeRemote) Logs(context.Context) (string, error) { return "the logs\n", nil }

func (r *fakeRemote) Load(_ context.Context, dir string, args []string) (string, error) {
	r.note("load %s", strings.Join(args, " "))
	return r.load(dir, args)
}

func (r *fakeRemote) Zones() map[string][]string {
	z := map[string][]string{}
	for i, name := range []string{"us-west-2a", "us-west-2b", "us-west-2c"} {
		for _, g := range GroupIDs() {
			z[name] = append(z[name], Service(g, i+1))
		}
	}
	z["us-west-2a"] = append(z["us-west-2a"], "router-1")
	return z
}

func (r *fakeRemote) did() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// On a remote cluster every fault goes through the Remote, on the leader the
// nodes report, and healing undoes it. No docker command is run here.
func TestRemoteFaults(t *testing.T) {
	for _, c := range []struct {
		kind   Kind
		target Target
		want   []string
	}{
		{KillLeader, TargetShard, []string{"kill shard1-node2", "start shard1-node2"}},
		{KillFollower, TargetCoordinator, []string{"kill coord-1", "start coord-1"}},
		{PauseLeader, TargetCoordinator, []string{"pause coord-2", "unpause coord-2"}},
		{Partition, TargetShard, []string{"partition shard1-node2", "rejoin shard1-node2"}},
		{Partition, TargetCoordinator, []string{"partition coord-2", "rejoin coord-2"}},
		{IsolateFollower, TargetShard, []string{"partition shard1-node1", "rejoin shard1-node1"}},
		{QuorumLoss, TargetShard, []string{"kill shard1-node2,shard1-node1", "start shard1-node2,shard1-node1"}},
	} {
		t.Run(string(c.kind)+" "+string(c.target), func(t *testing.T) {
			r := &fakeRemote{}
			in := &Injector{C: &Cluster{Remote: r}, Rand: rand.New(rand.NewPCG(1, 2))}
			f, err := in.InjectAt(context.Background(), c.kind, c.target)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Heal(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := r.did()
			// The shard and the follower are picked at random: compare the
			// shape, with shard 1 and follower 1 standing in.
			norm := func(s string) string {
				for _, sh := range []string{"shard2", "shard3"} {
					s = strings.ReplaceAll(s, sh, "shard1")
				}
				return strings.NewReplacer("node3", "node1", "coord-3", "coord-1").Replace(s)
			}
			for i := range got {
				got[i] = norm(got[i])
			}
			if !slices.Equal(got, c.want) {
				t.Fatalf("did %q, want %q", got, c.want)
			}
		})
	}
	// Faults that need tools inside the container are refused, not skipped
	// silently.
	in := &Injector{C: &Cluster{Remote: &fakeRemote{}}, Rand: rand.New(rand.NewPCG(1, 2))}
	for _, k := range []Kind{DelayShard, RollingRestart} {
		if _, err := in.InjectAt(context.Background(), k, TargetShard); err == nil || !strings.Contains(err.Error(), "not available on a remote cluster") {
			t.Fatalf("%s: %v", k, err)
		}
	}
}

// An az-outage kills every service of one zone, router included, and starts
// them again. Every group keeps two of its three nodes.
func TestAZOutage(t *testing.T) {
	r := &fakeRemote{}
	in := &Injector{C: &Cluster{Remote: r}, Rand: rand.New(rand.NewPCG(1, 2))}
	f, err := in.InjectAt(context.Background(), AZOutage, "")
	if err != nil {
		t.Fatal(err)
	}
	if f.Zone == "" || len(f.Targets) < 4 || f.TargetName() != "-" || !strings.Contains(f.String(), "zone "+f.Zone) {
		t.Fatalf("%+v", f)
	}
	perGroup := map[int]int{}
	for _, svc := range f.Targets {
		for _, g := range GroupIDs() {
			for _, n := range NodeIDs {
				if svc == Service(g, n) {
					perGroup[g]++
				}
			}
		}
	}
	for _, g := range GroupIDs() {
		if perGroup[g] != 1 {
			t.Fatalf("zone %s holds %d nodes of %s", f.Zone, perGroup[g], GroupName(g))
		}
	}
	if err := f.Heal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.did(); len(got) != 2 || got[0] != "kill "+strings.Join(f.Targets, ",") || got[1] != "start "+strings.Join(f.Targets, ",") {
		t.Fatalf("%q", got)
	}

	// The Compose cluster has no zones: zone N is node N of every group.
	d := &fakeDocker{leader: 2}
	f, err = newInjector(d).InjectAt(context.Background(), AZOutage, "")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if _, err := fmt.Sscanf(f.Zone, "node-%d", &n); err != nil || n < 1 || n > 3 {
		t.Fatalf("zone %q", f.Zone)
	}
	want := fmt.Sprintf("kill -s KILL seatsafe-coord-%d-1 seatsafe-shard1-node%d-1 seatsafe-shard2-node%d-1 seatsafe-shard3-node%d-1", n, n, n, n)
	if got := d.commands(); len(got) != 1 || got[0] != want {
		t.Fatalf("%q, want %q", got, want)
	}
	if kinds, err := ParseKinds("az-outage,none"); err != nil || len(kinds) != 2 {
		t.Fatalf("%v %v", kinds, err)
	}
}

// history writes a run directory whose history has one order per 100 ms
// from 0 to 2 s; those first called in [fromMs, toMs) and listed in
// unanswered got no answer.
func outageRun(t *testing.T, dir string, started time.Time, unanswered map[int]bool) {
	t.Helper()
	var b strings.Builder
	b.WriteString(`{"kind":"list","phase":"baseline","event":"e","section":"s","free":["a"],"call_ns":-20,"return_ns":-10}` + "\n")
	for i := 0; i < 20; i++ {
		call := int64(i) * 100e6
		outcome, code := "SEAT_UNAVAILABLE", "OK"
		if unanswered[i] {
			outcome, code = "", "DeadlineExceeded"
		}
		fmt.Fprintf(&b, `{"client_id":1,"request_id":"o%d","seats":["e/s/z%d"],"call_ns":%d,"return_ns":%d,"outcome":"%s","replayed":false,"code":"%s","cross":false}`+"\n", i, i, call, call+5e6, outcome, code)
		if unanswered[i] && i%2 == 0 { // a retry that was answered: the order counts as answered
			fmt.Fprintf(&b, `{"client_id":1,"request_id":"o%d","seats":["e/s/z%d"],"call_ns":%d,"return_ns":%d,"outcome":"SEAT_UNAVAILABLE","replayed":false,"code":"OK","cross":false}`+"\n", i, i, call+50e6, call+55e6)
		}
	}
	writeRun(t, dir, auditPass, b.String())
	sum := fmt.Sprintf(`{"started_unix_ns": %d}`, started.UnixNano())
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), []byte(sum), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestAvailability(t *testing.T) {
	dir := t.TempDir()
	started := time.Unix(1_800_000_000, 0)
	// Orders 5..14 (0.5 s to 1.5 s) are in the outage; 6, 7, 9 and 11 got no
	// answer at first, and 6 was answered on a retry.
	outageRun(t, dir, started, map[int]bool{6: true, 7: true, 9: true, 11: true, 18: true})
	answered, total, err := Availability(dir, started.Add(500*time.Millisecond), started.Add(1500*time.Millisecond))
	if err != nil || total != 10 || answered != 7 {
		t.Fatalf("answered %d of %d, %v; want 7 of 10", answered, total, err)
	}
	if _, _, err := Availability(t.TempDir(), started, started.Add(time.Second)); err == nil {
		t.Fatal("no error for a directory without a run")
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Availability(dir, started, started.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "started_unix_ns") {
		t.Fatalf("%v", err)
	}
}

// A campaign on a remote cluster: the load goes through the Remote, an
// az-outage run reports the zone, every group's failover and the
// availability, and the history check is given the widening for clock skew.
func TestRemoteCampaign(t *testing.T) {
	r := &fakeRemote{}
	var loadStart time.Time
	r.load = func(dir string, args []string) (string, error) {
		loadStart = time.Now()
		// The load generator "runs" until after the fault has healed.
		time.Sleep(400 * time.Millisecond)
		outageRun(t, dir, loadStart, nil)
		return consolePass, nil
	}
	out := t.TempDir()
	cfg := Config{
		Runs: 3, Seed: 3, Plan: []Step{{AZOutage, ""}, {Partition, TargetCoordinator}, {None, ""}},
		InjectAfter: [2]time.Duration{50 * time.Millisecond, 50 * time.Millisecond}, FaultFor: [2]time.Duration{150 * time.Millisecond, 150 * time.Millisecond},
		HealTimeout: time.Second, Workload: "flash", Rate: 300, Duration: time.Second, CrossShardFrac: 0.2, BrowseRate: 5,
		Linearizable: true, Widen: 3 * time.Millisecond, Resume: true, Out: out, Logf: func(string, ...any) {},
	}
	results, err := Campaign(context.Background(), &Cluster{Remote: r}, cfg)
	if err != nil || len(results) != 3 {
		t.Fatalf("%d results, %v", len(results), err)
	}
	az := results[0]
	if az.Verdict != VerdictPass || az.Kind != AZOutage || az.Zone == "" || len(az.GroupFailover) != 4 || az.Failover < 0 {
		t.Fatalf("az-outage run: %+v", az)
	}
	for g, d := range az.GroupFailover {
		if d < 0 || d > time.Second {
			t.Fatalf("failover of group %d: %v", g, d)
		}
	}
	if az.Availability != 1 || az.OutageOrders == 0 {
		t.Fatalf("availability %v of %d orders", az.Availability, az.OutageOrders)
	}
	if p := results[1]; p.Verdict != VerdictPass || p.Target != "coordinator" || p.Failover < 0 {
		t.Fatalf("partition run: %+v", p)
	}
	did := strings.Join(r.did(), "\n")
	for _, want := range []string{"reset\nload -target router:7000 -shardmap /deploy/shardmap.json -seed-dir /deploy/seed -out /out -label chaos-0001 -mode open -workload flash -rate 300",
		"-cross-shard-frac 0.2 -browse-rate 5", "partition coord-2", "rejoin coord-2"} {
		if !strings.Contains(did, want) {
			t.Fatalf("the remote was not asked for %q:\n%s", want, did)
		}
	}
	if strings.Count(did, "kill ") != 1 || strings.Count(did, "start ") != 1 || r.resets != 1 {
		t.Fatalf("calls:\n%s", did)
	}
	s := Summarize(results)
	for _, want := range []string{"| az-outage | 1 | 1 | 0 | 0 |", "Zone outages", "- run 1: zone " + az.Zone + " down; failover per group: coordinator ", "availability during the outage: 100.0% of "} {
		if !strings.Contains(s, want) {
			t.Fatalf("summary is missing %q:\n%s", want, s)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(out, "run-0001", "fault.txt")); !strings.Contains(string(b), "zone "+az.Zone+" down; failover per group") {
		t.Fatalf("fault.txt:\n%s", b)
	}

	// The widening reaches the check.
	hc, err := CheckHistory(filepath.Join(out, "run-0003"), linearize.Options{Timeout: time.Minute, Widen: 3 * time.Millisecond})
	if err != nil || hc.Bookings != linearize.Pass {
		t.Fatalf("%+v %v", hc, err)
	}
}

// A stopped campaign goes on where it stopped: finished runs are kept and
// not run again, and the runs still to do inject what an uninterrupted
// campaign would have.
func TestCampaignResumes(t *testing.T) {
	run := func(out string, runs int, stopAfter int) ([]Result, []string, int) {
		r := &fakeRemote{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		loads := 0
		r.load = func(dir string, _ []string) (string, error) {
			loads++
			writeRun(t, dir, auditPass, goodHistory)
			if loads == stopAfter {
				defer cancel() // stopped right after this run's load
			}
			return consolePass, nil
		}
		cfg := Config{Runs: runs, Seed: 11, Kinds: []Kind{KillLeader, PauseLeader, Partition}, HealTimeout: time.Second, Duration: time.Second,
			Linearizable: true, Resume: true, Out: out, Logf: func(string, ...any) {}}
		results, err := Campaign(ctx, &Cluster{Remote: r}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		var faults []string
		for _, call := range r.did() {
			if strings.HasPrefix(call, "kill ") || strings.HasPrefix(call, "pause ") || strings.HasPrefix(call, "partition ") {
				faults = append(faults, call)
			}
		}
		return results, faults, loads
	}
	whole, wholeFaults, _ := run(t.TempDir(), 6, 0)
	if len(whole) != 6 || len(wholeFaults) != 6 {
		t.Fatalf("%d results, faults %q", len(whole), wholeFaults)
	}

	out := t.TempDir()
	first, firstFaults, _ := run(out, 6, 2)
	if len(first) < 2 || len(first) > 3 {
		t.Fatalf("the stopped campaign recorded %d runs", len(first))
	}
	done := len(first)
	rest, restFaults, loads := run(out, 6, 0)
	if len(rest) != 6 || loads != 6-done {
		t.Fatalf("after resuming: %d results from %d more loads, want 6 from %d", len(rest), loads, 6-done)
	}
	if got := append(slices.Clone(firstFaults[:done]), restFaults...); !slices.Equal(got, wholeFaults) {
		t.Fatalf("faults of the resumed campaign %q, of an uninterrupted one %q", got, wholeFaults)
	}
	for i, r := range rest {
		if r.Run != i+1 || r.Verdict != VerdictPass {
			t.Fatalf("run %d: %+v", i+1, r)
		}
	}
	got, err := ReadResults(out)
	if err != nil || len(got) != 6 {
		t.Fatalf("runs.jsonl: %d results, %v", len(got), err)
	}
	if b, _ := os.ReadFile(filepath.Join(out, "runs.csv")); strings.Count(string(b), "\n") != 7 || strings.Count(string(b), "run,verdict") != 1 {
		t.Fatalf("runs.csv:\n%s", b)
	}
	if b, _ := os.ReadFile(filepath.Join(out, "summary.md")); !strings.Contains(string(b), "6 runs: 6 PASS") {
		t.Fatalf("summary.md:\n%s", b)
	}
	// A finished campaign has nothing left to do.
	if again, _, loads := run(out, 6, 0); len(again) != 6 || loads != 0 {
		t.Fatalf("resuming a finished campaign ran %d loads", loads)
	}
}
