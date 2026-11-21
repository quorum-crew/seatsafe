package chaos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/linearize"
)

// The coordinator group takes the faults that act on one node, with the
// coordinator's service names; the others are refused.
func TestInjectAtCoordinator(t *testing.T) {
	for _, tc := range []struct {
		kind         Kind
		target       Target
		inject, heal string // prefixes of the docker commands
		service      string // prefix of the target service
	}{
		{Partition, TargetCoordinator, "network disconnect seatsafe_seatsafe seatsafe-coord-2-1", "network connect --alias coord-2 seatsafe_seatsafe seatsafe-coord-2-1", "coord-2"},
		{Partition, TargetShard, "network disconnect seatsafe_seatsafe seatsafe-shard", "network connect --alias shard", "shard"},
		{KillLeader, TargetCoordinator, "kill -s KILL seatsafe-coord-2-1", "start seatsafe-coord-2-1", "coord-2"},
		{PauseLeader, TargetCoordinator, "pause seatsafe-coord-2-1", "unpause seatsafe-coord-2-1", "coord-2"},
		{KillFollower, TargetCoordinator, "kill -s KILL seatsafe-coord-", "start seatsafe-coord-", "coord-"},
	} {
		t.Run(string(tc.kind)+" "+string(tc.target), func(t *testing.T) {
			d := &fakeDocker{leader: 2}
			f, err := newInjector(d).InjectAt(context.Background(), tc.kind, tc.target)
			if err != nil {
				t.Fatal(err)
			}
			g, ok := f.Group()
			if len(f.Targets) != 1 || !strings.HasPrefix(f.Targets[0], tc.service) || !ok || (g == CoordinatorGroup) != (tc.target == TargetCoordinator) {
				t.Fatalf("fault %s: targets %v, group %d %v", f, f.Targets, g, ok)
			}
			if tc.target == TargetCoordinator && (f.TargetName() != "coordinator" || !strings.Contains(f.String(), " coordinator coord-")) {
				t.Fatalf("fault described as %q, target %q", f, f.TargetName())
			}
			if tc.kind == KillFollower && f.Targets[0] == "coord-2" {
				t.Fatal("kill-follower picked the leader")
			}
			checkPrefixes(t, "inject", d.commands(), []string{tc.inject})
			before := len(d.commands())
			if err := f.Heal(context.Background()); err != nil {
				t.Fatal(err)
			}
			checkPrefixes(t, "heal", d.commands()[before:], []string{tc.heal})
		})
	}
	for _, k := range []Kind{QuorumLoss, DelayShard, KillRouter, RollingRestart} {
		d := &fakeDocker{leader: 2}
		if f, err := newInjector(d).InjectAt(context.Background(), k, TargetCoordinator); err == nil || f != nil || len(d.commands()) != 0 {
			t.Fatalf("%s on the coordinator: fault %v, err %v, commands %v", k, f, err, d.commands())
		}
	}
	if !Partition.RemovesLeader() {
		t.Fatal("partition takes the leader away")
	}
	if k, err := ParseKinds("partition,none"); err != nil || len(k) != 2 {
		t.Fatal(k, err)
	}
}

// The coordinator group is part of the health check and of the term count.
func TestCoordinatorHealth(t *testing.T) {
	d := &fakeDocker{leader: 1}
	c := &Cluster{Docker: d, Project: "seatsafe"}
	d.applied = map[string]uint64{"coord-3": 90}
	_, bad, err := c.WaitHealed(context.Background(), 600*time.Millisecond)
	if err == nil || len(bad) != 1 || bad[0].Shard != CoordinatorGroup || !strings.Contains(describe(bad), "coordinator: applied indexes differ") {
		t.Fatalf("lagging coordinator replica: %v %v", err, bad)
	}
	if l := c.Leader(context.Background(), CoordinatorGroup); l != 1 {
		t.Fatalf("coordinator leader %d", l)
	}
	if terms, ok := c.Terms(context.Background()); !ok || len(terms) != 4 {
		t.Fatalf("terms %v %v", terms, ok)
	}
}

func TestMemAvailable(t *testing.T) {
	got, err := memAvailable("MemTotal:       16000000 kB\nMemFree:          200000 kB\nMemAvailable:    4194304 kB\n")
	if err != nil || got != 4<<30 {
		t.Fatalf("%d %v", got, err)
	}
	if _, err := memAvailable("MemTotal: 1 kB\n"); err == nil {
		t.Fatal("missing MemAvailable accepted")
	}
}

// campaignDocker is fakeDocker plus a load generator and a compose that can
// be made to fail: "docker run ... loadgen" calls load with the campaign's
// run number and the run's output directory, and "docker compose ... up"
// fails while upFails is positive.
type campaignDocker struct {
	*fakeDocker
	load     func(run int, dir string) (console string, err error)
	upFails  int
	ups      int
	loadArgs string
}

func (d *campaignDocker) Run(ctx context.Context, args ...string) (string, error) {
	switch {
	case args[0] == "compose" && slices.Contains(args, "up"):
		d.ups++
		if d.upFails > 0 {
			d.upFails--
			return "", errors.New("compose up: no space left on device")
		}
		return "", nil
	case args[0] == "run":
		d.loadArgs = strings.Join(args, " ")
		var run int
		var dir string
		for i, a := range args {
			switch {
			case a == "-v" && strings.HasSuffix(args[i+1], ":/out"):
				dir = strings.TrimSuffix(args[i+1], ":/out")
			case a == "-label":
				fmt.Sscanf(args[i+1], "chaos-%d", &run)
			}
		}
		return d.load(run, dir)
	}
	return d.fakeDocker.Run(ctx, args...)
}

const (
	auditPass = `{"pass": true, "orders": 3, "unknown_orders": 1}`
	auditFail = `{"pass": false, "orders": 3, "unknown_orders": 2, "a_double_booked": 1, "e_cross_shard_half_booked": 1, "e_taken_by_unconfirmed": 2}`
	// One seat, free before, booked once, a later order turned away, taken after.
	goodHistory = `{"kind":"list","phase":"baseline","event":"e","section":"s","free":["a"],"call_ns":-20,"return_ns":-10}
{"client_id":1,"request_id":"o1","seats":["e/s/a"],"call_ns":10,"return_ns":20,"outcome":"CONFIRMED","replayed":false,"code":"OK","cross":false}
{"client_id":2,"request_id":"o2","seats":["e/s/a"],"call_ns":30,"return_ns":40,"outcome":"SEAT_UNAVAILABLE","replayed":false,"code":"OK","cross":false,"unavailable_seats":["e/s/a"]}
{"kind":"list","phase":"final","event":"e","section":"s","free":[],"call_ns":50,"return_ns":60}
`
	// goodHistory with a browse that lists seat a as free after it was
	// confirmed.
	staleHistory = `{"kind":"list","phase":"baseline","event":"e","section":"s","free":["a"],"call_ns":-20,"return_ns":-10}
{"client_id":1,"request_id":"o1","seats":["e/s/a"],"call_ns":10,"return_ns":20,"outcome":"CONFIRMED","replayed":false,"code":"OK","cross":false}
{"kind":"list","phase":"browse","event":"e","section":"s","free":["a"],"call_ns":30,"return_ns":40}
{"kind":"list","phase":"final","event":"e","section":"s","free":[],"call_ns":50,"return_ns":60}
`
	// The same seat confirmed twice.
	badHistory = `{"client_id":1,"request_id":"o1","seats":["e/s/a"],"call_ns":10,"return_ns":20,"outcome":"CONFIRMED","replayed":false,"code":"OK","cross":false}
{"client_id":2,"request_id":"o2","seats":["e/s/a"],"call_ns":30,"return_ns":40,"outcome":"CONFIRMED","replayed":false,"code":"OK","cross":false}
`
	consolePass = "  all   3   1.00   2.00   3.00   4.00   5.00\n  audit PASS: 3 orders\n"
)

func writeRun(t *testing.T, dir, audit, history string) {
	t.Helper()
	for name, body := range map[string]string{"audit.json": audit, "history.jsonl": history} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A campaign over the mix keeps going when runs break in every way a run can
// break: the load generator dies without an audit, the cluster does not come
// up, the campaign code panics. Every run gets its row, the rows say what
// happened, and the run after a broken one starts from a reset cluster.
func TestCampaignSurvivesCrashes(t *testing.T) {
	old := resetPause
	resetPause = time.Millisecond
	defer func() { resetPause = old }()

	out := t.TempDir()
	d := &campaignDocker{fakeDocker: &fakeDocker{leader: 2}}
	panicNext := false
	d.load = func(run int, dir string) (string, error) {
		switch run {
		case 2: // the load generator dies: no audit, no history
			return "", errors.New("exit status 137")
		case 3: // the audit passes, but the history is not linearizable
			writeRun(t, dir, auditPass, badHistory)
		case 4: // the audit fails; the next reset will fail three times
			writeRun(t, dir, auditFail, goodHistory)
			d.upFails = resetTries
			return "  audit FAIL: 3 orders\n", nil
		case 6:
			writeRun(t, dir, auditPass, goodHistory)
			panicNext = true
		case 8: // a stale browse: reported, and the run still passes
			writeRun(t, dir, auditPass, staleHistory)
		default:
			writeRun(t, dir, auditPass, goodHistory)
		}
		return consolePass, nil
	}
	cfg := Config{
		Runs: 9, ResetEvery: 4, Seed: 7, Plan: Mix, HealTimeout: time.Second, Image: "seatsafe:dev", DeployDir: "deploy", Workload: "flash",
		Rate: 300, Duration: time.Second, CrossShardFrac: 0.2, BrowseRate: 5, Linearizable: true, Out: out, Title: "# test\n\n",
		MinFreeMem: 3 << 30, MemWait: time.Millisecond,
		FreeMem: func(context.Context) (uint64, error) {
			if panicNext {
				panicNext = false
				panic("boom")
			}
			return 4 << 30, nil
		},
		Logf: func(string, ...any) {},
	}
	c := &Cluster{Docker: d, Project: "seatsafe", Files: []string{"docker-compose.yml"}}
	results, err := Campaign(context.Background(), c, cfg)
	if err != nil || len(results) != 9 {
		t.Fatalf("%d results, %v", len(results), err)
	}
	var verdicts, kinds []string
	for _, r := range results {
		verdicts, kinds = append(verdicts, r.Verdict), append(kinds, string(r.Kind))
	}
	if got, want := strings.Join(verdicts, " "), "PASS FAIL FAIL FAIL CRASH PASS CRASH PASS PASS"; got != want {
		t.Fatalf("verdicts %s, want %s", got, want)
	}
	// Runs 5 and 7 broke before a fault was picked.
	if got, want := strings.Join(kinds, " "), "kill-leader pause-leader partition none  pause-leader  none kill-leader"; got != want {
		t.Fatalf("kinds %q, want %q", got, want)
	}
	r := func(i int) Result { return results[i-1] }
	if !strings.HasPrefix(r(1).Target, "shard ") || r(4).Target != "-" || r(6).Target != "coordinator" || r(5).Target != "-" {
		t.Fatalf("targets %q %q %q %q", r(1).Target, r(4).Target, r(6).Target, r(5).Target)
	}
	if !strings.Contains(r(2).Reason, "no audit result") || r(2).AuditRules != nil || r(2).Unknown() != -1 || r(2).Linearizable != "-" {
		t.Fatalf("run 2: %+v", r(2))
	}
	if r(3).Linearizable != linearize.Fail || !strings.Contains(r(3).Reason, "not linearizable: first bad seat e/s/a") {
		t.Fatalf("run 3: %+v", r(3))
	}
	for _, name := range []string{"linearizability.html", "linearizability.txt", "fault.txt", "console.txt"} {
		if _, err := os.Stat(filepath.Join(out, "run-0003", name)); err != nil {
			t.Fatal(err)
		}
	}
	if a := r(4).AuditRules; a == nil || a.DoubleBooked != 1 || a.TakenByFailed != 2 || r(4).Unknown() != 2 || r(4).Linearizable != linearize.Pass || r(4).Reason != "correctness audit failed" {
		t.Fatalf("run 4: %+v", r(4))
	}
	if !strings.Contains(r(5).Reason, "cluster reset failed 3 times") || !strings.Contains(r(5).Reason, "no space left") {
		t.Fatalf("run 5: %+v", r(5))
	}
	if r(8).Verdict != VerdictPass || r(8).Linearizable != linearize.Pass || r(8).StaleBrowses != 1 || !slices.Equal(r(8).StaleSeats, []string{"e/s/a"}) || r(8).Reason != "" {
		t.Fatalf("run 8: %+v", r(8))
	}
	if b, err := os.ReadFile(filepath.Join(out, "run-0008", "stale-browses.txt")); err != nil || !strings.Contains(string(b), "stale browses: 1 (seats e/s/a)") {
		t.Fatalf("stale-browses.txt: %v\n%s", err, b)
	}
	if b, _ := os.ReadFile(filepath.Join(out, "run-0008", "fault.txt")); !strings.Contains(string(b), "bookings: PASS") || !strings.Contains(string(b), "stale browses: 1 (seats e/s/a)") {
		t.Fatalf("fault.txt:\n%s", b)
	}
	if r(1).StaleBrowses != 0 || r(2).StaleBrowses != -1 {
		t.Fatalf("stale browses of runs 1 and 2: %d %d", r(1).StaleBrowses, r(2).StaleBrowses)
	}
	if r(7).Reason != "panic: boom" {
		t.Fatalf("run 7: %+v", r(7))
	}
	if r(1).P99 != 3*time.Millisecond || r(1).FreeMem != 4<<30 || r(1).CheckDetail != "1 seats, 4 operations, 0 browses" {
		t.Fatalf("run 1: %+v", r(1))
	}
	// Resets: before run 1, after each of the failed runs 2 and 3 (before
	// runs 3 and 4), three failed tries after the failed run 4 (5) and one
	// that worked (6), after the panic (8), and the scheduled one before run
	// 9. A run that did not pass may leave something behind that the next
	// run's baseline would see, so the next run starts from a reset cluster.
	if d.ups != 9 {
		t.Fatalf("%d compose ups, want 9", d.ups)
	}
	if !strings.Contains(d.loadArgs, "-cross-shard-frac 0.2") || !strings.Contains(d.loadArgs, "-browse-rate 5") || !strings.Contains(d.loadArgs, "-workload flash") {
		t.Fatalf("load generator arguments: %s", d.loadArgs)
	}

	b, err := os.ReadFile(filepath.Join(out, "summary.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# test\n\n9 runs: 4 PASS, 3 FAIL, 0 SKIP, 2 CRASH (the run itself broke)",
		"a run passes or fails on it): 5 pass, 1 fail, 0 timeout, 3 not checked",
		"Stale browses (checked separately, reported, never a failure): 1 in 1 runs",
		"| 8 | none | - | - | PASS | 0 | 0 | 0 | 0 | 0+0 | PASS | 1 (seats e/s/a) |",
		"| Bookings | Stale browses |",
		"| 2 | pause-leader | shard ",
		"| - | - | - | - | - | - | - | - | - | - | - | 0 | FAIL |",
		"| 4 | none | - | - | FAIL | 1 | 0 | 0 | 0 | 1+2 | PASS | 0 |",
		"| 6 | pause-leader | coordinator | - | PASS | 0 | 0 | 0 | 0 | 0+0 | PASS | 0 |",
		"| - | - | CRASH |",
		"| **Total** | 9 runs | | | 5/6 pass | 1 | 0 | 0 | 0 | 1+2 | 5 pass, 1 fail, 0 timeout | 1 in 1 runs |",
		"- run 3: partition shard ",
		"- run 5: not started: cluster reset failed 3 times",
		"- run 7: not started: panic: boom",
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("summary.md is missing %q", want)
		}
	}
	if t.Failed() {
		t.Logf("summary.md:\n%s", b)
	}
	if b, err := os.ReadFile(filepath.Join(out, "runs.csv")); err != nil || strings.Count(string(b), "\n") != 10 {
		t.Fatalf("runs.csv: %v\n%s", err, b)
	}
}

// A check that runs out of time makes the run a TIMEOUT: in the summary it
// is neither a pass nor a failure.
func TestCheckTimeoutIsNotAPass(t *testing.T) {
	dir := t.TempDir()
	writeRun(t, dir, auditPass, goodHistory)
	hc, err := CheckLinearizable(dir, time.Nanosecond)
	if err != nil || hc.Bookings != linearize.Timeout || hc.Detail != "1 of 1 seats not decided" {
		t.Fatalf("%+v %v", hc, err)
	}
	if hc, err := CheckLinearizable(dir, time.Minute); err != nil || hc.Bookings != linearize.Pass || hc.StaleBrowses != 0 {
		t.Fatalf("%+v %v", hc, err)
	}
	if hc, err := CheckLinearizable(t.TempDir(), time.Minute); err == nil || hc.Bookings != "-" || hc.StaleBrowses != -1 {
		t.Fatalf("missing history: %+v %v", hc, err)
	}

	d := &campaignDocker{fakeDocker: &fakeDocker{leader: 2}}
	d.load = func(_ int, dir string) (string, error) {
		writeRun(t, dir, auditPass, goodHistory)
		return consolePass, nil
	}
	out := t.TempDir()
	results, err := Campaign(context.Background(), &Cluster{Docker: d, Project: "seatsafe"}, Config{
		Runs: 1, Kinds: []Kind{None}, HealTimeout: time.Second, Duration: time.Second, Linearizable: true, CheckTimeout: time.Nanosecond,
		Out: out, Logf: func(string, ...any) {},
	})
	if err != nil || len(results) != 1 || results[0].Verdict != VerdictTimeout || !strings.Contains(results[0].Reason, "ran out of time") {
		t.Fatalf("%+v %v", results, err)
	}
	s := Summarize(results)
	for _, want := range []string{"1 runs: 0 PASS, 0 FAIL, 0 SKIP, 1 TIMEOUT (bookings check not finished; not a pass)", "0 pass, 0 fail, 1 timeout, 0 not checked", "| none | 1 | 0 | 0 | 0 |", "| TIMEOUT |"} {
		if !strings.Contains(s, want) {
			t.Fatalf("summary missing %q:\n%s", want, s)
		}
	}
}

// A fault that could not be injected never hides a failure: the run is SKIP
// only if nothing else went wrong. cmd/chaos exits 0 for SKIP, so an
// unattended campaign would not see the failure.
func TestFailureIsNotHiddenBySkip(t *testing.T) {
	for _, c := range []struct {
		name                string
		audit, linearizable string
		want, reason        string
	}{
		{"audit failed", "audit FAIL: 3 orders", linearize.Pass, VerdictFail, "correctness audit failed; could not inject: coordinator has no leader to target"},
		{"bookings not linearizable", "audit PASS: 3 orders", linearize.Fail, VerdictFail, "bookings are not linearizable: first bad seat e/s/a; could not inject: coordinator has no leader to target"},
		{"check ran out of time", "audit PASS: 3 orders", linearize.Timeout, VerdictTimeout, "the linearizability check ran out of time: first bad seat e/s/a; could not inject: coordinator has no leader to target"},
		{"nothing failed", "audit PASS: 3 orders", linearize.Pass, VerdictSkip, "could not inject: coordinator has no leader to target"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := blank(1)
			r.Audit, r.Linearizable, r.CheckDetail = c.audit, c.linearizable, "first bad seat e/s/a"
			r.InjectErr = "coordinator has no leader to target" // and no Fault: nothing was changed
			r.decide(nil, nil, nil, nil)
			if r.Verdict != c.want || r.Reason != c.reason {
				t.Fatalf("verdict %s (%q), want %s (%q)", r.Verdict, r.Reason, c.want, c.reason)
			}
		})
	}
	// A fault that was injected in part: the run is decided as usual, with
	// a note.
	r := blank(1)
	r.Audit, r.Linearizable, r.Fault, r.InjectErr = "audit PASS: 3 orders", linearize.Pass, "kill-leader shard 1", "restart: timeout"
	if r.decide(nil, nil, nil, nil); r.Verdict != VerdictPass || r.Reason != "partial inject: restart: timeout" {
		t.Fatalf("partial inject: %s %q", r.Verdict, r.Reason)
	}
}

// -target coordinator takes only the faults the coordinator group can get.
// With the default -faults all, four of the nine could only be skipped.
func TestCoordinatorPlan(t *testing.T) {
	_, err := CoordinatorPlan(AllKinds)
	if err == nil {
		t.Fatal("every fault kind accepted for the coordinator")
	}
	for _, k := range []Kind{QuorumLoss, DelayShard, KillRouter, RollingRestart} {
		if !strings.Contains(err.Error(), string(k)) {
			t.Fatalf("%v: does not name %s", err, k)
		}
	}
	if strings.Contains(strings.SplitN(err.Error(), "cannot", 2)[0], string(KillLeader)) {
		t.Fatalf("%v: names kill-leader as unsupported", err)
	}

	kinds := []Kind{KillLeader, KillFollower, IsolateLeader, IsolateFollower, PauseLeader, Partition, None}
	plan, err := CoordinatorPlan(kinds)
	if err != nil || len(plan) != len(kinds) {
		t.Fatalf("%v, %v", plan, err)
	}
	for i, s := range plan {
		if s.Kind != kinds[i] || s.Target != TargetCoordinator || (s.Kind != None && !s.Kind.onCoordinator()) {
			t.Fatalf("step %d: %+v", i, s)
		}
	}
}

// A run starts only after the last run's holds can have been resolved: it
// waits HoldDrain after the run before, unless the cluster is reset in
// between (after a run that did not pass), which clears every hold.
func TestCampaignWaitsForHolds(t *testing.T) {
	const drain = time.Second
	var starts []time.Time
	d := &campaignDocker{fakeDocker: &fakeDocker{leader: 2}}
	d.load = func(run int, dir string) (string, error) {
		starts = append(starts, time.Now())
		if run == 3 {
			writeRun(t, dir, auditFail, goodHistory)
			return "  audit FAIL: 3 orders\n", nil
		}
		writeRun(t, dir, auditPass, goodHistory)
		return consolePass, nil
	}
	results, err := Campaign(context.Background(), &Cluster{Docker: d, Project: "seatsafe"}, Config{
		Runs: 4, Kinds: []Kind{None}, HealTimeout: time.Second, Duration: time.Second, Linearizable: true,
		HoldDrain: drain, Out: t.TempDir(), Logf: func(string, ...any) {},
	})
	if err != nil || len(results) != 4 || len(starts) != 4 {
		t.Fatalf("%d results, %d loads, %v", len(results), len(starts), err)
	}
	if results[2].Verdict != VerdictFail {
		t.Fatalf("run 3: %+v", results[2])
	}
	for i, want := range []bool{true, true, false} { // before runs 2, 3, 4
		gap := starts[i+1].Sub(starts[i])
		if waited := gap >= drain; waited != want {
			t.Errorf("run %d started %v after run %d; want a wait of %v: %v", i+2, gap.Round(time.Millisecond), i+1, drain, want)
		}
	}
	if d.ups != 2 {
		t.Fatalf("%d compose ups, want 2 (before run 1, after the failed run 3)", d.ups)
	}
}
