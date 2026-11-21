package chaos

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quorum-crew/seatsafe/internal/linearize"
	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

// Step is one entry of a campaign plan: a fault and where it goes.
type Step struct {
	Kind   Kind
	Target Target // TargetShard or TargetCoordinator; ignored for None
}

// Mix is the plan of an unattended campaign: kill, pause and partition the
// leader, and a no-fault control, with the faults alternating between a
// shard and the coordinator. Twenty runs give five of each fault.
var Mix = []Step{
	{KillLeader, TargetShard}, {PauseLeader, TargetShard}, {Partition, TargetShard}, {None, ""},
	{KillLeader, TargetCoordinator}, {PauseLeader, TargetCoordinator}, {Partition, TargetCoordinator}, {None, ""},
}

// Config is one chaos campaign: Runs load runs, each with one fault.
type Config struct {
	Runs       int
	ResetEvery int // wipe and restart the cluster every N runs (0 = only before the first)
	Seed       uint64

	Kinds []Kind // a random one per run, on a random shard
	// Plan, if set, replaces Kinds: run i gets Plan[(i-1) % len(Plan)].
	Plan        []Step
	InjectAfter [2]time.Duration // fault starts this long into the load (min, max)
	FaultFor    [2]time.Duration // and is healed this long later (min, max)
	HealTimeout time.Duration    // time allowed for every group to recover after the load

	// Load generator, run as a container on the cluster network.
	Image          string // e.g. seatsafe:dev
	DeployDir      string // host path holding shardmap.json and seed/
	Workload       string
	Rate           float64
	Duration       time.Duration
	CrossShardFrac float64 // fraction of orders across two shards (2PC)
	BrowseRate     float64 // ListAvailableSeats calls per second during the load, for the linearizability check
	ExtraArgs      []string

	// Linearizable runs the Porcupine check on every run's history: the
	// bookings (orders, baseline and final read) decide the run, the browses
	// are only reported. CheckTimeout bounds it (0 =
	// linearize.DefaultTimeout); a bookings check that runs out of time is
	// reported as TIMEOUT, never as a pass.
	Linearizable bool
	CheckTimeout time.Duration

	// MinFreeMem is the memory, in bytes, that must be available to the
	// containers before a run starts (0 = don't check). If less is free the cluster is reset
	// once and the campaign waits up to MemWait for it.
	MinFreeMem uint64
	MemWait    time.Duration
	FreeMem    func(ctx context.Context) (uint64, error) // default: Cluster.FreeMem (Docker's MemAvailable)

	// HoldDrain is how long after a run the next one waits before its load
	// starts, unless the cluster is reset in between (0 = no wait). A
	// cross-shard order of the last run may have left a hold on a shard
	// (an Abort that was lost, an order that was never answered); the shard
	// resolves it with the coordinator once it expires. The next run's
	// baseline must not see it: the model starts every seat free or booked,
	// and a hold released during the run would look like a booked seat
	// coming free. DefaultHoldDrain covers the default hold timing.
	HoldDrain time.Duration

	// Widen is how far the history check moves every call earlier and
	// every return later, for histories merged from machines whose clocks
	// differ (linearize.Options.Widen). 0 on one machine.
	Widen time.Duration
	// Resume continues a campaign that was stopped: the runs recorded in
	// Out/runs.jsonl are kept and not run again. With it every run draws
	// its fault from a random stream of its own (seed and run number), so a
	// resumed campaign injects what an uninterrupted one would have.
	Resume bool

	Out        string // host directory for results
	Title      string // first lines of summary.md
	StopOnFail bool
	Logf       func(format string, args ...any)
}

// DefaultHoldDrain covers a hold's life with the default timing: it expires
// after the coordinator's hold TTL (5 s, HOLD_TTL_MS), the shard's next
// check finds it within a second plus jitter, and the coordinator answers
// within the 2 s decide timeout.
const DefaultHoldDrain = 9 * time.Second

// Verdicts of a run.
const (
	VerdictPass = "PASS"
	VerdictFail = "FAIL"
	VerdictSkip = "SKIP" // the fault could not be injected
	// VerdictTimeout: nothing failed, but the linearizability check ran out
	// of time, so the run is not known to be linearizable. Not a pass.
	VerdictTimeout = "TIMEOUT"
	// VerdictCrash: the run itself broke (the cluster would not start, the
	// campaign code panicked). It says nothing about the system under test.
	VerdictCrash = "CRASH"
	// VerdictIdempotency: one request_id was answered in two different ways
	// (two completed attempts with different outcomes or different
	// unavailable seats), and nothing else failed. A failure.
	VerdictIdempotency = loadgen.VerdictIdempotency
)

// Result is the outcome of one run.
type Result struct {
	Run       int
	Fault     string
	Kind      Kind
	Target    string        // "shard N", "coordinator", or "-"
	InjectAt  time.Duration // into the load
	HealAt    time.Duration
	InjectErr string
	Audit     string // the load generator's audit line
	// AuditRules is audit.json: the counts behind rules a to e. Nil if the
	// load generator did not get as far as the audit.
	AuditRules *loadgen.Audit
	// Linearizable is the bookings verdict of the Porcupine check
	// (linearize.Pass, Fail or Timeout), over the orders, the baseline and
	// the final read, or "-" if it did not run. CheckDetail is its one-line
	// report and CheckTime how long reading the history and checking it took.
	Linearizable string
	CheckDetail  string
	CheckTime    time.Duration
	// StaleBrowses is how many browses of the run contradict its bookings,
	// or -1 if they were not checked, and StaleSeats the seats they are
	// stale for. Reported only: stale browses do not fail a run.
	StaleBrowses int
	StaleSeats   []string
	// Failover is the time from the fault until the rest of the group
	// agreed on a new leader, for faults that take the leader away; -1 if
	// not measured. Recovery is the time from healing until every replica
	// was back, agreed on the leader and had applied everything committed
	// when the fault was healed; -1 if not measured. Both are sampled about
	// every 100-300 ms (one docker exec per node), so they are approximate.
	Failover time.Duration
	Recovery time.Duration
	// For an az-outage. Zone is the zone taken down. GroupFailover is, for
	// every Raft group, the time from the outage until its remaining nodes
	// agreed on a leader among themselves (close to 0 for a group whose
	// leader was not in the zone; -1 if they never did while the zone was
	// down); Failover is the longest of them. Availability is the fraction
	// of the orders first sent during the outage that were answered, of
	// OutageOrders such orders; -1 if unknown.
	Zone          string                `json:",omitempty"`
	GroupFailover map[int]time.Duration `json:",omitempty"`
	Availability  float64
	OutageOrders  int
	// Elections is how many elections happened during the run, summed over
	// the shards and the coordinator (Raft term increases); -1 if unknown.
	// In a "none" run every one is unnecessary.
	Elections int
	// P99 and Max are the load generator's client latency over the run
	// (from intended send time, so they include failover stalls); -1 if
	// unknown.
	P99, Max time.Duration
	FreeMem  int64  // bytes available to the containers when the run started; -1 if not checked
	HostFree int64  // bytes available on the host then; -1 if not checked or unknown
	Verdict  string // VerdictPass, VerdictFail, VerdictSkip, VerdictTimeout, VerdictIdempotency or VerdictCrash
	// Idempotency describes a request that was answered in two different
	// ways; empty if there is none.
	Idempotency string
	Reason      string
}

// Unknown is the number of orders that ended UNKNOWN, or -1 if there was no
// audit.
func (r Result) Unknown() int {
	if r.AuditRules == nil {
		return -1
	}
	return r.AuditRules.UnknownOrders
}

var auditLine = regexp.MustCompile(`(?m)^\s*audit (PASS|FAIL).*$`)

// resetTries is how often a failed cluster reset is tried again before the
// run is given up as a CRASH, and resetPause the wait between tries.
const resetTries = 3

var resetPause = 5 * time.Second

// Campaign runs cfg against c and returns every run's result. One run
// breaking (the cluster not starting, the load generator dying, a panic)
// does not end the campaign: that run is recorded and the next one starts
// from a reset cluster. runs.csv and summary.md are rewritten after every
// run, so they are complete up to the last finished run whatever happens.
func Campaign(ctx context.Context, c *Cluster, cfg Config) ([]Result, error) {
	if len(cfg.Kinds) == 0 {
		cfg.Kinds = AllKinds
	}
	if cfg.MemWait <= 0 {
		cfg.MemWait = 3 * time.Minute
	}
	if cfg.FreeMem == nil {
		cfg.FreeMem = c.FreeMem
	}
	if err := os.MkdirAll(cfg.Out, 0o755); err != nil {
		return nil, err
	}
	var results []Result
	if cfg.Resume {
		var err error
		if results, err = ReadResults(cfg.Out); err != nil {
			return nil, err
		}
		if len(results) > 0 {
			cfg.Logf("resuming: %d runs are done, starting at run %d", len(results), len(results)+1)
		}
	}
	csvFile, err := os.OpenFile(filepath.Join(cfg.Out, "runs.csv"), os.O_CREATE|os.O_WRONLY|appendOrTruncate(len(results) > 0), 0o644)
	if err != nil {
		return nil, err
	}
	jsonl, err := os.OpenFile(filepath.Join(cfg.Out, "runs.jsonl"), os.O_CREATE|os.O_WRONLY|appendOrTruncate(len(results) > 0), 0o644)
	if err != nil {
		return nil, err
	}
	defer jsonl.Close()
	defer csvFile.Close()
	w := csv.NewWriter(csvFile)
	if len(results) == 0 {
		_ = w.Write(csvHeader)
	}
	w.Flush()

	rnd := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	in := &Injector{C: c, Rand: rnd}
	needReset := true
	var lastEnd time.Time
	for i := len(results) + 1; i <= cfg.Runs && ctx.Err() == nil; i++ {
		if cfg.Resume {
			rnd = rand.New(rand.NewPCG(cfg.Seed, uint64(i)))
			in.Rand = rnd
		}
		if cfg.ResetEvery > 0 && i > 1 && (i-1)%cfg.ResetEvery == 0 {
			needReset = true
		}
		// A reset clears every hold; otherwise let the last run's holds be
		// resolved before this run's baseline.
		if wait := cfg.HoldDrain - time.Since(lastEnd); !needReset && !lastEnd.IsZero() && wait > 0 {
			cfg.Logf("run %d: waiting %s for the last run's holds to be resolved", i, wait.Round(100*time.Millisecond))
			sleep(ctx, wait)
		}
		r := guarded(i, func() Result { return prepareAndRun(ctx, c, in, rnd, cfg, i, &needReset) })
		if ctx.Err() != nil && r.Verdict != VerdictPass {
			break // interrupted: the run says nothing
		}
		// After a run that did not finish normally the cluster is in an
		// unknown state. After any run that did not pass it may also hold
		// something the next run's baseline would see, such as a hold of a
		// cross-shard order that was never decided: start the next run
		// clean.
		if r.Verdict != VerdictPass || r.AuditRules == nil {
			needReset = true
		}
		results = append(results, r)
		lastEnd = time.Now()
		if b, err := json.Marshal(r); err == nil {
			_, _ = jsonl.Write(append(b, '\n'))
		}
		cfg.Logf("run %d: %s  %s  failover %s  recovery %s  elections %d  client p99 %s max %s  bookings %s (%s)  %s  %s %s", i, r.Verdict, r.Fault,
			secs(r.Failover), secs(r.Recovery), r.Elections, millis(r.P99), millis(r.Max), r.Linearizable, secs(r.CheckTime), r.staleLine(), r.Audit, r.Reason)
		_ = w.Write(csvRow(r))
		w.Flush()
		_ = os.WriteFile(filepath.Join(cfg.Out, "summary.md"), []byte(cfg.Title+Summarize(results)), 0o644)
		if (r.Verdict == VerdictFail || r.Verdict == VerdictIdempotency) && cfg.StopOnFail {
			cfg.Logf("stopping after a failure; the cluster is left as it is for inspection")
			break
		}
	}
	return results, w.Error()
}

func appendOrTruncate(appendTo bool) int {
	if appendTo {
		return os.O_APPEND
	}
	return os.O_TRUNC
}

// ReadResults returns the results a campaign has recorded in dir/runs.jsonl,
// one per finished run; none if the file is not there.
func ReadResults(dir string) ([]Result, error) {
	b, err := os.ReadFile(filepath.Join(dir, "runs.jsonl"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Result
	for n, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var r Result
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", filepath.Join(dir, "runs.jsonl"), n+1, err)
		}
		if r.Run != len(out)+1 {
			return nil, fmt.Errorf("%s line %d: run %d, want run %d", filepath.Join(dir, "runs.jsonl"), n+1, r.Run, len(out)+1)
		}
		out = append(out, r)
	}
	return out, nil
}

var csvHeader = []string{"run", "verdict", "kind", "fault", "inject_at_s", "heal_at_s", "failover_s", "recovery_s", "elections", "client_p99_ms", "client_max_ms", "audit", "reason",
	"target", "a_double_booked", "b_confirmed_still_free", "c_unexplained_taken", "d_confirmed_pre_booked", "e_cross_shard_half_booked", "e_taken_by_unconfirmed",
	"unknown_orders", "bookings", "check_s", "check_detail", "free_mem_gb", "host_free_mem_gb", "stale_browses", "stale_browse_seats", "zone", "availability"}

func csvRow(r Result) []string {
	a := r.AuditRules
	if a == nil {
		a = &loadgen.Audit{DoubleBooked: -1, StillFree: -1, Unexplained: -1, PreBooked: -1, HalfBooked: -1, TakenByFailed: -1}
	}
	availability := ""
	if r.Availability >= 0 {
		availability = strconv.FormatFloat(r.Availability, 'f', 4, 64)
	}
	return []string{strconv.Itoa(r.Run), r.Verdict, string(r.Kind), r.Fault,
		secs(r.InjectAt), secs(r.HealAt), secs(r.Failover), secs(r.Recovery), strconv.Itoa(r.Elections),
		millis(r.P99), millis(r.Max), r.Audit, r.Reason,
		r.Target, strconv.Itoa(a.DoubleBooked), strconv.Itoa(a.StillFree), strconv.Itoa(a.Unexplained), strconv.Itoa(a.PreBooked), strconv.Itoa(a.HalfBooked), strconv.Itoa(a.TakenByFailed),
		strconv.Itoa(r.Unknown()), r.Linearizable, secs(r.CheckTime), r.CheckDetail, gib(r.FreeMem), gib(r.HostFree),
		strconv.Itoa(r.StaleBrowses), strings.Join(r.StaleSeats, " "), r.Zone, availability}
}

// guarded runs one run and turns a panic in it into a CRASH result.
func guarded(i int, run func() Result) (r Result) {
	defer func() {
		if p := recover(); p != nil {
			r = blank(i)
			r.Verdict, r.Reason = VerdictCrash, fmt.Sprintf("panic: %v", p)
		}
	}()
	return run()
}

// blank is a result with nothing measured.
func blank(i int) Result {
	return Result{Run: i, Target: "-", Linearizable: "-", StaleBrowses: -1, Failover: -1, Recovery: -1, Elections: -1, P99: -1, Max: -1, FreeMem: -1, HostFree: -1, Availability: -1}
}

// prepareAndRun gets the cluster ready for run i (reset if asked for or
// unhealthy, enough free memory) and runs it.
func prepareAndRun(ctx context.Context, c *Cluster, in *Injector, rnd *rand.Rand, cfg Config, i int, needReset *bool) Result {
	crash := func(format string, args ...any) Result {
		r := blank(i)
		r.Verdict, r.Reason = VerdictCrash, fmt.Sprintf(format, args...)
		return r
	}
	reset := func(why string) error {
		var err error
		for try := 1; try <= resetTries && ctx.Err() == nil; try++ {
			cfg.Logf("run %d: resetting the cluster (%s)", i, why)
			if err = c.Reset(ctx); err == nil {
				*needReset = false
				return nil
			}
			cfg.Logf("run %d: reset failed: %v", i, err)
			sleep(ctx, resetPause)
		}
		return err
	}
	if *needReset {
		if err := reset("scheduled, or the last run did not finish"); err != nil {
			return crash("cluster reset failed %d times: %v", resetTries, err)
		}
	}
	if _, bad, err := c.WaitHealed(ctx, cfg.HealTimeout); err != nil {
		// A run left the cluster broken: that run already failed. Start
		// clean so later runs still mean something.
		if err := reset("unhealthy before the run: " + describe(bad)); err != nil {
			return crash("cluster reset failed %d times: %v", resetTries, err)
		}
	}
	free, hostFree := int64(-1), int64(-1)
	if cfg.MinFreeMem > 0 && c.Remote == nil {
		if h, err := HostFreeMem(ctx); err == nil {
			hostFree = int64(h)
		}
		got, err := waitFreeMem(ctx, cfg, func() error { return reset("less than " + gib(int64(cfg.MinFreeMem)) + " GiB of memory free") })
		switch {
		case err != nil:
			cfg.Logf("run %d: cannot read the free memory: %v", i, err)
		default:
			free = int64(got)
			if got < cfg.MinFreeMem {
				cfg.Logf("run %d: only %s GiB of memory free after waiting %v, want %s; running anyway", i, gib(free), cfg.MemWait, gib(int64(cfg.MinFreeMem)))
			}
		}
	}
	r := runOnce(ctx, c, in, rnd, cfg, i)
	r.FreeMem, r.HostFree = free, hostFree
	return r
}

func runOnce(ctx context.Context, c *Cluster, in *Injector, rnd *rand.Rand, cfg Config, i int) Result {
	r := blank(i)
	step := Step{Kind: cfg.Kinds[rnd.IntN(len(cfg.Kinds))], Target: TargetShard}
	if len(cfg.Plan) > 0 {
		step = cfg.Plan[(i-1)%len(cfg.Plan)]
	}
	r.Kind = step.Kind
	termsBefore, termsOK := c.Terms(ctx)
	dir := filepath.Join(cfg.Out, fmt.Sprintf("run-%04d", i))
	_ = os.MkdirAll(dir, 0o755)
	abs, _ := filepath.Abs(dir)
	deploy, _ := filepath.Abs(cfg.DeployDir)

	// Load in the background.
	name := c.Project + "-chaos-loadgen"
	if c.Remote == nil {
		_, _ = c.Docker.Run(context.Background(), "rm", "-f", name)
	}
	args := []string{"run", "--rm", "--name", name, "--network", c.Network(),
		"-v", deploy + ":/deploy:ro", "-v", abs + ":/out", cfg.Image, "loadgen"}
	loadFrom := len(args)
	args = append(args, "-target", "router:7000", "-shardmap", "/deploy/shardmap.json", "-seed-dir", "/deploy/seed",
		"-out", "/out", "-label", fmt.Sprintf("chaos-%04d", i), "-mode", "open", "-workload", cfg.Workload,
		"-rate", strconv.FormatFloat(cfg.Rate, 'f', -1, 64), "-warmup", "2s", "-duration", cfg.Duration.String(),
		"-seed", strconv.FormatUint(cfg.Seed+uint64(i), 10), "-history", "-quiet")
	if cfg.CrossShardFrac > 0 {
		args = append(args, "-cross-shard-frac", strconv.FormatFloat(cfg.CrossShardFrac, 'f', -1, 64))
	}
	if cfg.BrowseRate > 0 {
		args = append(args, "-browse-rate", strconv.FormatFloat(cfg.BrowseRate, 'f', -1, 64))
	}
	args = append(args, cfg.ExtraArgs...)
	type loadResult struct {
		out string
		err error
	}
	loadDone := make(chan loadResult, 1)
	start := time.Now()
	go func() {
		var out string
		var err error
		if c.Remote != nil {
			out, err = c.Remote.Load(ctx, dir, args[loadFrom:])
		} else {
			out, err = c.Docker.Run(ctx, args...)
		}
		loadDone <- loadResult{out, err}
	}()
	defer func() {
		if ctx.Err() != nil && c.Remote == nil {
			_, _ = c.Docker.Run(context.Background(), "rm", "-f", name)
		}
	}()

	// The fault: inject, hold, heal. Healing always runs, even on Ctrl-C,
	// so the cluster is never left broken by an interrupted run.
	sleep(ctx, between(rnd, cfg.InjectAfter))
	r.InjectAt = time.Since(start)
	f, err := in.InjectAt(ctx, step.Kind, step.Target)
	if f != nil {
		r.Fault, r.Target, r.Zone = f.String(), f.TargetName(), f.Zone
		defer func() {
			if f != nil {
				_ = healRetry(context.Background(), f)
			}
		}()
	}
	if err != nil {
		r.InjectErr = err.Error()
	}
	var outage [2]time.Time
	if f != nil {
		injected := time.Now()
		holdFor := between(rnd, cfg.FaultFor)
		failover := make(chan time.Duration, 1)
		fctx, stopFailover := context.WithTimeout(ctx, holdFor)
		group, inGroup := f.Group()
		go func() {
			d := time.Duration(-1)
			if err == nil && inGroup && r.Kind.RemovesLeader() {
				d = waitFor(fctx, func() bool { return c.agreedLeader(fctx, group, f.Targets) != 0 }, injected)
			}
			failover <- d
		}()
		// An outage of a zone: how long until each group's nodes outside
		// the zone agree on a leader among themselves.
		var perGroup map[int]time.Duration
		var pg sync.WaitGroup
		if err == nil && r.Kind == AZOutage {
			perGroup = map[int]time.Duration{}
			var mu sync.Mutex
			for _, g := range GroupIDs() {
				pg.Add(1)
				go func() {
					defer pg.Done()
					d := waitFor(fctx, func() bool { return c.agreedLeader(fctx, g, f.Targets) != 0 }, injected)
					mu.Lock()
					perGroup[g] = d
					mu.Unlock()
				}()
			}
		}
		sleep(ctx, holdFor)
		stopFailover()
		r.Failover = <-failover
		pg.Wait()
		if perGroup != nil {
			r.GroupFailover = perGroup
			for _, d := range perGroup {
				if d < 0 {
					r.Failover = -1
					break
				}
				r.Failover = max(r.Failover, d)
			}
		}
		outageFrom := injected

		var target uint64
		if inGroup {
			target = c.commitIndex(ctx, group)
		}
		hctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		herr := healRetry(hctx, f)
		cancel()
		healed := time.Now()
		r.HealAt = time.Since(start)
		outage = [2]time.Time{outageFrom, healed}
		if herr != nil {
			r.Reason = "heal failed: " + herr.Error()
		} else if inGroup && r.Kind != RollingRestart {
			rctx, cancel := context.WithTimeout(ctx, cfg.HealTimeout)
			r.Recovery = waitFor(rctx, func() bool { return c.caughtUp(rctx, group, target) }, healed)
			cancel()
		}
		f = nil
	}

	ld := <-loadDone
	_ = os.WriteFile(filepath.Join(dir, "console.txt"), []byte(ld.out), 0o644)
	if m := auditLine.FindString(ld.out); m != "" {
		r.Audit = strings.TrimSpace(m)
	}
	r.P99, r.Max = clientLatency(ld.out)
	if b, err := os.ReadFile(filepath.Join(dir, "audit.json")); err == nil {
		var a loadgen.Audit
		if json.Unmarshal(b, &a) == nil {
			r.AuditRules = &a
		}
	}

	_, bad, herr := c.WaitHealed(ctx, cfg.HealTimeout)
	if after, ok := c.Terms(ctx); ok && termsOK {
		r.Elections = 0
		for s, t := range after {
			r.Elections += int(t - termsBefore[s])
		}
	}

	if r.Kind == AZOutage && !outage[0].IsZero() {
		if answered, total, err := Availability(dir, outage[0], outage[1]); err == nil && total > 0 {
			r.Availability, r.OutageOrders = float64(answered)/float64(total), total
		}
	}
	var checkErr error
	if cfg.Linearizable {
		var hc HistoryCheck
		hc, checkErr = CheckHistory(dir, linearize.Options{Timeout: cfg.CheckTimeout, Widen: cfg.Widen})
		r.Linearizable, r.CheckDetail, r.CheckTime = hc.Bookings, hc.Detail, hc.Took
		r.StaleBrowses, r.StaleSeats = hc.StaleBrowses, hc.StaleSeats
		r.Idempotency = hc.Idempotency
	}
	writeFaultLog(dir, r, bad)

	r.decide(ld.err, herr, bad, checkErr)
	if r.Verdict == VerdictFail || r.Verdict == VerdictIdempotency {
		// Keep the evidence: the next run may reset the cluster, which
		// deletes the containers and their logs.
		lctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		logs, err := "", error(nil)
		if c.Remote != nil {
			logs, err = c.Remote.Logs(lctx)
		} else {
			logs, err = c.compose(lctx, "logs", "--no-color", "--timestamps")
		}
		if err == nil {
			_ = os.WriteFile(filepath.Join(dir, "cluster-logs.txt"), []byte(logs), 0o644)
		}
		cancel()
	}
	return r
}

// decide sets r.Verdict and r.Reason from what the run found: the load
// generator's error, the cluster's health after the run (healErr, bad), and
// whether the history check could run.
//
// A failure of any kind decides the run, whether or not the fault could be
// injected: a run is SKIP only when nothing failed and no fault was
// injected, and a TIMEOUT is reported as such.
func (r *Result) decide(loadErr, healErr error, bad []ShardHealth, checkErr error) {
	switch {
	case strings.HasPrefix(r.Audit, "audit FAIL"):
		r.Verdict, r.Reason = VerdictFail, "correctness audit failed"
	case r.Audit == "":
		r.Verdict, r.Reason = VerdictFail, "load generator gave no audit result: "+errString(loadErr)
	case healErr != nil:
		r.Verdict, r.Reason = VerdictFail, "cluster did not heal: "+describe(bad)
	case r.Reason != "":
		r.Verdict = VerdictFail
	case r.Linearizable == linearize.Fail:
		r.Verdict, r.Reason = VerdictFail, "bookings are not linearizable: "+r.CheckDetail
	case checkErr != nil:
		r.Verdict, r.Reason = VerdictFail, "the linearizability check could not run: "+checkErr.Error()
	case r.Linearizable == linearize.Timeout:
		r.Verdict, r.Reason = VerdictTimeout, "the linearizability check ran out of time: "+r.CheckDetail
	case r.Fault == "" && r.InjectErr != "":
		r.Verdict, r.Reason = VerdictSkip, "could not inject: "+r.InjectErr
	default:
		r.Verdict = VerdictPass
	}
	// Bookings that are not linearizable are a failure whatever else failed.
	// Stale browses are not: they are in the result and change nothing here.
	if r.Linearizable == linearize.Fail && r.Verdict == VerdictFail && !strings.Contains(r.Reason, "not linearizable") {
		r.Reason += "; bookings are not linearizable: " + r.CheckDetail
	}
	// One request answered in two ways is a failure of its own kind. With
	// another failure it is added to the reason; otherwise it decides the
	// run, whatever the fault did.
	if r.Idempotency != "" {
		why := "one request, two different answers: " + r.Idempotency
		if r.Verdict == VerdictFail {
			r.Reason += "; " + why
		} else {
			r.Verdict, r.Reason = VerdictIdempotency, why
		}
	}
	// A fault that was not (fully) injected is noted on any other verdict.
	if r.InjectErr != "" && r.Verdict != VerdictSkip {
		note := "partial inject: "
		if r.Fault == "" {
			note = "could not inject: "
		}
		if r.Reason != "" {
			note = "; " + note
		}
		r.Reason += note + r.InjectErr
	}
}

// HistoryCheck is what the Porcupine check found in one run's history.
type HistoryCheck struct {
	// Bookings is the verdict over the orders, the baseline and the final
	// read: linearize.Pass, Fail or Timeout, or "-" if the history could not
	// be read. It is the one that decides the run.
	Bookings string
	Detail   string // one line on Bookings
	// StaleBrowses is the number of browses that contradict the bookings
	// (-1 if the history could not be read) and StaleSeats the seats they
	// are stale for. BrowseUndecided is the number of seats whose browses
	// were not decided in time.
	StaleBrowses    int
	StaleSeats      []string
	BrowseUndecided int
	// Idempotency describes the requests that were answered in two
	// different ways; empty if there are none.
	Idempotency string
	Took        time.Duration // reading the history and checking it
}

// CheckLinearizable runs the Porcupine check on dir/history.jsonl. When the
// bookings are not linearizable it writes linearizability.html (Porcupine's
// visualization of the first bad seat) and linearizability.txt (that seat's
// operations) into dir. When browses are stale it writes stale-browses.txt:
// the seats, and the operations of the first of them.
func CheckLinearizable(dir string, timeout time.Duration) (HistoryCheck, error) {
	return CheckHistory(dir, linearize.Options{Timeout: timeout})
}

// CheckHistory is CheckLinearizable with every option of the check, such as
// Widen for a history merged from several machines.
func CheckHistory(dir string, opt linearize.Options) (HistoryCheck, error) {
	start := time.Now()
	hc := HistoryCheck{Bookings: "-", StaleBrowses: -1}
	f, err := os.Open(filepath.Join(dir, "history.jsonl"))
	if err != nil {
		return hc, err
	}
	h, err := loadgen.ReadFullHistory(f)
	f.Close()
	if err != nil {
		hc.Took = time.Since(start)
		return hc, err
	}
	if cs := loadgen.IdempotencyConflicts(h.Attempts); len(cs) > 0 {
		hc.Idempotency = fmt.Sprintf("request %s %s", cs[0].Request, cs[0].Detail)
		if len(cs) > 1 {
			hc.Idempotency += fmt.Sprintf(" (and %d more requests)", len(cs)-1)
		}
		var b strings.Builder
		for _, c := range cs {
			fmt.Fprintf(&b, "request %s %s\n", c.Request, c.Detail)
		}
		_ = os.WriteFile(filepath.Join(dir, "idempotency.txt"), []byte(b.String()), 0o644)
	}
	rep := linearize.Check(h, opt)
	hc.Took = time.Since(start)
	hc.Bookings = rep.Verdict()
	hc.StaleBrowses, hc.StaleSeats, hc.BrowseUndecided = rep.StaleBrowses, rep.StaleSeats, len(rep.BrowseUndecided)
	hc.Detail = fmt.Sprintf("%d seats, %d operations, %d browses", rep.Seats, rep.Operations, rep.Browses)
	switch rep.Verdict() {
	case linearize.Fail:
		hc.Detail = fmt.Sprintf("first bad seat %s (%d bad of %d seats)", rep.Illegal[0], len(rep.Illegal), rep.Seats)
		_ = os.WriteFile(filepath.Join(dir, "linearizability.txt"), []byte(rep.String()+"\n"+rep.FirstBad()), 0o644)
		if out, err := os.Create(filepath.Join(dir, "linearizability.html")); err == nil {
			_ = rep.Visualize(out)
			out.Close()
		}
	case linearize.Timeout:
		hc.Detail = fmt.Sprintf("%d of %d seats not decided", len(rep.TimedOut), rep.Seats)
	}
	if rep.StaleBrowses > 0 {
		body := rep.StaleLine() + "\nseats: " + strings.Join(rep.StaleSeats, " ") + "\n" + rep.FirstStale()
		_ = os.WriteFile(filepath.Join(dir, "stale-browses.txt"), []byte(body), 0o644)
	}
	return hc, nil
}

// outageLine describes an az-outage run: the zone, every group's failover
// and the availability during the outage.
func (r Result) outageLine() string {
	var groups []string
	for _, g := range GroupIDs() {
		if d, ok := r.GroupFailover[g]; ok {
			groups = append(groups, fmt.Sprintf("%s %ss", GroupName(g), secs(d)))
		}
	}
	availability := "unknown"
	if r.Availability >= 0 {
		availability = fmt.Sprintf("%.1f%% of %d orders answered", r.Availability*100, r.OutageOrders)
	}
	return fmt.Sprintf("zone %s down; failover per group: %s; availability during the outage: %s", r.Zone, strings.Join(groups, ", "), availability)
}

// staleLine is "stale browses: N (seats ...)", or "stale browses: -" if the
// browses were not checked.
func (r Result) staleLine() string {
	switch {
	case r.StaleBrowses < 0:
		return "stale browses: -"
	case r.StaleBrowses == 0:
		return "stale browses: 0"
	}
	return fmt.Sprintf("stale browses: %d (seats %s)", r.StaleBrowses, linearize.Sample(r.StaleSeats))
}

// waitFor polls cond until it holds and returns the time since from, or -1
// if ctx ends first.
func waitFor(ctx context.Context, cond func() bool, from time.Time) time.Duration {
	for {
		if cond() {
			return time.Since(from)
		}
		select {
		case <-ctx.Done():
			return -1
		case <-time.After(100 * time.Millisecond):
		}
	}
}

var allLatency = regexp.MustCompile(`(?m)^\s*all\s+\d+\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$`)

// clientLatency reads p99 and max from the "all" row of the load
// generator's latency table (count, p50, p90, p99, p99.9, max, in ms).
func clientLatency(out string) (p99, max time.Duration) {
	m := allLatency.FindStringSubmatch(out)
	if m == nil {
		return -1, -1
	}
	ms := func(s string) time.Duration {
		v, _ := strconv.ParseFloat(s, 64)
		return time.Duration(v * float64(time.Millisecond))
	}
	return ms(m[3]), ms(m[5])
}

func healRetry(ctx context.Context, f *Fault) error {
	var err error
	for range 3 {
		if err = f.Heal(ctx); err == nil {
			return nil
		}
		sleep(ctx, time.Second)
	}
	return err
}

func writeFaultLog(dir string, r Result, bad []ShardHealth) {
	var b strings.Builder
	fmt.Fprintf(&b, "fault: %s\ninject at: %.1fs\nheal at: %.1fs\n", r.Fault, r.InjectAt.Seconds(), r.HealAt.Seconds())
	if r.InjectErr != "" {
		fmt.Fprintf(&b, "inject error: %s\n", r.InjectErr)
	}
	fmt.Fprintf(&b, "failover: %ss\nrecovery: %ss (-1 = not measured)\n", secs(r.Failover), secs(r.Recovery))
	if r.Kind == AZOutage {
		fmt.Fprintf(&b, "%s\n", r.outageLine())
	}
	fmt.Fprintf(&b, "bookings: %s (%s, %ss)\n%s\n", r.Linearizable, r.CheckDetail, secs(r.CheckTime), r.staleLine())
	for _, h := range bad {
		fmt.Fprintf(&b, "not healed: %s: %s\n", GroupName(h.Shard), h.Err)
	}
	_ = os.WriteFile(filepath.Join(dir, "fault.txt"), []byte(b.String()), 0o644)
}

// Summarize renders a campaign's results as Markdown: totals (runs, the
// bookings verdict, stale browses), per fault kind the number of runs,
// failures, and recovery times, and one row per run.
func Summarize(results []Result) string {
	var b strings.Builder
	count := map[string]int{}
	lin := map[string]int{}
	checked := false
	var stale, staleRuns int
	for _, r := range results {
		count[r.Verdict]++
		lin[r.Linearizable]++
		checked = checked || (r.Linearizable != "" && r.Linearizable != "-")
		if r.StaleBrowses > 0 {
			stale, staleRuns = stale+r.StaleBrowses, staleRuns+1
		}
	}
	fmt.Fprintf(&b, "%d runs: %d PASS, %d FAIL, %d SKIP", len(results), count[VerdictPass], count[VerdictFail], count[VerdictSkip])
	if n := count[VerdictTimeout]; n > 0 {
		fmt.Fprintf(&b, ", %d TIMEOUT (bookings check not finished; not a pass)", n)
	}
	if n := count[VerdictIdempotency]; n > 0 {
		fmt.Fprintf(&b, ", %d IDEMPOTENCY (one request answered in two different ways; a failure)", n)
	}
	if n := count[VerdictCrash]; n > 0 {
		fmt.Fprintf(&b, ", %d CRASH (the run itself broke)", n)
	}
	b.WriteString("\n\n")
	if checked {
		fmt.Fprintf(&b, "Bookings verdict (Porcupine, per seat: orders, baseline and final read; a run passes or fails on it): %d pass, %d fail, %d timeout, %d not checked\n\n",
			lin[linearize.Pass], lin[linearize.Fail], lin[linearize.Timeout], len(results)-lin[linearize.Pass]-lin[linearize.Fail]-lin[linearize.Timeout])
		fmt.Fprintf(&b, "Stale browses (checked separately, reported, never a failure): %d in %d runs\n\n", stale, staleRuns)
	}
	fmt.Fprintf(&b, "| Fault | Runs | Pass | Fail | Skip | Failover, median | Failover, max | Recovery, median | Recovery, max | Elections per run | Client p99, median | Client max, median |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, k := range append(slices.Clone(AllKinds), Partition, AZOutage, None) {
		var n, pass, fail, skip int
		var fo, rec, p99, mx []float64
		elections, counted := 0, 0
		for _, r := range results {
			if r.Kind != k {
				continue
			}
			n++
			switch r.Verdict {
			case VerdictPass:
				pass++
				if r.Failover >= 0 {
					fo = append(fo, r.Failover.Seconds())
				}
				if r.Recovery >= 0 {
					rec = append(rec, r.Recovery.Seconds())
				}
				if r.Elections >= 0 {
					elections, counted = elections+r.Elections, counted+1
				}
				if r.P99 >= 0 {
					p99, mx = append(p99, r.P99.Seconds()), append(mx, r.Max.Seconds())
				}
			case VerdictFail, VerdictIdempotency:
				fail++
			case VerdictSkip:
				skip++
			}
		}
		if n == 0 {
			continue
		}
		fmed, fmax := medMax(fo)
		rmed, rmax := medMax(rec)
		pmed := medianMillis(p99)
		mmed := medianMillis(mx)
		el := "-"
		if counted > 0 {
			el = strconv.FormatFloat(float64(elections)/float64(counted), 'f', 1, 64)
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %s | %s | %s | %s | %s | %s | %s |\n", k, n, pass, fail, skip, fmed, fmax, rmed, rmax, el, pmed, mmed)
	}

	// One row per run. Audit rules: a double-booked, b confirmed but still
	// free, c taken with no order, d confirmed but pre-booked, e cross-shard
	// half-booked + taken by an unconfirmed order.
	b.WriteString("\nRuns (audit columns are violation counts per rule; e is half-booked + taken-by-unconfirmed; Bookings is the Porcupine verdict, stale browses are reported only):\n\n")
	b.WriteString("| Run | Fault | Target | Failover | Audit | a | b | c | d | e | Bookings | Stale browses | Check time | UNKNOWN | Elections | Verdict |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	var tot loadgen.Audit
	var audits, auditPass, unknown, elections int
	var checkTime time.Duration
	for _, r := range results {
		audit, cols := "-", "- | - | - | - | -"
		if a := r.AuditRules; a != nil {
			audits++
			audit = "FAIL"
			if a.Pass {
				audit = "PASS"
				auditPass++
			}
			cols = fmt.Sprintf("%d | %d | %d | %d | %d+%d", a.DoubleBooked, a.StillFree, a.Unexplained, a.PreBooked, a.HalfBooked, a.TakenByFailed)
			tot.DoubleBooked += a.DoubleBooked
			tot.StillFree += a.StillFree
			tot.Unexplained += a.Unexplained
			tot.PreBooked += a.PreBooked
			tot.HalfBooked += a.HalfBooked
			tot.TakenByFailed += a.TakenByFailed
			unknown += a.UnknownOrders
		}
		failover, check, lz := "-", "-", r.Linearizable
		if r.Failover >= 0 {
			failover = secs(r.Failover) + " s"
		}
		if lz == "" {
			lz = "-"
		}
		if lz != "-" {
			check = fmt.Sprintf("%.2f s", r.CheckTime.Seconds())
			checkTime += r.CheckTime
		}
		if r.Elections > 0 {
			elections += r.Elections
		}
		target := r.Target
		if target == "" {
			target = "-"
		}
		staleCol := dash(r.StaleBrowses)
		if r.StaleBrowses > 0 {
			staleCol += " (seats " + linearize.Sample(r.StaleSeats) + ")"
		}
		fmt.Fprintf(&b, "| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", r.Run, r.Kind, target, failover, audit, cols, lz, staleCol, check, dash(r.Unknown()), dash(r.Elections), r.Verdict)
	}
	fmt.Fprintf(&b, "| **Total** | %d runs | | | %d/%d pass | %d | %d | %d | %d | %d+%d | %d pass, %d fail, %d timeout | %d in %d runs | %.2f s | %d | %d | %d PASS |\n",
		len(results), auditPass, audits, tot.DoubleBooked, tot.StillFree, tot.Unexplained, tot.PreBooked, tot.HalfBooked, tot.TakenByFailed,
		lin[linearize.Pass], lin[linearize.Fail], lin[linearize.Timeout], stale, staleRuns, checkTime.Seconds(), unknown, elections, count[VerdictPass])

	outages := false
	for _, r := range results {
		if r.Kind == AZOutage && r.Zone != "" {
			if !outages {
				b.WriteString("\nZone outages (every SeatSafe container of one zone killed at once; a failover of -1 means the group had no leader while the zone was down):\n\n")
				outages = true
			}
			fmt.Fprintf(&b, "- run %d: %s\n", r.Run, r.outageLine())
		}
	}

	var fails []string
	for _, r := range results {
		if r.Verdict != VerdictPass && r.Verdict != VerdictSkip {
			what := r.Fault
			if what == "" {
				what = string(r.Kind)
			}
			if what == "" {
				what = "not started"
			}
			fails = append(fails, fmt.Sprintf("- run %d: %s: %s", r.Run, what, r.Reason))
		}
	}
	if len(fails) > 0 {
		b.WriteString("\nFailures:\n" + strings.Join(fails, "\n") + "\n")
	}
	return b.String()
}

// dash renders a count, or "-" for an unknown one (-1).
func dash(n int) string {
	if n < 0 {
		return "-"
	}
	return strconv.Itoa(n)
}

// medianMillis renders the median of v (seconds) in milliseconds: client
// latencies are often well under 0.1 s, which "%.1f s" would show as 0.0 s.
func medianMillis(v []float64) string {
	if len(v) == 0 {
		return "-"
	}
	slices.Sort(v)
	return fmt.Sprintf("%.0f ms", v[len(v)/2]*1000)
}

func medMax(v []float64) (string, string) {
	if len(v) == 0 {
		return "-", "-"
	}
	slices.Sort(v)
	return fmt.Sprintf("%.1f s", v[len(v)/2]), fmt.Sprintf("%.1f s", v[len(v)-1])
}

func describe(bad []ShardHealth) string {
	var parts []string
	for _, h := range bad {
		parts = append(parts, fmt.Sprintf("%s: %s", GroupName(h.Shard), h.Err))
	}
	return strings.Join(parts, "; ")
}

func between(r *rand.Rand, d [2]time.Duration) time.Duration {
	if d[1] <= d[0] {
		return d[0]
	}
	return d[0] + time.Duration(r.Int64N(int64(d[1]-d[0])))
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func millis(d time.Duration) string {
	if d < 0 {
		return "-1"
	}
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 0, 64)
}

func secs(d time.Duration) string {
	if d < 0 {
		return "-1"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', 1, 64)
}

// gib renders bytes as GiB with one decimal, or "-1" for unknown.
func gib(b int64) string {
	if b < 0 {
		return "-1"
	}
	return strconv.FormatFloat(float64(b)/(1<<30), 'f', 1, 64)
}

func errString(err error) string {
	if err == nil {
		return "exited without one"
	}
	return err.Error()
}
