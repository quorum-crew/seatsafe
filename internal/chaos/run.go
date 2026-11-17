package chaos

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config is one chaos campaign: Runs load runs, each with one fault.
type Config struct {
	Runs       int
	ResetEvery int // wipe and restart the cluster every N runs (0 = only before the first)
	Seed       uint64

	Kinds       []Kind
	InjectAfter [2]time.Duration // fault starts this long into the load (min, max)
	FaultFor    [2]time.Duration // and is healed this long later (min, max)
	HealTimeout time.Duration    // time allowed for every shard to recover after the load

	// Load generator, run as a container on the cluster network.
	Image     string // e.g. seatsafe:dev
	DeployDir string // host path holding shardmap.json and seed/
	Workload  string
	Rate      float64
	Duration  time.Duration
	ExtraArgs []string

	Out        string // host directory for results
	StopOnFail bool
	Logf       func(format string, args ...any)
}

// Result is the outcome of one run.
type Result struct {
	Run       int
	Fault     string
	Kind      Kind
	InjectAt  time.Duration // into the load
	HealAt    time.Duration
	InjectErr string
	Audit     string // the load generator's audit line
	// Failover is the time from the fault until the rest of the shard
	// agreed on a new leader, for faults that take the leader away; -1 if
	// not measured. Recovery is the time from healing until every replica
	// was back, agreed on the leader and had applied everything committed
	// when the fault was healed; -1 if not measured. Both are sampled about
	// every 100-300 ms (one docker exec per node), so they are approximate.
	Failover time.Duration
	Recovery time.Duration
	Verdict  string // PASS, FAIL, or SKIP (the fault could not be injected)
	Reason   string
}

var auditLine = regexp.MustCompile(`(?m)^\s*audit (PASS|FAIL).*$`)

// Campaign runs cfg against c and returns every run's result.
func Campaign(ctx context.Context, c *Cluster, cfg Config) ([]Result, error) {
	if len(cfg.Kinds) == 0 {
		cfg.Kinds = AllKinds
	}
	if err := os.MkdirAll(cfg.Out, 0o755); err != nil {
		return nil, err
	}
	csvFile, err := os.Create(filepath.Join(cfg.Out, "runs.csv"))
	if err != nil {
		return nil, err
	}
	defer csvFile.Close()
	w := csv.NewWriter(csvFile)
	_ = w.Write([]string{"run", "verdict", "kind", "fault", "inject_at_s", "heal_at_s", "failover_s", "recovery_s", "audit", "reason"})
	w.Flush()

	rnd := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	in := &Injector{C: c, Rand: rnd}
	var results []Result
	for i := 1; i <= cfg.Runs && ctx.Err() == nil; i++ {
		if i == 1 || (cfg.ResetEvery > 0 && (i-1)%cfg.ResetEvery == 0) {
			cfg.Logf("run %d: resetting the cluster", i)
			if err := c.Reset(ctx); err != nil {
				return results, fmt.Errorf("reset: %w", err)
			}
		}
		if _, bad, err := c.WaitHealed(ctx, cfg.HealTimeout); err != nil {
			// A run left the cluster broken: that run already failed. Start
			// clean so later runs still mean something.
			cfg.Logf("run %d: cluster unhealthy before the run (%v), resetting", i, describe(bad))
			if err := c.Reset(ctx); err != nil {
				return results, fmt.Errorf("reset: %w", err)
			}
		}
		r := runOnce(ctx, c, in, rnd, cfg, i)
		results = append(results, r)
		cfg.Logf("run %d: %s  %s  failover %s  recovery %s  %s %s", i, r.Verdict, r.Fault, secs(r.Failover), secs(r.Recovery), r.Audit, r.Reason)
		_ = w.Write([]string{strconv.Itoa(r.Run), r.Verdict, string(r.Kind), r.Fault,
			secs(r.InjectAt), secs(r.HealAt), secs(r.Failover), secs(r.Recovery), r.Audit, r.Reason})
		w.Flush()
		if r.Verdict == "FAIL" && cfg.StopOnFail {
			cfg.Logf("stopping after a failure; the cluster is left as it is for inspection")
			break
		}
	}
	return results, w.Error()
}

func runOnce(ctx context.Context, c *Cluster, in *Injector, rnd *rand.Rand, cfg Config, i int) Result {
	r := Result{Run: i, Kind: cfg.Kinds[rnd.IntN(len(cfg.Kinds))], Failover: -1, Recovery: -1}
	dir := filepath.Join(cfg.Out, fmt.Sprintf("run-%04d", i))
	_ = os.MkdirAll(dir, 0o755)
	abs, _ := filepath.Abs(dir)
	deploy, _ := filepath.Abs(cfg.DeployDir)

	// Load in the background.
	name := c.Project + "-chaos-loadgen"
	_, _ = c.Docker.Run(context.Background(), "rm", "-f", name)
	args := []string{"run", "--rm", "--name", name, "--network", c.Network(),
		"-v", deploy + ":/deploy:ro", "-v", abs + ":/out", cfg.Image,
		"loadgen", "-target", "router:7000", "-shardmap", "/deploy/shardmap.json", "-seed-dir", "/deploy/seed",
		"-out", "/out", "-label", fmt.Sprintf("chaos-%04d", i), "-mode", "open", "-workload", cfg.Workload,
		"-rate", strconv.FormatFloat(cfg.Rate, 'f', -1, 64), "-warmup", "2s", "-duration", cfg.Duration.String(),
		"-seed", strconv.FormatUint(cfg.Seed+uint64(i), 10), "-history", "-quiet"}
	args = append(args, cfg.ExtraArgs...)
	type loadResult struct {
		out string
		err error
	}
	loadDone := make(chan loadResult, 1)
	start := time.Now()
	go func() {
		out, err := c.Docker.Run(ctx, args...)
		loadDone <- loadResult{out, err}
	}()
	defer func() {
		if ctx.Err() != nil {
			_, _ = c.Docker.Run(context.Background(), "rm", "-f", name)
		}
	}()

	// The fault: inject, hold, heal. Healing always runs, even on Ctrl-C,
	// so the cluster is never left broken by an interrupted run.
	sleep(ctx, between(rnd, cfg.InjectAfter))
	r.InjectAt = time.Since(start)
	f, err := in.Inject(ctx, r.Kind)
	if f != nil {
		r.Fault = f.String()
		defer func() {
			if f != nil {
				_ = healRetry(context.Background(), f)
			}
		}()
	}
	if err != nil {
		r.InjectErr = err.Error()
	}
	if f != nil {
		injected := time.Now()
		holdFor := between(rnd, cfg.FaultFor)
		failover := make(chan time.Duration, 1)
		fctx, stopFailover := context.WithTimeout(ctx, holdFor)
		go func() {
			d := time.Duration(-1)
			if err == nil && r.Kind.RemovesLeader() {
				d = waitFor(fctx, func() bool { return c.agreedLeader(fctx, f.Shard, f.Targets) != 0 }, injected)
			}
			failover <- d
		}()
		sleep(ctx, holdFor)
		stopFailover()
		r.Failover = <-failover

		var target uint64
		if f.Shard != 0 {
			target = c.commitIndex(ctx, f.Shard)
		}
		hctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		herr := healRetry(hctx, f)
		cancel()
		healed := time.Now()
		r.HealAt = time.Since(start)
		if herr != nil {
			r.Reason = "heal failed: " + herr.Error()
		} else if f.Shard != 0 && r.Kind != RollingRestart {
			rctx, cancel := context.WithTimeout(ctx, cfg.HealTimeout)
			shard := f.Shard
			r.Recovery = waitFor(rctx, func() bool { return c.caughtUp(rctx, shard, target) }, healed)
			cancel()
		}
		f = nil
	}

	ld := <-loadDone
	_ = os.WriteFile(filepath.Join(dir, "console.txt"), []byte(ld.out), 0o644)
	if m := auditLine.FindString(ld.out); m != "" {
		r.Audit = strings.TrimSpace(m)
	}

	_, bad, herr := c.WaitHealed(ctx, cfg.HealTimeout)
	writeFaultLog(dir, r, bad)

	switch {
	case r.Fault == "" && r.InjectErr != "":
		r.Verdict, r.Reason = "SKIP", "could not inject: "+r.InjectErr
	case strings.HasPrefix(r.Audit, "audit FAIL"):
		r.Verdict, r.Reason = "FAIL", "correctness audit failed"
	case r.Audit == "":
		r.Verdict, r.Reason = "FAIL", "load generator gave no audit result: "+errString(ld.err)
	case herr != nil:
		r.Verdict, r.Reason = "FAIL", "cluster did not heal: "+describe(bad)
	case r.Reason != "":
		r.Verdict = "FAIL"
	default:
		r.Verdict = "PASS"
	}
	if r.InjectErr != "" && r.Verdict != "SKIP" && r.Reason == "" {
		r.Reason = "partial inject: " + r.InjectErr
	}
	if r.Verdict == "FAIL" {
		// Keep the evidence: the next run may reset the cluster, which
		// deletes the containers and their logs.
		lctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		if logs, err := c.compose(lctx, "logs", "--no-color", "--timestamps"); err == nil {
			_ = os.WriteFile(filepath.Join(dir, "cluster-logs.txt"), []byte(logs), 0o644)
		}
		cancel()
	}
	return r
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
	for _, h := range bad {
		fmt.Fprintf(&b, "not healed: shard %d: %s\n", h.Shard, h.Err)
	}
	_ = os.WriteFile(filepath.Join(dir, "fault.txt"), []byte(b.String()), 0o644)
}

// Summarize renders a campaign's results as Markdown: totals, then per fault
// kind the number of runs, failures, and recovery times.
func Summarize(results []Result) string {
	var b strings.Builder
	count := map[string]int{}
	for _, r := range results {
		count[r.Verdict]++
	}
	fmt.Fprintf(&b, "%d runs: %d PASS, %d FAIL, %d SKIP\n\n", len(results), count["PASS"], count["FAIL"], count["SKIP"])
	fmt.Fprintf(&b, "| Fault | Runs | Pass | Fail | Skip | Failover, median | Failover, max | Recovery, median | Recovery, max |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, k := range AllKinds {
		var n, pass, fail, skip int
		var fo, rec []float64
		for _, r := range results {
			if r.Kind != k {
				continue
			}
			n++
			switch r.Verdict {
			case "PASS":
				pass++
				if r.Failover >= 0 {
					fo = append(fo, r.Failover.Seconds())
				}
				if r.Recovery >= 0 {
					rec = append(rec, r.Recovery.Seconds())
				}
			case "FAIL":
				fail++
			default:
				skip++
			}
		}
		if n == 0 {
			continue
		}
		fmed, fmax := medMax(fo)
		rmed, rmax := medMax(rec)
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %s | %s | %s | %s |\n", k, n, pass, fail, skip, fmed, fmax, rmed, rmax)
	}
	var fails []string
	for _, r := range results {
		if r.Verdict == "FAIL" {
			fails = append(fails, fmt.Sprintf("- run %d: %s: %s", r.Run, r.Fault, r.Reason))
		}
	}
	if len(fails) > 0 {
		b.WriteString("\nFailures:\n" + strings.Join(fails, "\n") + "\n")
	}
	return b.String()
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
		parts = append(parts, fmt.Sprintf("shard %d: %s", h.Shard, h.Err))
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

func secs(d time.Duration) string {
	if d < 0 {
		return "-1"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', 1, 64)
}

func errString(err error) string {
	if err == nil {
		return "exited without one"
	}
	return err.Error()
}
