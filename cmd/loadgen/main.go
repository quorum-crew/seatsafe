// Command loadgen drives BookingService and reports throughput, latency and
// correctness. See internal/loadgen.
//
//	go run ./cmd/loadgen -mode open -workload fresh -rate 2000 -duration 60s
//	go run ./cmd/loadgen -mode closed -workload flash -users 10000 -duration 60s
//	go run ./cmd/loadgen -mode ramp -workload fresh
//	go run ./cmd/loadgen -table results/a/summary.json results/b/summary.json
//
// Several loadgen processes ("workers"), on different machines, can drive one
// cluster together. Each gets its number and the number of workers, and they
// start at the same wall-clock time. One baseline and one final snapshot of
// the seats are taken around them, and merge makes one run of the results:
//
//	go run ./cmd/loadgen snapshot -phase baseline -workload fresh -out results/r/baseline.jsonl
//	go run ./cmd/loadgen -worker-id 0 -workers 2 -start-at 1767225600 -history -workload fresh -out results/r/w0
//	go run ./cmd/loadgen -worker-id 1 -workers 2 -start-at 1767225600 -history -workload fresh -out results/r/w1
//	go run ./cmd/loadgen snapshot -phase final -workload fresh -out results/r/final.jsonl
//	go run ./cmd/loadgen merge -out results/r/merged -baseline results/r/baseline.jsonl -final results/r/final.jsonl results/r/w0 results/r/w1
//
// Exit code 2 means the correctness audit failed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "snapshot":
			os.Exit(snapshot(os.Args[2:]))
		case "merge":
			os.Exit(merge(os.Args[2:]))
		}
	}
	c := loadgen.Defaults()
	flag.StringVar(&c.Target, "target", c.Target, "BookingService address")
	flag.IntVar(&c.Conns, "conns", c.Conns, "gRPC connections, used round-robin")
	flag.StringVar(&c.ShardMap, "shardmap", c.ShardMap, "shard map file")
	flag.StringVar(&c.SeedDir, "seed-dir", c.SeedDir, "directory of the seed files")
	flag.StringVar(&c.Mode, "mode", c.Mode, "open, closed or ramp")
	flag.StringVar(&c.Workload, "workload", c.Workload, "uniform, flash or fresh")
	flag.Float64Var(&c.Rate, "rate", c.Rate, "open: orders per second")
	flag.DurationVar(&c.Duration, "duration", c.Duration, "measured time")
	flag.DurationVar(&c.Warmup, "warmup", c.Warmup, "time before measuring starts")
	flag.IntVar(&c.MaxInflight, "max-inflight", c.MaxInflight, "open: outstanding orders before orders are dropped and counted")
	flag.IntVar(&c.Users, "users", c.Users, "closed: virtual users")
	flag.DurationVar(&c.Think, "think", c.Think, "closed: wait between a user's orders")
	flag.Float64Var(&c.RateStart, "rate-start", c.RateStart, "ramp: first rate")
	flag.Float64Var(&c.RateEnd, "rate-end", c.RateEnd, "ramp: last rate")
	flag.Float64Var(&c.RateStep, "rate-step", c.RateStep, "ramp: rate added per step")
	flag.DurationVar(&c.StepDuration, "step-duration", c.StepDuration, "ramp: time per step")
	flag.DurationVar(&c.SLO, "slo", c.SLO, "ramp: p99 limit")
	flag.Float64Var(&c.ZipfS, "zipf-s", c.ZipfS, "flash: Zipf exponent for sections and rows, above 1")
	flag.StringVar(&c.OrderSizes, "order-sizes", c.OrderSizes, "seats per order as size:weight,...")
	flag.Float64Var(&c.CrossShardFrac, "cross-shard-frac", c.CrossShardFrac, "fraction of orders across two shards (needs the real coordinator)")
	flag.Uint64Var(&c.Seed, "seed", c.Seed, "random seed for picking seats")
	flag.Float64Var(&c.BrowseRate, "browse-rate", c.BrowseRate, "ListAvailableSeats calls per second during the load, written to the history (0 = none)")
	flag.IntVar(&c.WorkerID, "worker-id", c.WorkerID, "which worker this is, from 0, when several loadgen processes drive one cluster")
	flag.IntVar(&c.Workers, "workers", c.Workers, "how many loadgen processes drive the cluster; above 1 this one does not audit (see loadgen merge)")
	flag.Float64Var(&c.StartAt, "start-at", c.StartAt, "unix time in seconds (may have a fraction) at which the load starts, after the setup; 0 = at once")
	flag.DurationVar(&c.CallTimeout, "call-timeout", c.CallTimeout, "deadline per call")
	flag.DurationVar(&c.RetryBudget, "retry-budget", c.RetryBudget, "time an order may keep retrying")
	flag.StringVar(&c.Out, "out", "", "output directory (default results/<timestamp>-<label>)")
	flag.StringVar(&c.Label, "label", "run", "name of this run")
	flag.BoolVar(&c.History, "history", false, "write history.jsonl")
	flag.BoolVar(&c.Audit, "audit", c.Audit, "check correctness after the run")
	flag.StringVar(&c.GitCommit, "git-commit", "", "commit to record (default: ask git)")
	flag.BoolVar(&c.Quiet, "quiet", false, "no live lines")
	table := flag.Bool("table", false, "print a markdown table of the summary.json files given as arguments, and exit")
	flag.Parse()

	if *table {
		if err := printTable(flag.Args()); err != nil {
			fatal(err)
		}
		return
	}
	if c.GitCommit == "" {
		c.GitCommit = gitCommit()
	}
	if err := c.Validate(); err != nil {
		fatal(err)
	}
	u, err := loadgen.LoadUniverse(c.ShardMap, c.SeedDir)
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	sum, err := loadgen.Run(ctx, c, u, loadgen.Env{})
	if err != nil {
		fatal(err)
	}
	fmt.Print(loadgen.PrintSummary(sum))
	if sum.Audit != nil && !sum.Audit.Pass {
		fmt.Fprintln(os.Stderr, loadgen.ErrAuditFailed)
		os.Exit(2)
	}
}

// snapshot is "loadgen snapshot": the seats that are free now, as history
// lines with absolute times.
func snapshot(args []string) int {
	c := loadgen.Defaults()
	c.Conns = 4
	fs := flag.NewFlagSet("loadgen snapshot", flag.ContinueOnError)
	phase := fs.String("phase", "", "baseline (before the workers start) or final (after they finished)")
	out := fs.String("out", "", "file to write, one line per section")
	fs.StringVar(&c.Target, "target", c.Target, "BookingService address")
	fs.IntVar(&c.Conns, "conns", c.Conns, "gRPC connections, used round-robin")
	fs.StringVar(&c.ShardMap, "shardmap", c.ShardMap, "shard map file")
	fs.StringVar(&c.SeedDir, "seed-dir", c.SeedDir, "directory of the seed files")
	fs.StringVar(&c.Workload, "workload", c.Workload, "uniform, flash or fresh: the workload whose sections are read")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: loadgen snapshot -phase baseline|final -out file.jsonl [-target addr] [-shardmap file] [-seed-dir dir] [-workload w]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *out == "" || fs.NArg() != 0 {
		fs.Usage()
		return 1
	}
	u, err := loadgen.LoadUniverse(c.ShardMap, c.SeedDir)
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		err = loadgen.Snapshot(ctx, c, u, loadgen.Env{}, *phase, *out)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// merge is "loadgen merge": one run out of the output directories of its
// workers. It reads and writes files only.
func merge(args []string) int {
	var in loadgen.MergeInput
	fs := flag.NewFlagSet("loadgen merge", flag.ContinueOnError)
	fs.StringVar(&in.Out, "out", "", "directory to write the merged run to")
	fs.StringVar(&in.Baseline, "baseline", "", "snapshot taken before the workers started")
	fs.StringVar(&in.Final, "final", "", "snapshot taken after the workers finished; with -baseline, the audit is run")
	fs.StringVar(&in.Label, "label", "", "name of the merged run (default: worker 0's)")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: loadgen merge -out DIR [-baseline base.jsonl] [-final final.jsonl] [-label x] WORKERDIR...")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if in.Out == "" || fs.NArg() == 0 {
		fs.Usage()
		return 1
	}
	in.Workers = fs.Args()
	sum, err := loadgen.Merge(in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Print(loadgen.PrintSummary(sum))
	if sum.Audit != nil && !sum.Audit.Pass {
		if sum.Audit.Unsettled() {
			fmt.Fprintln(os.Stderr, "loadgen: the audit is unsettled: only seats still held by aborted cross-shard orders fail it; take a later final snapshot and merge again")
		}
		fmt.Fprintln(os.Stderr, loadgen.ErrAuditFailed)
		return 2
	}
	return 0
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printTable(paths []string) error {
	fmt.Println("| Run | Attempted/s | Achieved/s | Confirmed orders/s | Confirmed seats/s | Conflict | p50 ms | p99 ms | p99.9 ms | Max ms | Unknown | Retries | Dropped | Audit |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var s loadgen.Summary
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		r := s.Results
		audit := "off"
		if s.Audit != nil {
			audit = "FAIL"
			if s.Audit.Pass {
				audit = "PASS"
			}
		}
		fmt.Printf("| %s | %.0f | %.0f | %.0f | %.0f | %.1f%% | %.2f | %.2f | %.2f | %.2f | %d | %d | %d | %s |\n",
			s.Config.Label, r.AttemptedRate, r.AchievedRate, r.ConfirmedRate, r.ConfirmedSeats, r.ConflictRate*100,
			r.Latency.P50, r.Latency.P99, r.Latency.P999, r.Latency.Max, r.Outcomes[loadgen.OutcomeUnknown], r.Retries, r.Dropped, audit)
	}
	return nil
}
