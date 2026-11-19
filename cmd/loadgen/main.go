// Command loadgen drives BookingService and reports throughput, latency and
// correctness. See internal/loadgen.
//
//	go run ./cmd/loadgen -mode open -workload fresh -rate 2000 -duration 60s
//	go run ./cmd/loadgen -mode closed -workload flash -users 10000 -duration 60s
//	go run ./cmd/loadgen -mode ramp -workload fresh
//	go run ./cmd/loadgen -table results/a/summary.json results/b/summary.json
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
