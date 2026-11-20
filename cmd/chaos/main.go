// Command chaos runs the load generator against the Docker Compose cluster
// again and again, injecting one fault into each run, and checks after every
// run that no seat was double-booked, that the history is linearizable and
// that every Raft group recovered.
//
// Build the image and generate a dataset first, then run it from the repo
// root:
//
//	go run ./cmd/datagen -out deploy
//	docker compose build
//	go run ./cmd/chaos -runs 500
//
// Each run: the load generator books seats for -duration; partway through,
// one fault (picked at random from -faults) is injected on a random shard and
// healed a few seconds later. After the load, the run passes only if the
// load generator's correctness audit passed, the history is linearizable,
// and every shard and the coordinator healed within -heal-timeout: one
// leader that every replica agrees on, and all three replicas at the same
// applied index.
//
// Results go to results/chaos-<timestamp>/: runs.csv (one line per run),
// summary.md (per fault kind: runs, failures, recovery times; then one row
// per run and totals), and run-NNNN/ with the load generator's output and
// history for each run.
//
// Faults: kill-leader, kill-follower, isolate-leader, isolate-follower,
// pause-leader, quorum-loss, delay-shard, kill-router, rolling-restart, and
// partition (cut the leader of the -target group off the network with docker
// network disconnect, then reconnect it). delay-shard needs tc (iproute2) in
// the image.
//
// The history of every run is checked for linearizability with Porcupine
// (-linearizable, on by default; the model is described in
// internal/linearize). A check that runs out of time (-check-timeout) makes
// the run a TIMEOUT, not a pass.
//
// -campaign runs the unattended mix instead of random faults: kill-leader,
// pause-leader, partition and a no-fault control in turn, the faults
// alternating between a shard and the coordinator, with a fifth of the
// orders cross-shard, browses for the linearizability check, and at least
// 3 GiB of memory free before each run. One run breaking does not end the
// campaign, and summary.md is rewritten after every run:
//
//	go run ./cmd/chaos -campaign -runs 20
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/quorum-crew/seatsafe/internal/chaos"
)

// campaignDefaults are the flag defaults -campaign sets, unless the flag was
// given.
var campaignDefaults = map[string]string{
	"rate": "300", "workload": "flash", "cross-shard-frac": "0.2", "browse-rate": "5", "reset-every": "5", "min-free-mem": "3",
}

func main() {
	var (
		runs        = flag.Int("runs", 20, "number of runs")
		resetEvery  = flag.Int("reset-every", 10, "wipe and restart the cluster every N runs (0 = only before the first)")
		seed        = flag.Uint64("seed", uint64(time.Now().UnixNano()), "random seed; the same seed replays the same faults and orders")
		faults      = flag.String("faults", "all", "comma-separated fault kinds, or all")
		duration    = flag.Duration("duration", 30*time.Second, "load per run")
		rate        = flag.Float64("rate", 200, "orders per second")
		workload    = flag.String("workload", "uniform", "load generator workload: uniform, flash or fresh")
		injectMin   = flag.Duration("inject-min", 5*time.Second, "earliest fault start, into the load")
		injectMax   = flag.Duration("inject-max", 10*time.Second, "latest fault start")
		faultMin    = flag.Duration("fault-min", 3*time.Second, "shortest fault")
		faultMax    = flag.Duration("fault-max", 10*time.Second, "longest fault")
		healTimeout = flag.Duration("heal-timeout", 60*time.Second, "time allowed after the load for every group to recover")
		project     = flag.String("project", "seatsafe", "compose project name")
		files       = flag.String("compose-files", "docker-compose.yml", "comma-separated compose files")
		image       = flag.String("image", "seatsafe:dev", "image with the loadgen binary")
		deploy      = flag.String("deploy", "deploy", "directory holding shardmap.json and seed/")
		out         = flag.String("out", "", "results directory (default results/chaos-<timestamp>)")
		stopOnFail  = flag.Bool("stop-on-fail", false, "stop at the first failure and leave the cluster as it is")

		campaign     = flag.Bool("campaign", false, "unattended mix: kill-leader, pause-leader, partition and none in turn, on a shard and on the coordinator; changes the defaults to -rate 300 -workload flash -cross-shard-frac 0.2 -browse-rate 5 -reset-every 5 -min-free-mem 3")
		target       = flag.String("target", "shard", "where the faults go when not in -campaign: shard or coordinator (the coordinator takes kill-leader, kill-follower, isolate-leader, isolate-follower, pause-leader and partition)")
		crossFrac    = flag.Float64("cross-shard-frac", 0, "fraction of orders across two shards (2PC)")
		browseRate   = flag.Float64("browse-rate", 0, "ListAvailableSeats calls per second during the load, recorded for the linearizability check")
		linearizable = flag.Bool("linearizable", true, "check every run's history for linearizability with Porcupine")
		checkTimeout = flag.Duration("check-timeout", 2*time.Minute, "time allowed for one linearizability check; running out makes the run a TIMEOUT, not a pass")
		minFreeMem   = flag.Float64("min-free-mem", 0, "GiB of memory that must be available to the containers (Docker's MemAvailable) before each run; 0 = don't check")
	)
	flag.Parse()
	if *campaign {
		given := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { given[f.Name] = true })
		for name, value := range campaignDefaults {
			if !given[name] {
				if err := flag.Set(name, value); err != nil {
					log.Fatal(err)
				}
			}
		}
	}

	kinds, err := chaos.ParseKinds(*faults)
	if err != nil {
		log.Fatal(err)
	}
	if *injectMax+*faultMax+5*time.Second > *duration {
		log.Fatalf("-duration %v is too short: faults can last until %v, and the load should outlast them", *duration, *injectMax+*faultMax)
	}
	if *out == "" {
		*out = filepath.Join("results", "chaos-"+time.Now().Format("20060102-150405"))
	}
	cfg := chaos.Config{
		Runs: *runs, ResetEvery: *resetEvery, Seed: *seed, Kinds: kinds,
		InjectAfter: [2]time.Duration{*injectMin, *injectMax}, FaultFor: [2]time.Duration{*faultMin, *faultMax},
		HealTimeout: *healTimeout, Image: *image, DeployDir: *deploy, Workload: *workload, Rate: *rate,
		Duration: *duration, Out: *out, StopOnFail: *stopOnFail, Logf: log.Printf,
		CrossShardFrac: *crossFrac, BrowseRate: *browseRate, Linearizable: *linearizable, CheckTimeout: *checkTimeout,
		MinFreeMem: uint64(*minFreeMem * (1 << 30)),
	}
	what := fmt.Sprintf("faults %v on a random shard", kinds)
	switch {
	case *campaign:
		cfg.Plan = chaos.Mix
		what = "the campaign mix (kill-leader, pause-leader, partition, none; shard and coordinator)"
	case *target == string(chaos.TargetCoordinator):
		for _, k := range kinds {
			cfg.Plan = append(cfg.Plan, chaos.Step{Kind: k, Target: chaos.TargetCoordinator})
		}
		what = fmt.Sprintf("faults %v on the coordinator, in turn", kinds)
	case *target != string(chaos.TargetShard):
		log.Fatalf("unknown -target %q (shard or coordinator)", *target)
	}
	cfg.Title = fmt.Sprintf("# Chaos campaign\n\nseed %d, %v load at %.0f orders/s (%s), %.0f%% cross-shard, %.0f browses/s; %s\n\n",
		*seed, *duration, *rate, *workload, *crossFrac*100, *browseRate, what)
	c := &chaos.Cluster{Docker: chaos.CLI{}, Project: *project, Files: strings.Split(*files, ",")}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("chaos: %d runs, seed %d, %s, results in %s", *runs, *seed, what, *out)
	results, err := chaos.Campaign(ctx, c, cfg)
	summary := chaos.Summarize(results)
	_ = os.WriteFile(filepath.Join(*out, "summary.md"), []byte(cfg.Title+summary), 0o644)
	fmt.Print("\n" + summary)
	if err != nil {
		log.Fatalf("chaos: %v", err)
	}
	for _, r := range results {
		if r.Verdict != chaos.VerdictPass && r.Verdict != chaos.VerdictSkip {
			os.Exit(2) // a failure, a check that ran out of time, or a run that broke
		}
	}
}
