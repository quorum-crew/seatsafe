// Command chaos runs the load generator against the Docker Compose cluster
// again and again, injecting one fault into each run, and checks after every
// run that no seat was double-booked and that every shard recovered.
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
// load generator's correctness audit passed and every shard healed within
// -heal-timeout: one leader that every replica agrees on, and all three
// replicas at the same applied index.
//
// Results go to results/chaos-<timestamp>/: runs.csv (one line per run),
// summary.md (per fault kind: runs, failures, recovery times), and run-NNNN/
// with the load generator's output and history for each run.
//
// Faults: kill-leader, kill-follower, isolate-leader, isolate-follower,
// pause-leader, quorum-loss, delay-shard, kill-router, rolling-restart.
// delay-shard needs tc (iproute2) in the image.
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
		healTimeout = flag.Duration("heal-timeout", 60*time.Second, "time allowed after the load for every shard to recover")
		project     = flag.String("project", "seatsafe", "compose project name")
		files       = flag.String("compose-files", "docker-compose.yml", "comma-separated compose files")
		image       = flag.String("image", "seatsafe:dev", "image with the loadgen binary")
		deploy      = flag.String("deploy", "deploy", "directory holding shardmap.json and seed/")
		out         = flag.String("out", "", "results directory (default results/chaos-<timestamp>)")
		stopOnFail  = flag.Bool("stop-on-fail", false, "stop at the first failure and leave the cluster as it is")
	)
	flag.Parse()

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
	}
	c := &chaos.Cluster{Docker: chaos.CLI{}, Project: *project, Files: strings.Split(*files, ",")}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("chaos: %d runs, seed %d, faults %v, results in %s", *runs, *seed, kinds, *out)
	results, err := chaos.Campaign(ctx, c, cfg)
	summary := chaos.Summarize(results)
	_ = os.WriteFile(filepath.Join(*out, "summary.md"), []byte(fmt.Sprintf("# Chaos campaign\n\nseed %d, %v load at %.0f orders/s (%s)\n\n%s", *seed, *duration, *rate, *workload, summary)), 0o644)
	fmt.Print("\n" + summary)
	if err != nil {
		log.Fatalf("chaos: %v", err)
	}
	for _, r := range results {
		if r.Verdict == "FAIL" {
			os.Exit(2)
		}
	}
}
