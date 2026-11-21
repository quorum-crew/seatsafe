// Command clusterctl runs SeatSafe on the machines of an inventory
// (deploy/aws/inventory.yaml, see docs/aws.md): it generates and copies the
// configuration, starts and resets the cluster, runs benchmarks from several
// load generator hosts, and runs chaos campaigns.
//
//	clusterctl gen -seats 1000000          write shardmap, seeds and env files to deploy/aws/gen
//	clusterctl push                        copy them to the hosts
//	clusterctl reset                       wipe /data everywhere, start, wait until ready
//	clusterctl status
//	clusterctl bench -preset fresh-at-rate -rate 2000
//	clusterctl campaign -runs 18 -seed 7
//	clusterctl campaign -detach -copy-key -name nov21 -runs 90
//	clusterctl campaign-status -name nov21
//
// -dry-run prints every command instead of running it. Flags before the
// subcommand apply to all of them; clusterctl <subcommand> -h lists a
// subcommand's own.
//
// Exit code 2 means a run failed (an audit, a bookings verdict, a node
// that needs a reset).
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
	"github.com/quorum-crew/seatsafe/internal/clusterctl"
	"github.com/quorum-crew/seatsafe/internal/inventory"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

const usage = `usage: clusterctl [-inventory file] [-dir dir] [-dry-run] [-ssh-private] <subcommand> [flags]

  gen              write shardmap.json, seed files, env files and prometheus.yml into -dir
  push             copy them to the hosts
  up               start every node and wait until all are ready
  down             stop every node (the data stays)
  reset            stop, wipe /data, start, wait until ready (after any instance stop, and between experiments)
  status           role, leader, term, applied index, restarts and disk of every node
  logs <host>      the end of a host's container log
  prom             print prometheus.yml for the monitor host
  presets          list the benchmark presets
  bench            run a preset from every load generator host, repeated, with a summary table
  campaign         chaos campaign: faults under load, audit and bookings verdict per run; resumable
  campaign-status  how a detached campaign is doing
  fetch            copy a results directory from the first load generator host
`

func main() {
	log.SetFlags(log.Ltime)
	global := flag.NewFlagSet("clusterctl", flag.ExitOnError)
	invFile := global.String("inventory", filepath.Join("deploy", "aws", "inventory.yaml"), "the inventory file")
	dir := global.String("dir", filepath.Join("deploy", "aws", "gen"), "where gen writes and push reads")
	dry := global.Bool("dry-run", false, "print every command instead of running it")
	private := global.Bool("ssh-private", false, "ssh to the hosts' private addresses (when run inside the cluster's network)")
	global.Usage = func() { fmt.Fprint(os.Stderr, usage); global.PrintDefaults() }
	_ = global.Parse(os.Args[1:])
	if global.NArg() == 0 {
		global.Usage()
		os.Exit(2)
	}
	sub, args := global.Arg(0), global.Args()[1:]

	if sub == "presets" {
		for _, p := range clusterctl.Presets(clusterctl.BenchOptions{}) {
			fmt.Printf("%-16s %s\n", p.Name, p.About)
		}
		return
	}
	full, err := inventory.Load(*invFile)
	if err != nil {
		log.Fatal(err)
	}
	for _, w := range full.Warnings {
		log.Printf("warning: %s", w)
	}
	var exec clusterctl.Executor
	switch {
	case *dry:
		exec = &clusterctl.DryRun{W: os.Stdout}
	case full.Executor == inventory.ExecLocal:
		exec = clusterctl.Local{}
	default:
		exec = clusterctl.SSH{User: full.SSHUser, Key: full.SSHKey, Private: *private}
	}
	c, err := clusterctl.New(full, *dir, exec)
	if err != nil {
		log.Fatal(err)
	}
	c.Dry, c.Logf = *dry, log.Printf
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fs := flag.NewFlagSet("clusterctl "+sub, flag.ExitOnError)
	// Every subcommand but gen works on the layout gen last wrote.
	useLayout := func() clusterctl.Layout {
		l, err := clusterctl.ReadLayout(*dir)
		if err != nil {
			log.Fatal(err)
		}
		inv, err := c.Full.Layout(l.Shards, l.Replicas)
		if err != nil {
			log.Fatal(err)
		}
		c.Inv = inv
		clusterctl.UseLayout(inv)
		return l
	}
	check := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	switch sub {
	case "gen":
		var l clusterctl.Layout
		var set listFlag
		fs.IntVar(&l.Shards, "shards", 0, "use only the first N shards of the inventory (0 = all)")
		fs.IntVar(&l.Replicas, "replicas", 0, "use only the first N nodes of every group (0 = all; 1 = single-node)")
		fs.IntVar(&l.Seats, "seats", 0, "size of the dataset: one flash event of about this many seats (0 = datagen's default, 69,200 seats)")
		fs.Var(&set, "set", "a setting for the env files: KEY=VALUE, or role:KEY=VALUE for one role (repeatable)")
		_ = fs.Parse(args)
		l.Set = set
		inv, err := clusterctl.Gen(c.Full, *dir, l)
		check(err)
		log.Printf("wrote %s: %d shard(s) of %d node(s), %d coordinator node(s), %d router(s); next: clusterctl push, then clusterctl reset",
			*dir, len(inv.Shards()), len(inv.Group(1)), len(inv.Group(0)), len(inv.ByRole(inventory.Router)))

	case "push":
		_ = fs.Parse(args)
		useLayout()
		check(c.Push(ctx))

	case "up", "reset":
		timeout := fs.Duration("timeout", 3*time.Minute, "how long to wait for every node to be ready")
		_ = fs.Parse(args)
		useLayout()
		c.ReadyTimeout = *timeout
		if sub == "up" {
			check(c.Up(ctx))
		} else {
			check(c.Reset(ctx))
		}
		log.Printf("every node is ready")

	case "down":
		all := fs.Bool("all", false, "also stop hosts of the inventory that the current layout does not use")
		_ = fs.Parse(args)
		useLayout()
		if *all {
			c.Inv = c.Full
		}
		check(c.Down(ctx))

	case "status":
		_ = fs.Parse(args)
		useLayout()
		rows := c.Status(ctx)
		fmt.Print(clusterctl.StatusTable(rows))
		for _, r := range rows {
			if r.NeedsReset() {
				log.Printf("%s needs a reset: its /data is not this cluster's (after an instance stop it is empty). Run: clusterctl reset", r.Host.Name)
				defer os.Exit(2)
			}
		}

	case "logs":
		tail := fs.Int("tail", 200, "lines")
		_ = fs.Parse(args)
		useLayout()
		if fs.NArg() != 1 {
			log.Fatal("usage: clusterctl logs [-tail N] <host>")
		}
		h, ok := c.Full.Host(fs.Arg(0))
		if !ok {
			log.Fatalf("no host %q in the inventory", fs.Arg(0))
		}
		out, err := c.Logs(ctx, h, *tail)
		fmt.Print(out)
		check(err)

	case "prom":
		_ = fs.Parse(args)
		useLayout()
		fmt.Print(clusterctl.Prometheus(c.Inv))

	case "bench":
		var o clusterctl.BenchOptions
		preset := fs.String("preset", "", "the experiment (clusterctl presets lists them)")
		fs.IntVar(&o.Repeats, "repeats", 3, "runs of every variant")
		fs.Float64Var(&o.Rate, "rate", 1000, "orders per second of the fixed-rate presets, over all load generators")
		fs.DurationVar(&o.Duration, "duration", 60*time.Second, "measured time of a run")
		fs.Uint64Var(&o.Seed, "seed", 1, "random seed")
		fs.StringVar(&o.Out, "out", "", "results directory (default results/bench-<preset>-<time>)")
		fs.DurationVar(&o.ClockDefault, "widen-fallback", 5*time.Millisecond, "widening for the history check when a load generator's clock cannot be read")
		fs.DurationVar(&c.Timing.Lead, "lead", 15*time.Second, "how far ahead the common start time of the load generators is set")
		_ = fs.Parse(args)
		p, err := clusterctl.FindPreset(*preset, o)
		check(err)
		if o.Out == "" {
			o.Out = filepath.Join("results", "bench-"+p.Name+"-"+time.Now().Format("20060102-150405"))
		}
		base := useLayout()
		runs, err := c.Bench(ctx, p, base, o)
		fmt.Print("\n" + clusterctl.BenchTable(p, runs))
		log.Printf("results in %s", o.Out)
		check(err)
		for _, r := range runs {
			if r.Verdict != "PASS" {
				os.Exit(2)
			}
		}

	case "campaign":
		var o clusterctl.CampaignOptions
		fs.IntVar(&o.Runs, "runs", 18, "number of runs (the mix has 9 steps)")
		fs.Uint64Var(&o.Seed, "seed", uint64(time.Now().UnixNano()), "random seed; the same seed replays the same faults")
		fs.Float64Var(&o.Rate, "rate", 300, "orders per second over all load generators")
		fs.DurationVar(&o.Duration, "duration", 35*time.Second, "load per run")
		fs.StringVar(&o.Out, "out", "", "results directory; a campaign found there is resumed (default results/campaign-<name>)")
		fs.IntVar(&o.ResetEvery, "reset-every", 5, "wipe and restart the cluster every N runs")
		fs.BoolVar(&o.StopOnFail, "stop-on-fail", false, "stop at the first failed run and leave the cluster as it is")
		faults := fs.String("faults", "", "comma-separated fault kinds on a random shard instead of the mix, e.g. kill-leader,partition,az-outage,none")
		name := fs.String("name", "", "name of the campaign (default: campaign-<seed>)")
		detach := fs.Bool("detach", false, "run it on the first load generator host under nohup and return; see campaign-status")
		copyKey := fs.Bool("copy-key", false, "-detach: copy the inventory's ssh key to that host (the campaign needs a key every host accepts)")
		fs.DurationVar(&c.Timing.Lead, "lead", 15*time.Second, "how far ahead the common start time of the load generators is set")
		_ = fs.Parse(args)
		useLayout()
		if *name == "" {
			*name = fmt.Sprintf("campaign-%d", o.Seed)
		}
		if *faults != "" {
			kinds, err := chaos.ParseKinds(*faults)
			check(err)
			for _, k := range kinds {
				o.Plan = append(o.Plan, chaos.Step{Kind: k, Target: chaos.TargetShard})
			}
		}
		if *detach {
			key := ""
			if *copyKey {
				key = c.Full.SSHKey
			}
			pass := []string{"-runs", fmt.Sprint(o.Runs), "-seed", fmt.Sprint(o.Seed), "-rate", fmt.Sprint(o.Rate), "-duration", o.Duration.String(),
				"-reset-every", fmt.Sprint(o.ResetEvery), "-name", *name}
			if *faults != "" {
				pass = append(pass, "-faults", *faults)
			}
			d, err := c.Detach(ctx, *invFile, key, *name, pass)
			check(err)
			log.Printf("started on %s: %s", d.Host.Name, d.Command)
			log.Printf("check it with: clusterctl campaign-status -name %s; get the results with: clusterctl fetch -name %s", *name, *name)
			return
		}
		if o.Out == "" {
			o.Out = filepath.Join("results", *name)
		}
		results, err := c.Campaign(ctx, o)
		fmt.Print("\n" + chaos.Summarize(results))
		log.Printf("results in %s", o.Out)
		check(err)
		for _, r := range results {
			if r.Verdict != chaos.VerdictPass && r.Verdict != chaos.VerdictSkip {
				os.Exit(2)
			}
		}

	case "campaign-status":
		name := fs.String("name", "", "name of the campaign")
		_ = fs.Parse(args)
		useLayout()
		if *name == "" {
			log.Fatal("usage: clusterctl campaign-status -name <name>")
		}
		out, err := c.CampaignStatus(ctx, *name)
		fmt.Print(out)
		check(err)

	case "fetch":
		name := fs.String("name", "", "directory under the first load generator host's results")
		to := fs.String("to", "", "local directory (default results/<name>)")
		_ = fs.Parse(args)
		useLayout()
		if *name == "" {
			log.Fatal("usage: clusterctl fetch -name <name> [-to dir]")
		}
		if *to == "" {
			*to = filepath.Join("results", *name)
		}
		check(c.FetchResults(ctx, *name, *to))
		log.Printf("copied to %s", *to)

	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}
