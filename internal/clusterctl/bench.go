package clusterctl

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/quorum-crew/seatsafe/internal/chaos"
	"github.com/quorum-crew/seatsafe/internal/inventory"
	"github.com/quorum-crew/seatsafe/internal/linearize"
)

// BenchOptions are the knobs of a benchmark.
type BenchOptions struct {
	Repeats      int           // runs of every variant; 0 means 3
	Rate         float64       // orders per second of the fixed-rate presets, over all load generators; 0 means 1000
	Duration     time.Duration // measured time of a run; 0 means 60 s
	Seed         uint64
	Out          string        // local directory for the results
	ClockDefault time.Duration // widening when a host's clock cannot be read; 0 means 5 ms
	CheckTimeout time.Duration // time for one history check; 0 means 2 minutes
}

func (o *BenchOptions) defaults() {
	if o.Repeats <= 0 {
		o.Repeats = 3
	}
	if o.Rate <= 0 {
		o.Rate = 1000
	}
	if o.Duration <= 0 {
		o.Duration = 60 * time.Second
	}
	if o.ClockDefault <= 0 {
		o.ClockDefault = 5 * time.Millisecond
	}
	if o.CheckTimeout <= 0 {
		o.CheckTimeout = 2 * time.Minute
	}
}

// Variant is one thing a preset measures: a load, on a layout, with some
// settings, with or without a fault.
type Variant struct {
	Name     string
	Set      []string // settings on top of the cluster's own (gen -set)
	Shards   int      // 0 = the cluster's layout
	Replicas int
	Load     []string     // loadgen flags of a run without a fault
	Fault    chaos.Kind   // "" = no chaos run; chaos.None = a chaos run without a fault
	Target   chaos.Target // of the fault
}

// Preset is a named experiment: its variants are run Repeats times each,
// with a cluster reset before every run.
type Preset struct {
	Name, About string
	// Alternate reverses the order of the variants on every other repeat,
	// so that drift during the session hits all of them.
	Alternate bool
	Variants  []Variant
}

// Presets returns the experiments, in the order they are listed.
func Presets(o BenchOptions) []Preset {
	o.defaults()
	rate := strconv.FormatFloat(o.Rate, 'f', -1, 64)
	open := func(workload string, more ...string) []string {
		return append([]string{"-mode", "open", "-workload", workload, "-rate", rate, "-duration", o.Duration.String(), "-warmup", "5s"}, more...)
	}
	out := []Preset{
		{Name: "ramp", About: "highest rate that keeps p99 under 100 ms: fresh seats, 500/s steps of 10 s, on one load generator",
			Variants: []Variant{{Name: "ramp", Load: []string{"-mode", "ramp", "-workload", "fresh", "-rate-start", "500", "-rate-step", "500", "-rate-end", "20000", "-step-duration", "10s", "-warmup", "5s"}}}},
		{Name: "fresh-at-rate", About: "fresh seats (no conflicts) at -rate", Variants: []Variant{{Name: "fresh", Load: open("fresh")}}},
		{Name: "flash", About: "flash sale (Zipf, heavy conflicts) at -rate", Variants: []Variant{{Name: "flash", Load: open("flash")}}},
		{Name: "users-10k", About: "10,000 closed-loop users on the flash sale",
			Variants: []Variant{{Name: "users-10k", Load: []string{"-mode", "closed", "-workload", "flash", "-users", "10000", "-duration", o.Duration.String(), "-warmup", "5s"}}}},
		{Name: "cross-shard", About: "fresh seats at -rate with a fifth of the orders across two shards (2PC)",
			Variants: []Variant{{Name: "single-shard", Load: open("fresh")}, {Name: "cross-20%", Load: open("fresh", "-cross-shard-frac", "0.2")}}},
		{Name: "shards", About: "fresh seats at -rate on 1, 2 and 3 shards"},
		{Name: "single-node", About: "fresh seats at -rate with one node per group against three",
			Variants: []Variant{{Name: "replicas-1", Replicas: 1, Load: open("fresh")}, {Name: "replicas-3", Replicas: 3, Load: open("fresh")}}},
		{Name: "pipeline-ab", About: "RAFT_LEADER_SEND_FIRST and RAFT_EARLY_APPLY off against on, same build, alternating", Alternate: true,
			Variants: []Variant{
				{Name: "pipeline-off", Set: []string{"RAFT_LEADER_SEND_FIRST=false", "RAFT_EARLY_APPLY=false"}, Load: open("fresh")},
				{Name: "pipeline-on", Set: []string{"RAFT_LEADER_SEND_FIRST=true", "RAFT_EARLY_APPLY=true"}, Load: open("fresh")}}},
		{Name: "tick", About: "Raft tick of 25 ms against 50 ms: a leader kill on a shard, for the failover, under the flash sale", Alternate: true,
			Variants: []Variant{
				{Name: "tick-25", Set: []string{"RAFT_TICK_MS=25"}, Fault: chaos.KillLeader, Target: chaos.TargetShard},
				{Name: "tick-50", Set: []string{"RAFT_TICK_MS=50"}, Fault: chaos.KillLeader, Target: chaos.TargetShard}}},
		{Name: "router-timeout", About: "router's shard attempt timeout of 500, 300, 250 and 200 ms, with a paused shard leader and with no fault"},
		{Name: "az-outage", About: "every SeatSafe container of one availability zone killed during the flash sale, then restarted",
			Variants: []Variant{{Name: "az-outage", Fault: chaos.AZOutage}}},
	}
	for i := range out {
		switch out[i].Name {
		case "shards":
			for n := 1; n <= 3; n++ {
				out[i].Variants = append(out[i].Variants, Variant{Name: fmt.Sprintf("shards-%d", n), Shards: n, Load: open("fresh")})
			}
		case "router-timeout":
			for _, ms := range []string{"500", "300", "250", "200"} {
				set := []string{"ROUTER_SHARD_ATTEMPT_MS=" + ms}
				out[i].Variants = append(out[i].Variants,
					Variant{Name: "attempt-" + ms + "-pause-leader", Set: set, Fault: chaos.PauseLeader, Target: chaos.TargetShard},
					Variant{Name: "attempt-" + ms + "-none", Set: set, Fault: chaos.None})
			}
		}
	}
	// shards-1, shards-2 and shards-3 are also presets of their own.
	for n := 1; n <= 3; n++ {
		name := fmt.Sprintf("shards-%d", n)
		out = append(out, Preset{Name: name, About: fmt.Sprintf("fresh seats at -rate on %d shard(s)", n), Variants: []Variant{{Name: name, Shards: n, Load: open("fresh")}}})
	}
	return out
}

// FindPreset returns the preset with that name.
func FindPreset(name string, o BenchOptions) (Preset, error) {
	var names []string
	for _, p := range Presets(o) {
		if p.Name == name {
			return p, nil
		}
		names = append(names, p.Name)
	}
	return Preset{}, fmt.Errorf("unknown preset %q (presets: %s)", name, strings.Join(names, ", "))
}

// BenchRun is one run of one variant.
type BenchRun struct {
	Variant  string             `json:"variant"`
	Repeat   int                `json:"repeat"`
	Dir      string             `json:"dir"`
	Metrics  map[string]float64 `json:"metrics"`
	Audit    string             `json:"audit"`    // PASS, FAIL or -
	Bookings string             `json:"bookings"` // the Porcupine verdict, or -
	Verdict  string             `json:"verdict"`  // PASS unless the audit, the bookings or the run failed
	Err      string             `json:"error,omitempty"`
}

// Configure makes the cluster the given layout with the given settings:
// every node is stopped (also those the new layout does not use), the
// files are generated and pushed. The cluster is left down; Reset starts it.
func (c *Ctl) Configure(ctx context.Context, l Layout) error {
	c.Inv = c.Full
	if err := c.Down(ctx); err != nil {
		return err
	}
	inv, err := Gen(c.Full, c.Dir, l)
	if err != nil {
		return err
	}
	c.Inv = inv
	UseLayout(inv)
	return c.Push(ctx)
}

// Bench runs a preset and returns every run. Out/runs.jsonl and
// Out/summary.md are written after every run.
func (c *Ctl) Bench(ctx context.Context, p Preset, base Layout, o BenchOptions) ([]BenchRun, error) {
	o.defaults()
	if err := os.MkdirAll(o.Out, 0o755); err != nil {
		return nil, err
	}
	var runs []BenchRun
	applied := ""
	for rep := 1; rep <= o.Repeats && ctx.Err() == nil; rep++ {
		order := append([]Variant(nil), p.Variants...)
		if p.Alternate && rep%2 == 0 {
			for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
				order[i], order[j] = order[j], order[i]
			}
		}
		for _, v := range order {
			if ctx.Err() != nil {
				break
			}
			l := base
			l.Set = append(append([]string(nil), base.Set...), v.Set...)
			if v.Shards > 0 {
				l.Shards = v.Shards
			}
			if v.Replicas > 0 {
				l.Replicas = v.Replicas
			}
			run := BenchRun{Variant: v.Name, Repeat: rep, Dir: filepath.Join(o.Out, v.Name, fmt.Sprintf("run-%d", rep)), Audit: "-", Bookings: "-", Metrics: map[string]float64{}}
			c.Logf("%s, run %d of %d", v.Name, rep, o.Repeats)
			err := func() error {
				if key := fmt.Sprint(l); key != applied {
					if err := c.Configure(ctx, l); err != nil {
						return fmt.Errorf("configure: %w", err)
					}
					applied = key
				}
				if v.Fault != "" {
					return c.benchFault(ctx, v, &run, o, uint64(rep)) // the chaos run resets the cluster itself
				}
				if err := c.Reset(ctx); err != nil {
					return fmt.Errorf("reset: %w", err)
				}
				return c.benchLoad(ctx, v, &run, o, uint64(rep))
			}()
			if err != nil {
				run.Err, run.Verdict = err.Error(), "ERROR"
				c.Logf("%s, run %d: %v", v.Name, rep, err)
			}
			runs = append(runs, run)
			if err := writeBench(o.Out, p, runs); err != nil {
				return runs, err
			}
		}
	}
	return runs, ctx.Err()
}

func writeBench(out string, p Preset, runs []BenchRun) error {
	var b strings.Builder
	for _, r := range runs {
		line, _ := json.Marshal(r)
		b.Write(append(line, '\n'))
	}
	if err := os.WriteFile(filepath.Join(out, "runs.jsonl"), []byte(b.String()), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "summary.md"), []byte(BenchTable(p, runs)), 0o644)
}

// measureClocks records the load generators' clocks in dir/clock.json and
// returns the widening for the history check.
func (c *Ctl) measureClocks(ctx context.Context, dir string, fallback time.Duration) time.Duration {
	rep := c.Clocks(ctx, fallback)
	_ = os.MkdirAll(dir, 0o755)
	b, _ := json.MarshalIndent(rep, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "clock.json"), append(b, '\n'), 0o644)
	return rep.Widen()
}

// ReadWiden returns the widening recorded in dir/clock.json.
func ReadWiden(dir string) time.Duration {
	var rep ClockReport
	if b, err := os.ReadFile(filepath.Join(dir, "clock.json")); err == nil && json.Unmarshal(b, &rep) == nil {
		return rep.Widen()
	}
	return 0
}

// ChaosLoader is the load of a chaos run: the clocks are measured into the
// run's directory first, then the load runs on every load generator host.
func (c *Ctl) ChaosLoader(fallback time.Duration) func(ctx context.Context, dir string, args []string) (string, error) {
	return func(ctx context.Context, dir string, args []string) (string, error) {
		c.measureClocks(ctx, dir, fallback)
		return c.Load(ctx, dir, args)
	}
}

// benchLoad is a run without the chaos tool: the load, the audit, the
// history check, and what the hosts did meanwhile.
func (c *Ctl) benchLoad(ctx context.Context, v Variant, run *BenchRun, o BenchOptions, n uint64) error {
	widen := c.measureClocks(ctx, run.Dir, o.ClockDefault)
	before := c.hostStats(ctx)
	sum, err := c.load(ctx, run.Dir, append(append([]string(nil), v.Load...), "-seed", strconv.FormatUint(o.Seed+n, 10), "-label", v.Name))
	after := c.hostStats(ctx)
	c.saveNodeMetrics(ctx, run.Dir)
	usage := hostUsage(c.Inv.Hosts, before, after)
	_ = os.WriteFile(filepath.Join(run.Dir, "hosts.md"), []byte(usageTable(usage)), 0o644)
	if err != nil {
		return err
	}
	if sum == nil {
		return nil // a dry run
	}
	r, m := sum.Results, run.Metrics
	m["attempted_per_s"], m["achieved_per_s"] = r.AttemptedRate, r.AchievedRate
	m["confirmed_orders_per_s"], m["confirmed_seats_per_s"] = r.ConfirmedRate, r.ConfirmedSeats
	m["p50_ms"], m["p99_ms"], m["p99_9_ms"], m["max_ms"] = r.Latency.P50, r.Latency.P99, r.Latency.P999, r.Latency.Max
	m["conflict_pct"], m["unknown_orders"], m["dropped"] = r.ConflictRate*100, float64(r.Outcomes["UNKNOWN"]), float64(r.Dropped)
	if sum.MaxSustainable > 0 || len(sum.Steps) > 0 {
		m["max_sustainable_per_s"] = sum.MaxSustainable
	}
	for role, u := range usage.byRole() {
		m["cpu_"+role+"_mean_pct"], m["cpu_"+role+"_max_pct"] = u[0], u[1]
	}
	run.Verdict = "PASS"
	if sum.Audit != nil {
		run.Audit = "PASS"
		if !sum.Audit.Pass {
			run.Audit, run.Verdict = "FAIL", "FAIL"
		}
	}
	if _, err := os.Stat(filepath.Join(run.Dir, "history.jsonl")); err == nil {
		hc, err := chaos.CheckHistory(run.Dir, linearize.Options{Timeout: o.CheckTimeout, Widen: widen})
		if err != nil {
			return fmt.Errorf("history check: %w", err)
		}
		run.Bookings = hc.Bookings
		if hc.Idempotency != "" {
			run.Bookings = chaos.VerdictIdempotency
		}
		if run.Bookings != linearize.Pass {
			run.Verdict = run.Bookings
		}
		m["stale_browses"] = float64(hc.StaleBrowses)
	}
	return nil
}

// benchFault is one chaos run of the variant's fault under the flash sale.
func (c *Ctl) benchFault(ctx context.Context, v Variant, run *BenchRun, o BenchOptions, n uint64) error {
	remote := &Remote{C: c, Loader: c.ChaosLoader(o.ClockDefault)}
	if err := remote.Cleanup(ctx); err != nil {
		return err
	}
	dur := max(o.Duration, 40*time.Second)
	results, err := chaos.Campaign(ctx, &chaos.Cluster{Remote: remote}, chaos.Config{
		Runs: 1, Seed: o.Seed + n, Plan: []chaos.Step{{Kind: v.Fault, Target: v.Target}},
		InjectAfter: [2]time.Duration{10 * time.Second, 10 * time.Second}, FaultFor: [2]time.Duration{10 * time.Second, 10 * time.Second},
		HealTimeout: 2 * time.Minute, Workload: "flash", Rate: o.Rate, Duration: dur, BrowseRate: 5,
		Linearizable: true, CheckTimeout: o.CheckTimeout, WidenFor: ReadWiden, Out: run.Dir, Logf: c.Logf,
	})
	if err != nil {
		return err
	}
	if len(results) != 1 {
		return fmt.Errorf("the chaos run did not finish")
	}
	r, m := results[0], run.Metrics
	set := func(name string, v float64, ok bool) {
		if ok {
			m[name] = v
		}
	}
	set("failover_s", r.Failover.Seconds(), r.Failover >= 0)
	set("recovery_s", r.Recovery.Seconds(), r.Recovery >= 0)
	set("elections", float64(r.Elections), r.Elections >= 0)
	set("p99_ms", float64(r.P99)/float64(time.Millisecond), r.P99 >= 0)
	set("max_ms", float64(r.Max)/float64(time.Millisecond), r.Max >= 0)
	set("unknown_orders", float64(r.Unknown()), r.Unknown() >= 0)
	set("availability_pct", r.Availability*100, r.Availability >= 0)
	for g, d := range r.GroupFailover {
		set("failover_"+strings.ReplaceAll(chaos.GroupName(g), " ", "_")+"_s", d.Seconds(), d >= 0)
	}
	run.Verdict, run.Bookings = r.Verdict, r.Linearizable
	if r.AuditRules != nil {
		run.Audit = "FAIL"
		if r.AuditRules.Pass {
			run.Audit = "PASS"
		}
	}
	if r.Verdict != chaos.VerdictPass {
		run.Err = r.Reason
	}
	return nil
}

// hostStat is a host's counters at one moment.
type hostStat struct {
	busy, total uint64 // CPU jiffies since boot
	memAvailKB  uint64
	diskUsedKB  uint64
	ok          bool
}

const statCmd = `head -1 /proc/stat; grep MemAvailable /proc/meminfo; df -Pk %s 2>/dev/null | tail -1`

func parseStat(out string) hostStat {
	var s hostStat
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) >= 8 && f[0] == "cpu":
			for i, v := range f[1:] {
				n, _ := strconv.ParseUint(v, 10, 64)
				s.total += n
				if i != 3 && i != 4 { // not idle, not iowait
					s.busy += n
				}
			}
			s.ok = true
		case len(f) >= 2 && f[0] == "MemAvailable:":
			s.memAvailKB, _ = strconv.ParseUint(f[1], 10, 64)
		case len(f) >= 6 && strings.HasSuffix(f[4], "%"):
			s.diskUsedKB, _ = strconv.ParseUint(f[2], 10, 64)
		}
	}
	return s
}

// hostStats reads every host's CPU, memory and data disk counters.
func (c *Ctl) hostStats(ctx context.Context) map[string]hostStat {
	out := make([]hostStat, len(c.Inv.Hosts))
	_ = each(c.Inv.Hosts, func(h inventory.Host) error {
		dir := "/"
		if hasGroup(h) && c.Inv.DataDir != "volume" {
			dir = c.dataMount(h)
		}
		text, err := c.Exec.Run(ctx, h, fmt.Sprintf(statCmd, shq(dir)))
		for i := range c.Inv.Hosts {
			if c.Inv.Hosts[i].Name == h.Name && err == nil {
				out[i] = parseStat(text)
			}
		}
		return nil
	})
	m := map[string]hostStat{}
	for i, h := range c.Inv.Hosts {
		m[h.Name] = out[i]
	}
	return m
}

// usage is what one host did during a run.
type usage struct {
	Host      inventory.Host
	CPU       float64 // mean percent of all its cores; -1 if unknown
	MemAvailM float64 // memory available at the end, in MiB
	DiskM     float64 // growth of the data disk's use, in MiB
}

type usages []usage

func hostUsage(hosts []inventory.Host, before, after map[string]hostStat) usages {
	var out usages
	for _, h := range hosts {
		b, a := before[h.Name], after[h.Name]
		u := usage{Host: h, CPU: -1, MemAvailM: float64(a.memAvailKB) / 1024, DiskM: (float64(a.diskUsedKB) - float64(b.diskUsedKB)) / 1024}
		if b.ok && a.ok && a.total > b.total {
			u.CPU = 100 * float64(a.busy-b.busy) / float64(a.total-b.total)
		}
		out = append(out, u)
	}
	return out
}

// byRole returns the mean and the highest CPU use of the hosts of each role.
func (us usages) byRole() map[string][2]float64 {
	sum, n, hi := map[string]float64{}, map[string]int{}, map[string]float64{}
	for _, u := range us {
		if u.CPU < 0 {
			continue
		}
		sum[u.Host.Role] += u.CPU
		n[u.Host.Role]++
		hi[u.Host.Role] = math.Max(hi[u.Host.Role], u.CPU)
	}
	out := map[string][2]float64{}
	for role, k := range n {
		out[role] = [2]float64{sum[role] / float64(k), hi[role]}
	}
	return out
}

func usageTable(us usages) string {
	var b strings.Builder
	b.WriteString("| Host | Role | CPU, mean of the run | Memory available at the end | Data disk growth |\n|---|---|---|---|---|\n")
	for _, u := range us {
		cpu := "-"
		if u.CPU >= 0 {
			cpu = fmt.Sprintf("%.1f%%", u.CPU)
		}
		disk := "-"
		if hasGroup(u.Host) {
			disk = fmt.Sprintf("%.0f MiB", u.DiskM)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %.0f MiB | %s |\n", u.Host.Name, u.Host.Role, cpu, u.MemAvailM, disk)
	}
	return b.String()
}

// saveNodeMetrics writes every node's /metrics into dir/metrics.
func (c *Ctl) saveNodeMetrics(ctx context.Context, dir string) {
	_ = os.MkdirAll(filepath.Join(dir, "metrics"), 0o755)
	_ = each(c.Inv.Nodes(), func(h inventory.Host) error {
		out, err := c.Exec.Run(ctx, h, "docker exec "+c.Container(h)+" "+metricsCmd)
		if err == nil && out != "" {
			_ = os.WriteFile(filepath.Join(dir, "metrics", h.Name+".txt"), []byte(out), 0o644)
		}
		return nil
	})
}

// The metrics of the summary table, in order, with their headings.
var benchColumns = [][2]string{
	{"max_sustainable_per_s", "Max sustainable /s"}, {"achieved_per_s", "Achieved /s"}, {"confirmed_orders_per_s", "Confirmed orders /s"},
	{"confirmed_seats_per_s", "Confirmed seats /s"}, {"p50_ms", "p50 ms"}, {"p99_ms", "p99 ms"}, {"p99_9_ms", "p99.9 ms"}, {"max_ms", "Max ms"},
	{"conflict_pct", "Conflict %"}, {"unknown_orders", "UNKNOWN orders"}, {"failover_s", "Failover s"}, {"recovery_s", "Recovery s"},
	{"availability_pct", "Availability in the outage %"}, {"elections", "Elections"},
	{"cpu_storage_max_pct", "Storage CPU, busiest host %"}, {"cpu_router_max_pct", "Router CPU, busiest host %"}, {"cpu_loadgen_max_pct", "Loadgen CPU, busiest host %"},
}

// meanSD returns the mean and the sample standard deviation.
func meanSD(v []float64) (mean, sd float64) {
	if len(v) == 0 {
		return math.NaN(), math.NaN()
	}
	for _, x := range v {
		mean += x
	}
	mean /= float64(len(v))
	if len(v) < 2 {
		return mean, 0
	}
	for _, x := range v {
		sd += (x - mean) * (x - mean)
	}
	return mean, math.Sqrt(sd / float64(len(v)-1))
}

func cell(v []float64) string {
	if len(v) == 0 {
		return "-"
	}
	mean, sd := meanSD(v)
	digits := 2
	if math.Abs(mean) >= 100 {
		digits = 0
	} else if math.Abs(mean) >= 10 {
		digits = 1
	}
	if len(v) == 1 {
		return strconv.FormatFloat(mean, 'f', digits, 64)
	}
	return fmt.Sprintf("%s ± %s", strconv.FormatFloat(mean, 'f', digits, 64), strconv.FormatFloat(sd, 'f', digits, 64))
}

// BenchTable is the summary of a benchmark: one row per variant with the
// mean and the standard deviation of every metric over its runs, then
// every run, then the per-group failovers and the runs that did not pass.
func BenchTable(p Preset, runs []BenchRun) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n%s\n\nEvery cell is the mean ± the standard deviation over the variant's runs; a single number is one run.\n\n", p.Name, p.About)
	var cols [][2]string
	for _, c := range benchColumns {
		for _, r := range runs {
			if _, ok := r.Metrics[c[0]]; ok {
				cols = append(cols, c)
				break
			}
		}
	}
	b.WriteString("| Variant | Runs | Audit | Bookings |")
	for _, c := range cols {
		b.WriteString(" " + c[1] + " |")
	}
	b.WriteString("\n|---|---|---|---|" + strings.Repeat("---|", len(cols)) + "\n")
	for _, v := range p.Variants {
		n, audit, bookings, audited, checked := 0, 0, 0, 0, 0
		vals := map[string][]float64{}
		for _, r := range runs {
			if r.Variant != v.Name {
				continue
			}
			n++
			if r.Audit != "-" {
				audited++
			}
			if r.Audit == "PASS" {
				audit++
			}
			if r.Bookings != "-" {
				checked++
			}
			if r.Bookings == linearize.Pass {
				bookings++
			}
			for k, x := range r.Metrics {
				vals[k] = append(vals[k], x)
			}
		}
		if n == 0 {
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d/%d pass | %d/%d pass |", v.Name, n, audit, audited, bookings, checked)
		for _, c := range cols {
			b.WriteString(" " + cell(vals[c[0]]) + " |")
		}
		b.WriteString("\n")
	}
	b.WriteString("\n## Runs\n\n| Variant | Run | Verdict | Audit | Bookings | Directory |\n|---|---|---|---|---|---|\n")
	for _, r := range runs {
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s |\n", r.Variant, r.Repeat, r.Verdict, r.Audit, r.Bookings, filepath.ToSlash(r.Dir))
	}
	var groups, bad []string
	for _, r := range runs {
		var parts []string
		for _, k := range sortedMetricKeys(r.Metrics) {
			if strings.HasPrefix(k, "failover_") && k != "failover_s" {
				parts = append(parts, fmt.Sprintf("%s %.2f s", strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(k, "failover_"), "_s"), "_", " "), r.Metrics[k]))
			}
		}
		if len(parts) > 0 {
			groups = append(groups, fmt.Sprintf("- %s, run %d: %s", r.Variant, r.Repeat, strings.Join(parts, ", ")))
		}
		if r.Verdict != "PASS" {
			bad = append(bad, fmt.Sprintf("- %s, run %d: %s: %s", r.Variant, r.Repeat, r.Verdict, r.Err))
		}
	}
	if len(groups) > 0 {
		b.WriteString("\n## Failover per group\n\n" + strings.Join(groups, "\n") + "\n")
	}
	if len(bad) > 0 {
		b.WriteString("\n## Runs that did not pass\n\n" + strings.Join(bad, "\n") + "\n")
	}
	return b.String()
}

func sortedMetricKeys(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
