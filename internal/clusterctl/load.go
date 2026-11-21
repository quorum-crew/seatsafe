package clusterctl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quorum-crew/seatsafe/internal/inventory"
	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

// Load timing. The workers are given a start time this far ahead, on the
// clock of the first load generator host, so that every one of them has
// connected and read the seats by then. After the load, holds of cross-shard
// orders that were answered are given time to go before the final read.
type LoadTiming struct {
	Lead   time.Duration // 0 means 15 s
	Settle time.Duration // 0 means 3 s
	// Rereads is how often the final read is taken again while the audit
	// fails only on seats a hold being released would explain.
	Rereads int // 0 means 8
}

var loadSeq atomic.Int64

// splitArgs removes the flags in drop from loadgen arguments and returns the
// rest (a "-flag=value" comes back as "-flag value"), with the value of
// every flag in want.
func splitArgs(args []string, drop map[string]bool, want ...string) (rest []string, got map[string]string) {
	got = map[string]string{}
	boolean := map[string]bool{"-history": true, "-quiet": true, "-audit": true}
	for i := 0; i < len(args); i++ {
		name, value, inline := strings.Cut(args[i], "=")
		hasValue := inline
		if !inline && !boolean[name] && i+1 < len(args) {
			value, hasValue = args[i+1], true
			i++
		}
		for _, w := range want {
			if name == w {
				got[w] = value
			}
		}
		if drop[name] {
			continue
		}
		if hasValue && boolean[name] {
			rest = append(rest, name+"="+value)
			continue
		}
		rest = append(rest, name)
		if hasValue {
			rest = append(rest, value)
		}
	}
	return rest, got
}

func (c *Ctl) loadgenName(h inventory.Host) string {
	if c.local() {
		return "ss-loadgen-" + h.Name
	}
	return "seatsafe-loadgen"
}

// loadgenRun is a docker run of the load generator on a load generator
// host, with the host's config directory (shard map and seeds) and one
// results directory mounted.
func (c *Ctl) loadgenRun(h inventory.Host, results string, name string, args []string) string {
	net := "--network host"
	if c.Inv.Network != inventory.NetworkHost {
		net = "--network " + shq(c.Inv.Network)
	}
	cmd := "docker run --rm"
	if name != "" {
		cmd = "docker rm -f " + name + " >/dev/null 2>&1; docker run --rm --name " + name
	}
	cmd += fmt.Sprintf(" %s -v %s:%s:ro -v %s:/out --ulimit nofile=65536:65536 %s loadgen", net, shq(c.ConfigDir(h)), inConfig, shq(results), shq(c.Inv.Image))
	for _, a := range args {
		cmd += " " + shq(a)
	}
	return cmd
}

// Load runs one load on every load generator host at once and makes one run
// of it in dir: a baseline read of the seats, the workers started together,
// a final read, the workers' results merged, and the audit run once on the
// merged result. args are loadgen's flags; the target, the dataset and the
// output are set here, and -rate and -users are shared out among the
// workers. It returns the merged run's summary as loadgen prints it.
//
// A ramp cannot be merged and runs on the first load generator host alone.
func (c *Ctl) Load(ctx context.Context, dir string, args []string) (string, error) {
	sum, err := c.load(ctx, dir, args)
	if sum == nil {
		return "", err
	}
	return loadgen.PrintSummary(sum), err
}

func (c *Ctl) load(ctx context.Context, dir string, args []string) (*loadgen.Summary, error) {
	hosts := c.Inv.ByRole(inventory.Loadgen)
	routers := c.Inv.ByRole(inventory.Router)
	if len(hosts) == 0 || len(routers) == 0 {
		return nil, fmt.Errorf("the inventory needs at least one loadgen and one router host")
	}
	rest, got := splitArgs(args, map[string]bool{"-target": true, "-shardmap": true, "-seed-dir": true, "-out": true, "-audit": true,
		"-worker-id": true, "-workers": true, "-start-at": true, "-rate": true, "-users": true, "-history": true, "-quiet": true},
		"-mode", "-workload", "-rate", "-users", "-label")
	if got["-mode"] == loadgen.ModeRamp {
		hosts = hosts[:1]
	}
	n := len(hosts)
	workload := got["-workload"]
	if workload == "" {
		workload = loadgen.Defaults().Workload
	}
	label := got["-label"]
	if label == "" {
		label = "run"
	}
	lead, settle, rereads := c.Timing.Lead, c.Timing.Settle, c.Timing.Rereads
	if lead <= 0 {
		lead = 15 * time.Second
	}
	if settle <= 0 {
		settle = 3 * time.Second
	}
	if rereads <= 0 {
		rereads = 8
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	// One results directory per host for this run, under its config
	// directory (never under /data).
	id := fmt.Sprintf("%s-%s-%d", label, time.Now().UTC().Format("20060102T150405"), loadSeq.Add(1))
	results := func(h inventory.Host) string { return c.ConfigDir(h) + "/results/" + id }
	if err := each(hosts, func(h inventory.Host) error {
		_, err := c.Exec.Run(ctx, h, "mkdir -p "+shq(results(h)))
		return err
	}); err != nil {
		return nil, err
	}
	defer func() {
		// The files belong to the container's user: remove them from a container.
		_ = each(hosts, func(h inventory.Host) error {
			_, err := c.Exec.Run(context.Background(), h, fmt.Sprintf("docker run --rm -v %s:/r %s rm -rf %s", shq(c.ConfigDir(h)+"/results"), shq(c.Inv.Image), shq("/r/"+id)))
			return err
		})
	}()

	common := []string{"-shardmap", inShardMap, "-seed-dir", inSeedDir}
	target := func(i int) string { return routers[i%len(routers)].Addr() }
	snapshot := func(phase string) (string, error) {
		h := hosts[0]
		file := phase + ".jsonl"
		args := append([]string{"snapshot", "-phase", phase, "-target", target(0), "-workload", workload, "-out", "/out/" + file}, common...)
		if _, err := c.Exec.Run(ctx, h, c.loadgenRun(h, results(h), "", args)); err != nil {
			return "", fmt.Errorf("%s snapshot: %w", phase, err)
		}
		local := filepath.Join(dir, file)
		if err := c.Exec.Fetch(ctx, h, results(h)+"/"+file, local); err != nil {
			return "", err
		}
		return local, nil
	}

	share := func(flag string, i int) []string {
		v, err := strconv.ParseFloat(got[flag], 64)
		if got[flag] == "" || err != nil {
			return nil
		}
		if flag == "-users" {
			u := int(v) / n
			if i < int(v)%n {
				u++
			}
			return []string{flag, strconv.Itoa(u)}
		}
		return []string{flag, strconv.FormatFloat(v/float64(n), 'f', -1, 64)}
	}
	workerArgs := func(i int, startAt float64) []string {
		a := append([]string{}, rest...)
		a = append(a, share("-rate", i)...)
		a = append(a, share("-users", i)...)
		a = append(a, "-target", target(i), "-out", fmt.Sprintf("/out/w%d", i), "-history", "-quiet")
		a = append(a, common...)
		if n > 1 {
			a = append(a, "-worker-id", strconv.Itoa(i), "-workers", strconv.Itoa(n), "-start-at", strconv.FormatFloat(startAt, 'f', 3, 64))
		}
		return a
	}

	// One machine alone: the load generator does everything itself.
	if n == 1 {
		h := hosts[0]
		out, runErr := c.Exec.Run(ctx, h, c.loadgenRun(h, results(h), c.loadgenName(h), workerArgs(0, 0)))
		_ = os.WriteFile(filepath.Join(dir, "worker-0.txt"), []byte(out), 0o644)
		if err := c.Exec.Fetch(ctx, h, results(h)+"/w0", dir); err != nil && runErr == nil {
			return nil, err
		}
		sum, err := readSummary(dir)
		if err != nil && !c.Dry {
			if runErr != nil {
				return nil, runErr
			}
			return nil, err
		}
		// Exit code 2 is a failed audit: the summary says so.
		return sum, nil
	}

	baseline, err := snapshot(loadgen.PhaseBaseline)
	if err != nil {
		return nil, err
	}
	now, err := c.Exec.Run(ctx, hosts[0], "date +%s")
	if err != nil {
		return nil, err
	}
	startAt, _ := strconv.ParseFloat(strings.TrimSpace(now), 64)
	if startAt == 0 {
		startAt = float64(time.Now().Unix())
	}
	startAt += lead.Seconds()

	workerDirs := make([]string, n)
	var mu sync.Mutex
	var failed []string
	_ = each(hosts, func(h inventory.Host) error {
		i := 0
		for hosts[i].Name != h.Name {
			i++
		}
		out, err := c.Exec.Run(ctx, h, c.loadgenRun(h, results(h), c.loadgenName(h), workerArgs(i, startAt)))
		_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("worker-%d.txt", i)), []byte(out), 0o644)
		workerDirs[i] = filepath.Join(dir, "workers", fmt.Sprintf("w%d", i))
		if ferr := c.Exec.Fetch(ctx, h, fmt.Sprintf("%s/w%d", results(h), i), workerDirs[i]); ferr != nil && err == nil {
			err = ferr
		}
		if err != nil {
			mu.Lock()
			failed = append(failed, fmt.Sprintf("worker %d on %s: %v", i, h.Name, err))
			mu.Unlock()
		}
		return nil
	})
	if len(failed) > 0 {
		return nil, fmt.Errorf("the load failed: %s", strings.Join(failed, "; "))
	}
	if c.Dry {
		_, err := snapshot(loadgen.PhaseFinal)
		return nil, err
	}

	var sum *loadgen.Summary
	for read := 0; ; read++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(settle):
		}
		final, err := snapshot(loadgen.PhaseFinal)
		if err != nil {
			return nil, err
		}
		if sum, err = loadgen.Merge(loadgen.MergeInput{Out: dir, Workers: workerDirs, Baseline: baseline, Final: final, Label: label}); err != nil {
			return nil, err
		}
		if sum.Audit == nil || !sum.Audit.Unsettled() || read >= rereads {
			break
		}
		c.Logf("the audit is unsettled (holds not released yet): reading the seats again")
	}
	return sum, nil
}

func readSummary(dir string) (*loadgen.Summary, error) {
	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		return nil, err
	}
	var s loadgen.Summary
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
