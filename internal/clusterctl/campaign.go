package clusterctl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/quorum-crew/seatsafe/internal/chaos"
	"github.com/quorum-crew/seatsafe/internal/inventory"
)

// Mix is the fault mix of a campaign on real machines: the Compose
// campaign's (kill, pause and partition the leader of a shard and of the
// coordinator, with no-fault controls) and an outage of a whole zone.
// Eighteen runs give two of each.
var Mix = append(append([]chaos.Step(nil), chaos.Mix...), chaos.Step{Kind: chaos.AZOutage})

// CampaignOptions are the knobs of a campaign.
type CampaignOptions struct {
	Runs         int
	Seed         uint64
	Rate         float64       // orders per second over all load generators; 0 means 300
	Duration     time.Duration // load per run; 0 means 30 s
	Plan         []chaos.Step  // nil means Mix
	Out          string        // local directory; a campaign found there is resumed
	ResetEvery   int           // 0 means 5
	ClockDefault time.Duration // widening when a host's clock cannot be read; 0 means 5 ms
	CheckTimeout time.Duration
	StopOnFail   bool
}

// Campaign runs a chaos campaign against the cluster: every run is a load
// on all load generator hosts with one fault of the plan, checked by the
// audit and the Porcupine bookings verdict. It is resumable: runs already
// recorded in Out are not run again. Whatever a killed run left on the nodes
// (a partition, a paused container) is removed first.
func (c *Ctl) Campaign(ctx context.Context, o CampaignOptions) ([]chaos.Result, error) {
	if o.Rate <= 0 {
		o.Rate = 300
	}
	if o.Duration <= 0 {
		o.Duration = 30 * time.Second
	}
	if o.Plan == nil {
		o.Plan = Mix
	}
	if o.ResetEvery <= 0 {
		o.ResetEvery = 5
	}
	if o.ClockDefault <= 0 {
		o.ClockDefault = 5 * time.Millisecond
	}
	if o.CheckTimeout <= 0 {
		o.CheckTimeout = 2 * time.Minute
	}
	UseLayout(c.Inv)
	remote := &Remote{C: c, Loader: c.ChaosLoader(o.ClockDefault)}
	if err := remote.Cleanup(ctx); err != nil {
		return nil, fmt.Errorf("cleaning up before the campaign: %w", err)
	}
	// A partition, a pause and a kill are healed by the chaos package even
	// on Ctrl-C; this is for whatever else goes wrong.
	defer func() { _ = remote.Cleanup(context.Background()) }()
	title := fmt.Sprintf("# Chaos campaign\n\nseed %d, %v load at %.0f orders/s (flash) from %d load generator host(s), 20%% cross-shard, 5 browses/s; %d shard(s) of %d node(s), zones %s\n\n",
		o.Seed, o.Duration, o.Rate, len(c.Inv.ByRole(inventory.Loadgen)), len(c.Inv.Shards()), len(c.Inv.Group(0)), strings.Join(c.Inv.Zones(), ", "))
	return chaos.Campaign(ctx, &chaos.Cluster{Remote: remote}, chaos.Config{
		Runs: o.Runs, ResetEvery: o.ResetEvery, Seed: o.Seed, Plan: o.Plan,
		InjectAfter: [2]time.Duration{5 * time.Second, 15 * time.Second}, FaultFor: [2]time.Duration{5 * time.Second, 15 * time.Second},
		HealTimeout: 2 * time.Minute, Workload: "flash", Rate: o.Rate, Duration: max(o.Duration, 35*time.Second),
		CrossShardFrac: 0.2, BrowseRate: 5, Linearizable: true, CheckTimeout: o.CheckTimeout, WidenFor: ReadWiden,
		HoldDrain: chaos.DefaultHoldDrain, Resume: true, Out: o.Out, Title: title, StopOnFail: o.StopOnFail, Logf: c.Logf,
	})
}

// ChaosCluster returns the cluster of c as the chaos tool's target: faults
// go to its hosts and the load runs on its load generator hosts. cmd/chaos
// uses it for -inventory.
func (c *Ctl) ChaosCluster(clockDefault time.Duration) (*chaos.Cluster, *Remote) {
	UseLayout(c.Inv)
	r := &Remote{C: c, Loader: c.ChaosLoader(clockDefault)}
	return &chaos.Cluster{Remote: r}, r
}

// Detached describes a campaign started on the first load generator host.
type Detached struct {
	Host    inventory.Host
	Dir     string // the campaign's directory on that host
	Log     string
	Command string
}

func (c *Ctl) campaignHost() (inventory.Host, error) {
	hosts := c.Inv.ByRole(inventory.Loadgen)
	if len(hosts) == 0 {
		return inventory.Host{}, fmt.Errorf("the inventory has no loadgen host")
	}
	return hosts[0], nil
}

// Detach starts a campaign on the first load generator host, so it runs
// unattended inside the cluster's network: the clusterctl of the cluster's
// own image is put next to the config, with the inventory, the layout and
// the ssh key, and started under nohup. name is the campaign's directory
// under the host's results; args are the campaign's own flags.
//
// The ssh key is copied only if keyFile is given: the campaign needs a key
// that every host accepts, and whether the cluster's key may be on a load
// generator host is the owner's call.
func (c *Ctl) Detach(ctx context.Context, inventoryFile, keyFile, name string, args []string) (*Detached, error) {
	h, err := c.campaignHost()
	if err != nil {
		return nil, err
	}
	cfg := c.ConfigDir(h)
	d := &Detached{Host: h, Dir: cfg + "/results/" + name, Log: cfg + "/results/" + name + ".log"}
	b, err := os.ReadFile(inventoryFile)
	if err != nil {
		return nil, err
	}
	// On that host the key is next to the inventory.
	var lines []string
	for _, line := range strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "ssh_key:") {
			line = "ssh_key: " + cfg + "/ssh_key"
		}
		lines = append(lines, line)
	}
	tmp, err := os.MkdirTemp("", "clusterctl")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	inv := filepath.Join(tmp, "inventory.yaml")
	if err := os.WriteFile(inv, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		return nil, err
	}
	steps := []func() error{
		func() error {
			_, err := c.Exec.Run(ctx, h, "mkdir -p "+shq(cfg+"/bin")+" "+shq(cfg+"/gen")+" "+shq(cfg+"/results"))
			return err
		},
		func() error { return c.Exec.Copy(ctx, h, inv, cfg+"/inventory.yaml") },
		func() error {
			return c.Exec.Copy(ctx, h, filepath.Join(c.Dir, "layout.json"), cfg+"/gen/layout.json")
		},
		func() error {
			if keyFile == "" {
				return nil
			}
			if err := c.Exec.Copy(ctx, h, expandHome(keyFile), cfg+"/ssh_key"); err != nil {
				return err
			}
			_, err := c.Exec.Run(ctx, h, "chmod 600 "+shq(cfg+"/ssh_key"))
			return err
		},
		func() error {
			// The binary of the image the cluster runs: the same build, for Linux.
			_, err := c.Exec.Run(ctx, h, fmt.Sprintf("docker rm -f ss-extract >/dev/null 2>&1; docker create --name ss-extract %s >/dev/null && docker cp ss-extract:/usr/local/bin/clusterctl %s && docker rm ss-extract >/dev/null",
				shq(c.Inv.Image), shq(cfg+"/bin/clusterctl")))
			return err
		},
		func() error {
			if _, err := c.Exec.Run(ctx, h, "test -f "+shq(cfg+"/ssh_key")); err != nil && !c.Dry {
				return fmt.Errorf("%s has no %s/ssh_key: the campaign needs a key every host accepts; put one there or pass -copy-key", h.Name, cfg)
			}
			return nil
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return nil, err
		}
	}
	run := []string{cfg + "/bin/clusterctl", "-inventory", cfg + "/inventory.yaml", "-dir", cfg + "/gen", "-ssh-private", "campaign", "-out", d.Dir}
	run = append(run, args...)
	for i, a := range run {
		run[i] = shq(a)
	}
	d.Command = strings.Join(run, " ")
	_, err = c.Exec.Run(ctx, h, fmt.Sprintf("cd %s && nohup %s > %s 2>&1 < /dev/null & echo started", shq(cfg), d.Command, shq(d.Log)))
	return d, err
}

// CampaignStatus reports on a detached campaign: whether it is still
// running, the end of its log and its summary so far.
func (c *Ctl) CampaignStatus(ctx context.Context, name string) (string, error) {
	h, err := c.campaignHost()
	if err != nil {
		return "", err
	}
	dir := c.ConfigDir(h) + "/results/" + name
	return c.Exec.Run(ctx, h, fmt.Sprintf(
		"if pgrep -f 'clusterctl .*campaign .*%s' >/dev/null; then echo 'state: RUNNING'; else echo 'state: not running (finished or stopped; start it again with the same -name to resume)'; fi; "+
			"echo; echo '== last lines of the log'; tail -n 8 %s 2>/dev/null; echo; echo '== summary so far'; cat %s 2>/dev/null || echo 'no run has finished yet'",
		name, shq(dir+".log"), shq(dir+"/summary.md")))
}

// FetchResults copies a directory under the first load generator host's
// results to this machine.
func (c *Ctl) FetchResults(ctx context.Context, name, local string) error {
	h, err := c.campaignHost()
	if err != nil {
		return err
	}
	return c.Exec.Fetch(ctx, h, c.ConfigDir(h)+"/results/"+name, local)
}
