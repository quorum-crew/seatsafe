package clusterctl

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/chaos"
	"github.com/quorum-crew/seatsafe/internal/inventory"
)

func TestPresets(t *testing.T) {
	ps := Presets(BenchOptions{Rate: 2000, Duration: 30 * time.Second})
	by := map[string]Preset{}
	var names []string
	for _, p := range ps {
		by[p.Name] = p
		names = append(names, p.Name)
		if p.About == "" || len(p.Variants) == 0 {
			t.Fatalf("preset %s: %+v", p.Name, p)
		}
		seen := map[string]bool{}
		for _, v := range p.Variants {
			if v.Name == "" || seen[v.Name] || (len(v.Load) == 0) == (v.Fault == "") {
				t.Fatalf("preset %s, variant %+v: needs a name of its own and either a load or a fault", p.Name, v)
			}
			seen[v.Name] = true
			if _, err := parseSet(v.Set); err != nil {
				t.Fatalf("preset %s, variant %s: %v", p.Name, v.Name, err)
			}
		}
	}
	for _, want := range []string{"ramp", "fresh-at-rate", "flash", "users-10k", "cross-shard", "shards", "shards-1", "shards-2", "shards-3",
		"single-node", "pipeline-ab", "tick", "router-timeout", "az-outage"} {
		if _, ok := by[want]; !ok {
			t.Fatalf("no preset %q in %v", want, names)
		}
	}
	if got := strings.Join(by["fresh-at-rate"].Variants[0].Load, " "); got != "-mode open -workload fresh -rate 2000 -duration 30s -warmup 5s" {
		t.Fatalf("fresh-at-rate: %s", got)
	}
	ab := by["pipeline-ab"]
	if !ab.Alternate || len(ab.Variants) != 2 ||
		!slices.Equal(ab.Variants[0].Set, []string{"RAFT_LEADER_SEND_FIRST=false", "RAFT_EARLY_APPLY=false"}) ||
		!slices.Equal(ab.Variants[1].Set, []string{"RAFT_LEADER_SEND_FIRST=true", "RAFT_EARLY_APPLY=true"}) ||
		!slices.Equal(ab.Variants[0].Load, ab.Variants[1].Load) {
		t.Fatalf("pipeline-ab: %+v", ab)
	}
	if tick := by["tick"]; tick.Variants[0].Set[0] != "RAFT_TICK_MS=25" || tick.Variants[1].Set[0] != "RAFT_TICK_MS=50" {
		t.Fatalf("tick: %+v", tick)
	}
	rt := by["router-timeout"]
	if len(rt.Variants) != 8 {
		t.Fatalf("router-timeout has %d variants", len(rt.Variants))
	}
	for i, ms := range []string{"500", "300", "250", "200"} {
		pause, none := rt.Variants[2*i], rt.Variants[2*i+1]
		if pause.Set[0] != "ROUTER_SHARD_ATTEMPT_MS="+ms || pause.Fault != chaos.PauseLeader || none.Set[0] != pause.Set[0] || none.Fault != chaos.None {
			t.Fatalf("router-timeout %s: %+v %+v", ms, pause, none)
		}
	}
	if s := by["shards"]; len(s.Variants) != 3 || s.Variants[0].Shards != 1 || s.Variants[2].Shards != 3 || by["shards-2"].Variants[0].Shards != 2 {
		t.Fatalf("shards: %+v", s)
	}
	if sn := by["single-node"]; sn.Variants[0].Replicas != 1 || sn.Variants[1].Replicas != 3 {
		t.Fatalf("single-node: %+v", sn)
	}
	if by["az-outage"].Variants[0].Fault != chaos.AZOutage || !slices.Contains(by["users-10k"].Variants[0].Load, "10000") || !slices.Contains(by["cross-shard"].Variants[1].Load, "-cross-shard-frac") {
		t.Fatal("az-outage, users-10k or cross-shard")
	}
	if _, err := FindPreset("fresh", BenchOptions{}); err == nil || !strings.Contains(err.Error(), "fresh-at-rate") {
		t.Fatalf("%v", err)
	}
}

func TestSplitArgs(t *testing.T) {
	rest, got := splitArgs([]string{"-target", "router:7000", "-mode", "open", "-history", "-rate", "300", "-quiet", "-seed=7", "-audit=false", "-out", "/out", "-workload", "flash"},
		map[string]bool{"-target": true, "-out": true, "-rate": true, "-audit": true, "-history": true}, "-rate", "-workload", "-mode", "-label")
	if want := []string{"-mode", "open", "-quiet", "-seed", "7", "-workload", "flash"}; !slices.Equal(rest, want) {
		t.Fatalf("rest %q, want %q", rest, want)
	}
	if got["-rate"] != "300" || got["-workload"] != "flash" || got["-mode"] != "open" || got["-label"] != "" {
		t.Fatalf("%v", got)
	}
}

// The load of a run: one baseline read, the workers started together on
// every load generator host with the rate shared out, each against a router
// of its own, one final read. (The merge itself is tested in
// internal/loadgen, and end to end by the local proof in docs/aws.md.)
func TestLoadCommands(t *testing.T) {
	f := &Fake{Reply: func(_ inventory.Host, cmd string) (string, error) {
		if cmd == "date +%s" {
			return "1800000000\n", nil
		}
		return "", nil
	}}
	c := newCtl(t, f)
	c.Dry = true
	c.Timing = LoadTiming{Lead: 20 * time.Second}
	dir := t.TempDir()
	args := []string{"-target", "router:7000", "-shardmap", "/deploy/shardmap.json", "-seed-dir", "/deploy/seed", "-out", "/out", "-label", "chaos-0003",
		"-mode", "open", "-workload", "flash", "-rate", "300", "-warmup", "2s", "-duration", "30s", "-seed", "9", "-history", "-quiet", "-cross-shard-frac", "0.2"}
	if _, err := c.Load(context.Background(), dir, args); err != nil {
		t.Fatal(err)
	}
	one, two := f.On("loadgen-1"), f.On("loadgen-2")
	if len(one) != 9 || len(two) != 4 {
		t.Fatalf("%d commands on loadgen-1, %d on loadgen-2:\n%s", len(one), len(two), strings.Join(one, "\n"))
	}
	results := strings.TrimPrefix(one[0], "mkdir -p ")
	if !strings.HasPrefix(results, "/opt/seatsafe/results/chaos-0003-") {
		t.Fatalf("results directory %q", results)
	}
	img := "123456789012.dkr.ecr.us-west-2.amazonaws.com/seatsafe:latest"
	run := "--network host -v /opt/seatsafe:/opt/seatsafe:ro -v " + results + ":/out --ulimit nofile=65536:65536 " + img + " loadgen "
	if want := "docker run --rm " + run + "snapshot -phase baseline -target 10.0.1.30:7000 -workload flash -out /out/baseline.jsonl -shardmap /opt/seatsafe/shardmap.json -seed-dir /opt/seatsafe/seed"; one[1] != want {
		t.Fatalf("baseline:\n%s\nwant:\n%s", one[1], want)
	}
	if !strings.HasPrefix(one[2], "fetch "+results+"/baseline.jsonl -> ") || one[3] != "date +%s" {
		t.Fatalf("%q", one[2:4])
	}
	worker := func(i int, target string) string {
		return "docker rm -f seatsafe-loadgen >/dev/null 2>&1; docker run --rm --name seatsafe-loadgen " + run +
			"-label chaos-0003 -mode open -workload flash -warmup 2s -duration 30s -seed 9 -cross-shard-frac 0.2 -rate 150 -target " + target +
			" -out /out/w" + string(rune('0'+i)) + " -history -quiet -shardmap /opt/seatsafe/shardmap.json -seed-dir /opt/seatsafe/seed -worker-id " + string(rune('0'+i)) + " -workers 2 -start-at 1800000020.000"
	}
	if one[4] != worker(0, "10.0.1.30:7000") {
		t.Fatalf("worker 0:\n%s\nwant:\n%s", one[4], worker(0, "10.0.1.30:7000"))
	}
	results2 := strings.TrimPrefix(two[0], "mkdir -p ")
	if got, want := two[1], strings.ReplaceAll(worker(1, "10.0.2.30:7000"), results, results2); got != want {
		t.Fatalf("worker 1:\n%s\nwant:\n%s", got, want)
	}
	if !strings.HasPrefix(one[5], "fetch "+results+"/w0 -> ") || !strings.Contains(one[6], "snapshot -phase final") ||
		!strings.Contains(one[len(one)-1], "rm -rf /r/"+filepath.Base(results)) || !strings.Contains(two[len(two)-1], "rm -rf /r/") {
		t.Fatalf("after the workers:\n%s", strings.Join(one[5:], "\n"))
	}
	// Nothing of a run is ever written under the data directory.
	for _, call := range f.Calls() {
		if strings.Contains(call.Cmd, " /data") || strings.Contains(call.Cmd, "-v /data") {
			t.Fatalf("%s: %s", call.Host, call.Cmd)
		}
	}

	// A ramp cannot be merged: it runs on the first load generator alone and
	// audits itself.
	f = &Fake{}
	c.Exec = f
	if _, err := c.Load(context.Background(), t.TempDir(), []string{"-mode", "ramp", "-workload", "fresh", "-rate-start", "500"}); err != nil {
		t.Fatal(err)
	}
	cmds := strings.Join(f.On("loadgen-1"), "\n")
	if len(f.On("loadgen-2")) != 0 || strings.Contains(cmds, "-workers") || strings.Contains(cmds, "snapshot") || !strings.Contains(cmds, "-mode ramp -workload fresh -rate-start 500 -target 10.0.1.30:7000 -out /out/w0") {
		t.Fatalf("ramp:\n%s", cmds)
	}

	// 10,000 users on 3 load generators: 3334, 3333 and 3333.
	inv, _ := c.Full.Layout(0, 0)
	inv.Hosts = append(inv.Hosts, inventory.Host{Name: "loadgen-3", Role: inventory.Loadgen, AZ: "us-west-2c", PrivateIP: "10.0.3.40"})
	f = &Fake{}
	c.Exec, c.Inv = f, inv
	if _, err := c.Load(context.Background(), t.TempDir(), []string{"-mode", "closed", "-workload", "flash", "-users", "10000"}); err != nil {
		t.Fatal(err)
	}
	for host, want := range map[string]string{"loadgen-1": "-users 3334 -target 10.0.1.30:7000", "loadgen-2": "-users 3333 -target 10.0.2.30:7000", "loadgen-3": "-users 3333 -target 10.0.1.30:7000"} {
		if !strings.Contains(strings.Join(f.On(host), "\n"), want) {
			t.Fatalf("%s: no %q in\n%s", host, want, strings.Join(f.On(host), "\n"))
		}
	}
}

// A worker that fails fails the run, and names the host.
func TestLoadFails(t *testing.T) {
	f := &Fake{Reply: func(h inventory.Host, cmd string) (string, error) {
		if h.Name == "loadgen-2" && strings.Contains(cmd, "-worker-id") {
			return "", errors.New("exit status 1: cannot reach the router")
		}
		return "", nil
	}}
	c := newCtl(t, f)
	_, err := c.Load(context.Background(), t.TempDir(), []string{"-mode", "open", "-workload", "fresh", "-rate", "100"})
	if err == nil || !strings.Contains(err.Error(), "worker 1 on loadgen-2") || !strings.Contains(err.Error(), "cannot reach the router") {
		t.Fatalf("%v", err)
	}
}

// A benchmark that compares settings generates and pushes them before each
// variant's runs, resets before every run, and alternates the order.
func TestBenchAlternatesAndConfigures(t *testing.T) {
	var pushed []string // RAFT_LEADER_SEND_FIRST of shard1-node1's env at every push
	f := &Fake{}
	f.OnCopy = func(h inventory.Host, local, _ string) {
		if h.Name == "shard1-node1" && strings.HasSuffix(filepath.ToSlash(local), "/env") {
			b, _ := os.ReadFile(local)
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "RAFT_LEADER_SEND_FIRST=") || strings.HasPrefix(line, "RAFT_TICK_MS=") {
					pushed = append(pushed, line)
				}
			}
		}
	}
	c := newCtl(t, f)
	c.Dir, c.Dry = t.TempDir(), true
	p, err := FindPreset("pipeline-ab", BenchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	runs, err := c.Bench(context.Background(), p, Layout{Set: []string{"RAFT_TICK_MS=50"}}, BenchOptions{Repeats: 4, Out: out})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, r := range runs {
		order = append(order, r.Variant)
	}
	if got, want := strings.Join(order, " "), "pipeline-off pipeline-on pipeline-on pipeline-off pipeline-off pipeline-on pipeline-on pipeline-off"; got != want {
		t.Fatalf("order %s, want %s", got, want)
	}
	// Pushed once per change of settings (5 changes in 8 runs), with the
	// cluster's own setting kept.
	if got, want := strings.Join(pushed, " "), "RAFT_TICK_MS=50 RAFT_LEADER_SEND_FIRST=false RAFT_TICK_MS=50 RAFT_LEADER_SEND_FIRST=true RAFT_TICK_MS=50 RAFT_LEADER_SEND_FIRST=false RAFT_TICK_MS=50 RAFT_LEADER_SEND_FIRST=true RAFT_TICK_MS=50 RAFT_LEADER_SEND_FIRST=false"; got != want {
		t.Fatalf("pushed %s", got)
	}
	resets := 0
	for _, cmd := range f.On("coord-1") {
		if strings.HasPrefix(cmd, "echo ") && strings.HasSuffix(cmd, "/opt/seatsafe/cluster-id") {
			resets++
		}
	}
	if resets != 8 {
		t.Fatalf("%d resets, want one before each of the 8 runs", resets)
	}
	for _, name := range []string{"runs.jsonl", "summary.md"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatal(err)
		}
	}

	// A layout preset stops the hosts the smaller layout does not use.
	f = &Fake{}
	c.Exec = f
	p, _ = FindPreset("shards-1", BenchOptions{})
	if _, err := c.Bench(context.Background(), p, Layout{}, BenchOptions{Repeats: 1, Out: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if got := f.On("shard3-node2"); len(got) != 1 || !strings.HasPrefix(got[0], "docker rm -f seatsafe") {
		t.Fatalf("a host outside the 1-shard layout: %q", got)
	}
	if len(c.Inv.Shards()) != 1 || !slices.Equal(chaos.ShardIDs, []int{1}) {
		t.Fatalf("layout after shards-1: %v, chaos %v", c.Inv.Shards(), chaos.ShardIDs)
	}
	chaos.ShardIDs, chaos.NodeIDs = []int{1, 2, 3}, []int{1, 2, 3}
}

func TestBenchTable(t *testing.T) {
	p := Preset{Name: "pipeline-ab", About: "off against on", Variants: []Variant{{Name: "off"}, {Name: "on"}, {Name: "never-run"}}}
	run := func(variant string, rep int, p99 float64, audit, bookings string) BenchRun {
		v := "PASS"
		if audit != "PASS" || bookings != "PASS" {
			v = "FAIL"
		}
		return BenchRun{Variant: variant, Repeat: rep, Dir: "out/" + variant, Audit: audit, Bookings: bookings, Verdict: v,
			Metrics: map[string]float64{"achieved_per_s": 1000 + float64(rep), "p99_ms": p99, "failover_shard_1_s": 0.4}}
	}
	runs := []BenchRun{run("off", 1, 10, "PASS", "PASS"), run("on", 1, 5.5, "PASS", "PASS"), run("on", 2, 6.5, "PASS", "PASS"),
		run("off", 2, 12, "PASS", "PASS"), run("off", 3, 14, "FAIL", "PASS")}
	runs[4].Err = "correctness audit failed"
	table := BenchTable(p, runs)
	for _, want := range []string{
		"| Variant | Runs | Audit | Bookings | Achieved /s | p99 ms |",
		"| off | 3 | 2/3 pass | 3/3 pass | 1002 ± 1 | 12.0 ± 2.0 |",
		"| on | 2 | 2/2 pass | 2/2 pass | 1002 ± 1 | 6.00 ± 0.71 |",
		"| off | 3 | FAIL | FAIL | PASS | out/off |",
		"- off, run 1: shard 1 0.40 s",
		"- off, run 3: FAIL: correctness audit failed",
	} {
		if !strings.Contains(table, want) {
			t.Fatalf("the table is missing %q:\n%s", want, table)
		}
	}
	if strings.Contains(table, "never-run") {
		t.Fatalf("a row for a variant without runs:\n%s", table)
	}
	if m, sd := meanSD([]float64{2, 4, 4, 4, 5, 5, 7, 9}); m != 5 || math.Abs(sd-2.138089935) > 1e-6 {
		t.Fatalf("mean %v, sd %v", m, sd)
	}
	if m, sd := meanSD([]float64{3}); m != 3 || sd != 0 || cell([]float64{3}) != "3.00" || cell(nil) != "-" {
		t.Fatal("one value")
	}
}

func TestParseStat(t *testing.T) {
	s := parseStat("cpu  100 0 50 800 50 0 0 0 0 0\nMemAvailable:    2048000 kB\n/dev/nvme1n1 104857600 5242880 99614720 5% /data\n")
	if !s.ok || s.total != 1000 || s.busy != 150 || s.memAvailKB != 2048000 || s.diskUsedKB != 5242880 {
		t.Fatalf("%+v", s)
	}
	hosts := []inventory.Host{{Name: "s1", Role: inventory.Storage, Shard: 1, Node: 1}, {Name: "s2", Role: inventory.Storage, Shard: 1, Node: 2}, {Name: "r1", Role: inventory.Router}}
	before := map[string]hostStat{"s1": {busy: 100, total: 1000, ok: true, diskUsedKB: 1024}, "s2": {busy: 0, total: 1000, ok: true}, "r1": {}}
	after := map[string]hostStat{"s1": {busy: 400, total: 2000, ok: true, diskUsedKB: 3072, memAvailKB: 1024 * 512}, "s2": {busy: 100, total: 2000, ok: true}, "r1": {}}
	us := hostUsage(hosts, before, after)
	if us[0].CPU != 30 || us[0].DiskM != 2 || us[0].MemAvailM != 512 || us[1].CPU != 10 || us[2].CPU != -1 {
		t.Fatalf("%+v", us)
	}
	if by := us.byRole(); by[inventory.Storage] != [2]float64{20, 30} || len(by) != 1 {
		t.Fatalf("%v", by)
	}
	if table := usageTable(us); !strings.Contains(table, "| s1 | storage | 30.0% | 512 MiB | 2 MiB |") || !strings.Contains(table, "| r1 | router | - | 0 MiB | - |") {
		t.Fatalf("%s", table)
	}
}

// A detached campaign: the image's own clusterctl, the inventory, the
// layout and (only if asked) the key go to the first load generator host,
// and the campaign is started there under nohup, on private addresses.
func TestDetach(t *testing.T) {
	work := t.TempDir()
	invFile := filepath.Join(work, "inventory.yaml")
	src, _ := os.ReadFile(filepath.Join("..", "..", "deploy", "aws", "inventory.example.yaml"))
	if err := os.WriteFile(invFile, src, 0o644); err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(work, "key.pem")
	_ = os.WriteFile(key, []byte("KEY"), 0o600)
	var remoteInv string
	f := &Fake{}
	f.OnCopy = func(_ inventory.Host, local, remote string) {
		if remote == "/opt/seatsafe/inventory.yaml" {
			b, _ := os.ReadFile(local)
			remoteInv = string(b)
		}
	}
	c := newCtl(t, f)
	d, err := c.Detach(context.Background(), invFile, key, "nov21", []string{"-runs", "90", "-seed", "7"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Host.Name != "loadgen-1" || d.Dir != "/opt/seatsafe/results/nov21" || d.Log != "/opt/seatsafe/results/nov21.log" {
		t.Fatalf("%+v", d)
	}
	got := f.On("loadgen-1")
	want := []string{
		"mkdir -p /opt/seatsafe/bin /opt/seatsafe/gen /opt/seatsafe/results",
		"", // the inventory, checked below
		"copy gen/layout.json -> /opt/seatsafe/gen/layout.json",
		"copy " + filepath.ToSlash(key) + " -> /opt/seatsafe/ssh_key",
		"chmod 600 /opt/seatsafe/ssh_key",
		"docker rm -f ss-extract >/dev/null 2>&1; docker create --name ss-extract 123456789012.dkr.ecr.us-west-2.amazonaws.com/seatsafe:latest >/dev/null && docker cp ss-extract:/usr/local/bin/clusterctl /opt/seatsafe/bin/clusterctl && docker rm ss-extract >/dev/null",
		"test -f /opt/seatsafe/ssh_key",
		"cd /opt/seatsafe && nohup /opt/seatsafe/bin/clusterctl -inventory /opt/seatsafe/inventory.yaml -dir /opt/seatsafe/gen -ssh-private campaign -out /opt/seatsafe/results/nov21 -runs 90 -seed 7 > /opt/seatsafe/results/nov21.log 2>&1 < /dev/null & echo started",
	}
	if len(got) != len(want) {
		t.Fatalf("%d commands:\n%s", len(got), strings.Join(got, "\n"))
	}
	for i := range want {
		if want[i] != "" && got[i] != want[i] {
			t.Fatalf("command %d:\n%s\nwant:\n%s", i, got[i], want[i])
		}
	}
	if !strings.HasSuffix(got[1], " -> /opt/seatsafe/inventory.yaml") || !strings.Contains(remoteInv, "ssh_key: /opt/seatsafe/ssh_key\n") || strings.Contains(remoteInv, "~/.ssh") {
		t.Fatalf("the inventory on the host:\n%s", remoteInv)
	}
	if _, err := inventory.Parse([]byte(remoteInv)); err != nil {
		t.Fatalf("the inventory on the host does not load: %v", err)
	}
	if len(f.Calls()) != len(got) {
		t.Fatal("something was done on another host")
	}

	// Without a key file nothing is copied, and a host without a key is an error.
	f = &Fake{Reply: func(_ inventory.Host, cmd string) (string, error) {
		if strings.HasPrefix(cmd, "test -f") {
			return "", errors.New("exit status 1")
		}
		return "", nil
	}}
	c.Exec = f
	_, err = c.Detach(context.Background(), invFile, "", "nov21", nil)
	if err == nil || !strings.Contains(err.Error(), "-copy-key") {
		t.Fatalf("%v", err)
	}
	for _, cmd := range f.On("loadgen-1") {
		if strings.Contains(cmd, "ssh_key") && !strings.HasPrefix(cmd, "test -f") || strings.Contains(cmd, "nohup") {
			t.Fatalf("without a key: %s", cmd)
		}
	}

	f = &Fake{}
	c.Exec = f
	if _, err := c.CampaignStatus(context.Background(), "nov21"); err != nil {
		t.Fatal(err)
	}
	if cmd := f.On("loadgen-1")[0]; !strings.Contains(cmd, "tail -n 8 /opt/seatsafe/results/nov21.log") || !strings.Contains(cmd, "cat /opt/seatsafe/results/nov21/summary.md") || !strings.Contains(cmd, "pgrep -f") {
		t.Fatalf("%s", cmd)
	}
	if err := c.FetchResults(context.Background(), "nov21", "results/nov21"); err != nil || f.On("loadgen-1")[1] != "fetch /opt/seatsafe/results/nov21 -> results/nov21" {
		t.Fatalf("%v %q", err, f.On("loadgen-1"))
	}
}

// The campaign's mix has every fault of the Compose campaign and the zone
// outage.
func TestMix(t *testing.T) {
	if len(Mix) != 9 || Mix[8].Kind != chaos.AZOutage || len(chaos.Mix) != 8 {
		t.Fatalf("%+v", Mix)
	}
	kinds := map[chaos.Kind]int{}
	for _, s := range Mix {
		kinds[s.Kind]++
	}
	if kinds[chaos.KillLeader] != 2 || kinds[chaos.PauseLeader] != 2 || kinds[chaos.Partition] != 2 || kinds[chaos.None] != 2 {
		t.Fatalf("%v", kinds)
	}
}
