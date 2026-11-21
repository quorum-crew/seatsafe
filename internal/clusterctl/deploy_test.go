package clusterctl

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/inventory"
	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

func example(t *testing.T) *inventory.Inventory {
	t.Helper()
	inv, err := inventory.Load(filepath.Join("..", "..", "deploy", "aws", "inventory.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

func read(t *testing.T, parts ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// gen writes a shard map with the private addresses, the seed files, and an
// env file per node that names its peers by private address.
func TestGen(t *testing.T) {
	dir := t.TempDir()
	inv, err := Gen(example(t), dir, Layout{Set: []string{"RAFT_TICK_MS=50", "router:PPROF=1", "ROUTER_SHARD_ATTEMPT_MS=250"}})
	if err != nil {
		t.Fatal(err)
	}
	sm, err := shardmap.Load(filepath.Join(dir, "shardmap.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sm.ShardNodes(2), ","); got != "10.0.1.12:7000,10.0.2.12:7000,10.0.3.12:7000" {
		t.Fatalf("shard 2 nodes %s", got)
	}
	if got := strings.Join(sm.CoordinatorNodes(), ","); got != "10.0.1.20:7000,10.0.2.20:7000,10.0.3.20:7000" {
		t.Fatalf("coordinator nodes %s", got)
	}
	for _, s := range inv.Shards() {
		if _, err := seed.Load(seed.Path(filepath.Join(dir, "seed"), uint32(s)), uint32(s)); err != nil {
			t.Fatal(err)
		}
	}
	want := `NODE_ID=10.0.2.12
SHARD_ID=2
PEERS=10.0.1.12:7000,10.0.2.12:7000,10.0.3.12:7000
COORDINATOR_PEERS=10.0.1.20:7000,10.0.2.20:7000,10.0.3.20:7000
SEED_DIR=/opt/seatsafe/seed
DATA_DIR=/data
RAFT_TICK_MS=50
RAFT_ELECTION_TICKS=10
RAFT_HEARTBEAT_TICKS=1
RAFT_SNAPSHOT_ENTRIES=100000
RAFT_CATCHUP_ENTRIES=5000
GC_RETENTION_MS=120000
RAFT_LEADER_SEND_FIRST=true
RAFT_EARLY_APPLY=true
PPROF=0
`
	if got := read(t, dir, "hosts", "shard2-node2", "env"); got != want {
		t.Fatalf("storage env:\n%s", got)
	}
	want = `NODE_ID=10.0.3.20
PEERS=10.0.1.20:7000,10.0.2.20:7000,10.0.3.20:7000
SHARDMAP=/opt/seatsafe/shardmap.json
DATA_DIR=/data
RAFT_TICK_MS=50
RAFT_ELECTION_TICKS=10
RAFT_HEARTBEAT_TICKS=1
HOLD_TTL_MS=5000
GC_RETENTION_MS=120000
RAFT_LEADER_SEND_FIRST=true
RAFT_EARLY_APPLY=true
PPROF=0
`
	if got := read(t, dir, "hosts", "coord-3", "env"); got != want {
		t.Fatalf("coordinator env:\n%s", got)
	}
	want = "NODE_ID=router-1\nSHARDMAP=/opt/seatsafe/shardmap.json\nROUTER_SHARD_ATTEMPT_MS=250\nROUTER_COORD_ATTEMPT_MS=1000\nPPROF=1\n"
	if got := read(t, dir, "hosts", "router-1", "env"); got != want {
		t.Fatalf("router env:\n%s", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "hosts", "loadgen-1")); err == nil {
		t.Fatal("an env file for a load generator host")
	}
	prom := read(t, dir, "prometheus.yml")
	for _, w := range []string{"job_name: storage", `targets: ["10.0.3.13:8080"]`, `labels: {host: shard3-node3, az: us-west-2c, shard: "3"}`, `targets: ["10.0.1.30:8080"]`, "job_name: coordinator"} {
		if !strings.Contains(prom, w) {
			t.Fatalf("prometheus.yml is missing %q:\n%s", w, prom)
		}
	}
	if l, err := ReadLayout(dir); err != nil || l.Set[0] != "RAFT_TICK_MS=50" || l.Shards != 0 {
		t.Fatalf("layout %+v %v", l, err)
	}
}

// Smaller layouts: 1 and 2 shards, and a single node per group. A later gen
// leaves nothing of an earlier, larger one behind.
func TestGenLayouts(t *testing.T) {
	dir := t.TempDir()
	if _, err := Gen(example(t), dir, Layout{}); err != nil {
		t.Fatal(err)
	}
	inv, err := Gen(example(t), dir, Layout{Shards: 2, Replicas: 1, Seats: 4000})
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.ByRole(inventory.Storage)) != 2 || len(inv.Group(0)) != 1 {
		t.Fatalf("layout hosts: %+v", inv.Hosts)
	}
	sm, err := shardmap.Load(filepath.Join(dir, "shardmap.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := sm.ShardIDs(); len(got) != 2 || strings.Join(sm.ShardNodes(1), ",") != "10.0.1.11:7000" || len(sm.CoordinatorNodes()) != 1 {
		t.Fatalf("shards %v, shard 1 %v, coordinator %v", got, sm.ShardNodes(1), sm.CoordinatorNodes())
	}
	seats := 0
	for _, s := range sm.ShardIDs() {
		l, err := seed.Load(seed.Path(filepath.Join(dir, "seed"), s), s)
		if err != nil {
			t.Fatal(err)
		}
		seats += len(l.Seats)
	}
	if seats != 4000 {
		t.Fatalf("%d seats, want 4000", seats)
	}
	for _, gone := range []string{filepath.Join("seed", "shard3.json"), filepath.Join("hosts", "shard3-node1"), filepath.Join("hosts", "shard1-node2")} {
		if _, err := os.Stat(filepath.Join(dir, gone)); err == nil {
			t.Fatalf("%s is left from the larger layout", gone)
		}
	}
	if env := read(t, dir, "hosts", "shard1-node1", "env"); !strings.Contains(env, "PEERS=10.0.1.11:7000\n") {
		t.Fatalf("single-node env:\n%s", env)
	}
	if l, _ := ReadLayout(dir); l.Shards != 2 || l.Replicas != 1 || l.Seats != 4000 || len(l.Set) != 0 {
		t.Fatalf("layout %+v", l)
	}
}

func TestSetRejectsUnknown(t *testing.T) {
	for set, want := range map[string]string{
		"RAFT_TICKS_MS=50":           "no such setting",
		"router:RAFT_TICK_MS=50":     "no such setting",
		"proxy:PPROF=1":              "role must be storage, coordinator or router",
		"RAFT_TICK_MS":               "want KEY=VALUE",
		"NODE_ID=x":                  "no such setting",
		"=5":                         "want KEY=VALUE",
		"storage:GC_RETENTION_MS=":   "want KEY=VALUE",
		"coordinator:RAFT_EARLY=off": "no such setting",
	} {
		if _, err := Gen(example(t), t.TempDir(), Layout{Set: []string{set}}); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("-set %s: %v, want %q", set, err, want)
		}
	}
}

func newCtl(t *testing.T, exec Executor) *Ctl {
	t.Helper()
	c, err := New(example(t), "gen", exec)
	if err != nil {
		t.Fatal(err)
	}
	c.newID = func() string { return "ID1" }
	c.ReadyTimeout = 50 * time.Millisecond
	return c
}

// healthy answers like a cluster whose data matches its config.
func healthy(_ inventory.Host, cmd string) (string, error) {
	if strings.Contains(cmd, "ls -A /data") && !strings.Contains(cmd, "echo state=") {
		return "ok\n", nil
	}
	return "", nil
}

func TestPush(t *testing.T) {
	f := &Fake{}
	c := newCtl(t, f)
	if err := c.Push(context.Background()); err != nil {
		t.Fatal(err)
	}
	for host, want := range map[string][]string{
		"shard2-node3": {"mkdir -p /opt/seatsafe/seed /opt/seatsafe/results", "copy gen/seed/shard2.json -> /opt/seatsafe/seed/shard2.json",
			"copy gen/shardmap.json -> /opt/seatsafe/shardmap.json", "copy gen/hosts/shard2-node3/env -> /opt/seatsafe/env"},
		"coord-1":  {"mkdir -p /opt/seatsafe/seed /opt/seatsafe/results", "copy gen/shardmap.json -> /opt/seatsafe/shardmap.json", "copy gen/hosts/coord-1/env -> /opt/seatsafe/env"},
		"router-2": {"mkdir -p /opt/seatsafe/seed /opt/seatsafe/results", "copy gen/shardmap.json -> /opt/seatsafe/shardmap.json", "copy gen/hosts/router-2/env -> /opt/seatsafe/env"},
		"loadgen-1": {"mkdir -p /opt/seatsafe/seed /opt/seatsafe/results", "copy gen/seed/shard1.json -> /opt/seatsafe/seed/shard1.json",
			"copy gen/seed/shard2.json -> /opt/seatsafe/seed/shard2.json", "copy gen/seed/shard3.json -> /opt/seatsafe/seed/shard3.json", "copy gen/shardmap.json -> /opt/seatsafe/shardmap.json"},
		"monitor": {"mkdir -p /opt/seatsafe/seed /opt/seatsafe/results", "copy gen/prometheus.yml -> /opt/seatsafe/prometheus.yml"},
	} {
		if got := f.On(host); strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s:\n%s\nwant:\n%s", host, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	// Nothing is ever written under the data directory.
	for _, call := range f.Calls() {
		if strings.Contains(call.Cmd, "-> /data") || strings.Contains(call.Cmd, "mkdir -p /data") {
			t.Fatalf("push wrote under /data: %+v", call)
		}
	}
}

func TestUpDownReset(t *testing.T) {
	f := &Fake{Reply: healthy}
	c := newCtl(t, f)
	ctx := context.Background()
	if err := c.Up(ctx); err != nil {
		t.Fatal(err)
	}
	check := `docker run --rm -v /data:/data -v /opt/seatsafe:/opt/seatsafe:ro 123456789012.dkr.ecr.us-west-2.amazonaws.com/seatsafe:latest sh -c '` + checkData + `'`
	runStorage := "docker rm -f seatsafe >/dev/null 2>&1; docker run -d --name seatsafe --restart no --network host --env-file /opt/seatsafe/env -v /opt/seatsafe:/opt/seatsafe:ro -v /data:/data --ulimit nofile=65536:65536 123456789012.dkr.ecr.us-west-2.amazonaws.com/seatsafe:latest storage"
	if got := f.On("shard1-node2"); len(got) != 3 || got[0] != check || got[1] != runStorage || got[2] != "docker exec seatsafe wget -qO- http://localhost:8080/readyz" {
		t.Fatalf("storage up:\n%s", strings.Join(got, "\n"))
	}
	if got := f.On("coord-2"); !strings.HasSuffix(got[1], "seatsafe:latest coordinator") || !strings.Contains(got[1], "-v /data:/data") {
		t.Fatalf("coordinator up: %v", got)
	}
	router := f.On("router-1")
	if len(router) != 2 || strings.Contains(router[0], "/data") || !strings.HasSuffix(router[0], "seatsafe:latest router") || !strings.HasSuffix(router[1], "/healthz") {
		t.Fatalf("router up: %v", router)
	}
	if len(f.On("loadgen-1"))+len(f.On("monitor")) != 0 {
		t.Fatal("up touched a load generator or the monitor")
	}

	f = &Fake{Reply: healthy}
	c.Exec = f
	if err := c.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	got := f.On("shard3-node1")
	wipe := `docker run --rm -v /data:/data -v /opt/seatsafe:/opt/seatsafe:ro 123456789012.dkr.ecr.us-west-2.amazonaws.com/seatsafe:latest sh -c '` + wipeData + `'`
	want := []string{"docker rm -f seatsafe >/dev/null 2>&1; true", "echo ID1 > /opt/seatsafe/cluster-id", wipe, check, runStorage, "docker exec seatsafe wget -qO- http://localhost:8080/readyz"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reset:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if r := f.On("router-2"); len(r) != 3 || strings.Contains(strings.Join(r, " "), "cluster-id") {
		t.Fatalf("router reset: %v", r)
	}

	f = &Fake{}
	c.Exec = f
	if err := c.Down(ctx); err != nil || len(f.Calls()) != 14 {
		t.Fatalf("down: %v, %d calls", err, len(f.Calls()))
	}
}

// A node whose /data was wiped by an instance stop is not started.
func TestUpRefusesWipedData(t *testing.T) {
	for state, want := range map[string]string{"empty": "its data directory is empty", "other": "its data directory is from another cluster"} {
		f := &Fake{Reply: func(h inventory.Host, cmd string) (string, error) {
			if h.Name == "shard2-node1" && strings.Contains(cmd, "ls -A /data") {
				return state + "\n", nil
			}
			return healthy(h, cmd)
		}}
		err := newCtl(t, f).Up(context.Background())
		if err == nil || !strings.Contains(err.Error(), "shard2-node1: "+want) || !strings.Contains(err.Error(), "clusterctl reset") {
			t.Fatalf("%s: %v", state, err)
		}
		for _, cmd := range f.On("shard2-node1") {
			if strings.Contains(cmd, "docker run -d") {
				t.Fatalf("%s: the node was started: %s", state, cmd)
			}
		}
	}
}

func TestWaitReadyTimesOut(t *testing.T) {
	f := &Fake{Reply: func(h inventory.Host, cmd string) (string, error) {
		if h.Name == "coord-3" && strings.HasSuffix(cmd, "/readyz") {
			return "not ready: leader=0", errors.New("exit status 1")
		}
		return healthy(h, cmd)
	}}
	err := newCtl(t, f).Up(context.Background())
	if err == nil || !strings.Contains(err.Error(), "coord-3: not ready after 50ms: not ready: leader=0") {
		t.Fatalf("%v", err)
	}
}

func TestStatus(t *testing.T) {
	f := &Fake{Reply: func(h inventory.Host, cmd string) (string, error) {
		switch h.Name {
		case "shard1-node1":
			return "state=running\nrestarts=0\nconfig=yes\ndata=ok\ndisk=12% used, 98G free\nready=ready node=10.0.1.11 leader=1 term=3 applied=4521\n" +
				"seatsafe_raft_is_leader{shard=\"1\",node=\"10.0.1.11\"} 1\nseatsafe_raft_leader_id{shard=\"1\"} 1\nseatsafe_raft_term{shard=\"1\"} 3\nseatsafe_raft_applied_index{shard=\"1\"} 4521\n", nil
		case "shard1-node2":
			return "state=running\nrestarts=2\nconfig=yes\ndata=ok\ndisk=11% used, 99G free\nready=ready\nseatsafe_raft_is_leader 0\nseatsafe_raft_leader_id 1\nseatsafe_raft_term 3\nseatsafe_raft_applied_index 4520\n", nil
		case "shard1-node3": // the instance was stopped and started: /data is empty, the config is still there
			return "state=absent\nrestarts=\nconfig=yes\ndata=empty\ndisk=1% used, 110G free\nready=Error: No such container\n", nil
		case "shard2-node1":
			return "state=running\nrestarts=0\nconfig=yes\ndata=other\n", nil
		case "coord-1":
			return "state=absent\nconfig=no\ndata=empty\n", nil
		case "coord-2":
			return "", errors.New("ssh: connect to host 203.0.113.42 port 22: Connection timed out")
		case "router-1":
			return "state=exited\nrestarts=0\nconfig=yes\nready=\n", nil
		}
		return "state=running\nrestarts=0\nconfig=yes\ndata=ok\nready=ok\n", nil
	}}
	c := newCtl(t, f)
	rows := c.Status(context.Background())
	if len(rows) != 14 {
		t.Fatalf("%d rows", len(rows))
	}
	by := map[string]NodeStatus{}
	for _, r := range rows {
		by[r.Host.Name] = r
	}
	if s := by["shard1-node1"]; !s.Leader || s.Term != 3 || s.Applied != 4521 || s.Disk != "12% used, 98G free" || s.NeedsReset() {
		t.Fatalf("%+v", s)
	}
	if s := by["shard1-node2"]; s.Leader || s.LeaderID != 1 || s.Restarts != 2 {
		t.Fatalf("%+v", s)
	}
	if !by["shard1-node3"].NeedsReset() || !by["shard2-node1"].NeedsReset() || by["coord-1"].NeedsReset() || by["router-1"].NeedsReset() {
		t.Fatal("needs reset")
	}
	table := StatusTable(rows)
	for _, want := range []string{
		"| shard1-node1 | storage, shard 1 node 1 | us-west-2a | running | leader | 3 | 4521 | 0 | 12% used, 98G free |  |",
		"| shard1-node2 | storage, shard 1 node 2 | us-west-2b | running | follower of node 1 | 3 | 4520 | 2 |",
		"| shard1-node3 | storage, shard 1 node 3 | us-west-2c | absent | - | - | - | 0 | 1% used, 110G free | NEEDS RESET: /data is empty (wiped by an instance stop, or never set up); run clusterctl reset |",
		"NEEDS RESET: /data is from another cluster; run clusterctl reset",
		"| coord-1 | coordinator, node 1 | us-west-2a | absent | - | - | - | 0 | - | no config: run clusterctl push |",
		"| coord-2 | coordinator, node 2 | us-west-2b | unknown | - | - | - | 0 | - | unreachable: ssh: connect to host 203.0.113.42 port 22: Connection timed out |",
		"| router-1 | router | us-west-2a | exited | - | - | - | 0 | - | not running |",
	} {
		if !strings.Contains(table, want) {
			t.Fatalf("status table is missing %q:\n%s", want, table)
		}
	}
	if cmd := f.On("router-1")[0]; strings.Contains(cmd, "/data") || !strings.Contains(cmd, "/healthz") {
		t.Fatalf("router status command: %s", cmd)
	}
}

// With the local executor every host is this machine: containers and config
// directories are per host, /data is a volume, and the containers share a
// Docker network on which they answer to their addresses.
func TestLocalCommands(t *testing.T) {
	inv, err := inventory.Parse([]byte(`executor: local
network: seatsafe-local
config_dir: local/opt
data_dir: volume
image: seatsafe:dev
hosts:
  - {name: s1, role: storage, shard: 1, node: 1, az: z1, private_ip: s1}
  - {name: c1, role: coordinator, node: 1, az: z1, private_ip: c1}
  - {name: r1, role: router, private_ip: r1}
`))
	if err != nil {
		t.Fatal(err)
	}
	f := &Fake{Reply: healthy}
	c, err := New(inv, "gen", f)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(filepath.FromSlash(c.Inv.ConfigDir)) || strings.Contains(c.Inv.ConfigDir, `\`) {
		t.Fatalf("config dir %q", c.Inv.ConfigDir)
	}
	if err := c.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := c.Inv.ConfigDir + "/s1"
	got := f.On("s1")
	if got[0] != "docker network inspect seatsafe-local >/dev/null 2>&1 || docker network create seatsafe-local" {
		t.Fatalf("network: %s", got[0])
	}
	want := "docker rm -f ss-s1 >/dev/null 2>&1; docker run -d --name ss-s1 --restart no --network seatsafe-local --network-alias s1 --env-file " + shq(cfg+"/env") +
		" -v " + shq(cfg) + ":/opt/seatsafe:ro -v ss-s1-data:/data --ulimit nofile=65536:65536 seatsafe:dev storage"
	if got[2] != want {
		t.Fatalf("local run:\n%s\nwant:\n%s", got[2], want)
	}
	if !strings.Contains(got[1], "-v ss-s1-data:/data") || got[3] != "docker exec ss-s1 wget -qO- http://localhost:8080/readyz" {
		t.Fatalf("%v", got)
	}
}

func TestDryRunPrints(t *testing.T) {
	var buf bytes.Buffer
	d := &DryRun{W: &buf}
	c := newCtl(t, d)
	c.Dry = true
	ctx := context.Background()
	if err := c.Push(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"[shard1-node1] copy gen/hosts/shard1-node1/env -> /opt/seatsafe/env", "[coord-2] echo ID1 > /opt/seatsafe/cluster-id",
		"[router-1] docker rm -f seatsafe >/dev/null 2>&1; docker run -d --name seatsafe --restart no --network host", "[shard3-node3] docker exec seatsafe wget -qO- http://localhost:8080/readyz"} {
		if !strings.Contains(out, want) {
			t.Fatalf("dry run output is missing %q:\n%s", want, out)
		}
	}
}

func TestShq(t *testing.T) {
	for in, want := range map[string]string{"/opt/seatsafe": "/opt/seatsafe", "a b": "'a b'", "it's": `'it'\''s'`, "": "''", "C:/Users/x y/opt": "'C:/Users/x y/opt'", "$(x)": "'$(x)'"} {
		if got := shq(in); got != want {
			t.Fatalf("shq(%q) = %s, want %s", in, got, want)
		}
	}
}
