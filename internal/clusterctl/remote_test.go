package clusterctl

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/chaos"
	"github.com/quorum-crew/seatsafe/internal/inventory"
)

// metricsOf answers /metrics like a cluster in which node 2 leads every
// group.
func metricsOf(h inventory.Host, cmd string) (string, error) {
	if strings.HasSuffix(cmd, metricsCmd) {
		leader := 0
		if h.Node == 2 {
			leader = 1
		}
		return "seatsafe_raft_is_leader " + string(rune('0'+leader)) + "\nseatsafe_raft_leader_id 2\nseatsafe_raft_applied_index 100\nseatsafe_raft_term 4\n", nil
	}
	return healthy(h, cmd)
}

func newRemote(t *testing.T, f *Fake) *Remote {
	t.Helper()
	c := newCtl(t, f)
	UseLayout(c.Inv)
	t.Cleanup(func() { chaos.ShardIDs, chaos.NodeIDs = []int{1, 2, 3}, []int{1, 2, 3} })
	return &Remote{C: c, Token: func() string { return "tok1" }}
}

// Kill, pause and their heals are docker commands on the host of the node.
func TestRemoteKillPause(t *testing.T) {
	f := &Fake{Reply: metricsOf}
	r := newRemote(t, f)
	in := &chaos.Injector{C: &chaos.Cluster{Remote: r}, Rand: rand.New(rand.NewPCG(1, 2))}
	ctx := context.Background()

	fault, err := in.InjectAt(ctx, chaos.KillLeader, chaos.TargetCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	if err := fault.Heal(ctx); err != nil {
		t.Fatal(err)
	}
	got := f.On("coord-2")
	if n := len(got); n < 2 || got[n-2] != "docker kill -s KILL seatsafe" || got[n-1] != "docker start seatsafe" {
		t.Fatalf("kill-leader on the coordinator: %q", got)
	}

	fault, err = in.InjectAt(ctx, chaos.PauseLeader, chaos.TargetShard)
	if err != nil {
		t.Fatal(err)
	}
	if err := fault.Heal(ctx); err != nil {
		t.Fatal(err)
	}
	leader := "shard" + string(rune('0'+fault.Shard)) + "-node2"
	got = f.On(leader)
	if n := len(got); n < 2 || got[n-2] != "docker pause seatsafe" || !strings.HasPrefix(got[n-1], "docker unpause seatsafe") {
		t.Fatalf("pause-leader on %s: %q", leader, got)
	}
}

// A partition is iptables on the leader's host: packets from and to the
// other cluster nodes are dropped, in a chain of its own. It never touches
// ssh, and the host removes it by itself even if nobody heals it.
func TestRemotePartition(t *testing.T) {
	f := &Fake{Reply: metricsOf}
	r := newRemote(t, f)
	in := &chaos.Injector{C: &chaos.Cluster{Remote: r}, Rand: rand.New(rand.NewPCG(1, 2))}
	ctx := context.Background()
	fault, err := in.InjectAt(ctx, chaos.Partition, chaos.TargetCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	got := f.On("coord-2")
	cmd := got[len(got)-1]

	// Every other storage, coordinator and router host is cut off, both ways.
	for _, h := range r.C.Inv.Nodes() {
		if has := strings.Contains(cmd, " "+h.PrivateIP+" ") || strings.Contains(cmd, " "+h.PrivateIP+";"); has != (h.Name != "coord-2") {
			t.Fatalf("peer %s (%s) in the rules: %v\n%s", h.Name, h.PrivateIP, has, cmd)
		}
	}
	for _, want := range []string{"sudo iptables -A SEATSAFE_CHAOS -s $ip -j DROP", "sudo iptables -A SEATSAFE_CHAOS -d $ip -j DROP",
		"sudo iptables -I INPUT 1 -j SEATSAFE_CHAOS", "sudo iptables -I OUTPUT 1 -j SEATSAFE_CHAOS"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("the partition is missing %q:\n%s", want, cmd)
		}
	}
	// ssh stays: no load generator, monitor or public address, no port, no
	// policy, no flush of a chain that is not ours.
	for _, h := range r.C.Inv.Hosts {
		if h.Role == inventory.Loadgen || h.Role == inventory.Monitor {
			if strings.Contains(cmd, h.PrivateIP) {
				t.Fatalf("the rules name %s (%s)", h.Name, h.PrivateIP)
			}
		}
		if strings.Contains(cmd, h.PublicIP) {
			t.Fatalf("the rules name the public address of %s", h.Name)
		}
	}
	for _, bad := range []string{"--dport", "--sport", " 22", "-P INPUT", "-P OUTPUT", "-F INPUT", "-F OUTPUT", "iptables -F;", "iptables -F ;", "-j REJECT"} {
		if strings.Contains(cmd, bad) {
			t.Fatalf("the partition contains %q:\n%s", bad, cmd)
		}
	}
	// The dead-man timer is set up before the first rule.
	timer := "echo tok1 > /tmp/seatsafe-chaos-token; (nohup sh -c 'sleep 300; [ \"$(cat /tmp/seatsafe-chaos-token 2>/dev/null)\" = tok1 ] || exit 0; " + healPartition + "' >/dev/null 2>&1 &); "
	if !strings.HasPrefix(cmd, timer) {
		t.Fatalf("the partition does not start with its own removal:\n%s", cmd)
	}

	if err := fault.Heal(ctx); err != nil {
		t.Fatal(err)
	}
	got = f.On("coord-2")
	if heal := got[len(got)-1]; heal != healPartition || !strings.Contains(heal, "iptables -D INPUT -j SEATSAFE_CHAOS") || !strings.Contains(heal, "iptables -X SEATSAFE_CHAOS") {
		t.Fatalf("heal: %s", heal)
	}
	// Only that host was given any iptables command.
	for _, call := range f.Calls() {
		if strings.Contains(call.Cmd, "iptables") && call.Host != "coord-2" {
			t.Fatalf("iptables on %s: %s", call.Host, call.Cmd)
		}
	}
}

// Cleanup undoes what a killed run may have left on every node.
func TestRemoteCleanup(t *testing.T) {
	f := &Fake{}
	r := newRemote(t, f)
	if err := r.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.Calls()) != 14 {
		t.Fatalf("%d calls, want one per node", len(f.Calls()))
	}
	want := healPartition + "; docker unpause seatsafe >/dev/null 2>&1; docker start seatsafe >/dev/null 2>&1; true"
	for _, call := range f.Calls() {
		if call.Cmd != want {
			t.Fatalf("%s: %s", call.Host, call.Cmd)
		}
	}
}

// Zones are the inventory's: one node of every group and the routers there.
func TestRemoteZones(t *testing.T) {
	r := newRemote(t, &Fake{})
	z := r.Zones()
	if len(z) != 3 {
		t.Fatalf("%v", z)
	}
	a := slices.Clone(z["us-west-2a"])
	slices.Sort(a)
	if got := strings.Join(a, " "); got != "coord-1 router-1 shard1-node1 shard2-node1 shard3-node1" {
		t.Fatalf("us-west-2a: %s", got)
	}
	if got := len(z["us-west-2c"]); got != 4 {
		t.Fatalf("us-west-2c has %d services", got)
	}
	// An az-outage kills them all at once and starts them again.
	f := r.C.Exec.(*Fake)
	in := &chaos.Injector{C: &chaos.Cluster{Remote: r}, Rand: rand.New(rand.NewPCG(5, 2))}
	fault, err := in.InjectAt(context.Background(), chaos.AZOutage, "")
	if err != nil {
		t.Fatal(err)
	}
	killed := 0
	for _, call := range f.Calls() {
		if call.Cmd == "docker kill -s KILL seatsafe" {
			killed++
			if h, _ := r.C.Inv.Host(call.Host); h.AZ != fault.Zone {
				t.Fatalf("killed %s, which is in %s, not %s", call.Host, h.AZ, fault.Zone)
			}
		}
	}
	if killed != len(z[fault.Zone]) {
		t.Fatalf("killed %d containers, zone %s has %d", killed, fault.Zone, len(z[fault.Zone]))
	}
}

// On one machine a partition takes the container off the Docker network.
func TestLocalPartition(t *testing.T) {
	inv, err := inventory.Parse([]byte(`executor: local
network: seatsafe-local
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
	f := &Fake{}
	c, err := New(inv, "gen", f)
	if err != nil {
		t.Fatal(err)
	}
	r := &Remote{C: c}
	ctx := context.Background()
	if err := r.Partition(ctx, "shard1-node1"); err != nil {
		t.Fatal(err)
	}
	if err := r.Rejoin(ctx, "shard1-node1"); err != nil {
		t.Fatal(err)
	}
	got := f.On("s1")
	if len(got) != 2 || got[0] != "docker network disconnect seatsafe-local ss-s1" || !strings.HasPrefix(got[1], "docker network connect --alias s1 seatsafe-local ss-s1") {
		t.Fatalf("%q", got)
	}
	if strings.Contains(strings.Join(got, " "), "iptables") {
		t.Fatal("iptables on a Docker network")
	}
	if _, err := r.Metrics(ctx, "shard9-node1"); err == nil {
		t.Fatal("metrics of a node that is not in the inventory")
	}
}

const chronyOut = `Reference ID    : A9FEA97B (169.254.169.123)
Stratum         : 4
Ref time (UTC)  : Fri Nov 20 20:14:08 2026
System time     : 0.000412345 seconds slow of NTP time
Last offset     : -0.000003118 seconds
RMS offset      : 0.000011223 seconds
Frequency       : 12.431 ppm slow
Residual freq   : -0.001 ppm
Skew            : 0.027 ppm
Root delay      : 0.000512000 seconds
Root dispersion : 0.000233000 seconds
Update interval : 16.2 seconds
Leap status     : Normal
`

// The widening is the largest offset plus root dispersion of any load
// generator host, plus 1 ms.
func TestClocks(t *testing.T) {
	cl := parseChrony("loadgen-1", chronyOut)
	if cl.Err != "" || math.Abs(cl.OffsetMs+0.412345) > 1e-9 || math.Abs(cl.RootDispersion-0.233) > 1e-9 || math.Abs(cl.RootDelay-0.512) > 1e-9 {
		t.Fatalf("%+v", cl)
	}
	f := &Fake{Reply: func(h inventory.Host, cmd string) (string, error) {
		if cmd != "chronyc tracking" {
			t.Errorf("%s: %s", h.Name, cmd)
		}
		if h.Name == "loadgen-2" {
			return strings.Replace(strings.Replace(chronyOut, "0.000412345 seconds slow", "0.001900000 seconds fast", 1), "0.000233000", "0.000100000", 1), nil
		}
		return chronyOut, nil
	}}
	c := newCtl(t, f)
	rep := c.Clocks(context.Background(), 5*time.Millisecond)
	if len(rep.Clocks) != 2 || rep.Note != "" || math.Abs(rep.WidenMs-3.0) > 1e-9 || rep.Widen() < 2999*time.Microsecond || rep.Widen() > 3001*time.Microsecond {
		t.Fatalf("%+v", rep)
	}
	if len(f.On("loadgen-1")) != 1 || len(f.On("shard1-node1")) != 0 {
		t.Fatal("the clocks of the load generator hosts are the ones that matter")
	}

	// A host without chronyc: the fallback, and the report says so.
	f = &Fake{Reply: func(h inventory.Host, _ string) (string, error) {
		if h.Name == "loadgen-2" {
			return "", errors.New("sh: chronyc: command not found")
		}
		return chronyOut, nil
	}}
	rep = newCtl(t, f).Clocks(context.Background(), 5*time.Millisecond)
	if rep.WidenMs != 5 || !strings.Contains(rep.Note, "could not be read") || rep.Clocks[1].Err == "" {
		t.Fatalf("%+v", rep)
	}

	// One machine: no widening, and nothing is asked.
	inv, _ := inventory.Parse([]byte("executor: local\nimage: x\nhosts:\n  - {name: l1, role: loadgen, private_ip: l1}\n  - {name: r1, role: router, private_ip: r1}\n"))
	f = &Fake{}
	lc, _ := New(inv, "gen", f)
	if rep := lc.Clocks(context.Background(), 5*time.Millisecond); rep.WidenMs != 0 || len(f.Calls()) != 0 {
		t.Fatalf("%+v", rep)
	}
}
