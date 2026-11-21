package clusterctl

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/quorum-crew/seatsafe/internal/chaos"
	"github.com/quorum-crew/seatsafe/internal/inventory"
)

// Remote makes the cluster of a Ctl the target of the chaos tool
// (chaos.Cluster.Remote): faults are docker and iptables commands on the
// hosts, and the load runs on the load generator hosts.
type Remote struct {
	C *Ctl
	// Loader runs the load of one chaos run; see Ctl.Load.
	Loader func(ctx context.Context, dir string, args []string) (string, error)
	// PartitionMax is how long a partition can last at most: the host
	// removes it by itself after that long, whatever happens to this
	// program. 0 means 5 minutes.
	PartitionMax time.Duration
	// Token names a partition; nil means the time. Replaced in tests.
	Token func() string
}

var _ chaos.Remote = (*Remote)(nil)

// UseLayout tells the chaos package the shape of the cluster: its shard ids
// and the node numbers of a group.
func UseLayout(inv *inventory.Inventory) {
	chaos.ShardIDs = inv.Shards()
	chaos.NodeIDs = nil
	for _, h := range inv.Group(0) {
		chaos.NodeIDs = append(chaos.NodeIDs, h.Node)
	}
}

func (r *Remote) host(service string) (inventory.Host, error) {
	for _, h := range r.C.Inv.Nodes() {
		if h.Service() == service {
			return h, nil
		}
	}
	return inventory.Host{}, fmt.Errorf("no host runs %s", service)
}

func (r *Remote) hosts(services []string) ([]inventory.Host, error) {
	var out []inventory.Host
	for _, s := range services {
		h, err := r.host(s)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, nil
}

func (r *Remote) on(ctx context.Context, services []string, cmd func(h inventory.Host) string) error {
	hosts, err := r.hosts(services)
	if err != nil {
		return err
	}
	return each(hosts, func(h inventory.Host) error {
		_, err := r.C.Exec.Run(ctx, h, cmd(h))
		return err
	})
}

func (r *Remote) Metrics(ctx context.Context, service string) (string, error) {
	h, err := r.host(service)
	if err != nil {
		return "", err
	}
	return r.C.Exec.Run(ctx, h, "docker exec "+r.C.Container(h)+" "+metricsCmd)
}

func (r *Remote) Kill(ctx context.Context, services []string) error {
	return r.on(ctx, services, func(h inventory.Host) string { return "docker kill -s KILL " + r.C.Container(h) })
}

func (r *Remote) Start(ctx context.Context, services []string) error {
	return r.on(ctx, services, func(h inventory.Host) string { return "docker start " + r.C.Container(h) })
}

func (r *Remote) Pause(ctx context.Context, service string) error {
	return r.on(ctx, []string{service}, func(h inventory.Host) string { return "docker pause " + r.C.Container(h) })
}

func (r *Remote) Unpause(ctx context.Context, service string) error {
	return r.on(ctx, []string{service}, func(h inventory.Host) string {
		return "docker unpause " + r.C.Container(h) + " 2>&1 | grep -v 'is not paused'; true"
	})
}

// chain is the iptables chain every rule of a partition goes into, so that
// removing the partition is removing the chain and nothing else on the host
// is ever touched.
const chain = "SEATSAFE_CHAOS"

// tokenFile holds the token of the partition that is in place, so that the
// timer of an earlier partition cannot end a later one early.
const tokenFile = "/tmp/seatsafe-chaos-token"

// healPartition removes the chain. It is safe to run when there is none.
const healPartition = "sudo iptables -D INPUT -j " + chain + " 2>/dev/null; sudo iptables -D OUTPUT -j " + chain + " 2>/dev/null; " +
	"sudo iptables -F " + chain + " 2>/dev/null; sudo iptables -X " + chain + " 2>/dev/null; rm -f " + tokenFile + "; true"

// partitionCmd cuts a host off from the other nodes of the cluster, in both
// directions, by dropping packets from and to their private addresses.
//
// It never touches ssh: the only addresses in the rules are those of the
// other storage, coordinator and router hosts, never the machine this runs
// from, a load generator or the monitor, and no policy or other chain is
// changed. Before any rule is added, the host is told to remove the chain by
// itself after limit, so the partition ends even if this program is killed
// or loses its connection. The timer acts only if this partition (its
// token) is still the one in place.
func partitionCmd(peers []string, limit time.Duration, token string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "echo %s > %s; (nohup sh -c 'sleep %d; [ \"$(cat %s 2>/dev/null)\" = %s ] || exit 0; %s' >/dev/null 2>&1 &); ",
		token, tokenFile, int(math.Ceil(limit.Seconds())), tokenFile, token, healPartition)
	fmt.Fprintf(&b, "sudo iptables -N %s 2>/dev/null || sudo iptables -F %s; ", chain, chain)
	fmt.Fprintf(&b, "for ip in %s; do sudo iptables -A %s -s $ip -j DROP && sudo iptables -A %s -d $ip -j DROP || exit 1; done; ", strings.Join(peers, " "), chain, chain)
	fmt.Fprintf(&b, "sudo iptables -C INPUT -j %s 2>/dev/null || sudo iptables -I INPUT 1 -j %s; ", chain, chain)
	fmt.Fprintf(&b, "sudo iptables -C OUTPUT -j %s 2>/dev/null || sudo iptables -I OUTPUT 1 -j %s", chain, chain)
	return b.String()
}

// token names one partition.
func (r *Remote) token() string {
	if r.Token != nil {
		return r.Token()
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func (r *Remote) hostNetwork() bool { return r.C.Inv.Network == inventory.NetworkHost }

// Partition cuts a node off from the rest of the cluster. On real machines
// (host networking) that is iptables on its host. On one machine the
// containers share a Docker network, and the node is taken off it.
func (r *Remote) Partition(ctx context.Context, service string) error {
	h, err := r.host(service)
	if err != nil {
		return err
	}
	if !r.hostNetwork() {
		_, err := r.C.Exec.Run(ctx, h, "docker network disconnect "+shq(r.C.Inv.Network)+" "+r.C.Container(h))
		return err
	}
	var peers []string
	for _, p := range r.C.Inv.Nodes() {
		if p.Name != h.Name {
			peers = append(peers, p.PrivateIP)
		}
	}
	limit := r.PartitionMax
	if limit <= 0 {
		limit = 5 * time.Minute
	}
	_, err = r.C.Exec.Run(ctx, h, partitionCmd(peers, limit, r.token()))
	return err
}

func (r *Remote) Rejoin(ctx context.Context, service string) error {
	h, err := r.host(service)
	if err != nil {
		return err
	}
	cmd := healPartition
	if !r.hostNetwork() {
		cmd = fmt.Sprintf("docker network connect --alias %s %s %s 2>&1 | grep -v 'already exists'; true", shq(h.PrivateIP), shq(r.C.Inv.Network), r.C.Container(h))
	}
	_, err = r.C.Exec.Run(ctx, h, cmd)
	return err
}

// Cleanup removes whatever a chaos run that was killed may have left on the
// nodes: a partition, a paused container, a stopped one. A campaign runs it
// before its first run.
func (r *Remote) Cleanup(ctx context.Context) error {
	return each(r.C.Inv.Nodes(), func(h inventory.Host) error {
		name := r.C.Container(h)
		cmd := "docker unpause " + name + " >/dev/null 2>&1; docker start " + name + " >/dev/null 2>&1; true"
		if r.hostNetwork() {
			cmd = healPartition + "; " + cmd
		} else {
			cmd = fmt.Sprintf("docker network connect --alias %s %s %s >/dev/null 2>&1; ", shq(h.PrivateIP), shq(r.C.Inv.Network), name) + cmd
		}
		_, err := r.C.Exec.Run(ctx, h, cmd)
		return err
	})
}

func (r *Remote) Reset(ctx context.Context) error { return r.C.Reset(ctx) }

func (r *Remote) Logs(ctx context.Context) (string, error) {
	nodes := r.C.Inv.Nodes()
	logs := make([]string, len(nodes))
	err := each(nodes, func(h inventory.Host) error {
		out, err := r.C.Exec.Run(ctx, h, "docker logs --timestamps --tail 5000 "+r.C.Container(h)+" 2>&1")
		for i := range nodes {
			if nodes[i].Name == h.Name {
				logs[i] = "==== " + h.Name + " (" + h.Service() + ")\n" + out
			}
		}
		return err
	})
	return strings.Join(logs, "\n"), err
}

func (r *Remote) Load(ctx context.Context, dir string, args []string) (string, error) {
	return r.Loader(ctx, dir, args)
}

// Zones returns the services in each availability zone: the nodes of the
// Raft groups and the routers.
func (r *Remote) Zones() map[string][]string {
	out := map[string][]string{}
	for _, h := range r.C.Inv.Nodes() {
		if h.AZ != "" {
			out[h.AZ] = append(out[h.AZ], h.Service())
		}
	}
	return out
}

// Clock is what chronyc tracking says about one host's clock.
type Clock struct {
	Host           string  `json:"host"`
	OffsetMs       float64 `json:"offset_ms"`          // system time against NTP time; positive = fast
	RootDispersion float64 `json:"root_dispersion_ms"` // the error bound chrony carries
	RootDelay      float64 `json:"root_delay_ms"`
	Err            string  `json:"error,omitempty"`
}

// ClockReport is the clocks of the load generator hosts before a run and
// the widening the history check is given for them.
type ClockReport struct {
	Clocks  []Clock `json:"clocks"`
	WidenMs float64 `json:"widen_ms"`
	Note    string  `json:"note,omitempty"`
}

var (
	chronySystem = regexp.MustCompile(`(?m)^System time\s*:\s*([0-9.eE+-]+) seconds (fast|slow)`)
	chronyDisp   = regexp.MustCompile(`(?m)^Root dispersion\s*:\s*([0-9.eE+-]+) seconds`)
	chronyDelay  = regexp.MustCompile(`(?m)^Root delay\s*:\s*([0-9.eE+-]+) seconds`)
)

// parseChrony reads the output of chronyc tracking.
func parseChrony(host, out string) Clock {
	c := Clock{Host: host}
	sys, disp := chronySystem.FindStringSubmatch(out), chronyDisp.FindStringSubmatch(out)
	if sys == nil || disp == nil {
		c.Err = "chronyc tracking gave no system time and root dispersion"
		return c
	}
	off, _ := strconv.ParseFloat(sys[1], 64)
	if sys[2] == "slow" {
		off = -off
	}
	d, _ := strconv.ParseFloat(disp[1], 64)
	c.OffsetMs, c.RootDispersion = off*1000, d*1000
	if m := chronyDelay.FindStringSubmatch(out); m != nil {
		delay, _ := strconv.ParseFloat(m[1], 64)
		c.RootDelay = delay * 1000
	}
	return c
}

// Clocks asks every load generator host for its clock's offset and error
// bound and returns the widening for histories merged from them: the largest
// offset plus root dispersion of any host, plus 1 ms. On one machine there
// is one clock and nothing to widen. If a host cannot say (no chronyc),
// fallback is used and the report says so.
func (c *Ctl) Clocks(ctx context.Context, fallback time.Duration) ClockReport {
	var rep ClockReport
	if c.local() {
		rep.Note = "one machine, one clock: no widening"
		return rep
	}
	hosts := c.Inv.ByRole(inventory.Loadgen)
	rep.Clocks = make([]Clock, len(hosts))
	_ = each(hosts, func(h inventory.Host) error {
		out, err := c.Exec.Run(ctx, h, "chronyc tracking")
		cl := parseChrony(h.Name, out)
		if err != nil {
			cl.Err = firstLine(err.Error())
		}
		for i := range hosts {
			if hosts[i].Name == h.Name {
				rep.Clocks[i] = cl
			}
		}
		return nil
	})
	worst, failed := 0.0, false
	for _, cl := range rep.Clocks {
		failed = failed || cl.Err != ""
		worst = math.Max(worst, math.Abs(cl.OffsetMs)+cl.RootDispersion)
	}
	rep.WidenMs = worst + 1
	if failed {
		rep.WidenMs = math.Max(rep.WidenMs, float64(fallback)/float64(time.Millisecond))
		rep.Note = fmt.Sprintf("a host's clock could not be read: widening by at least the fallback of %v", fallback)
	}
	return rep
}

// Widen is the report's widening as a duration.
func (r ClockReport) Widen() time.Duration {
	return time.Duration(r.WidenMs * float64(time.Millisecond))
}
