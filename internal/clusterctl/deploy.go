package clusterctl

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/quorum-crew/seatsafe/internal/inventory"
)

// Ctl drives one cluster: the hosts of Inv (a layout), through Exec.
type Ctl struct {
	Inv  *inventory.Inventory
	Exec Executor
	// Dir is the local directory gen writes to and push reads from.
	Dir string
	// Dry is set with a DryRun executor: nothing is waited for.
	Dry bool
	// ReadyTimeout bounds the wait for every node's /readyz; 0 means 3 minutes.
	ReadyTimeout time.Duration
	Logf         func(format string, args ...any)
	// Timing of a load on several load generator hosts (Load).
	Timing LoadTiming
	// Full is the whole inventory Inv is a layout of. A benchmark that
	// compares layouts or settings generates and pushes again from it.
	Full *inventory.Inventory

	// newID makes the cluster id of a reset; replaced in tests.
	newID func() string
}

// New returns a Ctl. With the local executor the config directory is made
// absolute, because Docker mounts it.
func New(inv *inventory.Inventory, dir string, exec Executor) (*Ctl, error) {
	if inv.Executor == inventory.ExecLocal {
		abs, err := filepath.Abs(inv.ConfigDir)
		if err != nil {
			return nil, err
		}
		cp := *inv
		cp.ConfigDir = filepath.ToSlash(abs)
		inv = &cp
	}
	return &Ctl{Inv: inv, Full: inv, Exec: exec, Dir: dir, Logf: func(string, ...any) {},
		newID: func() string { return time.Now().UTC().Format("20060102T150405.000000000Z") }}, nil
}

func (c *Ctl) local() bool { return c.Inv.Executor == inventory.ExecLocal }

// Container is the name of a host's SeatSafe container. On real machines
// there is one per host; on one machine they are told apart by host name.
func (c *Ctl) Container(h inventory.Host) string {
	if c.local() {
		return "ss-" + h.Name
	}
	return "seatsafe"
}

// ConfigDir is where a host keeps its env file, the shard map and its seed
// files. It is never under the data directory, which AWS wipes when an
// instance stops.
func (c *Ctl) ConfigDir(h inventory.Host) string {
	if c.local() {
		return c.Inv.ConfigDir + "/" + h.Name
	}
	return c.Inv.ConfigDir
}

// dataMount is what a node mounts at /data: the data directory, or a Docker
// volume of its own.
func (c *Ctl) dataMount(h inventory.Host) string {
	switch {
	case c.Inv.DataDir == "volume":
		return c.Container(h) + "-data"
	case c.local():
		return c.Inv.DataDir + "/" + h.Name
	}
	return c.Inv.DataDir
}

// netArgs puts a container on the cluster's network: the host's own, or a
// Docker network on which the container answers to the host's address.
func (c *Ctl) netArgs(h inventory.Host) string {
	if c.Inv.Network == inventory.NetworkHost {
		return "--network host"
	}
	return "--network " + shq(c.Inv.Network) + " --network-alias " + shq(h.PrivateIP)
}

// helper is a short-lived container with the host's data and config
// directories mounted, for looking at and wiping /data without needing root
// on the host.
func (c *Ctl) helper(h inventory.Host, script string) string {
	return fmt.Sprintf("docker run --rm -v %s:%s -v %s:%s:ro %s sh -c %s",
		shq(c.dataMount(h)), inData, shq(c.ConfigDir(h)), inConfig, shq(c.Inv.Image), shq(script))
}

// The cluster id ties a node's data to the cluster it was written for. reset
// writes a new one into every config directory and every /data. A node whose
// /data does not carry its config's id (empty after an instance stop, or
// left from another cluster) must not join: it would vote with a log it does
// not have.
const (
	idFile     = "cluster-id"
	checkData  = `if [ -z "$(ls -A /data 2>/dev/null)" ]; then echo empty; elif [ "$(cat /data/.cluster-id 2>/dev/null)" = "$(cat /opt/seatsafe/cluster-id 2>/dev/null)" ] && [ -s /opt/seatsafe/cluster-id ]; then echo ok; else echo other; fi`
	wipeData   = `rm -rf /data/* /data/.[!.]* 2>/dev/null; cp /opt/seatsafe/cluster-id /data/.cluster-id`
	metricsCmd = "wget -qO- http://localhost:8080/metrics"
)

func hasGroup(h inventory.Host) bool {
	return h.Role == inventory.Storage || h.Role == inventory.Coordinator
}

// Push copies the generated files to every host: the env file and the shard
// map to the nodes, a storage node's own seed file, everything a load
// generator needs to pick seats, and prometheus.yml to the monitor.
func (c *Ctl) Push(ctx context.Context) error {
	shards := c.Inv.Shards()
	return each(c.Inv.Hosts, func(h inventory.Host) error {
		dir := c.ConfigDir(h)
		if _, err := c.Exec.Run(ctx, h, "mkdir -p "+shq(dir+"/seed")+" "+shq(dir+"/results")); err != nil {
			return err
		}
		type file struct{ local, remote string }
		var files []file
		seed := func(s int) file {
			name := fmt.Sprintf("shard%d.json", s)
			return file{filepath.Join(c.Dir, "seed", name), dir + "/seed/" + name}
		}
		switch h.Role {
		case inventory.Storage:
			files = append(files, seed(h.Shard))
		case inventory.Loadgen:
			for _, s := range shards {
				files = append(files, seed(s))
			}
		case inventory.Monitor:
			files = append(files, file{filepath.Join(c.Dir, "prometheus.yml"), dir + "/prometheus.yml"})
		}
		if h.Role != inventory.Monitor {
			files = append(files, file{filepath.Join(c.Dir, "shardmap.json"), dir + "/shardmap.json"})
		}
		if h.Role != inventory.Loadgen && h.Role != inventory.Monitor {
			files = append(files, file{filepath.Join(c.Dir, "hosts", h.Name, "env"), dir + "/env"})
		}
		for _, f := range files {
			if err := c.Exec.Copy(ctx, h, f.local, f.remote); err != nil {
				return err
			}
		}
		c.Logf("%s: %d files", h.Name, len(files))
		return nil
	})
}

// runNode is the command that starts a host's SeatSafe container, replacing
// one that is there. The container is not restarted by Docker: after an
// instance stop its /data is gone, and it must not come back by itself.
func (c *Ctl) runNode(h inventory.Host) string {
	name, cfg := c.Container(h), c.ConfigDir(h)
	cmd := fmt.Sprintf("docker rm -f %s >/dev/null 2>&1; docker run -d --name %s --restart no %s --env-file %s -v %s:%s:ro",
		name, name, c.netArgs(h), shq(cfg+"/env"), shq(cfg), inConfig)
	if hasGroup(h) {
		cmd += fmt.Sprintf(" -v %s:%s", shq(c.dataMount(h)), inData)
	}
	return cmd + " --ulimit nofile=65536:65536 " + shq(c.Inv.Image) + " " + h.Role
}

// Up starts every node and waits until all of them are ready. A storage or
// coordinator node whose /data is empty or belongs to another cluster is not
// started: the cluster needs a reset.
func (c *Ctl) Up(ctx context.Context) error {
	if !c.local() || c.Inv.Network == inventory.NetworkHost {
		// nothing to prepare: the hosts' own network
	} else if _, err := c.Exec.Run(ctx, c.Inv.Hosts[0], "docker network inspect "+shq(c.Inv.Network)+" >/dev/null 2>&1 || docker network create "+shq(c.Inv.Network)); err != nil {
		return err
	}
	err := each(c.Inv.Nodes(), func(h inventory.Host) error {
		if hasGroup(h) {
			out, err := c.Exec.Run(ctx, h, c.helper(h, checkData))
			if err != nil {
				return err
			}
			if got := strings.TrimSpace(out); got != "ok" && !c.Dry {
				return fmt.Errorf("its data directory is %s: the node needs a reset (clusterctl reset); not started", describeData(got))
			}
		}
		_, err := c.Exec.Run(ctx, h, c.runNode(h))
		return err
	})
	if err != nil {
		return err
	}
	return c.WaitReady(ctx)
}

func describeData(state string) string {
	switch state {
	case "empty":
		return "empty (wiped by an instance stop, or never set up)"
	case "other":
		return "from another cluster"
	}
	return "unreadable (" + state + ")"
}

// Down stops and removes every node's container. The data stays.
func (c *Ctl) Down(ctx context.Context) error {
	return each(c.Inv.Nodes(), func(h inventory.Host) error {
		_, err := c.Exec.Run(ctx, h, "docker rm -f "+c.Container(h)+" >/dev/null 2>&1; true")
		return err
	})
}

// Reset stops every node, wipes every /data, gives the cluster a new id and
// starts it again, waiting until every node is ready. Run it after any
// instance stop and start, and between experiments.
func (c *Ctl) Reset(ctx context.Context) error {
	if err := c.Down(ctx); err != nil {
		return err
	}
	id := c.newID()
	err := each(c.Inv.Nodes(), func(h inventory.Host) error {
		if !hasGroup(h) {
			return nil
		}
		if _, err := c.Exec.Run(ctx, h, "echo "+id+" > "+shq(c.ConfigDir(h)+"/"+idFile)); err != nil {
			return err
		}
		_, err := c.Exec.Run(ctx, h, c.helper(h, wipeData))
		return err
	})
	if err != nil {
		return err
	}
	return c.Up(ctx)
}

// readyCmd asks a node whether it is ready: a storage or coordinator node
// when its group has a leader (and a shard is seeded), a router when it is
// up.
func (c *Ctl) readyCmd(h inventory.Host) string {
	path := "/readyz"
	if h.Role == inventory.Router {
		path = "/healthz"
	}
	return "docker exec " + c.Container(h) + " wget -qO- http://localhost:8080" + path
}

// WaitReady waits until every node is ready.
func (c *Ctl) WaitReady(ctx context.Context) error {
	timeout := c.ReadyTimeout
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	return each(c.Inv.Nodes(), func(h inventory.Host) error {
		for {
			out, err := c.Exec.Run(ctx, h, c.readyCmd(h))
			if err == nil || c.Dry {
				return nil
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				return fmt.Errorf("not ready after %v: %s %v", timeout, strings.TrimSpace(out), err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	})
}

// NodeStatus is one row of Status.
type NodeStatus struct {
	Host     inventory.Host
	State    string // Docker's state of the container, or "absent"
	Leader   bool
	LeaderID int // the group's leader as this node sees it; 0 = none
	Term     uint64
	Applied  uint64
	Restarts int
	Disk     string // use of the data directory's filesystem
	Config   bool   // the env file is on the host
	Data     string // "ok", "empty" or "other" (see checkData); "" if not asked
	Ready    string // the node's /readyz answer
	Err      string
}

// NeedsReset reports whether the node's config is there but its data is
// not the data of this cluster: what an instance stop leaves behind.
func (s NodeStatus) NeedsReset() bool {
	return hasGroup(s.Host) && s.Config && (s.Data == "empty" || s.Data == "other")
}

func (c *Ctl) statusCmd(h inventory.Host) string {
	name, cfg := c.Container(h), c.ConfigDir(h)
	var b strings.Builder
	fmt.Fprintf(&b, "echo state=$(docker inspect -f '{{.State.Status}}' %s 2>/dev/null || echo absent); ", name)
	fmt.Fprintf(&b, "echo restarts=$(docker inspect -f '{{.RestartCount}}' %s 2>/dev/null); ", name)
	fmt.Fprintf(&b, "echo config=$(test -f %s && echo yes || echo no); ", shq(cfg+"/env"))
	if hasGroup(h) {
		fmt.Fprintf(&b, "echo data=$(%s 2>/dev/null); ", c.helper(h, checkData))
		if c.Inv.DataDir != "volume" {
			fmt.Fprintf(&b, "echo disk=$(df -h %s 2>/dev/null | tail -1 | awk '{print $5\" used, \"$4\" free\"}'); ", shq(c.dataMount(h)))
		}
	}
	fmt.Fprintf(&b, "echo ready=$(%s 2>&1 | head -1); ", c.readyCmd(h))
	fmt.Fprintf(&b, "docker exec %s %s 2>/dev/null | grep -E '^seatsafe_raft_(is_leader|leader_id|term|applied_index)[ {]'; true", name, metricsCmd)
	return b.String()
}

// Status asks every node for its state.
func (c *Ctl) Status(ctx context.Context) []NodeStatus {
	nodes := c.Inv.Nodes()
	out := make([]NodeStatus, len(nodes))
	_ = each(nodes, func(h inventory.Host) error {
		i := 0
		for nodes[i].Name != h.Name {
			i++
		}
		text, err := c.Exec.Run(ctx, h, c.statusCmd(h))
		out[i] = parseStatus(h, text)
		if err != nil {
			out[i].Err = err.Error()
		}
		return nil
	})
	return out
}

func parseStatus(h inventory.Host, text string) NodeStatus {
	s := NodeStatus{Host: h, State: "unknown"}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if k, v, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(line, "seatsafe_") {
			switch k {
			case "state":
				s.State = v
			case "restarts":
				s.Restarts, _ = strconv.Atoi(v)
			case "config":
				s.Config = v == "yes"
			case "data":
				s.Data = v
			case "disk":
				s.Disk = v
			case "ready":
				s.Ready = v
			}
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			continue
		}
		switch name {
		case "seatsafe_raft_is_leader":
			s.Leader = f == 1
		case "seatsafe_raft_leader_id":
			s.LeaderID = int(f)
		case "seatsafe_raft_term":
			s.Term = uint64(f)
		case "seatsafe_raft_applied_index":
			s.Applied = uint64(f)
		}
	}
	return s
}

// StatusTable is the status as a markdown table, with a note for every node
// that needs attention.
func StatusTable(rows []NodeStatus) string {
	var b strings.Builder
	b.WriteString("| Host | Role | AZ | Container | Raft | Term | Applied | Restarts | Data disk | Note |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for _, s := range rows {
		role := s.Host.Role
		if s.Host.Role == inventory.Storage {
			role = fmt.Sprintf("storage, shard %d node %d", s.Host.Shard, s.Host.Node)
		} else if s.Host.Role == inventory.Coordinator {
			role = fmt.Sprintf("coordinator, node %d", s.Host.Node)
		}
		raft, term, applied := "-", "-", "-"
		if hasGroup(s.Host) && s.State == "running" {
			switch {
			case s.Leader:
				raft = "leader"
			case s.LeaderID != 0:
				raft = fmt.Sprintf("follower of node %d", s.LeaderID)
			default:
				raft = "no leader"
			}
			term, applied = fmt.Sprint(s.Term), fmt.Sprint(s.Applied)
		}
		var notes []string
		switch {
		case s.Err != "":
			notes = append(notes, "unreachable: "+firstLine(s.Err))
		case s.NeedsReset():
			notes = append(notes, "NEEDS RESET: /data is "+describeData(s.Data)+"; run clusterctl reset")
		case !s.Config:
			notes = append(notes, "no config: run clusterctl push")
		case s.State != "running":
			notes = append(notes, "not running")
		}
		disk := s.Disk
		if disk == "" {
			disk = "-"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %d | %s | %s |\n", s.Host.Name, role, dashIfEmpty(s.Host.AZ), s.State, raft, term, applied, s.Restarts, disk, strings.Join(notes, "; "))
	}
	return b.String()
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	if len(s) > 120 {
		s = s[:120] + "..."
	}
	return s
}

// Logs returns the last lines of a host's container log.
func (c *Ctl) Logs(ctx context.Context, h inventory.Host, tail int) (string, error) {
	return c.Exec.Run(ctx, h, fmt.Sprintf("docker logs --tail %d %s 2>&1", tail, c.Container(h)))
}
