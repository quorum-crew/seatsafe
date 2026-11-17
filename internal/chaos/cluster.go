// Package chaos injects faults into the Docker Compose cluster while the load
// generator runs, and checks after every run that no seat was double-booked
// (the load generator's audit) and that every shard healed: a leader, all
// three replicas back, and all of them at the same applied index.
//
// Faults are done with the Docker CLI only, so the same tool works on Linux,
// macOS and Windows hosts: docker kill/start (crash and restart), docker
// pause/unpause (a frozen process), docker network disconnect/connect (a node
// cut off from everyone), and tc netem inside the container (network delay).
package chaos

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Docker runs one docker CLI command and returns its stdout.
type Docker interface {
	Run(ctx context.Context, args ...string) (string, error)
}

// CLI runs the real docker binary.
type CLI struct{}

func (CLI) Run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if msg == "" {
			msg = strings.TrimSpace(out.String())
		}
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, msg)
	}
	return out.String(), nil
}

// Shards and replicas of the Compose cluster.
var (
	ShardIDs = []int{1, 2, 3}
	NodeIDs  = []int{1, 2, 3} // Raft ID = position in PEERS = node number
)

// Cluster is the Compose project the faults are injected into.
type Cluster struct {
	Docker  Docker
	Project string   // compose project name, e.g. seatsafe
	Files   []string // compose files, e.g. docker-compose.yml
}

// Service names a storage node's Compose service.
func Service(shard, node int) string { return fmt.Sprintf("shard%d-node%d", shard, node) }

// Container is the container name Compose gives a service.
func (c *Cluster) Container(service string) string { return c.Project + "-" + service + "-1" }

// Network is the cluster's Compose network.
func (c *Cluster) Network() string { return c.Project + "_seatsafe" }

func (c *Cluster) compose(ctx context.Context, args ...string) (string, error) {
	full := []string{"compose", "-p", c.Project}
	for _, f := range c.Files {
		full = append(full, "-f", f)
	}
	return c.Docker.Run(ctx, append(full, args...)...)
}

// Reset wipes the cluster, Raft logs included, and starts it again, waiting
// until every node is healthy (storage nodes: leader known and seeded).
func (c *Cluster) Reset(ctx context.Context) error {
	if _, err := c.compose(ctx, "down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	_, err := c.compose(ctx, "up", "-d", "--wait", "--wait-timeout", "600")
	return err
}

// Metrics reads a node's Raft gauges from its /metrics endpoint.
func (c *Cluster) Metrics(ctx context.Context, service string) (map[string]float64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := c.Docker.Run(ctx, "exec", c.Container(service), "wget", "-qO-", "http://localhost:8080/metrics")
	if err != nil {
		return nil, err
	}
	m := map[string]float64{}
	sc := bufio.NewScanner(strings.NewReader(out))
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "seatsafe_raft_") {
			continue
		}
		name, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if i := strings.IndexByte(name, '{'); i >= 0 {
			if strings.HasPrefix(name, "seatsafe_raft_follower_") {
				continue // per-follower series; not needed here
			}
			name = name[:i]
		}
		if v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64); err == nil {
			m[name] = v
		}
	}
	return m, sc.Err()
}

// Leader returns the node number of the shard's leader as its replicas see
// it, or 0 if none of them knows one.
func (c *Cluster) Leader(ctx context.Context, shard int) int {
	for _, n := range NodeIDs {
		m, err := c.Metrics(ctx, Service(shard, n))
		if err != nil {
			continue
		}
		if m["seatsafe_raft_is_leader"] == 1 {
			return n
		}
		if l := int(m["seatsafe_raft_leader_id"]); l != 0 {
			return l
		}
	}
	return 0
}

// ShardHealth is what WaitHealed found for one shard.
type ShardHealth struct {
	Shard   int
	Leader  int
	Applied []uint64 // per node; equal when converged
	Err     string   // why it is not healed, if it isn't
}

// healthy reports whether every replica answers, all agree on one leader,
// and all have applied the same index.
func (c *Cluster) shardHealth(ctx context.Context, shard int) ShardHealth {
	h := ShardHealth{Shard: shard}
	leaders := map[int]bool{}
	for _, n := range NodeIDs {
		m, err := c.Metrics(ctx, Service(shard, n))
		if err != nil {
			h.Err = fmt.Sprintf("node %d unreachable", n)
			return h
		}
		leaders[int(m["seatsafe_raft_leader_id"])] = true
		h.Applied = append(h.Applied, uint64(m["seatsafe_raft_applied_index"]))
	}
	if len(leaders) != 1 || leaders[0] {
		h.Err = fmt.Sprintf("replicas disagree on the leader or have none: %v", keys(leaders))
		return h
	}
	for l := range leaders {
		h.Leader = l
	}
	for _, a := range h.Applied[1:] {
		if a != h.Applied[0] {
			h.Err = fmt.Sprintf("applied indexes differ: %v", h.Applied)
			return h
		}
	}
	return h
}

// WaitHealed waits up to timeout for every shard to be healthy and returns
// how long it took, or the shards that never healed.
func (c *Cluster) WaitHealed(ctx context.Context, timeout time.Duration) (time.Duration, []ShardHealth, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		var bad []ShardHealth
		for _, s := range ShardIDs {
			if h := c.shardHealth(ctx, s); h.Err != "" {
				bad = append(bad, h)
			}
		}
		if len(bad) == 0 {
			return time.Since(start), nil, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return time.Since(start), bad, fmt.Errorf("%d shard(s) not healed after %v", len(bad), timeout)
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func keys(m map[int]bool) []int {
	var out []int
	for k := range m {
		out = append(out, k)
	}
	return out
}
