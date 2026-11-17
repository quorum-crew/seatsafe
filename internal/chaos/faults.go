package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"
)

// Kind is a type of fault.
type Kind string

// The fault kinds. A target is picked at injection time, so "leader" means
// whichever node leads that shard at that moment.
const (
	KillLeader      Kind = "kill-leader"      // SIGKILL the leader, restart it later
	KillFollower    Kind = "kill-follower"    // SIGKILL a follower, restart it later
	IsolateLeader   Kind = "isolate-leader"   // cut the leader off the network, then reconnect it
	IsolateFollower Kind = "isolate-follower" // cut a follower off the network, then reconnect it
	PauseLeader     Kind = "pause-leader"     // freeze the leader's process (like a long GC pause)
	QuorumLoss      Kind = "quorum-loss"      // kill two of a shard's three nodes, so it can't commit
	DelayShard      Kind = "delay-shard"      // add network delay to every node of a shard
	KillRouter      Kind = "kill-router"      // SIGKILL the stateless router, restart it later
	RollingRestart  Kind = "rolling-restart"  // restart a shard's nodes one after another
)

// AllKinds lists every fault kind, in a stable order.
var AllKinds = []Kind{KillLeader, KillFollower, IsolateLeader, IsolateFollower, PauseLeader, QuorumLoss, DelayShard, KillRouter, RollingRestart}

// ParseKinds parses a comma-separated list of kinds ("all" for every kind).
func ParseKinds(s string) ([]Kind, error) {
	if s == "" || s == "all" {
		return slices.Clone(AllKinds), nil
	}
	var out []Kind
	for _, p := range strings.Split(s, ",") {
		k := Kind(strings.TrimSpace(p))
		if !slices.Contains(AllKinds, k) {
			return nil, fmt.Errorf("unknown fault %q (known: %v)", k, AllKinds)
		}
		out = append(out, k)
	}
	return out, nil
}

// Fault is one injected fault. Heal undoes it; it is safe to call more than
// once and after a failed Inject.
type Fault struct {
	Kind    Kind
	Shard   int      // 0 for the router
	Targets []string // compose services affected
	Detail  string   // e.g. "delay 120ms±30ms"

	heal func(ctx context.Context) error
}

func (f *Fault) String() string {
	s := string(f.Kind)
	if f.Shard != 0 {
		s += fmt.Sprintf(" shard %d", f.Shard)
	}
	s += " " + strings.Join(f.Targets, ",")
	if f.Detail != "" {
		s += " (" + f.Detail + ")"
	}
	return s
}

// Heal undoes the fault.
func (f *Fault) Heal(ctx context.Context) error {
	if f.heal == nil {
		return nil
	}
	return f.heal(ctx)
}

// Injector applies faults to a cluster.
type Injector struct {
	C    *Cluster
	Rand *rand.Rand
}

// Inject picks a target for kind and applies the fault. The returned Fault is
// non-nil whenever something may have been changed, even with an error, so
// the caller can always Heal it.
func (in *Injector) Inject(ctx context.Context, kind Kind) (*Fault, error) {
	shard := ShardIDs[in.Rand.IntN(len(ShardIDs))]
	f := &Fault{Kind: kind, Shard: shard}
	pickLeader := func() (int, error) {
		l := in.C.Leader(ctx, shard)
		if l == 0 {
			return 0, fmt.Errorf("shard %d has no leader to target", shard)
		}
		return l, nil
	}
	follower := func(leader int) int {
		var fs []int
		for _, n := range NodeIDs {
			if n != leader {
				fs = append(fs, n)
			}
		}
		return fs[in.Rand.IntN(len(fs))]
	}

	switch kind {
	case KillLeader, KillFollower, PauseLeader, IsolateLeader, IsolateFollower:
		l, err := pickLeader()
		if err != nil {
			return nil, err
		}
		n := l
		if kind == KillFollower || kind == IsolateFollower {
			n = follower(l)
		}
		svc := Service(shard, n)
		f.Targets = []string{svc}
		switch kind {
		case KillLeader, KillFollower:
			return f, in.kill(ctx, f, svc)
		case PauseLeader:
			return f, in.pause(ctx, f, svc)
		default:
			return f, in.isolate(ctx, f, svc)
		}

	case QuorumLoss:
		l, err := pickLeader()
		if err != nil {
			return nil, err
		}
		a, b := Service(shard, l), Service(shard, follower(l))
		f.Targets = []string{a, b}
		return f, in.kill(ctx, f, a, b)

	case DelayShard:
		delay := time.Duration(50+in.Rand.IntN(151)) * time.Millisecond // 50-200 ms
		jitter := delay / 4
		f.Detail = fmt.Sprintf("delay %v±%v", delay, jitter)
		for _, n := range NodeIDs {
			f.Targets = append(f.Targets, Service(shard, n))
		}
		return f, in.delay(ctx, f, delay, jitter)

	case KillRouter:
		f.Shard = 0
		f.Targets = []string{"router"}
		return f, in.kill(ctx, f, "router")

	case RollingRestart:
		for _, n := range NodeIDs {
			f.Targets = append(f.Targets, Service(shard, n))
		}
		// Done during Inject: one node at a time, each back before the next,
		// so the shard never loses its quorum.
		for _, svc := range f.Targets {
			if _, err := in.C.Docker.Run(ctx, "restart", "-t", "0", in.C.Container(svc)); err != nil {
				return f, err
			}
			if err := in.waitUp(ctx, svc, 20*time.Second); err != nil {
				return f, err
			}
		}
		return f, nil
	}
	return nil, fmt.Errorf("unknown fault %q", kind)
}

func (in *Injector) kill(ctx context.Context, f *Fault, services ...string) error {
	var containers []string
	for _, s := range services {
		containers = append(containers, in.C.Container(s))
	}
	f.heal = func(ctx context.Context) error {
		_, err := in.C.Docker.Run(ctx, append([]string{"start"}, containers...)...)
		return err
	}
	_, err := in.C.Docker.Run(ctx, append([]string{"kill", "-s", "KILL"}, containers...)...)
	return err
}

func (in *Injector) pause(ctx context.Context, f *Fault, svc string) error {
	c := in.C.Container(svc)
	f.heal = func(ctx context.Context) error {
		_, err := in.C.Docker.Run(ctx, "unpause", c)
		if err != nil && strings.Contains(err.Error(), "is not paused") {
			return nil
		}
		return err
	}
	_, err := in.C.Docker.Run(ctx, "pause", c)
	return err
}

func (in *Injector) isolate(ctx context.Context, f *Fault, svc string) error {
	c, net := in.C.Container(svc), in.C.Network()
	f.heal = func(ctx context.Context) error {
		// The alias keeps the service name resolvable: peers dial shardN-nodeM.
		_, err := in.C.Docker.Run(ctx, "network", "connect", "--alias", svc, net, c)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	_, err := in.C.Docker.Run(ctx, "network", "disconnect", net, c)
	return err
}

func (in *Injector) delay(ctx context.Context, f *Fault, delay, jitter time.Duration) error {
	var done []string
	f.heal = func(ctx context.Context) error {
		var first error
		for _, svc := range done {
			_, err := in.C.Docker.Run(ctx, "exec", "--privileged", in.C.Container(svc), "tc", "qdisc", "del", "dev", "eth0", "root")
			if err != nil && first == nil && !strings.Contains(err.Error(), "No such file") {
				first = err
			}
		}
		return first
	}
	for _, svc := range f.Targets {
		_, err := in.C.Docker.Run(ctx, "exec", "--privileged", in.C.Container(svc), "tc", "qdisc", "add", "dev", "eth0", "root",
			"netem", "delay", ms(delay), ms(jitter))
		if err != nil {
			return err
		}
		done = append(done, svc)
	}
	return nil
}

// waitUp waits until a storage node answers /metrics again.
func (in *Injector) waitUp(ctx context.Context, svc string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := in.C.Metrics(ctx, svc); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not back after %v", svc, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func ms(d time.Duration) string { return fmt.Sprintf("%dms", d.Milliseconds()) }
