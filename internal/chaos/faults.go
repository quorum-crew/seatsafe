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

	// Partition cuts the current leader of the chosen group (a shard or the
	// coordinator) off the network with docker network disconnect and
	// reconnects it when the fault is healed. On a shard it is what
	// isolate-leader does. Not part of "all".
	Partition Kind = "partition"

	// None injects nothing: a control run under the same load, to count
	// elections nobody caused. Not part of "all".
	None Kind = "none"
)

// RemovesLeader reports whether the fault takes the shard's leader away, so
// the rest of the group has to elect a new one (a failover).
func (k Kind) RemovesLeader() bool {
	return k == KillLeader || k == IsolateLeader || k == PauseLeader || k == Partition
}

// Target says which kind of Raft group a fault goes to.
type Target string

const (
	TargetShard       Target = "shard"       // one of the shards, picked at random
	TargetCoordinator Target = "coordinator" // the coordinator group
)

// onCoordinator reports whether the fault can be aimed at the coordinator
// group: the ones that act on one node of a group.
func (k Kind) onCoordinator() bool {
	switch k {
	case KillLeader, KillFollower, IsolateLeader, IsolateFollower, PauseLeader, Partition:
		return true
	}
	return false
}

// CoordinatorPlan is a plan that targets the coordinator group with each of
// kinds in turn. It is an error if any of them cannot target the
// coordinator (quorum-loss, delay-shard, kill-router and rolling-restart are
// shard or router faults): those runs could only be skipped.
func CoordinatorPlan(kinds []Kind) ([]Step, error) {
	var plan []Step
	var cannot, can []string
	for _, k := range kinds {
		if k != None && !k.onCoordinator() {
			cannot = append(cannot, string(k))
			continue
		}
		plan = append(plan, Step{Kind: k, Target: TargetCoordinator})
	}
	if len(cannot) > 0 {
		for _, k := range append(slices.Clone(AllKinds), Partition) {
			if k.onCoordinator() {
				can = append(can, string(k))
			}
		}
		return nil, fmt.Errorf("faults %s cannot target the coordinator; with -target coordinator use -faults with some of %s, none",
			strings.Join(cannot, ", "), strings.Join(can, ", "))
	}
	return plan, nil
}

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
		if !slices.Contains(AllKinds, k) && k != None && k != Partition {
			return nil, fmt.Errorf("unknown fault %q (known: %v, partition, none)", k, AllKinds)
		}
		out = append(out, k)
	}
	return out, nil
}

// Fault is one injected fault. Heal undoes it; it is safe to call more than
// once and after a failed Inject.
type Fault struct {
	Kind        Kind
	Shard       int      // 0 for the router and for the coordinator
	Coordinator bool     // the fault is in the coordinator group
	Targets     []string // compose services affected
	Detail      string   // e.g. "delay 120ms±30ms"

	heal func(ctx context.Context) error
}

// Group returns the Raft group the fault is in. ok is false for a fault
// outside every group (the router, none).
func (f *Fault) Group() (group int, ok bool) {
	if f.Coordinator {
		return CoordinatorGroup, true
	}
	return f.Shard, f.Shard != 0
}

// TargetName is "shard N", "coordinator", or "-" for a fault outside every
// group.
func (f *Fault) TargetName() string {
	if g, ok := f.Group(); ok {
		return GroupName(g)
	}
	return "-"
}

func (f *Fault) String() string {
	s := string(f.Kind)
	if g, ok := f.Group(); ok {
		s += " " + GroupName(g)
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

// Inject picks a shard for kind and applies the fault there.
func (in *Injector) Inject(ctx context.Context, kind Kind) (*Fault, error) {
	return in.InjectAt(ctx, kind, TargetShard)
}

// InjectAt picks a target for kind in a shard or in the coordinator group
// and applies the fault. The returned Fault is non-nil whenever something may
// have been changed, even with an error, so the caller can always Heal it.
func (in *Injector) InjectAt(ctx context.Context, kind Kind, target Target) (*Fault, error) {
	shard := ShardIDs[in.Rand.IntN(len(ShardIDs))]
	f := &Fault{Kind: kind, Shard: shard}
	if kind == None {
		f.Shard = 0
		return f, nil
	}
	switch target {
	case TargetShard, "":
	case TargetCoordinator:
		if !kind.onCoordinator() {
			return nil, fmt.Errorf("fault %q cannot target the coordinator", kind)
		}
		shard, f.Shard, f.Coordinator = CoordinatorGroup, 0, true
	default:
		return nil, fmt.Errorf("unknown target %q", target)
	}
	pickLeader := func() (int, error) {
		l := in.C.Leader(ctx, shard)
		if l == 0 {
			return 0, fmt.Errorf("%s has no leader to target", GroupName(shard))
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
	case KillLeader, KillFollower, PauseLeader, IsolateLeader, IsolateFollower, Partition:
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
