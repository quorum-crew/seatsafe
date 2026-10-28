// Package groupclient talks to one Raft group (a shard, or the coordinator
// group) through its current leader.
//
// A Group keeps one gRPC connection per node and caches the leader. Call runs
// an RPC against the cached leader and handles the error contract:
//
//	OK, INVALID_ARGUMENT, other codes  returned as is
//	FAILED_PRECONDITION + trailer      switch to the leader named in the
//	                                   "seatsafe-leader" trailer and retry
//	                                   there at once (once per Call)
//	UNAVAILABLE, connection errors,    ask every node for NodeService.Status
//	attempt DEADLINE_EXCEEDED,         in parallel, adopt the leader they
//	FAILED_PRECONDITION without a      report, and retry with backoff
//	usable trailer                     (10, 25, 50, 100, then 200 ms) until
//	                                   the caller's ctx expires
//
// Each attempt gets its own deadline (500 ms by default, capped by ctx). The
// request is never changed between attempts, so retries are safe only for
// idempotent RPCs, which every SeatSafe write is (keyed by request_id or
// txn_id).
package groupclient

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/metrics"
)

// LeaderTrailer is the trailer key a follower sets to its leader's host:port.
const LeaderTrailer = "seatsafe-leader"

// Config tunes a Group. Zero fields take the defaults.
type Config struct {
	AttemptTimeout time.Duration   // per attempt; default 500ms
	StatusTimeout  time.Duration   // per discovery round; default 100ms
	Backoff        []time.Duration // waits between retries, last one repeats; default 10, 25, 50, 100, 200ms
	DialOptions    []grpc.DialOption
}

// DefaultBackoff is the retry schedule after a failed attempt.
var DefaultBackoff = []time.Duration{10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}

func (c Config) withDefaults() Config {
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = 500 * time.Millisecond
	}
	if c.StatusTimeout <= 0 {
		c.StatusTimeout = 100 * time.Millisecond
	}
	if len(c.Backoff) == 0 {
		c.Backoff = DefaultBackoff
	}
	return c
}

// Metrics are shared by every Group of a process.
type Metrics struct {
	Retries         *metrics.CounterVec   // group, reason
	LeaderChanges   *metrics.CounterVec   // group
	LeaderRecovery  *metrics.HistogramVec // group
	DiscoveryRounds *metrics.CounterVec   // group, result
}

// Retry reasons.
const (
	ReasonRedirect     = "redirect"        // FAILED_PRECONDITION with a leader trailer
	ReasonNotLeader    = "not_leader"      // FAILED_PRECONDITION without a usable trailer
	ReasonUnavailable  = "unavailable"     // UNAVAILABLE or a connection error
	ReasonAttemptTimer = "attempt_timeout" // the attempt's own deadline passed
)

// RecoveryBuckets cover failovers from a few ms to several seconds.
var RecoveryBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10}

// NewMetrics registers the group client metrics.
func NewMetrics(r *metrics.Registry) *Metrics {
	return &Metrics{
		Retries:       r.NewCounterVec("seatsafe_groupclient_retries_total", "Retried attempts by group and reason.", "group", "reason"),
		LeaderChanges: r.NewCounterVec("seatsafe_groupclient_leader_changes_total", "Times the cached leader of a group changed.", "group"),
		LeaderRecovery: r.NewHistogramVec("seatsafe_groupclient_leader_recovery_seconds",
			"Time from the first failed attempt against a group to its next success (failover time seen by the router).",
			RecoveryBuckets, "group"),
		DiscoveryRounds: r.NewCounterVec("seatsafe_groupclient_discovery_rounds_total", "NodeService.Status rounds by group and result (found, none).", "group", "result"),
	}
}

// Group is a client for one Raft group. Safe for concurrent use.
type Group struct {
	name  string
	addrs []string
	conns map[string]*grpc.ClientConn
	cfg   Config
	m     *Metrics

	mu           sync.Mutex
	leader       string        // cached leader; "" after it failed, until rediscovered
	lastLeader   string        // last leader ever cached, to count changes
	failingSince time.Time     // first failure since the last answer; zero when healthy
	discovering  chan struct{} // closed when the discovery round in flight ends
}

// New dials every node (lazily, as grpc.NewClient does) and returns the
// group. name labels its metrics, e.g. "shard-1" or "coordinator".
func New(name string, addrs []string, cfg Config, m *Metrics) (*Group, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("groupclient: group %s has no nodes", name)
	}
	cfg = cfg.withDefaults()
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, cfg.DialOptions...)
	g := &Group{name: name, addrs: append([]string(nil), addrs...), conns: map[string]*grpc.ClientConn{}, cfg: cfg, m: m}
	for _, a := range addrs {
		// passthrough: each address is one node, resolved by the dialer on
		// every (re)connect, so a restarted container's new IP is picked up.
		cc, err := grpc.NewClient("passthrough:///"+a, opts...)
		if err != nil {
			g.Close()
			return nil, fmt.Errorf("groupclient: %s: %w", a, err)
		}
		g.conns[a] = cc
	}
	return g, nil
}

// Name returns the group's metric label.
func (g *Group) Name() string { return g.name }

// Leader returns the cached leader address ("" if unknown).
func (g *Group) Leader() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader
}

// Close closes every connection.
func (g *Group) Close() error {
	var errs []error
	for _, cc := range g.conns {
		errs = append(errs, cc.Close())
	}
	return errors.Join(errs...)
}

// RPC performs one attempt on cc. It must pass opts to the generated client
// method so Call can read the response trailer.
type RPC func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error

// Call runs rpc against the group's leader, retrying as described in the
// package comment until it gets an answer or ctx ends. When ctx ends it
// returns DEADLINE_EXCEEDED (or CANCELED), never a guess.
func (g *Group) Call(ctx context.Context, rpc RPC) error {
	redirected := false
	retries := 0
	discovered := false // a round just ran at the end of the last retry
	for {
		target := g.Leader()
		if target == "" && !discovered {
			target = g.discover(ctx)
		}
		discovered = false
		if target == "" {
			target = g.addrs[retries%len(g.addrs)] // nobody knows: try nodes in turn
		}

		actx, cancel := context.WithTimeout(ctx, g.cfg.AttemptTimeout)
		var trailer metadata.MD
		err := rpc(actx, g.conns[target], grpc.Trailer(&trailer))
		cancel()

		reason := ""
		switch code := status.Code(err); {
		case ctx.Err() != nil && err != nil:
			g.markFailed()
			return ctxError(ctx)
		case code == codes.FailedPrecondition:
			if hint := g.usableHint(trailer, target); hint != "" && !redirected {
				redirected = true
				g.markFailed()
				g.m.Retries.Inc(g.name, ReasonRedirect)
				g.setLeader(target, hint)
				continue
			}
			reason = ReasonNotLeader
		case code == codes.Unavailable:
			reason = ReasonUnavailable
		case code == codes.DeadlineExceeded:
			reason = ReasonAttemptTimer
		default:
			// OK, INVALID_ARGUMENT, or any other answer from the server.
			g.markAnswered()
			return err
		}

		g.markFailed()
		g.m.Retries.Inc(g.name, reason)
		g.forget(target)
		if !g.sleep(ctx, g.cfg.Backoff[min(retries, len(g.cfg.Backoff)-1)]) {
			return ctxError(ctx)
		}
		retries++
		g.discover(ctx)
		discovered = true
	}
}

func ctxError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return status.Error(codes.Canceled, "groupclient: request canceled")
	}
	return status.Error(codes.DeadlineExceeded, "groupclient: deadline exceeded before the group answered")
}

func (g *Group) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// usableHint returns the trailer's leader address if it names another node
// of this group.
func (g *Group) usableHint(md metadata.MD, from string) string {
	vals := md.Get(LeaderTrailer)
	if len(vals) == 0 || vals[0] == from {
		return ""
	}
	if _, ok := g.conns[vals[0]]; !ok {
		return ""
	}
	return vals[0]
}

func (g *Group) markFailed() {
	g.mu.Lock()
	if g.failingSince.IsZero() {
		g.failingSince = time.Now()
	}
	g.mu.Unlock()
}

func (g *Group) markAnswered() {
	g.mu.Lock()
	since := g.failingSince
	g.failingSince = time.Time{}
	g.mu.Unlock()
	if !since.IsZero() {
		g.m.LeaderRecovery.Observe(time.Since(since).Seconds(), g.name)
	}
}

// setLeader replaces the cached leader if it is still old.
func (g *Group) setLeader(old, leader string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.leader != old && g.leader != "" {
		return // someone else already moved it
	}
	g.adopt(leader)
}

// adopt caches a leader and counts a change if it differs from the last
// one; g.mu must be held.
func (g *Group) adopt(leader string) {
	if leader == "" {
		return
	}
	if g.lastLeader != "" && g.lastLeader != leader {
		g.m.LeaderChanges.Inc(g.name)
	}
	g.leader, g.lastLeader = leader, leader
}

// forget drops target as the cached leader so the next attempt rediscovers.
func (g *Group) forget(target string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.leader == target {
		g.leader = ""
	}
}

// discover asks every node for its status and caches the leader they
// report. Concurrent callers share one round. It returns the cached leader
// afterwards ("" if none was found).
func (g *Group) discover(ctx context.Context) string {
	g.mu.Lock()
	if ch := g.discovering; ch != nil {
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		return g.Leader()
	}
	ch := make(chan struct{})
	g.discovering = ch
	g.mu.Unlock()

	leader := g.statusRound(ctx)

	g.mu.Lock()
	g.discovering = nil
	g.adopt(leader)
	cur := g.leader
	g.mu.Unlock()
	close(ch)

	result := "found"
	if leader == "" {
		result = "none"
	}
	g.m.DiscoveryRounds.Inc(g.name, result)
	return cur
}

// statusRound calls Status on every node in parallel and picks the leader
// of the highest term seen, counting both nodes that say they are LEADER and
// nodes that report a leader_addr. At that term a node saying LEADER wins
// over a reported leader_addr. So an old leader cut off from the group,
// still claiming LEADER at term 5, loses to followers that report a new
// leader at term 6. Ties break on address order so the choice does not
// depend on which reply came first.
func (g *Group) statusRound(ctx context.Context) string {
	sctx, cancel := context.WithTimeout(ctx, g.cfg.StatusTimeout)
	defer cancel()
	replies := make([]*seatsafev1.NodeStatus, len(g.addrs))
	var wg sync.WaitGroup
	for i, a := range g.addrs {
		wg.Add(1)
		go func(i int, a string) {
			defer wg.Done()
			st, err := seatsafev1.NewNodeServiceClient(g.conns[a]).Status(sctx, &seatsafev1.StatusRequest{})
			if err == nil {
				replies[i] = st
			}
		}(i, a)
	}
	wg.Wait()

	type cand struct {
		addr   string
		term   uint64
		leader bool
	}
	var cands []cand
	for i, st := range replies {
		switch {
		case st == nil:
		case st.GetRole() == seatsafev1.RaftRole_RAFT_ROLE_LEADER:
			cands = append(cands, cand{g.addrs[i], st.GetTerm(), true})
		case st.GetLeaderAddr() != "":
			if _, ok := g.conns[st.GetLeaderAddr()]; ok {
				cands = append(cands, cand{st.GetLeaderAddr(), st.GetTerm(), false})
			}
		}
	}
	if len(cands) == 0 {
		return ""
	}
	sort.Slice(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.term != b.term {
			return a.term > b.term
		}
		if a.leader != b.leader {
			return a.leader
		}
		return a.addr < b.addr
	})
	return cands[0].addr
}
