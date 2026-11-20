// Package coordinator runs two-phase commit for orders whose seats span two
// or more shards. It serves CoordinatorService on the leader of the
// coordinator Raft group (group 0) and keeps every txn in that group's
// replicated state machine (internal/coordsm), so a new leader can finish
// what the old one started.
//
// # SubmitOrder
//
//  1. If this leader is already running the order, wait for that run.
//     Otherwise log TxnStarted with the participants; if the txn turns out
//     to be decided already, answer with the stored result (replayed = true).
//  2. Prepare on every participant in parallel, within PrepareTimeout.
//  3. All YES: propose TxnDecided(COMMIT, CONFIRMED). Any NO that names
//     conflicting seats: ABORT, SEAT_UNAVAILABLE with the conflicts of every
//     NO. Anything else (a timeout, a NO without conflicts): ABORT, ABORTED.
//  4. The first TxnDecided applied wins, and the answer is the decision that
//     won, which may be an ABORT that GetDecision or a new leader logged.
//  5. In the background, send Commit or Abort to each participant until it
//     answers, then log TxnAcked. A participant that refuses the decision
//     (FAILED_PRECONDITION without a leader trailer) contradicts the
//     coordinator's log: that is counted and logged, and not retried.
//
// The order runs to its decision even if the caller gives up.
//
// # GetDecision
//
// A decided txn returns its decision. Otherwise ABORT is proposed first
// (outcome ABORTED), also for a txn this group has never seen, and the
// decision that won is returned. It never answers before a decision is
// applied on this node, and never UNSPECIFIED.
//
// # Leadership
//
// A follower answers FAILED_PRECONDITION with the leader's address in the
// seatsafe-leader trailer; a node that knows no leader answers UNAVAILABLE.
// A node that becomes leader first applies everything logged before its
// term, then proposes ABORT for every txn without a decision (their Prepares
// may or may not have happened; an ABORT is always safe before a COMMIT is
// logged) and resends the decision of every decided txn that is missing an
// ack.
//
// The leader also proposes a GcOp every GCInterval. coordsm drops only txns
// that are decided, older than GCRetention and acked by every participant.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/coordsm"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

// Defaults for Config.
const (
	DefaultPrepareTimeout  = 300 * time.Millisecond // the whole prepare phase
	DefaultHoldTTL         = 5 * time.Second        // hold_ttl_ms sent with Prepare
	DefaultDecisionTimeout = 2 * time.Second        // one Commit or Abort attempt
	DefaultProposeTimeout  = 2 * time.Second        // one proposal made in the background
	DefaultGCInterval      = 30 * time.Second
	DefaultGCRetention     = 2 * time.Minute
	DefaultLeaderPoll      = 20 * time.Millisecond
	// DefaultStuckAfter is how old an undecided txn that nobody is running
	// must be before the leader aborts it (see sweep).
	DefaultStuckAfter = 30 * time.Second
)

// DefaultBackoff is the wait between attempts to deliver a decision to a
// shard; the last value repeats.
var DefaultBackoff = []time.Duration{
	25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond,
	500 * time.Millisecond, time.Second, 2 * time.Second,
}

// Config configures a Coordinator. Node, SM, Addrs and Shards are required.
type Config struct {
	Node   *raftnode.Node
	SM     *coordsm.Machine
	Addrs  map[uint64]string             // Raft ID -> gRPC address, for leader redirects
	Shards map[uint32]*groupclient.Group // participant groups by shard id

	PrepareTimeout  time.Duration
	HoldTTL         time.Duration
	DecisionTimeout time.Duration
	ProposeTimeout  time.Duration
	Backoff         []time.Duration
	GCInterval      time.Duration
	GCRetention     time.Duration
	StuckAfter      time.Duration
	LeaderPoll      time.Duration // how often to check whether this node became leader

	Registry *metrics.Registry // nil: metrics are kept but not exported
	Logf     func(format string, args ...any)

	now func() time.Time // the leader's clock; replaced in tests

	// Test hooks, called while an order is being run: after the votes are in
	// and before the decision is proposed, and after the decision is applied
	// and before anyone is told. ctx ends when the coordinator is closed.
	beforeDecide func(ctx context.Context, txnID string)
	afterDecided func(ctx context.Context, txnID string)
}

func (c Config) withDefaults() Config {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.PrepareTimeout, DefaultPrepareTimeout)
	def(&c.HoldTTL, DefaultHoldTTL)
	def(&c.DecisionTimeout, DefaultDecisionTimeout)
	def(&c.ProposeTimeout, DefaultProposeTimeout)
	def(&c.GCInterval, DefaultGCInterval)
	def(&c.GCRetention, DefaultGCRetention)
	def(&c.StuckAfter, DefaultStuckAfter)
	def(&c.LeaderPoll, DefaultLeaderPoll)
	if len(c.Backoff) == 0 {
		c.Backoff = DefaultBackoff
	}
	if c.Registry == nil {
		c.Registry = metrics.NewRegistry()
	}
	if c.Logf == nil {
		c.Logf = log.Printf
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c
}

// Coordinator is one node of the coordinator group.
type Coordinator struct {
	cfg  Config
	node *raftnode.Node
	sm   *coordsm.Machine

	Metrics *Metrics

	ctx    context.Context // ends on Close
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	flights    map[string]*flight // orders being run on this node, by txn id
	delivering map[delivery]bool  // decisions being delivered from this node
	started    bool               // Start was called
}

// flight is one order being run on this node. Later SubmitOrder calls for the
// same txn wait on done instead of running it again.
type flight struct {
	done     chan struct{}
	txn      *coordsm.Txn // the decided record, when err is nil
	replayed bool         // the txn was decided before this run began
	err      error
}

type delivery struct {
	txn   string
	shard uint32
}

// New creates the coordinator for one node. Call Register to serve it and
// Start to run its background work; Close stops both.
func New(cfg Config) (*Coordinator, error) {
	switch {
	case cfg.Node == nil || cfg.SM == nil:
		return nil, errors.New("coordinator: Node and SM are required")
	case len(cfg.Shards) == 0:
		return nil, errors.New("coordinator: no shard groups")
	}
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		cfg: cfg, node: cfg.Node, sm: cfg.SM, Metrics: newMetrics(cfg.Registry),
		ctx: ctx, cancel: cancel,
		flights: map[string]*flight{}, delivering: map[delivery]bool{},
	}, nil
}

// Register serves CoordinatorService on gs.
func (c *Coordinator) Register(gs grpc.ServiceRegistrar) {
	seatsafev1.RegisterCoordinatorServiceServer(gs, service{c: c})
}

// Start runs the leader's background work: taking over after an election,
// and garbage collection.
func (c *Coordinator) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.started {
		return
	}
	c.started = true
	c.wg.Add(2)
	go c.watchLeadership()
	go c.collect()
}

// Close stops background work and orders in progress, and waits for them. A
// txn left undecided or undelivered is finished by the next leader.
func (c *Coordinator) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cancel()
	c.wg.Wait()
}

// WriteMetrics appends the gauges to /metrics: orders this node is running,
// and the group's txn records by state.
func (c *Coordinator) WriteMetrics(w io.Writer) {
	c.mu.Lock()
	inflight := len(c.flights)
	c.mu.Unlock()
	total, undecided, unacked := c.sm.Counts()
	fmt.Fprintf(w, "# HELP seatsafe_coordinator_inflight_txns Cross-shard orders this node is running (prepare or decide in progress).\n# TYPE seatsafe_coordinator_inflight_txns gauge\nseatsafe_coordinator_inflight_txns %d\n", inflight)
	fmt.Fprintf(w, "# HELP seatsafe_coordinator_txn_records Txn records in the coordinator state machine: all, without a decision, and decided but missing an ack.\n# TYPE seatsafe_coordinator_txn_records gauge\n")
	fmt.Fprintf(w, "seatsafe_coordinator_txn_records{state=\"all\"} %d\nseatsafe_coordinator_txn_records{state=\"undecided\"} %d\nseatsafe_coordinator_txn_records{state=\"unacked\"} %d\n", total, undecided, unacked)
}

// Applier adapts a coordsm.StateMachine to raftnode.StateMachine by decoding
// each log entry as a CoordCommand. Apply returns a coordsm.Result.
type Applier struct{ SM coordsm.StateMachine }

// Apply implements raftnode.StateMachine.
func (a Applier) Apply(index uint64, data []byte) any {
	var cmd seatsafev1.CoordCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		// Every replica sees the same bytes, so skipping is still deterministic.
		log.Printf("coordinator: entry %d is not a CoordCommand, skipped: %v", index, err)
		return coordsm.Result{}
	}
	return a.SM.Apply(index, &cmd)
}

// Snapshot implements raftnode.StateMachine.
func (a Applier) Snapshot() ([]byte, error) { return a.SM.Snapshot() }

// Restore implements raftnode.StateMachine.
func (a Applier) Restore(data []byte) error { return a.SM.Restore(data) }

var (
	errNotLeader = errors.New("coordinator: not the leader any more")
	errClosed    = errors.New("coordinator: shutting down")
)

// propose stamps cmd with the leader's clock, replicates it, and waits for
// the result of applying it on this node.
func (c *Coordinator) propose(ctx context.Context, cmd *seatsafev1.CoordCommand) (coordsm.Result, error) {
	cmd.NowMs = c.cfg.now().UnixMilli()
	data, err := proto.Marshal(cmd)
	if err != nil {
		return coordsm.Result{}, fmt.Errorf("marshal command: %w", err)
	}
	res, err := c.node.Propose(ctx, data)
	if err != nil {
		return coordsm.Result{}, err
	}
	r, ok := res.(coordsm.Result)
	if !ok {
		return coordsm.Result{}, fmt.Errorf("unexpected apply result %T", res)
	}
	return r, nil
}

// proposeWhileLeader proposes cmd until it is applied, for as long as this
// node is leader and open. Every command proposed this way may be applied
// more than once without harm: decisions (the first wins), acks, and the
// no-op used as a barrier.
func (c *Coordinator) proposeWhileLeader(cmd *seatsafev1.CoordCommand) (coordsm.Result, error) {
	for attempt := 0; ; attempt++ {
		if c.ctx.Err() != nil {
			return coordsm.Result{}, errClosed
		}
		if !c.node.Status().IsLeader() {
			return coordsm.Result{}, errNotLeader
		}
		ctx, cancel := context.WithTimeout(c.ctx, c.cfg.ProposeTimeout)
		res, err := c.propose(ctx, cmd)
		cancel()
		switch {
		case err == nil:
			return res, nil
		case errors.Is(err, raftnode.ErrStopped):
			return coordsm.Result{}, errClosed
		case !transient(err):
			return coordsm.Result{}, err
		}
		c.sleep(c.cfg.Backoff[min(attempt, len(c.cfg.Backoff)-1)])
	}
}

// transient reports whether a failed proposal is worth making again: nothing
// says the command itself is wrong.
func transient(err error) bool {
	return errors.Is(err, raft.ErrProposalDropped) || errors.Is(err, raftnode.ErrLeadershipLost) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// sleep waits d, or less if the coordinator is closed.
func (c *Coordinator) sleep(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-c.ctx.Done():
	}
}

// statusErr turns a failed proposal or run into the gRPC status the caller
// sees. Everything a retry can fix is UNAVAILABLE or DEADLINE_EXCEEDED.
func statusErr(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "timed out; retry with the same txn_id")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	case errors.Is(err, raft.ErrProposalDropped), errors.Is(err, raftnode.ErrLeadershipLost), errors.Is(err, raftnode.ErrStopped),
		errors.Is(err, errNotLeader), errors.Is(err, errClosed):
		return status.Errorf(codes.Unavailable, "%v; retry with the same txn_id", err)
	}
	return status.Errorf(codes.Internal, "coordinator: %v", err)
}

// requireLeader rejects a call on a follower with FAILED_PRECONDITION and the
// leader's address in the seatsafe-leader trailer, or with UNAVAILABLE while
// no leader is known. Same contract as the shard services.
func (c *Coordinator) requireLeader(ctx context.Context) error {
	st := c.node.Status()
	if st.IsLeader() {
		return nil
	}
	if st.Leader == 0 {
		return status.Error(codes.Unavailable, "no leader yet; retry with the same txn_id")
	}
	addr := c.cfg.Addrs[st.Leader]
	_ = grpc.SetTrailer(ctx, metadata.Pairs(groupclient.LeaderTrailer, addr))
	return status.Errorf(codes.FailedPrecondition, "not the leader; leader is %s", addr)
}
