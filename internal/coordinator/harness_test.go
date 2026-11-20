package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/coordsm"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/raftstore"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
	"github.com/quorum-crew/seatsafe/internal/shard"
	"github.com/quorum-crew/seatsafe/internal/transport"
)

const (
	confirmed   = seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED
	unavailable = seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
	aborted     = seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED

	commit = seatsafev1.Decision_DECISION_COMMIT
	abort  = seatsafev1.Decision_DECISION_ABORT

	seatsPerShard = 400
	event         = "evt-1"
)

var (
	raftIDs  = []uint64{1, 2, 3}
	shardIDs = []uint32{1, 2, 3}
	quiet    = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
)

// seat is seat n of shard s: every shard owns one section, sec-<s>.
func seat(s uint32, n int) *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: event, SectionId: fmt.Sprintf("sec-%d", s), SeatId: fmt.Sprintf("S%03d", n)}
}

// on is one participant: seats ns of shard s.
func on(s uint32, ns ...int) *seatsafev1.ShardSeats {
	p := &seatsafev1.ShardSeats{ShardId: s}
	for _, n := range ns {
		p.Seats = append(p.Seats, seat(s, n))
	}
	return p
}

// seedOf is a shard's seed file: one chunk with all its seats.
type seedOf uint32

func (s seedOf) Version() string  { return fmt.Sprintf("seed-%d", uint32(s)) }
func (s seedOf) TotalChunks() int { return 1 }
func (s seedOf) Chunk(int) []*seatsafev1.SeatRef {
	out := make([]*seatsafev1.SeatRef, seatsPerShard)
	for n := range out {
		out[n] = seat(uint32(s), n)
	}
	return out
}

// group is the part every in-process Raft group shares: an in-memory Raft
// network, real gRPC listeners on localhost, and a data directory per node.
type group struct {
	t       testing.TB
	durable bool
	net     *raftnode.Network
	addrs   map[uint64]string
	dirs    map[uint64]string
	lis     map[uint64]net.Listener // bound at creation, until the node's first start
}

func newGroup(t testing.TB, durable bool) *group {
	g := &group{t: t, durable: durable, net: raftnode.NewNetwork(), addrs: map[uint64]string{}, dirs: map[uint64]string{}, lis: map[uint64]net.Listener{}}
	for _, id := range raftIDs {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g.lis[id], g.addrs[id] = lis, lis.Addr().String()
		g.dirs[id] = t.TempDir()
	}
	return g
}

func (g *group) addrList() []string {
	var out []string
	for _, id := range raftIDs {
		out = append(out, g.addrs[id])
	}
	return out
}

// listener returns the node's listener: the one bound at creation the first
// time, a new one on the same address after a restart.
func (g *group) listener(id uint64) net.Listener {
	if lis, first := g.lis[id]; first {
		delete(g.lis, id)
		return lis
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		lis, err := net.Listen("tcp", g.addrs[id])
		if err == nil {
			return lis
		}
		if !errors.Is(err, syscall.EADDRINUSE) || time.Now().After(deadline) {
			g.t.Fatalf("restart: cannot rebind %s: %v", g.addrs[id], err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// raft starts the node's Raft member, on its store if the group is durable.
// The returned function closes the store.
func (g *group) raft(id uint64, sm raftnode.StateMachine) (*raftnode.Node, func()) {
	cfg := raftnode.Config{ID: id, Peers: raftIDs, SM: sm, Transport: g.net, TickInterval: 20 * time.Millisecond, Logger: quiet}
	closeStore := func() {}
	if g.durable {
		store, err := raftstore.Open(g.dirs[id])
		if err != nil {
			g.t.Fatal(err)
		}
		cfg.Store = store
		closeStore = func() { store.Close() }
	}
	n, err := raftnode.Start(cfg)
	if err != nil {
		g.t.Fatal(err)
	}
	g.net.Add(id, n)
	return n, closeStore
}

type shardNode struct {
	node       *raftnode.Node
	sm         *seatsm.Machine
	closeStore func()
	gs         *grpc.Server
	stop       context.CancelFunc
}

type coordNode struct {
	node       *raftnode.Node
	sm         *coordsm.Machine
	closeStore func()
	gs         *grpc.Server
	coord      *Coordinator
	shards     map[uint32]*groupclient.Group
}

type hook func(ctx context.Context, txnID string)

// options tune the coordinators of a cluster.
type options struct {
	gcInterval, gcRetention, stuckAfter time.Duration
	// prepareTimeout defaults to 2 s here, not the real 300 ms: on a loaded
	// test machine a healthy shard can take longer than 300 ms to vote, and
	// only the tests about that deadline want orders aborted for it.
	prepareTimeout time.Duration
	// durable keeps every Raft log on disk, which a node needs to be
	// restarted. Tests that restart nobody keep the logs in memory: with
	// twelve nodes fsyncing to one disk, a slow fsync on a leader is enough
	// to start an election, and most tests here are about something else.
	durable bool
}

// cluster is a real coordinator group and three real shards in one process:
// Raft over in-memory networks, every service over gRPC.
type cluster struct {
	t    testing.TB
	opts options

	mu         sync.Mutex
	shardG     map[uint32]*group
	shardN     map[uint32]map[uint64]*shardNode
	coordG     *group
	coordN     map[uint64]*coordNode
	allMetrics []*Metrics // of every coordinator this cluster has run, summed by value()
	logs       []string

	client *groupclient.Group            // the router's view of the coordinator group
	shards map[uint32]*groupclient.Group // the test's own clients for the shards

	before, after atomic.Pointer[hook]
}

func startCluster(t testing.TB, opts options) *cluster {
	t.Helper()
	if opts.prepareTimeout == 0 {
		opts.prepareTimeout = 2 * time.Second
	}
	c := &cluster{t: t, opts: opts, shardG: map[uint32]*group{}, shardN: map[uint32]map[uint64]*shardNode{},
		coordN: map[uint64]*coordNode{}, shards: map[uint32]*groupclient.Group{}}
	gm := groupclient.NewMetrics(metrics.NewRegistry())
	for _, s := range shardIDs {
		c.shardG[s] = newGroup(t, opts.durable)
		c.shardN[s] = map[uint64]*shardNode{}
		for _, id := range raftIDs {
			c.startShardNode(s, id)
		}
		cl, err := groupclient.New(fmt.Sprintf("shard-%d", s), c.shardG[s].addrList(), groupclient.Config{}, gm)
		if err != nil {
			t.Fatal(err)
		}
		c.shards[s] = cl
	}
	c.coordG = newGroup(t, opts.durable)
	for _, id := range raftIDs {
		c.startCoordNode(id)
	}
	var err error
	// As the router does: 1 s per attempt for the coordinator group.
	if c.client, err = groupclient.New("coordinator", c.coordG.addrList(), groupclient.Config{AttemptTimeout: time.Second}, gm); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	c.waitSeeded()
	c.coordLeader()
	return c
}

func (c *cluster) close() {
	c.mu.Lock()
	var coords []uint64
	for id := range c.coordN {
		coords = append(coords, id)
	}
	type sn struct {
		s  uint32
		id uint64
	}
	var storage []sn
	for s, nodes := range c.shardN {
		for id := range nodes {
			storage = append(storage, sn{s, id})
		}
	}
	c.mu.Unlock()
	for _, id := range coords {
		c.killCoord(id)
	}
	for _, n := range storage {
		c.killShardNode(n.s, n.id)
	}
	_ = c.client.Close()
	for _, cl := range c.shards {
		_ = cl.Close()
	}
	c.coordG.net.Close()
	for _, g := range c.shardG {
		g.net.Close()
	}
}

func (c *cluster) startShardNode(s uint32, id uint64) {
	g := c.shardG[s]
	if _, first := g.lis[id]; !first && !g.durable {
		c.t.Fatal("restarting a node needs options.durable")
	}
	sm := seatsm.New()
	node, closeStore := g.raft(id, shard.Applier{SM: sm})
	gs := grpc.NewServer()
	shard.NewService(node, sm, g.addrs).Register(gs)
	transport.NewStatusServer(fmt.Sprintf("shard%d-node%d", s, id), s, node, g.addrs).Register(gs)
	go gs.Serve(g.listener(id))
	ctx, cancel := context.WithCancel(context.Background())
	go (&shard.Seeder{Node: node, SM: sm, Interval: 10 * time.Millisecond,
		Load: func() (shard.SeedSource, error) { return seedOf(s), nil }, Logf: func(string, ...any) {}}).Run(ctx)
	c.mu.Lock()
	c.shardN[s][id] = &shardNode{node: node, sm: sm, closeStore: closeStore, gs: gs, stop: cancel}
	c.mu.Unlock()
}

func (c *cluster) killShardNode(s uint32, id uint64) {
	c.mu.Lock()
	n := c.shardN[s][id]
	delete(c.shardN[s], id)
	c.mu.Unlock()
	if n == nil {
		return
	}
	n.stop()
	n.gs.Stop()
	n.node.Stop()
	n.closeStore()
}

func (c *cluster) startCoordNode(id uint64) {
	g := c.coordG
	if _, first := g.lis[id]; !first && !g.durable {
		c.t.Fatal("restarting a node needs options.durable")
	}
	sm := coordsm.New()
	node, closeStore := g.raft(id, Applier{SM: sm})
	shards := map[uint32]*groupclient.Group{}
	gm := groupclient.NewMetrics(metrics.NewRegistry())
	for _, s := range shardIDs {
		cl, err := groupclient.New(fmt.Sprintf("shard-%d", s), c.shardG[s].addrList(), groupclient.Config{}, gm)
		if err != nil {
			c.t.Fatal(err)
		}
		shards[s] = cl
	}
	coord, err := New(Config{
		Node: node, SM: sm, Addrs: g.addrs, Shards: shards,
		PrepareTimeout: c.opts.prepareTimeout,
		GCInterval:     c.opts.gcInterval, GCRetention: c.opts.gcRetention, StuckAfter: c.opts.stuckAfter,
		LeaderPoll: 5 * time.Millisecond,
		Logf: func(format string, args ...any) {
			c.mu.Lock()
			c.logs = append(c.logs, fmt.Sprintf(format, args...))
			c.mu.Unlock()
		},
		beforeDecide: func(ctx context.Context, txn string) {
			if h := c.before.Load(); h != nil {
				(*h)(ctx, txn)
			}
		},
		afterDecided: func(ctx context.Context, txn string) {
			if h := c.after.Load(); h != nil {
				(*h)(ctx, txn)
			}
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	gs := grpc.NewServer()
	coord.Register(gs)
	transport.NewStatusServer(fmt.Sprintf("coord-%d", id), 0, node, g.addrs).Register(gs)
	go gs.Serve(g.listener(id))
	coord.Start()
	c.mu.Lock()
	c.coordN[id] = &coordNode{node: node, sm: sm, closeStore: closeStore, gs: gs, coord: coord, shards: shards}
	c.allMetrics = append(c.allMetrics, coord.Metrics)
	c.mu.Unlock()
}

// killCoord stops a coordinator node the way a crash would look to everyone
// else: its background work stops first, so nothing it had in progress is
// finished on the way down.
func (c *cluster) killCoord(id uint64) {
	c.mu.Lock()
	n := c.coordN[id]
	delete(c.coordN, id)
	c.mu.Unlock()
	if n == nil {
		return
	}
	n.coord.Close()
	n.gs.Stop()
	n.node.Stop()
	n.closeStore()
	for _, cl := range n.shards {
		_ = cl.Close()
	}
}

func (c *cluster) waitSeeded() {
	c.t.Helper()
	c.eventually(15*time.Second, "every shard node seeded", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, nodes := range c.shardN {
			for _, n := range nodes {
				if !shard.Seeded(n.sm) {
					return false
				}
			}
		}
		return true
	})
}

func (c *cluster) eventually(within time.Duration, what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("not within %v: %s\ncoordinator log:\n%s", within, what, c.logText())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *cluster) logText() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := ""
	for _, l := range c.logs {
		out += "  " + l + "\n"
	}
	return out
}

// coordLeader waits for the coordinator group's leader: the running node
// that leads the highest term, once a majority of running nodes follows it.
func (c *cluster) coordLeader() (uint64, *coordNode) {
	c.t.Helper()
	var id uint64
	var n *coordNode
	c.eventually(15*time.Second, "a coordinator leader", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		id, n = 0, nil
		var term uint64
		for i, cn := range c.coordN {
			if st := cn.node.Status(); st.IsLeader() && st.Term > term {
				id, n, term = i, cn, st.Term
			}
		}
		if n == nil {
			return false
		}
		for _, cn := range c.coordN {
			if st := cn.node.Status(); st.Leader != id || st.Term != term {
				return false
			}
		}
		return true
	})
	return id, n
}

func (c *cluster) shardLeader(s uint32) (uint64, *shardNode) {
	c.t.Helper()
	var id uint64
	var n *shardNode
	c.eventually(15*time.Second, fmt.Sprintf("a leader for shard %d", s), func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		id, n = 0, nil
		var term uint64
		for i, sn := range c.shardN[s] {
			if st := sn.node.Status(); st.IsLeader() && st.Term > term {
				id, n, term = i, sn, st.Term
			}
		}
		return n != nil
	})
	return id, n
}

// await waits for a hook to report a txn, and fails the test instead of
// hanging if it never does.
func (c *cluster) await(reached <-chan string, what string) string {
	c.t.Helper()
	select {
	case txn := <-reached:
		return txn
	case <-time.After(30 * time.Second):
		c.t.Fatalf("not within 30s: %s\ncoordinator log:\n%s", what, c.logText())
		return ""
	}
}

// running returns the coordinator node that is running txn.
func (c *cluster) running(txn string) (uint64, *coordNode) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.coordN {
		if n.coord.inFlight(txn) {
			return id, n
		}
	}
	c.t.Fatalf("no coordinator node is running %s", txn)
	return 0, nil
}

// submit sends SubmitOrder the way the router does, following the leader,
// until it gets an answer or ctx ends.
func (c *cluster) submit(ctx context.Context, txn string, parts ...*seatsafev1.ShardSeats) (*seatsafev1.BookSeatsResponse, error) {
	req := &seatsafev1.SubmitOrderRequest{TxnId: txn, UserId: "user-" + txn, Participants: parts}
	var resp *seatsafev1.BookSeatsResponse
	err := c.client.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		resp, err = seatsafev1.NewCoordinatorServiceClient(cc).SubmitOrder(ctx, req, opts...)
		return err
	})
	return resp, err
}

func (c *cluster) mustSubmit(txn string, parts ...*seatsafev1.ShardSeats) *seatsafev1.BookSeatsResponse {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := c.submit(ctx, txn, parts...)
	if err != nil {
		c.t.Fatalf("SubmitOrder(%s): %v\ncoordinator log:\n%s", txn, err, c.logText())
	}
	return resp
}

func (c *cluster) getDecision(txn string) seatsafev1.Decision {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var resp *seatsafev1.GetDecisionResponse
	err := c.client.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		resp, err = seatsafev1.NewCoordinatorServiceClient(cc).GetDecision(ctx, &seatsafev1.GetDecisionRequest{TxnId: txn}, opts...)
		return err
	})
	if err != nil {
		c.t.Fatalf("GetDecision(%s): %v", txn, err)
	}
	return resp.GetDecision()
}

// bookDirect books seats of one shard without the coordinator, as a
// single-shard order would.
func (c *cluster) bookDirect(req string, s uint32, ns ...int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var resp *seatsafev1.BookSeatsResponse
	err := c.shards[s].Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		resp, err = seatsafev1.NewShardServiceClient(cc).Book(ctx, &seatsafev1.ShardBookRequest{RequestId: req, UserId: "direct", Seats: on(s, ns...).Seats}, opts...)
		return err
	})
	if err != nil || resp.GetOutcome() != confirmed {
		c.t.Fatalf("direct booking %s on shard %d: %v, %v", req, s, resp, err)
	}
}

// taken returns the seat numbers of shard s that are not available (held or
// booked) on its leader, sorted.
func (c *cluster) taken(s uint32) []int {
	c.t.Helper()
	_, n := c.shardLeader(s)
	free := map[string]bool{}
	for _, sec := range n.sm.ListAvailable(event, fmt.Sprintf("sec-%d", s)).GetSections() {
		for _, id := range sec.GetAvailableSeatIds() {
			free[id] = true
		}
	}
	var out []int
	for i := range seatsPerShard {
		if !free[seat(s, i).GetSeatId()] {
			out = append(out, i)
		}
	}
	return out
}

// holds returns the txns still PREPARED on shard s's leader.
func (c *cluster) holds(s uint32) []string {
	c.t.Helper()
	_, n := c.shardLeader(s)
	return n.sm.ExpiredHolds(math.MaxInt64)
}

// settled waits until the coordinator has nothing left to do (no undecided
// txn, none missing an ack) and no shard holds a seat, then checks every
// state machine's invariants.
func (c *cluster) settled() {
	c.t.Helper()
	c.eventually(30*time.Second, "coordinator settled and all holds released", func() bool {
		c.mu.Lock()
		var leader *coordNode
		var term uint64
		for _, cn := range c.coordN {
			if st := cn.node.Status(); st.IsLeader() && st.Term >= term {
				leader, term = cn, st.Term
			}
		}
		c.mu.Unlock()
		if leader == nil {
			return false
		}
		if _, undecided, unacked := leader.sm.Counts(); undecided != 0 || unacked != 0 {
			return false
		}
		for _, s := range shardIDs {
			if len(c.holds(s)) != 0 {
				return false
			}
		}
		return true
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, cn := range c.coordN {
		if err := cn.sm.CheckInvariants(); err != nil {
			c.t.Fatalf("coordinator node %d: %v", id, err)
		}
	}
	for s, nodes := range c.shardN {
		for id, sn := range nodes {
			if err := sn.sm.CheckInvariants(); err != nil {
				c.t.Fatalf("shard %d node %d: %v", s, id, err)
			}
		}
	}
}

// wantTaken checks which seats of each shard are unavailable.
func (c *cluster) wantTaken(want map[uint32][]int) {
	c.t.Helper()
	for _, s := range shardIDs {
		got, w := c.taken(s), append([]int(nil), want[s]...)
		sort.Ints(w)
		if fmt.Sprint(got) != fmt.Sprint(w) {
			c.t.Fatalf("shard %d: seats taken %v, want %v", s, got, w)
		}
	}
}

// value sums a counter over every coordinator this cluster has run.
func (c *cluster) value(pick func(*Metrics) *metrics.CounterVec, labels ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var sum float64
	for _, m := range c.allMetrics {
		sum += pick(m).Value(labels...)
	}
	return sum
}

func (c *cluster) violations() float64 {
	var sum float64
	for _, s := range shardIDs {
		for _, d := range []string{"commit", "abort"} {
			sum += c.value(func(m *Metrics) *metrics.CounterVec { return m.Violations }, shardLabel(s), d)
		}
	}
	return sum
}

func wantResp(t testing.TB, resp *seatsafev1.BookSeatsResponse, txn string, outcome seatsafev1.BookingOutcome, replayed bool, conflicts ...*seatsafev1.SeatRef) {
	t.Helper()
	if resp.GetOrderId() != txn || resp.GetOutcome() != outcome || resp.GetReplayed() != replayed || !resp.GetCrossShard() {
		t.Fatalf("response %v, want order %s, outcome %v, replayed %v, cross_shard", resp, txn, outcome, replayed)
	}
	if len(resp.GetUnavailableSeats()) != len(conflicts) {
		t.Fatalf("unavailable seats %v, want %v", resp.GetUnavailableSeats(), conflicts)
	}
	for i, s := range conflicts {
		if got := resp.GetUnavailableSeats()[i]; got.GetSectionId() != s.GetSectionId() || got.GetSeatId() != s.GetSeatId() || got.GetEventId() != s.GetEventId() {
			t.Fatalf("unavailable seats %v, want %v", resp.GetUnavailableSeats(), conflicts)
		}
	}
}

func wantCode(t testing.TB, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("error %v, want code %v", err, code)
	}
}
