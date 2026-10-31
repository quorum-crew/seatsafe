package shard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/raftstore"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
)

// fakeSeed is a seed file whose chunk i is section sec-<i> with 10 seats.
type fakeSeed struct {
	version string
	chunks  int
}

func (f fakeSeed) Version() string  { return f.version }
func (f fakeSeed) TotalChunks() int { return f.chunks }
func (f fakeSeed) Chunk(i int) []*seatsafev1.SeatRef {
	var seats []*seatsafev1.SeatRef
	for s := range 10 {
		seats = append(seats, seat(i, s))
	}
	return seats
}

func seat(section, n int) *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: "evt-001", SectionId: fmt.Sprintf("sec-%03d", section), SeatId: fmt.Sprintf("R01-S%03d", n)}
}

type storageNode struct {
	node  *raftnode.Node
	sm    *seatsm.Machine
	store *raftstore.Pebble
	gs    *grpc.Server
	addr  string
	stop  context.CancelFunc
}

// shard runs a 3-node shard: Raft over the in-memory network, ShardService and
// ParticipantService over real gRPC on localhost, and a seeder on every node.
type shard struct {
	t       *testing.T
	src     SeedSource
	durable bool
	net     *raftnode.Network
	addrs   map[uint64]string
	dirs    map[uint64]string
	nodes   map[uint64]*storageNode
	lis     map[uint64]net.Listener // bound in startShard, until the node's first start
}

var ids = []uint64{1, 2, 3}

func startShard(t *testing.T, src SeedSource, durable bool) *shard {
	t.Helper()
	c := &shard{t: t, src: src, durable: durable, net: raftnode.NewNetwork(),
		addrs: map[uint64]string{}, dirs: map[uint64]string{}, nodes: map[uint64]*storageNode{}, lis: map[uint64]net.Listener{}}
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.lis[id], c.addrs[id] = lis, lis.Addr().String() // stays open, so the port stays ours
		c.dirs[id] = t.TempDir()
	}
	for _, id := range ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
		c.net.Close()
	})
	return c
}

var quiet = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

func (c *shard) start(id uint64) {
	c.t.Helper()
	sm := seatsm.New()
	cfg := raftnode.Config{ID: id, Peers: ids, SM: Applier{SM: sm}, Transport: c.net,
		TickInterval: 10 * time.Millisecond, Logger: quiet}
	var store *raftstore.Pebble
	if c.durable {
		var err error
		if store, err = raftstore.Open(c.dirs[id]); err != nil {
			c.t.Fatal(err)
		}
		cfg.Store = store
	}
	n, err := raftnode.Start(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.Add(id, n)
	lis, first := c.lis[id]
	if first {
		delete(c.lis, id)
	} else {
		lis = c.rebind(c.addrs[id])
	}
	gs := grpc.NewServer()
	NewService(n, sm, c.addrs).Register(gs)
	go gs.Serve(lis)
	ctx, cancel := context.WithCancel(context.Background())
	src := c.src
	go (&Seeder{Node: n, SM: sm, Interval: 10 * time.Millisecond,
		Load: func() (SeedSource, error) { return src, nil }, Logf: func(string, ...any) {}}).Run(ctx)
	c.nodes[id] = &storageNode{node: n, sm: sm, store: store, gs: gs, addr: c.addrs[id], stop: cancel}
}

// rebind listens on addr again when a node restarts. Peers know the address,
// so it must stay the same, but something else on the machine may hold the
// freed port for a moment.
func (c *shard) rebind(addr string) net.Listener {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis
		}
		if !errors.Is(err, syscall.EADDRINUSE) || time.Now().After(deadline) {
			c.t.Fatalf("restart: cannot rebind %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *shard) stop(id uint64) {
	n := c.nodes[id]
	n.stop()
	n.gs.Stop()
	n.node.Stop()
	if n.store != nil {
		n.store.Close()
	}
	delete(c.nodes, id)
}

func (c *shard) waitSeeded() {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, n := range c.nodes {
			done = done && Seeded(n.sm)
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("shard not seeded on every node within 10s")
}

func (c *shard) leader() (uint64, *storageNode) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.node.Status().IsLeader() {
				return id, n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0, nil
}

// waitIdentical waits until every replica's seatsm snapshot is byte-identical
// (seatsm snapshots are canonical) and returns it.
func (c *shard) waitIdentical() []byte {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var ref []byte
		same := true
		for _, n := range c.nodes {
			b, err := n.sm.Snapshot()
			if err != nil {
				c.t.Fatal(err)
			}
			if ref != nil && !bytes.Equal(b, ref) {
				same = false
			}
			ref = b
		}
		if same {
			return ref
		}
		if time.Now().After(deadline) {
			c.t.Fatal("replicas did not converge to identical state within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func conn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func book(t *testing.T, addr, req, user string, seats ...*seatsafev1.SeatRef) *seatsafev1.BookSeatsResponse {
	t.Helper()
	resp, err := seatsafev1.NewShardServiceClient(conn(t, addr)).Book(ctx5(t),
		&seatsafev1.ShardBookRequest{RequestId: req, UserId: user, Seats: seats})
	if err != nil {
		t.Fatalf("book %s: %v", req, err)
	}
	return resp
}

func TestSeedsEveryReplica(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 5}, false)
	c.waitSeeded()
	c.waitIdentical()
	for id, n := range c.nodes {
		resp := n.sm.ListAvailable("evt-001", "")
		free := 0
		for _, s := range resp.Sections {
			free += len(s.AvailableSeatIds)
		}
		if len(resp.Sections) != 5 || free != 50 {
			t.Errorf("node %d: %d sections, %d free seats; want 5 and 50", id, len(resp.Sections), free)
		}
	}
}

func TestBookingThroughRaft(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 2}, false)
	c.waitSeeded()
	_, lead := c.leader()
	a := seat(0, 1)

	if r := book(t, lead.addr, "req-1", "alice", a); r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED {
		t.Fatalf("first booking: %v", r)
	}
	r := book(t, lead.addr, "req-2", "bob", a)
	if r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE || len(r.UnavailableSeats) != 1 {
		t.Fatalf("second user on the same seat: %v", r)
	}
	if r := book(t, lead.addr, "req-1", "alice", a); !r.Replayed || r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED {
		t.Fatalf("retry of req-1: %v, want a replayed CONFIRMED", r)
	}
	c.waitIdentical() // every replica made the same decisions
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	leadID, lead := c.leader()
	follower := c.nodes[leadID%3+1]

	var trailer metadata.MD
	_, err := seatsafev1.NewShardServiceClient(conn(t, follower.addr)).Book(ctx5(t),
		&seatsafev1.ShardBookRequest{RequestId: "req-1", Seats: []*seatsafev1.SeatRef{seat(0, 0)}}, grpc.Trailer(&trailer))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower returned %v, want FailedPrecondition", err)
	}
	if got := trailer.Get(LeaderTrailer); len(got) != 1 || got[0] != lead.addr {
		t.Fatalf("leader trailer = %v, want [%s]", got, lead.addr)
	}
}

func TestValidatesInput(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	_, lead := c.leader()
	cc := conn(t, lead.addr)
	if _, err := seatsafev1.NewShardServiceClient(cc).Book(ctx5(t), &seatsafev1.ShardBookRequest{RequestId: "r"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Book with no seats: %v", err)
	}
	if _, err := seatsafev1.NewParticipantServiceClient(cc).Prepare(ctx5(t), &seatsafev1.PrepareRequest{Seats: []*seatsafev1.SeatRef{seat(0, 0)}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Prepare with no txn_id: %v", err)
	}
	if _, err := seatsafev1.NewParticipantServiceClient(cc).Commit(ctx5(t), &seatsafev1.DecisionRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Commit with no txn_id: %v", err)
	}
}

// The participant side of 2PC, end to end through Raft.
func TestParticipantTwoPhaseCommit(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	_, lead := c.leader()
	p := seatsafev1.NewParticipantServiceClient(conn(t, lead.addr))
	a, b := seat(0, 1), seat(0, 2)

	vote, err := p.Prepare(ctx5(t), &seatsafev1.PrepareRequest{TxnId: "t1", UserId: "alice", Seats: []*seatsafev1.SeatRef{a, b}})
	if err != nil || vote.Vote != seatsafev1.Vote_VOTE_YES {
		t.Fatalf("prepare t1: %v %v", vote, err)
	}
	// Held seats can't be booked or prepared by anyone else (no-wait).
	if r := book(t, lead.addr, "req-x", "bob", a); r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE {
		t.Fatalf("booking a held seat: %v", r)
	}
	if v, _ := p.Prepare(ctx5(t), &seatsafev1.PrepareRequest{TxnId: "t2", Seats: []*seatsafev1.SeatRef{b}}); v.GetVote() != seatsafev1.Vote_VOTE_NO {
		t.Fatalf("second prepare on a held seat voted %v", v.GetVote())
	}
	if _, err := p.Commit(ctx5(t), &seatsafev1.DecisionRequest{TxnId: "t1"}); err != nil {
		t.Fatalf("commit t1: %v", err)
	}
	if _, err := p.Commit(ctx5(t), &seatsafev1.DecisionRequest{TxnId: "t1"}); err != nil {
		t.Fatalf("repeated commit should be a no-op: %v", err)
	}
	if _, err := p.Abort(ctx5(t), &seatsafev1.DecisionRequest{TxnId: "t1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abort after commit: %v, want FailedPrecondition", err)
	}
	// Abort of a txn this shard never saw leaves a tombstone: a late Prepare votes NO.
	if _, err := p.Abort(ctx5(t), &seatsafev1.DecisionRequest{TxnId: "t3"}); err != nil {
		t.Fatalf("abort unknown t3: %v", err)
	}
	if v, _ := p.Prepare(ctx5(t), &seatsafev1.PrepareRequest{TxnId: "t3", Seats: []*seatsafev1.SeatRef{seat(0, 5)}}); v.GetVote() != seatsafev1.Vote_VOTE_NO {
		t.Fatalf("late prepare after abort voted %v", v.GetVote())
	}
	c.waitIdentical()
	if err := lead.sm.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

// After every node restarts, seat state is rebuilt from the Pebble logs alone.
func TestRestartRebuildsSeatState(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 3}, true)
	c.waitSeeded()
	_, lead := c.leader()
	for i := range 10 {
		book(t, lead.addr, fmt.Sprintf("req-%d", i), "alice", seat(i%3, i))
	}
	before := c.waitIdentical()

	for _, id := range ids {
		c.stop(id)
	}
	for _, id := range ids {
		c.start(id) // fresh, empty seatsm on every node
	}
	c.waitSeeded()
	if after := c.waitIdentical(); !bytes.Equal(after, before) {
		t.Fatal("seat state after a full restart differs from before")
	}
	// And the rebuilt shard still remembers req-0 (idempotency survives restarts).
	_, lead = c.leader()
	if r := book(t, lead.addr, "req-0", "alice", seat(0, 0)); !r.Replayed {
		t.Fatalf("retry after restart: %v, want replayed", r)
	}
}

func TestSeederRefusesDifferentSeed(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 2}, false)
	c.waitSeeded()
	leadID, _ := c.leader()
	n := c.nodes[leadID]
	// A leader with a different seed file proposes nothing once seeded, and a
	// stray SeedOp with another version is refused by seatsm.
	s := &Seeder{Node: n.node, SM: n.sm, Load: func() (SeedSource, error) { return fakeSeed{version: "v2", chunks: 2}, nil }}
	if err := s.proposeMissing(context.Background(), fakeSeed{version: "v2", chunks: 2}); err != nil {
		t.Fatalf("seeded shard: proposeMissing = %v, want nil (nothing missing)", err)
	}
	version, _, _, done := n.sm.SeedStatus()
	if version != "v1" || !done {
		t.Fatalf("seed status = %s done=%v, want v1 done", version, done)
	}
}

// An RPC with no client deadline must not hang when the leader it reached is
// deposed before the write commits (#10): it gets UNAVAILABLE promptly, and a
// retry with the same request_id on the new leader is safe.
func TestBookFailsFastWhenLeaderIsDeposed(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	oldID, old := c.leader()
	client := seatsafev1.NewShardServiceClient(conn(t, old.addr))

	c.net.Isolate(oldID) // its Raft traffic is cut; gRPC to it still works
	errc := make(chan error, 1)
	go func() {
		_, err := client.Book(context.Background(), // no deadline
			&seatsafev1.ShardBookRequest{RequestId: "req-1", UserId: "alice", Seats: []*seatsafev1.SeatRef{seat(0, 1)}})
		errc <- err
	}()
	select {
	case err := <-errc:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("Book on a deposed leader returned %v, want UNAVAILABLE", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Book with no deadline is still hanging after the leader was deposed")
	}

	c.net.Heal()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, lead := c.leader()
		resp, err := seatsafev1.NewShardServiceClient(conn(t, lead.addr)).Book(ctx5(t),
			&seatsafev1.ShardBookRequest{RequestId: "req-1", UserId: "alice", Seats: []*seatsafev1.SeatRef{seat(0, 1)}})
		if err == nil {
			if resp.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED {
				t.Fatalf("retry of req-1: %v", resp)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry of req-1 kept failing: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.waitIdentical()
}

func TestTransientSeedErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{raftnode.ErrLeadershipLost, true},
		{raft.ErrProposalDropped, true},
		{context.DeadlineExceeded, true},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), true},
		{context.Canceled, false}, // shutting down: Run exits on ctx anyway
		{raftnode.ErrStopped, false},
		{fmt.Errorf("disk full"), false},
	} {
		if got := transient(tc.err); got != tc.want {
			t.Errorf("transient(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// A seed proposal that times out (#16) must be retried at the normal
// interval, not after Backoff. Every proposal here times out (1 ns), but the
// entries still commit in the background, so a seeder that keeps retrying
// finishes; one that backs off for an hour does not.
func TestSeederRetriesTimedOutChunksWithoutBackoff(t *testing.T) {
	nw := raftnode.NewNetwork()
	sm := seatsm.New()
	n, err := raftnode.Start(raftnode.Config{ID: 1, Peers: []uint64{1}, SM: Applier{SM: sm}, Transport: nw,
		TickInterval: 10 * time.Millisecond, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	nw.Add(1, n)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); n.Stop(); nw.Close() })

	src := fakeSeed{version: "v1", chunks: 3}
	go (&Seeder{Node: n, SM: sm, Load: func() (SeedSource, error) { return src, nil }, Logf: func(string, ...any) {},
		Interval: 5 * time.Millisecond, Backoff: time.Hour, ChunkTimeout: time.Nanosecond}).Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for !Seeded(sm) {
		if time.Now().After(deadline) {
			_, applied, total, _ := sm.SeedStatus()
			t.Fatalf("not seeded after 5s (%d/%d chunks): the seeder backed off after a timed-out proposal", applied, total)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// collectAt runs one GC round as if the leader's clock read now+ahead.
func collectAt(t *testing.T, n *raftnode.Node, ahead time.Duration) {
	t.Helper()
	c := &Collector{Node: n, now: func() time.Time { return time.Now().Add(ahead) }}
	if err := c.collect(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

// Records younger than the retention survive a GC round; older ones are
// dropped on every replica, so a very late retry is no longer recognized (#12).
func TestCollectorDropsRecordsOlderThanRetention(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	_, lead := c.leader()
	a := seat(0, 1)
	book(t, lead.addr, "req-old", "alice", a)

	collectAt(t, lead.node, 5*time.Minute) // within the 10 min retention
	if r := book(t, lead.addr, "req-old", "alice", a); !r.Replayed {
		t.Fatalf("5 min later the retry was not replayed: %v", r)
	}

	collectAt(t, lead.node, 11*time.Minute) // past the retention
	r := book(t, lead.addr, "req-old", "alice", a)
	if r.Replayed || r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE {
		t.Fatalf("11 min later req-old should be forgotten (seat still booked, no replay): %v", r)
	}
	c.waitIdentical() // every replica dropped the same records
	if err := lead.sm.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

// Only the leader proposes GcOps: a collector on a follower changes nothing,
// while the leader's Run loop collects on its interval.
func TestCollectorRunsOnlyOnLeader(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	leadID, lead := c.leader()
	follower := c.nodes[leadID%3+1]
	a := seat(0, 2)
	book(t, lead.addr, "req-1", "alice", a)

	run := func(n *raftnode.Node) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		(&Collector{Node: n, Logf: t.Logf, Interval: 10 * time.Millisecond,
			now: func() time.Time { return time.Now().Add(time.Hour) }}).Run(ctx)
	}

	run(follower.node)
	if r := book(t, lead.addr, "req-1", "alice", a); !r.Replayed {
		t.Fatalf("a follower's collector dropped records: %v", r)
	}

	run(lead.node)
	if r := book(t, lead.addr, "req-1", "alice", a); r.Replayed {
		t.Fatalf("the leader's collector did not drop req-1: %v", r)
	}
}
