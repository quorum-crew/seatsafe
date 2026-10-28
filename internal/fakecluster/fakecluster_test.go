package fakecluster

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/seed"
)

func newTestCluster(t *testing.T) *Cluster {
	t.Helper()
	sm, seeds, err := SmallDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return time.UnixMilli(1_000_000) }
	c, err := NewBufconn(sm, seeds, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// dial connects straight to one node, bypassing leader discovery.
func dial(t *testing.T, c *Cluster, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+addr, append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, c.DialOptions()...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return c
}

var (
	s1 = &seatsafev1.SeatRef{EventId: "evt-flash", SectionId: "sec-101", SeatId: "R01-S001"} // shard 1
	s2 = &seatsafev1.SeatRef{EventId: "evt-flash", SectionId: "sec-102", SeatId: "R01-S001"} // shard 2
)

func order(txn string) *seatsafev1.SubmitOrderRequest {
	return &seatsafev1.SubmitOrderRequest{TxnId: txn, UserId: "u", Participants: []*seatsafev1.ShardSeats{
		{ShardId: 1, Seats: []*seatsafev1.SeatRef{s1}}, {ShardId: 2, Seats: []*seatsafev1.SeatRef{s2}},
	}}
}

func TestStatusAndLeadership(t *testing.T) {
	c := newTestCluster(t)
	g := c.Shards[1]
	node := func(i int) seatsafev1.NodeServiceClient {
		return seatsafev1.NewNodeServiceClient(dial(t, c, g.Nodes[i].Addr))
	}

	st, err := node(1).Status(ctx(t), &seatsafev1.StatusRequest{})
	if err != nil || st.Role != seatsafev1.RaftRole_RAFT_ROLE_FOLLOWER || st.LeaderAddr != "shard1-node1:7000" || st.NodeId != "shard1-node2" || st.AppliedIndex == 0 {
		t.Fatalf("follower status %v, %v", st, err)
	}
	g.SetNoLeader()
	st, _ = node(0).Status(ctx(t), &seatsafev1.StatusRequest{})
	if st.Role != seatsafev1.RaftRole_RAFT_ROLE_CANDIDATE || st.LeaderAddr != "" || st.Term != 2 || g.LeaderAddr() != "" || g.LeaderIndex() != -1 {
		t.Fatalf("election status %v", st)
	}
	shard := seatsafev1.NewShardServiceClient(dial(t, c, g.Nodes[0].Addr))
	if _, err := shard.Book(ctx(t), &seatsafev1.ShardBookRequest{RequestId: "r", Seats: []*seatsafev1.SeatRef{s1}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("no leader: %v", err)
	}
	g.SetLeader(2)
	var md metadata.MD
	if _, err := shard.Book(ctx(t), &seatsafev1.ShardBookRequest{RequestId: "r", Seats: []*seatsafev1.SeatRef{s1}}, grpc.Trailer(&md)); status.Code(err) != codes.FailedPrecondition || md.Get(groupclient.LeaderTrailer)[0] != "shard1-node3:7000" {
		t.Fatalf("follower: %v, trailer %v", err, md)
	}
	if st, _ := node(2).Status(ctx(t), &seatsafev1.StatusRequest{}); st.Role != seatsafev1.RaftRole_RAFT_ROLE_LEADER || st.LeaderId != 3 {
		t.Fatalf("leader status %v", st)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("SetLeader(9) did not panic")
		}
	}()
	g.SetLeader(9)
}

func TestParticipantPaths(t *testing.T) {
	c := newTestCluster(t)
	p := seatsafev1.NewParticipantServiceClient(dial(t, c, c.Shards[1].Nodes[0].Addr))
	v, err := p.Prepare(ctx(t), &seatsafev1.PrepareRequest{TxnId: "t1", Seats: []*seatsafev1.SeatRef{s1}})
	if err != nil || v.Vote != seatsafev1.Vote_VOTE_YES {
		t.Fatalf("prepare %v %v", v, err)
	}
	// No hold_ttl_ms: the default TTL applies, measured on the injected clock.
	if got := c.Shards[1].Machine.ExpiredHolds(1_000_000 + DefaultHoldTTL.Milliseconds()); len(got) != 1 {
		t.Fatalf("hold expiry %v", got)
	}
	if _, err := p.Commit(ctx(t), &seatsafev1.DecisionRequest{TxnId: "t1"}); err != nil {
		t.Fatal(err)
	}
	// Abort after Commit is a protocol violation, reported as INTERNAL (FAILED_PRECONDITION means "not leader").
	if _, err := p.Abort(ctx(t), &seatsafev1.DecisionRequest{TxnId: "t1"}); status.Code(err) != codes.Internal {
		t.Fatalf("abort of committed: %v", err)
	}
	c.Shards[1].SetLeader(1)
	for _, call := range []func() error{
		func() error { _, err := p.Prepare(ctx(t), &seatsafev1.PrepareRequest{TxnId: "t2"}); return err },
		func() error { _, err := p.Commit(ctx(t), &seatsafev1.DecisionRequest{TxnId: "t2"}); return err },
	} {
		if status.Code(call()) != codes.FailedPrecondition {
			t.Fatal("follower accepted a participant call")
		}
	}
}

func TestCoordinator(t *testing.T) {
	c := newTestCluster(t)
	coord := seatsafev1.NewCoordinatorServiceClient(dial(t, c, c.Coordinator.Nodes[0].Addr))

	for name, req := range map[string]*seatsafev1.SubmitOrderRequest{
		"no txn":        {Participants: order("x").Participants},
		"no parts":      {TxnId: "x"},
		"unknown shard": {TxnId: "x", Participants: []*seatsafev1.ShardSeats{{ShardId: 9}}},
	} {
		if _, err := coord.SubmitOrder(ctx(t), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}

	// GetDecision before SubmitOrder logs ABORT first; the order then aborts.
	d, err := coord.GetDecision(ctx(t), &seatsafev1.GetDecisionRequest{TxnId: "early"})
	if err != nil || d.Decision != seatsafev1.Decision_DECISION_ABORT {
		t.Fatalf("GetDecision %v %v", d, err)
	}
	resp, err := coord.SubmitOrder(ctx(t), order("early"))
	if err != nil || resp.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED {
		t.Fatalf("order after abort decision: %v %v", resp, err)
	}
	if c.Shards[1].Machine.ListAvailable("evt-flash", "sec-101").Sections[0].AvailableSeatIds[0] != "R01-S001" {
		t.Fatal("seat held after the abort decision")
	}

	// A shard holding an abort tombstone votes NO without conflicts: ABORTED.
	tomb := seatsafev1.NewParticipantServiceClient(dial(t, c, c.Shards[2].Nodes[0].Addr))
	if _, err := tomb.Abort(ctx(t), &seatsafev1.DecisionRequest{TxnId: "tomb"}); err != nil {
		t.Fatal(err)
	}
	resp, err = coord.SubmitOrder(ctx(t), order("tomb"))
	if err != nil || resp.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED || len(resp.UnavailableSeats) != 0 {
		t.Fatalf("order over a tombstone: %v %v", resp, err)
	}

	// Concurrent retries of one txn run 2PC once and agree.
	var wg sync.WaitGroup
	results := make([]*seatsafev1.BookSeatsResponse, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _ = coord.SubmitOrder(ctx(t), order("dup"))
		}(i)
	}
	wg.Wait()
	fresh := 0
	for _, r := range results {
		if r.GetOutcome() != seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED || !r.GetCrossShard() {
			t.Fatalf("retry got %v", r)
		}
		if !r.GetReplayed() {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d answers not marked replayed, want 1", fresh)
	}
	d, _ = coord.GetDecision(ctx(t), &seatsafev1.GetDecisionRequest{TxnId: "dup"})
	if d.Decision != seatsafev1.Decision_DECISION_COMMIT {
		t.Fatalf("decision %v", d)
	}

	// A waiting retry gives up with its own deadline.
	c.Coordinator.mu.Lock()
	c.Coordinator.inflight["stuck"] = make(chan struct{})
	c.Coordinator.mu.Unlock()
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := coord.SubmitOrder(short, order("stuck")); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("stuck retry: %v", err)
	}

	c.Coordinator.SetLeader(1)
	if _, err := coord.GetDecision(ctx(t), &seatsafev1.GetDecisionRequest{TxnId: "z"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower GetDecision: %v", err)
	}
	if _, err := coord.SubmitOrder(ctx(t), order("z")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower SubmitOrder: %v", err)
	}
	if st, _ := seatsafev1.NewNodeServiceClient(dial(t, c, c.Coordinator.Nodes[1].Addr)).Status(ctx(t), &seatsafev1.StatusRequest{}); st.Role != seatsafev1.RaftRole_RAFT_ROLE_LEADER || st.GroupId != 0 {
		t.Fatalf("coordinator status %v", st)
	}
}

func TestNewErrors(t *testing.T) {
	sm, seeds, err := SmallDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(sm, seeds, Options{}); err == nil {
		t.Fatal("missing Listen accepted")
	}
	failing := func(string) (net.Listener, error) { return nil, net.ErrClosed }
	if _, err := New(sm, seeds, Options{Listen: failing}); err == nil {
		t.Fatal("listen error ignored")
	}
	// A seed file whose version disagrees mid-way is refused.
	bad := *seeds[1]
	bad.Version = ""
	if _, err := New(sm, map[uint32]*seed.Loaded{1: &bad}, Options{Listen: func(string) (net.Listener, error) { return nil, nil }}); err == nil {
		t.Fatal("bad seed accepted")
	}
	// Without seed files the shards stay unseeded and the cluster still runs.
	c, err := NewBufconn(sm, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, _, done := c.Shards[1].Machine.SeedStatus(); done {
		t.Fatal("unseeded shard reports seeded")
	}
	if _, _, err := (Dataset{Shards: 0, FlashSections: 1, FlashRows: 1, FlashSeatsPerRow: 1, ChunkSize: 1}).Build(); err == nil {
		t.Fatal("dataset with no shards accepted")
	}
}
