package shard

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
)

// fakeDecider stands in for the coordinator: it answers with whatever
// decision is set (an error while none is), and counts calls per node.
type fakeDecider struct {
	mu       sync.Mutex
	decision seatsafev1.Decision
	calls    map[uint64]int
}

func (f *fakeDecider) set(d seatsafev1.Decision) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.decision = d
}

func (f *fakeDecider) callsBy(id uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

func (f *fakeDecider) forNode(id uint64) DecideFunc {
	return func(context.Context, string) (seatsafev1.Decision, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.calls == nil {
			f.calls = map[uint64]int{}
		}
		f.calls[id]++
		if f.decision == seatsafev1.Decision_DECISION_UNSPECIFIED {
			return 0, errors.New("coordinator unreachable")
		}
		return f.decision, nil
	}
}

// runResolvers starts a HoldResolver on every node of c, checking every 10 ms.
func runResolvers(t *testing.T, c *shard, d *fakeDecider) *atomic.Int32 {
	t.Helper()
	resolved := new(atomic.Int32)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for id, n := range c.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			(&HoldResolver{Node: n.node, SM: n.sm, Decide: d.forNode(id), Logf: t.Logf,
				Interval: 10 * time.Millisecond, OnResolved: func(string) { resolved.Add(1) }}).Run(ctx)
		}()
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	return resolved
}

// prepareHold prepares txn on the leader for seats with a hold of ttl.
func prepareHold(t *testing.T, addr, txn string, ttl time.Duration, seats ...*seatsafev1.SeatRef) {
	t.Helper()
	v, err := seatsafev1.NewParticipantServiceClient(conn(t, addr)).Prepare(ctx5(t),
		&seatsafev1.PrepareRequest{TxnId: txn, UserId: "alice", Seats: seats, HoldTtlMs: ttl.Milliseconds()})
	if err != nil || v.GetVote() != seatsafev1.Vote_VOTE_YES {
		t.Fatalf("prepare %s: %v %v", txn, v, err)
	}
}

func available(n *storageNode, s *seatsafev1.SeatRef) bool {
	for _, sec := range n.sm.ListAvailable(s.EventId, s.SectionId).GetSections() {
		if slices.Contains(sec.GetAvailableSeatIds(), s.SeatId) {
			return true
		}
	}
	return false
}

// waitResolved waits until no node holds a prepared txn any more.
func waitResolved(t *testing.T, c *shard) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		pending := 0
		for _, n := range c.nodes {
			pending += len(n.sm.ExpiredHolds(1 << 62))
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d holds still prepared after 5s", pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A hold whose Commit never arrived is committed once it expires, on every
// replica, and only the leader asks the coordinator.
func TestExpiredHoldCommitted(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	leadID, lead := c.leader()
	a, b := seat(0, 1), seat(0, 2)
	prepareHold(t, lead.addr, "t1", 100*time.Millisecond, a, b)

	d := &fakeDecider{decision: seatsafev1.Decision_DECISION_COMMIT}
	if resolved := runResolvers(t, c, d); resolved.Load() != 0 {
		t.Fatal("resolved before starting")
	}
	waitResolved(t, c)
	c.waitIdentical()
	for id, n := range c.nodes {
		if available(n, a) || available(n, b) {
			t.Fatalf("node %d: committed seats still available", id)
		}
		if err := n.sm.CheckInvariants(); err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		if id != leadID && d.callsBy(id) != 0 {
			t.Fatalf("follower %d asked the coordinator %d times", id, d.callsBy(id))
		}
	}
	if r := book(t, lead.addr, "req-late", "bob", a); r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE {
		t.Fatalf("booking a committed seat: %v", r)
	}
}

// An expired hold the coordinator aborted releases its seats.
func TestExpiredHoldAborted(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	_, lead := c.leader()
	a := seat(0, 3)
	prepareHold(t, lead.addr, "t1", 100*time.Millisecond, a)
	runResolvers(t, c, &fakeDecider{decision: seatsafev1.Decision_DECISION_ABORT})
	waitResolved(t, c)
	c.waitIdentical()
	if r := book(t, lead.addr, "req-after", "bob", a); r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED {
		t.Fatalf("booking a seat whose hold was aborted: %v", r)
	}
}

// Without the coordinator's answer a hold is never released, however long it
// is expired: the coordinator may already have committed. Once it answers,
// the hold is resolved.
func TestHoldKeptUntilCoordinatorAnswers(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	leadID, lead := c.leader()
	a := seat(0, 4)
	prepareHold(t, lead.addr, "t1", 50*time.Millisecond, a)
	d := &fakeDecider{} // unreachable
	runResolvers(t, c, d)

	deadline := time.Now().Add(5 * time.Second)
	for d.callsBy(leadID) < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("leader asked only %d times in 5s", d.callsBy(leadID))
		}
		time.Sleep(10 * time.Millisecond)
	}
	for id, n := range c.nodes {
		if available(n, a) || len(n.sm.ExpiredHolds(1<<62)) != 1 {
			t.Fatalf("node %d released the hold without a decision", id)
		}
	}
	if r := book(t, lead.addr, "req-x", "bob", a); r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE {
		t.Fatalf("booking a held seat: %v", r)
	}

	d.set(seatsafev1.Decision_DECISION_ABORT)
	waitResolved(t, c)
}

// A hold that hasn't expired is left alone.
func TestLiveHoldNotTouched(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	leadID, lead := c.leader()
	prepareHold(t, lead.addr, "t1", time.Minute, seat(0, 5))
	d := &fakeDecider{decision: seatsafev1.Decision_DECISION_ABORT}
	runResolvers(t, c, d)
	time.Sleep(200 * time.Millisecond) // about 20 rounds
	if n := d.callsBy(leadID); n != 0 {
		t.Fatalf("asked the coordinator %d times about a live hold", n)
	}
	if got := len(lead.sm.ExpiredHolds(time.Now().Add(2 * time.Minute).UnixMilli())); got != 1 {
		t.Fatalf("hold gone: %d prepared", got)
	}
}

// coordinatorStub serves GetDecision on one of three addresses, like a
// coordinator group whose leader is that node; the others redirect to it.
type coordinatorStub struct {
	seatsafev1.UnimplementedCoordinatorServiceServer
	leader string
	self   string
	asked  atomic.Int32
}

func (s *coordinatorStub) GetDecision(ctx context.Context, req *seatsafev1.GetDecisionRequest) (*seatsafev1.GetDecisionResponse, error) {
	if s.self != s.leader {
		_ = grpc.SetTrailer(ctx, map[string][]string{LeaderTrailer: {s.leader}})
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}
	s.asked.Add(1)
	return &seatsafev1.GetDecisionResponse{Decision: seatsafev1.Decision_DECISION_COMMIT}, nil
}

// CoordinatorDecider reaches the coordinator group's leader through
// groupclient, following a redirect from a follower.
func TestCoordinatorDecider(t *testing.T) {
	var addrs []string
	var lis []net.Listener
	for range 3 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis, addrs = append(lis, l), append(addrs, l.Addr().String())
	}
	var leader *coordinatorStub
	for i, l := range lis {
		s := &coordinatorStub{leader: addrs[2], self: addrs[i]}
		if i == 2 {
			leader = s
		}
		gs := grpc.NewServer()
		seatsafev1.RegisterCoordinatorServiceServer(gs, s)
		go gs.Serve(l)
		t.Cleanup(gs.Stop)
	}
	g, err := groupclient.New("coordinator", addrs, groupclient.Config{}, groupclient.NewMetrics(metrics.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })

	got, err := CoordinatorDecider(g)(ctx5(t), "t1")
	if err != nil || got != seatsafev1.Decision_DECISION_COMMIT {
		t.Fatalf("decision %v, err %v", got, err)
	}
	if leader.asked.Load() != 1 {
		t.Fatalf("leader asked %d times", leader.asked.Load())
	}
}

// If the coordinator's answer contradicts the shard's log, the shard keeps
// its first decision and counts the refusal (#39). Here the coordinator's
// Abort reaches the shard while the resolver is asking GetDecision, which
// then (wrongly) says COMMIT.
func TestRefusedDecisionCounted(t *testing.T) {
	c := startShard(t, fakeSeed{version: "v1", chunks: 1}, false)
	c.waitSeeded()
	leadID, lead := c.leader()
	a := seat(0, 6)
	prepareHold(t, lead.addr, "t1", 50*time.Millisecond, a)

	var aborted sync.Once
	var refused, resolved atomic.Int32
	decide := func(ctx context.Context, txn string) (seatsafev1.Decision, error) {
		aborted.Do(func() {
			if _, err := seatsafev1.NewParticipantServiceClient(conn(t, lead.addr)).Abort(ctx, &seatsafev1.DecisionRequest{TxnId: txn}); err != nil {
				t.Errorf("abort: %v", err)
			}
		})
		return seatsafev1.Decision_DECISION_COMMIT, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&HoldResolver{Node: c.nodes[leadID].node, SM: c.nodes[leadID].sm, Decide: decide, Logf: t.Logf, Interval: 10 * time.Millisecond,
			OnRefused: func(string) { refused.Add(1) }, OnResolved: func(string) { resolved.Add(1) }}).Run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(5 * time.Second)
	for refused.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("refused decision never counted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if resolved.Load() != 0 {
		t.Fatalf("a refused decision was also counted as resolved")
	}
	if r := book(t, lead.addr, "req-after", "bob", a); r.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED {
		t.Fatalf("the abort should stand and free the seat: %v", r)
	}
}
