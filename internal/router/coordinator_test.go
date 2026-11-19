package router_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/router"
)

// slowCoordinator answers SubmitOrder the way the real coordinator does, but
// takes delay to decide: the first call for a txn runs it, a call that
// arrives meanwhile waits for that run, and a call that arrives after it
// gets the stored answer with replayed = true.
type slowCoordinator struct {
	seatsafev1.UnimplementedCoordinatorServiceServer
	delay time.Duration
	calls atomic.Int32

	mu       sync.Mutex
	decided  map[string]bool
	inflight map[string]chan struct{}
}

func (s *slowCoordinator) SubmitOrder(ctx context.Context, req *seatsafev1.SubmitOrderRequest) (*seatsafev1.BookSeatsResponse, error) {
	s.calls.Add(1)
	id := req.GetTxnId()
	answer := func(replayed bool) *seatsafev1.BookSeatsResponse {
		return &seatsafev1.BookSeatsResponse{OrderId: id, Outcome: confirmed, Replayed: replayed, CrossShard: true}
	}
	s.mu.Lock()
	if s.decided[id] {
		s.mu.Unlock()
		return answer(true), nil
	}
	if run := s.inflight[id]; run != nil {
		s.mu.Unlock()
		select {
		case <-run:
			return answer(false), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	run := make(chan struct{})
	s.inflight[id] = run
	s.mu.Unlock()

	time.Sleep(s.delay) // the order runs to its decision whoever is waiting
	s.mu.Lock()
	s.decided[id] = true
	delete(s.inflight, id)
	s.mu.Unlock()
	close(run)
	return answer(false), nil
}

// newSlowEnv is the real router in front of a slowCoordinator on every
// coordinator address of SmallDataset's shard map.
func newSlowEnv(t *testing.T, delay time.Duration, cfg router.Config) (*router.Router, *slowCoordinator) {
	t.Helper()
	sm, _, err := fakecluster.SmallDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	coord := &slowCoordinator{delay: delay, decided: map[string]bool{}, inflight: map[string]chan struct{}{}}
	listeners := map[string]*bufconn.Listener{}
	for _, addr := range sm.CoordinatorNodes() {
		lis := bufconn.Listen(1 << 20)
		listeners[addr] = lis
		gs := grpc.NewServer()
		seatsafev1.RegisterCoordinatorServiceServer(gs, coord)
		go func() { _ = gs.Serve(lis) }()
		t.Cleanup(gs.Stop)
	}
	cfg.Group.DialOptions = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			if lis, ok := listeners[addr]; ok {
				return lis.DialContext(ctx)
			}
			return nil, net.ErrClosed // shards are not part of this test
		}),
	}
	rt, err := router.New(sm, cfg, metrics.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	return rt, coord
}

func bookCross(t *testing.T, rt *router.Router, id string) *seatsafev1.BookSeatsResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// sec-101 is on shard 1 and sec-102 on shard 2: a cross-shard order.
	resp, err := rt.BookSeats(ctx, &seatsafev1.BookSeatsRequest{RequestId: id, UserId: "alice",
		Seats: []*seatsafev1.SeatRef{flash("sec-101", 1, 1), flash("sec-102", 1, 1)}})
	if err != nil {
		t.Fatalf("BookSeats(%s): %v", id, err)
	}
	return resp
}

// The first answer to a cross-shard order is never marked replayed, even when
// 2PC is slow: an order that takes 700 ms (Prepare alone may take 300 ms)
// fits in one SubmitOrder attempt, so the router does not retry it and get
// its own order's stored answer back. A retry by the client is still
// answered from the store.
func TestSubmitOrderFirstAnswerIsNeverReplayed(t *testing.T) {
	rt, coord := newSlowEnv(t, 700*time.Millisecond, router.Config{})
	for _, id := range []string{"slow-1", "slow-2", "slow-3"} {
		before := coord.calls.Load()
		wantOutcome(t, bookCross(t, rt, id), confirmed, true, false)
		if n := coord.calls.Load() - before; n != 1 {
			t.Fatalf("%s: the router sent SubmitOrder %d times for one BookSeats", id, n)
		}
	}
	wantOutcome(t, bookCross(t, rt, "slow-1"), confirmed, true, true)
	if got := rt.GroupMetrics.Retries.Value("coordinator", groupclient.ReasonAttemptTimer); got != 0 {
		t.Fatalf("%v SubmitOrder attempts timed out", got)
	}
}

// With the shard groups' 500 ms the same order is cut off and sent again,
// which is what ROUTER_COORD_ATTEMPT_MS exists to avoid.
func TestShortCoordinatorAttemptRetriesSlowOrders(t *testing.T) {
	rt, coord := newSlowEnv(t, 700*time.Millisecond, router.Config{Coordinator: groupclient.Config{AttemptTimeout: 500 * time.Millisecond}})
	wantOutcome(t, bookCross(t, rt, "slow-1"), confirmed, true, false)
	if n := coord.calls.Load(); n < 2 {
		t.Fatalf("SubmitOrder sent %d times, expected a retry after the 500 ms attempt", n)
	}
	if got := rt.GroupMetrics.Retries.Value("coordinator", groupclient.ReasonAttemptTimer); got < 1 {
		t.Fatalf("attempt timeouts counted: %v", got)
	}
}
