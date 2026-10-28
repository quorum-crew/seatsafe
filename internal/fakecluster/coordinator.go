package fakecluster

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
)

// Coordinator timings.
const (
	PrepareTimeout       = 300 * time.Millisecond // whole prepare phase
	DecisionFirstTimeout = 200 * time.Millisecond // synchronous first try of each Commit/Abort
	DecisionRetryTimeout = 2 * time.Second        // each background retry after that
	HoldTTL              = 5 * time.Second        // hold_ttl_ms sent with Prepare
)

// FakeCoordinator is an in-memory 2PC coordinator group of 3 nodes. Unlike
// the real one it logs nothing: a restart forgets every txn.
type FakeCoordinator struct {
	*leadership
	Nodes  []*FakeCoordNode
	shards map[uint32]*groupclient.Group

	mu        sync.Mutex
	results   map[string]*seatsafev1.BookSeatsResponse
	decisions map[string]seatsafev1.Decision
	inflight  map[string]chan struct{}

	bg     sync.WaitGroup
	closed chan struct{}
}

// NewFakeCoordinator builds the coordinator group. shards are clients for
// the participant groups, keyed by shard_id. Node 0 starts as leader.
func NewFakeCoordinator(addrs []string, shards map[uint32]*groupclient.Group) *FakeCoordinator {
	c := &FakeCoordinator{
		leadership: newLeadership(0, addrs),
		shards:     shards,
		results:    map[string]*seatsafev1.BookSeatsResponse{},
		decisions:  map[string]seatsafev1.Decision{},
		inflight:   map[string]chan struct{}{},
		closed:     make(chan struct{}),
	}
	for i, a := range addrs {
		c.Nodes = append(c.Nodes, &FakeCoordNode{c: c, idx: i, Addr: a})
	}
	return c
}

// Close stops background decision retries and waits for them.
func (c *FakeCoordinator) Close() {
	close(c.closed)
	c.bg.Wait()
}

// FakeCoordNode is one node of the FakeCoordinator group.
type FakeCoordNode struct {
	c    *FakeCoordinator
	idx  int
	Addr string
}

// Register adds the node's CoordinatorService and NodeService to s.
func (n *FakeCoordNode) Register(s grpc.ServiceRegistrar) {
	seatsafev1.RegisterCoordinatorServiceServer(s, coordService{n: n})
	seatsafev1.RegisterNodeServiceServer(s, nodeService{status: func() *seatsafev1.NodeStatus {
		return n.c.status(n.idx, nodeID(n.Addr), 0)
	}})
}

type coordService struct {
	seatsafev1.UnimplementedCoordinatorServiceServer
	n *FakeCoordNode
}

func (s coordService) SubmitOrder(ctx context.Context, req *seatsafev1.SubmitOrderRequest) (*seatsafev1.BookSeatsResponse, error) {
	c := s.n.c
	if err := c.checkLeader(ctx, s.n.idx); err != nil {
		return nil, err
	}
	if req.GetTxnId() == "" || len(req.GetParticipants()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "fakecluster: txn_id and participants are required")
	}
	for _, p := range req.GetParticipants() {
		if c.shards[p.GetShardId()] == nil {
			return nil, status.Errorf(codes.InvalidArgument, "fakecluster: unknown shard %d", p.GetShardId())
		}
	}
	id := req.GetTxnId()
	for {
		c.mu.Lock()
		if r := c.results[id]; r != nil {
			c.mu.Unlock()
			out := proto.Clone(r).(*seatsafev1.BookSeatsResponse)
			out.Replayed = true
			return out, nil
		}
		if ch := c.inflight[id]; ch != nil {
			c.mu.Unlock()
			select { // a retry while the first try runs: wait for its result
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		}
		ch := make(chan struct{})
		c.inflight[id] = ch
		c.mu.Unlock()

		// The order runs to completion even if this caller gives up.
		resp := c.run(req)
		c.mu.Lock()
		c.results[id] = resp
		delete(c.inflight, id)
		c.mu.Unlock()
		close(ch)
		return proto.Clone(resp).(*seatsafev1.BookSeatsResponse), nil
	}
}

func (s coordService) GetDecision(ctx context.Context, req *seatsafev1.GetDecisionRequest) (*seatsafev1.GetDecisionResponse, error) {
	if err := s.n.c.checkLeader(ctx, s.n.idx); err != nil {
		return nil, err
	}
	// No decision yet: ABORT is recorded first, so the answer is final.
	return &seatsafev1.GetDecisionResponse{Decision: s.n.c.decide(req.GetTxnId(), seatsafev1.Decision_DECISION_ABORT)}, nil
}

// decide records d unless a decision exists, and returns the winner.
func (c *FakeCoordinator) decide(id string, d seatsafev1.Decision) seatsafev1.Decision {
	c.mu.Lock()
	defer c.mu.Unlock()
	if won, ok := c.decisions[id]; ok {
		return won
	}
	c.decisions[id] = d
	return d
}

type vote struct {
	resp *seatsafev1.PrepareResponse
	err  error
}

// run executes 2PC for one order. Participants are visited in request
// order, which the router sorts by shard_id.
func (c *FakeCoordinator) run(req *seatsafev1.SubmitOrderRequest) *seatsafev1.BookSeatsResponse {
	parts := req.GetParticipants()
	votes := make([]vote, len(parts))
	pctx, cancel := context.WithTimeout(context.Background(), PrepareTimeout)
	var wg sync.WaitGroup
	for i, p := range parts {
		wg.Add(1)
		go func(i int, p *seatsafev1.ShardSeats) {
			defer wg.Done()
			preq := &seatsafev1.PrepareRequest{TxnId: req.GetTxnId(), UserId: req.GetUserId(), Seats: p.GetSeats(), HoldTtlMs: HoldTTL.Milliseconds()}
			var resp *seatsafev1.PrepareResponse
			err := c.shards[p.GetShardId()].Call(pctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
				var err error
				resp, err = seatsafev1.NewParticipantServiceClient(cc).Prepare(ctx, preq, opts...)
				return err
			})
			votes[i] = vote{resp, err}
		}(i, p)
	}
	wg.Wait()
	cancel()

	want := seatsafev1.Decision_DECISION_COMMIT
	var conflicts []*seatsafev1.SeatRef
	failed := false
	for _, v := range votes {
		switch {
		case v.err != nil:
			failed = true
			want = seatsafev1.Decision_DECISION_ABORT
		case v.resp.GetVote() != seatsafev1.Vote_VOTE_YES:
			want = seatsafev1.Decision_DECISION_ABORT
			conflicts = append(conflicts, v.resp.GetConflicts()...)
		}
	}
	d := c.decide(req.GetTxnId(), want)

	for _, p := range parts {
		wg.Add(1)
		go func(shard uint32) {
			defer wg.Done()
			c.sendDecision(shard, req.GetTxnId(), d)
		}(p.GetShardId())
	}
	wg.Wait()

	resp := &seatsafev1.BookSeatsResponse{OrderId: req.GetTxnId(), CrossShard: true}
	switch {
	case d == seatsafev1.Decision_DECISION_COMMIT:
		resp.Outcome = seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED
	case len(conflicts) > 0:
		resp.Outcome = seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
		resp.UnavailableSeats = conflicts
	case failed || want == seatsafev1.Decision_DECISION_COMMIT:
		// A participant timed out, or GetDecision aborted the txn first.
		resp.Outcome = seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED
	default:
		// Every NO came without conflicts: an unknown seat or an unseeded shard.
		resp.Outcome = seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID
	}
	return resp
}

// sendDecision delivers Commit or Abort to one shard. The first try is
// synchronous (and short) so callers usually see settled seats; if it fails,
// it keeps retrying in the background until it succeeds or the coordinator
// is closed.
func (c *FakeCoordinator) sendDecision(shard uint32, txnID string, d seatsafev1.Decision) {
	send := func(ctx context.Context) error {
		return c.shards[shard].Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
			client := seatsafev1.NewParticipantServiceClient(cc)
			var err error
			if d == seatsafev1.Decision_DECISION_COMMIT {
				_, err = client.Commit(ctx, &seatsafev1.DecisionRequest{TxnId: txnID}, opts...)
			} else {
				_, err = client.Abort(ctx, &seatsafev1.DecisionRequest{TxnId: txnID}, opts...)
			}
			return err
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), DecisionFirstTimeout)
	err := send(ctx)
	cancel()
	if err == nil {
		return
	}
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), DecisionRetryTimeout)
			err := send(ctx)
			cancel()
			if err == nil {
				return
			}
			select {
			case <-c.closed:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
}
