package coordinator

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/coordsm"
)

type service struct {
	seatsafev1.UnimplementedCoordinatorServiceServer
	c *Coordinator
}

// SubmitOrder runs 2PC for one cross-shard order and returns its outcome.
func (s service) SubmitOrder(ctx context.Context, req *seatsafev1.SubmitOrderRequest) (*seatsafev1.BookSeatsResponse, error) {
	c := s.c
	start := time.Now()
	resp, err := c.submit(ctx, req)
	label := codeLabel(status.Code(err))
	if err == nil {
		label = outcomeLabel(resp.GetOutcome())
	}
	c.Metrics.Submit.Inc(label)
	c.Metrics.Total.Observe(time.Since(start).Seconds(), label)
	return resp, err
}

func (c *Coordinator) submit(ctx context.Context, req *seatsafev1.SubmitOrderRequest) (*seatsafev1.BookSeatsResponse, error) {
	if err := c.requireLeader(ctx); err != nil {
		return nil, err
	}
	if err := c.validate(req); err != nil {
		return nil, err
	}
	res, err := c.propose(ctx, &seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Started{Started: &seatsafev1.TxnStarted{
		TxnId: req.GetTxnId(), UserId: req.GetUserId(), Participants: req.GetParticipants(),
	}}})
	if err != nil {
		return nil, statusErr(err)
	}
	if res.Err != nil || res.Txn == nil {
		return nil, status.Errorf(codes.Internal, "coordinator: TxnStarted refused: %v", res.Err)
	}
	if res.Txn.Decided() {
		return res.Txn.Response(true), nil // the stored answer to an earlier try
	}

	f, ok := c.join(res.Txn)
	if !ok {
		return nil, statusErr(errClosed)
	}
	select {
	case <-f.done:
	case <-ctx.Done():
		// The order carries on; a retry waits for it or reads its result.
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if f.err != nil {
		return nil, statusErr(f.err)
	}
	return f.txn.Response(false), nil
}

// validate checks what the router promises: a txn id, and participants sorted
// by shard id, each a shard this coordinator knows, with at least one seat.
func (c *Coordinator) validate(req *seatsafev1.SubmitOrderRequest) error {
	if req.GetTxnId() == "" {
		return status.Error(codes.InvalidArgument, "txn_id is required")
	}
	parts := req.GetParticipants()
	if len(parts) == 0 {
		return status.Error(codes.InvalidArgument, "at least one participant is required")
	}
	for i, p := range parts {
		switch {
		case c.cfg.Shards[p.GetShardId()] == nil:
			return status.Errorf(codes.InvalidArgument, "unknown shard %d", p.GetShardId())
		case i > 0 && p.GetShardId() <= parts[i-1].GetShardId():
			return status.Error(codes.InvalidArgument, "participants must be sorted by shard_id, one entry per shard")
		case len(p.GetSeats()) == 0:
			return status.Errorf(codes.InvalidArgument, "no seats for shard %d", p.GetShardId())
		}
	}
	return nil
}

// join returns the run of t in progress on this node, starting one if there
// is none. ok is false if the coordinator is closed.
func (c *Coordinator) join(t *coordsm.Txn) (f *flight, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f := c.flights[t.ID]; f != nil {
		return f, true
	}
	if c.closed {
		return nil, false
	}
	f = &flight{done: make(chan struct{})}
	c.flights[t.ID] = f
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(f, t)
	}()
	return f, true
}

func (c *Coordinator) inFlight(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flights[id] != nil
}

// land ends a run: waiting callers get the decided record or the error.
func (c *Coordinator) land(f *flight, id string, t *coordsm.Txn, err error) {
	c.mu.Lock()
	delete(c.flights, id)
	c.mu.Unlock()
	f.txn, f.err = t, err
	close(f.done)
}

// run takes one undecided txn from Prepare to its decision, then hands the
// decision to the background deliveries. It does not depend on any caller.
func (c *Coordinator) run(f *flight, t *coordsm.Txn) {
	// A run that ended between this caller's TxnStarted and its join has
	// already decided the txn.
	if cur, ok := c.sm.Get(t.ID); ok && cur.Decided() {
		c.land(f, t.ID, cur, nil)
		return
	}

	start := time.Now()
	votes := c.prepare(t)
	c.Metrics.Prepare.Observe(time.Since(start).Seconds())
	if c.cfg.beforeDecide != nil {
		c.cfg.beforeDecide(c.ctx, t.ID)
	}
	if c.ctx.Err() != nil {
		c.land(f, t.ID, nil, errClosed)
		return
	}

	start = time.Now()
	res, err := c.proposeWhileLeader(&seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Decided{Decided: decide(t, votes)}})
	c.Metrics.Decide.Observe(time.Since(start).Seconds())
	if err == nil && (res.Err != nil || res.Txn == nil) {
		err = status.Errorf(codes.Internal, "TxnDecided refused: %v", res.Err)
	}
	if err != nil {
		// Not leader any more, or shutting down: the next leader aborts the
		// txn or finds the decision if this proposal still commits.
		c.land(f, t.ID, nil, err)
		return
	}
	if res.Won {
		c.Metrics.Decisions.Inc(decisionLabel(res.Txn.Decision), SourceVotes)
	}
	if c.cfg.afterDecided != nil {
		c.cfg.afterDecided(c.ctx, t.ID)
	}
	if c.ctx.Err() != nil {
		c.land(f, t.ID, nil, errClosed)
		return
	}
	c.land(f, t.ID, res.Txn, nil)
	c.deliver(res.Txn)
}

type vote struct {
	resp *seatsafev1.PrepareResponse
	err  error
}

// prepare asks every participant to hold its seats, in parallel, and returns
// the votes in participant order. A participant that has not answered within
// PrepareTimeout counts as a failure.
func (c *Coordinator) prepare(t *coordsm.Txn) []vote {
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.PrepareTimeout)
	defer cancel()
	votes := make([]vote, len(t.Participants))
	var wg sync.WaitGroup
	for i, p := range t.Participants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &seatsafev1.PrepareRequest{TxnId: t.ID, UserId: t.User, Seats: coordsm.Refs(p.Seats), HoldTtlMs: c.cfg.HoldTTL.Milliseconds()}
			g := c.cfg.Shards[p.Shard]
			if g == nil {
				votes[i].err = status.Errorf(codes.Internal, "unknown shard %d", p.Shard)
				return
			}
			var resp *seatsafev1.PrepareResponse
			err := g.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
				var err error
				resp, err = seatsafev1.NewParticipantServiceClient(cc).Prepare(ctx, req, opts...)
				return err
			})
			votes[i] = vote{resp, err}
		}()
	}
	wg.Wait()
	return votes
}

// decide turns the votes into the decision to propose:
//
//	every vote YES            COMMIT, CONFIRMED
//	a NO that names seats     ABORT, SEAT_UNAVAILABLE with the conflicts of every
//	                          NO in participant order, even if another shard
//	                          failed to answer
//	anything else             ABORT, ABORTED: a shard did not answer in time, or
//	                          voted NO without conflicts (an abort tombstone, an
//	                          unknown seat, an unseeded shard)
func decide(t *coordsm.Txn, votes []vote) *seatsafev1.TxnDecided {
	d := &seatsafev1.TxnDecided{TxnId: t.ID, Decision: seatsafev1.Decision_DECISION_COMMIT, Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED}
	seen := map[coordsm.Seat]bool{}
	allYes := true
	for _, v := range votes {
		if v.err == nil && v.resp.GetVote() == seatsafev1.Vote_VOTE_YES {
			continue
		}
		allYes = false
		for _, r := range v.resp.GetConflicts() {
			k := coordsm.Seat{Event: r.GetEventId(), Section: r.GetSectionId(), Seat: r.GetSeatId()}
			if !seen[k] {
				seen[k] = true
				d.UnavailableSeats = append(d.UnavailableSeats, k.Ref())
			}
		}
	}
	switch {
	case allYes:
	case len(d.UnavailableSeats) > 0:
		d.Decision, d.Outcome = seatsafev1.Decision_DECISION_ABORT, seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
	default:
		d.Decision, d.Outcome = seatsafev1.Decision_DECISION_ABORT, seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED
	}
	return d
}

// GetDecision returns a txn's decision, logging ABORT first if it has none.
func (s service) GetDecision(ctx context.Context, req *seatsafev1.GetDecisionRequest) (*seatsafev1.GetDecisionResponse, error) {
	c := s.c
	if err := c.requireLeader(ctx); err != nil {
		return nil, err
	}
	id := req.GetTxnId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "txn_id is required")
	}
	// A decision applied here is committed and final, whoever leads now.
	if t, ok := c.sm.Get(id); ok && t.Decided() {
		return &seatsafev1.GetDecisionResponse{Decision: t.Decision}, nil
	}
	res, err := c.propose(ctx, abortCommand(id))
	if err != nil {
		return nil, statusErr(err)
	}
	if res.Err != nil || res.Txn == nil || !res.Txn.Decided() {
		return nil, status.Errorf(codes.Internal, "coordinator: TxnDecided refused: %v", res.Err)
	}
	if res.Won {
		c.Metrics.Decisions.Inc(decisionLabel(res.Txn.Decision), SourceGetDecision)
	}
	// The other participants still have to hear about it.
	c.deliver(res.Txn)
	return &seatsafev1.GetDecisionResponse{Decision: res.Txn.Decision}, nil
}

func abortCommand(id string) *seatsafev1.CoordCommand {
	return &seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Decided{Decided: &seatsafev1.TxnDecided{
		TxnId: id, Decision: seatsafev1.Decision_DECISION_ABORT, Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED,
	}}}
}
