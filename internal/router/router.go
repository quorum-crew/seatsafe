// Package router implements BookingService, the public API. It routes each
// order by the shard map: seats on one shard go to that shard's leader as
// ShardService.Book; seats on two or more shards go to the coordinator as
// SubmitOrder (2PC). Leader discovery, redirects and retries live in
// internal/groupclient.
//
// Error contract (to clients):
//
//	OK                 read outcome
//	INVALID_ARGUMENT   empty request_id or seats, mixed events, duplicate seats; do not retry
//	DEADLINE_EXCEEDED  the deadline ran out first; retry with the same request_id, never
//	                   assume the order failed
//
// An unknown section is a normal answer: OK with outcome INVALID.
package router

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// DefaultDeadline applies when a client sends no deadline (clients should
// send 2s themselves).
const DefaultDeadline = 2 * time.Second

// DefaultCoordinatorAttemptTimeout is how long one SubmitOrder attempt may
// take. A cross-shard order spends up to 300 ms in Prepare and then waits for
// two Raft commits, so the 500 ms that suits a single-shard Book would cut
// slow orders off and retry them. The retry would then find the order
// finished and return its stored answer, marked replayed, to a client that
// asked only once.
const DefaultCoordinatorAttemptTimeout = time.Second

// DefaultShardAttemptTimeout is how long one Book attempt to a shard group
// may take before the router gives up on that node and looks for the leader
// again. It bounds what a client waits when a shard leader stops answering
// without closing its connections (a paused or partitioned process). Chaos
// runs on a laptop found no shorter setting that kept healthy requests from
// timing out (docs/results/2026-11-19-router-attempt-timeout.md), so it stays
// at 500 ms until the same runs are repeated on AWS.
const DefaultShardAttemptTimeout = 500 * time.Millisecond

// Config tunes a Router.
type Config struct {
	// Group is the shard groups' client config. A zero AttemptTimeout means
	// DefaultShardAttemptTimeout.
	Group groupclient.Config
	// Coordinator is the coordinator group's client config. Zero fields
	// take Group's values, except AttemptTimeout, which defaults to
	// DefaultCoordinatorAttemptTimeout.
	Coordinator     groupclient.Config
	DefaultDeadline time.Duration // default DefaultDeadline
}

// shardConfig fills cfg.Group's attempt timeout.
func (c Config) shardConfig() groupclient.Config {
	out := c.Group
	if out.AttemptTimeout <= 0 {
		out.AttemptTimeout = DefaultShardAttemptTimeout
	}
	return out
}

func (c Config) deadline() time.Duration {
	if c.DefaultDeadline > 0 {
		return c.DefaultDeadline
	}
	return DefaultDeadline
}

// Validate checks the attempt timeouts against the client deadline. An
// attempt that can outlast the deadline would never be retried, so the
// router would wait out a dead leader instead of finding the new one.
func (c Config) Validate() error {
	deadline := c.deadline()
	for _, a := range []struct {
		name string
		set  time.Duration
		got  time.Duration
	}{
		{"shard", c.Group.AttemptTimeout, c.shardConfig().AttemptTimeout},
		{"coordinator", c.Coordinator.AttemptTimeout, c.coordinatorConfig().AttemptTimeout},
	} {
		switch {
		case a.set < 0:
			return fmt.Errorf("router: %s attempt timeout %v must be positive", a.name, a.set)
		case a.got >= deadline:
			return fmt.Errorf("router: %s attempt timeout %v must be shorter than the client deadline %v", a.name, a.got, deadline)
		}
	}
	return nil
}

// coordinatorConfig fills cfg.Coordinator's zero fields.
func (c Config) coordinatorConfig() groupclient.Config {
	out := c.Coordinator
	if out.AttemptTimeout <= 0 {
		out.AttemptTimeout = DefaultCoordinatorAttemptTimeout
	}
	if out.StatusTimeout <= 0 {
		out.StatusTimeout = c.Group.StatusTimeout
	}
	if len(out.Backoff) == 0 {
		out.Backoff = c.Group.Backoff
	}
	if len(out.DialOptions) == 0 {
		out.DialOptions = c.Group.DialOptions
	}
	return out
}

// Path label values.
const (
	PathSingle = "single" // one shard
	PathCross  = "cross"  // 2+ shards (2PC for BookSeats, fan-out for ListAvailableSeats)
	PathNone   = "none"   // rejected before any shard was called
)

// LatencyBuckets are dense from 0.5 ms to 100 ms, then sparse up to 2 s.
var LatencyBuckets = []float64{
	0.0005, 0.001, 0.0015, 0.002, 0.003, 0.004, 0.005, 0.0075, 0.01, 0.0125, 0.015, 0.0175, 0.02,
	0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.15, 0.25, 0.5, 1, 2,
}

// Metrics are the router's own series.
type Metrics struct {
	Duration *metrics.HistogramVec // method, outcome, path
	Requests *metrics.CounterVec   // method, outcome, path
}

// Router implements seatsafev1.BookingServiceServer.
type Router struct {
	seatsafev1.UnimplementedBookingServiceServer

	sm       *shardmap.Map
	shards   map[uint32]*groupclient.Group
	coord    *groupclient.Group
	deadline time.Duration

	Metrics      *Metrics
	GroupMetrics *groupclient.Metrics
}

// New builds a router with one group client per shard and one for the
// coordinator group. Metrics are registered in reg.
func New(sm *shardmap.Map, cfg Config, reg *metrics.Registry) (*Router, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := &Router{
		sm:       sm,
		shards:   map[uint32]*groupclient.Group{},
		deadline: cfg.DefaultDeadline,
		Metrics: &Metrics{
			Duration: reg.NewHistogramVec("seatsafe_router_request_duration_seconds", "Router request latency.", LatencyBuckets, "method", "outcome", "path"),
			Requests: reg.NewCounterVec("seatsafe_router_requests_total", "Router requests by outcome.", "method", "outcome", "path"),
		},
		GroupMetrics: groupclient.NewMetrics(reg),
	}
	r.deadline = cfg.deadline()
	for _, id := range sm.ShardIDs() {
		name := fmt.Sprintf("shard-%d", id)
		r.GroupMetrics.Init(name)
		g, err := groupclient.New(name, sm.ShardNodes(id), cfg.shardConfig(), r.GroupMetrics)
		if err != nil {
			r.Close()
			return nil, err
		}
		r.shards[id] = g
	}
	r.GroupMetrics.Init("coordinator")
	g, err := groupclient.New("coordinator", sm.CoordinatorNodes(), cfg.coordinatorConfig(), r.GroupMetrics)
	if err != nil {
		r.Close()
		return nil, err
	}
	r.coord = g
	return r, nil
}

// Close closes every group client.
func (r *Router) Close() {
	for _, g := range r.shards {
		_ = g.Close()
	}
	if r.coord != nil {
		_ = r.coord.Close()
	}
}

func (r *Router) withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.deadline)
}

func (r *Router) observe(method, outcome, path string, start time.Time) {
	r.Metrics.Duration.Observe(time.Since(start).Seconds(), method, outcome, path)
	r.Metrics.Requests.Inc(method, outcome, path)
}

// BookSeats books every seat of an order or none.
func (r *Router) BookSeats(ctx context.Context, req *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
	start := time.Now()
	ctx, cancel := r.withDeadline(ctx)
	defer cancel()
	resp, path, err := r.book(ctx, req)
	outcome := codeLabel(status.Code(err))
	if err == nil {
		outcome = outcomeLabel(resp.GetOutcome())
	}
	r.observe("BookSeats", outcome, path, start)
	return resp, err
}

func (r *Router) book(ctx context.Context, req *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, string, error) {
	if err := validate(req); err != nil {
		return nil, PathNone, err
	}
	// Group seats by shard, keeping request order within each shard.
	byShard := map[uint32][]*seatsafev1.SeatRef{}
	var ids []uint32
	for _, s := range req.GetSeats() {
		id, ok := r.sm.Lookup(s.GetEventId(), s.GetSectionId())
		if !ok {
			return &seatsafev1.BookSeatsResponse{OrderId: req.GetRequestId(), Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID}, PathNone, nil
		}
		if _, seen := byShard[id]; !seen {
			ids = append(ids, id)
		}
		byShard[id] = append(byShard[id], s)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var resp *seatsafev1.BookSeatsResponse
	if len(ids) == 1 {
		sreq := &seatsafev1.ShardBookRequest{RequestId: req.GetRequestId(), UserId: req.GetUserId(), Seats: req.GetSeats()}
		err := r.shards[ids[0]].Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
			var err error
			resp, err = seatsafev1.NewShardServiceClient(cc).Book(ctx, sreq, opts...)
			return err
		})
		if err != nil {
			return nil, PathSingle, err
		}
		resp.CrossShard = false
		return resp, PathSingle, nil
	}

	sub := &seatsafev1.SubmitOrderRequest{TxnId: req.GetRequestId(), UserId: req.GetUserId()}
	for _, id := range ids {
		sub.Participants = append(sub.Participants, &seatsafev1.ShardSeats{ShardId: id, Seats: byShard[id]})
	}
	err := r.coord.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		resp, err = seatsafev1.NewCoordinatorServiceClient(cc).SubmitOrder(ctx, sub, opts...)
		return err
	})
	if err != nil {
		return nil, PathCross, err
	}
	resp.CrossShard = true
	return resp, PathCross, nil
}

func validate(req *seatsafev1.BookSeatsRequest) error {
	if req.GetRequestId() == "" {
		return status.Error(codes.InvalidArgument, "request_id is required")
	}
	seats := req.GetSeats()
	if len(seats) == 0 {
		return status.Error(codes.InvalidArgument, "at least one seat is required")
	}
	type key struct{ e, s, id string }
	seen := make(map[key]bool, len(seats))
	for _, s := range seats {
		if s.GetEventId() != seats[0].GetEventId() {
			return status.Error(codes.InvalidArgument, "all seats must belong to one event")
		}
		k := key{s.GetEventId(), s.GetSectionId(), s.GetSeatId()}
		if seen[k] {
			return status.Errorf(codes.InvalidArgument, "seat %s/%s/%s listed twice", k.e, k.s, k.id)
		}
		seen[k] = true
	}
	return nil
}

// ListAvailableSeats asks the shard owning section_id, or every shard of the
// event when section_id is empty, and merges the sections.
func (r *Router) ListAvailableSeats(ctx context.Context, req *seatsafev1.ListAvailableSeatsRequest) (*seatsafev1.ListAvailableSeatsResponse, error) {
	start := time.Now()
	ctx, cancel := r.withDeadline(ctx)
	defer cancel()
	resp, path, err := r.list(ctx, req)
	outcome := codeLabel(status.Code(err))
	r.observe("ListAvailableSeats", outcome, path, start)
	return resp, err
}

func (r *Router) list(ctx context.Context, req *seatsafev1.ListAvailableSeatsRequest) (*seatsafev1.ListAvailableSeatsResponse, string, error) {
	var ids []uint32
	if req.GetSectionId() != "" {
		if id, ok := r.sm.Lookup(req.GetEventId(), req.GetSectionId()); ok {
			ids = []uint32{id}
		}
	} else {
		ids = r.sm.EventShards(req.GetEventId())
	}
	if len(ids) == 0 {
		return &seatsafev1.ListAvailableSeatsResponse{}, PathNone, nil
	}
	path := PathSingle
	if len(ids) > 1 {
		path = PathCross
	}

	parts := make([]*seatsafev1.ListAvailableSeatsResponse, len(ids))
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, g *groupclient.Group) {
			defer wg.Done()
			errs[i] = g.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
				var err error
				parts[i], err = seatsafev1.NewShardServiceClient(cc).ListAvailable(ctx, req, opts...)
				return err
			})
		}(i, r.shards[id])
	}
	wg.Wait()
	out := &seatsafev1.ListAvailableSeatsResponse{}
	for i := range ids {
		if errs[i] != nil {
			return nil, path, errs[i]
		}
		out.Sections = append(out.Sections, parts[i].GetSections()...)
	}
	sort.Slice(out.Sections, func(i, j int) bool { return out.Sections[i].GetSectionId() < out.Sections[j].GetSectionId() })
	return out, path, nil
}

// outcomeLabel turns BOOKING_OUTCOME_CONFIRMED into CONFIRMED.
func outcomeLabel(o seatsafev1.BookingOutcome) string {
	const prefix = "BOOKING_OUTCOME_"
	s := o.String()
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

// codeLabel turns codes.DeadlineExceeded into DEADLINE_EXCEEDED (and OK into OK).
func codeLabel(c codes.Code) string {
	var out []rune
	prevLower := false
	for _, ch := range c.String() {
		if unicode.IsUpper(ch) && prevLower {
			out = append(out, '_')
		}
		prevLower = unicode.IsLower(ch)
		out = append(out, unicode.ToUpper(ch))
	}
	return string(out)
}
