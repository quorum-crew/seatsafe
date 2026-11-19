package loadgen

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// Final outcomes of an order.
const (
	OutcomeConfirmed   = "CONFIRMED"
	OutcomeUnavailable = "SEAT_UNAVAILABLE"
	OutcomeAborted     = "ABORTED"
	OutcomeInvalid     = "INVALID"
	OutcomeUnknown     = "UNKNOWN" // gave up: the order may or may not have booked
)

// Backoff is the wait before each retry; the last value repeats.
var Backoff = []time.Duration{10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}

// BookFunc makes one call.
type BookFunc func(ctx context.Context, req *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error)

// Pool is a set of gRPC connections used round-robin.
type Pool struct {
	conns []*grpc.ClientConn
	next  atomic.Uint64
}

// Dial opens n connections to target. Each is its own TCP connection.
func Dial(target string, n int, opts ...grpc.DialOption) (*Pool, error) {
	p := &Pool{}
	opts = append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 << 20)),
	}, opts...)
	for range n {
		cc, err := grpc.NewClient(target, opts...)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.conns = append(p.conns, cc)
	}
	return p, nil
}

// Close closes every connection.
func (p *Pool) Close() {
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func (p *Pool) client() seatsafev1.BookingServiceClient {
	return seatsafev1.NewBookingServiceClient(p.conns[p.next.Add(1)%uint64(len(p.conns))])
}

// Book makes one BookSeats call on the next connection.
func (p *Pool) Book(ctx context.Context, req *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
	return p.client().BookSeats(ctx, req)
}

// List reads one section's free seats.
func (p *Pool) List(ctx context.Context, s Section) ([]string, error) {
	r, err := p.client().ListAvailableSeats(ctx, &seatsafev1.ListAvailableSeatsRequest{EventId: s.Event, SectionId: s.ID})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, sec := range r.GetSections() {
		out = append(out, sec.GetAvailableSeatIds()...)
	}
	return out, nil
}

// NewRequestID returns a random UUID (version 4). One per order, reused on
// every retry of that order.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("loadgen: no randomness: %v", err))
	}
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// Result is what happened to one order.
type Result struct {
	Order     Order
	RequestID string
	Outcome   string
	Replayed  bool
	Retries   int
	Start     time.Time // first call
	Return    time.Time // last call returned
}

// Client books orders with the agreed retry rules.
type Client struct {
	Call        BookFunc
	CallTimeout time.Duration
	RetryBudget time.Duration
	History     *HistoryWriter

	sleep func(time.Duration) // replaced in tests
}

func outcomeOf(o seatsafev1.BookingOutcome) string {
	switch o {
	case seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED:
		return OutcomeConfirmed
	case seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE:
		return OutcomeUnavailable
	case seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED:
		return OutcomeAborted
	case seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID:
		return OutcomeInvalid
	}
	return OutcomeUnknown
}

// Book runs one order to its final outcome. UNAVAILABLE and DEADLINE_EXCEEDED
// are retried with the same request_id until RetryBudget, counted from the
// first call, runs out; a call never outlives the budget. INVALID_ARGUMENT is
// final. Any other code leaves the order UNKNOWN: there is no rule that says
// retrying it is safe or that the order failed.
func (c *Client) Book(clientID uint64, o Order) Result {
	sleep := c.sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	req := &seatsafev1.BookSeatsRequest{RequestId: NewRequestID(), UserId: fmt.Sprintf("user-%d", clientID)}
	for _, s := range o.Seats {
		req.Seats = append(req.Seats, &seatsafev1.SeatRef{EventId: s.Event, SectionId: s.Section, SeatId: s.ID})
	}
	res := Result{Order: o, RequestID: req.RequestId, Outcome: OutcomeUnknown, Start: time.Now()}
	deadline := res.Start.Add(c.RetryBudget)
	for attempt := 0; ; attempt++ {
		timeout := min(c.CallTimeout, time.Until(deadline))
		if timeout <= 0 {
			return res
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		callAt := time.Now()
		resp, err := c.Call(ctx, req)
		res.Return = time.Now()
		cancel()
		code := status.Code(err)
		if c.History != nil {
			c.History.Write(clientID, req.RequestId, o.Seats, callAt, res.Return, resp, code)
		}
		switch code {
		case codes.OK:
			res.Outcome, res.Replayed = outcomeOf(resp.GetOutcome()), resp.GetReplayed()
			return res
		case codes.InvalidArgument:
			res.Outcome = OutcomeInvalid
			return res
		case codes.Unavailable, codes.DeadlineExceeded:
		default:
			return res
		}
		wait := Backoff[min(attempt, len(Backoff)-1)]
		if time.Now().Add(wait).After(deadline) {
			return res
		}
		sleep(wait)
		res.Retries++
	}
}
