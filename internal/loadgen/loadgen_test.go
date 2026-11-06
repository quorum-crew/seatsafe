package loadgen

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/router"
)

var testDataset = fakecluster.Dataset{
	Shards: 3, FlashSections: 30, FlashRows: 20, FlashSeatsPerRow: 50,
	Events: 2, Sections: 3, Rows: 4, SeatsPerRow: 5, ChunkSize: 500,
}

func testUniverse(t *testing.T) *Universe {
	t.Helper()
	sm, seeds, err := testDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	u, err := NewUniverse(sm, seeds)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func testConfig(t *testing.T, workload string) Config {
	t.Helper()
	c := Defaults()
	c.Workload, c.Warmup, c.Quiet, c.Out = workload, 0, true, t.TempDir()
	return c
}

func freeAll(u *Universe) map[Seat]bool {
	free := map[Seat]bool{}
	for _, r := range u.Rows {
		for _, s := range r.Seats {
			free[Seat{r.Event, r.Section, s}] = true
		}
	}
	return free
}

// stubRun is a run whose calls go to book, with a series that writes nowhere
// that matters.
func stubRun(t *testing.T, c Config, book BookFunc) *run {
	t.Helper()
	u := testUniverse(t)
	w, err := NewWorkload(c, u, freeAll(u))
	if err != nil {
		t.Fatal(err)
	}
	s, err := newSeries(filepath.Join(t.TempDir(), "ts.csv"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.f.Close() })
	return newRun(c, w, &Client{Call: book, CallTimeout: c.CallTimeout, RetryBudget: c.RetryBudget}, s)
}

func confirm(context.Context, *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
	return &seatsafev1.BookSeatsResponse{Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED}, nil
}

// The arrival rate must not depend on the responses: a cluster that takes
// 300 ms per call still sees every order at its intended time.
func TestSchedulerKeepsRateWhenResponsesAreSlow(t *testing.T) {
	c := testConfig(t, WorkloadUniform)
	var calls atomic.Int64
	r := stubRun(t, c, func(ctx context.Context, q *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		return confirm(ctx, q)
	})
	const rate, secs = 2000.0, 0.2
	start := time.Now()
	rec := NewRecorder(start, start.Add(time.Duration(secs*float64(time.Second))))
	r.addSink(rec)
	r.schedule(context.Background(), rate, start, rec.to)
	sentBy := time.Since(start)
	if n := calls.Load(); n < 350 {
		t.Fatalf("only %d of 400 orders were sent by %v: the scheduler waited for responses", n, sentBy)
	}
	if sentBy >= 300*time.Millisecond {
		t.Fatalf("scheduling took %v: it waited for responses", sentBy)
	}
	r.wg.Wait()
	res := rec.Results()
	if res.Attempted != 400 || res.Dropped != 0 {
		t.Fatalf("attempted %d, dropped %d, want 400 and 0", res.Attempted, res.Dropped)
	}
	if res.Latency.P50 < 300 {
		t.Fatalf("p50 %.1fms, below the 300ms every call takes", res.Latency.P50)
	}
}

// A stalled scheduler shows up as latency: an order is measured from when it
// was meant to go out, not from when it did.
func TestLatencyIsMeasuredFromIntendedTime(t *testing.T) {
	c := testConfig(t, WorkloadUniform)
	r := stubRun(t, c, confirm)
	const stall = 200 * time.Millisecond
	stalled := false
	r.sleepUntil = func(until time.Time) {
		if !stalled {
			stalled = true
			time.Sleep(stall)
		}
		time.Sleep(time.Until(until))
	}
	start := time.Now()
	rec := NewRecorder(start, start.Add(100*time.Millisecond))
	r.addSink(rec)
	r.schedule(context.Background(), 1000, start, rec.to)
	r.wg.Wait()
	res := rec.Results()
	if res.Attempted != 100 {
		t.Fatalf("attempted %d, want 100", res.Attempted)
	}
	if res.Latency.P50 < 100 || res.Latency.Max < float64(stall/time.Millisecond)-20 {
		t.Fatalf("p50 %.1fms, max %.1fms: the stall is missing from latency", res.Latency.P50, res.Latency.Max)
	}
}

// Hitting -max-inflight drops orders and counts them; it never slows down.
func TestMaxInflightDropsAndCounts(t *testing.T) {
	c := testConfig(t, WorkloadUniform)
	c.MaxInflight = 10
	release := make(chan struct{})
	r := stubRun(t, c, func(ctx context.Context, q *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
		<-release
		return confirm(ctx, q)
	})
	start := time.Now()
	rec := NewRecorder(start, start.Add(100*time.Millisecond))
	r.addSink(rec)
	r.schedule(context.Background(), 1000, start, rec.to)
	close(release)
	r.wg.Wait()
	res := rec.Results()
	if res.Attempted != 100 || res.Dropped != 90 {
		t.Fatalf("attempted %d, dropped %d, want 100 and 90", res.Attempted, res.Dropped)
	}
}

func TestParseOrderSizes(t *testing.T) {
	for _, bad := range []string{"", "1", "x:1", "1:x", "0:5", "1:0", "1:70,"} {
		if _, err := ParseOrderSizes(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	o, err := ParseOrderSizes("1:70, 2:20,4:10")
	if err != nil || o.total != 100 || o.largest != 4 {
		t.Fatalf("%+v, %v", o, err)
	}
}

func TestOrderSizeDistribution(t *testing.T) {
	c := testConfig(t, WorkloadUniform)
	c.OrderSizes = "1:70,2:20,4:10"
	u := testUniverse(t)
	w, err := NewWorkload(c, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	k := w.Worker(1)
	counts := map[int]int{}
	const n = 100000
	for range n {
		o, _ := k.Next()
		// Rows of the small events have 5 seats, so every size fits.
		counts[len(o.Seats)]++
		for _, s := range o.Seats[1:] {
			if s.Event != o.Seats[0].Event || s.Section != o.Seats[0].Section || strings.Split(s.ID, "-")[0] != strings.Split(o.Seats[0].ID, "-")[0] {
				t.Fatalf("seats not in one row: %v", o.Seats)
			}
		}
		if adjacent := wantAdjacent(u, o); !adjacent {
			t.Fatalf("seats not adjacent: %v", o.Seats)
		}
	}
	for size, want := range map[int]float64{1: .7, 2: .2, 4: .1} {
		if got := float64(counts[size]) / n; got < want-.01 || got > want+.01 {
			t.Errorf("size %d: %.3f of orders, want %.2f", size, got, want)
		}
	}
	if len(counts) != 3 {
		t.Errorf("sizes %v", counts)
	}
}

func wantAdjacent(u *Universe, o Order) bool {
	for _, r := range u.Rows {
		if r.Event != o.Seats[0].Event || r.Section != o.Seats[0].Section {
			continue
		}
		for i, s := range r.Seats {
			if s == o.Seats[0].ID {
				for j, x := range o.Seats {
					if i+j >= len(r.Seats) || r.Seats[i+j] != x.ID {
						return false
					}
				}
				return true
			}
		}
	}
	return false
}

// Zipf: a few popular sections and rows take most orders, and low ranks beat
// high ranks.
func TestZipfSkew(t *testing.T) {
	c := testConfig(t, WorkloadFlash)
	c.OrderSizes = "1:1"
	u := testUniverse(t)
	w, err := NewWorkload(c, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	k := w.Worker(1)
	bySection := map[string]int{}
	byRow := map[string]int{}
	const n = 100000
	for range n {
		o, _ := k.Next()
		s := o.Seats[0]
		if s.Event != FlashEvent {
			t.Fatalf("flash order for %v", s)
		}
		bySection[s.Section]++
		byRow[strings.Split(s.ID, "-")[0]]++
	}
	if len(bySection) < 15 {
		t.Errorf("only %d of 30 sections were hit", len(bySection))
	}
	if top := float64(bySection["sec-101"]) / n; top < .25 {
		t.Errorf("the top section got %.3f of orders", top)
	}
	if bySection["sec-101"] <= bySection["sec-102"] || bySection["sec-102"] <= bySection["sec-105"] || bySection["sec-105"] <= bySection["sec-120"] {
		t.Errorf("not skewed: %v", bySection)
	}
	if byRow["R01"] <= byRow["R02"] || byRow["R02"] <= byRow["R10"] {
		t.Errorf("rows not skewed: %v", byRow)
	}
}

// Fresh never offers a seat twice, only offers free seats, and ends.
func TestFreshOffersEverySeatOnce(t *testing.T) {
	c := testConfig(t, WorkloadFresh)
	u := testUniverse(t)
	free := freeAll(u)
	taken := Seat{FlashEvent, "sec-101", "R01-S005"}
	delete(free, taken)
	w, err := NewWorkload(c, u, free)
	if err != nil {
		t.Fatal(err)
	}
	k := w.Worker(1)
	seen := map[Seat]bool{}
	for {
		o, ok := k.Next()
		if !ok {
			break
		}
		for _, s := range o.Seats {
			if seen[s] || !free[s] {
				t.Fatalf("seat %v offered again or not free", s)
			}
			seen[s] = true
		}
		if !wantAdjacent(u, o) {
			t.Fatalf("not adjacent: %v", o.Seats)
		}
	}
	if len(seen) != len(free) {
		t.Fatalf("offered %d of %d free seats", len(seen), len(free))
	}
}

func TestCrossShardOrders(t *testing.T) {
	u := testUniverse(t)
	for _, workload := range []string{WorkloadUniform, WorkloadFlash, WorkloadFresh} {
		c := testConfig(t, workload)
		c.CrossShardFrac = 1
		w, err := NewWorkload(c, u, freeAll(u))
		if err != nil {
			t.Fatal(err)
		}
		k := w.Worker(1)
		cross := 0
		for range 2000 {
			o, ok := k.Next()
			if !ok {
				t.Fatal("ran out of seats")
			}
			if o.Cross {
				cross++
				if len(o.Seats) < 2 || o.Seats[0].Section == o.Seats[len(o.Seats)-1].Section || o.Seats[0].Event != o.Seats[len(o.Seats)-1].Event {
					t.Fatalf("%s: bad cross order %v", workload, o.Seats)
				}
			}
		}
		if cross < 1900 {
			t.Errorf("%s: %d of 2000 orders crossed shards", workload, cross)
		}
	}
}

func TestRetriesKeepRequestID(t *testing.T) {
	var ids []string
	var mu sync.Mutex
	codesToReturn := []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Unavailable, codes.OK}
	call := func(ctx context.Context, q *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		i := len(ids)
		ids = append(ids, q.RequestId)
		if c := codesToReturn[i]; c != codes.OK {
			return nil, status.Error(c, "no")
		}
		return &seatsafev1.BookSeatsResponse{Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED, Replayed: true}, nil
	}
	var waits []time.Duration
	c := &Client{Call: call, CallTimeout: time.Second, RetryBudget: 5 * time.Second, sleep: func(d time.Duration) { waits = append(waits, d) }}
	res := c.Book(1, Order{Seats: []Seat{{"e", "s", "a"}}})
	if res.Outcome != OutcomeConfirmed || !res.Replayed || res.Retries != 3 {
		t.Fatalf("%+v", res)
	}
	for _, id := range ids {
		if id != res.RequestID {
			t.Fatalf("request ids changed: %v", ids)
		}
	}
	if len(ids) != 4 || waits[0] != 10*time.Millisecond || waits[1] != 25*time.Millisecond || waits[2] != 50*time.Millisecond {
		t.Fatalf("ids %v, waits %v", ids, waits)
	}
}

func TestBackoffSchedule(t *testing.T) {
	want := []time.Duration{10, 25, 50, 100, 200, 200, 200}
	var got []time.Duration
	c := &Client{CallTimeout: time.Second, RetryBudget: time.Hour}
	calls := 0
	c.Call = func(context.Context, *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
		calls++
		if calls > len(want) {
			return confirm(nil, nil)
		}
		return nil, status.Error(codes.Unavailable, "no")
	}
	c.sleep = func(d time.Duration) { got = append(got, d) }
	c.Book(1, Order{})
	for i := range want {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("waits %v", got)
		}
	}
}

func TestFinalOutcomes(t *testing.T) {
	counting := func(n *int, err error) BookFunc {
		return func(context.Context, *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
			*n++
			return nil, err
		}
	}
	noWait := func(time.Duration) {}

	var n int
	c := &Client{Call: counting(&n, status.Error(codes.InvalidArgument, "no")), CallTimeout: time.Second, RetryBudget: time.Second, sleep: noWait}
	if res := c.Book(1, Order{}); res.Outcome != OutcomeInvalid || n != 1 {
		t.Fatalf("INVALID_ARGUMENT: %+v after %d calls", res, n)
	}

	n = 0
	c = &Client{Call: counting(&n, status.Error(codes.Internal, "no")), CallTimeout: time.Second, RetryBudget: time.Second, sleep: noWait}
	if res := c.Book(1, Order{}); res.Outcome != OutcomeUnknown || n != 1 {
		t.Fatalf("INTERNAL: %+v after %d calls", res, n)
	}

	n = 0
	c = &Client{Call: counting(&n, status.Error(codes.Unavailable, "no")), CallTimeout: time.Second, RetryBudget: 30 * time.Millisecond}
	if res := c.Book(1, Order{}); res.Outcome != OutcomeUnknown || n < 2 {
		t.Fatalf("budget: %+v after %d calls", res, n)
	}

	for proto, want := range map[seatsafev1.BookingOutcome]string{
		seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE: OutcomeUnavailable,
		seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED:          OutcomeAborted,
		seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID:          OutcomeInvalid,
		seatsafev1.BookingOutcome_BOOKING_OUTCOME_UNSPECIFIED:      OutcomeUnknown,
	} {
		c = &Client{CallTimeout: time.Second, RetryBudget: time.Second, Call: func(context.Context, *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
			return &seatsafev1.BookSeatsResponse{Outcome: proto}, nil
		}}
		if res := c.Book(1, Order{}); res.Outcome != want {
			t.Errorf("%v: got %s", proto, res.Outcome)
		}
	}
}

func TestNewRequestID(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if a == b || len(a) != 36 || a[14] != '4' || a[8] != '-' {
		t.Fatalf("%q %q", a, b)
	}
}

func TestAudit(t *testing.T) {
	a, b, c, d := Seat{"e", "s", "a"}, Seat{"e", "s", "b"}, Seat{"e", "s", "c"}, Seat{"e", "s", "d"}
	all := map[Seat]bool{a: true, b: true, c: true, d: true}

	clean := RunAudit(all, map[Seat]bool{d: true}, []AuditOrder{
		{"r1", []Seat{a}, OutcomeConfirmed},
		{"r2", []Seat{a}, OutcomeUnavailable},
		{"r3", []Seat{b}, OutcomeUnknown},
		{"r4", []Seat{c}, OutcomeConfirmed},
	})
	if !clean.Pass || clean.SeatsTaken != 3 || clean.ConfirmedSeats != 2 || clean.UnknownOrders != 1 {
		t.Fatalf("%+v", clean)
	}

	double := RunAudit(all, map[Seat]bool{b: true, c: true, d: true}, []AuditOrder{
		{"r1", []Seat{a}, OutcomeConfirmed},
		{"r2", []Seat{a}, OutcomeConfirmed},
	})
	if double.Pass || double.DoubleBooked != 1 || len(double.Samples) != 1 {
		t.Fatalf("double booking not caught: %+v", double)
	}

	free := RunAudit(all, all, []AuditOrder{{"r1", []Seat{a}, OutcomeConfirmed}})
	if free.Pass || free.StillFree != 1 {
		t.Fatalf("confirmed but free not caught: %+v", free)
	}

	gone := RunAudit(all, map[Seat]bool{b: true, c: true, d: true}, []AuditOrder{{"r1", []Seat{a}, OutcomeUnavailable}})
	if gone.Pass || gone.Unexplained != 1 {
		t.Fatalf("unexplained seat not caught: %+v", gone)
	}

	// Seat a was already booked before the run (not in before), and the
	// cluster CONFIRMs it again. It is not free after, and rule c only looks
	// at seats free before, so only rule d can catch it.
	preBooked := map[Seat]bool{b: true, c: true, d: true}
	again := RunAudit(preBooked, preBooked, []AuditOrder{{"r1", []Seat{a}, OutcomeConfirmed}})
	if again.Pass || again.PreBooked != 1 || again.DoubleBooked != 0 || again.StillFree != 0 || again.Unexplained != 0 || len(again.Samples) != 1 {
		t.Fatalf("confirmation of a pre-booked seat not caught: %+v", again)
	}
}

func TestAvailability(t *testing.T) {
	failures := 2
	var mu sync.Mutex
	list := func(_ context.Context, s Section) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		if failures > 0 {
			failures--
			return nil, status.Error(codes.Unavailable, "no")
		}
		return []string{s.ID + "-x"}, nil
	}
	got, err := Availability(context.Background(), list, []Section{{"e", "a"}, {"e", "b"}})
	if err != nil || !got[Seat{"e", "a", "a-x"}] || !got[Seat{"e", "b", "b-x"}] {
		t.Fatalf("%v %v", got, err)
	}
}

func attempt(req string, seat string, call, ret int64, outcome string) Attempt {
	return Attempt{RequestID: req, Seats: []string{seat}, CallNs: call, ReturnNs: ret, Outcome: outcome}
}

func TestCheckHistory(t *testing.T) {
	const c, u = OutcomeConfirmed, OutcomeUnavailable
	clean := []Attempt{
		attempt("r1", "s", 0, 10, ""), // timed out,
		attempt("r1", "s", 12, 20, c), // then confirmed on the retry,
		attempt("r1", "s", 22, 25, c), // and replayed
		attempt("r2", "s", 30, 40, u), // a later order is refused
		attempt("r3", "t", 5, 8, c),
	}
	for _, check := range Checks {
		if v := check.Run(clean); len(v) != 0 {
			t.Errorf("%s: %v", check.Name, v)
		}
	}

	overlapping := []Attempt{attempt("r1", "s", 0, 10, c), attempt("r2", "s", 5, 15, c)}
	if v := CheckNoDoubleBooking(overlapping); len(v) != 1 || v[0].Seat != "s" {
		t.Fatalf("double booking: %v", v)
	}
	if v := CheckRealTime(overlapping); len(v) != 0 {
		t.Fatalf("overlapping calls are not a real-time violation: %v", v)
	}

	ordered := []Attempt{attempt("r1", "s", 0, 10, c), attempt("r2", "s", 11, 15, c)}
	if v := CheckRealTime(ordered); len(v) != 1 {
		t.Fatalf("real time: %v", v)
	}

	// The retry confirmed late, but the order was called before r1 returned.
	retried := []Attempt{attempt("r1", "s", 0, 10, c), attempt("r2", "s", 5, 7, ""), attempt("r2", "s", 11, 15, c)}
	if v := CheckRealTime(retried); len(v) != 0 {
		t.Fatalf("real time uses the first call: %v", v)
	}
}

func TestHistoryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.jsonl")
	base := time.Now()
	h, err := NewHistoryWriter(path, base)
	if err != nil {
		t.Fatal(err)
	}
	h.Write(3, "r1", []Seat{{"e", "s", "a"}}, base.Add(time.Millisecond), base.Add(2*time.Millisecond),
		&seatsafev1.BookSeatsResponse{Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED, Replayed: true}, codes.OK)
	h.Write(3, "r2", nil, base, base, nil, codes.Unavailable)
	if n, err := h.Close(); n != 2 || err != nil {
		t.Fatalf("%d %v", n, err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := ReadHistory(f)
	if err != nil || len(got) != 2 {
		t.Fatalf("%v %v", got, err)
	}
	if a := got[0]; a.ClientID != 3 || a.Seats[0] != "e/s/a" || a.CallNs != 1e6 || a.ReturnNs != 2e6 || a.Outcome != OutcomeConfirmed || !a.Replayed || a.Code != "OK" {
		t.Fatalf("%+v", a)
	}
	if got[1].Outcome != "" || got[1].Code != "Unavailable" {
		t.Fatalf("%+v", got[1])
	}
	if _, err := ReadHistory(strings.NewReader("{bad")); err == nil {
		t.Fatal("bad history accepted")
	}
}

func TestValidate(t *testing.T) {
	mod := func(f func(*Config)) Config {
		c := Defaults()
		f(&c)
		return c
	}
	if err := Defaults().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]Config{
		"mode":     mod(func(c *Config) { c.Mode = "x" }),
		"workload": mod(func(c *Config) { c.Workload = "x" }),
		"rate":     mod(func(c *Config) { c.Rate = 0 }),
		"users":    mod(func(c *Config) { c.Mode, c.Users = ModeClosed, 0 }),
		"ramp":     mod(func(c *Config) { c.Mode, c.RateStep = ModeRamp, 0 }),
		"zipf":     mod(func(c *Config) { c.Workload, c.ZipfS = WorkloadFlash, 1 }),
		"conns":    mod(func(c *Config) { c.Conns = 0 }),
		"cross":    mod(func(c *Config) { c.CrossShardFrac = 2 }),
		"duration": mod(func(c *Config) { c.Duration = 0 }),
		"sizes":    mod(func(c *Config) { c.OrderSizes = "x" }),
	} {
		if c.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// testRouter runs a real router over a fake cluster, on in-memory connections.
func testRouter(t *testing.T) (*Universe, grpc.DialOption) {
	t.Helper()
	sm, seeds, err := testDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := fakecluster.NewBufconn(sm, seeds, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	rt, err := router.New(sm, router.Config{Group: groupclient.Config{DialOptions: cluster.DialOptions()}}, metrics.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	seatsafev1.RegisterBookingServiceServer(gs, rt)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	u, err := NewUniverse(sm, seeds)
	if err != nil {
		t.Fatal(err)
	}
	return u, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
}

func runAgainstFake(t *testing.T, c Config, u *Universe, dial grpc.DialOption) *Summary {
	t.Helper()
	c.Target = "passthrough:///router"
	c.Conns = 4
	c.History = true
	c.DialOptions = []grpc.DialOption{dial, grpc.WithTransportCredentials(insecure.NewCredentials())}
	sum, err := Run(context.Background(), c, u, Env{})
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// 2,000 flash orders against a real router and a fake cluster: real collisions,
// and the audit and the history checker must both agree nothing went wrong.
func TestIntegrationFlash(t *testing.T) {
	u, dial := testRouter(t)
	c := testConfig(t, WorkloadFlash)
	c.Rate, c.Duration = 2000, time.Second
	sum := runAgainstFake(t, c, u, dial)

	r := sum.Results
	if r.Attempted != 2000 || r.Outcomes[OutcomeUnknown] != 0 || r.Dropped != 0 {
		t.Fatalf("%+v", r)
	}
	if r.Outcomes[OutcomeConfirmed] == 0 || r.Outcomes[OutcomeUnavailable] == 0 {
		t.Fatalf("flash orders should both book and collide: %v", r.Outcomes)
	}
	if r.Outcomes[OutcomeConfirmed]+r.Outcomes[OutcomeUnavailable] != 2000 {
		t.Fatalf("outcomes %v", r.Outcomes)
	}
	if !sum.Audit.Pass || sum.Audit.Orders != 2000 {
		t.Fatalf("audit %+v", sum.Audit)
	}
	if sum.Audit.ConfirmedSeats != sum.Audit.SeatsTaken {
		t.Fatalf("%d confirmed seats, %d seats taken", sum.Audit.ConfirmedSeats, sum.Audit.SeatsTaken)
	}

	f, err := os.Open(filepath.Join(c.Out, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	history, err := ReadHistory(f)
	if err != nil || int64(len(history)) != sum.HistoryAttempts || len(history) < 2000 {
		t.Fatalf("%d attempts of %d: %v", len(history), sum.HistoryAttempts, err)
	}
	for _, check := range Checks {
		if v := check.Run(history); len(v) != 0 {
			t.Errorf("%s: %v", check.Name, v[0])
		}
	}

	b, err := os.ReadFile(filepath.Join(c.Out, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var back Summary
	if err := json.Unmarshal(b, &back); err != nil || back.Results.Attempted != 2000 || back.Audit == nil {
		t.Fatalf("summary.json: %v %+v", err, back.Results)
	}
	if b, err := os.ReadFile(filepath.Join(c.Out, "timeseries.csv")); err != nil || !strings.HasPrefix(string(b), "second,sent,") {
		t.Fatalf("timeseries.csv: %v %.40q", err, b)
	}
	if !strings.Contains(PrintSummary(sum), "audit PASS") {
		t.Fatalf("no audit line in:\n%s", PrintSummary(sum))
	}
}

func TestIntegrationFreshConfirmsEverything(t *testing.T) {
	u, dial := testRouter(t)
	c := testConfig(t, WorkloadFresh)
	c.Rate, c.Duration = 1000, 500*time.Millisecond
	sum := runAgainstFake(t, c, u, dial)
	if got := sum.Results.Outcomes[OutcomeConfirmed]; got != 500 || !sum.Audit.Pass {
		t.Fatalf("%v %+v", sum.Results.Outcomes, sum.Audit)
	}
}

func TestIntegrationClosed(t *testing.T) {
	u, dial := testRouter(t)
	c := testConfig(t, WorkloadFlash)
	c.Mode, c.Users, c.Duration, c.Think = ModeClosed, 20, 300*time.Millisecond, time.Millisecond
	sum := runAgainstFake(t, c, u, dial)
	if sum.Results.Attempted == 0 || sum.Results.AchievedRate == 0 || !sum.Audit.Pass {
		t.Fatalf("%+v %+v", sum.Results, sum.Audit)
	}
}

func TestIntegrationRamp(t *testing.T) {
	u, dial := testRouter(t)
	c := testConfig(t, WorkloadFresh)
	c.Mode, c.RateStart, c.RateEnd, c.RateStep, c.StepDuration = ModeRamp, 200, 400, 200, 200*time.Millisecond
	c.Warmup = 50 * time.Millisecond
	sum := runAgainstFake(t, c, u, dial)
	if len(sum.Steps) != 2 || sum.MaxSustainable != 400 || !sum.Audit.Pass {
		t.Fatalf("%+v max %v audit %+v", sum.Steps, sum.MaxSustainable, sum.Audit)
	}
	if !strings.Contains(PrintSummary(sum), "max sustainable rate: 400/s") {
		t.Fatalf("%s", PrintSummary(sum))
	}
}

// A step that misses the SLO ends the ramp and the last good step is the answer.
func TestRampStopsAtSLO(t *testing.T) {
	c := testConfig(t, WorkloadUniform)
	c.Mode, c.RateStart, c.RateEnd, c.RateStep, c.StepDuration = ModeRamp, 100, 500, 100, 100*time.Millisecond
	c.SLO = 50 * time.Millisecond
	var calls atomic.Int64
	r := stubRun(t, c, func(ctx context.Context, q *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
		if calls.Add(1) > 12 {
			time.Sleep(80 * time.Millisecond)
		}
		return confirm(ctx, q)
	})
	r.series.start()
	defer r.series.stop()
	_, steps, best, why := r.ramp(context.Background())
	if len(steps) != 2 || best != 100 || steps[1].Passed || !strings.Contains(why, "SLO") {
		t.Fatalf("steps %+v best %v why %q", steps, best, why)
	}
}
