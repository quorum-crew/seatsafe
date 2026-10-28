package router_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/router"
)

const (
	confirmed   = seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED
	unavailable = seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
	invalid     = seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID
)

// env is the real router over bufconn in front of a fake cluster.
type env struct {
	t       testing.TB
	cluster *fakecluster.Cluster
	router  *router.Router
	client  seatsafev1.BookingServiceClient
}

func newEnv(t testing.TB, ds fakecluster.Dataset) *env {
	t.Helper()
	sm, seeds, err := ds.Build()
	if err != nil {
		t.Fatal(err)
	}
	c, err := fakecluster.NewBufconn(sm, seeds, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	rt, err := router.New(sm, router.Config{Group: groupclient.Config{DialOptions: c.DialOptions()}}, metrics.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	seatsafev1.RegisterBookingServiceServer(s, rt)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	cc, err := grpc.NewClient("passthrough:///router",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return &env{t: t, cluster: c, router: rt, client: seatsafev1.NewBookingServiceClient(cc)}
}

// Seats of SmallDataset's flash event. Sections go round-robin from shard 1:
// sec-101 and sec-104 on shard 1, sec-102 and sec-105 on 2, sec-103 and sec-106 on 3.
func flash(section string, row, n int) *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: "evt-flash", SectionId: section, SeatId: fmt.Sprintf("R%02d-S%03d", row, n)}
}

func shardOf(section string) uint32 {
	return map[string]uint32{"sec-101": 1, "sec-104": 1, "sec-102": 2, "sec-105": 2, "sec-103": 3, "sec-106": 3}[section]
}

// book sends BookSeats with the client's 2s deadline.
func (e *env) book(req, user string, seats ...*seatsafev1.SeatRef) (*seatsafev1.BookSeatsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return e.client.BookSeats(ctx, &seatsafev1.BookSeatsRequest{RequestId: req, UserId: user, Seats: seats})
}

func (e *env) mustBook(req, user string, seats ...*seatsafev1.SeatRef) *seatsafev1.BookSeatsResponse {
	e.t.Helper()
	resp, err := e.book(req, user, seats...)
	if err != nil {
		e.t.Fatalf("BookSeats(%s): %v", req, err)
	}
	return resp
}

func (e *env) available(section string) map[string]bool {
	e.t.Helper()
	out := map[string]bool{}
	for _, s := range e.cluster.Shards[shardOf(section)].Machine.ListAvailable("evt-flash", section).Sections {
		for _, id := range s.AvailableSeatIds {
			out[id] = true
		}
	}
	return out
}

func (e *env) checkMachines() {
	e.t.Helper()
	for id, g := range e.cluster.Shards {
		if err := g.Machine.CheckInvariants(); err != nil {
			e.t.Fatalf("shard %d: %v", id, err)
		}
		if held := g.Machine.ExpiredHolds(math.MaxInt64); len(held) != 0 {
			e.t.Fatalf("shard %d still holds seats for %v", id, held)
		}
	}
}

func wantOutcome(t *testing.T, resp *seatsafev1.BookSeatsResponse, outcome seatsafev1.BookingOutcome, cross, replayed bool, unavail ...*seatsafev1.SeatRef) {
	t.Helper()
	if resp.Outcome != outcome || resp.CrossShard != cross || resp.Replayed != replayed {
		t.Fatalf("response %v, want outcome %v cross %v replayed %v", resp, outcome, cross, replayed)
	}
	if len(resp.UnavailableSeats) != len(unavail) {
		t.Fatalf("unavailable %v, want %v", resp.UnavailableSeats, unavail)
	}
	for i := range unavail {
		if !proto.Equal(resp.UnavailableSeats[i], unavail[i]) {
			t.Fatalf("unavailable %v, want %v", resp.UnavailableSeats, unavail)
		}
	}
}

func TestSingleShardBooking(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	seat := flash("sec-102", 1, 1)
	first := e.mustBook("req-a", "alice", seat)
	wantOutcome(t, first, confirmed, false, false)
	if first.OrderId != "req-a" {
		t.Fatalf("order_id %q", first.OrderId)
	}
	wantOutcome(t, e.mustBook("req-b", "bob", seat), unavailable, false, false, seat)
	wantOutcome(t, e.mustBook("req-a", "alice", seat), confirmed, false, true)
	if e.available("sec-102")["R01-S001"] {
		t.Fatal("booked seat still listed as available")
	}
	if e.router.Metrics.Requests.Value("BookSeats", "CONFIRMED", "single") != 2 ||
		e.router.Metrics.Duration.Count("BookSeats", "SEAT_UNAVAILABLE", "single") != 1 {
		t.Fatal("request metrics not recorded")
	}
	e.checkMachines()
}

func TestCrossShardBooking(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	a, b := flash("sec-101", 1, 1), flash("sec-102", 1, 1)
	wantOutcome(t, e.mustBook("x1", "alice", a, b), confirmed, true, false)
	wantOutcome(t, e.mustBook("x1", "alice", a, b), confirmed, true, true)

	// sec-103 R01-S002 is taken by a single-shard order; a cross-shard order
	// that also wants sec-104 R01-S002 must get SEAT_UNAVAILABLE and leave
	// nothing held on shard 1.
	taken, free := flash("sec-103", 1, 2), flash("sec-104", 1, 2)
	wantOutcome(t, e.mustBook("s1", "bob", taken), confirmed, false, false)
	wantOutcome(t, e.mustBook("x2", "carol", free, taken), unavailable, true, false, taken)
	if !e.available("sec-104")["R01-S002"] {
		t.Fatal("seat on the other shard was not released")
	}
	// Three shards at once.
	wantOutcome(t, e.mustBook("x3", "dan", flash("sec-106", 2, 1), flash("sec-101", 2, 1), flash("sec-105", 2, 1)), confirmed, true, false)
	if e.router.Metrics.Requests.Value("BookSeats", "CONFIRMED", "cross") != 3 {
		t.Fatal("cross-shard metrics not recorded")
	}
	e.checkMachines()
}

func TestUnknownSectionAndValidation(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	ok := flash("sec-101", 1, 1)
	unknown := flash("sec-999", 1, 1)
	wantOutcome(t, e.mustBook("u1", "alice", ok, unknown), invalid, false, false)
	if !e.available("sec-101")["R01-S001"] {
		t.Fatal("an INVALID order booked a seat")
	}
	other := &seatsafev1.SeatRef{EventId: "evt-001", SectionId: "sec-101", SeatId: "R01-S001"}
	for name, req := range map[string]*seatsafev1.BookSeatsRequest{
		"empty request_id": {UserId: "u", Seats: []*seatsafev1.SeatRef{ok}},
		"empty seats":      {RequestId: "r", UserId: "u"},
		"mixed events":     {RequestId: "r", UserId: "u", Seats: []*seatsafev1.SeatRef{ok, other}},
		"duplicate seats":  {RequestId: "r", UserId: "u", Seats: []*seatsafev1.SeatRef{ok, flash("sec-101", 1, 1)}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := e.client.BookSeats(ctx, req)
		cancel()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want INVALID_ARGUMENT", name, err)
		}
	}
	if e.router.Metrics.Requests.Value("BookSeats", "INVALID_ARGUMENT", "none") != 4 {
		t.Fatal("validation failures not counted")
	}
}

func TestLeaderMoveFollowsTrailerOnce(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	gm := e.router.GroupMetrics
	wantOutcome(t, e.mustBook("w1", "alice", flash("sec-101", 1, 1)), confirmed, false, false) // warms the leader cache

	e.cluster.Shards[1].SetLeader(1)
	before := gm.Retries.Value("shard-1", groupclient.ReasonRedirect)
	wantOutcome(t, e.mustBook("w2", "alice", flash("sec-101", 1, 2)), confirmed, false, false)
	if got := gm.Retries.Value("shard-1", groupclient.ReasonRedirect) - before; got != 1 {
		t.Fatalf("redirects = %v, want exactly 1", got)
	}
	for _, reason := range []string{groupclient.ReasonNotLeader, groupclient.ReasonUnavailable, groupclient.ReasonAttemptTimer} {
		if gm.Retries.Value("shard-1", reason) != 0 {
			t.Fatalf("unexpected %s retry", reason)
		}
	}
	if gm.LeaderChanges.Value("shard-1") != 1 {
		t.Fatalf("leader changes = %v", gm.LeaderChanges.Value("shard-1"))
	}
	// The coordinator group moves too.
	wantOutcome(t, e.mustBook("w3", "alice", flash("sec-101", 1, 3), flash("sec-102", 1, 3)), confirmed, true, false)
	e.cluster.Coordinator.SetLeader(2)
	wantOutcome(t, e.mustBook("w4", "alice", flash("sec-101", 1, 4), flash("sec-102", 1, 4)), confirmed, true, false)
	if gm.Retries.Value("coordinator", groupclient.ReasonRedirect) != 1 {
		t.Fatal("coordinator redirect not followed")
	}
}

func TestOutageThenNewLeader(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	gm := e.router.GroupMetrics
	wantOutcome(t, e.mustBook("o1", "alice", flash("sec-102", 1, 1)), confirmed, false, false)

	g := e.cluster.Shards[2]
	g.SetNoLeader()
	type result struct {
		resp *seatsafev1.BookSeatsResponse
		err  error
		took time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		resp, err := e.book("o2", "bob", flash("sec-102", 1, 2))
		done <- result{resp, err, time.Since(start)}
	}()
	time.Sleep(300 * time.Millisecond)
	g.SetLeader(2)
	r := <-done
	if r.err != nil {
		t.Fatalf("booking during outage: %v", r.err)
	}
	wantOutcome(t, r.resp, confirmed, false, false)
	if r.took < 300*time.Millisecond || r.took > 2*time.Second {
		t.Fatalf("took %v", r.took)
	}
	if n := gm.LeaderRecovery.Count("shard-2"); n != 1 {
		t.Fatalf("leader_recovery_seconds count = %d", n)
	}
	if s := gm.LeaderRecovery.Sum("shard-2"); s < 0.3 || s > 1 {
		t.Fatalf("leader_recovery_seconds = %.3fs, want about 0.3s", s)
	}
	t.Logf("booking took %v, recovery %.3fs", r.took, gm.LeaderRecovery.Sum("shard-2"))
	if gm.Retries.Value("shard-2", groupclient.ReasonUnavailable) == 0 || gm.LeaderChanges.Value("shard-2") != 1 {
		t.Fatal("outage retries or leader change not counted")
	}
}

func TestLongOutageDeadlineExceeded(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	e.cluster.Shards[3].SetNoLeader()
	start := time.Now()
	_, err := e.book("d1", "alice", flash("sec-103", 1, 1))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("got %v, want DEADLINE_EXCEEDED", err)
	}
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Fatalf("took %v", d)
	}
	// The router never answered with a guess, and the order did not happen.
	if !e.available("sec-103")["R01-S001"] {
		t.Fatal("seat booked although no leader existed")
	}
}

func TestRouterDefaultDeadline(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	e.cluster.Shards[3].SetNoLeader()
	// No client deadline: the router applies DefaultDeadline itself.
	start := time.Now()
	_, err := e.client.BookSeats(context.Background(), &seatsafev1.BookSeatsRequest{RequestId: "n1", Seats: []*seatsafev1.SeatRef{flash("sec-103", 1, 1)}})
	if status.Code(err) != codes.DeadlineExceeded || time.Since(start) > 3*time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

func TestListAvailableSeats(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	list := func(event, section string) *seatsafev1.ListAvailableSeatsResponse {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := e.client.ListAvailableSeats(ctx, &seatsafev1.ListAvailableSeatsRequest{EventId: event, SectionId: section})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	all := list("evt-flash", "")
	var ids []string
	for _, s := range all.Sections {
		ids = append(ids, s.SectionId)
		if len(s.AvailableSeatIds) != 50 {
			t.Fatalf("%s has %d seats", s.SectionId, len(s.AvailableSeatIds))
		}
	}
	if !sort.StringsAreSorted(ids) || len(ids) != 6 {
		t.Fatalf("sections %v", ids)
	}

	e.mustBook("l1", "alice", flash("sec-104", 1, 1), flash("sec-105", 1, 1))
	e.mustBook("l2", "alice", flash("sec-104", 1, 2))
	one := list("evt-flash", "sec-104")
	if len(one.Sections) != 1 || len(one.Sections[0].AvailableSeatIds) != 48 || one.Sections[0].AvailableSeatIds[0] != "R01-S003" {
		t.Fatalf("sec-104 = %v", one)
	}
	for _, s := range list("evt-flash", "").Sections {
		want := map[string]int{"sec-104": 48, "sec-105": 49}[s.SectionId]
		if want == 0 {
			want = 50
		}
		if len(s.AvailableSeatIds) != want {
			t.Fatalf("%s has %d seats, want %d", s.SectionId, len(s.AvailableSeatIds), want)
		}
	}
	if len(list("evt-nope", "").Sections) != 0 || len(list("evt-flash", "sec-999").Sections) != 0 {
		t.Fatal("unknown event or section returned sections")
	}
	if len(list("evt-001", "").Sections) != 3 {
		t.Fatal("regular event should have 3 sections")
	}
	if e.router.Metrics.Requests.Value("ListAvailableSeats", "OK", "cross") != 3 || e.router.Metrics.Requests.Value("ListAvailableSeats", "OK", "single") != 1 {
		t.Fatalf("list metrics not recorded: cross %v single %v", e.router.Metrics.Requests.Value("ListAvailableSeats", "OK", "cross"), e.router.Metrics.Requests.Value("ListAvailableSeats", "OK", "single"))
	}

	// Reads may be served by any replica, so they keep working during an election.
	e.cluster.Shards[1].SetNoLeader()
	if got := list("evt-flash", ""); len(got.Sections) != 6 {
		t.Fatalf("read during election: %v", got)
	}
}

func TestCrossShardTimeoutAborts(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	a, b := flash("sec-101", 3, 1), flash("sec-102", 3, 1)
	e.cluster.Shards[2].SetNoLeader()
	start := time.Now()
	resp := e.mustBook("t1", "alice", a, b)
	if resp.Outcome != seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED || !resp.CrossShard || len(resp.UnavailableSeats) != 0 {
		t.Fatalf("got %v, want ABORTED", resp)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("abort took %v", d)
	}
	if !e.available("sec-101")["R03-S001"] {
		t.Fatal("seat held on the shard that voted YES")
	}
	// Once shard 2 elects a leader, the background Abort writes a tombstone
	// there, and the stored answer is replayed.
	g := e.cluster.Shards[2]
	before := g.Machine.AppliedIndex()
	g.SetLeader(1)
	for deadline := time.Now().Add(3 * time.Second); g.Machine.AppliedIndex() == before; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("background Abort never reached shard 2")
		}
	}
	wantOutcome(t, e.mustBook("t1", "alice", a, b), seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED, true, true)
	e.checkMachines()
}

func TestCrossShardNoWithoutConflicts(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	// The section is known, so the router sends it on; shard 2 votes NO with
	// no conflicts. That is a failure of the order, not a bad request.
	nope := &seatsafev1.SeatRef{EventId: "evt-flash", SectionId: "sec-102", SeatId: "nope"}
	wantOutcome(t, e.mustBook("i1", "alice", flash("sec-101", 4, 1), nope), seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED, true, false)
	if !e.available("sec-101")["R04-S001"] {
		t.Fatal("seat held on the shard that voted YES")
	}
	// On a single shard the same seat is still INVALID (the shard's own answer).
	wantOutcome(t, e.mustBook("i2", "alice", nope), invalid, false, false)
	e.checkMachines()
}

func TestCrossShardConflictBeatsTimeout(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	taken := flash("sec-101", 4, 2)
	wantOutcome(t, e.mustBook("c0", "bob", taken), confirmed, false, false)
	// Shard 1 votes NO over the taken seat while shard 2 has no leader and times out.
	e.cluster.Shards[2].SetNoLeader()
	wantOutcome(t, e.mustBook("c1", "alice", taken, flash("sec-102", 4, 2)), unavailable, true, false, taken)
	g := e.cluster.Shards[2]
	before := g.Machine.AppliedIndex()
	g.SetLeader(0)
	for deadline := time.Now().Add(3 * time.Second); g.Machine.AppliedIndex() == before; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("background Abort never reached shard 2")
		}
	}
	e.checkMachines()
}

// 1,000 orders race for 100 seats spread over 3 shards: single seats,
// same-shard pairs and cross-shard pairs.
func TestContention(t *testing.T) {
	e := newEnv(t, fakecluster.SmallDataset)
	sections := []string{"sec-101", "sec-102", "sec-103", "sec-104"}
	var pool []*seatsafev1.SeatRef
	for i := 0; len(pool) < 100; i++ {
		pool = append(pool, flash(sections[i%len(sections)], 1+i/40, 1+(i/len(sections))%10))
	}
	seen := map[string]bool{}
	for _, s := range pool {
		k := s.SectionId + "/" + s.SeatId
		if seen[k] {
			t.Fatalf("pool has %s twice", k)
		}
		seen[k] = true
	}

	r := rand.New(rand.NewSource(7))
	orders := make([][]*seatsafev1.SeatRef, 1000)
	cross := 0
	for i := range orders {
		a := pool[r.Intn(len(pool))]
		if r.Intn(2) == 0 {
			orders[i] = []*seatsafev1.SeatRef{a}
			continue
		}
		b := pool[r.Intn(len(pool))]
		for proto.Equal(a, b) {
			b = pool[r.Intn(len(pool))]
		}
		if shardOf(a.SectionId) != shardOf(b.SectionId) {
			cross++
		}
		orders[i] = []*seatsafev1.SeatRef{a, b}
	}

	type result struct {
		resp *seatsafev1.BookSeatsResponse
		err  error
	}
	results := make([]result, len(orders))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range orders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resp, err := e.book(fmt.Sprintf("c-%04d", i), fmt.Sprintf("user-%d", i), orders[i]...)
			results[i] = result{resp, err}
		}(i)
	}
	close(start)
	wg.Wait()

	owner := map[string]int{}
	counts := map[string]int{}
	for i, res := range results {
		if res.err != nil {
			counts[status.Code(res.err).String()]++
			continue
		}
		counts[res.resp.Outcome.String()]++
		if res.resp.Outcome != confirmed {
			continue
		}
		for _, s := range orders[i] {
			k := s.SectionId + "/" + s.SeatId
			if prev, dup := owner[k]; dup {
				t.Fatalf("seat %s confirmed to orders %d and %d", k, prev, i)
			}
			owner[k] = i
		}
	}
	bookedInShards := 0
	for _, sec := range sections {
		avail := e.available(sec)
		for _, s := range pool {
			if s.SectionId == sec && !avail[s.SeatId] {
				bookedInShards++
			}
		}
	}
	if len(owner) != bookedInShards {
		t.Fatalf("%d seats confirmed to clients, %d BOOKED in shards", len(owner), bookedInShards)
	}
	if counts[confirmed.String()] == 0 || counts[unavailable.String()] == 0 || cross < 100 {
		t.Fatalf("contention did not happen: %v (cross-shard orders %d)", counts, cross)
	}
	e.checkMachines()
	t.Logf("%d orders (%d cross-shard): %v; %d seats booked", len(orders), cross, counts, len(owner))
}

func BenchmarkRouterBookSingleShard(b *testing.B) {
	benchBook(b, func(i int) []*seatsafev1.SeatRef {
		return []*seatsafev1.SeatRef{benchSeat("sec-101", i)}
	})
}

func BenchmarkRouterBookCrossShard(b *testing.B) {
	benchBook(b, func(i int) []*seatsafev1.SeatRef {
		return []*seatsafev1.SeatRef{benchSeat("sec-101", i), benchSeat("sec-102", i)}
	})
}

// benchDataset gives sec-101 (shard 1) and sec-102 (shard 2) 50,000 seats each.
var benchDataset = fakecluster.Dataset{Shards: 3, FlashSections: 3, FlashRows: 100, FlashSeatsPerRow: 500, ChunkSize: 1000}

func benchSeat(section string, i int) *seatsafev1.SeatRef {
	i %= 50_000
	return flash(section, 1+i/500, 1+i%500)
}

func benchBook(b *testing.B, seats func(i int) []*seatsafev1.SeatRef) {
	e := newEnv(b, benchDataset)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.client.BookSeats(ctx, &seatsafev1.BookSeatsRequest{RequestId: fmt.Sprintf("b-%d", i), UserId: "u", Seats: seats(i)})
		if err != nil {
			b.Fatal(err)
		}
		if i < 50_000 && resp.Outcome != confirmed {
			b.Fatalf("booking %d: %v", i, resp)
		}
	}
}
