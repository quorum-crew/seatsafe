package linearize

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/loadgen"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/router"
)

// Seats of two sections; a and b are in one, x in another (another shard for
// a cross-shard order).
const (
	seatA = "e/s1/a"
	seatB = "e/s1/b"
	seatX = "e/s2/x"
)

// hb builds a history. Times are in ms.
type hb struct {
	h loadgen.History
	n int
}

const ms = int64(time.Millisecond)

// order adds a one-attempt order answered with outcome. named lists the seats
// a SEAT_UNAVAILABLE answer names.
func (b *hb) order(id string, call, ret int64, outcome string, seats []string, named ...string) *hb {
	cross := sectionOf(seats[0]) != sectionOf(seats[len(seats)-1])
	b.n++
	b.h.Attempts = append(b.h.Attempts, loadgen.Attempt{ClientID: uint64(b.n), RequestID: id, Seats: seats, CallNs: call * ms, ReturnNs: ret * ms,
		Outcome: outcome, Code: "OK", Unavailable: named, Cross: &cross})
	return b
}

// unknown adds an order whose only attempt timed out.
func (b *hb) unknown(id string, call, ret int64, seats ...string) *hb {
	b.order(id, call, ret, "", seats)
	b.h.Attempts[len(b.h.Attempts)-1].Code = "DeadlineExceeded"
	return b
}

// list adds a listing of section sec with the given free seat ids.
func (b *hb) list(phase string, call, ret int64, sec string, free ...string) *hb {
	event, section, _ := strings.Cut(sec, "/")
	b.h.Listings = append(b.h.Listings, loadgen.Listing{Phase: phase, Event: event, Section: section, Free: free, CallNs: call * ms, ReturnNs: ret * ms})
	return b
}

func (b *hb) check(t *testing.T) *Report {
	t.Helper()
	return Check(&b.h, Options{Timeout: 30 * time.Second})
}

const (
	confirmed   = loadgen.OutcomeConfirmed
	unavailable = loadgen.OutcomeUnavailable
	aborted     = loadgen.OutcomeAborted
	invalid     = loadgen.OutcomeInvalid
	baseline    = loadgen.PhaseBaseline
	browse      = loadgen.PhaseBrowse
	final       = loadgen.PhaseFinal
)

func one(s string) []string { return []string{s} }

// Histories that are not linearizable. Every one of them must fail.
func TestRejects(t *testing.T) {
	for name, c := range map[string]struct {
		h    *hb
		seat string // the seat that must be reported
	}{
		"two CONFIRMED orders for one seat": {
			new(hb).order("o1", 10, 20, confirmed, one(seatA)).order("o2", 30, 40, confirmed, one(seatA)), seatA},
		"two concurrent CONFIRMED orders for one seat": {
			new(hb).order("o1", 10, 30, confirmed, one(seatA)).order("o2", 20, 40, confirmed, one(seatA)), seatA},
		"two CONFIRMED multi-seat orders sharing one seat": {
			new(hb).order("o1", 10, 20, confirmed, []string{seatA, seatB}).order("o2", 15, 25, confirmed, []string{seatB, seatX}), seatB},
		"SEAT_UNAVAILABLE for a seat free for the whole operation (baseline and final say free)": {
			new(hb).list(baseline, -20, -10, "e/s1", "a", "b").
				order("o1", 10, 20, unavailable, one(seatA), seatA).
				list(final, 50, 60, "e/s1", "a", "b"), seatA},
		"SEAT_UNAVAILABLE for a seat free for the whole operation (booked only later)": {
			new(hb).order("o1", 10, 20, unavailable, one(seatA), seatA).order("o2", 30, 40, confirmed, one(seatA)), seatA},
		"SEAT_UNAVAILABLE for a seat free for the whole operation (old history, one seat, no names)": {
			new(hb).order("o1", 10, 20, unavailable, one(seatA)).order("o2", 30, 40, confirmed, one(seatA)), seatA},
		"SEAT_UNAVAILABLE for a seat free for the whole operation (the hold that explains it came later)": {
			new(hb).list(baseline, -20, -10, "e/s1", "a").
				order("o1", 10, 20, unavailable, one(seatA), seatA).
				order("x1", 30, 40, aborted, []string{seatA, seatX}), seatA},
		"stale browse: seat listed free after a CONFIRMED that returned before the browse started": {
			new(hb).order("o1", 10, 20, confirmed, one(seatA)).list(browse, 30, 40, "e/s1", "a", "b"), seatA},
		"stale browse: seat listed taken, then free again with nothing releasing it": {
			new(hb).order("o1", 10, 20, confirmed, one(seatA)).list(browse, 30, 40, "e/s1", "b").list(browse, 50, 60, "e/s1", "a", "b"), seatA},
		"CONFIRMED seat still free in the final read": {
			new(hb).order("o1", 10, 20, confirmed, one(seatA)).list(final, 30, 40, "e/s1", "a"), seatA},
		"CONFIRMED for a seat the baseline says was taken": {
			new(hb).list(baseline, -20, -10, "e/s1", "b").order("o1", 10, 20, confirmed, one(seatA)), seatA},
		"seat taken in the final read with no order that could have booked it": {
			new(hb).list(baseline, -20, -10, "e/s1", "a").
				order("o1", 10, 20, unavailable, []string{seatA, seatB}, seatB).
				list(final, 30, 40, "e/s1"), seatA},
		"seat no order names goes from free to taken": {
			new(hb).list(baseline, -20, -10, "e/s1", "a", "b").order("o1", 10, 20, confirmed, one(seatA)).list(final, 30, 40, "e/s1"), seatB},
		"an ABORTED cross-shard order's hold is still there in the final read": {
			new(hb).list(baseline, -20, -10, "e/s1", "a").
				order("x1", 10, 20, aborted, []string{seatA, seatX}).
				list(final, 30, 40, "e/s1"), seatA},
		"UNKNOWN order cannot explain a seat that was taken before it was called": {
			new(hb).list(baseline, -20, -10, "e/s1", "a").
				order("o1", 10, 20, unavailable, one(seatA), seatA).
				unknown("u1", 30, 40, seatA), seatA},
		"single-shard ABORTED order cannot hold a seat": {
			new(hb).list(baseline, -20, -10, "e/s1", "a", "b").
				order("o0", 5, 25, aborted, []string{seatA, seatB}).
				order("o1", 10, 20, unavailable, one(seatA), seatA), seatA},
	} {
		t.Run(name, func(t *testing.T) {
			r := c.h.check(t)
			if r.Verdict() != Fail || !reflect.DeepEqual(r.Illegal, []string{c.seat}) {
				t.Fatalf("verdict %s, illegal %v, want FAIL for %s\n%s", r.Verdict(), r.Illegal, c.seat, r)
			}
			if !strings.Contains(r.FirstBad(), "seat "+c.seat+" is not linearizable") {
				t.Fatalf("FirstBad: %q", r.FirstBad())
			}
			var html bytes.Buffer
			if err := r.Visualize(&html); err != nil || !strings.Contains(html.String(), "<html") {
				t.Fatalf("Visualize: %v, %d bytes", err, html.Len())
			}
		})
	}
}

// Histories that are linearizable.
func TestAccepts(t *testing.T) {
	for name, h := range map[string]*hb{
		"empty": new(hb),
		"concurrent orders for one seat: one CONFIRMED, the rest SEAT_UNAVAILABLE": new(hb).
			list(baseline, -20, -10, "e/s1", "a", "b").
			order("o1", 10, 40, unavailable, one(seatA), seatA).
			order("o2", 15, 35, confirmed, one(seatA)).
			order("o3", 20, 45, unavailable, []string{seatA, seatB}, seatA).
			order("o4", 50, 60, unavailable, one(seatA), seatA).
			list(final, 70, 80, "e/s1", "b"),
		"concurrent orders: the loser returned first, but overlapped the winner": new(hb).
			order("o1", 10, 20, unavailable, one(seatA), seatA).
			order("o2", 5, 30, confirmed, one(seatA)),
		"UNKNOWN order that booked: the seat is taken with no CONFIRMED": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			unknown("u1", 10, 20, seatA).
			order("o1", 30, 40, unavailable, one(seatA), seatA).
			list(final, 50, 60, "e/s1"),
		"UNKNOWN order that did not book: a later order is CONFIRMED": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			unknown("u1", 10, 20, seatA).
			order("o1", 30, 40, confirmed, one(seatA)).
			list(final, 50, 60, "e/s1"),
		"UNKNOWN order that took effect after it timed out": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			unknown("u1", 10, 20, seatA).
			list(browse, 30, 40, "e/s1", "a").
			list(browse, 50, 60, "e/s1").
			list(final, 70, 80, "e/s1"),
		"two UNKNOWN orders for one seat, seat free at the end": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			unknown("u1", 10, 20, seatA).unknown("u2", 15, 25, seatA).
			list(final, 70, 80, "e/s1", "a"),
		"retried order: UNAVAILABLE, then CONFIRMED and replayed": func() *hb {
			b := new(hb).unknown("o1", 10, 20, seatA).order("o1", 25, 30, confirmed, one(seatA))
			return b.order("o2", 12, 18, unavailable, one(seatA), seatA) // legal only if o1 took effect in its first attempt
		}(),
		"cross-shard order CONFIRMED on both shards": new(hb).
			list(baseline, -20, -10, "e/s1", "a", "b").list(baseline, -20, -10, "e/s2", "x").
			order("x1", 10, 30, confirmed, []string{seatA, seatX}).
			order("o1", 15, 25, unavailable, one(seatX), seatX).
			list(browse, 40, 50, "e/s2").
			list(final, 70, 80, "e/s1", "b").list(final, 70, 80, "e/s2"),
		"cross-shard order ABORTED: its hold turned another order away, then the seat was booked": new(hb).
			list(baseline, -20, -10, "e/s1", "a").list(baseline, -20, -10, "e/s2", "x").
			order("x1", 10, 30, unavailable, []string{seatA, seatX}, seatX). // held a, x was taken
			order("o0", 1, 5, confirmed, one(seatX)).
			order("o1", 15, 20, unavailable, one(seatA), seatA). // saw x1's hold
			list(browse, 16, 19, "e/s1").                        // so did a browse
			order("o2", 40, 50, confirmed, one(seatA)).          // after the hold was released
			list(final, 70, 80, "e/s1").list(final, 70, 80, "e/s2"),
		"cross-shard order ABORTED: the hold outlived the answer and was released later": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			order("x1", 10, 20, aborted, []string{seatA, seatX}).
			order("o1", 30, 35, unavailable, one(seatA), seatA).
			list(final, 70, 80, "e/s1", "a"),
		"UNKNOWN cross-shard order: held, turned an order away, aborted later": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			unknown("x1", 10, 20, seatA, seatX).
			order("o1", 30, 35, unavailable, one(seatA), seatA).
			order("o2", 40, 45, confirmed, one(seatA)).
			list(final, 70, 80, "e/s1"),
		"UNKNOWN cross-shard order still holding in the final read": new(hb).
			list(baseline, -20, -10, "e/s1", "a").
			unknown("x1", 10, 20, seatA, seatX).
			list(final, 70, 80, "e/s1"),
		"INVALID and single-shard ABORTED orders change nothing": new(hb).
			list(baseline, -20, -10, "e/s1", "a", "b").
			order("o1", 10, 20, invalid, one(seatA)).order("o2", 10, 20, aborted, []string{seatA, seatB}).
			list(final, 70, 80, "e/s1", "a", "b"),
		"browse concurrent with the CONFIRMED may still list the seat": new(hb).
			order("o1", 10, 30, confirmed, one(seatA)).list(browse, 20, 40, "e/s1", "a"),
		"seat booked before the run": new(hb).
			list(baseline, -20, -10, "e/s1", "b").
			order("o1", 10, 20, unavailable, one(seatA), seatA).
			list(final, 70, 80, "e/s1", "b"),
	} {
		t.Run(name, func(t *testing.T) {
			if r := h.check(t); r.Verdict() != Pass {
				t.Fatalf("%s\n%s", r, r.FirstBad())
			}
		})
	}
}

// A history from before listings, unavailable_seats and cross existed is
// read as documented: no reads, a multi-seat SEAT_UNAVAILABLE names no seat,
// and seats in two sections make an order cross-shard.
func TestOldHistory(t *testing.T) {
	old := func(id string, call, ret int64, outcome string, seats ...string) loadgen.Attempt {
		return loadgen.Attempt{RequestID: id, Seats: seats, CallNs: call * ms, ReturnNs: ret * ms, Outcome: outcome, Code: "OK"}
	}
	ok := &loadgen.History{Attempts: []loadgen.Attempt{
		old("o1", 10, 20, unavailable, seatA, seatB), // names neither seat
		old("o2", 30, 40, confirmed, seatA),
		old("o3", 30, 40, confirmed, seatB),
		old("x1", 50, 60, aborted, seatA, seatX), // two sections: may hold x
		old("o4", 62, 64, unavailable, seatX),    // one seat: names it; x1's hold explains it
		old("o5", 70, 80, confirmed, seatX),
	}}
	if r := Check(ok, Options{}); r.Verdict() != Pass || r.Cross != 1 || r.Listings != 0 {
		t.Fatalf("%s\n%s", r, r.FirstBad())
	}
	bad := &loadgen.History{Attempts: []loadgen.Attempt{old("o1", 10, 20, confirmed, seatA), old("o2", 30, 40, confirmed, seatA, seatB)}}
	if r := Check(bad, Options{}); r.Verdict() != Fail || !reflect.DeepEqual(r.Illegal, []string{seatA}) {
		t.Fatalf("%s", r)
	}
}

// The seat model, transition by transition.
func TestStep(t *testing.T) {
	fr, bk, hd, hu := state{free, 0}, state{booked, 7}, state{held, 7}, state{heldUnknown, 7}
	other := state{held, 8}
	for _, c := range []struct {
		name string
		s    state
		in   input
		out  output
		want []state
	}{
		{"book a free seat", fr, input{kind: opBook, order: 7}, output{}, []state{bk}},
		{"book a booked seat", bk, input{kind: opBook, order: 9}, output{}, nil},
		{"book a held seat", other, input{kind: opBook, order: 7}, output{}, nil},
		{"unavailable, seat free", fr, input{kind: opTaken}, output{}, nil},
		{"unavailable, seat booked", bk, input{kind: opTaken}, output{}, []state{bk}},
		{"unavailable, seat held", hd, input{kind: opTaken}, output{}, []state{hd}},
		{"hold a free seat, or not", fr, input{kind: opHold, order: 7}, output{}, []state{fr, hd}},
		{"hold a taken seat", bk, input{kind: opHold, order: 9}, output{}, []state{bk}},
		{"release own hold", hd, input{kind: opRelease, order: 7}, output{}, []state{fr}},
		{"release own unknown hold", hu, input{kind: opRelease, order: 7}, output{}, []state{fr}},
		{"release another order's hold", other, input{kind: opRelease, order: 7}, output{}, []state{other}},
		{"release a booking", bk, input{kind: opRelease, order: 7}, output{}, []state{bk}},
		{"unknown, free seat", fr, input{kind: opUnknown, order: 7}, output{}, []state{fr, bk}},
		{"unknown cross-shard, free seat", fr, input{kind: opUnknown, order: 7, cross: true}, output{}, []state{fr, bk, hu}},
		{"unknown, taken seat", other, input{kind: opUnknown, order: 7, cross: true}, output{}, []state{other}},
		{"read free, free", fr, input{kind: opRead}, output{free: true}, []state{fr}},
		{"read free, booked", bk, input{kind: opRead}, output{free: true}, nil},
		{"read free, held", hd, input{kind: opRead}, output{free: true}, nil},
		{"read taken, free", fr, input{kind: opRead}, output{}, nil},
		{"read taken, held", hd, input{kind: opRead}, output{}, []state{hd}},
		{"final read taken, booked", bk, input{kind: opRead, final: true}, output{}, []state{bk}},
		{"final read taken, held by an answered order", hd, input{kind: opRead, final: true}, output{}, nil},
		{"final read taken, held by an unknown order", hu, input{kind: opRead, final: true}, output{}, []state{hu}},
	} {
		if got := step(c.s, c.in, c.out); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The set of possible states Porcupine steps through: sorted, without
// duplicates, unchanged (and not reallocated) by operations that only observe.
func TestStepSet(t *testing.T) {
	init := encode([]state{{booked, -1}, {free, 0}, {free, 0}})
	if init.len() != 2 || init.at(0) != (state{free, 0}) || init.at(1) != (state{booked, -1}) {
		t.Fatalf("%q", init)
	}
	// Unknown before the run; a cross-shard UNKNOWN order may book or hold.
	next, ok, same := stepSet(init, input{kind: opUnknown, order: 3, cross: true}, output{})
	want := encode([]state{{free, 0}, {booked, 3}, {heldUnknown, 3}, {booked, -1}})
	if !ok || same || next != want || next.len() != 4 {
		t.Fatalf("%v %v %q, want %q", ok, same, next, want)
	}
	// A read that sees the seat free rules out every taken state.
	if got, ok, same := stepSet(next, input{kind: opRead}, output{free: true}); !ok || same || got != encode([]state{{free, 0}}) {
		t.Fatalf("%v %v %q", ok, same, got)
	}
	// One that sees it taken keeps the three taken states.
	taken, ok, _ := stepSet(next, input{kind: opRead}, output{})
	if !ok || taken.len() != 3 {
		t.Fatalf("%v %q", ok, taken)
	}
	// Observing again changes nothing.
	if got, ok, same := stepSet(taken, input{kind: opTaken}, output{}); !ok || !same || got != taken {
		t.Fatalf("%v %v %q", ok, same, got)
	}
	// Booking needs a state in which the seat is free.
	if _, ok, _ := stepSet(taken, input{kind: opBook, order: 9}, output{}); ok {
		t.Fatal("booked a seat that is taken in every possible state")
	}
	// Releasing the hold merges two states into one.
	if got, ok, _ := stepSet(taken, input{kind: opRelease, order: 3}, output{}); !ok || got != encode([]state{{free, 0}, {booked, 3}, {booked, -1}}) {
		t.Fatalf("%v %q", ok, got)
	}
}

// A check that runs out of time is a TIMEOUT, not a PASS, and it names the
// seats it could not decide.
func TestTimeoutIsNotAPass(t *testing.T) {
	b := new(hb)
	for i := range 200 {
		b.order("o"+string(rune('a'+i%26))+strings.Repeat("x", i), int64(i), int64(i)+5, confirmed, one("e/s1/seat"+strings.Repeat("y", i)))
	}
	r := Check(&b.h, Options{Timeout: time.Nanosecond})
	if r.Verdict() != Timeout || len(r.TimedOut) == 0 || len(r.Illegal) != 0 {
		t.Fatalf("%s", r)
	}
	if !strings.HasPrefix(r.String(), "TIMEOUT") {
		t.Fatalf("%s", r)
	}
	// With time, the same history passes.
	if r := b.check(t); r.Verdict() != Pass || r.Seats != 200 {
		t.Fatalf("%s", r)
	}
	// And with a double booking added, it fails.
	b.order("dup", 1000, 1001, confirmed, one("e/s1/seat"))
	if r := b.check(t); r.Verdict() != Fail {
		t.Fatalf("%s", r)
	}
}

func TestNoBrowsesOption(t *testing.T) {
	b := new(hb).list(baseline, -20, -10, "e/s1", "a").
		order("o1", 10, 20, confirmed, one(seatA)).
		list(browse, 30, 40, "e/s1", "a"). // stale
		list(final, 50, 60, "e/s1")
	if r := b.check(t); r.Verdict() != Fail || r.Browses != 1 {
		t.Fatalf("%s", r)
	}
	if r := Check(&b.h, Options{NoBrowses: true}); r.Verdict() != Pass || r.Browses != 0 || r.Listings != 2 {
		t.Fatalf("%s", r)
	}
}

// fakeRun runs the real load generator against a real router and the fake
// cluster and returns the history it wrote.
func fakeRun(t *testing.T, mod func(*loadgen.Config)) (*loadgen.History, *loadgen.Summary) {
	t.Helper()
	sm, seeds, err := fakecluster.Dataset{
		Shards: 3, FlashSections: 30, FlashRows: 20, FlashSeatsPerRow: 50,
		Events: 2, Sections: 3, Rows: 4, SeatsPerRow: 5, ChunkSize: 500,
	}.Build()
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
	u, err := loadgen.NewUniverse(sm, seeds)
	if err != nil {
		t.Fatal(err)
	}

	c := loadgen.Defaults()
	c.Workload, c.Warmup, c.Quiet, c.Out = loadgen.WorkloadFlash, 0, true, t.TempDir()
	c.Rate, c.Duration, c.History = 1000, time.Second, true
	c.Target, c.Conns = "passthrough:///router", 4
	c.DialOptions = []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	mod(&c)
	sum, err := loadgen.Run(context.Background(), c, u, loadgen.Env{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(c.Out, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := loadgen.ReadFullHistory(f)
	if err != nil {
		t.Fatal(err)
	}
	return h, sum
}

// 1,000 flash orders, a fifth of them cross-shard, with browses, against the
// fake cluster: hundreds of real collisions and 2PC aborts. The history is
// linearizable, and stops being so when one answer in it is changed.
func TestRealHistory(t *testing.T) {
	h, sum := fakeRun(t, func(c *loadgen.Config) { c.CrossShardFrac, c.BrowseRate = 0.2, 200 })
	if !sum.Audit.Pass {
		t.Fatalf("audit %+v", sum.Audit)
	}
	slow := Options{Timeout: 3 * time.Minute} // the race detector makes the check many times slower
	r := Check(h, slow)
	t.Log(r)
	if r.Verdict() != Pass {
		t.Fatalf("%s\n%s", r, r.FirstBad())
	}
	if r.Orders != 1000 || r.Cross < 100 || r.Browses < 20 || r.Listings < 60+r.Browses || r.Seats < 250 {
		t.Fatalf("the run did not exercise the check: %s", r)
	}
	var sawUnavailable, sawCrossFail bool
	for _, a := range h.Attempts {
		sawUnavailable = sawUnavailable || len(a.Unavailable) > 0
		sawCrossFail = sawCrossFail || (*a.Cross && a.Outcome != confirmed)
	}
	if !sawUnavailable || !sawCrossFail {
		t.Fatalf("no SEAT_UNAVAILABLE naming a seat (%v) or no failed cross-shard order (%v)", sawUnavailable, sawCrossFail)
	}

	// Turn one SEAT_UNAVAILABLE that named a seat into a CONFIRMED: the seat
	// it named now has two owners, or an owner that was never free.
	tampered := *h
	tampered.Attempts = append([]loadgen.Attempt(nil), h.Attempts...)
	for i, a := range tampered.Attempts {
		if len(a.Unavailable) > 0 {
			a.Outcome, a.Unavailable = confirmed, nil
			tampered.Attempts[i] = a
			break
		}
	}
	if r := Check(&tampered, slow); r.Verdict() != Fail {
		t.Fatalf("tampered history: %s", r)
	}

	// Drop a CONFIRMED order from the history: its seats were taken by
	// nobody.
	dropped := *h
	dropped.Attempts = nil
	done := false
	for _, a := range h.Attempts {
		if !done && a.Outcome == confirmed {
			done = true
			continue
		}
		dropped.Attempts = append(dropped.Attempts, a)
	}
	if r := Check(&dropped, slow); r.Verdict() != Fail {
		t.Fatalf("history without one CONFIRMED order: %s", r)
	}
}

// The fresh workload books every seat it asks for exactly once.
func TestRealHistoryFresh(t *testing.T) {
	h, _ := fakeRun(t, func(c *loadgen.Config) { c.Workload, c.CrossShardFrac = loadgen.WorkloadFresh, 0.3 })
	if r := Check(h, Options{Timeout: 3 * time.Minute}); r.Verdict() != Pass || r.Unknown != 0 {
		t.Fatalf("%s\n%s", r, r.FirstBad())
	}
}
