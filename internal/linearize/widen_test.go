package linearize

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// tenths turns the builder's times, given in tenths of a millisecond, into
// what they mean: the histories here need times closer than 1 ms.
func (b *hb) tenths() *hb {
	for i := range b.h.Attempts {
		b.h.Attempts[i].CallNs /= 10
		b.h.Attempts[i].ReturnNs /= 10
	}
	for i := range b.h.Listings {
		b.h.Listings[i].CallNs /= 10
		b.h.Listings[i].ReturnNs /= 10
	}
	return b
}

func (b *hb) widened(d time.Duration) *Report {
	return Check(&b.h, Options{Timeout: 30 * time.Second, Widen: d})
}

// A history merged from two machines whose clocks differ by 2 ms. Machine A's
// order was CONFIRMED for the seat from 10.0 to 10.5 ms. Then machine B's
// order for the same seat ran from 10.8 to 11.2 ms and was refused: correct.
// But B's clock is 2 ms behind, so its history says 8.8 to 9.2 ms, before A's
// order was even called, when the seat was still free.
func skewed() *hb {
	return new(hb).
		list(baseline, -200, -100, "e/s1", "a").
		order("a1", 100, 105, confirmed, one(seatA)).
		order("b1", 88, 92, unavailable, one(seatA), seatA).
		list(final, 300, 310, "e/s1").
		tenths()
}

func TestWidenAllowsForClockSkew(t *testing.T) {
	r := skewed().widened(0)
	if r.Verdict() != Fail || !reflect.DeepEqual(r.Illegal, []string{seatA}) || r.Widen != 0 || strings.Contains(r.String(), "widened") {
		t.Fatalf("without widening: %s", r)
	}
	r = skewed().widened(3 * time.Millisecond)
	if r.Verdict() != Pass || r.Widen != 3*time.Millisecond || !strings.HasSuffix(r.String(), "; widened by 3ms") {
		t.Fatalf("widened by 3ms: %s\n%s", r, r.FirstBad())
	}
	// A negative widening is none.
	if r := skewed().widened(-time.Second); r.Verdict() != Fail || r.Widen != 0 {
		t.Fatalf("negative widening: %s", r)
	}

	// Listings are widened too. The final read was taken on the machine
	// whose clock is behind: it says the seat was taken before the only
	// order for it was called.
	late := func() *hb {
		return new(hb).
			list(baseline, -200, -100, "e/s1", "a").
			order("a1", 100, 105, confirmed, one(seatA)).
			list(final, 88, 92, "e/s1").
			tenths()
	}
	if r := late().widened(0); r.Verdict() != Fail {
		t.Fatalf("final read before the order, without widening: %s", r)
	}
	if r := late().widened(3 * time.Millisecond); r.Verdict() != Pass {
		t.Fatalf("final read before the order, widened: %s\n%s", r, r.FirstBad())
	}
	// And the baseline: taken on the machine whose clock is ahead, it seems
	// to see the seat free after the order that booked it.
	early := func() *hb {
		return new(hb).
			list(baseline, 110, 115, "e/s1", "a").
			order("a1", 100, 105, confirmed, one(seatA)).
			list(final, 300, 310, "e/s1").
			tenths()
	}
	if r := early().widened(0); r.Verdict() != Fail {
		t.Fatalf("baseline after the order, without widening: %s", r)
	}
	if r := early().widened(3 * time.Millisecond); r.Verdict() != Pass {
		t.Fatalf("baseline after the order, widened: %s\n%s", r, r.FirstBad())
	}
}

// Widening explains operations a few milliseconds apart, never a real
// violation.
func TestWidenKeepsRealViolations(t *testing.T) {
	const widen = 3 * time.Millisecond
	for name, c := range map[string]struct {
		h    *hb
		seat string
	}{
		"two CONFIRMED orders for one seat, seconds apart": {
			new(hb).list(baseline, -20, -10, "e/s1", "a").
				order("o1", 10, 20, confirmed, one(seatA)).
				order("o2", 3000, 3010, confirmed, one(seatA)).
				list(final, 5000, 5010, "e/s1"), seatA},
		"two CONFIRMED orders for one seat, at the same time": {
			new(hb).order("o1", 10, 20, confirmed, one(seatA)).order("o2", 11, 19, confirmed, one(seatA)), seatA},
		"two CONFIRMED orders from machines with skewed clocks": {
			new(hb).order("o1", 100, 105, confirmed, one(seatA)).order("o2", 88, 92, confirmed, one(seatA)).tenths(), seatA},
		"SEAT_UNAVAILABLE for a seat that was free for seconds around it": {
			new(hb).list(baseline, -20, -10, "e/s1", "a").
				order("o1", 1000, 1010, unavailable, one(seatA), seatA).
				order("o2", 3000, 3010, confirmed, one(seatA)).
				list(final, 5000, 5010, "e/s1"), seatA},
		"CONFIRMED seat free in the final read": {
			new(hb).order("o1", 10, 20, confirmed, one(seatA)).list(final, 1000, 1010, "e/s1", "a"), seatA},
		"seat taken with no order that could have booked it": {
			new(hb).list(baseline, -20, -10, "e/s1", "a", "b").
				order("o1", 10, 20, confirmed, one(seatA)).
				list(final, 1000, 1010, "e/s1"), seatB},
	} {
		t.Run(name, func(t *testing.T) {
			r := c.h.widened(widen)
			if r.Verdict() != Fail || !reflect.DeepEqual(r.Illegal, []string{c.seat}) {
				t.Fatalf("verdict %s, illegal %v, want FAIL for %s\n%s", r.Verdict(), r.Illegal, c.seat, r)
			}
			if !strings.Contains(r.String(), "widened by 3ms") {
				t.Fatalf("the report does not say it was widened: %s", r)
			}
		})
	}
}

// The histories that pass without widening pass with it.
func TestWidenKeepsLinearizableHistories(t *testing.T) {
	h := new(hb).
		list(baseline, -20, -10, "e/s1", "a", "b").
		order("o1", 10, 40, unavailable, one(seatA), seatA).
		order("o2", 15, 35, confirmed, one(seatA)).
		order("o3", 50, 60, unavailable, one(seatA), seatA).
		unknown("u1", 70, 80, seatB).
		list(browse, 90, 95, "e/s1", "b").
		list(final, 100, 110, "e/s1", "b")
	for _, d := range []time.Duration{0, time.Millisecond, 3 * time.Millisecond, time.Second} {
		if r := h.widened(d); r.Verdict() != Pass || r.StaleBrowses != 0 {
			t.Fatalf("widened by %v: %s\n%s", d, r, r.FirstBad())
		}
	}
}
