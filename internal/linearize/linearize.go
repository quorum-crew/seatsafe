// Package linearize checks a load generator history for linearizability with
// Porcupine (github.com/anishathalye/porcupine).
//
// # Model
//
// The history is partitioned by seat: every seat is checked on its own,
// against a model of that one seat. A seat is free, booked by an order, or
// held by a cross-shard order that has prepared but not finished (2PC). Before
// the run it is free or booked, and the model does not assume which.
//
// Every order is one operation per seat, from its first call to its last
// return (retries included), projected onto each of its seats:
//
//   - CONFIRMED: the seat goes from free to booked by this order. Legal only
//     if the seat was free.
//   - SEAT_UNAVAILABLE naming this seat: nothing changes. Legal only if the
//     seat was not free (booked, or held by another order).
//   - SEAT_UNAVAILABLE naming only other seats, ABORTED, INVALID: no effect
//     on this seat. If the order is cross-shard it may have held the seat for
//     a while: the seat may go from free to held by this order at some point
//     of the call, and a second operation, running from the call to the end
//     of the history, releases it. (The coordinator sends Abort after it
//     answered, so the release has no deadline other than the final read.)
//   - UNKNOWN (no answer): the order may or may not have taken effect, at any
//     time from its call to the end of the history. The seat may go from free
//     to booked by this order, or, for a cross-shard order, be held and
//     released as above.
//
// Listings (ListAvailableSeats answers recorded in the history) are reads of
// every seat they cover: a listed seat was free, an unlisted seat of the
// listed section was not. The pre-run baseline is such a read before the
// first order and the post-run final state one after the last. A read that
// sees a seat taken is legal while the seat is booked or held, except that
// the final read does not accept a hold by an order that was answered: those
// holds must be gone by then.
//
// What the model does not check: that the seats of one order are booked all
// or nothing (the audit's rule e does), because each seat is a partition of
// its own.
//
// # Old histories
//
// A history written before listings, unavailable_seats and cross existed is
// still checked, more weakly: there are no reads, a SEAT_UNAVAILABLE order
// with several seats says nothing about any one of them (with one seat it
// names that seat), and an order counts as cross-shard when its seats are in
// more than one section.
//
// # Timeouts
//
// Checking linearizability is NP-hard. Options.Timeout bounds the whole
// check. A seat that could not be decided in time is reported as timed out:
// the verdict is then TIMEOUT, never PASS.
package linearize

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

// Verdicts of a check.
const (
	Pass    = "PASS"
	Fail    = "FAIL"    // at least one seat is not linearizable
	Timeout = "TIMEOUT" // no seat failed, but at least one was not decided in time
)

// state is one seat.
type state struct {
	kind  stateKind
	order int32 // index of the order that booked or holds the seat; -1 for booked before the run
}

type stateKind uint8

const (
	free stateKind = iota
	booked
	held        // by a cross-shard order that was answered without CONFIRMED
	heldUnknown // by a cross-shard order that was never answered
)

type opKind uint8

const (
	opBook    opKind = iota // CONFIRMED
	opTaken                 // SEAT_UNAVAILABLE naming this seat
	opHold                  // answered, not CONFIRMED, cross-shard: may have held the seat
	opRelease               // the end of such a hold
	opUnknown               // never answered
	opRead                  // a listing
)

// input is one operation on one seat.
type input struct {
	kind    opKind
	order   int32  // index of the order; unused for reads
	cross   bool   // opUnknown: the order may have held the seat
	final   bool   // opRead: the post-run read
	label   string // request id or listing phase, for descriptions
	outcome string // the order's outcome, for descriptions
}

// output is what the operation observed. Only reads have one that is not
// implied by the kind: whether the seat was listed as free.
type output struct{ free bool }

// step returns every state the seat can be in after op, or none if op is
// illegal in state s.
func step(s state, in input, out output) []state {
	same := []state{s}
	switch in.kind {
	case opBook:
		if s.kind == free {
			return []state{{booked, in.order}}
		}
		return nil
	case opTaken:
		if s.kind == free {
			return nil
		}
		return same
	case opHold:
		if s.kind == free {
			return []state{s, {held, in.order}}
		}
		return same
	case opRelease:
		if (s.kind == held || s.kind == heldUnknown) && s.order == in.order {
			return []state{{free, 0}}
		}
		return same
	case opUnknown:
		if s.kind != free {
			return same
		}
		next := []state{s, {booked, in.order}}
		if in.cross {
			next = append(next, state{heldUnknown, in.order})
		}
		return next
	case opRead:
		switch {
		case out.free != (s.kind == free):
			return nil
		case in.final && s.kind == held:
			return nil // an answered order's hold must be gone by the final read
		}
		return same
	}
	return nil
}

// Model returns the Porcupine model of one seat. requests maps order indexes
// to request ids for the descriptions; it may be nil.
func Model(requests []string) porcupine.Model {
	name := func(i int32) string {
		if i < 0 {
			return "an order before the run"
		}
		if int(i) < len(requests) {
			return short(requests[i])
		}
		return fmt.Sprintf("order %d", i)
	}
	nm := porcupine.NondeterministicModel{
		Init: func() []any { return []any{state{free, 0}, state{booked, -1}} },
		Step: func(s, in, out any) []any {
			next := step(s.(state), in.(input), out.(output))
			res := make([]any, len(next))
			for i, n := range next {
				res[i] = n
			}
			return res
		},
		Equal: func(a, b any) bool { return a.(state) == b.(state) },
		Hash:  func(s any) uint64 { x := s.(state); return uint64(x.kind)<<32 | uint64(uint32(x.order)) },
		DescribeOperation: func(in, out any) string {
			i, o := in.(input), out.(output)
			switch i.kind {
			case opBook:
				return fmt.Sprintf("order %s -> CONFIRMED", short(i.label))
			case opTaken:
				return fmt.Sprintf("order %s -> SEAT_UNAVAILABLE (this seat)", short(i.label))
			case opHold:
				return fmt.Sprintf("order %s -> %s (may have held the seat)", short(i.label), i.outcome)
			case opRelease:
				return fmt.Sprintf("order %s: hold released, if any", short(i.label))
			case opUnknown:
				return fmt.Sprintf("order %s -> no answer (UNKNOWN)", short(i.label))
			}
			if o.free {
				return i.label + " listing -> free"
			}
			return i.label + " listing -> not free"
		},
		DescribeState: func(s any) string {
			switch x := s.(state); x.kind {
			case booked:
				return "booked by " + name(x.order)
			case held, heldUnknown:
				return "held by " + name(x.order)
			}
			return "free"
		},
	}
	return nm.ToModel()
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// order is one request id: every attempt of it together.
type order struct {
	request   string
	seats     []string
	call, ret int64
	outcome   string
	named     map[string]bool // seats a SEAT_UNAVAILABLE answer named
	cross     bool
}

// orders collapses the attempts by request id, in order of first call.
func orders(attempts []loadgen.Attempt) []*order {
	byID := map[string]*order{}
	var out []*order
	lastOK := map[string]int64{}
	for _, a := range attempts {
		o := byID[a.RequestID]
		if o == nil {
			o = &order{request: a.RequestID, seats: a.Seats, call: a.CallNs, ret: a.ReturnNs, outcome: loadgen.OutcomeUnknown}
			byID[a.RequestID] = o
			out = append(out, o)
			// Before the cross field existed: seats in two sections may be
			// on two shards.
			sections := map[string]bool{}
			for _, s := range a.Seats {
				sections[sectionOf(s)] = true
			}
			o.cross = len(sections) > 1
		}
		o.call, o.ret = min(o.call, a.CallNs), max(o.ret, a.ReturnNs)
		if a.Cross != nil {
			o.cross = *a.Cross
		}
		switch {
		case a.Outcome != "":
			if t, seen := lastOK[a.RequestID]; seen && t > a.ReturnNs {
				continue
			}
			lastOK[a.RequestID] = a.ReturnNs
			o.outcome, o.named = a.Outcome, nil
			if a.Outcome == loadgen.OutcomeUnavailable {
				o.named = map[string]bool{}
				for _, s := range a.Unavailable {
					o.named[s] = true
				}
				if len(a.Unavailable) == 0 && len(a.Seats) == 1 {
					o.named[a.Seats[0]] = true // the only seat it can mean
				}
			}
		case a.Code == "InvalidArgument" && o.outcome == loadgen.OutcomeUnknown:
			o.outcome = loadgen.OutcomeInvalid
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].call < out[j].call })
	return out
}

// sectionOf returns "event/section" of an "event/section/seat" name.
func sectionOf(seat string) string {
	i := strings.IndexByte(seat, '/')
	if i < 0 {
		return seat
	}
	j := strings.IndexByte(seat[i+1:], '/')
	if j < 0 {
		return seat
	}
	return seat[:i+1+j]
}

// Options tunes a check.
type Options struct {
	// Timeout bounds the whole check; 0 means DefaultTimeout.
	Timeout time.Duration
	// NoBrowses leaves out the listings taken while orders were running,
	// keeping the baseline and the final read.
	NoBrowses bool
	// Workers is how many seats are checked at once; 0 means one per CPU.
	Workers int
}

// DefaultTimeout bounds a check that was not given a timeout.
const DefaultTimeout = time.Minute

// Report is the result of a check.
type Report struct {
	Orders     int // request ids in the history
	Unknown    int // of them, never answered
	Cross      int // of them, cross-shard
	Listings   int // listings used as reads
	Browses    int // of them, taken while orders were running
	Seats      int // seats checked with Porcupine: every seat in an order, and every seat whose listings disagree
	Operations int // operations over all those seats

	Illegal  []string // seats that are not linearizable, earliest first
	TimedOut []string // seats not decided within the timeout, by name
	Elapsed  time.Duration

	first *seatCheck // the first illegal seat
	model porcupine.Model
}

// Verdict is Pass, Fail or Timeout.
func (r *Report) Verdict() string {
	switch {
	case len(r.Illegal) > 0:
		return Fail
	case len(r.TimedOut) > 0:
		return Timeout
	}
	return Pass
}

// String is a one-line summary.
func (r *Report) String() string {
	return fmt.Sprintf("%s: %d seats (%d operations) from %d orders (%d UNKNOWN, %d cross-shard) and %d listings (%d browses); %d not linearizable, %d timed out; %.2fs",
		r.Verdict(), r.Seats, r.Operations, r.Orders, r.Unknown, r.Cross, r.Listings, r.Browses, len(r.Illegal), len(r.TimedOut), r.Elapsed.Seconds())
}

// Visualize writes Porcupine's HTML visualization of the first seat that is
// not linearizable. It is an error to call it when there is none.
func (r *Report) Visualize(w io.Writer) error {
	if r.first == nil {
		return fmt.Errorf("linearize: every checked seat is linearizable, nothing to visualize")
	}
	return porcupine.Visualize(r.model, r.first.info, w)
}

// FirstBad describes the first seat that is not linearizable: its name and
// its operations in order of their calls. Empty if there is none.
func (r *Report) FirstBad() string {
	if r.first == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "seat %s is not linearizable; its %d operations (times in ms from the start of the run):\n", r.first.seat, len(r.first.ops))
	ops := append([]porcupine.Operation(nil), r.first.ops...)
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	for _, op := range ops {
		fmt.Fprintf(&b, "  [%10.3f, %10.3f]  %s\n", float64(op.Call)/1e6, float64(op.Return)/1e6, r.model.DescribeOperation(op.Input, op.Output))
	}
	return b.String()
}

// seatCheck is one seat's operations and, after the check, its result.
type seatCheck struct {
	seat   string
	ops    []porcupine.Operation
	first  int64 // earliest call of an order on this seat
	result porcupine.CheckResult
	info   porcupine.LinearizationInfo
}

// Check checks h. It never returns Pass for a history it could not finish.
func Check(h *loadgen.History, opt Options) *Report {
	start := time.Now()
	if opt.Timeout <= 0 {
		opt.Timeout = DefaultTimeout
	}
	if opt.Workers <= 0 {
		opt.Workers = runtime.GOMAXPROCS(0)
	}
	deadline := start.Add(opt.Timeout)

	all := orders(h.Attempts)
	requests := make([]string, len(all))
	r := &Report{Orders: len(all)}

	// The end of the history: after everything in it. Operations that may
	// take effect at any later time return here.
	var end int64
	for _, o := range all {
		end = max(end, o.ret)
	}
	var listings []loadgen.Listing
	for _, l := range h.Listings {
		if opt.NoBrowses && l.Phase == loadgen.PhaseBrowse {
			continue
		}
		listings = append(listings, l)
		end = max(end, l.ReturnNs)
		r.Listings++
		if l.Phase == loadgen.PhaseBrowse {
			r.Browses++
		}
	}
	end++

	seats := map[string]*seatCheck{}
	bySection := map[string][]*seatCheck{} // the seats of every section that are in an order
	seat := func(name string, at int64) *seatCheck {
		c := seats[name]
		if c == nil {
			c = &seatCheck{seat: name, first: at}
			seats[name] = c
			sec := sectionOf(name)
			bySection[sec] = append(bySection[sec], c)
		}
		return c
	}
	add := func(c *seatCheck, in input, out output, call, ret int64) {
		c.ops = append(c.ops, porcupine.Operation{ClientId: len(c.ops), Input: in, Output: out, Call: call, Return: ret})
	}
	for i, o := range all {
		requests[i] = o.request
		if o.outcome == loadgen.OutcomeUnknown {
			r.Unknown++
		}
		if o.cross {
			r.Cross++
		}
		in := input{order: int32(i), label: o.request, outcome: o.outcome}
		for _, name := range o.seats {
			c := seat(name, o.call)
			switch {
			case o.outcome == loadgen.OutcomeConfirmed:
				in.kind = opBook
				add(c, in, output{}, o.call, o.ret)
			case o.outcome == loadgen.OutcomeUnavailable && o.named[name]:
				in.kind = opTaken
				add(c, in, output{}, o.call, o.ret)
			case o.outcome == loadgen.OutcomeUnknown:
				in.kind, in.cross = opUnknown, o.cross
				add(c, in, output{}, o.call, end)
				if o.cross {
					in.kind = opRelease
					add(c, in, output{}, o.call, end)
				}
			case o.cross:
				in.kind = opHold
				add(c, in, output{}, o.call, o.ret)
				in.kind = opRelease
				add(c, in, output{}, o.call, end)
			}
		}
	}

	// Listings: reads of every seat in an order, and of every other seat the
	// listings of its section disagree about. A seat no order names and all
	// listings agree on has nothing to check.
	perSection := map[string][]loadgen.Listing{}
	for _, l := range listings {
		sec := l.Event + "/" + l.Section
		perSection[sec] = append(perSection[sec], l)
	}
	for sec, ls := range perSection {
		listed := make([]map[string]bool, len(ls))
		count := map[string]int{}
		for i, l := range ls {
			listed[i] = make(map[string]bool, len(l.Free))
			for _, id := range l.Free {
				name := sec + "/" + id
				if !listed[i][name] {
					listed[i][name] = true
					count[name]++
				}
			}
		}
		for name, n := range count {
			if n < len(ls) && seats[name] == nil {
				seat(name, ls[0].CallNs) // free in some listings only, and no order explains it
			}
		}
		for _, c := range bySection[sec] {
			for i, l := range ls {
				add(c, input{kind: opRead, final: l.Phase == loadgen.PhaseFinal, label: l.Phase}, output{free: listed[i][c.seat]}, l.CallNs, l.ReturnNs)
			}
		}
	}

	checks := make([]*seatCheck, 0, len(seats))
	for _, c := range seats {
		if len(c.ops) > 0 {
			checks = append(checks, c)
			r.Operations += len(c.ops)
		}
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].seat < checks[j].seat })
	r.Seats = len(checks)
	r.model = Model(requests)

	jobs := make(chan *seatCheck)
	var wg sync.WaitGroup
	for range opt.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				left := time.Until(deadline)
				if left <= 0 {
					c.result = porcupine.Unknown // out of time before it started
					continue
				}
				c.result, c.info = porcupine.CheckOperationsVerbose(r.model, c.ops, left)
				if c.result != porcupine.Illegal {
					c.info = porcupine.LinearizationInfo{}
				}
			}
		}()
	}
	for _, c := range checks {
		jobs <- c
	}
	close(jobs)
	wg.Wait()

	var bad []*seatCheck
	for _, c := range checks {
		switch c.result {
		case porcupine.Illegal:
			bad = append(bad, c)
		case porcupine.Unknown:
			r.TimedOut = append(r.TimedOut, c.seat)
		}
	}
	sort.SliceStable(bad, func(i, j int) bool { return bad[i].first < bad[j].first })
	for _, c := range bad {
		r.Illegal = append(r.Illegal, c.seat)
	}
	if len(bad) > 0 {
		r.first = bad[0]
	}
	r.Elapsed = time.Since(start)
	return r
}
