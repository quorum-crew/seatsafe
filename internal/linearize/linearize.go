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
// # Two results
//
// The bookings verdict (Report.Verdict) is over the orders, the baseline and
// the final read: the two reads taken while the cluster is quiet. It is the
// pass/fail criterion.
//
// The browses, the listings taken while orders were running, are checked
// separately and never change the verdict. A seat whose bookings are
// linearizable is checked again with its browse reads added. If that fails,
// the browses are added one at a time in order of their calls: one that makes
// the seat's history illegal is counted as stale and left out, the others are
// kept. Report.StaleBrowses is the number of browses that are stale for at
// least one seat and Report.StaleSeats names those seats.
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
	"slices"
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
func step(s state, in input, out output) []state { return appendStep(nil, s, in, out) }

// appendStep appends to dst every state the seat can be in after op in state
// s; it appends nothing if op is illegal there.
func appendStep(dst []state, s state, in input, out output) []state {
	switch in.kind {
	case opBook:
		if s.kind == free {
			return append(dst, state{booked, in.order})
		}
		return dst
	case opTaken:
		if s.kind == free {
			return dst
		}
	case opHold:
		if s.kind == free {
			return append(dst, s, state{held, in.order})
		}
	case opRelease:
		if (s.kind == held || s.kind == heldUnknown) && s.order == in.order {
			return append(dst, state{free, 0})
		}
	case opUnknown:
		if s.kind == free {
			dst = append(dst, s, state{booked, in.order})
			if in.cross {
				dst = append(dst, state{heldUnknown, in.order})
			}
			return dst
		}
	case opRead:
		switch {
		case out.free != (s.kind == free):
			return dst
		case in.final && s.kind == held:
			return dst // an answered order's hold must be gone by the final read
		}
	default:
		return dst
	}
	return append(dst, s)
}

// states is what Porcupine sees as the model's state: the set of states the
// seat may be in, given the operations linearized so far. The model of one
// seat is nondeterministic (an UNKNOWN order may or may not have booked, a
// failed cross-shard order may or may not have held), and tracking the set
// makes it deterministic. Each member is stateSize bytes (kind, then order
// big-endian); members are sorted and distinct, so equal sets are equal
// strings.
type states string

const stateSize = 5

func (ss states) len() int { return len(ss) / stateSize }

func (ss states) at(i int) state {
	b := ss[i*stateSize : (i+1)*stateSize]
	return state{stateKind(b[0]), int32(uint32(b[1])<<24 | uint32(b[2])<<16 | uint32(b[3])<<8 | uint32(b[4]))}
}

func less(a, b state) bool {
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	return uint32(a.order) < uint32(b.order)
}

// encode sorts set in place, drops duplicates and returns it as states.
func encode(set []state) states {
	for i := 1; i < len(set); i++ {
		for j := i; j > 0 && less(set[j], set[j-1]); j-- {
			set[j], set[j-1] = set[j-1], set[j]
		}
	}
	b := make([]byte, 0, len(set)*stateSize)
	for i, x := range set {
		if i > 0 && x == set[i-1] {
			continue
		}
		o := uint32(x.order)
		b = append(b, byte(x.kind), byte(o>>24), byte(o>>16), byte(o>>8), byte(o))
	}
	return states(b)
}

// stepSet applies op to every state in ss. ok is false if op is illegal in
// all of them. same says that the set did not change, which is the common
// case (reads, refusals) and lets the caller keep the value it has.
func stepSet(ss states, in input, out output) (next states, ok, same bool) {
	var buf [16]state
	set := buf[:0]
	same = true
	for i := range ss.len() {
		s := ss.at(i)
		before := len(set)
		set = appendStep(set, s, in, out)
		if len(set) != before+1 || set[before] != s {
			same = false
		}
	}
	switch {
	case len(set) == 0:
		return "", false, false
	case same:
		return ss, true, true
	}
	return encode(set), true, false
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
	return porcupine.Model{
		Init: func() any { return encode([]state{{free, 0}, {booked, -1}}) },
		Step: func(s, in, out any) (bool, any) {
			next, ok, same := stepSet(s.(states), in.(input), out.(output))
			if same {
				return true, s // no new value to allocate
			}
			return ok, next
		},
		Equal: func(a, b any) bool { return a.(states) == b.(states) },
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
			ss := s.(states)
			parts := make([]string, ss.len())
			for i := range parts {
				switch x := ss.at(i); x.kind {
				case booked:
					parts[i] = "booked by " + name(x.order)
				case held, heldUnknown:
					parts[i] = "held by " + name(x.order)
				default:
					parts[i] = "free"
				}
			}
			return strings.Join(parts, " or ")
		},
	}
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
	// NoBrowses leaves out the listings taken while orders were running:
	// only the bookings verdict is computed, and no stale browses are
	// looked for.
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
	Browses    int // of them, taken while orders were running (0 with Options.NoBrowses)
	Seats      int // seats checked with Porcupine: every seat in an order, and every seat whose listings disagree
	Operations int // operations over all those seats, browse reads included

	// The bookings verdict: orders, baseline and final read.
	Illegal  []string // seats that are not linearizable, earliest first
	TimedOut []string // seats not decided within the timeout, by name

	// The browses: reported, never part of the verdict.
	BrowsesChecked  bool     // false with Options.NoBrowses
	StaleBrowses    int      // browses that are stale for at least one seat
	StaleSeats      []string // seats with a stale browse, by name
	BrowseUndecided []string // seats whose browses were not decided within the timeout, by name

	Elapsed time.Duration

	first      *seatCheck // the first illegal seat
	firstStale *seatCheck // the first seat with a stale browse
	model      porcupine.Model
}

// Verdict is the bookings verdict: Pass, Fail or Timeout. Stale browses do
// not change it.
func (r *Report) Verdict() string {
	switch {
	case len(r.Illegal) > 0:
		return Fail
	case len(r.TimedOut) > 0:
		return Timeout
	}
	return Pass
}

// StaleLine is the one-line report on the browses: "stale browses: N (seats
// ...)", then how many browses were checked.
func (r *Report) StaleLine() string {
	if !r.BrowsesChecked {
		return "stale browses: not checked"
	}
	s := fmt.Sprintf("stale browses: %d", r.StaleBrowses)
	if len(r.StaleSeats) > 0 {
		s += " (seats " + Sample(r.StaleSeats) + ")"
	}
	s += fmt.Sprintf("; %d browses checked", r.Browses)
	if n := len(r.BrowseUndecided); n > 0 {
		s += fmt.Sprintf("; %d seats not decided in time", n)
	}
	return s
}

// Sample lists up to five names.
func Sample(names []string) string {
	if len(names) > 5 {
		return strings.Join(names[:5], ", ") + fmt.Sprintf(", ... and %d more", len(names)-5)
	}
	return strings.Join(names, ", ")
}

// String is a one-line summary of the bookings verdict.
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
	r.describe(&b, r.first.ops, nil)
	return b.String()
}

// FirstStale describes the first seat with a stale browse: its name and its
// operations in order of their calls, the stale browses marked. Empty if
// there is none.
func (r *Report) FirstStale() string {
	c := r.firstStale
	if c == nil {
		return ""
	}
	ops := append([]porcupine.Operation(nil), c.ops...)
	stale := map[int]bool{}
	for _, br := range c.browses {
		if slices.Contains(c.stale, br.listing) {
			stale[len(ops)] = true
		}
		ops = append(ops, br.op)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "seat %s has %d stale browse(s); its %d operations (times in ms from the start of the run):\n", c.seat, len(c.stale), len(ops))
	r.describe(&b, ops, stale)
	return b.String()
}

// describe writes ops in order of their calls, one per line. marked holds the
// indexes of the operations to mark as stale.
func (r *Report) describe(b *strings.Builder, ops []porcupine.Operation, marked map[int]bool) {
	order := make([]int, len(ops))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return ops[order[i]].Call < ops[order[j]].Call })
	for _, i := range order {
		op, mark := ops[i], ""
		if marked[i] {
			mark = "  <- stale"
		}
		fmt.Fprintf(b, "  [%10.3f, %10.3f]  %s%s\n", float64(op.Call)/1e6, float64(op.Return)/1e6, r.model.DescribeOperation(op.Input, op.Output), mark)
	}
}

// seatCheck is one seat's operations and, after the check, its results.
type seatCheck struct {
	seat    string
	ops     []porcupine.Operation // the orders, the baseline and the final read
	browses []browseRead          // what the browses saw of this seat, in order of their calls
	first   int64                 // earliest call of an order on this seat
	result  porcupine.CheckResult // of ops: the bookings
	info    porcupine.LinearizationInfo

	stale     []int // listing numbers of the browses that are stale for this seat
	undecided bool  // the browses were not decided in time
}

// browseRead is one browse's read of one seat.
type browseRead struct {
	listing int // which browse, numbered over the listings of the history
	op      porcupine.Operation
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
	r.BrowsesChecked = !opt.NoBrowses
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
	// Reads by a browse are kept apart from the rest: they are not part of
	// the bookings verdict.
	type numbered struct {
		loadgen.Listing
		n int // position among the listings used
	}
	perSection := map[string][]numbered{}
	for i, l := range listings {
		sec := l.Event + "/" + l.Section
		perSection[sec] = append(perSection[sec], numbered{l, i})
	}
	for sec, ls := range perSection {
		sort.SliceStable(ls, func(i, j int) bool { return ls[i].CallNs < ls[j].CallNs })
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
				in, out := input{kind: opRead, final: l.Phase == loadgen.PhaseFinal, label: l.Phase}, output{free: listed[i][c.seat]}
				if l.Phase == loadgen.PhaseBrowse {
					c.browses = append(c.browses, browseRead{l.n, porcupine.Operation{Input: in, Output: out, Call: l.CallNs, Return: l.ReturnNs}})
					continue
				}
				add(c, in, out, l.CallNs, l.ReturnNs)
			}
		}
	}

	checks := make([]*seatCheck, 0, len(seats))
	for _, c := range seats {
		if len(c.ops)+len(c.browses) > 0 {
			checks = append(checks, c)
			r.Operations += len(c.ops) + len(c.browses)
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
				if c.result == porcupine.Ok && len(c.browses) > 0 {
					c.checkBrowses(r.model, deadline)
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
	staleListings := map[int]bool{}
	for _, c := range checks {
		switch c.result {
		case porcupine.Illegal:
			bad = append(bad, c)
		case porcupine.Unknown:
			r.TimedOut = append(r.TimedOut, c.seat)
		}
		if len(c.stale) > 0 {
			r.StaleSeats = append(r.StaleSeats, c.seat)
			if r.firstStale == nil {
				r.firstStale = c
			}
			for _, n := range c.stale {
				staleListings[n] = true
			}
		}
		if c.undecided {
			r.BrowseUndecided = append(r.BrowseUndecided, c.seat)
		}
	}
	r.StaleBrowses = len(staleListings)
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

// checkBrowses looks for the stale browses of a seat whose bookings are
// linearizable. The seat is checked with every browse read added; only if
// that fails are the browses added one at a time, in order of their calls,
// keeping those that leave the history legal and recording the others in
// c.stale.
func (c *seatCheck) checkBrowses(model porcupine.Model, deadline time.Time) {
	check := func(ops []porcupine.Operation) porcupine.CheckResult {
		left := time.Until(deadline)
		if left <= 0 {
			return porcupine.Unknown
		}
		return porcupine.CheckOperationsTimeout(model, ops, left)
	}
	with := func(ops []porcupine.Operation, extra ...browseRead) []porcupine.Operation {
		out := make([]porcupine.Operation, len(ops), len(ops)+len(extra))
		copy(out, ops)
		for _, b := range extra {
			b.op.ClientId = len(out)
			out = append(out, b.op)
		}
		return out
	}
	switch check(with(c.ops, c.browses...)) {
	case porcupine.Ok:
		return
	case porcupine.Unknown:
		c.undecided = true
		return
	}
	kept := c.ops
	for _, b := range c.browses {
		try := with(kept, b)
		switch check(try) {
		case porcupine.Ok:
			kept = try
		case porcupine.Illegal:
			c.stale = append(c.stale, b.listing)
		default:
			c.undecided = true
			return
		}
	}
}
