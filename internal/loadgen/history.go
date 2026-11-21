package loadgen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// Attempt is one line of history.jsonl: one call, retries included.
type Attempt struct {
	ClientID  uint64   `json:"client_id"`
	RequestID string   `json:"request_id"`
	Seats     []string `json:"seats"`
	CallNs    int64    `json:"call_ns"`   // monotonic, from the start of the run
	ReturnNs  int64    `json:"return_ns"` // monotonic, from the start of the run
	Outcome   string   `json:"outcome"`   // empty when the call returned an error
	Replayed  bool     `json:"replayed"`
	Code      string   `json:"code"` // gRPC code
	// Unavailable lists the seats a SEAT_UNAVAILABLE answer named. Histories
	// written before this field existed do not have it.
	Unavailable []string `json:"unavailable_seats,omitempty"`
	// Cross says whether the order's seats are on two or more shards (it went
	// through 2PC). Nil in histories written before this field existed.
	Cross *bool `json:"cross,omitempty"`
}

// Phases of a Listing.
const (
	PhaseBaseline = "baseline" // before the first order was sent
	PhaseBrowse   = "browse"   // while orders were running
	PhaseFinal    = "final"    // after the last order was answered (the audit's last read)
)

// Listing is a history line with "kind":"list": one answered
// ListAvailableSeats call for one section, with every seat it listed as free.
// A seat of that section it did not list was not free.
type Listing struct {
	Phase    string   `json:"phase"`
	Event    string   `json:"event"`
	Section  string   `json:"section"`
	Free     []string `json:"free"`      // seat ids
	CallNs   int64    `json:"call_ns"`   // from the start of the run; negative for the baseline
	ReturnNs int64    `json:"return_ns"` // from the start of the run
}

// History is everything in a history.jsonl.
type History struct {
	Attempts []Attempt
	Listings []Listing // empty in histories written before listings were recorded
}

// kindList marks a Listing line. Attempt lines have no kind.
const kindList = "list"

// line is the union of both kinds of history line.
type line struct {
	Kind string `json:"kind,omitempty"`
	Attempt
	Phase   string   `json:"phase,omitempty"`
	Event   string   `json:"event,omitempty"`
	Section string   `json:"section,omitempty"`
	Free    []string `json:"free,omitempty"`
}

// HistoryWriter appends attempts to history.jsonl. Safe for concurrent use.
type HistoryWriter struct {
	mu    sync.Mutex
	f     *os.File
	w     *bufio.Writer
	base  time.Time
	count int64
	err   error
}

// NewHistoryWriter creates path. Times are measured from base.
func NewHistoryWriter(path string, base time.Time) (*HistoryWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &HistoryWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), base: base}, nil
}

// Write records one call of order o.
func (h *HistoryWriter) Write(clientID uint64, requestID string, o Order, call, ret time.Time, resp *seatsafev1.BookSeatsResponse, code codes.Code) {
	cross := o.Cross
	a := Attempt{ClientID: clientID, RequestID: requestID, Seats: make([]string, len(o.Seats)),
		CallNs: call.Sub(h.base).Nanoseconds(), ReturnNs: ret.Sub(h.base).Nanoseconds(), Code: code.String(), Cross: &cross}
	for i, s := range o.Seats {
		a.Seats[i] = s.String()
	}
	if code == codes.OK {
		a.Outcome, a.Replayed = outcomeOf(resp.GetOutcome()), resp.GetReplayed()
		for _, r := range resp.GetUnavailableSeats() {
			a.Unavailable = append(a.Unavailable, Seat{r.GetEventId(), r.GetSectionId(), r.GetSeatId()}.String())
		}
	}
	b, err := json.Marshal(a)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.put(b, err)
	h.count++
}

// WriteListing records one answered ListAvailableSeats call. Listings are not
// counted by Close.
func (h *HistoryWriter) WriteListing(phase string, s Section, free []string, call, ret time.Time) {
	b, err := listingLine(Listing{phase, s.Event, s.ID, free, call.Sub(h.base).Nanoseconds(), ret.Sub(h.base).Nanoseconds()})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.put(b, err)
}

// listingLine is the history line of l.
func listingLine(l Listing) ([]byte, error) {
	if l.Free == nil {
		l.Free = []string{}
	}
	return json.Marshal(struct {
		Kind string `json:"kind"`
		Listing
	}{kindList, l})
}

// Widen returns a copy of attempts with every call moved earlier by d and
// every return moved later by d. A history merged from several machines has
// times from clocks that differ by a few milliseconds; a check that compares
// a return on one machine with a call on another (CheckRealTime) is run on
// the widened copy so that it only reports what no such difference explains.
func Widen(attempts []Attempt, d time.Duration) []Attempt {
	out := make([]Attempt, len(attempts))
	for i, a := range attempts {
		a.CallNs -= d.Nanoseconds()
		a.ReturnNs += d.Nanoseconds()
		out[i] = a
	}
	return out
}

// put appends one line. The caller holds h.mu.
func (h *HistoryWriter) put(b []byte, err error) {
	if err == nil {
		_, err = h.w.Write(append(b, '\n'))
	}
	if err != nil && h.err == nil {
		h.err = err
	}
}

// Close flushes the file and reports the first write error.
func (h *HistoryWriter) Close() (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.w.Flush(); err != nil && h.err == nil {
		h.err = err
	}
	if err := h.f.Close(); err != nil && h.err == nil {
		h.err = err
	}
	return h.count, h.err
}

// ReadHistory reads the attempts of a history.jsonl, skipping listings.
func ReadHistory(r io.Reader) ([]Attempt, error) {
	h, err := ReadFullHistory(r)
	if err != nil {
		return nil, err
	}
	return h.Attempts, nil
}

// ReadFullHistory reads a history.jsonl: attempts and listings. A file
// written before listings existed gives attempts only.
func ReadFullHistory(r io.Reader) (*History, error) {
	h := &History{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for n := 1; sc.Scan(); n++ {
		var l line
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return nil, fmt.Errorf("loadgen: history line %d: %w", n, err)
		}
		switch l.Kind {
		case "":
			h.Attempts = append(h.Attempts, l.Attempt)
		case kindList:
			h.Listings = append(h.Listings, Listing{l.Phase, l.Event, l.Section, l.Free, l.CallNs, l.ReturnNs})
		default:
			return nil, fmt.Errorf("loadgen: history line %d: unknown kind %q", n, l.Kind)
		}
	}
	return h, sc.Err()
}

// Violation is one broken rule, found offline.
type Violation struct {
	Check  string `json:"check"`
	Seat   string `json:"seat"`
	Detail string `json:"detail"`
}

// Check examines a history and returns what it finds wrong.
type Check struct {
	Name string
	Run  func([]Attempt) []Violation
	// Verdict is what a history with violations is called; "" means FAIL.
	Verdict string
}

// Checks are what cmd/checkhistory runs, in order.
//
// The linearizability check (a Porcupine model of one seat) is in
// internal/linearize; cmd/checkhistory -linearizable runs it.
var Checks = []Check{
	{Name: "no-double-booking", Run: CheckNoDoubleBooking},
	{Name: "real-time-order", Run: CheckRealTime},
	{Name: "idempotency", Run: CheckIdempotency, Verdict: VerdictIdempotency},
}

// confirmed is one order confirmed for one seat.
type confirmed struct {
	request   string
	call, ret int64
}

// confirmedBySeat groups the CONFIRMED orders of every seat. An order's call is
// its first attempt and its return is its earliest CONFIRMED answer.
func confirmedBySeat(history []Attempt) map[string][]confirmed {
	first := map[string]int64{}
	for _, a := range history {
		if t, ok := first[a.RequestID]; !ok || a.CallNs < t {
			first[a.RequestID] = a.CallNs
		}
	}
	type key struct{ seat, request string }
	seen := map[key]int{}
	out := map[string][]confirmed{}
	for _, a := range history {
		if a.Outcome != OutcomeConfirmed {
			continue
		}
		for _, s := range a.Seats {
			k := key{s, a.RequestID}
			if i, ok := seen[k]; ok {
				if a.ReturnNs < out[s][i].ret {
					out[s][i].ret = a.ReturnNs
				}
				continue
			}
			seen[k] = len(out[s])
			out[s] = append(out[s], confirmed{a.RequestID, first[a.RequestID], a.ReturnNs})
		}
	}
	return out
}

// CheckNoDoubleBooking is rule (a): no seat in two CONFIRMED orders with
// different request_ids.
func CheckNoDoubleBooking(history []Attempt) []Violation {
	var out []Violation
	for seat, cs := range confirmedBySeat(history) {
		if len(cs) > 1 {
			out = append(out, Violation{"no-double-booking", seat, fmt.Sprintf("%d orders CONFIRMED: %s and %s", len(cs), cs[0].request, cs[1].request)})
		}
	}
	sortViolations(out)
	return out
}

// CheckRealTime: if a CONFIRMED for one order returned before another order's
// call started, the later order must not be CONFIRMED for that seat.
func CheckRealTime(history []Attempt) []Violation {
	var out []Violation
	for seat, cs := range confirmedBySeat(history) {
		sort.Slice(cs, func(i, j int) bool { return cs[i].ret < cs[j].ret })
		for i, a := range cs {
			for _, b := range cs[i+1:] {
				if a.ret < b.call {
					out = append(out, Violation{"real-time-order", seat,
						fmt.Sprintf("%s CONFIRMED at %d, before %s was called at %d, and that was CONFIRMED too", a.request, a.ret, b.request, b.call)})
				}
			}
		}
	}
	sortViolations(out)
	return out
}

func sortViolations(v []Violation) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].Seat != v[j].Seat {
			return v[i].Seat < v[j].Seat
		}
		return v[i].Detail < v[j].Detail
	})
}
