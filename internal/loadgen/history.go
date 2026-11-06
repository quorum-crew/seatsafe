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

// Write records one call.
func (h *HistoryWriter) Write(clientID uint64, requestID string, seats []Seat, call, ret time.Time, resp *seatsafev1.BookSeatsResponse, code codes.Code) {
	a := Attempt{ClientID: clientID, RequestID: requestID, Seats: make([]string, len(seats)),
		CallNs: call.Sub(h.base).Nanoseconds(), ReturnNs: ret.Sub(h.base).Nanoseconds(), Code: code.String()}
	for i, s := range seats {
		a.Seats[i] = s.String()
	}
	if code == codes.OK {
		a.Outcome, a.Replayed = outcomeOf(resp.GetOutcome()), resp.GetReplayed()
	}
	line, err := json.Marshal(a)
	h.mu.Lock()
	defer h.mu.Unlock()
	if err == nil {
		_, err = h.w.Write(append(line, '\n'))
	}
	if err != nil && h.err == nil {
		h.err = err
	}
	h.count++
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

// ReadHistory reads history.jsonl.
func ReadHistory(r io.Reader) ([]Attempt, error) {
	var out []Attempt
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for line := 1; sc.Scan(); line++ {
		var a Attempt
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			return nil, fmt.Errorf("loadgen: history line %d: %w", line, err)
		}
		out = append(out, a)
	}
	return out, sc.Err()
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
}

// Checks are what cmd/checkhistory runs, in order.
//
// To add linearizability checking, write a Porcupine model of one seat
// (state: free or booked by a request_id; operations: book returning CONFIRMED
// or SEAT_UNAVAILABLE), turn each order's attempts into one operation with
// porcupine.Operation{ClientId, Input, Call, Output, Return} using its first
// call_ns and final return_ns, keep the seats of multi-seat orders together
// with a partition by seat, treat orders with no final outcome as open-ended,
// and append a Check here that calls porcupine.CheckOperations per seat.
var Checks = []Check{
	{"no-double-booking", CheckNoDoubleBooking},
	{"real-time-order", CheckRealTime},
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
