package loadgen

import (
	"sort"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	histMin = 1          // 1 µs
	histMax = 60_000_000 // 60 s
	histSig = 3
)

func newHist() *hdrhistogram.Histogram { return hdrhistogram.New(histMin, histMax, histSig) }

func record(h *hdrhistogram.Histogram, d time.Duration) {
	_ = h.RecordValue(min(max(d.Microseconds(), histMin), histMax))
}

// Latency is one latency table row, in milliseconds.
type Latency struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p99_9_ms"`
	Max   float64 `json:"max_ms"`
}

func latencyOf(h *hdrhistogram.Histogram) Latency {
	ms := func(q float64) float64 { return float64(h.ValueAtQuantile(q)) / 1000 }
	return Latency{h.TotalCount(), ms(50), ms(90), ms(99), ms(99.9), float64(h.Max()) / 1000}
}

// Recorder counts one measured window. Latency belongs to orders whose
// intended time is in the window, however late they finish, so a backlog can't
// drop out of the numbers. Rates count completions inside the window.
type Recorder struct {
	mu       sync.Mutex
	from, to time.Time

	attempted, dropped int64
	completedInWindow  int64
	confirmedInWindow  int64
	confirmedSeats     int64
	retries, replayed  int64
	outcomes           map[string]int64
	all                *hdrhistogram.Histogram
	byOutcome          map[string]*hdrhistogram.Histogram
	byPath             map[string]*hdrhistogram.Histogram
}

// NewRecorder measures [from, to).
func NewRecorder(from, to time.Time) *Recorder {
	return &Recorder{from: from, to: to, outcomes: map[string]int64{}, all: newHist(),
		byOutcome: map[string]*hdrhistogram.Histogram{}, byPath: map[string]*hdrhistogram.Histogram{}}
}

func (r *Recorder) inWindow(t time.Time) bool { return !t.Before(r.from) && t.Before(r.to) }

// Attempt counts an order the scheduler meant to send at intended.
func (r *Recorder) Attempt(intended time.Time, dropped bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inWindow(intended) {
		r.attempted++
		if dropped {
			r.dropped++
		}
	}
}

// Done records a finished order. Latency runs from intended to the last return.
func (r *Recorder) Done(res Result, intended time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inWindow(res.Return) {
		r.completedInWindow++
		if res.Outcome == OutcomeConfirmed {
			r.confirmedInWindow++
			r.confirmedSeats += int64(len(res.Order.Seats))
		}
	}
	if !r.inWindow(intended) {
		return
	}
	r.outcomes[res.Outcome]++
	r.retries += int64(res.Retries)
	if res.Replayed {
		r.replayed++
	}
	d := res.Return.Sub(intended)
	record(r.all, d)
	record(hist(r.byOutcome, res.Outcome), d)
	path := "single"
	if res.Order.Cross {
		path = "cross"
	}
	record(hist(r.byPath, path), d)
}

func hist(m map[string]*hdrhistogram.Histogram, k string) *hdrhistogram.Histogram {
	h := m[k]
	if h == nil {
		h = newHist()
		m[k] = h
	}
	return h
}

// Results is the summary of a window.
type Results struct {
	DurationSec    float64            `json:"duration_s"`
	Attempted      int64              `json:"attempted"`
	AttemptedRate  float64            `json:"attempted_requests_per_s"`
	AchievedRate   float64            `json:"achieved_requests_per_s"`
	ConfirmedRate  float64            `json:"confirmed_orders_per_s"`
	ConfirmedSeats float64            `json:"confirmed_seats_per_s"`
	Outcomes       map[string]int64   `json:"outcomes"`
	Retries        int64              `json:"retries"`
	Replayed       int64              `json:"replayed"`
	Dropped        int64              `json:"dropped_by_loadgen"`
	Latency        Latency            `json:"latency"`
	LatencyOutcome map[string]Latency `json:"latency_by_outcome"`
	LatencyPath    map[string]Latency `json:"latency_by_path"`
	ConflictRate   float64            `json:"conflict_rate"`
	ErrorRate      float64            `json:"error_rate"`
}

// Results computes the summary. Call it after the run has drained.
func (r *Recorder) Results() Results {
	r.mu.Lock()
	defer r.mu.Unlock()
	secs := r.to.Sub(r.from).Seconds()
	out := Results{
		DurationSec: secs, Attempted: r.attempted, AttemptedRate: float64(r.attempted) / secs,
		AchievedRate: float64(r.completedInWindow) / secs, ConfirmedRate: float64(r.confirmedInWindow) / secs,
		ConfirmedSeats: float64(r.confirmedSeats) / secs, Outcomes: map[string]int64{}, Retries: r.retries,
		Replayed: r.replayed, Dropped: r.dropped, Latency: latencyOf(r.all),
		LatencyOutcome: map[string]Latency{}, LatencyPath: map[string]Latency{},
	}
	var total int64
	for k, v := range r.outcomes {
		out.Outcomes[k] = v
		total += v
	}
	if total > 0 {
		out.ConflictRate = float64(r.outcomes[OutcomeUnavailable]) / float64(total)
		out.ErrorRate = float64(r.outcomes[OutcomeUnknown]) / float64(total)
	}
	for k, h := range r.byOutcome {
		out.LatencyOutcome[k] = latencyOf(h)
	}
	for k, h := range r.byPath {
		out.LatencyPath[k] = latencyOf(h)
	}
	return out
}

// Step is one ramp step.
type Step struct {
	Rate    float64 `json:"rate"`
	Passed  bool    `json:"passed"`
	Why     string  `json:"why,omitempty"`
	Results Results `json:"results"`
}

// Summary is summary.json.
type Summary struct {
	Config          Config  `json:"config"`
	GitCommit       string  `json:"git_commit"`
	StartedAt       string  `json:"started_at"`
	Seats           int     `json:"seats_in_dataset"`
	Results         Results `json:"results"`
	Steps           []Step  `json:"steps,omitempty"`
	MaxSustainable  float64 `json:"max_sustainable_rate,omitempty"`
	RampStopped     string  `json:"ramp_stopped,omitempty"`
	SeatsExhausted  bool    `json:"seats_exhausted"`
	Audit           *Audit  `json:"audit,omitempty"`
	HistoryAttempts int64   `json:"history_attempts,omitempty"`
}

// sortedKeys returns m's keys in order.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
