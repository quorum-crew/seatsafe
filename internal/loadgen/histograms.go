package loadgen

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// Histograms is histograms.json: everything a Recorder counted in its window,
// with the latency histograms themselves, so that the Results of several
// workers can be rebuilt exactly as if one Recorder had seen every order.
//
// A histogram is the HdrHistogram library's own V2 compressed encoding, which
// is a base64 string.
type Histograms struct {
	// StartedUnixNs is the run's time base (Summary.StartedUnixNs); FromNs
	// and ToNs are the measured window, from that time.
	StartedUnixNs int64 `json:"started_unix_ns"`
	FromNs        int64 `json:"from_ns"`
	ToNs          int64 `json:"to_ns"`

	Attempted         int64            `json:"attempted"`
	Dropped           int64            `json:"dropped"`
	CompletedInWindow int64            `json:"completed_in_window"`
	ConfirmedInWindow int64            `json:"confirmed_in_window"`
	ConfirmedSeats    int64            `json:"confirmed_seats_in_window"`
	Retries           int64            `json:"retries"`
	Replayed          int64            `json:"replayed"`
	Outcomes          map[string]int64 `json:"outcomes"`

	All       string            `json:"all"`
	ByOutcome map[string]string `json:"by_outcome"`
	ByPath    map[string]string `json:"by_path"`
}

func encodeHist(h *hdrhistogram.Histogram) (string, error) {
	b, err := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	return string(b), err
}

// decodeHist reads what encodeHist wrote into a histogram of our own shape,
// so that histograms from different files merge value for value.
func decodeHist(s string) (*hdrhistogram.Histogram, error) {
	if raw, err := base64.StdEncoding.DecodeString(s); err != nil || len(raw) < 8 {
		return nil, fmt.Errorf("loadgen: not an encoded histogram (%d characters)", len(s))
	}
	got, err := hdrhistogram.Decode([]byte(s))
	if err != nil {
		return nil, fmt.Errorf("loadgen: decoding a histogram: %w", err)
	}
	h := newHist()
	if dropped := h.Merge(got); dropped > 0 {
		return nil, fmt.Errorf("loadgen: a histogram holds %d values outside %d..%d µs", dropped, histMin, histMax)
	}
	return h, nil
}

func encodeHists(m map[string]*hdrhistogram.Histogram) (map[string]string, error) {
	out := make(map[string]string, len(m))
	for k, h := range m {
		s, err := encodeHist(h)
		if err != nil {
			return nil, err
		}
		out[k] = s
	}
	return out, nil
}

func decodeHists(m map[string]string) (map[string]*hdrhistogram.Histogram, error) {
	out := make(map[string]*hdrhistogram.Histogram, len(m))
	for k, s := range m {
		h, err := decodeHist(s)
		if err != nil {
			return nil, err
		}
		out[k] = h
	}
	return out, nil
}

// Histograms returns what r holds, with the window measured from base. Call
// it after the run has drained.
func (r *Recorder) Histograms(base time.Time) (*Histograms, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := &Histograms{
		StartedUnixNs: base.UnixNano(), FromNs: r.from.Sub(base).Nanoseconds(), ToNs: r.to.Sub(base).Nanoseconds(),
		Attempted: r.attempted, Dropped: r.dropped, CompletedInWindow: r.completedInWindow,
		ConfirmedInWindow: r.confirmedInWindow, ConfirmedSeats: r.confirmedSeats,
		Retries: r.retries, Replayed: r.replayed, Outcomes: make(map[string]int64, len(r.outcomes)),
	}
	for k, v := range r.outcomes {
		h.Outcomes[k] = v
	}
	var err error
	if h.All, err = encodeHist(r.all); err != nil {
		return nil, err
	}
	if h.ByOutcome, err = encodeHists(r.byOutcome); err != nil {
		return nil, err
	}
	if h.ByPath, err = encodeHists(r.byPath); err != nil {
		return nil, err
	}
	return h, nil
}

// Recorder rebuilds the Recorder h was taken from.
func (h *Histograms) Recorder() (*Recorder, error) {
	from := time.Unix(0, h.StartedUnixNs+h.FromNs)
	r := NewRecorder(from, from.Add(time.Duration(h.ToNs-h.FromNs)))
	r.attempted, r.dropped, r.completedInWindow = h.Attempted, h.Dropped, h.CompletedInWindow
	r.confirmedInWindow, r.confirmedSeats = h.ConfirmedInWindow, h.ConfirmedSeats
	r.retries, r.replayed = h.Retries, h.Replayed
	for k, v := range h.Outcomes {
		r.outcomes[k] = v
	}
	var err error
	if r.all, err = decodeHist(h.All); err != nil {
		return nil, err
	}
	if r.byOutcome, err = decodeHists(h.ByOutcome); err != nil {
		return nil, err
	}
	if r.byPath, err = decodeHists(h.ByPath); err != nil {
		return nil, err
	}
	return r, nil
}

// Results computes the summary of the window, as Recorder.Results does.
func (h *Histograms) Results() (Results, error) {
	r, err := h.Recorder()
	if err != nil {
		return Results{}, err
	}
	return r.Results(), nil
}

// absorb adds everything o counted to r. The window of r becomes one that
// starts at the earlier of the two starts and is as long as the longer of the
// two: every worker counts what falls in its own window, and the workers'
// windows have the same length, so that is the time the sums are over.
func (r *Recorder) absorb(o *Recorder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	length := max(r.to.Sub(r.from), o.to.Sub(o.from))
	if o.from.Before(r.from) {
		r.from = o.from
	}
	r.to = r.from.Add(length)
	r.attempted += o.attempted
	r.dropped += o.dropped
	r.completedInWindow += o.completedInWindow
	r.confirmedInWindow += o.confirmedInWindow
	r.confirmedSeats += o.confirmedSeats
	r.retries += o.retries
	r.replayed += o.replayed
	for k, v := range o.outcomes {
		r.outcomes[k] += v
	}
	r.all.Merge(o.all)
	for k, h := range o.byOutcome {
		hist(r.byOutcome, k).Merge(h)
	}
	for k, h := range o.byPath {
		hist(r.byPath, k).Merge(h)
	}
}

// ReadHistograms reads a histograms.json.
func ReadHistograms(path string) (*Histograms, error) {
	h := &Histograms{}
	if err := readJSON(path, h); err != nil {
		return nil, err
	}
	return h, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Second is one line of timeseries.hdr.jsonl: one row of timeseries.csv with
// the latency histogram of that second instead of its p50 and p99.
type Second struct {
	Second    int    `json:"second"`
	EndNs     int64  `json:"end_ns"` // when the row was written, from the run's time base
	Sent      int64  `json:"sent"`
	Completed int64  `json:"completed"`
	Confirmed int64  `json:"confirmed"`
	Errors    int64  `json:"errors"`
	InFlight  int64  `json:"in_flight"`
	Hist      string `json:"hist"`
}

// ReadSeconds reads a timeseries.hdr.jsonl.
func ReadSeconds(path string) ([]Second, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Second
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<26)
	for n := 1; sc.Scan(); n++ {
		var s Second
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, fmt.Errorf("%s: line %d: %w", path, n, err)
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

// seriesHeader is the first line of timeseries.csv.
const seriesHeader = "second,sent,completed,confirmed,errors,p50_ms,p99_ms,in_flight"

// seriesRow is one line of timeseries.csv.
func seriesRow(s Second, h *hdrhistogram.Histogram) string {
	p50, p99 := float64(h.ValueAtQuantile(50))/1000, float64(h.ValueAtQuantile(99))/1000
	return fmt.Sprintf("%d,%d,%d,%d,%d,%.3f,%.3f,%d", s.Second, s.Sent, s.Completed, s.Confirmed, s.Errors, p50, p99, s.InFlight)
}

// mergeSeconds puts the rows of several workers on one time base and adds
// them up second by second. offsets[i] is how long after the common base
// worker i started, in nanoseconds. A row counts for the wall-clock second
// its middle falls in. The histograms of the result are returned next to it.
func mergeSeconds(workers [][]Second, offsets []int64) ([]Second, []*hdrhistogram.Histogram, error) {
	var out []Second
	var hists []*hdrhistogram.Histogram
	for i, rows := range workers {
		inflight := map[int]int64{} // the worker's last in_flight of every merged second
		var prev int64
		for _, row := range rows {
			mid := offsets[i] + (prev+row.EndNs)/2
			prev = row.EndNs
			n := int(max(mid, 0) / int64(time.Second))
			for len(out) <= n {
				out = append(out, Second{Second: len(out) + 1, EndNs: int64(len(out)+1) * int64(time.Second)})
				hists = append(hists, newHist())
			}
			h, err := decodeHist(row.Hist)
			if err != nil {
				return nil, nil, err
			}
			hists[n].Merge(h)
			out[n].Sent += row.Sent
			out[n].Completed += row.Completed
			out[n].Confirmed += row.Confirmed
			out[n].Errors += row.Errors
			inflight[n] = row.InFlight
		}
		for n, v := range inflight {
			out[n].InFlight += v
		}
	}
	for n := range out {
		var err error
		if out[n].Hist, err = encodeHist(hists[n]); err != nil {
			return nil, nil, err
		}
	}
	return out, hists, nil
}
