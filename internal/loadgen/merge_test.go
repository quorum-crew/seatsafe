package loadgen

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// synthetic is one order as a Recorder and a series see it.
type synthetic struct {
	res      Result
	intended time.Time
	dropped  bool
}

// syntheticResults makes n orders around the window [from, to): some before
// it, some after, some finishing outside it, with every outcome and both
// paths, and latencies from microseconds to more than a histogram holds.
func syntheticResults(n int, from, to time.Time) []synthetic {
	rng := rand.New(rand.NewPCG(3, 4))
	outcomes := []string{OutcomeConfirmed, OutcomeConfirmed, OutcomeConfirmed, OutcomeUnavailable, OutcomeUnavailable, OutcomeAborted, OutcomeInvalid, OutcomeUnknown}
	span := to.Sub(from) + 400*time.Millisecond
	out := make([]synthetic, n)
	for i := range out {
		s := &out[i]
		s.intended = from.Add(-200*time.Millisecond + time.Duration(rng.Int64N(int64(span))))
		s.dropped = rng.IntN(50) == 0
		latency := 50*time.Microsecond + time.Duration(rng.ExpFloat64()*float64(20*time.Millisecond))
		if rng.IntN(500) == 0 {
			latency = 70 * time.Second
		}
		s.res = Result{RequestID: fmt.Sprintf("r%d", i), Outcome: outcomes[rng.IntN(len(outcomes))], Replayed: rng.IntN(20) == 0,
			Retries: rng.IntN(3), Start: s.intended, Return: s.intended.Add(latency)}
		s.res.Order.Seats = make([]Seat, 1+rng.IntN(4))
		s.res.Order.Cross = rng.IntN(5) == 0
	}
	return out
}

func feed(r *Recorder, items []synthetic) {
	for _, s := range items {
		r.Attempt(s.intended, s.dropped)
		if !s.dropped {
			r.Done(s.res, s.intended)
		}
	}
}

// fakeWorker is the output directory of one worker, written by hand.
type fakeWorker struct {
	id, n     int
	started   int64 // unix ns
	rec       *Recorder
	seconds   []Second
	attempts  []Attempt
	listings  []Listing
	noHistory bool
	change    func(*Summary)
}

func (f fakeWorker) write(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(0, f.started)
	if f.rec == nil {
		f.rec = NewRecorder(base, base.Add(10*time.Second))
	}
	c := Defaults()
	c.WorkerID, c.Workers, c.Rate, c.Users, c.Duration, c.Label = f.id, f.n, 100, 7, 10*time.Second, "w"
	sum := &Summary{Config: c, StartedUnixNs: f.started, Seats: 12, Results: f.rec.Results()}
	if f.change != nil {
		f.change(sum)
	}
	h, err := f.rec.Histograms(base)
	if err != nil {
		t.Fatal(err)
	}
	var seconds, history bytes.Buffer
	for _, s := range f.seconds {
		b, _ := json.Marshal(s)
		seconds.Write(append(b, '\n'))
	}
	for _, a := range f.attempts {
		b, _ := json.Marshal(a)
		history.Write(append(b, '\n'))
	}
	for _, l := range f.listings {
		b, _ := listingLine(l)
		history.Write(append(b, '\n'))
	}
	for name, err := range map[string]error{
		"summary.json":         writeJSON(filepath.Join(dir, "summary.json"), sum),
		"histograms.json":      writeJSON(filepath.Join(dir, "histograms.json"), h),
		"timeseries.hdr.jsonl": os.WriteFile(filepath.Join(dir, "timeseries.hdr.jsonl"), seconds.Bytes(), 0o644),
	} {
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if !f.noHistory {
		if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), history.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// writeSnapshot writes what Snapshot writes: listings with absolute times.
func writeSnapshot(t *testing.T, path, phase string, at int64, free map[Section][]string) string {
	t.Helper()
	var b bytes.Buffer
	for s, ids := range free {
		line, err := listingLine(Listing{phase, s.Event, s.ID, ids, at, at + int64(time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		b.Write(append(line, '\n'))
	}
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func readMergedHistory(t *testing.T, dir string) *History {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := ReadFullHistory(f)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// The same orders seen by one Recorder and split over k workers: the merged
// Results are those of the one, to the last digit.
func TestMergedResultsEqualSingleRun(t *testing.T) {
	from := time.Unix(1_700_000_000, 0)
	to := from.Add(10 * time.Second)
	items := syntheticResults(20000, from, to)
	single := NewRecorder(from, to)
	feed(single, items)
	want := single.Results()
	if want.Latency.Count < 10000 || len(want.LatencyOutcome) != 5 || len(want.LatencyPath) != 2 || want.Dropped == 0 || want.Latency.Max < 59000 {
		t.Fatalf("the synthetic orders do not cover enough: %+v", want)
	}
	for _, k := range []int{2, 3} {
		root := t.TempDir()
		var dirs []string
		for i := range k {
			rec := NewRecorder(from, to)
			var part []synthetic
			for j, s := range items {
				if j%k == i {
					part = append(part, s)
				}
			}
			feed(rec, part)
			// Every worker has its own time base; the window is the same.
			w := fakeWorker{id: i, n: k, started: from.UnixNano() - int64(time.Second) + int64(i)*137_000_000, rec: rec}
			dirs = append(dirs, w.write(t, filepath.Join(root, fmt.Sprintf("w%d", i))))
		}
		out := filepath.Join(root, "merged")
		sum, err := Merge(MergeInput{Out: out, Workers: dirs, Label: "all"})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sum.Results, want) {
			t.Fatalf("%d workers:\nmerged %+v\nsingle %+v", k, sum.Results, want)
		}
		// The files say the same.
		h, err := ReadHistograms(filepath.Join(out, "histograms.json"))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := h.Results(); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%d workers: histograms.json gives %+v, %v", k, got, err)
		}
		if h.StartedUnixNs != from.UnixNano()-int64(time.Second) || h.FromNs != int64(time.Second) || h.ToNs != int64(11*time.Second) {
			t.Fatalf("merged window: started %d, [%d, %d]", h.StartedUnixNs, h.FromNs, h.ToNs)
		}
		var back Summary
		if err := readJSON(filepath.Join(out, "summary.json"), &back); err != nil || !reflect.DeepEqual(back.Results, want) {
			t.Fatalf("summary.json: %v %+v", err, back.Results)
		}
		c := back.Config
		if c.Workers != k || c.WorkerID != 0 || c.Rate != float64(100*k) || c.Users != 7*k || c.Label != "all" || c.Out != out ||
			back.StartedUnixNs != h.StartedUnixNs || len(back.MergedFrom) != k || back.Audit != nil {
			t.Fatalf("merged summary: %+v", back)
		}
		if !strings.Contains(PrintSummary(sum), fmt.Sprintf("%-18s %10d", "all", want.Latency.Count)) {
			t.Fatalf("no latency row in:\n%s", PrintSummary(sum))
		}
	}
}

// One second of the time series, seen by one series and split over k: the
// merged row is the row of the one.
func TestMergedSecondEqualsSingleRun(t *testing.T) {
	from := time.Unix(1_700_000_000, 0)
	items := syntheticResults(3000, from, from.Add(time.Second))
	dir := t.TempDir()
	open := func(name string) *series {
		s, err := newSeries(filepath.Join(dir, name+".csv"), true)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.withHistograms(filepath.Join(dir, name+".hdr.jsonl"), time.Now()); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// row writes the one row and returns it as the two files have it.
	row := func(s *series, name string) (string, Second) {
		s.row(1)
		if err := s.w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := s.hw.Flush(); err != nil || s.herr != nil {
			t.Fatal(err, s.herr)
		}
		s.f.Close()
		s.hf.Close()
		b, err := os.ReadFile(filepath.Join(dir, name+".csv"))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		rows, err := ReadSeconds(filepath.Join(dir, name+".hdr.jsonl"))
		if err != nil || len(rows) != 1 || len(lines) != 2 || lines[0] != seriesHeader {
			t.Fatalf("%s: %v, %d rows, csv %q", name, err, len(rows), lines)
		}
		rows[0].EndNs = int64(time.Second) // a full second
		return lines[1], rows[0]
	}
	add := func(s *series, part []synthetic, inflight int64) {
		for _, x := range part {
			if !x.dropped {
				s.sent()
				s.done(x.res, x.intended)
			}
		}
		s.setInflight(inflight)
	}
	for _, k := range []int{2, 3} {
		name := fmt.Sprintf("single%d", k)
		single := open(name)
		add(single, items, int64(10*k+k*(k-1)/2))
		want, _ := row(single, name)
		if strings.Contains(want, ",0.000,") {
			t.Fatalf("no latency in the row %q", want)
		}

		parts := make([][]Second, k)
		offsets := make([]int64, k)
		for i := range k {
			var part []synthetic
			for j, x := range items {
				if j%k == i {
					part = append(part, x)
				}
			}
			name := fmt.Sprintf("part%d-%d", k, i)
			s := open(name)
			add(s, part, int64(10+i))
			_, sec := row(s, name)
			parts[i] = []Second{sec}
			offsets[i] = int64(i) * int64(150*time.Millisecond) // started a little apart, same second
		}
		merged, hists, err := mergeSeconds(parts, offsets)
		if err != nil || len(merged) != 1 {
			t.Fatalf("%v, %d rows", err, len(merged))
		}
		if got := seriesRow(merged[0], hists[0]); got != want {
			t.Fatalf("%d workers:\nmerged %s\nsingle %s", k, got, want)
		}
		// The merged line holds the merged histogram.
		if h, err := decodeHist(merged[0].Hist); err != nil || !h.Equals(hists[0]) {
			t.Fatalf("merged histogram does not decode to itself: %v", err)
		}
		// A worker that started 0.7 s late: its first second is mostly the
		// run's second one.
		offsets[k-1] = int64(700 * time.Millisecond)
		merged, _, err = mergeSeconds(parts, offsets)
		if err != nil || len(merged) != 2 || merged[1].Sent != parts[k-1][0].Sent || merged[0].Sent+merged[1].Sent != sumSent(parts) ||
			merged[1].InFlight != int64(10+k-1) || merged[1].Second != 2 {
			t.Fatalf("late worker: %v %+v", err, merged)
		}
	}
}

func sumSent(parts [][]Second) int64 {
	var n int64
	for _, p := range parts {
		for _, s := range p {
			n += s.Sent
		}
	}
	return n
}

func TestMergeRejects(t *testing.T) {
	const started = int64(1_700_000_000_000_000_000)
	pair := func(t *testing.T, a, b fakeWorker) MergeInput {
		root := t.TempDir()
		return MergeInput{Out: filepath.Join(root, "out"), Workers: []string{a.write(t, filepath.Join(root, "wa")), b.write(t, filepath.Join(root, "wb"))}}
	}
	w := func(id, n int) fakeWorker { return fakeWorker{id: id, n: n, started: started} }
	with := func(f fakeWorker, change func(*Summary)) fakeWorker { f.change = change; return f }

	// A request id in the histories of two workers.
	a, b := w(0, 2), w(1, 2)
	a.attempts = []Attempt{attempt("only-a", "e/s/a", 1, 2, OutcomeConfirmed), attempt("dup-7", "e/s/b", 3, 4, OutcomeUnavailable)}
	b.attempts = []Attempt{attempt("only-b", "e/s/c", 1, 2, OutcomeConfirmed), attempt("dup-7", "e/s/b", 5, 6, OutcomeUnavailable)}
	in := pair(t, a, b)
	if _, err := Merge(in); err == nil || !strings.Contains(err.Error(), "request id dup-7") || !strings.Contains(err.Error(), in.Workers[0]) || !strings.Contains(err.Error(), in.Workers[1]) {
		t.Fatalf("shared request id: %v", err)
	}
	// The same id twice in one worker is a retry.
	a.attempts = append(a.attempts, attempt("only-a", "e/s/a", 7, 8, OutcomeConfirmed))
	b.attempts = b.attempts[:1]
	if _, err := Merge(pair(t, a, b)); err != nil {
		t.Fatalf("retries within a worker: %v", err)
	}

	for name, c := range map[string]struct {
		a, b fakeWorker
		want string
	}{
		"the same worker twice":  {w(1, 2), w(1, 2), "both worker 1"},
		"a worker of three":      {w(0, 3), w(1, 3), "worker 0 of 3, but 2 worker directories"},
		"different counts":       {w(0, 2), w(1, 3), "worker 1 of 3"},
		"single runs":            {w(0, 1), w(0, 1), "worker 0 of 1, but 2"},
		"different workloads":    {w(0, 2), with(w(1, 2), func(s *Summary) { s.Config.Workload = WorkloadFlash }), "open flash"},
		"different modes":        {w(0, 2), with(w(1, 2), func(s *Summary) { s.Config.Mode = ModeClosed }), "closed uniform"},
		"different durations":    {w(0, 2), with(w(1, 2), func(s *Summary) { s.Config.Duration = time.Minute }), "1m0s"},
		"a ramp":                 {with(w(0, 2), func(s *Summary) { s.Config.Mode = ModeRamp }), w(1, 2), "ramp runs cannot be merged"},
		"an old worker dir":      {w(0, 2), with(w(1, 2), func(s *Summary) { s.StartedUnixNs = 0 }), "started_unix_ns"},
		"worker id out of range": {w(0, 2), with(w(1, 2), func(s *Summary) { s.Config.WorkerID = 5 }), "worker id 5"},
	} {
		if _, err := Merge(pair(t, c.a, c.b)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := Merge(MergeInput{Out: t.TempDir()}); err == nil {
		t.Error("no worker directories accepted")
	}
	if _, err := Merge(MergeInput{Out: t.TempDir(), Workers: []string{filepath.Join(t.TempDir(), "missing")}}); err == nil {
		t.Error("missing worker directory accepted")
	}
}

// Two workers started 250 ms apart, and snapshots with absolute times: all of
// it ends up on the time base of the worker that started first.
func TestMergeTimeBases(t *testing.T) {
	const started = int64(1_700_000_000_000_000_000)
	const msNs = int64(time.Millisecond)
	root := t.TempDir()
	sec := Section{"e", "s"}
	early := fakeWorker{id: 1, n: 2, started: started, // the worker ids are not in starting order
		attempts: []Attempt{{ClientID: 1<<workerShift | 3, RequestID: "early", Seats: []string{"e/s/a"}, CallNs: 100 * msNs, ReturnNs: 120 * msNs, Outcome: OutcomeConfirmed, Code: "OK"}},
		listings: []Listing{{PhaseBrowse, "e", "s", []string{"a", "b"}, 50 * msNs, 60 * msNs}},
		seconds:  []Second{{Second: 1, EndNs: 1000 * msNs, Sent: 1, Hist: mustEncode(t)}},
	}
	late := fakeWorker{id: 0, n: 2, started: started + 250*msNs,
		attempts: []Attempt{{ClientID: 3, RequestID: "late", Seats: []string{"e/s/b"}, CallNs: 10 * msNs, ReturnNs: 30 * msNs, Outcome: OutcomeConfirmed, Code: "OK"}},
		listings: []Listing{{PhaseBrowse, "e", "s", []string{"b"}, 20 * msNs, 25 * msNs}},
		seconds:  []Second{{Second: 1, EndNs: 1000 * msNs, Sent: 2, Hist: mustEncode(t)}},
	}
	in := MergeInput{
		Out:      filepath.Join(root, "out"),
		Workers:  []string{early.write(t, filepath.Join(root, "early")), late.write(t, filepath.Join(root, "late"))},
		Baseline: writeSnapshot(t, filepath.Join(root, "base.jsonl"), PhaseBaseline, started-2000*msNs, map[Section][]string{sec: {"a", "b", "c"}}),
		Final:    writeSnapshot(t, filepath.Join(root, "final.jsonl"), PhaseFinal, started+900*msNs, map[Section][]string{sec: {"c"}}),
	}
	sum, err := Merge(in)
	if err != nil {
		t.Fatal(err)
	}
	if sum.StartedUnixNs != started || sum.HistoryAttempts != 2 || sum.Audit == nil || !sum.Audit.Pass {
		t.Fatalf("%+v audit %+v", sum, sum.Audit)
	}
	h := readMergedHistory(t, in.Out)
	wantAttempts := []Attempt{
		{ClientID: 1<<workerShift | 3, RequestID: "early", Seats: []string{"e/s/a"}, CallNs: 100 * msNs, ReturnNs: 120 * msNs, Outcome: OutcomeConfirmed, Code: "OK"},
		{ClientID: 3, RequestID: "late", Seats: []string{"e/s/b"}, CallNs: 260 * msNs, ReturnNs: 280 * msNs, Outcome: OutcomeConfirmed, Code: "OK"},
	}
	wantListings := []Listing{
		{PhaseBaseline, "e", "s", []string{"a", "b", "c"}, -2000 * msNs, -1999 * msNs},
		{PhaseBrowse, "e", "s", []string{"a", "b"}, 50 * msNs, 60 * msNs},
		{PhaseBrowse, "e", "s", []string{"b"}, 270 * msNs, 275 * msNs},
		{PhaseFinal, "e", "s", []string{"c"}, 900 * msNs, 901 * msNs},
	}
	if !reflect.DeepEqual(h.Attempts, wantAttempts) || !reflect.DeepEqual(h.Listings, wantListings) {
		t.Fatalf("merged history:\n%+v\n%+v", h.Attempts, h.Listings)
	}
	// In the file: the baseline, then calls in order, then the final read.
	b, err := os.ReadFile(filepath.Join(in.Out, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var x struct {
			Phase   string `json:"phase"`
			Request string `json:"request_id"`
		}
		if err := json.Unmarshal([]byte(l), &x); err != nil {
			t.Fatal(err)
		}
		order = append(order, x.Phase+x.Request)
	}
	if got := strings.Join(order, " "); got != "baseline browse early late browse final" {
		t.Fatalf("order of the lines: %s", got)
	}
	// The two workers' first seconds are one merged second.
	rows, err := ReadSeconds(filepath.Join(in.Out, "timeseries.hdr.jsonl"))
	if err != nil || len(rows) != 1 || rows[0].Sent != 3 {
		t.Fatalf("merged seconds: %v %+v", err, rows)
	}
	if csv, err := os.ReadFile(filepath.Join(in.Out, "timeseries.csv")); err != nil || string(csv) != seriesHeader+"\n1,3,0,0,0,0.000,0.000,0\n" {
		t.Fatalf("timeseries.csv: %v %q", err, csv)
	}

	// Client ids of workers that did not make them distinct are made so.
	clash := late
	clash.id, clash.attempts = 1, []Attempt{{ClientID: 3, RequestID: "other", Seats: []string{"e/s/c"}, CallNs: 1, ReturnNs: 2, Outcome: OutcomeUnavailable, Code: "OK"}}
	in = MergeInput{Out: filepath.Join(root, "out2"), Workers: []string{late.write(t, filepath.Join(root, "l0")), clash.write(t, filepath.Join(root, "l1"))}}
	if _, err := Merge(in); err != nil {
		t.Fatal(err)
	}
	h = readMergedHistory(t, in.Out)
	if len(h.Attempts) != 2 || h.Attempts[0].ClientID != 1<<workerShift|3 || h.Attempts[1].ClientID != 3 {
		t.Fatalf("client ids: %+v", h.Attempts)
	}
}

func mustEncode(t *testing.T) string {
	t.Helper()
	s, err := encodeHist(newHist())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The audit over the orders of every worker.
func TestMergeAudit(t *testing.T) {
	const started = int64(1_700_000_000_000_000_000)
	sec := Section{"e", "s"}
	yes, no := true, false
	order := func(req string, call int64, outcome, code string, cross *bool, seats ...string) Attempt {
		return Attempt{RequestID: req, Seats: seats, CallNs: call, ReturnNs: call + 5, Outcome: outcome, Code: code, Cross: cross}
	}
	merge := func(t *testing.T, a, b fakeWorker, after []string) (*Summary, string, error) {
		root := t.TempDir()
		in := MergeInput{
			Out:      filepath.Join(root, "out"),
			Workers:  []string{a.write(t, filepath.Join(root, "w0")), b.write(t, filepath.Join(root, "w1"))},
			Baseline: writeSnapshot(t, filepath.Join(root, "base.jsonl"), PhaseBaseline, started-int64(time.Second), map[Section][]string{sec: {"a", "b", "c", "d", "x"}, {"e", "t"}: {"y"}}),
			Final:    writeSnapshot(t, filepath.Join(root, "final.jsonl"), PhaseFinal, started+int64(time.Minute), map[Section][]string{sec: after, {"e", "t"}: {"y"}}),
		}
		sum, err := Merge(in)
		return sum, in.Out, err
	}
	a := fakeWorker{id: 0, n: 2, started: started, attempts: []Attempt{
		order("a1", 10, OutcomeConfirmed, "OK", &no, "e/s/a"),
		order("a2", 20, "", "Unavailable", &no, "e/s/b"), // retried,
		order("a2", 30, OutcomeConfirmed, "OK", &no, "e/s/b"),
		order("a3", 40, "", "DeadlineExceeded", &no, "e/s/c"), // never answered: UNKNOWN, and it booked
		order("a4", 50, "", "InvalidArgument", &no, "e/s/x"),
	}}
	b := fakeWorker{id: 1, n: 2, started: started + 1000, attempts: []Attempt{
		order("b1", 10, OutcomeUnavailable, "OK", &no, "e/s/a"),
		order("b2", 20, OutcomeAborted, "OK", &yes, "e/s/x", "e/t/y"),
	}}

	// A clean run.
	sum, out, err := merge(t, a, b, []string{"d", "x"})
	if err != nil {
		t.Fatal(err)
	}
	au := sum.Audit
	if !au.Pass || au.Orders != 6 || au.ConfirmedSeats != 2 || au.UnknownOrders != 1 || au.SeatsTaken != 3 || !sum.Config.Audit {
		t.Fatalf("clean run: %+v", au)
	}
	var onDisk Audit
	if err := readJSON(filepath.Join(out, "audit.json"), &onDisk); err != nil || !reflect.DeepEqual(&onDisk, au) {
		t.Fatalf("audit.json: %v %+v", err, onDisk)
	}
	if !strings.Contains(PrintSummary(sum), "audit PASS: 6 orders") {
		t.Fatalf("%s", PrintSummary(sum))
	}

	// The orders are what the load generator itself would have audited.
	orders, err := AuditOrders(append(append([]Attempt{}, a.attempts...), b.attempts...))
	want := []AuditOrder{
		{"a1", []Seat{{"e", "s", "a"}}, OutcomeConfirmed, false},
		{"a2", []Seat{{"e", "s", "b"}}, OutcomeConfirmed, false},
		{"a3", []Seat{{"e", "s", "c"}}, OutcomeUnknown, false},
		{"a4", []Seat{{"e", "s", "x"}}, OutcomeInvalid, false},
		{"b1", []Seat{{"e", "s", "a"}}, OutcomeUnavailable, false},
		{"b2", []Seat{{"e", "s", "x"}, {"e", "t", "y"}}, OutcomeAborted, true},
	}
	if err != nil || !reflect.DeepEqual(orders, want) {
		t.Fatalf("AuditOrders: %v\n%+v", err, orders)
	}
	if _, err := AuditOrders([]Attempt{{RequestID: "r", Seats: []string{"no-slashes"}}}); err == nil {
		t.Fatal("a seat that is not event/section/seat was accepted")
	}

	// Seat a CONFIRMED by an order of each worker.
	double := b
	double.attempts = append([]Attempt{}, b.attempts...)
	double.attempts[0] = order("b1", 10, OutcomeConfirmed, "OK", &no, "e/s/a")
	sum, _, err = merge(t, a, double, []string{"d", "x"})
	if err != nil {
		t.Fatal(err)
	}
	if au := sum.Audit; au.Pass || au.DoubleBooked != 1 || au.Unsettled() || !strings.Contains(strings.Join(au.Samples, "\n"), "seat e/s/a CONFIRMED by a1 and b1") {
		t.Fatalf("double booking across workers: %+v", au)
	}
	if !strings.Contains(PrintSummary(sum), "audit FAIL") {
		t.Fatalf("%s", PrintSummary(sum))
	}

	// The aborted cross-shard order's seat is still held in the final
	// snapshot: unsettled, a later snapshot may pass.
	sum, _, err = merge(t, a, b, []string{"d"})
	if err != nil {
		t.Fatal(err)
	}
	if au := sum.Audit; au.Pass || !au.Unsettled() {
		t.Fatalf("held seat: %+v", au)
	}

	// No audit without the orders of every worker.
	silent := b
	silent.noHistory = true
	if _, _, err := merge(t, a, silent, []string{"d", "x"}); err == nil || !strings.Contains(err.Error(), "has no history.jsonl") || !strings.Contains(err.Error(), "w1") {
		t.Fatalf("worker without a history: %v", err)
	}
	// Without the snapshots there is no audit to ask for, and it merges.
	root := t.TempDir()
	sum, err = Merge(MergeInput{Out: filepath.Join(root, "out"), Workers: []string{a.write(t, filepath.Join(root, "w0")), silent.write(t, filepath.Join(root, "w1"))}})
	if err != nil || sum.Audit != nil || sum.Config.History || sum.Config.Audit {
		t.Fatalf("merge without snapshots: %v %+v", err, sum)
	}
	if _, err := os.Stat(filepath.Join(root, "out", "audit.json")); err == nil {
		t.Fatal("audit.json written without an audit")
	}
}

// orderDigest is a fingerprint of the first n orders of one generator.
func orderDigest(w *Workload, id uint64, n int) string {
	h := sha256.New()
	k := w.Worker(id)
	for range n {
		o, ok := k.Next()
		if !ok {
			fmt.Fprint(h, "end")
			break
		}
		fmt.Fprintln(h, o.Seats, o.Cross)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// A single load generator (worker 0 of 1, or no worker flags at all) makes
// the orders it made before there were workers. The fingerprints were taken
// from the code as it was then.
func TestSingleWorkerOrdersUnchanged(t *testing.T) {
	u := testUniverse(t)
	for workload, want := range map[string][2]string{
		WorkloadUniform: {"b6334febb592f41d", "696bb55292a65e15"},
		WorkloadFlash:   {"18aa11132c17d522", "5a92f66554ce5a49"},
		WorkloadFresh:   {"0537a0a8d946cea7", "6b62a22f6793ad35"},
	} {
		for _, workers := range []int{0, 1} {
			c := testConfig(t, workload)
			c.Seed, c.CrossShardFrac, c.Workers = 42, 0.2, workers
			w, err := NewWorkload(c, u, freeAll(u))
			if err != nil {
				t.Fatal(err)
			}
			if got := [2]string{orderDigest(w, 0, 3000), orderDigest(w, 5, 3000)}; got != want {
				t.Errorf("%s, workers=%d: orders changed: %v, want %v", workload, workers, got, want)
			}
		}
	}
}

// Fresh: every free seat belongs to exactly one worker, whatever the order of
// the rows and whatever else is free.
func TestFreshWorkersShareNoSeat(t *testing.T) {
	u := testUniverse(t)
	free := freeAll(u)
	for i, r := range u.Rows { // some seats are taken, also in the middle of rows
		if i%7 == 0 {
			delete(free, Seat{r.Event, r.Section, r.Seats[len(r.Seats)/2]})
		}
	}
	// offered returns every seat a worker's workload ever gives out.
	offered := func(u *Universe, free map[Seat]bool, id, n int, seed uint64, cross float64) map[Seat]bool {
		c := testConfig(t, WorkloadFresh)
		c.WorkerID, c.Workers, c.Seed, c.CrossShardFrac = id, n, seed, cross
		w, err := NewWorkload(c, u, free)
		if err != nil {
			t.Fatal(err)
		}
		got := map[Seat]bool{}
		for g := uint64(0); ; g++ { // several goroutines' generators
			o, ok := w.Worker(g % 4).Next()
			if !ok {
				return got
			}
			for _, s := range o.Seats {
				if got[s] {
					t.Fatalf("worker %d of %d offered %v twice", id, n, s)
				}
				got[s] = true
			}
		}
	}
	for _, n := range []int{2, 3} {
		union := map[Seat]int{}
		var sets []map[Seat]bool
		for id := range n {
			set := offered(u, free, id, n, 1, 0.3)
			if len(set) < len(free)/(n*2) {
				t.Errorf("worker %d of %d got %d of %d seats", id, n, len(set), len(free))
			}
			for s := range set {
				if !free[s] {
					t.Fatalf("worker %d of %d offered %v, which is not free", id, n, s)
				}
				if prev, dup := union[s]; dup {
					t.Fatalf("seat %v belongs to workers %d and %d of %d", s, prev, id, n)
				}
				union[s] = id
			}
			sets = append(sets, set)
		}
		if len(union) != len(free) {
			t.Fatalf("%d workers offer %d of %d free seats", n, len(union), len(free))
		}

		// The same split with the rows in another order, another seed and
		// other seats free: a row's worker depends on its name only.
		back := &Universe{Sections: u.Sections, Seats: u.Seats}
		for i := len(u.Rows) - 1; i >= 0; i-- {
			back.Rows = append(back.Rows, u.Rows[i])
		}
		for id := range n {
			for s := range offered(back, freeAll(u), id, n, 99, 0) {
				if owner, ok := union[s]; ok && owner != id {
					t.Fatalf("seat %v moved from worker %d to worker %d of %d", s, owner, id, n)
				}
			}
			if !reflect.DeepEqual(offered(back, free, id, n, 99, 0), sets[id]) {
				t.Fatalf("worker %d of %d: its seats depend on the order of the rows or the seed", id, n)
			}
		}
	}
}

// Flash and uniform: the workers share the seats and differ in their random
// streams.
func TestWorkersUseDifferentStreams(t *testing.T) {
	u := testUniverse(t)
	for _, workload := range []string{WorkloadFlash, WorkloadUniform} {
		digests := map[string]int{}
		for id := range 3 {
			c := testConfig(t, workload)
			c.WorkerID, c.Workers, c.Seed = id, 3, 42
			w, err := NewWorkload(c, u, nil)
			if err != nil {
				t.Fatal(err)
			}
			d := orderDigest(w, 0, 500) + orderDigest(w, 1, 500) // the open loop's stream and a closed user's
			if prev, dup := digests[d]; dup {
				t.Fatalf("%s: workers %d and %d of 3 make the same orders", workload, prev, id)
			}
			digests[d] = id
			if !reflect.DeepEqual(w.Touchable(), mustTouchable(t, workload, u)) {
				t.Fatalf("%s: worker %d books other sections", workload, id)
			}
		}
		// The same worker again makes the same orders: a run is repeatable.
		c := testConfig(t, workload)
		c.WorkerID, c.Workers, c.Seed = 2, 3, 42
		w, _ := NewWorkload(c, u, nil)
		if d := orderDigest(w, 0, 500) + orderDigest(w, 1, 500); digests[d] != 2 {
			t.Fatalf("%s: worker 2 is not repeatable", workload)
		}
	}
}

func mustTouchable(t *testing.T, workload string, u *Universe) []Section {
	t.Helper()
	w, err := NewWorkload(testConfig(t, workload), u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return w.Touchable()
}

func TestValidateWorkers(t *testing.T) {
	for _, c := range []struct {
		id, n int
		at    float64
		ok    bool
	}{
		{0, 1, 0, true}, {0, 0, 0, true}, {2, 3, 1.7e9, true}, {0, 4, 0.5, true},
		{1, 1, 0, false}, {1, 0, 0, false}, {3, 3, 0, false}, {-1, 2, 0, false}, {0, -1, 0, false}, {0, 2, -1, false},
	} {
		cfg := Defaults()
		cfg.WorkerID, cfg.Workers, cfg.StartAt = c.id, c.n, c.at
		if err := cfg.Validate(); (err == nil) != c.ok {
			t.Errorf("worker %d of %d, start at %v: %v", c.id, c.n, c.at, err)
		}
	}
	a, b := Defaults(), Defaults()
	b.WorkerID, b.Workers = 1, 2
	if a.clientBase() != 0 || a.streamSalt() != 0 || b.clientBase() == 0 || b.streamSalt() == 0 {
		t.Fatal("worker 0 must be the single load generator, and worker 1 must differ")
	}
}

func TestWaitUntil(t *testing.T) {
	at := time.Now().Add(150 * time.Millisecond)
	if err := waitUntil(context.Background(), float64(at.UnixNano())/1e9); err != nil || time.Now().Before(at.Add(-time.Millisecond)) {
		t.Fatalf("returned %v before the start time: %v", time.Until(at), err)
	}
	start := time.Now()
	if err := waitUntil(context.Background(), 0); err != nil || time.Since(start) > time.Second {
		t.Fatalf("no start time: %v after %v", err, time.Since(start))
	}
	if err := waitUntil(context.Background(), 1); err != nil || time.Since(start) > time.Second {
		t.Fatalf("a start time in the past: %v after %v", err, time.Since(start))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitUntil(ctx, float64(time.Now().Add(time.Hour).Unix())); err == nil {
		t.Fatal("a cancelled wait returned no error")
	}
}

// Two workers against a real router and a fake cluster, a snapshot before and
// one after, and the merge: the audit and the history checks agree that
// nothing went wrong.
func TestIntegrationWorkersMerged(t *testing.T) {
	for _, workload := range []string{WorkloadFlash, WorkloadFresh} {
		t.Run(workload, func(t *testing.T) {
			u, dial := testRouter(t)
			root := t.TempDir()
			base := testConfig(t, workload)
			base.Target, base.Conns, base.History, base.Workers = "passthrough:///router", 4, true, 2
			base.DialOptions = []grpc.DialOption{dial, grpc.WithTransportCredentials(insecure.NewCredentials())}
			base.Rate, base.Duration, base.BrowseRate = 200, 500*time.Millisecond, 40
			baseline, final := filepath.Join(root, "baseline.jsonl"), filepath.Join(root, "final.jsonl")
			if err := Snapshot(context.Background(), base, u, Env{}, PhaseBaseline, baseline); err != nil {
				t.Fatal(err)
			}
			startAt := time.Now().Add(300 * time.Millisecond)
			base.StartAt = float64(startAt.UnixNano()) / 1e9
			type result struct {
				sum *Summary
				err error
			}
			results := make([]chan result, 2)
			var dirs []string
			for id := range results {
				c := base
				c.WorkerID, c.Out = id, filepath.Join(root, fmt.Sprintf("w%d", id))
				dirs = append(dirs, c.Out)
				results[id] = make(chan result, 1)
				go func() {
					sum, err := Run(context.Background(), c, u, Env{})
					results[id] <- result{sum, err}
				}()
			}
			var attempted, browses int64
			for id, ch := range results {
				r := <-ch
				if r.err != nil {
					t.Fatalf("worker %d: %v", id, r.err)
				}
				if r.sum.Audit != nil || r.sum.Results.Attempted != 100 || r.sum.Config.WorkerID != id {
					t.Fatalf("worker %d: audit %+v, %+v", id, r.sum.Audit, r.sum.Results)
				}
				// Not before the common start (the flag is a float, and the
				// wall clock of a test machine may be set while this runs:
				// allow for both), and both soon after it.
				if late := r.sum.StartedUnixNs - startAt.UnixNano(); late < -int64(50*time.Millisecond) || late > int64(5*time.Second) {
					t.Fatalf("worker %d started %v after the start time", id, time.Duration(late))
				}
				if _, err := os.Stat(filepath.Join(dirs[id], "audit.json")); err == nil {
					t.Fatalf("worker %d wrote an audit.json", id)
				}
				for _, l := range readMergedHistory(t, dirs[id]).Listings {
					if l.Phase != PhaseBrowse {
						t.Fatalf("worker %d wrote a %s listing", id, l.Phase)
					}
				}
				attempted += r.sum.Results.Attempted
				browses += r.sum.Browses
			}
			if err := Snapshot(context.Background(), base, u, Env{}, PhaseFinal, final); err != nil {
				t.Fatal(err)
			}

			out := filepath.Join(root, "merged")
			sum, err := Merge(MergeInput{Out: out, Workers: []string{dirs[1], dirs[0]}, Baseline: baseline, Final: final})
			if err != nil {
				t.Fatal(err)
			}
			r := sum.Results
			if r.Attempted != attempted || r.Dropped != 0 || r.Outcomes[OutcomeUnknown] != 0 || r.Latency.Count != attempted || sum.Browses != browses || sum.Config.Rate != 400 {
				t.Fatalf("%+v", sum)
			}
			if sum.Audit == nil || !sum.Audit.Pass || int64(sum.Audit.Orders) != attempted || sum.Audit.ConfirmedSeats != sum.Audit.SeatsTaken || sum.Audit.ConfirmedSeats == 0 {
				t.Fatalf("audit %+v", sum.Audit)
			}
			switch workload {
			case WorkloadFresh: // no two workers ever asked for the same seat
				if r.Outcomes[OutcomeConfirmed] != attempted {
					t.Fatalf("fresh orders of two workers collided: %v", r.Outcomes)
				}
			case WorkloadFlash:
				if r.Outcomes[OutcomeConfirmed] == 0 || r.Outcomes[OutcomeConfirmed]+r.Outcomes[OutcomeUnavailable] != attempted {
					t.Fatalf("outcomes %v", r.Outcomes)
				}
			}
			if !strings.Contains(PrintSummary(sum), "audit PASS") {
				t.Fatalf("%s", PrintSummary(sum))
			}

			h := readMergedHistory(t, out)
			if int64(len(h.Attempts)) != sum.HistoryAttempts || int64(len(h.Attempts)) < attempted {
				t.Fatalf("%d attempts in the merged history, %d in the summary", len(h.Attempts), sum.HistoryAttempts)
			}
			for _, check := range Checks {
				if v := check.Run(h.Attempts); len(v) != 0 {
					t.Errorf("%s: %v", check.Name, v[0])
				}
			}
			var last int64
			users := map[uint64]bool{}
			requests := map[string]uint64{}
			for _, a := range h.Attempts {
				last = max(last, a.ReturnNs)
				if a.CallNs < 0 {
					t.Fatalf("attempt before the start of the run: %+v", a)
				}
				users[a.ClientID>>workerShift] = true
				if w, ok := requests[a.RequestID]; ok && w != a.ClientID>>workerShift {
					t.Fatalf("request %s was sent by two workers", a.RequestID)
				}
				requests[a.RequestID] = a.ClientID >> workerShift
			}
			if len(users) != 2 || int64(len(requests)) != attempted {
				t.Fatalf("client ids of %d workers, %d requests", len(users), len(requests))
			}
			phases := map[string]int{}
			for _, l := range h.Listings {
				phases[l.Phase]++
				switch l.Phase {
				case PhaseBaseline:
					if l.ReturnNs >= int64(50*time.Millisecond) {
						t.Fatalf("baseline listing at [%d, %d], after the start", l.CallNs, l.ReturnNs)
					}
				case PhaseFinal:
					// Wall clock against the workers' monotonic clocks:
					// allow for a clock that was set in between.
					if l.CallNs <= last-int64(50*time.Millisecond) {
						t.Fatalf("final listing called at %d, before the last answer at %d", l.CallNs, last)
					}
				}
			}
			sections := len(mustTouchable(t, workload, u))
			if phases[PhaseBaseline] != sections || phases[PhaseFinal] != sections || int64(phases[PhaseBrowse]) != browses {
				t.Fatalf("listings by phase: %v, want %d sections and %d browses", phases, sections, browses)
			}
			// The rows of the merged time series add up to the run.
			rows, err := ReadSeconds(filepath.Join(out, "timeseries.hdr.jsonl"))
			if err != nil || sumSent([][]Second{rows}) != attempted {
				t.Fatalf("merged time series: %v, %d sent", err, sumSent([][]Second{rows}))
			}
		})
	}
}
