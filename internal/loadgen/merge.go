package loadgen

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Snapshot reads the availability of every section the workload can book (the
// sections Run reads) and writes one history "list" line per section to path,
// with the given phase (PhaseBaseline or PhaseFinal). Unlike in a run's own
// history, call_ns and return_ns are absolute unix nanoseconds: Merge moves
// them to the time base of the run.
//
// Take a baseline before the first worker starts and a final one after the
// last has finished; one pair serves every worker of the run.
func Snapshot(ctx context.Context, cfg Config, u *Universe, env Env, phase, path string) error {
	if phase != PhaseBaseline && phase != PhaseFinal {
		return fmt.Errorf("loadgen: snapshot phase %q, want %s or %s", phase, PhaseBaseline, PhaseFinal)
	}
	probe, err := NewWorkload(Config{Workload: cfg.Workload, OrderSizes: cfg.OrderSizes, Seed: cfg.Seed, ZipfS: cfg.ZipfS}, u, nil)
	if err != nil {
		return err
	}
	if env.List == nil {
		pool, err := Dial(cfg.Target, cfg.Conns, cfg.DialOptions...)
		if err != nil {
			return err
		}
		defer pool.Close()
		env.List = pool.List
	}
	lists := &listRecorder{}
	if _, err := Availability(ctx, lists.wrap(env.List), probe.Touchable()); err != nil {
		return err
	}
	got := lists.take()
	sort.SliceStable(got, func(i, j int) bool {
		a, b := got[i].section, got[j].section
		if a.Event != b.Event {
			return a.Event < b.Event
		}
		return a.ID < b.ID
	})
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	// Measured from the start of unix time, the times are absolute.
	h, err := NewHistoryWriter(path, time.Unix(0, 0))
	if err != nil {
		return err
	}
	for _, l := range got {
		h.WriteListing(phase, l.section, l.free, l.call, l.ret)
	}
	_, err = h.Close()
	return err
}

// MergeInput says what Merge reads and where it writes.
type MergeInput struct {
	Out      string   // directory to write, made if it is not there
	Workers  []string // the output directory of every worker of the run
	Baseline string   // snapshot taken before the workers started; "" for none
	Final    string   // snapshot taken after they finished; "" for none
	Label    string   // label of the merged run; "" keeps worker 0's
}

// workerDir is what one worker left behind.
type workerDir struct {
	dir     string
	sum     Summary
	hist    *Histograms
	seconds []Second
	history *History // nil if the worker wrote none
	offset  int64    // how long after the common time base it started, ns
}

func readWorkerDir(dir string) (*workerDir, error) {
	w := &workerDir{dir: dir}
	if err := readJSON(filepath.Join(dir, "summary.json"), &w.sum); err != nil {
		return nil, fmt.Errorf("loadgen: merge: %w", err)
	}
	if w.sum.Config.Mode == ModeRamp {
		return nil, fmt.Errorf("loadgen: merge: %s is a ramp run, and ramp runs cannot be merged (every worker stops at its own rate)", dir)
	}
	if w.sum.StartedUnixNs == 0 {
		return nil, fmt.Errorf("loadgen: merge: %s/summary.json has no started_unix_ns: it was written by an older loadgen", dir)
	}
	var err error
	if w.hist, err = ReadHistograms(filepath.Join(dir, "histograms.json")); err != nil {
		return nil, fmt.Errorf("loadgen: merge: %w", err)
	}
	if w.seconds, err = ReadSeconds(filepath.Join(dir, "timeseries.hdr.jsonl")); err != nil {
		return nil, fmt.Errorf("loadgen: merge: %w", err)
	}
	f, err := os.Open(filepath.Join(dir, "history.jsonl"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return w, nil
	case err != nil:
		return nil, fmt.Errorf("loadgen: merge: %w", err)
	}
	defer f.Close()
	if w.history, err = ReadFullHistory(f); err != nil {
		return nil, fmt.Errorf("loadgen: merge: %s: %w", dir, err)
	}
	return w, nil
}

// readSnapshot reads a file written by Snapshot and moves its times, which
// are absolute, to base. Every line gets the given phase.
func readSnapshot(path, phase string, base int64) ([]Listing, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("loadgen: merge: %w", err)
	}
	defer f.Close()
	h, err := ReadFullHistory(f)
	if err != nil {
		return nil, fmt.Errorf("loadgen: merge: %s: %w", path, err)
	}
	if len(h.Listings) == 0 {
		return nil, fmt.Errorf("loadgen: merge: %s holds no listings", path)
	}
	for i := range h.Listings {
		l := &h.Listings[i]
		l.Phase, l.CallNs, l.ReturnNs = phase, l.CallNs-base, l.ReturnNs-base
	}
	sort.SliceStable(h.Listings, func(i, j int) bool { return h.Listings[i].CallNs < h.Listings[j].CallNs })
	return h.Listings, nil
}

// freeSeats is the set of seats the listings list as free.
func freeSeats(listings []Listing) map[Seat]bool {
	free := map[Seat]bool{}
	for _, l := range listings {
		for _, id := range l.Free {
			free[Seat{l.Event, l.Section, id}] = true
		}
	}
	return free
}

// parseSeat reads "event/section/seat", as Seat.String writes it.
func parseSeat(s string) (Seat, error) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 3 {
		return Seat{}, fmt.Errorf("loadgen: seat %q is not event/section/seat", s)
	}
	return Seat{parts[0], parts[1], parts[2]}, nil
}

// AuditOrders rebuilds from a history what the audit needs: one order per
// request id, in order of first appearance, with its final outcome. An order
// none of whose attempts was answered is UNKNOWN (INVALID if the call was
// refused as an invalid argument), as it is for the load generator itself.
func AuditOrders(attempts []Attempt) ([]AuditOrder, error) {
	index := map[string]int{}
	var out []AuditOrder
	answered := map[string]int64{} // return time of the answer used
	for _, a := range attempts {
		i, ok := index[a.RequestID]
		if !ok {
			i = len(out)
			index[a.RequestID] = i
			o := AuditOrder{RequestID: a.RequestID, Outcome: OutcomeUnknown}
			sections := map[Section]bool{}
			for _, name := range a.Seats {
				s, err := parseSeat(name)
				if err != nil {
					return nil, err
				}
				o.Seats = append(o.Seats, s)
				sections[Section{s.Event, s.Section}] = true
			}
			o.Cross = len(sections) > 1 // for a history from before the cross field
			out = append(out, o)
		}
		o := &out[i]
		if a.Cross != nil {
			o.Cross = *a.Cross
		}
		switch {
		case a.Outcome != "":
			if t, seen := answered[a.RequestID]; seen && t > a.ReturnNs {
				continue
			}
			answered[a.RequestID] = a.ReturnNs
			o.Outcome = a.Outcome
		case a.Code == "InvalidArgument" && o.Outcome == OutcomeUnknown:
			o.Outcome = OutcomeInvalid
		}
	}
	return out, nil
}

// Merge makes one run out of the output directories of its workers. It works
// offline, on the files alone:
//
//   - every time is moved to one base, the start of the worker that started
//     first (Summary.StartedUnixNs), and client ids are made distinct;
//   - history.jsonl is the baseline snapshot, then every attempt and browse
//     in order of their calls, then the final snapshot;
//   - histograms.json, timeseries.csv and timeseries.hdr.jsonl are the sums
//     of the workers', with the latency histograms merged value for value, so
//     the percentiles are those of one run that saw every order;
//   - summary.json has the Results of the merged window and worker 0's
//     config, with the rates, users and browse rates added up;
//   - with both snapshots, the audit is run once over every order and
//     written to audit.json.
//
// It refuses a request id that two workers both used, worker directories that
// are not exactly workers 0..n-1 of one run, and ramp runs.
func Merge(in MergeInput) (*Summary, error) {
	if len(in.Workers) == 0 {
		return nil, errors.New("loadgen: merge: no worker directories given")
	}
	if in.Out == "" {
		return nil, errors.New("loadgen: merge: no output directory given")
	}
	var ws []*workerDir
	for _, dir := range in.Workers {
		w, err := readWorkerDir(dir)
		if err != nil {
			return nil, err
		}
		ws = append(ws, w)
	}
	n := len(ws)
	byID := map[int]*workerDir{}
	for _, w := range ws {
		c, first := w.sum.Config, ws[0].sum.Config
		if c.workers() != n {
			return nil, fmt.Errorf("loadgen: merge: %s is worker %d of %d, but %d worker directories were given", w.dir, c.WorkerID, c.workers(), n)
		}
		if c.WorkerID < 0 || c.WorkerID >= n {
			return nil, fmt.Errorf("loadgen: merge: %s has worker id %d, outside 0..%d", w.dir, c.WorkerID, n-1)
		}
		if other := byID[c.WorkerID]; other != nil {
			return nil, fmt.Errorf("loadgen: merge: %s and %s are both worker %d", other.dir, w.dir, c.WorkerID)
		}
		byID[c.WorkerID] = w
		if c.Mode != first.Mode || c.Workload != first.Workload || c.Duration != first.Duration {
			return nil, fmt.Errorf("loadgen: merge: %s ran %s %s for %v, but %s ran %s %s for %v", w.dir, c.Mode, c.Workload, c.Duration, ws[0].dir, first.Mode, first.Workload, first.Duration)
		}
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i].sum.Config.WorkerID < ws[j].sum.Config.WorkerID })

	// One time base: the earliest start.
	base := ws[0].sum.StartedUnixNs
	for _, w := range ws {
		base = min(base, w.sum.StartedUnixNs)
	}
	for _, w := range ws {
		w.offset = w.sum.StartedUnixNs - base
	}

	// The history.
	var before, after, middle []Listing
	var err error
	if in.Baseline != "" {
		if before, err = readSnapshot(in.Baseline, PhaseBaseline, base); err != nil {
			return nil, err
		}
	}
	if in.Final != "" {
		if after, err = readSnapshot(in.Final, PhaseFinal, base); err != nil {
			return nil, err
		}
	}
	auditing := in.Baseline != "" && in.Final != ""
	var attempts []Attempt
	owner := map[string]*workerDir{}
	histories := 0
	for _, w := range ws {
		if w.history == nil {
			if auditing {
				return nil, fmt.Errorf("loadgen: merge: %s has no history.jsonl (run the worker with -history): the audit needs the orders of every worker", w.dir)
			}
			continue
		}
		histories++
		id := uint64(w.sum.Config.WorkerID)
		for _, a := range w.history.Attempts {
			if other, ok := owner[a.RequestID]; ok && other != w {
				return nil, fmt.Errorf("loadgen: merge: request id %s is in the histories of both %s and %s", a.RequestID, other.dir, w.dir)
			}
			owner[a.RequestID] = w
			a.CallNs += w.offset
			a.ReturnNs += w.offset
			if a.ClientID>>workerShift == 0 {
				a.ClientID |= id << workerShift
			}
			attempts = append(attempts, a)
		}
		for _, l := range w.history.Listings {
			l.CallNs += w.offset
			l.ReturnNs += w.offset
			switch {
			case l.Phase == PhaseBaseline && in.Baseline == "":
				before = append(before, l)
			case l.Phase == PhaseFinal && in.Final == "":
				after = append(after, l)
			case l.Phase == PhaseBrowse:
				middle = append(middle, l)
			}
		}
	}
	sort.SliceStable(attempts, func(i, j int) bool { return attempts[i].CallNs < attempts[j].CallNs })
	sort.SliceStable(middle, func(i, j int) bool { return middle[i].CallNs < middle[j].CallNs })

	if err := os.MkdirAll(in.Out, 0o755); err != nil {
		return nil, err
	}
	if histories > 0 || len(before)+len(after) > 0 {
		if err := writeMergedHistory(filepath.Join(in.Out, "history.jsonl"), before, attempts, middle, after); err != nil {
			return nil, err
		}
	}

	// The window and the time series.
	var rec *Recorder
	seconds, offsets := make([][]Second, n), make([]int64, n)
	for i, w := range ws {
		r, err := w.hist.Recorder()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", w.dir, err)
		}
		if rec == nil {
			rec = r
		} else {
			rec.absorb(r)
		}
		seconds[i], offsets[i] = w.seconds, w.offset
	}
	hists, err := rec.Histograms(time.Unix(0, base))
	if err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(in.Out, "histograms.json"), hists); err != nil {
		return nil, err
	}
	if err := writeMergedSeries(in.Out, seconds, offsets); err != nil {
		return nil, fmt.Errorf("loadgen: merge: time series: %w", err)
	}

	// The summary.
	cfg := ws[0].sum.Config
	cfg.Rate, cfg.Users, cfg.BrowseRate = 0, 0, 0
	sum := &Summary{GitCommit: ws[0].sum.GitCommit, StartedAt: time.Unix(0, base).UTC().Format(time.RFC3339),
		Seats: ws[0].sum.Seats, Results: rec.Results(), StartedUnixNs: base}
	for _, w := range ws {
		cfg.Rate += w.sum.Config.Rate
		cfg.Users += w.sum.Config.Users
		cfg.BrowseRate += w.sum.Config.BrowseRate
		sum.Browses += w.sum.Browses
		sum.SeatsExhausted = sum.SeatsExhausted || w.sum.SeatsExhausted
		sum.MergedFrom = append(sum.MergedFrom, w.dir)
	}
	cfg.WorkerID, cfg.Workers = 0, n
	cfg.Out, cfg.History, cfg.Audit = in.Out, histories == n, auditing
	if in.Label != "" {
		cfg.Label = in.Label
	}
	sum.Config = cfg
	sum.HistoryAttempts = int64(len(attempts))

	switch {
	case auditing:
		orders, err := AuditOrders(attempts)
		if err != nil {
			return nil, err
		}
		sum.Audit = RunAudit(freeSeats(before), freeSeats(after), orders)
		if err := writeJSON(filepath.Join(in.Out, "audit.json"), sum.Audit); err != nil {
			return nil, err
		}
	case n == 1:
		sum.Audit = ws[0].sum.Audit // a single run's own audit
	}
	if err := writeJSON(filepath.Join(in.Out, "summary.json"), sum); err != nil {
		return nil, err
	}
	return sum, nil
}

// writeMergedHistory writes a history.jsonl in the usual format.
func writeMergedHistory(path string, before []Listing, attempts []Attempt, browses []Listing, after []Listing) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	put := func(b []byte, e error) {
		if e == nil {
			_, e = w.Write(append(b, '\n'))
		}
		if e != nil && err == nil {
			err = e
		}
	}
	for _, l := range before {
		put(listingLine(l))
	}
	// Attempts and browses, in order of their calls.
	for len(attempts) > 0 || len(browses) > 0 {
		if len(browses) == 0 || (len(attempts) > 0 && attempts[0].CallNs <= browses[0].CallNs) {
			put(json.Marshal(attempts[0]))
			attempts = attempts[1:]
			continue
		}
		put(listingLine(browses[0]))
		browses = browses[1:]
	}
	for _, l := range after {
		put(listingLine(l))
	}
	if e := w.Flush(); err == nil {
		err = e
	}
	if e := f.Close(); err == nil {
		err = e
	}
	return err
}

// writeMergedSeries writes timeseries.csv and timeseries.hdr.jsonl.
func writeMergedSeries(dir string, workers [][]Second, offsets []int64) error {
	rows, hists, err := mergeSeconds(workers, offsets)
	if err != nil {
		return err
	}
	var csv, lines strings.Builder
	csv.WriteString(seriesHeader + "\n")
	for i, row := range rows {
		csv.WriteString(seriesRow(row, hists[i]) + "\n")
		b, err := json.Marshal(row)
		if err != nil {
			return err
		}
		lines.Write(append(b, '\n'))
	}
	if err := os.WriteFile(filepath.Join(dir, "timeseries.csv"), []byte(csv.String()), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "timeseries.hdr.jsonl"), []byte(lines.String()), 0o644)
}
