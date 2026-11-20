package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// run is the state of one load generator run.
type run struct {
	cfg      Config
	workload *Workload
	client   *Client

	mu       sync.Mutex
	orders   []AuditOrder
	sinks    []*Recorder
	series   *series
	inflight atomic.Int64
	wg       sync.WaitGroup
	next     atomic.Uint64
	out      atomic.Bool // a fresh workload ran out of seats

	sleepUntil func(time.Time) // replaced in tests
}

func newRun(cfg Config, w *Workload, c *Client, s *series) *run {
	return &run{cfg: cfg, workload: w, client: c, series: s,
		sleepUntil: func(t time.Time) { time.Sleep(time.Until(t)) }}
}

func (r *run) addSink(s *Recorder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks = append(r.sinks, s)
}

func (r *run) removeSink(s *Recorder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, x := range r.sinks {
		if x == s {
			r.sinks = append(r.sinks[:i], r.sinks[i+1:]...)
			return
		}
	}
}

func (r *run) attempt(intended time.Time, dropped bool) {
	r.mu.Lock()
	sinks := r.sinks
	r.mu.Unlock()
	for _, s := range sinks {
		s.Attempt(intended, dropped)
	}
	if !dropped {
		r.series.sent()
	}
}

func (r *run) done(res Result, intended time.Time) {
	r.mu.Lock()
	r.orders = append(r.orders, AuditOrder{res.RequestID, res.Order.Seats, res.Outcome, res.Order.Cross})
	sinks := r.sinks
	r.mu.Unlock()
	for _, s := range sinks {
		s.Done(res, intended)
	}
	r.series.done(res, intended)
}

// schedule sends at a fixed rate from start to end without waiting for
// responses. An order's latency is measured from its intended time, so if this
// loop falls behind, that shows up as latency. When MaxInflight orders are
// outstanding the order is dropped and counted; the rate never slows down.
func (r *run) schedule(ctx context.Context, rate float64, start, end time.Time) {
	worker := r.workload.Worker(0)
	interval := float64(time.Second) / rate
	for i := int64(0); ; i++ {
		intended := start.Add(time.Duration(float64(i) * interval))
		if !intended.Before(end) || ctx.Err() != nil {
			return
		}
		if time.Now().Before(intended) {
			r.sleepUntil(intended)
		}
		order, ok := worker.Next()
		if !ok {
			r.out.Store(true)
			return
		}
		if r.inflight.Load() >= int64(r.cfg.MaxInflight) {
			r.attempt(intended, true)
			continue
		}
		r.attempt(intended, false)
		r.inflight.Add(1)
		r.series.setInflight(r.inflight.Load())
		r.wg.Add(1)
		id := r.next.Add(1)
		go func() {
			defer r.wg.Done()
			res := r.client.Book(id, order)
			r.inflight.Add(-1)
			r.done(res, intended)
		}()
	}
}

// open runs one fixed-rate window after the warmup.
func (r *run) open(ctx context.Context) *Recorder {
	start := time.Now()
	from := start.Add(r.cfg.Warmup)
	rec := NewRecorder(from, from.Add(r.cfg.Duration))
	r.addSink(rec)
	r.schedule(ctx, r.cfg.Rate, start, rec.to)
	r.wg.Wait()
	return rec
}

// closed runs virtual users. Each books, optionally thinks, and books again.
// A user only sends after its last order finished, so latency here is measured
// from the send and the achieved rate is the result.
func (r *run) closed(ctx context.Context) *Recorder {
	start := time.Now()
	from := start.Add(r.cfg.Warmup)
	rec := NewRecorder(from, from.Add(r.cfg.Duration))
	r.addSink(rec)
	for u := range r.cfg.Users {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			worker := r.workload.Worker(uint64(u) + 1)
			for time.Now().Before(rec.to) && ctx.Err() == nil {
				order, ok := worker.Next()
				if !ok {
					r.out.Store(true)
					return
				}
				intended := time.Now()
				r.attempt(intended, false)
				r.series.setInflight(r.inflight.Add(1))
				res := r.client.Book(uint64(u)+1, order)
				r.inflight.Add(-1)
				r.done(res, intended)
				if r.cfg.Think > 0 {
					time.Sleep(r.cfg.Think)
				}
			}
		}()
	}
	r.wg.Wait()
	return rec
}

// ramp raises the rate step by step until a step misses the SLO or more than
// 1% of its orders end in errors, and returns every step and the highest rate
// that met both.
func (r *run) ramp(ctx context.Context) (*Recorder, []Step, float64, string) {
	if r.cfg.Warmup > 0 {
		start := time.Now()
		r.schedule(ctx, r.cfg.RateStart, start, start.Add(r.cfg.Warmup))
		r.wg.Wait()
	}
	total := NewRecorder(time.Now(), time.Now().Add(365*24*time.Hour))
	r.addSink(total)
	var steps []Step
	var best float64
	stopped := fmt.Sprintf("reached -rate-end %.0f", r.cfg.RateEnd)
	for rate := r.cfg.RateStart; rate <= r.cfg.RateEnd && ctx.Err() == nil; rate += r.cfg.RateStep {
		start := time.Now()
		rec := NewRecorder(start, start.Add(r.cfg.StepDuration))
		r.addSink(rec)
		r.schedule(ctx, rate, start, rec.to)
		r.wg.Wait()
		r.removeSink(rec)
		res := rec.Results()
		step := Step{Rate: rate, Results: res}
		switch {
		case r.out.Load():
			step.Why = "ran out of fresh seats"
		case res.Latency.P99 > float64(r.cfg.SLO)/float64(time.Millisecond):
			step.Why = fmt.Sprintf("p99 %.1fms over the %v SLO", res.Latency.P99, r.cfg.SLO)
		case res.ErrorRate > 0.01:
			step.Why = fmt.Sprintf("%.2f%% errors", res.ErrorRate*100)
		case res.Dropped > 0:
			step.Why = fmt.Sprintf("%d dropped by the load generator", res.Dropped)
		default:
			step.Passed = true
			best = rate
		}
		steps = append(steps, step)
		if !r.cfg.Quiet {
			fmt.Printf("step %6.0f/s: %s p50=%.1fms p99=%.1fms confirmed=%.0f/s %s\n", rate, map[bool]string{true: "PASS", false: "FAIL"}[step.Passed], res.Latency.P50, res.Latency.P99, res.ConfirmedRate, step.Why)
		}
		if !step.Passed {
			stopped = step.Why
			break
		}
	}
	total.to = time.Now()
	return total, steps, best, stopped
}

// listed is one answered ListAvailableSeats call.
type listed struct {
	section   Section
	free      []string
	call, ret time.Time
}

// listRecorder keeps the answered calls of a ListFunc for the history.
type listRecorder struct {
	mu  sync.Mutex
	got []listed
}

// wrap returns list with every answered call recorded. A nil recorder
// records nothing.
func (r *listRecorder) wrap(list ListFunc) ListFunc {
	if r == nil {
		return list
	}
	return func(ctx context.Context, s Section) ([]string, error) {
		call := time.Now()
		free, err := list(ctx, s)
		ret := time.Now()
		if err == nil {
			r.mu.Lock()
			r.got = append(r.got, listed{s, free, call, ret})
			r.mu.Unlock()
		}
		return free, err
	}
}

// take returns what was recorded since the last take.
func (r *listRecorder) take() []listed {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	got := r.got
	r.got = nil
	return got
}

// browse calls ListAvailableSeats for one section at a time, rate times a
// second, until ctx ends, and writes every answer to the history. Sections
// are picked the way orders pick theirs, so browses land where the bookings
// are. A browse that fails or times out is not recorded: it says nothing.
func (r *run) browse(ctx context.Context, list ListFunc, sections []Section) int64 {
	var wg sync.WaitGroup
	var n atomic.Int64
	rng := rand.New(rand.NewPCG(r.cfg.Seed, 11))
	var worker *Worker
	if r.cfg.Workload != WorkloadFresh { // a fresh worker would use up seats
		worker = r.workload.Worker(1 << 40)
	}
	tick := time.NewTicker(time.Duration(float64(time.Second) / r.cfg.BrowseRate))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return n.Load()
		case <-tick.C:
		}
		s := sections[rng.IntN(len(sections))]
		if worker != nil {
			if o, ok := worker.Next(); ok && len(o.Seats) > 0 {
				s = Section{o.Seats[0].Event, o.Seats[0].Section}
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(context.Background(), r.cfg.CallTimeout)
			defer cancel()
			call := time.Now()
			free, err := list(cctx, s)
			if err != nil {
				return
			}
			n.Add(1)
			if r.client.History != nil {
				r.client.History.WriteListing(PhaseBrowse, s, free, call, time.Now())
			}
		}()
	}
}

// Env is what a run talks to. Run builds it from Config when empty.
type Env struct {
	Book BookFunc
	List ListFunc
}

// Run performs a whole run: availability, load, audit, output files.
func Run(ctx context.Context, cfg Config, u *Universe, env Env) (*Summary, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Out == "" {
		cfg.Out = filepath.Join("results", time.Now().Format("20060102-150405")+"-"+cfg.Label)
	}
	if err := os.MkdirAll(cfg.Out, 0o755); err != nil {
		return nil, err
	}
	if env.Book == nil {
		pool, err := Dial(cfg.Target, cfg.Conns, cfg.DialOptions...)
		if err != nil {
			return nil, err
		}
		defer pool.Close()
		env = Env{Book: pool.Book, List: pool.List}
	}

	probe, err := NewWorkload(Config{Workload: cfg.Workload, OrderSizes: cfg.OrderSizes, Seed: cfg.Seed, ZipfS: cfg.ZipfS, CrossShardFrac: 0}, u, nil)
	if err != nil {
		return nil, err
	}
	// With a history, the availability reads go into it: the linearizability
	// check uses the one before the run and the last one after it.
	var lists *listRecorder
	if cfg.History {
		lists = &listRecorder{}
	}
	list := lists.wrap(env.List)
	var before map[Seat]bool
	if cfg.Audit || cfg.Workload == WorkloadFresh {
		if before, err = Availability(ctx, list, probe.Touchable()); err != nil {
			return nil, err
		}
	}
	baseline := lists.take()
	workload, err := NewWorkload(cfg, u, before)
	if err != nil {
		return nil, err
	}

	begin := time.Now()
	client := &Client{Call: env.Book, CallTimeout: cfg.CallTimeout, RetryBudget: cfg.RetryBudget}
	if cfg.History {
		if client.History, err = NewHistoryWriter(filepath.Join(cfg.Out, "history.jsonl"), begin); err != nil {
			return nil, err
		}
		for _, l := range baseline {
			client.History.WriteListing(PhaseBaseline, l.section, l.free, l.call, l.ret)
		}
	}
	s, err := newSeries(filepath.Join(cfg.Out, "timeseries.csv"), cfg.Quiet)
	if err != nil {
		return nil, err
	}
	s.start()
	r := newRun(cfg, workload, client, s)

	sum := &Summary{Config: cfg, GitCommit: cfg.GitCommit, StartedAt: begin.UTC().Format(time.RFC3339), Seats: u.Seats}
	browsed := make(chan int64, 1)
	bctx, stopBrowsing := context.WithCancel(ctx)
	defer stopBrowsing()
	if cfg.BrowseRate > 0 && env.List != nil {
		go func() { browsed <- r.browse(bctx, env.List, probe.Touchable()) }()
	} else {
		browsed <- 0
	}
	var rec *Recorder
	switch cfg.Mode {
	case ModeOpen:
		rec = r.open(ctx)
	case ModeClosed:
		rec = r.closed(ctx)
	case ModeRamp:
		rec, sum.Steps, sum.MaxSustainable, sum.RampStopped = r.ramp(ctx)
	}
	if err := s.stop(); err != nil {
		return nil, err
	}
	stopBrowsing()
	sum.Browses = <-browsed
	sum.Results = rec.Results()
	sum.SeatsExhausted = r.out.Load()
	closeHistory := func() error {
		if client.History == nil {
			return nil
		}
		var err error
		sum.HistoryAttempts, err = client.History.Close()
		client.History = nil
		return err
	}
	defer closeHistory()

	if cfg.Audit {
		after, err := Availability(context.Background(), list, probe.Touchable())
		if err != nil {
			return nil, err
		}
		final := lists.take()
		sum.Audit = RunAudit(before, after, r.orders)
		// An aborted cross-shard order's seats stay HELD until the
		// coordinator's Abort arrives, which is after the order was
		// answered. Give those holds time to go before calling a seat
		// unexplained.
		for reads := 1; sum.Audit.Unsettled() && reads <= auditRereads; reads++ {
			time.Sleep(auditRereadEvery)
			if after, err = Availability(context.Background(), list, probe.Touchable()); err != nil {
				return nil, err
			}
			final = lists.take()
			sum.Audit = RunAudit(before, after, r.orders)
			sum.Audit.Rereads = reads
		}
		// The history gets the read the audit's verdict is based on.
		if client.History != nil {
			for _, l := range final {
				client.History.WriteListing(PhaseFinal, l.section, l.free, l.call, l.ret)
			}
		}
		if err := closeHistory(); err != nil {
			return nil, err
		}
		if err := writeJSON(filepath.Join(cfg.Out, "audit.json"), sum.Audit); err != nil {
			return nil, err
		}
	}
	if err := closeHistory(); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(cfg.Out, "summary.json"), sum); err != nil {
		return nil, err
	}
	return sum, nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// PrintSummary writes the final table.
func PrintSummary(sum *Summary) string {
	var b strings.Builder
	r := sum.Results
	fmt.Fprintf(&b, "\n%s %s, %.0fs window\n", sum.Config.Mode, sum.Config.Workload, r.DurationSec)
	fmt.Fprintf(&b, "  attempted %.0f/s, achieved %.0f/s, confirmed %.0f orders/s, %.0f seats/s\n", r.AttemptedRate, r.AchievedRate, r.ConfirmedRate, r.ConfirmedSeats)
	fmt.Fprintf(&b, "  retries %d, replayed %d, dropped by loadgen %d\n", r.Retries, r.Replayed, r.Dropped)
	if sum.SeatsExhausted {
		fmt.Fprintf(&b, "  ran out of fresh seats\n")
	}
	fmt.Fprintf(&b, "  %-18s %10s %9s %9s %9s %9s %9s\n", "latency (ms)", "count", "p50", "p90", "p99", "p99.9", "max")
	row := func(name string, l Latency) {
		fmt.Fprintf(&b, "  %-18s %10d %9.2f %9.2f %9.2f %9.2f %9.2f\n", name, l.Count, l.P50, l.P90, l.P99, l.P999, l.Max)
	}
	row("all", r.Latency)
	for _, k := range sortedKeys(r.LatencyOutcome) {
		row(k, r.LatencyOutcome[k])
	}
	for _, k := range sortedKeys(r.LatencyPath) {
		row("path "+k, r.LatencyPath[k])
	}
	if sum.MaxSustainable > 0 || len(sum.Steps) > 0 {
		fmt.Fprintf(&b, "  max sustainable rate: %.0f/s (stopped: %s)\n", sum.MaxSustainable, sum.RampStopped)
	}
	if a := sum.Audit; a != nil {
		verdict := "PASS"
		if !a.Pass {
			verdict = "FAIL"
		}
		fmt.Fprintf(&b, "  audit %s: %d orders, %d confirmed seats, %d seats taken, %d unknown; double-booked %d, confirmed-but-free %d, unexplained %d, confirmed-pre-booked %d, cross-shard-half-booked %d, taken-by-unconfirmed %d\n",
			verdict, a.Orders, a.ConfirmedSeats, a.SeatsTaken, a.UnknownOrders, a.DoubleBooked, a.StillFree, a.Unexplained, a.PreBooked, a.HalfBooked, a.TakenByFailed)
		for _, s := range a.Samples {
			fmt.Fprintf(&b, "    %s\n", s)
		}
	}
	return b.String()
}

// How often and how long the audit reads the seats again while the only
// failures are seats that a hold being released would explain: 15 s covers
// the coordinator's delivery backoff after a leader change.
const (
	auditRereads     = 15
	auditRereadEvery = time.Second
)

// ErrAuditFailed marks a run whose audit failed.
var ErrAuditFailed = errors.New("loadgen: audit failed")
