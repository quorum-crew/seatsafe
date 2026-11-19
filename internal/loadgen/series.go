package loadgen

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// series writes timeseries.csv and the live console line, one row per second.
type series struct {
	mu                                sync.Mutex
	f                                 *os.File
	w                                 *bufio.Writer
	quiet                             bool
	sentN, completed, confirmed, errs int64
	inflight                          int64
	h                                 *hdrhistogram.Histogram

	stopc chan struct{}
	donec chan struct{}
}

func newSeries(path string, quiet bool) (*series, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	s := &series{f: f, w: bufio.NewWriter(f), quiet: quiet, h: newHist(), stopc: make(chan struct{}), donec: make(chan struct{})}
	fmt.Fprintln(s.w, "second,sent,completed,confirmed,errors,p50_ms,p99_ms,in_flight")
	return s, nil
}

func (s *series) start() {
	go func() {
		defer close(s.donec)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for sec := 1; ; sec++ {
			select {
			case <-t.C:
				s.row(sec)
			case <-s.stopc:
				s.row(sec)
				return
			}
		}
	}()
}

func (s *series) row(sec int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p50, p99 := float64(s.h.ValueAtQuantile(50))/1000, float64(s.h.ValueAtQuantile(99))/1000
	fmt.Fprintf(s.w, "%d,%d,%d,%d,%d,%.3f,%.3f,%d\n", sec, s.sentN, s.completed, s.confirmed, s.errs, p50, p99, s.inflight)
	if !s.quiet {
		fmt.Printf("%4ds sent=%-6d done=%-6d confirmed=%-6d errors=%-4d p50=%-8.2f p99=%-8.2f inflight=%d\n", sec, s.sentN, s.completed, s.confirmed, s.errs, p50, p99, s.inflight)
	}
	s.sentN, s.completed, s.confirmed, s.errs = 0, 0, 0, 0
	s.h.Reset()
}

func (s *series) sent() {
	s.mu.Lock()
	s.sentN++
	s.mu.Unlock()
}

func (s *series) setInflight(n int64) {
	s.mu.Lock()
	s.inflight = n
	s.mu.Unlock()
}

func (s *series) done(res Result, intended time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed++
	switch res.Outcome {
	case OutcomeConfirmed:
		s.confirmed++
	case OutcomeUnknown:
		s.errs++
	}
	record(s.h, res.Return.Sub(intended))
}

func (s *series) stop() error {
	close(s.stopc)
	<-s.donec
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}
