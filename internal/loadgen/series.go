package loadgen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// series writes timeseries.csv and the live console line, one row per second.
// With withHistograms it also writes timeseries.hdr.jsonl, the same rows with
// each second's histogram.
type series struct {
	mu                                sync.Mutex
	f                                 *os.File
	w                                 *bufio.Writer
	quiet                             bool
	sentN, completed, confirmed, errs int64
	inflight                          int64
	h                                 *hdrhistogram.Histogram
	spare                             *hdrhistogram.Histogram // the last second's, emptied, for the next

	base time.Time // the histogram file's times are measured from here
	hf   *os.File
	hw   *bufio.Writer
	herr error

	stopc chan struct{}
	donec chan struct{}
}

func newSeries(path string, quiet bool) (*series, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	s := &series{f: f, w: bufio.NewWriter(f), quiet: quiet, h: newHist(), stopc: make(chan struct{}), donec: make(chan struct{})}
	fmt.Fprintln(s.w, seriesHeader)
	return s, nil
}

// withHistograms also writes every row, with its histogram, to path. Times
// are measured from base. Call it before start.
func (s *series) withHistograms(path string, base time.Time) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	s.base, s.hf, s.hw = base, f, bufio.NewWriter(f)
	return nil
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
	// Take the second's numbers and histogram and let the orders go on: the
	// lock is the one every order takes, and writing the row (encoding the
	// histogram above all) must not hold them up.
	s.mu.Lock()
	row := Second{Second: sec, Sent: s.sentN, Completed: s.completed, Confirmed: s.confirmed, Errors: s.errs, InFlight: s.inflight}
	if s.hw != nil {
		row.EndNs = time.Since(s.base).Nanoseconds()
	}
	h := s.h
	if s.spare == nil {
		s.spare = newHist()
	}
	s.h, s.spare = s.spare, nil
	s.sentN, s.completed, s.confirmed, s.errs = 0, 0, 0, 0
	s.mu.Unlock()

	fmt.Fprintln(s.w, seriesRow(row, h))
	if !s.quiet {
		p50, p99 := float64(h.ValueAtQuantile(50))/1000, float64(h.ValueAtQuantile(99))/1000
		fmt.Printf("%4ds sent=%-6d done=%-6d confirmed=%-6d errors=%-4d p50=%-8.2f p99=%-8.2f inflight=%d\n", sec, row.Sent, row.Completed, row.Confirmed, row.Errors, p50, p99, row.InFlight)
	}
	if s.hw != nil {
		var b []byte
		var err error
		if row.Hist, err = encodeHist(h); err == nil {
			if b, err = json.Marshal(row); err == nil {
				_, err = s.hw.Write(append(b, '\n'))
			}
		}
		if err != nil && s.herr == nil {
			s.herr = err
		}
	}
	h.Reset()
	s.mu.Lock()
	s.spare = h
	s.mu.Unlock()
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
	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	if s.hw != nil {
		if err == nil {
			err = s.herr
		}
		if ferr := s.hw.Flush(); err == nil {
			err = ferr
		}
		if cerr := s.hf.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
