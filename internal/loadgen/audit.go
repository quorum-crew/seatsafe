package loadgen

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ListFunc reads one section's free seats.
type ListFunc func(ctx context.Context, s Section) ([]string, error)

// listRetries covers a leader change while the audit reads.
const (
	listRetries = 20
	listTimeout = 10 * time.Second
	listWorkers = 16
)

// Availability reads every section and returns the free seats.
func Availability(ctx context.Context, list ListFunc, sections []Section) (map[Seat]bool, error) {
	free := map[Seat]bool{}
	var mu sync.Mutex
	var firstErr error
	jobs := make(chan Section)
	var wg sync.WaitGroup
	for range listWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				var seats []string
				var err error
				for range listRetries {
					cctx, cancel := context.WithTimeout(ctx, listTimeout)
					seats, err = list(cctx, s)
					cancel()
					if err == nil {
						break
					}
					time.Sleep(250 * time.Millisecond)
				}
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("loadgen: listing %s/%s: %w", s.Event, s.ID, err)
				}
				for _, id := range seats {
					free[Seat{s.Event, s.ID, id}] = true
				}
				mu.Unlock()
			}
		}()
	}
	for _, s := range sections {
		jobs <- s
	}
	close(jobs)
	wg.Wait()
	return free, firstErr
}

// AuditOrder is what the audit needs from one order.
type AuditOrder struct {
	RequestID string
	Seats     []Seat
	Outcome   string
}

// maxAuditSamples bounds the examples kept in audit.json.
const maxAuditSamples = 20

// Audit is audit.json.
type Audit struct {
	Pass           bool `json:"pass"`
	Orders         int  `json:"orders"`
	ConfirmedSeats int  `json:"confirmed_seats"`
	UnknownOrders  int  `json:"unknown_orders"`
	SeatsTaken     int  `json:"seats_taken_during_run"` // free before, not after

	DoubleBooked int      `json:"a_double_booked"`        // rule a
	StillFree    int      `json:"b_confirmed_still_free"` // rule b
	Unexplained  int      `json:"c_unexplained_taken"`    // rule c
	PreBooked    int      `json:"d_confirmed_pre_booked"` // rule d
	Samples      []string `json:"samples,omitempty"`
}

func (a *Audit) sample(format string, args ...any) {
	if len(a.Samples) < maxAuditSamples {
		a.Samples = append(a.Samples, fmt.Sprintf(format, args...))
	}
}

// RunAudit applies the four rules:
//
//	a) no seat is in two CONFIRMED orders with different request_ids
//	b) every seat of a CONFIRMED order is no longer available
//	c) every seat that became unavailable belongs to a CONFIRMED or UNKNOWN order
//	d) every seat of a CONFIRMED order was available before the run: confirming
//	   a seat that was already booked is a double booking against state the
//	   run did not create (reruns with RESET_BETWEEN=0, fault injection)
//
// before and after are the free seats around the run, read over every section
// the workload can book.
func RunAudit(before, after map[Seat]bool, orders []AuditOrder) *Audit {
	a := &Audit{Orders: len(orders)}
	owner := make(map[Seat]string)
	explained := make(map[Seat]bool)
	for _, o := range orders {
		switch o.Outcome {
		case OutcomeConfirmed:
			for _, s := range o.Seats {
				a.ConfirmedSeats++
				explained[s] = true
				if prev, dup := owner[s]; dup && prev != o.RequestID {
					a.DoubleBooked++
					a.sample("seat %s CONFIRMED by %s and %s", s, prev, o.RequestID)
				}
				owner[s] = o.RequestID
				if !before[s] {
					a.PreBooked++
					a.sample("seat %s CONFIRMED by %s but already unavailable before the run", s, o.RequestID)
				}
				if after[s] {
					a.StillFree++
					a.sample("seat %s CONFIRMED by %s but still available", s, o.RequestID)
				}
			}
		case OutcomeUnknown:
			a.UnknownOrders++
			for _, s := range o.Seats {
				explained[s] = true
			}
		}
	}
	for s := range before {
		if after[s] {
			continue
		}
		a.SeatsTaken++
		if !explained[s] {
			a.Unexplained++
			a.sample("seat %s became unavailable with no CONFIRMED or UNKNOWN order", s)
		}
	}
	a.Pass = a.DoubleBooked == 0 && a.StillFree == 0 && a.Unexplained == 0 && a.PreBooked == 0
	return a
}
