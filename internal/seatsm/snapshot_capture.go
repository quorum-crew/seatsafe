package seatsm

import "maps"

// CaptureSnapshot copies the state under the read lock and returns a function
// that encodes the copy, giving exactly the bytes Snapshot would have returned
// at the moment of the capture.
//
// Copying is much cheaper than encoding (no tables, sorting or compression),
// so a caller can hold up Apply only for the copy and run the returned
// function on another goroutine while Apply continues.
func (m *Machine) CaptureSnapshot() func() ([]byte, error) {
	m.mu.RLock()
	c := m.state.frozen()
	m.mu.RUnlock()
	return func() ([]byte, error) { return compress(encodeCompact(c)), nil }
}

// frozen returns a copy of s that later Applies cannot change, holding only
// what encodeCompact reads: sections carry their sorted seats but no lookup
// map, and the prepared index is left out. It is for encoding, never for
// applying.
//
// Apply changes seats and txn records in place, so those are copied; req/
// records and seat lists are never changed after they are created, so they
// are shared.
func (s *state) frozen() *state {
	c := &state{
		applied:     s.applied,
		sections:    make(map[sectionKey]*section, len(s.sections)),
		events:      make(map[string][]string, len(s.events)),
		reqs:        maps.Clone(s.reqs),
		txns:        make(map[string]*txnRecord, len(s.txns)),
		seedVersion: s.seedVersion,
		seedTotal:   s.seedTotal,
		seedChunks:  maps.Clone(s.seedChunks),
	}
	for e, secs := range s.events {
		c.events[e] = append([]string(nil), secs...)
	}
	for sk, sec := range s.sections {
		seats := make([]seat, len(sec.sorted))
		sorted := make([]*seat, len(sec.sorted))
		for i, st := range sec.sorted {
			seats[i] = *st
			sorted[i] = &seats[i]
		}
		c.sections[sk] = &section{sorted: sorted}
	}
	for id, t := range s.txns {
		tc := *t
		c.txns[id] = &tc
	}
	return c
}
