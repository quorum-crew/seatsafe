package coordsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// snapshotVersion is the first byte of a snapshot: its format. Format 2 adds
// acked_ms; format 1 (without it) is still read.
const (
	snapshotVersion   byte = 2
	snapshotVersionV1 byte = 1
)

type snapFile struct {
	AppliedIndex uint64    `json:"applied_index"`
	Txns         []snapTxn `json:"txns"` // sorted by id
}

// snapRef is [event, section, seat].
type snapRef [3]string

type snapPart struct {
	Shard uint32    `json:"shard"`
	Seats []snapRef `json:"seats"`
}

type snapTxn struct {
	ID           string     `json:"id"`
	User         string     `json:"user,omitempty"`
	Participants []snapPart `json:"participants,omitempty"`
	Decision     int32      `json:"decision,omitempty"`
	Outcome      int32      `json:"outcome,omitempty"`
	Unavailable  []snapRef  `json:"unavailable,omitempty"`
	Acked        []uint32   `json:"acked,omitempty"`
	StartedMs    int64      `json:"started_ms"`
	DecidedMs    int64      `json:"decided_ms,omitempty"`
	AckedMs      int64      `json:"acked_ms,omitempty"` // format 2
}

func toSnapRefs(seats []Seat) []snapRef {
	if len(seats) == 0 {
		return nil
	}
	out := make([]snapRef, len(seats))
	for i, s := range seats {
		out[i] = snapRef{s.Event, s.Section, s.Seat}
	}
	return out
}

func fromSnapRefs(rs []snapRef) []Seat {
	if len(rs) == 0 {
		return nil
	}
	out := make([]Seat, len(rs))
	for i, r := range rs {
		out[i] = Seat{r[0], r[1], r[2]}
	}
	return out
}

// Snapshot encodes every txn record and AppliedIndex. Equal states give
// byte-identical snapshots.
func (m *Machine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	f := snapFile{AppliedIndex: m.applied, Txns: make([]snapTxn, 0, len(m.txns))}
	for _, t := range m.txns {
		st := snapTxn{
			ID: t.ID, User: t.User, Decision: int32(t.Decision), Outcome: int32(t.Outcome),
			Unavailable: toSnapRefs(t.Unavailable), StartedMs: t.StartedMs, DecidedMs: t.DecidedMs, AckedMs: t.AckedMs,
		}
		if len(t.Acked) > 0 {
			st.Acked = t.Acked
		}
		for _, p := range t.Participants {
			st.Participants = append(st.Participants, snapPart{Shard: p.Shard, Seats: toSnapRefs(p.Seats)})
		}
		f.Txns = append(f.Txns, st)
	}
	sort.Slice(f.Txns, func(i, j int) bool { return f.Txns[i].ID < f.Txns[j].ID })
	body, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("coordsm: encode snapshot: %w", err)
	}
	return append([]byte{snapshotVersion}, body...), nil
}

// Restore replaces all state, AppliedIndex included, with a snapshot. On
// error the machine is left unchanged.
func (m *Machine) Restore(data []byte) error {
	if len(data) == 0 {
		return errors.New("coordsm: empty snapshot")
	}
	version := data[0]
	if version != snapshotVersion && version != snapshotVersionV1 {
		return fmt.Errorf("coordsm: snapshot format %d, want %d or %d", version, snapshotVersion, snapshotVersionV1)
	}
	var f snapFile
	dec := json.NewDecoder(bytes.NewReader(data[1:]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return fmt.Errorf("coordsm: decode snapshot: %w", err)
	}
	s := newState()
	s.applied = f.AppliedIndex
	for _, st := range f.Txns {
		if _, dup := s.txns[st.ID]; dup || st.ID == "" {
			return fmt.Errorf("coordsm: snapshot txn %q empty or listed twice", st.ID)
		}
		t := &Txn{
			ID: st.ID, User: st.User, Decision: seatsafev1.Decision(st.Decision), Outcome: seatsafev1.BookingOutcome(st.Outcome),
			Unavailable: fromSnapRefs(st.Unavailable), StartedMs: st.StartedMs, DecidedMs: st.DecidedMs, AckedMs: st.AckedMs,
		}
		if len(st.Acked) > 0 {
			t.Acked = st.Acked
		}
		for _, p := range st.Participants {
			t.Participants = append(t.Participants, Participant{Shard: p.Shard, Seats: fromSnapRefs(p.Seats)})
		}
		if version == snapshotVersionV1 {
			if st.AckedMs != 0 {
				return fmt.Errorf("coordsm: format %d snapshot txn %q has acked_ms", version, st.ID)
			}
			// Format 1 predates AckedMs: retention then counted from the
			// decision, so a fully acked txn keeps that.
			if t.Decided() && len(t.Acked) == len(t.Participants) {
				t.AckedMs = ackedAt(t.DecidedMs, t.DecidedMs)
			}
		}
		s.txns[st.ID] = t
		switch {
		case !t.Decided():
			s.undecided[st.ID] = t
		case len(t.Acked) < len(t.Participants):
			s.unacked[st.ID] = t
		}
	}
	if err := s.check(); err != nil {
		return fmt.Errorf("coordsm: snapshot rejected: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = s
	return nil
}
