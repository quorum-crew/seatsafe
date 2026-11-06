package seatsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// The first byte of a snapshot is its format. Snapshot writes the compact
// format (snapshot_compact.go). Restore also reads JSON, because stores and
// followers can hold snapshots written before the compact format existed.
const (
	snapshotVersion byte = 1 // JSON
	compactVersion  byte = 2
)

// The JSON shape of format 1.

type snapFile struct {
	AppliedIndex uint64        `json:"applied_index"`
	Seed         snapSeed      `json:"seed"`
	Sections     []snapSection `json:"sections"`
	Reqs         []snapReq     `json:"reqs"`
	Txns         []snapTxn     `json:"txns"`
}

type snapSeed struct {
	Version string   `json:"version"`
	Total   uint32   `json:"total_chunks"`
	Chunks  []uint32 `json:"chunks"`
}

type snapSection struct {
	Event   string     `json:"event"`
	Section string     `json:"section"`
	Seats   []snapSeat `json:"seats"`
}

type snapSeat struct {
	ID    string `json:"id"`
	State int32  `json:"st"`
	Owner string `json:"owner,omitempty"`
	User  string `json:"user,omitempty"`
}

// snapRef is [event, section, seat].
type snapRef [3]string

type snapReq struct {
	ID          string    `json:"id"`
	AppliedAt   int64     `json:"applied_at"`
	OrderID     string    `json:"order_id"`
	Outcome     int32     `json:"outcome"`
	Unavailable []snapRef `json:"unavailable,omitempty"`
	CrossShard  bool      `json:"cross_shard,omitempty"`
}

type snapTxn struct {
	ID          string    `json:"id"`
	Status      int       `json:"status"`
	User        string    `json:"user,omitempty"`
	Seats       []snapRef `json:"seats,omitempty"`
	HoldExpires int64     `json:"hold_expires_at_ms,omitempty"`
	Vote        int32     `json:"vote,omitempty"`
	Conflicts   []snapRef `json:"conflicts,omitempty"`
	DecidedAt   int64     `json:"decided_at,omitempty"`
}

func fromSnapRefs(rs []snapRef) []seatKey {
	if len(rs) == 0 {
		return nil
	}
	out := make([]seatKey, len(rs))
	for i, r := range rs {
		out[i] = seatKey{r[0], r[1], r[2]}
	}
	return out
}

// Snapshot encodes all state: seats, req/, txn/, seed progress and
// AppliedIndex. Equal states give byte-identical snapshots.
func (m *Machine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return compress(encodeCompact(m.state)), nil
}

// Restore replaces all state, AppliedIndex included, with a snapshot. On
// error the machine is left unchanged.
func (m *Machine) Restore(data []byte) error {
	if len(data) == 0 {
		return errors.New("seatsm: empty snapshot")
	}
	var s *state
	var err error
	switch data[0] {
	case snapshotVersion:
		s, err = decodeJSON(data[1:])
	case compactVersion:
		s, err = decodeCompact(data[1:])
	default:
		err = fmt.Errorf("seatsm: snapshot format %d, want %d or %d", data[0], snapshotVersion, compactVersion)
	}
	if err != nil {
		return err
	}
	if err := s.check(); err != nil {
		return fmt.Errorf("seatsm: snapshot rejected: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = s
	return nil
}

// setSeed restores seed progress, whichever format it came from.
func (s *state) setSeed(version string, total uint32, chunks []uint32) error {
	s.seedVersion, s.seedTotal = version, total
	for _, c := range chunks {
		if c >= total {
			return fmt.Errorf("seatsm: snapshot seed chunk %d of %d out of range", c, total)
		}
		s.seedChunks[c] = struct{}{}
	}
	if (version == "") != (total == 0) || (version == "" && len(chunks) > 0) {
		return errors.New("seatsm: snapshot seed progress is inconsistent")
	}
	return nil
}

func decodeJSON(body []byte) (*state, error) {
	var f snapFile
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("seatsm: decode snapshot: %w", err)
	}
	s := newState()
	s.applied = f.AppliedIndex
	if err := s.setSeed(f.Seed.Version, f.Seed.Total, f.Seed.Chunks); err != nil {
		return nil, err
	}

	for _, ss := range f.Sections {
		for _, st := range ss.Seats {
			k := seatKey{ss.Event, ss.Section, st.ID}
			if k.event == "" || k.section == "" || k.seat == "" {
				return nil, errors.New("seatsm: snapshot seat has an empty field")
			}
			if s.addSeat(k, seatsafev1.SeatState(st.State), st.Owner, st.User) == nil {
				return nil, fmt.Errorf("seatsm: snapshot lists seat %s twice", k)
			}
		}
	}
	for _, sec := range s.sections {
		sortSeats(sec.sorted)
	}

	for _, r := range f.Reqs {
		if _, dup := s.reqs[r.ID]; dup || r.ID == "" {
			return nil, fmt.Errorf("seatsm: snapshot req %q empty or listed twice", r.ID)
		}
		resp := &seatsafev1.BookSeatsResponse{
			OrderId: r.OrderID, Outcome: seatsafev1.BookingOutcome(r.Outcome), CrossShard: r.CrossShard,
			UnavailableSeats: refs(fromSnapRefs(r.Unavailable)),
		}
		s.reqs[r.ID] = &reqRecord{resp: resp, appliedAt: r.AppliedAt}
	}

	for _, st := range f.Txns {
		if _, dup := s.txns[st.ID]; dup || st.ID == "" {
			return nil, fmt.Errorf("seatsm: snapshot txn %q empty or listed twice", st.ID)
		}
		status := txnStatus(st.Status)
		if status < txnPrepared || status > txnAborted {
			return nil, fmt.Errorf("seatsm: snapshot txn %q has status %d", st.ID, st.Status)
		}
		t := &txnRecord{
			status: status, user: st.User, seats: fromSnapRefs(st.Seats), holdExpires: st.HoldExpires,
			vote: seatsafev1.Vote(st.Vote), conflicts: fromSnapRefs(st.Conflicts), decidedAt: st.DecidedAt,
		}
		s.txns[st.ID] = t
		if status == txnPrepared {
			s.prepared[st.ID] = t
		}
	}
	return s, nil
}
