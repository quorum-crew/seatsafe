package seatsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// snapshotVersion is the first byte of every snapshot.
const snapshotVersion byte = 1

// The JSON shapes below hold only slices, each sorted by key, so the same
// state always encodes to the same bytes.

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

func toSnapRefs(keys []seatKey) []snapRef {
	if len(keys) == 0 {
		return nil
	}
	out := make([]snapRef, len(keys))
	for i, k := range keys {
		out[i] = snapRef{k.event, k.section, k.seat}
	}
	return out
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

	f := snapFile{
		AppliedIndex: m.applied,
		Seed:         snapSeed{Version: m.seedVersion, Total: m.seedTotal, Chunks: []uint32{}},
		Sections:     make([]snapSection, 0, len(m.sections)),
		Reqs:         make([]snapReq, 0, len(m.reqs)),
		Txns:         make([]snapTxn, 0, len(m.txns)),
	}
	for c := range m.seedChunks {
		f.Seed.Chunks = append(f.Seed.Chunks, c)
	}
	sort.Slice(f.Seed.Chunks, func(i, j int) bool { return f.Seed.Chunks[i] < f.Seed.Chunks[j] })

	for sk, sec := range m.sections {
		ss := snapSection{Event: sk.event, Section: sk.section, Seats: make([]snapSeat, len(sec.sorted))}
		for i, st := range sec.sorted {
			ss.Seats[i] = snapSeat{ID: st.id, State: int32(st.state), Owner: st.owner, User: st.user}
		}
		f.Sections = append(f.Sections, ss)
	}
	sort.Slice(f.Sections, func(i, j int) bool {
		a, b := f.Sections[i], f.Sections[j]
		if a.Event != b.Event {
			return a.Event < b.Event
		}
		return a.Section < b.Section
	})

	for id, r := range m.reqs {
		sr := snapReq{ID: id, AppliedAt: r.appliedAt, OrderID: r.resp.OrderId, Outcome: int32(r.resp.Outcome), CrossShard: r.resp.CrossShard}
		for _, u := range r.resp.UnavailableSeats {
			sr.Unavailable = append(sr.Unavailable, snapRef{u.EventId, u.SectionId, u.SeatId})
		}
		f.Reqs = append(f.Reqs, sr)
	}
	sort.Slice(f.Reqs, func(i, j int) bool { return f.Reqs[i].ID < f.Reqs[j].ID })

	for id, t := range m.txns {
		f.Txns = append(f.Txns, snapTxn{
			ID: id, Status: int(t.status), User: t.user, Seats: toSnapRefs(t.seats), HoldExpires: t.holdExpires,
			Vote: int32(t.vote), Conflicts: toSnapRefs(t.conflicts), DecidedAt: t.decidedAt,
		})
	}
	sort.Slice(f.Txns, func(i, j int) bool { return f.Txns[i].ID < f.Txns[j].ID })

	body, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("seatsm: encode snapshot: %w", err)
	}
	return append([]byte{snapshotVersion}, body...), nil
}

// Restore replaces all state, AppliedIndex included, with a snapshot. On
// error the machine is left unchanged.
func (m *Machine) Restore(data []byte) error {
	if len(data) == 0 {
		return errors.New("seatsm: empty snapshot")
	}
	if data[0] != snapshotVersion {
		return fmt.Errorf("seatsm: snapshot format %d, want %d", data[0], snapshotVersion)
	}
	var f snapFile
	dec := json.NewDecoder(bytes.NewReader(data[1:]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return fmt.Errorf("seatsm: decode snapshot: %w", err)
	}
	s, err := fromSnap(f)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = s
	return nil
}

func fromSnap(f snapFile) (*state, error) {
	s := newState()
	s.applied = f.AppliedIndex
	s.seedVersion, s.seedTotal = f.Seed.Version, f.Seed.Total
	for _, c := range f.Seed.Chunks {
		if c >= s.seedTotal {
			return nil, fmt.Errorf("seatsm: snapshot seed chunk %d of %d out of range", c, s.seedTotal)
		}
		s.seedChunks[c] = struct{}{}
	}
	if (s.seedVersion == "") != (s.seedTotal == 0) || (s.seedVersion == "" && len(s.seedChunks) > 0) {
		return nil, errors.New("seatsm: snapshot seed progress is inconsistent")
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

	if err := s.check(); err != nil {
		return nil, fmt.Errorf("seatsm: snapshot rejected: %w", err)
	}
	return s, nil
}
