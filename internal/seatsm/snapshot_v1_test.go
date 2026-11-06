package seatsm

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"
)

// snapshotV1 is the format 1 (JSON) encoder as it was before format 2. Snapshot
// no longer writes format 1 but Restore must keep reading it, because stores
// and followers can hold older snapshots.
func snapshotV1(m *Machine) ([]byte, error) {
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
			ID: id, Status: int(t.status), User: t.user, Seats: snapRefsV1(t.seats), HoldExpires: t.holdExpires,
			Vote: int32(t.vote), Conflicts: snapRefsV1(t.conflicts), DecidedAt: t.decidedAt,
		})
	}
	sort.Slice(f.Txns, func(i, j int) bool { return f.Txns[i].ID < f.Txns[j].ID })

	body, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("seatsm: encode snapshot: %w", err)
	}
	return append([]byte{snapshotVersion}, body...), nil
}

func mustSnapshotV1(t testing.TB, m *Machine) []byte {
	t.Helper()
	b, err := snapshotV1(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func snapRefsV1(keys []seatKey) []snapRef {
	if len(keys) == 0 {
		return nil
	}
	out := make([]snapRef, len(keys))
	for i, k := range keys {
		out[i] = snapRef{k.event, k.section, k.seat}
	}
	return out
}
