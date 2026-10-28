package seatsm

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// StateMachine is what the shard's Raft node drives.
type StateMachine interface {
	Apply(index uint64, cmd *seatsafev1.ShardCommand) Result
	Snapshot() ([]byte, error) // seats + req/ + txn/ + seed progress
	Restore(data []byte) error
	AppliedIndex() uint64
}

// Result is the outcome of one Apply.
type Result struct {
	Book    *seatsafev1.BookSeatsResponse // set for book
	Prepare *seatsafev1.PrepareResponse   // set for prepare
	Err     error                         // protocol violation or refused command; state unchanged
}

const (
	available = seatsafev1.SeatState_SEAT_STATE_AVAILABLE
	held      = seatsafev1.SeatState_SEAT_STATE_HELD
	booked    = seatsafev1.SeatState_SEAT_STATE_BOOKED

	outcomeConfirmed   = seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED
	outcomeUnavailable = seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
	outcomeInvalid     = seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID

	voteYes = seatsafev1.Vote_VOTE_YES
	voteNo  = seatsafev1.Vote_VOTE_NO
)

type txnStatus int

const (
	txnPrepared txnStatus = iota + 1
	txnCommitted
	txnAborted
)

func (s txnStatus) String() string {
	switch s {
	case txnPrepared:
		return "PREPARED"
	case txnCommitted:
		return "COMMITTED"
	case txnAborted:
		return "ABORTED"
	}
	return fmt.Sprintf("txnStatus(%d)", int(s))
}

type sectionKey struct{ event, section string }

type seatKey struct{ event, section, seat string }

func (k seatKey) String() string { return k.event + "/" + k.section + "/" + k.seat }

func (k seatKey) ref() *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: k.event, SectionId: k.section, SeatId: k.seat}
}

func keyOf(r *seatsafev1.SeatRef) seatKey {
	return seatKey{r.GetEventId(), r.GetSectionId(), r.GetSeatId()}
}

type seat struct {
	id    string
	state seatsafev1.SeatState
	owner string // request_id or txn_id; empty when AVAILABLE
	user  string
}

type section struct {
	sorted []*seat // by seat id
	seats  map[string]*seat
}

type reqRecord struct {
	resp      *seatsafev1.BookSeatsResponse // replayed = false
	appliedAt int64
}

type txnRecord struct {
	status      txnStatus
	user        string
	seats       []seatKey // request order; empty for tombstones and invalid requests
	holdExpires int64
	vote        seatsafev1.Vote // UNSPECIFIED for a tombstone
	conflicts   []seatKey
	decidedAt   int64 // 0 while PREPARED
}

// state is everything a snapshot carries. Restore swaps it as a whole.
type state struct {
	applied  uint64
	sections map[sectionKey]*section
	events   map[string][]string // event -> sorted section ids
	reqs     map[string]*reqRecord
	txns     map[string]*txnRecord
	prepared map[string]*txnRecord // index of txns in PREPARED

	seedVersion string
	seedTotal   uint32
	seedChunks  map[uint32]struct{}
}

func newState() *state {
	return &state{
		sections:   map[sectionKey]*section{},
		events:     map[string][]string{},
		reqs:       map[string]*reqRecord{},
		txns:       map[string]*txnRecord{},
		prepared:   map[string]*txnRecord{},
		seedChunks: map[uint32]struct{}{},
	}
}

// Machine is the StateMachine implementation. Apply must be called from one
// goroutine; the read methods are safe to call concurrently with it.
type Machine struct {
	mu sync.RWMutex
	*state
}

var _ StateMachine = (*Machine)(nil)

// New returns an empty, unseeded machine.
func New() *Machine { return &Machine{state: newState()} }

// AppliedIndex is the index of the last applied entry.
func (m *Machine) AppliedIndex() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applied
}

// Apply applies one committed log entry.
func (m *Machine) Apply(index uint64, cmd *seatsafev1.ShardCommand) Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.applied {
		return Result{}
	}
	m.applied = index
	now := cmd.GetNowMs()
	switch op := cmd.GetOp().(type) {
	case *seatsafev1.ShardCommand_Book:
		return Result{Book: m.book(now, op.Book)}
	case *seatsafev1.ShardCommand_Prepare:
		return Result{Prepare: m.prepare(now, op.Prepare)}
	case *seatsafev1.ShardCommand_Commit:
		return Result{Err: m.commit(now, op.Commit.GetTxnId())}
	case *seatsafev1.ShardCommand_Abort:
		return Result{Err: m.abort(now, op.Abort.GetTxnId())}
	case *seatsafev1.ShardCommand_Gc:
		m.gc(op.Gc.GetDropBeforeMs())
		return Result{}
	case *seatsafev1.ShardCommand_Seed:
		return Result{Err: m.applySeed(op.Seed)}
	}
	return Result{Err: fmt.Errorf("seatsm: entry %d has no command", index)}
}

func (s *state) seeded() bool {
	return s.seedVersion != "" && uint32(len(s.seedChunks)) == s.seedTotal
}

func (s *state) lookup(k seatKey) *seat {
	sec := s.sections[sectionKey{k.event, k.section}]
	if sec == nil {
		return nil
	}
	return sec.seats[k.seat]
}

// resolve finds every requested seat. ok is false for an empty list, seats
// of more than one event, a duplicate seat or an unknown seat.
func (s *state) resolve(refs []*seatsafev1.SeatRef) (seats []*seat, keys []seatKey, ok bool) {
	if len(refs) == 0 {
		return nil, nil, false
	}
	seats = make([]*seat, len(refs))
	keys = make([]seatKey, len(refs))
	for i, r := range refs {
		k := keyOf(r)
		if k.event != keys[0].event && i > 0 {
			return nil, nil, false
		}
		for _, prev := range keys[:i] {
			if prev == k {
				return nil, nil, false
			}
		}
		st := s.lookup(k)
		if st == nil {
			return nil, nil, false
		}
		seats[i], keys[i] = st, k
	}
	return seats, keys, true
}

func refs(keys []seatKey) []*seatsafev1.SeatRef {
	if len(keys) == 0 {
		return nil
	}
	out := make([]*seatsafev1.SeatRef, len(keys))
	for i, k := range keys {
		out[i] = k.ref()
	}
	return out
}

func cloneBook(r *seatsafev1.BookSeatsResponse) *seatsafev1.BookSeatsResponse {
	c := &seatsafev1.BookSeatsResponse{
		OrderId:    r.OrderId,
		Outcome:    r.Outcome,
		Replayed:   r.Replayed,
		CrossShard: r.CrossShard,
	}
	if len(r.UnavailableSeats) > 0 {
		c.UnavailableSeats = make([]*seatsafev1.SeatRef, len(r.UnavailableSeats))
		for i, s := range r.UnavailableSeats {
			c.UnavailableSeats[i] = keyOf(s).ref()
		}
	}
	return c
}

func (s *state) book(now int64, op *seatsafev1.BookOp) *seatsafev1.BookSeatsResponse {
	id := op.GetRequestId()
	if id == "" || !s.seeded() {
		return &seatsafev1.BookSeatsResponse{OrderId: id, Outcome: outcomeInvalid}
	}
	if r, ok := s.reqs[id]; ok {
		c := cloneBook(r.resp)
		c.Replayed = true
		return c
	}
	resp := &seatsafev1.BookSeatsResponse{OrderId: id}
	seats, keys, ok := s.resolve(op.GetSeats())
	if !ok {
		resp.Outcome = outcomeInvalid
	} else {
		for i, st := range seats {
			if st.state != available {
				resp.UnavailableSeats = append(resp.UnavailableSeats, keys[i].ref())
			}
		}
		if len(resp.UnavailableSeats) > 0 {
			resp.Outcome = outcomeUnavailable
		} else {
			for _, st := range seats {
				st.state, st.owner, st.user = booked, id, op.GetUserId()
			}
			resp.Outcome = outcomeConfirmed
		}
	}
	s.reqs[id] = &reqRecord{resp: resp, appliedAt: now}
	return cloneBook(resp)
}

func (s *state) prepare(now int64, op *seatsafev1.PrepareOp) *seatsafev1.PrepareResponse {
	id := op.GetTxnId()
	if id == "" || !s.seeded() {
		return &seatsafev1.PrepareResponse{Vote: voteNo}
	}
	if t, ok := s.txns[id]; ok {
		if t.status == txnAborted {
			return &seatsafev1.PrepareResponse{Vote: voteNo, Conflicts: refs(t.conflicts)}
		}
		return &seatsafev1.PrepareResponse{Vote: voteYes}
	}
	seats, keys, ok := s.resolve(op.GetSeats())
	if !ok {
		s.txns[id] = &txnRecord{status: txnAborted, user: op.GetUserId(), vote: voteNo, decidedAt: now}
		return &seatsafev1.PrepareResponse{Vote: voteNo}
	}
	var conflicts []seatKey
	for i, st := range seats {
		if st.state != available {
			conflicts = append(conflicts, keys[i])
		}
	}
	if len(conflicts) > 0 {
		s.txns[id] = &txnRecord{
			status: txnAborted, user: op.GetUserId(), seats: keys, holdExpires: op.GetHoldExpiresAtMs(),
			vote: voteNo, conflicts: conflicts, decidedAt: now,
		}
		return &seatsafev1.PrepareResponse{Vote: voteNo, Conflicts: refs(conflicts)}
	}
	for _, st := range seats {
		st.state, st.owner, st.user = held, id, op.GetUserId()
	}
	t := &txnRecord{status: txnPrepared, user: op.GetUserId(), seats: keys, holdExpires: op.GetHoldExpiresAtMs(), vote: voteYes}
	s.txns[id] = t
	s.prepared[id] = t
	return &seatsafev1.PrepareResponse{Vote: voteYes}
}

func (s *state) commit(now int64, id string) error {
	if id == "" {
		return errors.New("seatsm: commit with empty txn_id")
	}
	t, ok := s.txns[id]
	if !ok || t.status == txnCommitted {
		return nil
	}
	if t.status == txnAborted {
		return fmt.Errorf("seatsm: commit of ABORTED txn %q", id)
	}
	for _, k := range t.seats {
		s.lookup(k).state = booked
	}
	t.status, t.decidedAt = txnCommitted, now
	delete(s.prepared, id)
	return nil
}

func (s *state) abort(now int64, id string) error {
	if id == "" {
		return errors.New("seatsm: abort with empty txn_id")
	}
	t, ok := s.txns[id]
	if !ok {
		s.txns[id] = &txnRecord{status: txnAborted, decidedAt: now}
		return nil
	}
	switch t.status {
	case txnAborted:
		return nil
	case txnCommitted:
		return fmt.Errorf("seatsm: abort of COMMITTED txn %q", id)
	}
	for _, k := range t.seats {
		st := s.lookup(k)
		st.state, st.owner, st.user = available, "", ""
	}
	t.status, t.decidedAt = txnAborted, now
	delete(s.prepared, id)
	return nil
}

func (s *state) gc(cutoff int64) {
	for id, r := range s.reqs {
		if r.appliedAt < cutoff {
			delete(s.reqs, id)
		}
	}
	for id, t := range s.txns {
		if t.status != txnPrepared && t.decidedAt < cutoff {
			delete(s.txns, id)
		}
	}
}

func (s *state) applySeed(op *seatsafev1.SeedOp) error {
	v, chunk, total := op.GetSeedVersion(), op.GetChunk(), op.GetTotalChunks()
	switch {
	case v == "":
		return errors.New("seatsm: seed op with empty seed_version")
	case total == 0 || chunk >= total:
		return fmt.Errorf("seatsm: seed chunk %d of %d out of range", chunk, total)
	case s.seedVersion != "" && (v != s.seedVersion || total != s.seedTotal):
		return fmt.Errorf("seatsm: seed %s (%d chunks) refused, already seeding %s (%d chunks)", v, total, s.seedVersion, s.seedTotal)
	}
	if _, done := s.seedChunks[chunk]; done {
		return nil
	}
	for i, r := range op.GetSeats() {
		if r.GetEventId() == "" || r.GetSectionId() == "" || r.GetSeatId() == "" {
			return fmt.Errorf("seatsm: seed chunk %d seat %d has an empty field", chunk, i)
		}
	}
	s.seedVersion, s.seedTotal = v, total
	s.seedChunks[chunk] = struct{}{}
	touched := map[*section]struct{}{}
	for _, r := range op.GetSeats() {
		if sec := s.addSeat(keyOf(r), available, "", ""); sec != nil {
			touched[sec] = struct{}{}
		}
	}
	for sec := range touched {
		sortSeats(sec.sorted)
	}
	return nil
}

// addSeat inserts a seat unless it exists. It returns the section whose
// sorted list must be re-sorted, or nil if nothing was added.
func (s *state) addSeat(k seatKey, st seatsafev1.SeatState, owner, user string) *section {
	sk := sectionKey{k.event, k.section}
	sec := s.sections[sk]
	if sec == nil {
		sec = &section{seats: map[string]*seat{}}
		s.sections[sk] = sec
		ids := append(s.events[k.event], k.section)
		sort.Strings(ids)
		s.events[k.event] = ids
	}
	if _, exists := sec.seats[k.seat]; exists {
		return nil
	}
	e := &seat{id: k.seat, state: st, owner: owner, user: user}
	sec.seats[k.seat] = e
	sec.sorted = append(sec.sorted, e)
	return sec
}

func sortSeats(ss []*seat) {
	sort.Slice(ss, func(i, j int) bool { return ss[i].id < ss[j].id })
}

// SeedStatus reports seeding progress.
func (m *Machine) SeedStatus() (version string, applied, total uint32, done bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.seedVersion, uint32(len(m.seedChunks)), m.seedTotal, m.seeded()
}

// NextMissingChunk returns the lowest chunk not applied yet. Before any
// SeedOp is applied it returns (0, true); once seeded it returns (0, false).
func (m *Machine) NextMissingChunk() (chunk uint32, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.seedVersion == "" {
		return 0, true
	}
	for c := uint32(0); c < m.seedTotal; c++ {
		if _, done := m.seedChunks[c]; !done {
			return c, true
		}
	}
	return 0, false
}

// ExpiredHolds returns the PREPARED txns whose hold_expires_at_ms <= nowMs,
// sorted. Expiry never releases seats; only a Commit or Abort does.
func (m *Machine) ExpiredHolds(nowMs int64) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	for id, t := range m.prepared {
		if t.holdExpires <= nowMs {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// ListAvailable returns the AVAILABLE seats of one section, or of every
// section of the event on this shard when sectionID is empty. Sections and
// seat IDs are sorted. A section on this shard with no free seats is listed
// with an empty seat list; sections not on this shard are left out.
func (m *Machine) ListAvailable(eventID, sectionID string) *seatsafev1.ListAvailableSeatsResponse {
	m.mu.RLock()
	defer m.mu.RUnlock()
	resp := &seatsafev1.ListAvailableSeatsResponse{}
	secIDs := m.events[eventID]
	if sectionID != "" {
		secIDs = nil
		if _, ok := m.sections[sectionKey{eventID, sectionID}]; ok {
			secIDs = []string{sectionID}
		}
	}
	for _, sid := range secIDs {
		sec := m.sections[sectionKey{eventID, sid}]
		ids := []string{}
		for _, st := range sec.sorted {
			if st.state == available {
				ids = append(ids, st.id)
			}
		}
		resp.Sections = append(resp.Sections, &seatsafev1.SectionAvailability{
			EventId: eventID, SectionId: sid, AvailableSeatIds: ids,
		})
	}
	return resp
}

// CheckInvariants verifies the safety invariants: every HELD seat is held by
// a PREPARED txn that lists it, every PREPARED txn's seats are HELD by it, no
// seat is in two PREPARED txns, every BOOKED seat has an owner and no
// AVAILABLE seat has one.
func (m *Machine) CheckInvariants() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.check()
}

func (s *state) check() error {
	var errs []string
	bad := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }

	for sk, sec := range s.sections {
		if len(sec.sorted) != len(sec.seats) {
			bad("section %s/%s: index has %d seats, map %d", sk.event, sk.section, len(sec.sorted), len(sec.seats))
		}
		for _, st := range sec.sorted {
			k := seatKey{sk.event, sk.section, st.id}
			switch st.state {
			case available:
				if st.owner != "" {
					bad("seat %s AVAILABLE but owned by %q", k, st.owner)
				}
			case held:
				t := s.txns[st.owner]
				if t == nil || t.status != txnPrepared || !containsKey(t.seats, k) {
					bad("seat %s HELD by %q, which is not a PREPARED txn listing it", k, st.owner)
				}
			case booked:
				if st.owner == "" {
					bad("seat %s BOOKED with no owner", k)
				}
			default:
				bad("seat %s in state %v", k, st.state)
			}
		}
	}

	holder := map[seatKey]string{}
	for id, t := range s.txns {
		if (t.status == txnPrepared) != (s.prepared[id] == t) {
			bad("txn %q (%v) and the PREPARED index disagree", id, t.status)
		}
		if t.status != txnPrepared {
			continue
		}
		for _, k := range t.seats {
			if st := s.lookup(k); st == nil || st.state != held || st.owner != id {
				bad("PREPARED txn %q lists seat %s, which it does not hold", id, k)
			}
			if other, dup := holder[k]; dup {
				bad("seat %s in PREPARED txns %q and %q", k, other, id)
			}
			holder[k] = id
		}
	}
	if len(s.prepared) != countPrepared(s.txns) {
		bad("PREPARED index has %d txns, txn/ has %d", len(s.prepared), countPrepared(s.txns))
	}

	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return errors.New("seatsm: invariant violated: " + strings.Join(errs, "; "))
}

func containsKey(keys []seatKey, k seatKey) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

func countPrepared(txns map[string]*txnRecord) int {
	n := 0
	for _, t := range txns {
		if t.status == txnPrepared {
			n++
		}
	}
	return n
}
