package coordsm

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// StateMachine is what the coordinator's Raft node drives.
type StateMachine interface {
	Apply(index uint64, cmd *seatsafev1.CoordCommand) Result
	Snapshot() ([]byte, error)
	Restore(data []byte) error
	AppliedIndex() uint64
}

const (
	decisionNone   = seatsafev1.Decision_DECISION_UNSPECIFIED
	decisionCommit = seatsafev1.Decision_DECISION_COMMIT
	decisionAbort  = seatsafev1.Decision_DECISION_ABORT

	outcomeNone        = seatsafev1.BookingOutcome_BOOKING_OUTCOME_UNSPECIFIED
	outcomeConfirmed   = seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED
	outcomeUnavailable = seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
	outcomeAborted     = seatsafev1.BookingOutcome_BOOKING_OUTCOME_ABORTED
)

// Seat identifies one seat.
type Seat struct{ Event, Section, Seat string }

func (s Seat) String() string { return s.Event + "/" + s.Section + "/" + s.Seat }

// Ref returns the seat as a SeatRef.
func (s Seat) Ref() *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: s.Event, SectionId: s.Section, SeatId: s.Seat}
}

func seatOf(r *seatsafev1.SeatRef) Seat {
	return Seat{r.GetEventId(), r.GetSectionId(), r.GetSeatId()}
}

func seatsOf(refs []*seatsafev1.SeatRef) []Seat {
	if len(refs) == 0 {
		return nil
	}
	out := make([]Seat, len(refs))
	for i, r := range refs {
		out[i] = seatOf(r)
	}
	return out
}

// Refs returns seats as SeatRefs, nil for none.
func Refs(seats []Seat) []*seatsafev1.SeatRef {
	if len(seats) == 0 {
		return nil
	}
	out := make([]*seatsafev1.SeatRef, len(seats))
	for i, s := range seats {
		out[i] = s.Ref()
	}
	return out
}

// Participant is one shard of a txn and the txn's seats on it.
type Participant struct {
	Shard uint32
	Seats []Seat
}

// Txn is one txn's record. Values returned by the machine are copies.
type Txn struct {
	ID           string
	User         string
	Participants []Participant // by shard id; empty for a decision logged for an unknown txn
	Decision     seatsafev1.Decision
	Outcome      seatsafev1.BookingOutcome
	Unavailable  []Seat   // conflicts, when Outcome is SEAT_UNAVAILABLE
	Acked        []uint32 // participant shards that applied the decision, sorted
	StartedMs    int64
	DecidedMs    int64 // 0 while undecided
}

// Decided reports whether the txn has a decision.
func (t *Txn) Decided() bool { return t.Decision != decisionNone }

// MissingAcks returns the participant shards that have not acked, in shard
// order.
func (t *Txn) MissingAcks() []uint32 {
	var out []uint32
	for _, p := range t.Participants {
		if !slices.Contains(t.Acked, p.Shard) {
			out = append(out, p.Shard)
		}
	}
	return out
}

// Response is the answer SubmitOrder gives for a decided txn.
func (t *Txn) Response(replayed bool) *seatsafev1.BookSeatsResponse {
	return &seatsafev1.BookSeatsResponse{
		OrderId: t.ID, Outcome: t.Outcome, UnavailableSeats: Refs(t.Unavailable), Replayed: replayed, CrossShard: true,
	}
}

func (t *Txn) clone() *Txn {
	c := *t
	c.Participants = make([]Participant, len(t.Participants))
	for i, p := range t.Participants {
		c.Participants[i] = Participant{Shard: p.Shard, Seats: slices.Clone(p.Seats)}
	}
	if len(t.Participants) == 0 {
		c.Participants = nil
	}
	c.Unavailable = slices.Clone(t.Unavailable)
	c.Acked = slices.Clone(t.Acked)
	return &c
}

// Result is the outcome of one Apply.
type Result struct {
	// Txn is the txn's record after the command, set for TxnStarted,
	// TxnDecided and TxnAcked unless the command was refused or (for an ack)
	// the txn is unknown.
	Txn *Txn
	// Created is set by a TxnStarted that created the record.
	Created bool
	// Won is set by a TxnDecided whose decision is the one that stands.
	Won bool
	// Dropped is the number of txns a Gc removed.
	Dropped int
	// Err is set when the command was refused; the state is unchanged.
	Err error
}

// state is everything a snapshot carries. Restore swaps it as a whole.
type state struct {
	applied   uint64
	txns      map[string]*Txn
	undecided map[string]*Txn // index: no decision yet
	unacked   map[string]*Txn // index: decided, a participant has not acked
}

func newState() *state {
	return &state{txns: map[string]*Txn{}, undecided: map[string]*Txn{}, unacked: map[string]*Txn{}}
}

// Machine is the StateMachine implementation. Apply must be called from one
// goroutine; the read methods are safe to call concurrently with it.
type Machine struct {
	mu sync.RWMutex
	*state
}

var _ StateMachine = (*Machine)(nil)

// New returns an empty machine.
func New() *Machine { return &Machine{state: newState()} }

// AppliedIndex is the index of the last applied entry.
func (m *Machine) AppliedIndex() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applied
}

// Apply applies one committed log entry.
func (m *Machine) Apply(index uint64, cmd *seatsafev1.CoordCommand) Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.applied {
		return Result{}
	}
	m.applied = index
	now := cmd.GetNowMs()
	switch op := cmd.GetOp().(type) {
	case *seatsafev1.CoordCommand_Started:
		return m.started(now, op.Started)
	case *seatsafev1.CoordCommand_Decided:
		return m.decided(now, op.Decided)
	case *seatsafev1.CoordCommand_Acked:
		return m.acked(op.Acked)
	case *seatsafev1.CoordCommand_Gc:
		return Result{Dropped: m.gc(op.Gc.GetDropBeforeMs())}
	}
	return Result{Err: fmt.Errorf("coordsm: entry %d has no command", index)}
}

func (s *state) started(now int64, op *seatsafev1.TxnStarted) Result {
	id := op.GetTxnId()
	if id == "" {
		return Result{Err: errors.New("coordsm: TxnStarted with empty txn_id")}
	}
	if t, ok := s.txns[id]; ok {
		return Result{Txn: t.clone()}
	}
	if len(op.GetParticipants()) == 0 {
		return Result{Err: fmt.Errorf("coordsm: TxnStarted %q has no participants", id)}
	}
	parts := make([]Participant, len(op.GetParticipants()))
	for i, p := range op.GetParticipants() {
		switch {
		case p.GetShardId() == 0:
			return Result{Err: fmt.Errorf("coordsm: TxnStarted %q has a participant with shard id 0", id)}
		case i > 0 && p.GetShardId() <= parts[i-1].Shard:
			return Result{Err: fmt.Errorf("coordsm: TxnStarted %q: participants are not sorted by shard id", id)}
		case len(p.GetSeats()) == 0:
			return Result{Err: fmt.Errorf("coordsm: TxnStarted %q has no seats on shard %d", id, p.GetShardId())}
		}
		parts[i] = Participant{Shard: p.GetShardId(), Seats: seatsOf(p.GetSeats())}
	}
	t := &Txn{ID: id, User: op.GetUserId(), Participants: parts, StartedMs: now}
	s.txns[id] = t
	s.undecided[id] = t
	return Result{Txn: t.clone(), Created: true}
}

func (s *state) decided(now int64, op *seatsafev1.TxnDecided) Result {
	id := op.GetTxnId()
	if id == "" {
		return Result{Err: errors.New("coordsm: TxnDecided with empty txn_id")}
	}
	unavailable := seatsOf(op.GetUnavailableSeats())
	if err := checkDecision(op.GetDecision(), op.GetOutcome(), len(unavailable)); err != nil {
		return Result{Err: fmt.Errorf("coordsm: TxnDecided %q: %w", id, err)}
	}
	t, ok := s.txns[id]
	if ok && t.Decided() {
		return Result{Txn: t.clone()} // the first decision stands
	}
	if !ok {
		t = &Txn{ID: id, StartedMs: now}
		s.txns[id] = t
	}
	t.Decision, t.Outcome, t.Unavailable, t.DecidedMs = op.GetDecision(), op.GetOutcome(), unavailable, now
	delete(s.undecided, id)
	if len(t.Participants) > 0 {
		s.unacked[id] = t
	}
	return Result{Txn: t.clone(), Won: true}
}

// checkDecision reports whether a decision, its outcome and its number of
// unavailable seats go together.
func checkDecision(d seatsafev1.Decision, o seatsafev1.BookingOutcome, unavailable int) error {
	switch {
	case d == decisionCommit && o == outcomeConfirmed && unavailable == 0:
	case d == decisionAbort && o == outcomeAborted && unavailable == 0:
	case d == decisionAbort && o == outcomeUnavailable && unavailable > 0:
	default:
		return fmt.Errorf("decision %v does not go with outcome %v and %d unavailable seats", d, o, unavailable)
	}
	return nil
}

func (s *state) acked(op *seatsafev1.TxnAcked) Result {
	id, shard := op.GetTxnId(), op.GetShardId()
	if id == "" {
		return Result{Err: errors.New("coordsm: TxnAcked with empty txn_id")}
	}
	t, ok := s.txns[id]
	if !ok {
		return Result{} // already collected
	}
	if !t.Decided() {
		return Result{Err: fmt.Errorf("coordsm: TxnAcked %q by shard %d before any decision", id, shard)}
	}
	if !slices.ContainsFunc(t.Participants, func(p Participant) bool { return p.Shard == shard }) {
		return Result{Err: fmt.Errorf("coordsm: TxnAcked %q by shard %d, which is not a participant", id, shard)}
	}
	if i, found := slices.BinarySearch(t.Acked, shard); !found {
		t.Acked = slices.Insert(t.Acked, i, shard)
	}
	if len(t.Acked) == len(t.Participants) {
		delete(s.unacked, id)
	}
	return Result{Txn: t.clone()}
}

func (s *state) gc(cutoff int64) int {
	n := 0
	for id, t := range s.txns {
		if t.Decided() && t.DecidedMs < cutoff && len(t.Acked) == len(t.Participants) {
			delete(s.txns, id)
			n++
		}
	}
	return n
}

// Get returns a copy of one txn's record.
func (m *Machine) Get(id string) (*Txn, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.txns[id]
	if !ok {
		return nil, false
	}
	return t.clone(), true
}

// Undecided returns the txns with no decision yet, sorted by id.
func (m *Machine) Undecided() []*Txn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return sortedCopies(m.undecided)
}

// Unacked returns the decided txns that a participant has not acked yet,
// sorted by id.
func (m *Machine) Unacked() []*Txn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return sortedCopies(m.unacked)
}

// Counts returns the number of txns in total, without a decision, and decided
// but missing an ack.
func (m *Machine) Counts() (total, undecided, unacked int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.txns), len(m.undecided), len(m.unacked)
}

func sortedCopies(txns map[string]*Txn) []*Txn {
	out := make([]*Txn, 0, len(txns))
	for _, t := range txns {
		out = append(out, t.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// CheckInvariants verifies that every record is well formed: participants
// sorted by shard with seats, an undecided txn has no outcome, seats, acks or
// decision time, a decision goes with its outcome, acks are sorted and come
// only from participants, and both indexes match the records.
func (m *Machine) CheckInvariants() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.check()
}

func (s *state) check() error {
	var errs []string
	bad := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }

	for id, t := range s.txns {
		if id == "" || t.ID != id {
			bad("txn stored under %q has id %q", id, t.ID)
		}
		for i, p := range t.Participants {
			if p.Shard == 0 || (i > 0 && p.Shard <= t.Participants[i-1].Shard) {
				bad("txn %q: participants not sorted by a positive shard id", id)
			}
			if len(p.Seats) == 0 {
				bad("txn %q: no seats on shard %d", id, p.Shard)
			}
		}
		if t.Decided() {
			if err := checkDecision(t.Decision, t.Outcome, len(t.Unavailable)); err != nil {
				bad("txn %q: %v", id, err)
			}
		} else {
			if t.Outcome != outcomeNone || len(t.Unavailable) > 0 || len(t.Acked) > 0 || t.DecidedMs != 0 {
				bad("txn %q is undecided but has an outcome, seats, acks or a decision time", id)
			}
			if len(t.Participants) == 0 {
				bad("txn %q is undecided and has no participants", id)
			}
		}
		for i, sh := range t.Acked {
			if i > 0 && sh <= t.Acked[i-1] {
				bad("txn %q: acks not sorted", id)
			}
			if !slices.ContainsFunc(t.Participants, func(p Participant) bool { return p.Shard == sh }) {
				bad("txn %q: ack from shard %d, which is not a participant", id, sh)
			}
		}
		if (s.undecided[id] == t) != !t.Decided() {
			bad("txn %q and the undecided index disagree", id)
		}
		if (s.unacked[id] == t) != (t.Decided() && len(t.Acked) < len(t.Participants)) {
			bad("txn %q and the unacked index disagree", id)
		}
	}
	for id := range s.undecided {
		if s.txns[id] == nil {
			bad("undecided index lists unknown txn %q", id)
		}
	}
	for id := range s.unacked {
		if s.txns[id] == nil {
			bad("unacked index lists unknown txn %q", id)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return errors.New("coordsm: invariant violated: " + strings.Join(errs, "; "))
}
