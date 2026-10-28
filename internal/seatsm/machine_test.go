package seatsm

import (
	"reflect"
	"strings"
	"testing"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"google.golang.org/protobuf/proto"
)

func ref(e, s, id string) *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: e, SectionId: s, SeatId: id}
}

// Test universe: evt-a/sec-1 {s1..s4}, evt-a/sec-2 {s1,s2}, evt-b/sec-1 {s1,s2},
// seeded in two chunks.
var (
	a1 = ref("evt-a", "sec-1", "s1")
	a2 = ref("evt-a", "sec-1", "s2")
	a3 = ref("evt-a", "sec-1", "s3")
	a4 = ref("evt-a", "sec-1", "s4")
	c1 = ref("evt-a", "sec-2", "s1")
	c2 = ref("evt-a", "sec-2", "s2")
	b1 = ref("evt-b", "sec-1", "s1")
	b2 = ref("evt-b", "sec-1", "s2")

	unknown = ref("evt-a", "sec-1", "nope")
)

const testVersion = "v1"

func seedCmd(now int64, version string, chunk, total uint32, seats ...*seatsafev1.SeatRef) *seatsafev1.ShardCommand {
	return &seatsafev1.ShardCommand{NowMs: now, Op: &seatsafev1.ShardCommand_Seed{Seed: &seatsafev1.SeedOp{
		SeedVersion: version, Chunk: chunk, TotalChunks: total, Seats: seats,
	}}}
}

func seedChunks() []*seatsafev1.ShardCommand {
	return []*seatsafev1.ShardCommand{
		seedCmd(1, testVersion, 0, 2, a3, a1, a4, a2),
		seedCmd(1, testVersion, 1, 2, c2, c1, b2, b1),
	}
}

func bookCmd(now int64, req string, seats ...*seatsafev1.SeatRef) *seatsafev1.ShardCommand {
	return &seatsafev1.ShardCommand{NowMs: now, Op: &seatsafev1.ShardCommand_Book{Book: &seatsafev1.BookOp{
		RequestId: req, UserId: "u-" + req, Seats: seats,
	}}}
}

func prepCmd(now int64, txn string, hold int64, seats ...*seatsafev1.SeatRef) *seatsafev1.ShardCommand {
	return &seatsafev1.ShardCommand{NowMs: now, Op: &seatsafev1.ShardCommand_Prepare{Prepare: &seatsafev1.PrepareOp{
		TxnId: txn, UserId: "u-" + txn, Seats: seats, HoldExpiresAtMs: hold,
	}}}
}

func commitCmd(now int64, txn string) *seatsafev1.ShardCommand {
	return &seatsafev1.ShardCommand{NowMs: now, Op: &seatsafev1.ShardCommand_Commit{Commit: &seatsafev1.TxnOp{TxnId: txn}}}
}

func abortCmd(now int64, txn string) *seatsafev1.ShardCommand {
	return &seatsafev1.ShardCommand{NowMs: now, Op: &seatsafev1.ShardCommand_Abort{Abort: &seatsafev1.TxnOp{TxnId: txn}}}
}

func gcCmd(now, before int64) *seatsafev1.ShardCommand {
	return &seatsafev1.ShardCommand{NowMs: now, Op: &seatsafev1.ShardCommand_Gc{Gc: &seatsafev1.GcOp{DropBeforeMs: before}}}
}

// harness applies commands with increasing indexes and checks invariants
// after each one.
type harness struct {
	t *testing.T
	m *Machine
}

func newHarness(t *testing.T, seeded bool) *harness {
	t.Helper()
	h := &harness{t: t, m: New()}
	if seeded {
		for _, c := range seedChunks() {
			if r := h.apply(c); r.Err != nil {
				t.Fatalf("seed: %v", r.Err)
			}
		}
	}
	return h
}

func (h *harness) apply(cmd *seatsafev1.ShardCommand) Result {
	h.t.Helper()
	r := h.m.Apply(h.m.AppliedIndex()+1, cmd)
	if err := h.m.CheckInvariants(); err != nil {
		h.t.Fatal(err)
	}
	return r
}

// applyAll applies cmds and fails on any Err.
func (h *harness) applyAll(cmds ...*seatsafev1.ShardCommand) {
	h.t.Helper()
	for _, c := range cmds {
		if r := h.apply(c); r.Err != nil {
			h.t.Fatalf("setup: %v", r.Err)
		}
	}
}

// seat returns state and owner of a seat.
func (h *harness) seat(r *seatsafev1.SeatRef) (seatsafev1.SeatState, string) {
	h.t.Helper()
	st := h.m.lookup(keyOf(r))
	if st == nil {
		h.t.Fatalf("seat %v missing", r)
	}
	return st.state, st.owner
}

func (h *harness) wantSeat(r *seatsafev1.SeatRef, state seatsafev1.SeatState, owner string) {
	h.t.Helper()
	if s, o := h.seat(r); s != state || o != owner {
		h.t.Fatalf("seat %s/%s = %v owned by %q, want %v by %q", r.SectionId, r.SeatId, s, o, state, owner)
	}
}

func (h *harness) txn(id string) *txnRecord { return h.m.txns[id] }

func (h *harness) wantTxn(id string, status txnStatus) *txnRecord {
	h.t.Helper()
	tr := h.m.txns[id]
	if tr == nil || tr.status != status {
		h.t.Fatalf("txn %q = %+v, want %v", id, tr, status)
	}
	return tr
}

func (h *harness) snap() []byte {
	h.t.Helper()
	b, err := h.m.Snapshot()
	if err != nil {
		h.t.Fatal(err)
	}
	return b
}

// unchanged runs cmd and fails unless the snapshot differs only in the
// applied index.
func (h *harness) unchanged(cmd *seatsafev1.ShardCommand) Result {
	h.t.Helper()
	before := h.snap()
	r := h.apply(cmd)
	after := h.snap()
	m2 := New()
	if err := m2.Restore(before); err != nil {
		h.t.Fatal(err)
	}
	m2.applied = h.m.applied
	b2, _ := m2.Snapshot()
	if string(b2) != string(after) {
		h.t.Fatalf("state changed:\nbefore %s\nafter  %s", before, after)
	}
	return r
}

func wantBook(t *testing.T, r Result, outcome seatsafev1.BookingOutcome, replayed bool, unavailable ...*seatsafev1.SeatRef) {
	t.Helper()
	if r.Err != nil || r.Book == nil || r.Prepare != nil {
		t.Fatalf("result = %+v", r)
	}
	b := r.Book
	if b.Outcome != outcome || b.Replayed != replayed || b.CrossShard {
		t.Fatalf("book = %v, want outcome %v replayed %v", b, outcome, replayed)
	}
	if !refsEqual(b.UnavailableSeats, unavailable) {
		t.Fatalf("unavailable = %v, want %v", b.UnavailableSeats, unavailable)
	}
}

func wantVote(t *testing.T, r Result, vote seatsafev1.Vote, conflicts ...*seatsafev1.SeatRef) {
	t.Helper()
	if r.Err != nil || r.Prepare == nil || r.Book != nil {
		t.Fatalf("result = %+v", r)
	}
	if r.Prepare.Vote != vote || !refsEqual(r.Prepare.Conflicts, conflicts) {
		t.Fatalf("prepare = %v, want %v %v", r.Prepare, vote, conflicts)
	}
}

func wantErr(t *testing.T, r Result, substr string) {
	t.Helper()
	if r.Err == nil || !strings.Contains(r.Err.Error(), substr) || r.Book != nil || r.Prepare != nil {
		t.Fatalf("result = %+v, want Err containing %q", r, substr)
	}
}

func wantOK(t *testing.T, r Result) {
	t.Helper()
	if !reflect.DeepEqual(r, Result{}) {
		t.Fatalf("result = %+v, want empty", r)
	}
}

func refsEqual(a, b []*seatsafev1.SeatRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// One case per rule.
func TestRules(t *testing.T) {
	cases := []struct {
		name   string
		seeded bool
		run    func(t *testing.T, h *harness)
	}{
		// Seed
		{"seed/first op sets version and total", false, func(t *testing.T, h *harness) {
			wantOK(t, h.apply(seedCmd(1, testVersion, 1, 2, c1)))
			v, applied, total, done := h.m.SeedStatus()
			if v != testVersion || applied != 1 || total != 2 || done {
				t.Fatalf("status = %q %d/%d %v", v, applied, total, done)
			}
			if c, ok := h.m.NextMissingChunk(); !ok || c != 0 {
				t.Fatalf("next = %d %v", c, ok)
			}
		}},
		{"seed/different version refused", false, func(t *testing.T, h *harness) {
			h.applyAll(seedChunks()[0])
			wantErr(t, h.unchanged(seedCmd(1, "v2", 1, 2, c1)), "refused")
		}},
		{"seed/different total refused", false, func(t *testing.T, h *harness) {
			h.applyAll(seedChunks()[0])
			wantErr(t, h.unchanged(seedCmd(1, testVersion, 1, 3, c1)), "refused")
		}},
		{"seed/malformed op refused", false, func(t *testing.T, h *harness) {
			wantErr(t, h.unchanged(seedCmd(1, "", 0, 1, a1)), "empty seed_version")
			wantErr(t, h.unchanged(seedCmd(1, testVersion, 0, 0, a1)), "out of range")
			wantErr(t, h.unchanged(seedCmd(1, testVersion, 2, 2, a1)), "out of range")
			wantErr(t, h.unchanged(seedCmd(1, testVersion, 0, 2, a1, ref("evt-a", "", "x"))), "empty field")
			if v, _, _, _ := h.m.SeedStatus(); v != "" {
				t.Fatalf("version = %q after refused ops", v)
			}
		}},
		{"seed/chunk already applied is a no-op", true, func(t *testing.T, h *harness) {
			h.applyAll(bookCmd(5, "r1", a1))
			wantOK(t, h.unchanged(seedCmd(6, testVersion, 0, 2, a1, ref("evt-a", "sec-9", "new"))))
			h.wantSeat(a1, booked, "r1")
		}},
		{"seed/existing seat is skipped", false, func(t *testing.T, h *harness) {
			h.applyAll(seedCmd(1, testVersion, 0, 3, a1, a2), seedCmd(1, testVersion, 1, 3, b1))
			h.applyAll(seedCmd(1, testVersion, 2, 3, a1, a3)) // a1 already exists
			h.applyAll(bookCmd(2, "r1", a1))
			if got := h.m.ListAvailable("evt-a", "sec-1").Sections[0].AvailableSeatIds; !reflect.DeepEqual(got, []string{"s2", "s3"}) {
				t.Fatalf("available = %v", got)
			}
		}},
		{"seed/done when all chunks applied", false, func(t *testing.T, h *harness) {
			h.applyAll(seedChunks()[1])
			if c, ok := h.m.NextMissingChunk(); !ok || c != 0 {
				t.Fatalf("next = %d %v", c, ok)
			}
			h.applyAll(seedChunks()[0])
			if _, applied, total, done := h.m.SeedStatus(); applied != 2 || total != 2 || !done {
				t.Fatalf("status = %d/%d %v", applied, total, done)
			}
			if _, ok := h.m.NextMissingChunk(); ok {
				t.Fatal("seeded machine reports a missing chunk")
			}
		}},

		// Not seeded
		{"unseeded/book INVALID and not stored", false, func(t *testing.T, h *harness) {
			h.applyAll(seedChunks()[0])
			wantBook(t, h.unchanged(bookCmd(2, "r1", a1)), outcomeInvalid, false)
			h.applyAll(seedChunks()[1])
			wantBook(t, h.apply(bookCmd(3, "r1", a1)), outcomeConfirmed, false)
		}},
		{"unseeded/prepare NO and not recorded", false, func(t *testing.T, h *harness) {
			wantVote(t, h.unchanged(prepCmd(2, "t1", 100, a1)), voteNo)
			h.applyAll(seedChunks()...)
			wantVote(t, h.apply(prepCmd(3, "t1", 100, a1)), voteYes)
		}},

		// Book
		{"book/empty request_id INVALID and not stored", true, func(t *testing.T, h *harness) {
			r := h.unchanged(bookCmd(2, "", a1))
			wantBook(t, r, outcomeInvalid, false)
			if len(h.m.reqs) != 0 {
				t.Fatal("stored a request with no id")
			}
		}},
		{"book/existing request replays a copy", true, func(t *testing.T, h *harness) {
			first := h.apply(bookCmd(2, "r1", a1))
			wantBook(t, first, outcomeConfirmed, false)
			// A retry with different seats still gets the stored answer.
			r := h.unchanged(bookCmd(3, "r1", a2))
			wantBook(t, r, outcomeConfirmed, true)
			r.Book.UnavailableSeats = append(r.Book.UnavailableSeats, a3)
			first.Book.Outcome = outcomeInvalid
			wantBook(t, h.apply(bookCmd(4, "r1", a1)), outcomeConfirmed, true)
			h.wantSeat(a2, available, "")
		}},
		{"book/empty seat list INVALID and stored", true, func(t *testing.T, h *harness) {
			wantBook(t, h.apply(bookCmd(2, "r1")), outcomeInvalid, false)
			wantBook(t, h.apply(bookCmd(3, "r1", a1)), outcomeInvalid, true)
			h.wantSeat(a1, available, "")
		}},
		{"book/seats of two events INVALID", true, func(t *testing.T, h *harness) {
			wantBook(t, h.apply(bookCmd(2, "r1", a1, b1)), outcomeInvalid, false)
			h.wantSeat(a1, available, "")
			wantBook(t, h.apply(bookCmd(3, "r1")), outcomeInvalid, true)
		}},
		{"book/duplicate seat INVALID", true, func(t *testing.T, h *harness) {
			wantBook(t, h.apply(bookCmd(2, "r1", a1, a2, ref("evt-a", "sec-1", "s1"))), outcomeInvalid, false)
			h.wantSeat(a1, available, "")
			h.wantSeat(a2, available, "")
		}},
		{"book/unknown seat INVALID", true, func(t *testing.T, h *harness) {
			wantBook(t, h.apply(bookCmd(2, "r1", a1, unknown)), outcomeInvalid, false)
			h.wantSeat(a1, available, "")
			wantBook(t, h.apply(bookCmd(3, "r2", ref("evt-z", "sec-1", "s1"))), outcomeInvalid, false)
			wantBook(t, h.apply(bookCmd(3, "r1")), outcomeInvalid, true)
		}},
		{"book/held or booked seat SEAT_UNAVAILABLE", true, func(t *testing.T, h *harness) {
			h.applyAll(bookCmd(2, "r0", a3), prepCmd(2, "t1", 100, a1))
			// The answer is stored, but no seat changes.
			r := h.apply(bookCmd(3, "r1", a1, a2, a3, c1))
			wantBook(t, r, outcomeUnavailable, false, a1, a3)
			h.wantSeat(a1, held, "t1")
			h.wantSeat(a2, available, "")
			h.wantSeat(a3, booked, "r0")
			h.wantSeat(c1, available, "")
			wantBook(t, h.apply(bookCmd(4, "r1", c2)), outcomeUnavailable, true, a1, a3)
		}},
		{"book/all available CONFIRMED", true, func(t *testing.T, h *harness) {
			r := h.apply(bookCmd(2, "r1", a2, c1))
			wantBook(t, r, outcomeConfirmed, false)
			if r.Book.OrderId != "r1" {
				t.Fatalf("order_id = %q", r.Book.OrderId)
			}
			h.wantSeat(a2, booked, "r1")
			h.wantSeat(c1, booked, "r1")
			if st := h.m.lookup(keyOf(a2)); st.user != "u-r1" {
				t.Fatalf("user = %q", st.user)
			}
		}},

		// Prepare
		{"prepare/PREPARED votes YES again", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1))
			wantVote(t, h.unchanged(prepCmd(3, "t1", 999, a2)), voteYes)
		}},
		{"prepare/COMMITTED votes YES again", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1), commitCmd(3, "t1"))
			wantVote(t, h.unchanged(prepCmd(4, "t1", 100, a1)), voteYes)
		}},
		{"prepare/ABORTED votes NO with stored conflicts", true, func(t *testing.T, h *harness) {
			h.applyAll(bookCmd(2, "r1", a2))
			wantVote(t, h.apply(prepCmd(3, "t1", 100, a1, a2)), voteNo, a2)
			wantVote(t, h.unchanged(prepCmd(4, "t1", 100, a3)), voteNo, a2)
		}},
		{"prepare/tombstone votes NO with no conflicts", true, func(t *testing.T, h *harness) {
			h.applyAll(abortCmd(2, "t1"))
			wantVote(t, h.unchanged(prepCmd(3, "t1", 100, a1)), voteNo)
			h.applyAll(prepCmd(3, "t2", 100, a2), abortCmd(4, "t2"))
			wantVote(t, h.unchanged(prepCmd(5, "t2", 100, a2)), voteNo)
		}},
		{"prepare/unknown seat NO, recorded ABORTED", true, func(t *testing.T, h *harness) {
			wantVote(t, h.apply(prepCmd(2, "t1", 100, a1, unknown)), voteNo)
			tr := h.wantTxn("t1", txnAborted)
			if tr.vote != voteNo || len(tr.conflicts) != 0 || tr.decidedAt != 2 {
				t.Fatalf("txn = %+v", tr)
			}
			h.wantSeat(a1, available, "")
		}},
		{"prepare/invalid request NO, recorded ABORTED", true, func(t *testing.T, h *harness) {
			for i, seats := range [][]*seatsafev1.SeatRef{nil, {a1, a1}, {a1, b1}} {
				id := "t" + string(rune('0'+i))
				wantVote(t, h.apply(prepCmd(2, id, 100, seats...)), voteNo)
				h.wantTxn(id, txnAborted)
			}
			h.wantSeat(a1, available, "")
		}},
		{"prepare/booked seat NO with conflicts, recorded ABORTED", true, func(t *testing.T, h *harness) {
			h.applyAll(bookCmd(2, "r1", a3, a1))
			wantVote(t, h.apply(prepCmd(3, "t1", 100, a3, a2, a1)), voteNo, a3, a1)
			tr := h.wantTxn("t1", txnAborted)
			if !reflect.DeepEqual(tr.conflicts, []seatKey{keyOf(a3), keyOf(a1)}) {
				t.Fatalf("conflicts = %v", tr.conflicts)
			}
			h.wantSeat(a2, available, "")
		}},
		{"prepare/seat held by another txn NO", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1))
			wantVote(t, h.apply(prepCmd(3, "t2", 100, a2, a1)), voteNo, a1)
			h.wantSeat(a1, held, "t1")
			h.wantSeat(a2, available, "")
			h.wantTxn("t2", txnAborted)
		}},
		{"prepare/all available YES, seats HELD", true, func(t *testing.T, h *harness) {
			wantVote(t, h.apply(prepCmd(2, "t1", 500, a1, c1)), voteYes)
			h.wantSeat(a1, held, "t1")
			h.wantSeat(c1, held, "t1")
			tr := h.wantTxn("t1", txnPrepared)
			if tr.holdExpires != 500 || tr.user != "u-t1" || tr.vote != voteYes {
				t.Fatalf("txn = %+v", tr)
			}
		}},
		{"prepare/empty txn_id NO and not recorded", true, func(t *testing.T, h *harness) {
			wantVote(t, h.unchanged(prepCmd(2, "", 100, a1)), voteNo)
		}},

		// Commit
		{"commit/PREPARED books its seats", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1, a2))
			wantOK(t, h.apply(commitCmd(3, "t1")))
			h.wantSeat(a1, booked, "t1")
			h.wantSeat(a2, booked, "t1")
			if tr := h.wantTxn("t1", txnCommitted); tr.decidedAt != 3 {
				t.Fatalf("decided at %d", tr.decidedAt)
			}
		}},
		{"commit/COMMITTED is a no-op", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1), commitCmd(3, "t1"))
			wantOK(t, h.unchanged(commitCmd(4, "t1")))
		}},
		{"commit/unknown txn is a no-op", true, func(t *testing.T, h *harness) {
			wantOK(t, h.unchanged(commitCmd(2, "t1")))
		}},
		{"commit/ABORTED is a protocol violation", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1), abortCmd(3, "t1"))
			wantErr(t, h.unchanged(commitCmd(4, "t1")), "commit of ABORTED")
			wantErr(t, h.unchanged(commitCmd(4, "")), "empty txn_id")
		}},

		// Abort
		{"abort/PREPARED releases its seats", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1, a2))
			wantOK(t, h.apply(abortCmd(3, "t1")))
			h.wantSeat(a1, available, "")
			h.wantSeat(a2, available, "")
			h.wantTxn("t1", txnAborted)
			wantBook(t, h.apply(bookCmd(4, "r1", a1)), outcomeConfirmed, false)
		}},
		{"abort/unknown txn writes a tombstone", true, func(t *testing.T, h *harness) {
			wantOK(t, h.apply(abortCmd(2, "t1")))
			if tr := h.wantTxn("t1", txnAborted); tr.decidedAt != 2 || len(tr.seats) != 0 {
				t.Fatalf("tombstone = %+v", tr)
			}
			wantVote(t, h.apply(prepCmd(3, "t1", 100, a1)), voteNo)
			h.wantSeat(a1, available, "")
		}},
		{"abort/ABORTED is a no-op", true, func(t *testing.T, h *harness) {
			h.applyAll(abortCmd(2, "t1"))
			wantOK(t, h.unchanged(abortCmd(3, "t1")))
		}},
		{"abort/COMMITTED is a protocol violation", true, func(t *testing.T, h *harness) {
			h.applyAll(prepCmd(2, "t1", 100, a1), commitCmd(3, "t1"))
			wantErr(t, h.unchanged(abortCmd(4, "t1")), "abort of COMMITTED")
			wantErr(t, h.unchanged(abortCmd(4, "")), "empty txn_id")
		}},

		// Gc
		{"gc/drops old req/ records", true, func(t *testing.T, h *harness) {
			h.applyAll(bookCmd(10, "r1", a1), bookCmd(20, "r2", a2), gcCmd(30, 20))
			if _, ok := h.m.reqs["r1"]; ok {
				t.Fatal("r1 not collected")
			}
			wantBook(t, h.apply(bookCmd(31, "r2", a2)), outcomeConfirmed, true)
			// A retry after GC is a new request: the seat is now taken.
			wantBook(t, h.apply(bookCmd(32, "r1", a1)), outcomeUnavailable, false, a1)
		}},
		{"gc/drops old decided txns only", true, func(t *testing.T, h *harness) {
			h.applyAll(
				prepCmd(10, "old-commit", 0, a1), commitCmd(11, "old-commit"),
				abortCmd(12, "old-abort"),
				prepCmd(13, "old-prepared", 0, a2),
				prepCmd(20, "new-commit", 0, a3), commitCmd(21, "new-commit"),
				gcCmd(30, 21),
			)
			for id, want := range map[string]bool{"old-commit": false, "old-abort": false, "old-prepared": true, "new-commit": true} {
				if _, ok := h.m.txns[id]; ok != want {
					t.Fatalf("txn %q kept = %v, want %v", id, ok, want)
				}
			}
			h.wantSeat(a1, booked, "old-commit")
			h.wantSeat(a2, held, "old-prepared")
			if _, _, _, done := h.m.SeedStatus(); !done {
				t.Fatal("gc dropped seed progress")
			}
			if len(h.m.ExpiredHolds(100)) != 1 {
				t.Fatal("gc dropped a hold")
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, newHarness(t, tc.seeded)) })
	}
}

func TestApplyIndex(t *testing.T) {
	h := newHarness(t, true)
	if h.m.AppliedIndex() != 2 {
		t.Fatalf("applied = %d", h.m.AppliedIndex())
	}
	// Old or repeated indexes are no-ops with an empty Result.
	for _, idx := range []uint64{0, 1, 2} {
		wantOK(t, h.m.Apply(idx, bookCmd(5, "r1", a1)))
	}
	h.wantSeat(a1, available, "")
	// Gaps are allowed.
	wantBook(t, h.m.Apply(10, bookCmd(5, "r1", a1)), outcomeConfirmed, false)
	if h.m.AppliedIndex() != 10 {
		t.Fatalf("applied = %d", h.m.AppliedIndex())
	}
	// Commands with no op are refused but still consume the index.
	wantErr(t, h.m.Apply(11, nil), "no command")
	wantErr(t, h.m.Apply(12, &seatsafev1.ShardCommand{NowMs: 1}), "no command")
	if h.m.AppliedIndex() != 12 {
		t.Fatalf("applied = %d", h.m.AppliedIndex())
	}
}

func TestExpiredHolds(t *testing.T) {
	h := newHarness(t, true)
	h.applyAll(
		prepCmd(1, "t-b", 100, a1),
		prepCmd(1, "t-a", 100, a2),
		prepCmd(1, "t-c", 200, a3),
		prepCmd(1, "t-done", 50, a4), commitCmd(2, "t-done"),
	)
	if got := h.m.ExpiredHolds(99); len(got) != 0 {
		t.Fatalf("ExpiredHolds(99) = %v", got)
	}
	if got := h.m.ExpiredHolds(100); !reflect.DeepEqual(got, []string{"t-a", "t-b"}) {
		t.Fatalf("ExpiredHolds(100) = %v", got)
	}
	if got := h.m.ExpiredHolds(1000); !reflect.DeepEqual(got, []string{"t-a", "t-b", "t-c"}) {
		t.Fatalf("ExpiredHolds(1000) = %v", got)
	}
	// Time passing (even in a command) does not release a hold.
	h.applyAll(gcCmd(5000, 0))
	h.wantSeat(a1, held, "t-b")
	h.applyAll(abortCmd(5001, "t-b"))
	if got := h.m.ExpiredHolds(1000); !reflect.DeepEqual(got, []string{"t-a", "t-c"}) {
		t.Fatalf("after abort = %v", got)
	}
}

func TestListAvailable(t *testing.T) {
	h := newHarness(t, true)
	h.applyAll(bookCmd(1, "r1", a2), prepCmd(1, "t1", 9, c1, c2))

	sec := func(e, s string, ids ...string) *seatsafev1.SectionAvailability {
		if ids == nil {
			ids = []string{}
		}
		return &seatsafev1.SectionAvailability{EventId: e, SectionId: s, AvailableSeatIds: ids}
	}
	cases := []struct {
		event, section string
		want           []*seatsafev1.SectionAvailability
	}{
		{"evt-a", "", []*seatsafev1.SectionAvailability{sec("evt-a", "sec-1", "s1", "s3", "s4"), sec("evt-a", "sec-2")}},
		{"evt-a", "sec-1", []*seatsafev1.SectionAvailability{sec("evt-a", "sec-1", "s1", "s3", "s4")}},
		{"evt-b", "", []*seatsafev1.SectionAvailability{sec("evt-b", "sec-1", "s1", "s2")}},
		{"evt-a", "sec-9", nil},
		{"evt-z", "", nil},
		{"", "", nil},
	}
	for _, tc := range cases {
		got := h.m.ListAvailable(tc.event, tc.section)
		want := &seatsafev1.ListAvailableSeatsResponse{Sections: tc.want}
		if !proto.Equal(got, want) {
			t.Errorf("ListAvailable(%q, %q) = %v, want %v", tc.event, tc.section, got, want)
		}
	}
}

func TestCheckInvariantsDetectsCorruption(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(m *Machine)
		want    string
	}{
		{"held seat without txn", func(m *Machine) { m.lookup(keyOf(a1)).state, m.lookup(keyOf(a1)).owner = held, "ghost" }, "not a PREPARED txn"},
		{"booked seat without owner", func(m *Machine) { m.lookup(keyOf(a1)).state = booked }, "no owner"},
		{"available seat with owner", func(m *Machine) { m.lookup(keyOf(a1)).owner = "x" }, "AVAILABLE but owned"},
		{"bad state", func(m *Machine) { m.lookup(keyOf(a1)).state = 0 }, "in state"},
		{"prepared txn not holding", func(m *Machine) { m.lookup(keyOf(a3)).state, m.lookup(keyOf(a3)).owner = available, "" }, "does not hold"},
		{"seat in two prepared txns", func(m *Machine) {
			tr := &txnRecord{status: txnPrepared, seats: []seatKey{keyOf(a3)}}
			m.txns["t2"], m.prepared["t2"] = tr, tr
		}, "in PREPARED txns"},
		{"prepared index stale", func(m *Machine) { delete(m.prepared, "t1") }, "disagree"},
		{"section index stale", func(m *Machine) {
			sec := m.sections[sectionKey{"evt-a", "sec-1"}]
			sec.sorted = sec.sorted[1:]
		}, "index has"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, true)
			h.applyAll(prepCmd(1, "t1", 9, a3))
			tc.corrupt(h.m)
			if err := h.m.CheckInvariants(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckInvariants = %v, want %q", err, tc.want)
			}
		})
	}
}
