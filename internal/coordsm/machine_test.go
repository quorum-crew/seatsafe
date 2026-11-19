package coordsm

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

func ref(section string, n int) *seatsafev1.SeatRef {
	return &seatsafev1.SeatRef{EventId: "evt-1", SectionId: section, SeatId: fmt.Sprintf("R01-S%03d", n)}
}

func parts(shards ...uint32) []*seatsafev1.ShardSeats {
	var out []*seatsafev1.ShardSeats
	for _, s := range shards {
		out = append(out, &seatsafev1.ShardSeats{ShardId: s, Seats: []*seatsafev1.SeatRef{ref(fmt.Sprintf("sec-%d", s), 1), ref(fmt.Sprintf("sec-%d", s), 2)}})
	}
	return out
}

func startedCmd(now int64, id string, shards ...uint32) *seatsafev1.CoordCommand {
	return &seatsafev1.CoordCommand{NowMs: now, Op: &seatsafev1.CoordCommand_Started{Started: &seatsafev1.TxnStarted{TxnId: id, UserId: "u-" + id, Participants: parts(shards...)}}}
}

func decidedCmd(now int64, id string, d seatsafev1.Decision, o seatsafev1.BookingOutcome, unavailable ...*seatsafev1.SeatRef) *seatsafev1.CoordCommand {
	return &seatsafev1.CoordCommand{NowMs: now, Op: &seatsafev1.CoordCommand_Decided{Decided: &seatsafev1.TxnDecided{TxnId: id, Decision: d, Outcome: o, UnavailableSeats: unavailable}}}
}

func commitCmd(now int64, id string) *seatsafev1.CoordCommand {
	return decidedCmd(now, id, decisionCommit, outcomeConfirmed)
}

func abortCmd(now int64, id string) *seatsafev1.CoordCommand {
	return decidedCmd(now, id, decisionAbort, outcomeAborted)
}

func ackedCmd(now int64, id string, shard uint32) *seatsafev1.CoordCommand {
	return &seatsafev1.CoordCommand{NowMs: now, Op: &seatsafev1.CoordCommand_Acked{Acked: &seatsafev1.TxnAcked{TxnId: id, ShardId: shard}}}
}

func gcCmd(now, cutoff int64) *seatsafev1.CoordCommand {
	return &seatsafev1.CoordCommand{NowMs: now, Op: &seatsafev1.CoordCommand_Gc{Gc: &seatsafev1.GcOp{DropBeforeMs: cutoff}}}
}

// harness applies commands at increasing indexes and checks the invariants
// after every one.
type harness struct {
	t     *testing.T
	m     *Machine
	index uint64
}

func newHarness(t *testing.T) *harness { return &harness{t: t, m: New()} }

func (h *harness) apply(cmd *seatsafev1.CoordCommand) Result {
	h.t.Helper()
	h.index++
	res := h.m.Apply(h.index, cmd)
	if err := h.m.CheckInvariants(); err != nil {
		h.t.Fatalf("after entry %d (%v): %v", h.index, cmd, err)
	}
	return res
}

func (h *harness) ok(cmd *seatsafev1.CoordCommand) Result {
	h.t.Helper()
	res := h.apply(cmd)
	if res.Err != nil {
		h.t.Fatalf("entry %d (%v) refused: %v", h.index, cmd, res.Err)
	}
	return res
}

func (h *harness) refused(cmd *seatsafev1.CoordCommand, want string) {
	h.t.Helper()
	before, _ := h.m.Snapshot()
	res := h.apply(cmd)
	if res.Err == nil || !strings.Contains(res.Err.Error(), want) {
		h.t.Fatalf("entry %d (%v): err %v, want one containing %q", h.index, cmd, res.Err, want)
	}
	if res.Txn != nil || res.Created || res.Won {
		h.t.Fatalf("refused command returned %+v", res)
	}
	// Only AppliedIndex may have moved.
	h.m.state.applied--
	after, _ := h.m.Snapshot()
	h.m.state.applied++
	if !bytes.Equal(before, after) {
		h.t.Fatalf("refused command changed the state:\n%s\n%s", before, after)
	}
}

func (h *harness) get(id string) *Txn {
	h.t.Helper()
	t, ok := h.m.Get(id)
	if !ok {
		h.t.Fatalf("txn %q not found", id)
	}
	return t
}

func ids(txns []*Txn) []string {
	out := []string{}
	for _, t := range txns {
		out = append(out, t.ID)
	}
	return out
}

func TestStarted(t *testing.T) {
	h := newHarness(t)
	res := h.ok(startedCmd(100, "t1", 1, 3))
	if !res.Created || res.Txn == nil {
		t.Fatalf("first TxnStarted: %+v", res)
	}
	want := &Txn{ID: "t1", User: "u-t1", StartedMs: 100, Participants: []Participant{
		{Shard: 1, Seats: []Seat{{"evt-1", "sec-1", "R01-S001"}, {"evt-1", "sec-1", "R01-S002"}}},
		{Shard: 3, Seats: []Seat{{"evt-1", "sec-3", "R01-S001"}, {"evt-1", "sec-3", "R01-S002"}}},
	}}
	if !reflect.DeepEqual(res.Txn, want) || !reflect.DeepEqual(h.get("t1"), want) {
		t.Fatalf("record %+v, want %+v", res.Txn, want)
	}
	if got := res.Txn.Participants[1].Seats[0].String(); got != "evt-1/sec-3/R01-S001" {
		t.Fatalf("seat prints as %q", got)
	}
	if res.Txn.Decided() || !reflect.DeepEqual(res.Txn.MissingAcks(), []uint32{1, 3}) {
		t.Fatalf("new txn: decided %v, missing acks %v", res.Txn.Decided(), res.Txn.MissingAcks())
	}

	// A second TxnStarted is a no-op, even with other contents.
	res = h.ok(startedCmd(200, "t1", 2))
	if res.Created || !reflect.DeepEqual(res.Txn, want) {
		t.Fatalf("second TxnStarted: %+v", res)
	}
	// And so is one for a txn that is already decided.
	h.ok(abortCmd(300, "t1"))
	res = h.ok(startedCmd(400, "t1", 1, 3))
	if res.Created || res.Txn.Decision != decisionAbort || res.Txn.StartedMs != 100 {
		t.Fatalf("TxnStarted after the decision: %+v", res.Txn)
	}
	if total, undecided, unacked := h.m.Counts(); total != 1 || undecided != 0 || unacked != 1 {
		t.Fatalf("counts %d %d %d", total, undecided, unacked)
	}
}

func TestStartedRefused(t *testing.T) {
	h := newHarness(t)
	h.ok(startedCmd(1, "keep", 1, 2))
	noSeats := startedCmd(1, "x", 1, 2)
	noSeats.GetStarted().Participants[1].Seats = nil
	for _, c := range []struct {
		cmd  *seatsafev1.CoordCommand
		want string
	}{
		{startedCmd(1, "", 1, 2), "empty txn_id"},
		{startedCmd(1, "x"), "no participants"},
		{startedCmd(1, "x", 0, 1), "shard id 0"},
		{startedCmd(1, "x", 2, 1), "not sorted"},
		{startedCmd(1, "x", 2, 2), "not sorted"},
		{noSeats, "no seats on shard 2"},
		{&seatsafev1.CoordCommand{NowMs: 1}, "no command"},
	} {
		h.refused(c.cmd, c.want)
	}
	if _, ok := h.m.Get("x"); ok {
		t.Fatal("a refused TxnStarted left a record")
	}
}

func TestDecided(t *testing.T) {
	conflict := ref("sec-2", 7)
	for _, c := range []struct {
		name        string
		cmd         *seatsafev1.CoordCommand
		decision    seatsafev1.Decision
		outcome     seatsafev1.BookingOutcome
		unavailable []Seat
	}{
		{"commit", commitCmd(50, "t"), decisionCommit, outcomeConfirmed, nil},
		{"abort", abortCmd(50, "t"), decisionAbort, outcomeAborted, nil},
		{"seat unavailable", decidedCmd(50, "t", decisionAbort, outcomeUnavailable, conflict), decisionAbort, outcomeUnavailable, []Seat{{"evt-1", "sec-2", "R01-S007"}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.ok(startedCmd(10, "t", 1, 2))
			res := h.ok(c.cmd)
			got := h.get("t")
			if !res.Won || !reflect.DeepEqual(res.Txn, got) {
				t.Fatalf("result %+v, record %+v", res, got)
			}
			if got.Decision != c.decision || got.Outcome != c.outcome || !reflect.DeepEqual(got.Unavailable, c.unavailable) || got.DecidedMs != 50 || got.StartedMs != 10 {
				t.Fatalf("record %+v", got)
			}
			resp := got.Response(true)
			if resp.OrderId != "t" || resp.Outcome != c.outcome || !resp.Replayed || !resp.CrossShard || len(resp.UnavailableSeats) != len(c.unavailable) {
				t.Fatalf("response %v", resp)
			}
			if len(h.m.Undecided()) != 0 || !reflect.DeepEqual(ids(h.m.Unacked()), []string{"t"}) {
				t.Fatalf("undecided %v, unacked %v", ids(h.m.Undecided()), ids(h.m.Unacked()))
			}
		})
	}
}

// The first decision applied wins: every later one, whatever it says, changes
// nothing and returns the decision that stands.
func TestFirstDecisionWins(t *testing.T) {
	conflict := ref("sec-2", 7)
	decisions := map[string]func(now int64, id string) *seatsafev1.CoordCommand{
		"commit": commitCmd,
		"abort":  abortCmd,
		"unavailable": func(now int64, id string) *seatsafev1.CoordCommand {
			return decidedCmd(now, id, decisionAbort, outcomeUnavailable, conflict)
		},
	}
	for first, mk1 := range decisions {
		for second, mk2 := range decisions {
			t.Run(first+" then "+second, func(t *testing.T) {
				h := newHarness(t)
				h.ok(startedCmd(10, "t", 1, 2))
				won := h.ok(mk1(20, "t"))
				snap, _ := h.m.Snapshot()
				lost := h.ok(mk2(30, "t"))
				if !won.Won || lost.Won {
					t.Fatalf("won %v, later %v", won.Won, lost.Won)
				}
				if !reflect.DeepEqual(lost.Txn, won.Txn) || lost.Txn.DecidedMs != 20 {
					t.Fatalf("later decision returned %+v, want the first %+v", lost.Txn, won.Txn)
				}
				h.m.state.applied--
				after, _ := h.m.Snapshot()
				h.m.state.applied++
				if !bytes.Equal(snap, after) {
					t.Fatalf("a later decision changed the state:\n%s\n%s", snap, after)
				}
			})
		}
	}
}

// GetDecision for a txn the coordinator never saw logs ABORT: the record has
// no participants, and a TxnStarted or COMMIT arriving later loses to it.
func TestDecidedUnknownTxn(t *testing.T) {
	h := newHarness(t)
	res := h.ok(abortCmd(70, "ghost"))
	want := &Txn{ID: "ghost", Decision: decisionAbort, Outcome: outcomeAborted, StartedMs: 70, DecidedMs: 70}
	if !res.Won || !reflect.DeepEqual(res.Txn, want) {
		t.Fatalf("decision for an unknown txn: %+v", res)
	}
	if total, undecided, unacked := h.m.Counts(); total != 1 || undecided != 0 || unacked != 0 {
		t.Fatalf("counts %d %d %d: a record without participants waits for no ack", total, undecided, unacked)
	}
	if res := h.ok(startedCmd(80, "ghost", 1, 2)); res.Created || !reflect.DeepEqual(res.Txn, want) {
		t.Fatalf("TxnStarted after the ABORT: %+v", res)
	}
	if res := h.ok(commitCmd(90, "ghost")); res.Won || res.Txn.Decision != decisionAbort {
		t.Fatalf("COMMIT after the ABORT: %+v", res)
	}
	// Nobody to wait for, so it is collected once it is old enough.
	if res := h.ok(gcCmd(100, 70)); res.Dropped != 0 {
		t.Fatal("dropped a txn decided at the cutoff")
	}
	if res := h.ok(gcCmd(100, 71)); res.Dropped != 1 {
		t.Fatal("a decided txn without participants was not collected")
	}
}

func TestDecidedRefused(t *testing.T) {
	h := newHarness(t)
	h.ok(startedCmd(1, "t", 1, 2))
	seat := ref("sec-1", 1)
	for _, c := range []struct {
		cmd  *seatsafev1.CoordCommand
		want string
	}{
		{commitCmd(2, ""), "empty txn_id"},
		{decidedCmd(2, "t", decisionNone, outcomeAborted), "does not go with"},
		{decidedCmd(2, "t", decisionCommit, outcomeAborted), "does not go with"},
		{decidedCmd(2, "t", decisionCommit, outcomeConfirmed, seat), "does not go with"},
		{decidedCmd(2, "t", decisionAbort, outcomeConfirmed), "does not go with"},
		{decidedCmd(2, "t", decisionAbort, outcomeAborted, seat), "does not go with"},
		{decidedCmd(2, "t", decisionAbort, outcomeUnavailable), "does not go with"},
		{decidedCmd(2, "t", decisionAbort, seatsafev1.BookingOutcome_BOOKING_OUTCOME_INVALID), "does not go with"},
		{decidedCmd(2, "new", decisionAbort, outcomeNone), "does not go with"},
	} {
		h.refused(c.cmd, c.want)
	}
	if h.get("t").Decided() {
		t.Fatal("a refused decision decided the txn")
	}
	if _, ok := h.m.Get("new"); ok {
		t.Fatal("a refused decision created a record")
	}
}

func TestAcked(t *testing.T) {
	h := newHarness(t)
	h.ok(startedCmd(1, "t", 1, 2, 3))
	h.refused(ackedCmd(2, "t", 1), "before any decision")
	h.ok(commitCmd(3, "t"))
	h.refused(ackedCmd(4, "t", 4), "not a participant")
	h.refused(ackedCmd(4, "", 1), "empty txn_id")

	if res := h.ok(ackedCmd(5, "t", 3)); !reflect.DeepEqual(res.Txn.Acked, []uint32{3}) || !reflect.DeepEqual(res.Txn.MissingAcks(), []uint32{1, 2}) {
		t.Fatalf("after shard 3 acked: %+v", res.Txn)
	}
	h.ok(ackedCmd(6, "t", 1))
	if res := h.ok(ackedCmd(7, "t", 1)); !reflect.DeepEqual(res.Txn.Acked, []uint32{1, 3}) {
		t.Fatalf("a repeated ack changed the record: %+v", res.Txn)
	}
	if !reflect.DeepEqual(ids(h.m.Unacked()), []string{"t"}) {
		t.Fatal("txn left the unacked list with shard 2 missing")
	}
	if res := h.ok(ackedCmd(8, "t", 2)); !reflect.DeepEqual(res.Txn.Acked, []uint32{1, 2, 3}) || len(res.Txn.MissingAcks()) != 0 {
		t.Fatalf("after every ack: %+v", res.Txn)
	}
	if len(h.m.Unacked()) != 0 {
		t.Fatal("fully acked txn still listed as unacked")
	}
	// An ack for a txn that was already collected is a no-op.
	if res := h.ok(ackedCmd(9, "gone", 1)); res.Txn != nil {
		t.Fatalf("ack for an unknown txn returned %+v", res.Txn)
	}
	if _, ok := h.m.Get("gone"); ok {
		t.Fatal("an ack created a record")
	}
}

// Gc drops only txns that are decided, old enough and acked by every
// participant. Undecided txns and txns missing an ack stay whatever their age.
func TestGc(t *testing.T) {
	h := newHarness(t)
	h.ok(startedCmd(10, "undecided", 1, 2))
	h.ok(startedCmd(10, "unacked", 1, 2))
	h.ok(commitCmd(20, "unacked"))
	h.ok(startedCmd(10, "half", 1, 2))
	h.ok(abortCmd(20, "half"))
	h.ok(ackedCmd(21, "half", 2))
	h.ok(startedCmd(10, "done", 1, 2))
	h.ok(commitCmd(20, "done"))
	h.ok(ackedCmd(21, "done", 1))
	h.ok(ackedCmd(22, "done", 2))
	h.ok(startedCmd(10, "young", 1, 2))
	h.ok(abortCmd(500, "young"))
	h.ok(ackedCmd(501, "young", 1))
	h.ok(ackedCmd(501, "young", 2))

	if res := h.ok(gcCmd(600, 20)); res.Dropped != 0 {
		t.Fatalf("dropped %d txns decided at the cutoff", res.Dropped)
	}
	if res := h.ok(gcCmd(600, 100)); res.Dropped != 1 {
		t.Fatalf("dropped %d txns, want only the acked old one", res.Dropped)
	}
	for _, id := range []string{"undecided", "unacked", "half", "young"} {
		if _, ok := h.m.Get(id); !ok {
			t.Fatalf("gc dropped %q", id)
		}
	}
	if _, ok := h.m.Get("done"); ok {
		t.Fatal("gc kept the acked, decided, old txn")
	}

	// However old they get, an undecided txn and one missing an ack stay.
	if res := h.ok(gcCmd(1<<50, 1<<50)); res.Dropped != 1 {
		t.Fatalf("dropped %d txns, want only %q", res.Dropped, "young")
	}
	if got := ids(h.m.Undecided()); !reflect.DeepEqual(got, []string{"undecided"}) {
		t.Fatalf("undecided %v", got)
	}
	if got := ids(h.m.Unacked()); !reflect.DeepEqual(got, []string{"half", "unacked"}) {
		t.Fatalf("unacked %v", got)
	}
	// Once the last ack arrives, the txn goes.
	h.ok(ackedCmd(700, "half", 1))
	h.ok(ackedCmd(700, "unacked", 1))
	if res := h.ok(gcCmd(800, 1<<50)); res.Dropped != 1 {
		t.Fatalf("dropped %d txns, want %q", res.Dropped, "half")
	}
	if total, undecided, unacked := h.m.Counts(); total != 2 || undecided != 1 || unacked != 1 {
		t.Fatalf("counts %d %d %d", total, undecided, unacked)
	}
}

// An entry at or below AppliedIndex was already applied (a replay after a
// restart from a snapshot): it must change nothing.
func TestApplyOldIndexIsNoop(t *testing.T) {
	m := New()
	if res := m.Apply(5, startedCmd(1, "t", 1, 2)); !res.Created {
		t.Fatalf("%+v", res)
	}
	for _, idx := range []uint64{1, 5} {
		if res := m.Apply(idx, commitCmd(2, "t")); res.Txn != nil || res.Err != nil || res.Won {
			t.Fatalf("entry %d applied again: %+v", idx, res)
		}
	}
	if tx, _ := m.Get("t"); tx.Decided() || m.AppliedIndex() != 5 {
		t.Fatalf("old entry changed the state: %+v, applied %d", tx, m.AppliedIndex())
	}
}

// Records handed out are copies: changing one must not change the machine.
func TestRecordsAreCopies(t *testing.T) {
	h := newHarness(t)
	h.ok(startedCmd(1, "t", 1, 2))
	h.ok(decidedCmd(2, "t", decisionAbort, outcomeUnavailable, ref("sec-1", 1)))
	h.ok(ackedCmd(3, "t", 1))
	before, _ := h.m.Snapshot()
	for _, tx := range []*Txn{h.get("t"), h.m.Unacked()[0]} {
		tx.Participants[0].Seats[0].Seat = "changed"
		tx.Participants[0].Shard = 9
		tx.Unavailable[0].Seat = "changed"
		tx.Acked[0] = 9
	}
	after, _ := h.m.Snapshot()
	if !bytes.Equal(before, after) {
		t.Fatal("changing a returned record changed the machine")
	}
	if err := h.m.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRestore(t *testing.T) {
	h := newHarness(t)
	h.ok(startedCmd(10, "a", 1, 2))
	h.ok(startedCmd(11, "b", 2, 3))
	h.ok(commitCmd(12, "b"))
	h.ok(ackedCmd(13, "b", 3))
	h.ok(startedCmd(14, "c", 1, 3))
	h.ok(decidedCmd(15, "c", decisionAbort, outcomeUnavailable, ref("sec-3", 1), ref("sec-1", 2)))
	h.ok(ackedCmd(16, "c", 1))
	h.ok(ackedCmd(16, "c", 3))
	h.ok(abortCmd(17, "ghost"))
	snap, err := h.m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := h.m.Snapshot(); !bytes.Equal(snap, again) {
		t.Fatal("two snapshots of one state differ")
	}

	m2 := New()
	m2.Apply(1, startedCmd(1, "old", 1, 2)) // replaced by the restore
	if err := m2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if snap2, _ := m2.Snapshot(); !bytes.Equal(snap, snap2) {
		t.Fatalf("restored machine snapshots differently:\n%s\n%s", snap, snap2)
	}
	if m2.AppliedIndex() != h.m.AppliedIndex() {
		t.Fatalf("applied %d, want %d", m2.AppliedIndex(), h.m.AppliedIndex())
	}
	for _, id := range []string{"a", "b", "c", "ghost"} {
		want, _ := h.m.Get(id)
		got, ok := m2.Get(id)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("txn %q restored as %+v, want %+v", id, got, want)
		}
	}
	if _, ok := m2.Get("old"); ok {
		t.Fatal("restore kept a txn that is not in the snapshot")
	}
	if !reflect.DeepEqual(ids(m2.Undecided()), []string{"a"}) || !reflect.DeepEqual(ids(m2.Unacked()), []string{"b"}) {
		t.Fatalf("indexes after restore: undecided %v, unacked %v", ids(m2.Undecided()), ids(m2.Unacked()))
	}
	// Both machines carry on identically.
	for i, cmd := range []*seatsafev1.CoordCommand{commitCmd(20, "a"), ackedCmd(21, "b", 2), gcCmd(22, 100)} {
		idx := h.m.AppliedIndex() + 1
		r1, r2 := h.m.Apply(idx, cmd), m2.Apply(idx, cmd)
		if !reflect.DeepEqual(r1, r2) {
			t.Fatalf("command %d: results differ: %+v and %+v", i, r1, r2)
		}
	}
	s1, _ := h.m.Snapshot()
	s2, _ := m2.Snapshot()
	if !bytes.Equal(s1, s2) {
		t.Fatal("machines diverged after the restore")
	}
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	txn := func(body string) []byte {
		return append([]byte{snapshotVersion}, []byte(`{"applied_index":3,"txns":[`+body+`]}`)...)
	}
	const part = `"participants":[{"shard":1,"seats":[["e","s","1"]]},{"shard":2,"seats":[["e","s","2"]]}]`
	for name, c := range map[string]struct {
		data []byte
		want string
	}{
		"empty":                 {nil, "empty snapshot"},
		"format":                {[]byte{9, '{', '}'}, "snapshot format 9"},
		"not json":              {[]byte{snapshotVersion, 'x'}, "decode snapshot"},
		"unknown field":         {append([]byte{snapshotVersion}, []byte(`{"applied_index":1,"txns":[],"extra":1}`)...), "decode snapshot"},
		"empty id":              {txn(`{"id":"","started_ms":1,` + part + `}`), "empty or listed twice"},
		"listed twice":          {txn(`{"id":"a","started_ms":1,` + part + `},{"id":"a","started_ms":1,` + part + `}`), "empty or listed twice"},
		"undecided, no shards":  {txn(`{"id":"a","started_ms":1}`), "undecided and has no participants"},
		"undecided with ack":    {txn(`{"id":"a","started_ms":1,"acked":[1],` + part + `}`), "undecided but has"},
		"undecided with time":   {txn(`{"id":"a","started_ms":1,"decided_ms":2,` + part + `}`), "undecided but has"},
		"commit, wrong outcome": {txn(`{"id":"a","started_ms":1,"decision":1,"outcome":3,"decided_ms":2,` + part + `}`), "does not go with"},
		"unsorted shards":       {txn(`{"id":"a","started_ms":1,"participants":[{"shard":2,"seats":[["e","s","1"]]},{"shard":1,"seats":[["e","s","2"]]}]}`), "not sorted"},
		"shard without seats":   {txn(`{"id":"a","started_ms":1,"participants":[{"shard":1,"seats":[]}]}`), "no seats on shard 1"},
		"ack from a stranger":   {txn(`{"id":"a","started_ms":1,"decision":2,"outcome":3,"decided_ms":2,"acked":[7],` + part + `}`), "not a participant"},
		"unsorted acks":         {txn(`{"id":"a","started_ms":1,"decision":2,"outcome":3,"decided_ms":2,"acked":[2,1],` + part + `}`), "acks not sorted"},
	} {
		t.Run(name, func(t *testing.T) {
			m := New()
			m.Apply(1, startedCmd(1, "keep", 1, 2))
			before, _ := m.Snapshot()
			err := m.Restore(c.data)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Restore: %v, want an error containing %q", err, c.want)
			}
			if after, _ := m.Snapshot(); !bytes.Equal(before, after) {
				t.Fatal("a rejected snapshot changed the machine")
			}
		})
	}
}

// CheckInvariants notices records and indexes that Apply could never produce.
func TestCheckInvariantsCatchesCorruption(t *testing.T) {
	for name, c := range map[string]struct {
		corrupt func(s *state)
		want    string
	}{
		"wrong key":             {func(s *state) { s.txns["a"].ID = "b" }, `stored under "a" has id "b"`},
		"decided, still listed": {func(s *state) { s.txns["a"].Decision, s.txns["a"].Outcome = decisionAbort, outcomeAborted }, "undecided index disagree"},
		"missing from unacked":  {func(s *state) { delete(s.unacked, "b") }, "unacked index disagree"},
		"stray undecided":       {func(s *state) { s.undecided["zz"] = s.txns["a"] }, `undecided index lists unknown txn "zz"`},
		"stray unacked":         {func(s *state) { s.unacked["zz"] = s.txns["b"] }, `unacked index lists unknown txn "zz"`},
	} {
		t.Run(name, func(t *testing.T) {
			m := New()
			m.Apply(1, startedCmd(1, "a", 1, 2))
			m.Apply(2, startedCmd(1, "b", 1, 2))
			m.Apply(3, commitCmd(2, "b"))
			if err := m.CheckInvariants(); err != nil {
				t.Fatal(err)
			}
			c.corrupt(m.state)
			if err := m.CheckInvariants(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("CheckInvariants: %v, want an error containing %q", err, c.want)
			}
		})
	}
}

// randomCommand builds one command over a small id space, so commands collide:
// repeated starts, competing decisions, acks before decisions, acks from
// strangers, malformed commands and collections at every age.
func randomCommand(rng *rand.Rand, now int64) *seatsafev1.CoordCommand {
	id := fmt.Sprintf("txn-%03d", rng.IntN(300))
	if rng.IntN(100) == 0 {
		id = ""
	}
	switch n := rng.IntN(100); {
	case n < 30:
		var shards []uint32
		for s := uint32(1); s <= 4; s++ {
			if rng.IntN(2) == 0 {
				shards = append(shards, s)
			}
		}
		if rng.IntN(50) == 0 {
			shards = append(shards, 1) // unsorted: refused
		}
		return startedCmd(now, id, shards...)
	case n < 45:
		return commitCmd(now, id)
	case n < 55:
		return abortCmd(now, id)
	case n < 62:
		return decidedCmd(now, id, decisionAbort, outcomeUnavailable, ref("sec-1", rng.IntN(5)), ref("sec-2", rng.IntN(5)))
	case n < 64:
		return decidedCmd(now, id, seatsafev1.Decision(rng.IntN(3)), seatsafev1.BookingOutcome(rng.IntN(5))) // often malformed
	case n < 94:
		return ackedCmd(now, id, uint32(rng.IntN(6)))
	case n < 99:
		return gcCmd(now, now-int64(rng.IntN(400)))
	}
	return &seatsafev1.CoordCommand{NowMs: now}
}

// Three replicas apply the same 5,000 random commands. After every command
// the results match and the invariants hold; at the end, and at points along
// the way, their snapshots are byte-identical. One replica is rebuilt from a
// snapshot halfway, as a node that restarts or falls behind would be.
func TestRandomReplayIsDeterministic(t *testing.T) {
	const commands = 5000
	seeds := []uint64{1, 2, 3}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 99))
			replicas := []*Machine{New(), New(), New()}
			now := int64(1_000_000)
			var decidedOnce, created, dropped, refused int
			// Model of the two rules that must never break.
			firstDecision := map[string]seatsafev1.Decision{}
			for i := 1; i <= commands; i++ {
				now += int64(rng.IntN(20))
				cmd := randomCommand(rng, now)
				var pending map[string]bool
				if gc := cmd.GetGc(); gc != nil {
					pending = map[string]bool{}
					for _, tx := range replicas[0].Undecided() {
						pending[tx.ID] = true
					}
					for _, tx := range replicas[0].Unacked() {
						pending[tx.ID] = true
					}
				}
				results := make([]Result, len(replicas))
				for r, m := range replicas {
					results[r] = m.Apply(uint64(i), cmd)
					if err := m.CheckInvariants(); err != nil {
						t.Fatalf("replica %d after command %d (%v): %v", r, i, cmd, err)
					}
				}
				for r := 1; r < len(replicas); r++ {
					if !reflect.DeepEqual(results[0], results[r]) {
						t.Fatalf("command %d (%v): replica 0 returned %+v, replica %d %+v", i, cmd, results[0], r, results[r])
					}
				}
				res := results[0]
				switch {
				case res.Err != nil:
					refused++
				case res.Created:
					created++
				case res.Won:
					decidedOnce++
				}
				dropped += res.Dropped
				if d := cmd.GetDecided(); d != nil && res.Err == nil {
					first, seen := firstDecision[d.GetTxnId()]
					if !seen {
						if !res.Won {
							t.Fatalf("command %d: first decision for %q did not win", i, d.GetTxnId())
						}
						firstDecision[d.GetTxnId()] = d.GetDecision()
					} else if res.Won || res.Txn.Decision != first {
						t.Fatalf("command %d: txn %q decided %v, then %v won=%v", i, d.GetTxnId(), first, res.Txn.Decision, res.Won)
					}
				}
				for id := range pending {
					if _, ok := replicas[0].Get(id); !ok {
						t.Fatalf("command %d: gc dropped %q, which was undecided or missing an ack", i, id)
					}
				}
				if cmd.GetGc() != nil {
					for id := range firstDecision {
						if _, ok := replicas[0].Get(id); !ok {
							delete(firstDecision, id) // collected: a later decision starts over
						}
					}
				}
				if i%500 == 0 || i == commands {
					base, err := replicas[0].Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					for r := 1; r < len(replicas); r++ {
						if s, _ := replicas[r].Snapshot(); !bytes.Equal(base, s) {
							t.Fatalf("after command %d: replica %d's snapshot differs from replica 0's", i, r)
						}
					}
					if i == commands/2 {
						fresh := New()
						if err := fresh.Restore(base); err != nil {
							t.Fatal(err)
						}
						replicas[2] = fresh
					}
				}
			}
			total, undecided, unacked := replicas[0].Counts()
			t.Logf("%d created, %d decided, %d dropped by gc, %d refused; %d left (%d undecided, %d missing acks)",
				created, decidedOnce, dropped, refused, total, undecided, unacked)
			if created < 200 || decidedOnce < 200 || dropped < 50 || refused < 50 || undecided == 0 || unacked == 0 {
				t.Fatal("the random commands did not exercise every rule")
			}
		})
	}
}
