package seatsm

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"google.golang.org/protobuf/proto"
)

const (
	logSeed   = 512
	logLength = 5000
)

// randomLog builds a reproducible mix of every command kind over a small
// universe (2 events x 3 sections x 60 seats, seeded in 6 chunks), so
// request_ids, txn_ids and seats collide often.
func randomLog(seed int64, n int) []*seatsafev1.ShardCommand {
	r := rand.New(rand.NewSource(seed))
	events := []string{"evt-a", "evt-b"}
	sections := []string{"sec-1", "sec-2", "sec-3"}
	const seatsPerSection = 60

	var chunks [][]*seatsafev1.SeatRef
	for _, e := range events {
		for _, s := range sections {
			var c []*seatsafev1.SeatRef
			for i := 0; i < seatsPerSection; i++ {
				c = append(c, ref(e, s, fmt.Sprintf("R%02d", i)))
			}
			chunks = append(chunks, c)
		}
	}
	total := uint32(len(chunks))

	pickSeats := func() []*seatsafev1.SeatRef {
		e := events[r.Intn(len(events))]
		var out []*seatsafev1.SeatRef
		for k := 1 + r.Intn(3); k > 0; k-- {
			out = append(out, ref(e, sections[r.Intn(len(sections))], fmt.Sprintf("R%02d", r.Intn(seatsPerSection))))
		}
		switch r.Intn(25) {
		case 0:
			out = append(out, ref(e, "sec-1", "missing"))
		case 1:
			out = append(out, ref(events[1-indexOf(events, e)], "sec-1", "R00"))
		case 2:
			out = nil
		}
		return out
	}

	now := int64(1_000_000)
	var cmds []*seatsafev1.ShardCommand
	for i := 0; i < n; i++ {
		now += int64(r.Intn(40))
		var c *seatsafev1.ShardCommand
		switch p := r.Intn(100); {
		case p < 4:
			ch := uint32(r.Intn(int(total)))
			version := "v1"
			if r.Intn(5) == 0 {
				version = "v2"
			}
			c = seedCmd(now, version, ch, total, chunks[ch]...)
		case p < 44:
			c = bookCmd(now, fmt.Sprintf("req-%d", r.Intn(i/3+20)), pickSeats()...)
		case p < 68:
			c = prepCmd(now, fmt.Sprintf("txn-%d", r.Intn(i/6+20)), now+int64(r.Intn(500)), pickSeats()...)
		case p < 82:
			c = commitCmd(now, fmt.Sprintf("txn-%d", r.Intn(i/6+20)))
		case p < 96:
			c = abortCmd(now, fmt.Sprintf("txn-%d", r.Intn(i/6+20)))
		default:
			c = gcCmd(now, now-int64(r.Intn(3000)))
		}
		cmds = append(cmds, c)
	}
	return cmds
}

func indexOf(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func resultsEqual(a, b Result) bool {
	if (a.Err == nil) != (b.Err == nil) || (a.Err != nil && a.Err.Error() != b.Err.Error()) {
		return false
	}
	return proto.Equal(a.Book, b.Book) && proto.Equal(a.Prepare, b.Prepare)
}

func mustSnapshot(t testing.TB, m *Machine) []byte {
	t.Helper()
	b, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReplayDeterminism(t *testing.T) {
	cmds := randomLog(logSeed, logLength)
	machines := []*Machine{New(), New(), New()}
	counts := map[string]int{}
	for i, c := range cmds {
		var first Result
		for j, m := range machines {
			// Each replica gets its own copy, as it would after unmarshalling.
			res := m.Apply(uint64(i+1), proto.Clone(c).(*seatsafev1.ShardCommand))
			if j == 0 {
				first = res
			} else if !resultsEqual(first, res) {
				t.Fatalf("command %d: replica %d result %+v, replica 0 %+v", i, j, res, first)
			}
		}
		counts[resultKind(first)]++
	}
	want := mustSnapshot(t, machines[0])
	for j, m := range machines[1:] {
		if got := mustSnapshot(t, m); !bytes.Equal(got, want) {
			t.Fatalf("replica %d snapshot differs", j+1)
		}
	}
	// The log must exercise every path, or this test proves little.
	for _, k := range []string{"CONFIRMED", "SEAT_UNAVAILABLE", "INVALID", "replayed", "YES", "NO", "err", "ok"} {
		if counts[k] == 0 {
			t.Errorf("random log never produced %s: %v", k, counts)
		}
	}
	t.Logf("result mix: %v", counts)
}

func resultKind(r Result) string {
	switch {
	case r.Err != nil:
		return "err"
	case r.Book != nil && r.Book.Replayed:
		return "replayed"
	case r.Book != nil:
		return r.Book.Outcome.String()[len("BOOKING_OUTCOME_"):]
	case r.Prepare != nil:
		return r.Prepare.Vote.String()[len("VOTE_"):]
	}
	return "ok"
}

// No seat is ever sold twice: after every command the invariants hold, and a
// seat once BOOKED by an owner stays BOOKED by that owner.
func TestNoDoubleBooking(t *testing.T) {
	m := New()
	bookedBy := map[seatKey]string{}
	for i, c := range randomLog(logSeed, logLength) {
		res := m.Apply(uint64(i+1), c)
		if err := m.CheckInvariants(); err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		// A replay answers for the original request's seats, not this retry's.
		if res.Book != nil && res.Book.Outcome == outcomeConfirmed && !res.Book.Replayed {
			for _, s := range c.GetBook().GetSeats() {
				if st := m.lookup(keyOf(s)); st.state != booked || st.owner != res.Book.OrderId {
					t.Fatalf("command %d: CONFIRMED seat %v is %v by %q", i, s, st.state, st.owner)
				}
			}
		}
		for sk, sec := range m.sections {
			for _, st := range sec.sorted {
				k := seatKey{sk.event, sk.section, st.id}
				prev, was := bookedBy[k]
				if was && (st.state != booked || st.owner != prev) {
					t.Fatalf("command %d: seat %s booked by %q is now %v by %q", i, k, prev, st.state, st.owner)
				}
				if st.state == booked {
					bookedBy[k] = st.owner
				}
			}
		}
	}
	if len(bookedBy) == 0 {
		t.Fatal("random log booked nothing")
	}
}

func TestSnapshotRestoreMidLog(t *testing.T) {
	cmds := randomLog(logSeed, logLength)
	const cut = 2500
	a := New()
	for i, c := range cmds[:cut] {
		a.Apply(uint64(i+1), c)
	}
	snap := mustSnapshot(t, a)

	b := New()
	b.Apply(1, seedCmd(1, "other", 0, 1, a1)) // Restore must discard this.
	if err := b.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if b.AppliedIndex() != cut {
		t.Fatalf("restored applied index = %d", b.AppliedIndex())
	}
	if got := mustSnapshot(t, b); !bytes.Equal(got, snap) {
		t.Fatal("snapshot of restored machine differs")
	}
	for i, c := range cmds[cut:] {
		idx := uint64(cut + i + 1)
		ra, rb := a.Apply(idx, c), b.Apply(idx, c)
		if !resultsEqual(ra, rb) {
			t.Fatalf("command %d: %+v vs %+v", idx, ra, rb)
		}
	}
	if !bytes.Equal(mustSnapshot(t, a), mustSnapshot(t, b)) {
		t.Fatal("final snapshots differ")
	}
}

// The same state reached through a different history encodes identically.
func TestSnapshotCanonical(t *testing.T) {
	build := func(order []int) []byte {
		h := newHarness(t, false)
		ch := seedChunks()
		for _, i := range order {
			h.applyAll(ch[i])
		}
		h.applyAll(bookCmd(5, "r2", b1), bookCmd(5, "r1", a1), prepCmd(5, "t2", 9, c2), prepCmd(5, "t1", 9, c1), abortCmd(5, "t9"))
		return h.snap()
	}
	if x, y := build([]int{0, 1}), build([]int{1, 0}); !bytes.Equal(x, y) {
		t.Fatalf("snapshots differ:\n%s\n%s", x, y)
	}
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	h := newHarness(t, true)
	h.applyAll(prepCmd(1, "t1", 9, a1))
	good := h.snap()
	body := string(good[1:])

	replace := func(old, new string) []byte {
		if !bytes.Contains(good, []byte(old)) {
			t.Fatalf("snapshot has no %q: %s", old, good)
		}
		return append([]byte{snapshotVersion}, bytes.Replace([]byte(body), []byte(old), []byte(new), 1)...)
	}
	cases := map[string][]byte{
		"empty":              nil,
		"wrong version":      append([]byte{9}, good[1:]...),
		"not json":           {snapshotVersion, '{'},
		"unknown field":      replace(`"applied_index"`, `"bogus":1,"applied_index"`),
		"chunk out of range": replace(`"chunks":[0,1]`, `"chunks":[0,5]`),
		"version no total":   replace(`"total_chunks":2`, `"total_chunks":0`),
		"duplicate seat":     replace(`{"id":"s2","st":1}`, `{"id":"s3","st":1}`),
		"empty seat id":      replace(`{"id":"s2","st":1}`, `{"id":"","st":1}`),
		"bad txn status":     replace(`"status":1`, `"status":7`),
		"duplicate txn":      replace(`"txns":[`, `"txns":[{"id":"t1","status":3},`),
		"invariant broken":   replace(`"id":"s1","st":2`, `"id":"s1","st":1`),
	}
	h.applyAll(bookCmd(2, "r1", a2))
	good2 := h.snap()
	cases["duplicate req"] = bytes.Replace(good2, []byte(`"reqs":[`), []byte(`"reqs":[{"id":"r1","applied_at":1,"order_id":"r1","outcome":1},`), 1)

	for name, data := range cases {
		m := New()
		m.Apply(7, seedCmd(1, "keep", 0, 1, a1))
		before := mustSnapshot(t, m)
		if err := m.Restore(data); err == nil {
			t.Errorf("%s: Restore accepted a bad snapshot", name)
		}
		if !bytes.Equal(mustSnapshot(t, m), before) {
			t.Errorf("%s: failed Restore changed the machine", name)
		}
	}
	if err := New().Restore(good2); err != nil {
		t.Fatalf("good snapshot rejected: %v", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	m := New()
	cmds := randomLog(logSeed, logLength)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				m.ListAvailable("evt-a", "")
				m.ListAvailable("evt-b", "sec-2")
				m.SeedStatus()
				m.NextMissingChunk()
				m.ExpiredHolds(int64(g) * 1_000_000)
				m.AppliedIndex()
			}
		}(g)
	}
	for i, c := range cmds {
		m.Apply(uint64(i+1), c)
		if i%500 == 0 {
			if _, err := m.Snapshot(); err != nil {
				t.Error(err)
			}
		}
	}
	close(done)
	wg.Wait()
}
