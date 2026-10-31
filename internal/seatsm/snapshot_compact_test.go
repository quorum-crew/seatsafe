package seatsm

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// machineAfter replays the first n commands of a random log.
func machineAfter(cmds []*seatsafev1.ShardCommand, n int) *Machine {
	m := New()
	for i, c := range cmds[:n] {
		m.Apply(uint64(i+1), c)
	}
	return m
}

// wantSameState compares two machines through format 1, which spells out every
// field, so it can't share a bug with the compact code.
func wantSameState(t *testing.T, got, want *Machine) {
	t.Helper()
	if a, b := mustSnapshotV1(t, got), mustSnapshotV1(t, want); !bytes.Equal(a, b) {
		t.Fatalf("states differ:\n got %.2000s\nwant %.2000s", a, b)
	}
	if err := got.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func restored(t testing.TB, snap []byte) *Machine {
	t.Helper()
	m := New()
	if err := m.Restore(snap); err != nil {
		t.Fatal(err)
	}
	return m
}

func wantRoundTrip(t *testing.T, m *Machine) {
	t.Helper()
	snap := mustSnapshot(t, m)
	if snap[0] != compactVersion {
		t.Fatalf("snapshot format %d", snap[0])
	}
	r := restored(t, snap)
	wantSameState(t, r, m)
	if !bytes.Equal(mustSnapshot(t, r), snap) {
		t.Fatal("snapshot of the restored machine differs")
	}
}

func TestCompactRoundTrip(t *testing.T) {
	for _, shape := range []logShape{smallLog, contendedLog} {
		cmds := randomLog(shape)
		for _, n := range []int{0, 1, 3, len(cmds) / 10, len(cmds) / 2, len(cmds)} {
			wantRoundTrip(t, machineAfter(cmds, n))
		}
	}
	for _, s := range stateShapes[:3] {
		wantRoundTrip(t, buildState(t, s))
	}
}

// Random states, including GC at random points, so seats outlive the records
// of the requests that booked them.
func TestCompactRandomStates(t *testing.T) {
	for seed := range int64(60) {
		r := rand.New(rand.NewSource(seed))
		shape := logShape{seed: seed, n: 200 + r.Intn(800), sections: 1 + r.Intn(4), seatsPerSection: 5 + r.Intn(60), hotSeats: r.Intn(6), settleRecent: seed%2 == 0}
		m := New()
		index := uint64(0)
		apply := func(c *seatsafev1.ShardCommand) {
			index++
			m.Apply(index, c)
		}
		for _, c := range randomLog(shape) {
			apply(c)
			if r.Intn(150) == 0 {
				apply(&seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Gc{Gc: &seatsafev1.GcOp{DropBeforeMs: c.GetNowMs() - int64(r.Intn(50))}}})
			}
		}
		wantRoundTrip(t, m)
	}
}

// A seat keeps its owner after GC dropped the owner's req/ record.
func TestCompactKeepsOwnerAfterGC(t *testing.T) {
	h := newHarness(t, true)
	h.apply(bookCmd(1, "r1", a1))
	h.apply(&seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Gc{Gc: &seatsafev1.GcOp{DropBeforeMs: 100}}})
	if len(h.m.reqs) != 0 {
		t.Fatal("req not collected")
	}
	wantRoundTrip(t, h.m)
	if st := restored(t, h.snap()).lookup(keyOf(a1)); st.owner != "r1" || st.user != "u-r1" {
		t.Fatalf("seat after restore: %+v", st)
	}
}

const uuidA, uuidB = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d", "ffffffff-0000-4000-8000-000000000000"

// v1Snapshot is format 1 written out by hand: it has what Apply makes plus what
// only a hand-made snapshot can have, like a reference to a seat that doesn't
// exist. Restore must carry all of it through format 2 unchanged.
var v1Snapshot = []byte("\x01" + `{"applied_index":42,"seed":{"version":"v1","total_chunks":3,"chunks":[0,2]},` +
	`"sections":[{"event":"e1","section":"s1","seats":[` +
	`{"id":"a","st":3,"owner":"` + uuidA + `","user":"u1"},{"id":"b","st":3,"owner":"` + uuidA + `","user":"u1"},` +
	`{"id":"c","st":3,"owner":"` + uuidA + `","user":"u2"},{"id":"d","st":2,"owner":"t1","user":"u3"},{"id":"e","st":1},{"id":"f","st":1,"user":"left-over"}]},` +
	`{"event":"e2","section":"s1","seats":[{"id":"a","st":3,"owner":"gone-from-req-table","user":"u1"}]}],` +
	`"reqs":[` +
	`{"id":"0A1B2C3D-4E5F-4A6B-8C7D-9E0F1A2B3C4D","applied_at":9007199254740993,"order_id":"","outcome":-7},` +
	`{"id":"` + uuidA + `","applied_at":1000,"order_id":"` + uuidA + `","outcome":1},` +
	`{"id":"` + uuidB + `","applied_at":-5,"order_id":"another-order","outcome":2,"unavailable":[["e1","s1","c"],["e1","s1","a"],["e9","ghost","x"]],"cross_shard":true}],` +
	`"txns":[` +
	`{"id":"t1","status":1,"user":"u3","seats":[["e1","s1","d"]],"hold_expires_at_ms":5000,"vote":1},` +
	`{"id":"t2","status":2,"seats":[["e2","s1","a"]],"vote":1,"decided_at":6000},` +
	`{"id":"t3","status":3,"user":"u9","seats":[["e1","s1","a"],["e9","ghost","x"]],"hold_expires_at_ms":-1,"vote":2,"conflicts":[["e1","s1","b"],["e1","s1","a"]],"decided_at":7000},` +
	`{"id":"t4","status":3}]}`)

func TestRestoreFormat1(t *testing.T) {
	m := restored(t, v1Snapshot)
	if got := mustSnapshotV1(t, m); !bytes.Equal(got, v1Snapshot) {
		t.Fatalf("format 1 did not survive:\n got %s\nwant %s", got, v1Snapshot)
	}
	wantRoundTrip(t, m)
	if got := mustSnapshotV1(t, restored(t, mustSnapshot(t, m))); !bytes.Equal(got, v1Snapshot) {
		t.Fatalf("format 2 changed it:\n got %s\nwant %s", got, v1Snapshot)
	}

	// Real states in format 1 give the same state as before.
	for _, shape := range []logShape{smallLog, contendedLog} {
		want := machineAfter(randomLog(shape), shape.n)
		got := restored(t, mustSnapshotV1(t, want))
		wantSameState(t, got, want)
		if !bytes.Equal(mustSnapshot(t, got), mustSnapshot(t, want)) {
			t.Fatal("format 2 of the restored state differs")
		}
	}
}

// Replaying every remembered request and txn after a restore must answer with
// exactly the bytes the original machine answers with.
func TestReplayAfterRestoreIsByteIdentical(t *testing.T) {
	marshal := func(m proto.Message) []byte {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, shape := range []logShape{smallLog, contendedLog} {
		want := machineAfter(randomLog(shape), shape.n)
		for name, snap := range map[string][]byte{"format 2": mustSnapshot(t, want), "format 1": mustSnapshotV1(t, want)} {
			got := restored(t, snap)
			next := uint64(shape.n)
			for id := range want.reqs {
				next++
				cmd := bookCmd(1, id, ref("evt-a", "sec-1", "R00"))
				a, b := want.Apply(next, cmd), got.Apply(next, cmd)
				if !a.Book.Replayed || !bytes.Equal(marshal(a.Book), marshal(b.Book)) {
					t.Fatalf("%s: replay of %s:\n got %v\nwant %v", name, id, b.Book, a.Book)
				}
			}
			for id := range want.txns {
				next++
				cmd := prepCmd(1, id, 9, ref("evt-a", "sec-1", "R00"))
				a, b := want.Apply(next, cmd), got.Apply(next, cmd)
				if !bytes.Equal(marshal(a.Prepare), marshal(b.Prepare)) {
					t.Fatalf("%s: prepare replay of %s:\n got %v\nwant %v", name, id, b.Prepare, a.Prepare)
				}
			}
			want = machineAfter(randomLog(shape), shape.n)
		}
	}
}

func TestParseUUID(t *testing.T) {
	raw, ok := parseUUID(uuidA)
	if !ok || formatUUID(raw[:]) != uuidA {
		t.Fatalf("%q: %x %v", uuidA, raw, ok)
	}
	for _, s := range []string{
		"", "req-1", strings.ToUpper(uuidA), uuidA + "0", uuidA[:35],
		"0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4g", "0a1b2c3dx4e5f-4a6b-8c7d-9e0f1a2b3c4d", "0a1b2c3d4e5f4a6b8c7d9e0f1a2b3c4d----",
	} {
		if _, ok := parseUUID(s); ok {
			t.Errorf("%q parsed as a UUID", s)
		}
	}
}

// A body builder for crafting broken snapshots.
type body struct{ w writer }

func (b *body) u(vs ...uint64) *body {
	for _, v := range vs {
		b.w.uvarint(v)
	}
	return b
}

func (b *body) s(ss ...string) *body {
	for _, s := range ss {
		b.w.str(s)
	}
	return b
}

func (b *body) raw(p ...byte) *body {
	b.w.b = append(b.w.b, p...)
	return b
}

func (b *body) v(vs ...int64) *body {
	for _, v := range vs {
		b.w.varint(v)
	}
	return b
}

// head writes everything before the sections: applied, a one-chunk seed, then
// one event "e", the given seat IDs and users, and one non-UUID id per ids.
func head(seats, users, ids []string) *body {
	b := &body{}
	b.u(7).s("v").u(1, 1, 0)
	b.u(1).s("e")
	b.u(uint64(len(seats))).s(seats...)
	b.u(uint64(len(users))).s(users...)
	b.u(0)
	b.u(uint64(len(ids))).s(ids...)
	return b.u(0)
}

func TestRestoreRejectsBadCompact(t *testing.T) {
	seats, users, ids := []string{"s1", "s2"}, []string{"u1"}, []string{"r1", "r2"}
	h := func() *body { return head(seats, users, ids) }
	const free, booked = 1, 3
	full := booked | seatHasOwner | seatHasUser
	// A valid snapshot: seat s1 free, seat s2 booked by r1, no records.
	valid := func() *body { return h().u(1).s("s").u(2).u(0).raw(free).u(0).raw(full).u(0, 0) }
	if err := New().Restore(compress(valid().v(0).u(0, 0).w.b)); err != nil {
		t.Fatalf("the base case must be valid: %v", err)
	}

	cases := map[string]*body{
		"trailing bytes":         valid().v(0).u(0, 0, 0),
		"empty":                  {},
		"bad number":             (&body{}).raw(0xff),
		"huge seed":              (&body{}).u(1).s("v").u(1 << 33),
		"chunks do not fit":      (&body{}).u(1).s("v").u(1, 1<<40),
		"chunk out of range":     (&body{}).u(1).s("v").u(1, 1, 5),
		"seed without version":   (&body{}).u(1).s("").u(1, 0),
		"empty event":            (&body{}).u(1).s("v").u(1, 0).u(1).s(""),
		"string overruns":        (&body{}).u(1).s("v").u(1, 0).u(1).u(50).raw('x', 'y', 'z'),
		"truncated uuid table":   (&body{}).u(1).s("v").u(1, 0).u(0, 0, 0).u(1).raw(1, 2, 3),
		"empty id":               (&body{}).u(7).s("v").u(1, 1, 0).u(1).s("e").u(0, 0, 0, 1).s(""),
		"empty section":          h().u(1).s("s").u(0),
		"empty section name":     h().u(1).s("").u(2),
		"empty seat list":        h().u(1).s("s").u(0).raw(free),
		"seat id out of range":   h().u(1).s("s").u(1).u(5).raw(free),
		"seat gap out of range":  h().u(1).s("s").u(2).u(0).raw(free).u(5).raw(free),
		"seats not sorted":       h().u(1).s("s").u(2).u(1).raw(free).u(0).raw(free),
		"missing seat flags":     h().u(1).s("s").u(2).u(0),
		"owner out of range":     h().u(1).s("s").u(1).u(0).raw(full).u(9, 0),
		"user out of range":      h().u(1).s("s").u(1).u(0).raw(full).u(0, 9),
		"owner with no ids":      head(seats, users, nil).u(1).s("s").u(1).u(0).raw(booked | seatHasOwner | seatSameAs),
		"user with no users":     head(seats, nil, ids).u(1).s("s").u(1).u(0).raw(booked | seatHasUser | seatSameAs),
		"section listed twice":   h().u(2).s("s").u(1).u(0).raw(free).s("s").u(1).u(0).raw(free),
		"req id out of range":    valid().v(0).u(1).u(9),
		"req past the last id":   valid().v(0).u(1).u(2),
		"duplicate req":          valid().v(0).u(2).u(0, 0, 0, 0).u(0, 0, 0, 0),
		"req seat out of range":  valid().v(0).u(1).u(0, 8, 1, 1).v(9),
		"req seat negative":      valid().v(0).u(1).u(0, 8, 1, 1).v(-1),
		"req order id overruns":  valid().v(0).u(1).u(0, 2).u(100).raw('x', 'y'),
		"txn id out of range":    valid().v(0).u(0).u(1).u(9),
		"duplicate txn":          valid().v(0).u(0).u(2).u(0, 3, 0, 0).v(0).u(0).u(0, 3, 0, 0).v(0).u(0),
		"txn past the last id":   valid().v(0).u(0).u(1).u(2),
		"txn status zero":        valid().v(0).u(0).u(1).u(0, 0),
		"txn user out of range":  valid().v(0).u(0).u(1).u(0, 3, 5),
		"invariant: bad state":   h().u(1).s("s").u(1).u(0).raw(9),
		"invariant: no owner":    h().u(1).s("s").u(1).u(0).raw(booked).v(0).u(0, 0),
		"invariant: held no txn": h().u(1).s("s").u(1).u(0).raw(2|seatHasOwner).u(0).v(0).u(0, 0),
	}
	before := mustSnapshot(t, New())
	for name, b := range cases {
		m := New()
		if err := m.Restore(compress(b.w.b)); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if !bytes.Equal(mustSnapshot(t, m), before) {
			t.Errorf("%s: failed Restore changed the machine", name)
		}
	}

	bad := map[string][]byte{
		"not snappy":     {compactVersion, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		"bad block":      {compactVersion, 40, 0xff, 0xff},
		"huge expansion": append([]byte{compactVersion}, binary.AppendUvarint(nil, 1<<30)...),
	}
	for name, data := range bad {
		if err := New().Restore(data); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Bad input must come back as an error or a valid state, never a panic.
func FuzzRestore(f *testing.F) {
	m := machineAfter(randomLog(smallLog), 800)
	f.Add(mustSnapshot(f, m))
	f.Add(v1Snapshot)
	f.Add(mustSnapshot(f, restored(f, v1Snapshot)))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := New()
		if err := r.Restore(data); err != nil {
			return
		}
		if err := r.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Snapshot(); err != nil {
			t.Fatal(err)
		}
	})
}

// Every truncation and many corruptions of a real body stay safe, as the fuzz
// target asserts. This runs on every go test.
func TestRestoreSurvivesDamage(t *testing.T) {
	body := encodeCompact(restored(t, v1Snapshot).state)
	check := func(b []byte) {
		t.Helper()
		m := New()
		if err := m.Restore(compress(b)); err != nil {
			return
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	for n := range body {
		check(body[:n])
	}
	if err := New().Restore(compress(body[:len(body)-1])); err == nil {
		t.Error("a truncated body was accepted")
	}
	r := rand.New(rand.NewSource(11))
	for range 20000 {
		b := bytes.Clone(body)
		for range 1 + r.Intn(3) {
			b[r.Intn(len(b))] = byte(r.Intn(256))
		}
		check(b)
	}
}
