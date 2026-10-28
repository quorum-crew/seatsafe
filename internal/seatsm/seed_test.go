package seatsm

import (
	"fmt"
	"testing"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/seed"
)

// seedFile builds a shard seed shaped like cmd/datagen's output: a flash
// event plus regular events, rows R01.. and seats S001..
func seedFile(t testing.TB, chunkSize int) *seed.Loaded {
	t.Helper()
	f := seed.File{FormatVersion: seed.FormatVersion, ShardID: 2, ChunkSize: chunkSize}
	add := func(event string, sections, rows, perRow int) {
		for s := 0; s < sections; s++ {
			for r := 1; r <= rows; r++ {
				for n := 1; n <= perRow; n++ {
					f.Seats = append(f.Seats, seed.Seat{EventID: event, SectionID: fmt.Sprintf("sec-%03d", 102+3*s), SeatID: fmt.Sprintf("R%02d-S%03d", r, n)})
				}
			}
		}
	}
	add("evt-flash", 17, 20, 50) // 17,000 seats
	add("evt-002", 4, 10, 40)    // 1,600 seats
	b, err := seed.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	l, err := seed.Parse(b, 2)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func seedOp(now int64, l *seed.Loaded, version string, chunk int) *seatsafev1.ShardCommand {
	var refs []*seatsafev1.SeatRef
	for _, s := range l.Chunk(chunk) {
		refs = append(refs, ref(s.EventID, s.SectionID, s.SeatID))
	}
	return seedCmd(now, version, uint32(chunk), uint32(l.TotalChunks()), refs...)
}

// seedMachine applies every chunk of l, in order.
func seedMachine(t testing.TB, l *seed.Loaded) *Machine {
	t.Helper()
	m := New()
	for c := 0; c < l.TotalChunks(); c++ {
		if r := m.Apply(m.AppliedIndex()+1, seedOp(1, l, l.Version, c)); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	return m
}

func countAvailable(m *Machine, event string) (sections, seats int) {
	for _, s := range m.ListAvailable(event, "").Sections {
		sections++
		seats += len(s.AvailableSeatIds)
	}
	return
}

func TestSeedWithRealData(t *testing.T) {
	l := seedFile(t, seed.DefaultChunkSize)
	total := l.TotalChunks()
	if total != 19 {
		t.Fatalf("chunks = %d", total)
	}
	m := New()
	idx := uint64(0)
	apply := func(c *seatsafev1.ShardCommand) Result {
		idx++
		return m.Apply(idx, c)
	}
	// Chunks arrive out of order (a new leader resumes from NextMissingChunk).
	for c := total - 1; c >= 0; c-- {
		if r := apply(seedOp(1, l, l.Version, c)); r.Err != nil {
			t.Fatal(r.Err)
		}
		if c == total-5 {
			if r := apply(seedOp(1, l, l.Version, c)); r.Err != nil { // duplicate
				t.Fatalf("duplicate chunk: %v", r.Err)
			}
			if r := apply(seedOp(1, l, "0123abcd", 0)); r.Err == nil { // wrong version
				t.Fatal("wrong-version chunk accepted")
			}
			if r := apply(bookCmd(1, "early", ref("evt-flash", "sec-102", "R01-S001"))); r.Book.Outcome != outcomeInvalid {
				t.Fatalf("book before seeded = %v", r.Book)
			}
			if next, ok := m.NextMissingChunk(); !ok || next != 0 {
				t.Fatalf("next = %d %v", next, ok)
			}
		}
	}
	if v, applied, tot, done := m.SeedStatus(); v != l.Version || applied != uint32(total) || tot != uint32(total) || !done {
		t.Fatalf("status = %s %d/%d %v", v, applied, tot, done)
	}
	if secs, seats := countAvailable(m, "evt-flash"); secs != 17 || seats != 17_000 {
		t.Fatalf("evt-flash: %d sections, %d seats", secs, seats)
	}
	if secs, seats := countAvailable(m, "evt-002"); secs != 4 || seats != 1_600 {
		t.Fatalf("evt-002: %d sections, %d seats", secs, seats)
	}
	got := m.ListAvailable("evt-002", "sec-105").Sections[0].AvailableSeatIds
	if len(got) != 400 || got[0] != "R01-S001" || got[399] != "R10-S040" {
		t.Fatalf("sec-105 seats: %d, first %s, last %s", len(got), got[0], got[len(got)-1])
	}
	if err := m.CheckInvariants(); err != nil {
		t.Fatal(err)
	}

	// The retried early booking now succeeds.
	if r := apply(bookCmd(2, "early", ref("evt-flash", "sec-102", "R01-S001"))); r.Book.Outcome != outcomeConfirmed {
		t.Fatalf("book after seeded = %v", r.Book)
	}
	if _, seats := countAvailable(m, "evt-flash"); seats != 16_999 {
		t.Fatalf("after booking: %d", seats)
	}
	// Seeded in a different order, the same inventory gives the same snapshot.
	m2 := seedMachine(t, l)
	m2.Apply(idx, bookCmd(2, "early", ref("evt-flash", "sec-102", "R01-S001")))
	if string(mustSnapshot(t, m)) != string(mustSnapshot(t, m2)) {
		t.Fatal("seed order changed the snapshot")
	}
}

// Single-seat bookings on a seeded 18,600-seat shard. Each iteration books a
// different free seat; when the shard sells out a fresh copy is restored
// outside the timer.
func BenchmarkApplyBook(b *testing.B) {
	l := seedFile(b, seed.DefaultChunkSize)
	snap := mustSnapshot(b, seedMachine(b, l))
	cmds := make([]*seatsafev1.ShardCommand, len(l.Seats))
	for i, s := range l.Seats {
		cmds[i] = bookCmd(int64(i), fmt.Sprintf("req-%08d", i), ref(s.EventID, s.SectionID, s.SeatID))
	}
	m := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(cmds)
		if j == 0 {
			b.StopTimer()
			if err := m.Restore(snap); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		if r := m.Apply(m.AppliedIndex()+1, cmds[j]); r.Book.Outcome != outcomeConfirmed {
			b.Fatalf("booking %d: %v", i, r.Book)
		}
	}
}
