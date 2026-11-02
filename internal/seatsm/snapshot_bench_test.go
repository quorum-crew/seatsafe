package seatsm

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// stateShape describes a shard state for measuring snapshots. Sections are
// 20 rows x 50 seats (flash) or 10 rows x 40 seats (regular), as datagen makes.
type stateShape struct {
	name          string
	flashSections int
	regularEvents int // each with 4 sections on this shard
	bookAll       bool
	reqs          int // total req/ records to end with; the extra ones are refused
}

// The first shapes match a real shard of the default dataset (23,400 seats).
// The last one is 1/3 of a 1M-seat dataset.
var stateShapes = []stateShape{
	{name: "seed-only", flashSections: 17, regularEvents: 4},
	{name: "all-booked", flashSections: 17, regularEvents: 4, bookAll: true},
	{name: "booked+50k-reqs", flashSections: 17, regularEvents: 4, bookAll: true, reqs: 50000},
	{name: "booked+100k-reqs", flashSections: 17, regularEvents: 4, bookAll: true, reqs: 100000},
	{name: "333k-seats+200k-reqs", flashSections: 333, bookAll: true, reqs: 200000},
}

// stateBuilder applies real commands, so req/ records and seats are what
// Apply stores: request IDs are random UUIDs, orders take 1 to 4 adjacent seats
// of a section, and applied-at times span the 10 minute retention window.
type stateBuilder struct {
	m     *Machine
	r     *rand.Rand
	index uint64
	now   int64
	step  int64
	reqs  int
}

func (b *stateBuilder) apply(cmd *seatsafev1.ShardCommand) Result {
	b.index++
	cmd.NowMs = b.now
	return b.m.Apply(b.index, cmd)
}

func (b *stateBuilder) uuid() string {
	var raw [16]byte
	for i := 0; i < 16; i += 8 {
		v := b.r.Uint64()
		for j := range 8 {
			raw[i+j] = byte(v >> (8 * j))
		}
	}
	raw[6], raw[8] = raw[6]&0x0f|0x40, raw[8]&0x3f|0x80
	h := hex.EncodeToString(raw[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func (b *stateBuilder) book(seats []*seatsafev1.SeatRef) Result {
	b.reqs++
	b.now += b.step
	return b.apply(&seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Book{Book: &seatsafev1.BookOp{
		RequestId: b.uuid(), UserId: fmt.Sprintf("user-%d", b.r.IntN(1_000_000)), Seats: seats}}})
}

// buildState makes a machine in the given state, identically on every call.
func buildState(t testing.TB, s stateShape) *Machine {
	t.Helper()
	b := &stateBuilder{m: New(), r: rand.New(rand.NewPCG(512, uint64(len(s.name)))), now: 1_790_000_000_000}
	if s.reqs > 0 {
		b.step = 600_000 / int64(s.reqs)
	} else {
		b.step = 10
	}

	// Each section is one contiguous run of seats.
	var sections [][]*seatsafev1.SeatRef
	add := func(event string, n, rows, perRow int) {
		for i := range n {
			var seats []*seatsafev1.SeatRef
			for row := 1; row <= rows; row++ {
				for seat := 1; seat <= perRow; seat++ {
					seats = append(seats, ref(event, fmt.Sprintf("sec-%03d", 101+i), fmt.Sprintf("R%02d-S%03d", row, seat)))
				}
			}
			sections = append(sections, seats)
		}
	}
	add("evt-flash", s.flashSections, 20, 50)
	for e := range s.regularEvents {
		add(fmt.Sprintf("evt-%03d", e+1), 4, 10, 40)
	}
	for i, seats := range sections {
		cmd := seedCmd(0, "seed-version", uint32(i), uint32(len(sections)), seats...)
		if r := b.apply(cmd); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if !s.bookAll {
		return b.m
	}

	b.r.Shuffle(len(sections), func(i, j int) { sections[i], sections[j] = sections[j], sections[i] })
	for _, seats := range sections {
		for len(seats) > 0 {
			n := min(1+b.r.IntN(4), len(seats))
			if r := b.book(seats[:n]); r.Book.Outcome != outcomeConfirmed {
				t.Fatalf("booking: %v", r.Book)
			}
			seats = seats[n:]
		}
	}
	for b.reqs < s.reqs {
		seats := sections[b.r.IntN(len(sections))]
		n := 1 + b.r.IntN(4)
		at := b.r.IntN(len(seats) - n + 1)
		if r := b.book(seats[at : at+n]); r.Book.Outcome != outcomeUnavailable {
			t.Fatalf("refused booking: %v", r.Book)
		}
	}
	return b.m
}

// BenchmarkSnapshotStates reports size, encode and decode time per state and
// format. Run with: go test -run '^$' -bench SnapshotStates -benchtime 5x ./internal/seatsm/
func BenchmarkSnapshotStates(b *testing.B) {
	formats := []struct {
		name   string
		encode func(*Machine) ([]byte, error)
	}{
		{"v1", snapshotV1},
		{"current", (*Machine).Snapshot},
	}
	for _, s := range stateShapes {
		m := buildState(b, s)
		for _, f := range formats {
			data, err := f.encode(m)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(s.name+"/"+f.name+"/snapshot", func(b *testing.B) {
				for b.Loop() {
					if _, err := f.encode(m); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(data))/(1<<20), "MiB")
			})
			b.Run(s.name+"/"+f.name+"/restore", func(b *testing.B) {
				for b.Loop() {
					if err := New().Restore(data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// BenchmarkStateMemory reports what the machine itself holds in memory.
func BenchmarkStateMemory(b *testing.B) {
	for b.Loop() {
		base := heapInUse()
		seeded := buildState(b, stateShapes[0])
		afterSeed := heapInUse()
		booked := buildState(b, stateShapes[1])
		afterBooked := heapInUse()
		reqs50k := buildState(b, stateShapes[2])
		after50k := heapInUse()
		reqs100k := buildState(b, stateShapes[3])
		after100k := heapInUse()
		seats := float64(len(seeded.sectionsSeats()))
		b.ReportMetric(float64(afterSeed-base)/seats, "bytes/seat-free")
		b.ReportMetric(float64(afterBooked-afterSeed)/seats, "bytes/seat-booked")
		b.ReportMetric(float64((after100k-after50k)-(after50k-afterBooked))/50000, "bytes/req")
		b.ReportMetric(float64(len(booked.reqs)+len(reqs50k.reqs)+len(reqs100k.reqs)), "reqs-held")
	}
}

func (m *Machine) sectionsSeats() []*seat {
	var out []*seat
	for _, sec := range m.sections {
		out = append(out, sec.sorted...)
	}
	return out
}
