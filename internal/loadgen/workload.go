package loadgen

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"

	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// Seat identifies one seat.
type Seat struct{ Event, Section, ID string }

func (s Seat) String() string { return s.Event + "/" + s.Section + "/" + s.ID }

// Section is one (event, section) of the dataset.
type Section struct{ Event, ID string }

// Row is a row of a section; its seats are adjacent, in order.
type Row struct {
	Event, Section string
	Shard          uint32
	Seats          []string
}

// Universe is every real seat, from the shard map and the seed files.
type Universe struct {
	Rows     []*Row
	Sections []Section
	Seats    int
}

// LoadUniverse reads deploy/shardmap.json and deploy/seed/shard*.json.
func LoadUniverse(mapPath, seedDir string) (*Universe, error) {
	sm, err := shardmap.Load(mapPath)
	if err != nil {
		return nil, err
	}
	files := map[uint32]*seed.Loaded{}
	for _, id := range sm.ShardIDs() {
		if files[id], err = seed.Load(seed.Path(seedDir, id), id); err != nil {
			return nil, err
		}
	}
	return NewUniverse(sm, files)
}

// NewUniverse groups seats into rows. A seat ID is "<row>-<seat>", as datagen
// writes them.
func NewUniverse(sm *shardmap.Map, files map[uint32]*seed.Loaded) (*Universe, error) {
	u := &Universe{}
	type rowKey struct{ event, section, row string }
	rows := map[rowKey]*Row{}
	sections := map[Section]bool{}
	for _, id := range sm.ShardIDs() {
		l := files[id]
		if l == nil {
			continue
		}
		for _, s := range l.Seats {
			if owner, ok := sm.Lookup(s.EventID, s.SectionID); !ok || owner != id {
				return nil, fmt.Errorf("loadgen: seed of shard %d has %s/%s, which the shard map puts on shard %d", id, s.EventID, s.SectionID, owner)
			}
			name, _, _ := strings.Cut(s.SeatID, "-")
			k := rowKey{s.EventID, s.SectionID, name}
			r := rows[k]
			if r == nil {
				r = &Row{Event: s.EventID, Section: s.SectionID, Shard: id}
				rows[k] = r
				u.Rows = append(u.Rows, r)
			}
			r.Seats = append(r.Seats, s.SeatID)
			sections[Section{s.EventID, s.SectionID}] = true
			u.Seats++
		}
	}
	for s := range sections {
		u.Sections = append(u.Sections, s)
	}
	sort.Slice(u.Sections, func(i, j int) bool {
		a, b := u.Sections[i], u.Sections[j]
		if a.Event != b.Event {
			return a.Event < b.Event
		}
		return a.ID < b.ID
	})
	if len(u.Rows) == 0 {
		return nil, fmt.Errorf("loadgen: the seed files hold no seats")
	}
	return u, nil
}

// event returns the rows of one event grouped by section, in section order,
// rows in file order.
func (u *Universe) event(id string) [][]*Row {
	var out [][]*Row
	index := map[string]int{}
	for _, s := range u.Sections {
		if s.Event == id {
			index[s.ID] = len(out)
			out = append(out, nil)
		}
	}
	for _, r := range u.Rows {
		if i, ok := index[r.Section]; ok && r.Event == id {
			out[i] = append(out[i], r)
		}
	}
	return out
}

// Order is one booking the load generator will make.
type Order struct {
	Seats []Seat
	Cross bool // seats on two shards
}

func (o *Order) add(r *Row, seats []string) {
	for _, s := range seats {
		o.Seats = append(o.Seats, Seat{r.Event, r.Section, s})
	}
}

// strip is a run of adjacent seats nobody has asked for yet.
type strip struct {
	row   *Row
	seats []string
}

// Workload makes orders. Touchable says which sections it can book. Worker
// gives each goroutine its own random stream, so a run is repeatable per
// goroutine and the Zipf generators (not safe to share) stay private.
type Workload struct {
	cfg    Config
	u      *Universe
	sizes  OrderSizes
	flash  [][]*Row // evt-flash: sections, each with its rows
	eventS map[string][][]*Row

	mu     sync.Mutex
	strips []strip // fresh only
}

// NewWorkload prepares a workload. available lists the seats that are free
// now; the fresh workload needs it so it only asks for those. It may be nil
// for the others.
func NewWorkload(cfg Config, u *Universe, available map[Seat]bool) (*Workload, error) {
	sizes, err := ParseOrderSizes(cfg.OrderSizes)
	if err != nil {
		return nil, err
	}
	w := &Workload{cfg: cfg, u: u, sizes: sizes, eventS: map[string][][]*Row{}}
	switch cfg.Workload {
	case WorkloadFlash:
		w.flash = u.event(FlashEvent)
		if len(w.flash) == 0 {
			return nil, fmt.Errorf("loadgen: the dataset has no event %s", FlashEvent)
		}
		for _, sec := range w.flash {
			for _, r := range sec {
				if len(r.Seats) < sizes.largest {
					return nil, fmt.Errorf("loadgen: -order-sizes asks for %d adjacent seats, a row has %d", sizes.largest, len(r.Seats))
				}
			}
		}
	case WorkloadFresh:
		for _, r := range u.Rows {
			start := -1
			for i, s := range r.Seats {
				free := available[Seat{r.Event, r.Section, s}]
				if free && start < 0 {
					start = i
				}
				if !free && start >= 0 {
					w.strips = append(w.strips, strip{r, r.Seats[start:i]})
					start = -1
				}
			}
			if start >= 0 {
				w.strips = append(w.strips, strip{r, r.Seats[start:]})
			}
		}
		rng := rand.New(rand.NewPCG(cfg.Seed, 7))
		rng.Shuffle(len(w.strips), func(i, j int) { w.strips[i], w.strips[j] = w.strips[j], w.strips[i] })
	}
	if cfg.CrossShardFrac > 0 {
		for _, s := range u.Sections {
			if _, ok := w.eventS[s.Event]; !ok {
				w.eventS[s.Event] = u.event(s.Event)
			}
		}
	}
	return w, nil
}

// Touchable returns the sections this workload can book.
func (w *Workload) Touchable() []Section {
	if w.cfg.Workload != WorkloadFlash {
		return w.u.Sections
	}
	var out []Section
	for _, s := range w.u.Sections {
		if s.Event == FlashEvent {
			out = append(out, s)
		}
	}
	return out
}

// Worker makes orders for one goroutine.
type Worker struct {
	w        *Workload
	rng      *rand.Rand
	sections *rand.Zipf
	rows     []*rand.Zipf // per section
}

// Worker returns a generator seeded by id.
func (w *Workload) Worker(id uint64) *Worker {
	wk := &Worker{w: w, rng: rand.New(rand.NewPCG(w.cfg.Seed, id))}
	if w.flash != nil {
		wk.sections = rand.NewZipf(wk.rng, w.cfg.ZipfS, 1, uint64(len(w.flash)-1))
		wk.rows = make([]*rand.Zipf, len(w.flash))
		for i, sec := range w.flash {
			wk.rows[i] = rand.NewZipf(wk.rng, w.cfg.ZipfS, 1, uint64(len(sec)-1))
		}
	}
	return wk
}

// adjacent picks n adjacent seats of a row.
func (k *Worker) adjacent(o *Order, r *Row, n int) {
	n = min(n, len(r.Seats))
	at := k.rng.IntN(len(r.Seats) - n + 1)
	o.add(r, r.Seats[at:at+n])
}

// Next returns the next order, or false when a fresh workload has run out of
// seats nobody asked for.
func (k *Worker) Next() (Order, bool) {
	w := k.w
	n := w.sizes.pick(k.rng.IntN(w.sizes.total))
	cross := w.cfg.CrossShardFrac > 0 && k.rng.Float64() < w.cfg.CrossShardFrac
	if cross {
		n = max(n, 2)
	}
	var o Order
	if w.cfg.Workload == WorkloadFresh {
		return k.fresh(n, cross)
	}
	if cross {
		if k.crossOrder(&o, n) {
			return o, true
		}
	}
	if w.flash != nil {
		s := k.sections.Uint64()
		sec := w.flash[s]
		k.adjacent(&o, sec[k.rows[s].Uint64()], n)
		return o, true
	}
	k.adjacent(&o, w.u.Rows[k.rng.IntN(len(w.u.Rows))], n)
	return o, true
}

// crossOrder puts half the seats in a section of one shard and the rest in a
// section of another shard of the same event.
func (k *Worker) crossOrder(o *Order, n int) bool {
	w := k.w
	event := FlashEvent
	if w.flash == nil {
		event = w.u.Rows[k.rng.IntN(len(w.u.Rows))].Event
	}
	secs := w.eventS[event]
	first := secs[k.rng.IntN(len(secs))]
	a := first[k.rng.IntN(len(first))]
	for range 32 {
		other := secs[k.rng.IntN(len(secs))]
		b := other[k.rng.IntN(len(other))]
		if b.Shard != a.Shard {
			k.adjacent(o, a, n/2)
			k.adjacent(o, b, n-n/2)
			o.Cross = true
			return true
		}
	}
	return false // an event on a single shard
}

func (k *Worker) fresh(n int, cross bool) (Order, bool) {
	w := k.w
	w.mu.Lock()
	defer w.mu.Unlock()
	var o Order
	if len(w.strips) == 0 {
		return o, false
	}
	first := n
	if cross {
		first = n / 2
	}
	i := k.rng.IntN(len(w.strips))
	event, shard := w.strips[i].row.Event, w.strips[i].row.Shard
	w.takeFrom(&o, i, first)
	if rest := n - first; rest > 0 {
		for range 64 {
			if len(w.strips) == 0 {
				break
			}
			j := k.rng.IntN(len(w.strips))
			if r := w.strips[j].row; r.Event == event && r.Shard != shard {
				w.takeFrom(&o, j, rest)
				o.Cross = true
				break
			}
		}
	}
	return o, true
}

// takeFrom moves up to n seats from strip i into o. The tail of a row can give
// a smaller order than asked; that happens at most once per strip.
func (w *Workload) takeFrom(o *Order, i, n int) {
	s := &w.strips[i]
	n = min(n, len(s.seats))
	o.add(s.row, s.seats[:n])
	s.seats = s.seats[n:]
	if len(s.seats) == 0 {
		last := len(w.strips) - 1
		w.strips[i] = w.strips[last]
		w.strips = w.strips[:last]
	}
}
