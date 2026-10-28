package fakecluster

import (
	"fmt"

	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// Dataset describes a synthetic dataset shaped like cmd/datagen's output:
// one flash event plus regular events, each event's sections spread
// round-robin over the shards starting at a different shard per event.
type Dataset struct {
	Shards                                     int
	FlashSections, FlashRows, FlashSeatsPerRow int
	Events, Sections, Rows, SeatsPerRow        int
	ChunkSize                                  int
}

// SmallDataset is 3 shards, a 6 x 5 x 10 flash event (300 seats) and two
// 3 x 4 x 5 events (60 seats each).
var SmallDataset = Dataset{
	Shards:        3,
	FlashSections: 6, FlashRows: 5, FlashSeatsPerRow: 10,
	Events: 2, Sections: 3, Rows: 4, SeatsPerRow: 5,
	ChunkSize: 100,
}

// Build returns the shard map (nodes "shard{N}-node{i}:7000" and
// "coord-{i}:7000") and each shard's seed file.
func (d Dataset) Build() (*shardmap.Map, map[uint32]*seed.Loaded, error) {
	if d.Shards < 1 {
		return nil, nil, fmt.Errorf("fakecluster: dataset needs at least 1 shard")
	}
	type ev struct {
		id                          string
		sections, rows, seatsPerRow int
	}
	var events []ev
	if d.FlashSections > 0 {
		events = append(events, ev{"evt-flash", d.FlashSections, d.FlashRows, d.FlashSeatsPerRow})
	}
	for i := 1; i <= d.Events; i++ {
		events = append(events, ev{fmt.Sprintf("evt-%03d", i), d.Sections, d.Rows, d.SeatsPerRow})
	}

	f := shardmap.File{Version: shardmap.CurrentVersion}
	for n := 1; n <= 3; n++ {
		f.Coordinator.Nodes = append(f.Coordinator.Nodes, fmt.Sprintf("coord-%d:7000", n))
	}
	var ids []uint32
	files := map[uint32]*seed.File{}
	for id := uint32(1); id <= uint32(d.Shards); id++ {
		s := shardmap.Shard{ShardID: id}
		for n := 1; n <= 3; n++ {
			s.Nodes = append(s.Nodes, fmt.Sprintf("shard%d-node%d:7000", id, n))
		}
		f.Shards = append(f.Shards, s)
		ids = append(ids, id)
		files[id] = &seed.File{FormatVersion: seed.FormatVersion, ShardID: id, ChunkSize: d.ChunkSize, Seats: []seed.Seat{}}
	}
	for ei, e := range events {
		var secIDs []string
		for s := 0; s < e.sections; s++ {
			secIDs = append(secIDs, fmt.Sprintf("sec-%03d", 101+s))
		}
		rot := append(append([]uint32{}, ids[ei%len(ids):]...), ids[:ei%len(ids)]...)
		secs := shardmap.RoundRobin(e.id, secIDs, rot)
		f.Sections = append(f.Sections, secs...)
		for _, sec := range secs {
			sf := files[sec.ShardID]
			for r := 1; r <= e.rows; r++ {
				for n := 1; n <= e.seatsPerRow; n++ {
					sf.Seats = append(sf.Seats, seed.Seat{EventID: e.id, SectionID: sec.SectionID, SeatID: fmt.Sprintf("R%02d-S%03d", r, n)})
				}
			}
		}
	}
	sm, err := shardmap.New(f)
	if err != nil {
		return nil, nil, err
	}
	seeds := map[uint32]*seed.Loaded{}
	for id, sf := range files {
		b, err := seed.Marshal(*sf)
		if err != nil {
			return nil, nil, err
		}
		if seeds[id], err = seed.Parse(b, id); err != nil {
			return nil, nil, err
		}
	}
	return sm, seeds, nil
}
