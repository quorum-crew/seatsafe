// Package datagen builds the synthetic dataset a SeatSafe cluster starts
// from: the shard map and every shard's seed file. cmd/datagen writes it for
// the Docker Compose cluster and cmd/clusterctl for a cluster on real
// machines.
package datagen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// Defaults is the default dataset: one flash-sale event of 50,000 seats and
// 4 regular events of 4,800 seats each, on 3 shards of 3 nodes with the
// Docker Compose host names.
func Defaults() Config {
	return Config{Shards: 3, NodesPerGroup: 3, Port: 7000, ShardHostFmt: "shard%d-node%d", CoordHostFmt: "coord-%d", ChunkSize: seed.DefaultChunkSize,
		FlashSections: 50, FlashRows: 20, FlashSeatsPerRow: 50, Events: 4, Sections: 12, Rows: 10, SeatsPerRow: 40}
}

// Config is everything that shapes the dataset.
type Config struct {
	Shards        int
	NodesPerGroup int
	Port          int
	ShardHostFmt  string // fmt with shard id then node index, e.g. "shard%d-node%d"
	CoordHostFmt  string // fmt with node index, e.g. "coord-%d"
	// CoordAddrs and ShardAddrs, if set, are the node addresses ("host:port")
	// of the coordinator group and of every shard, used as they are instead
	// of the formats above. A cluster on real machines sets them from its
	// inventory.
	CoordAddrs []string
	ShardAddrs map[uint32][]string
	ChunkSize  int

	FlashSections    int
	FlashRows        int
	FlashSeatsPerRow int

	Events      int
	Sections    int
	Rows        int
	SeatsPerRow int
}

// Output is the generated dataset, already encoded.
type Output struct {
	ShardMap  []byte
	SeedFiles map[uint32][]byte
	Stats     Stats
}

// Stats summarizes the dataset for the console and tests.
type Stats struct {
	Events, Sections, Seats int
	SeatsPerShard           map[uint32]int
	Versions                map[uint32]string
	Chunks                  map[uint32]int
}

type eventSpec struct {
	id                          string
	sections, rows, seatsPerRow int
}

// Generate builds the dataset in memory.
func Generate(c Config) (*Output, error) {
	if c.Shards < 1 || c.NodesPerGroup < 1 {
		return nil, fmt.Errorf("need at least 1 shard and 1 node per group")
	}
	if c.ChunkSize < 1 {
		return nil, fmt.Errorf("chunk-size must be positive")
	}

	var events []eventSpec
	if c.FlashSections > 0 {
		events = append(events, eventSpec{"evt-flash", c.FlashSections, c.FlashRows, c.FlashSeatsPerRow})
	}
	for i := 1; i <= c.Events; i++ {
		events = append(events, eventSpec{fmt.Sprintf("evt-%03d", i), c.Sections, c.Rows, c.SeatsPerRow})
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no events to generate")
	}

	shardIDs := make([]uint32, c.Shards)
	for i := range shardIDs {
		shardIDs[i] = uint32(i + 1)
	}

	f := shardmap.File{Version: shardmap.CurrentVersion}
	f.Coordinator.Nodes = append(f.Coordinator.Nodes, c.CoordAddrs...)
	for n := 1; n <= c.NodesPerGroup && c.CoordAddrs == nil; n++ {
		f.Coordinator.Nodes = append(f.Coordinator.Nodes, fmt.Sprintf(c.CoordHostFmt+":%d", n, c.Port))
	}
	for _, id := range shardIDs {
		s := shardmap.Shard{ShardID: id}
		if c.ShardAddrs != nil {
			if len(c.ShardAddrs[id]) == 0 {
				return nil, fmt.Errorf("no addresses for shard %d", id)
			}
			s.Nodes = append(s.Nodes, c.ShardAddrs[id]...)
		}
		for n := 1; n <= c.NodesPerGroup && c.ShardAddrs == nil; n++ {
			s.Nodes = append(s.Nodes, fmt.Sprintf(c.ShardHostFmt+":%d", id, n, c.Port))
		}
		f.Shards = append(f.Shards, s)
	}

	seeds := make(map[uint32]*seed.File, c.Shards)
	for _, id := range shardIDs {
		seeds[id] = &seed.File{FormatVersion: seed.FormatVersion, ShardID: id, ChunkSize: c.ChunkSize, Seats: []seed.Seat{}}
	}

	stats := Stats{SeatsPerShard: map[uint32]int{}, Versions: map[uint32]string{}, Chunks: map[uint32]int{}}
	for ei, ev := range events {
		if ev.sections < 1 || ev.rows < 1 || ev.seatsPerRow < 1 {
			return nil, fmt.Errorf("event %s: sections, rows and seats per row must be positive", ev.id)
		}
		secIDs := make([]string, ev.sections)
		for s := range secIDs {
			secIDs[s] = fmt.Sprintf("sec-%03d", 101+s)
		}
		// Rotate the starting shard per event.
		rot := append(append([]uint32{}, shardIDs[ei%len(shardIDs):]...), shardIDs[:ei%len(shardIDs)]...)
		secs := shardmap.RoundRobin(ev.id, secIDs, rot)
		f.Sections = append(f.Sections, secs...)

		for _, sec := range secs {
			sf := seeds[sec.ShardID]
			for r := 1; r <= ev.rows; r++ {
				for n := 1; n <= ev.seatsPerRow; n++ {
					sf.Seats = append(sf.Seats, seed.Seat{EventID: ev.id, SectionID: sec.SectionID, SeatID: fmt.Sprintf("R%02d-S%03d", r, n)})
				}
			}
			stats.SeatsPerShard[sec.ShardID] += ev.rows * ev.seatsPerRow
		}
		stats.Events++
		stats.Sections += ev.sections
		stats.Seats += ev.sections * ev.rows * ev.seatsPerRow
	}

	// Validate the shard map with the same code the router uses.
	if _, err := shardmap.New(f); err != nil {
		return nil, err
	}
	sm, err := marshalIndent(f)
	if err != nil {
		return nil, err
	}

	out := &Output{ShardMap: sm, SeedFiles: map[uint32][]byte{}, Stats: stats}
	for _, id := range shardIDs {
		b, err := seed.Marshal(*seeds[id])
		if err != nil {
			return nil, err
		}
		l, err := seed.Parse(b, id)
		if err != nil {
			return nil, err
		}
		out.SeedFiles[id] = b
		stats.Versions[id] = l.Version
		stats.Chunks[id] = l.TotalChunks()
	}
	return out, nil
}

// Write writes the dataset under dir, replacing each file atomically.
func Write(dir string, o *Output) error {
	seedDir := filepath.Join(dir, "seed")
	if err := os.MkdirAll(seedDir, 0o755); err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(dir, "shardmap.json"), o.ShardMap); err != nil {
		return err
	}
	for id, b := range o.SeedFiles {
		if err := writeAtomic(seed.Path(seedDir, id), b); err != nil {
			return err
		}
	}
	return nil
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func marshalIndent(f shardmap.File) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
