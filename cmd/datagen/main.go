// Command datagen writes the synthetic dataset a SeatSafe cluster starts from:
//
//	{out}/shardmap.json       which shard owns each (event, section), plus node addresses
//	{out}/seed/shard{N}.json  each shard's seat inventory (loaded through Raft as SeedOps)
//
// The output is fully deterministic: the same flags always produce
// byte-identical files, so seed versions stay stable across reruns.
//
// Default dataset: one flash-sale event of 50,000 seats (50 sections x
// 20 rows x 50 seats) plus 4 regular events of 4,800 seats each, about 69K
// seats in total. Each event's sections are spread round-robin across shards,
// starting at a different shard per event so small events don't all pile onto
// shard 1.
//
// Usage:
//
//	go run ./cmd/datagen -out deploy
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// Config is everything that shapes the dataset.
type Config struct {
	Shards        int
	NodesPerGroup int
	Port          int
	ShardHostFmt  string // fmt with shard id then node index, e.g. "shard%d-node%d"
	CoordHostFmt  string // fmt with node index, e.g. "coord-%d"
	ChunkSize     int

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

func main() {
	var c Config
	out := flag.String("out", "deploy", "output directory (shardmap.json and seed/ go here)")
	flag.IntVar(&c.Shards, "shards", 3, "number of shard Raft groups")
	flag.IntVar(&c.NodesPerGroup, "nodes", 3, "nodes per Raft group")
	flag.IntVar(&c.Port, "port", 8080, "gRPC port of every node")
	flag.StringVar(&c.ShardHostFmt, "shard-host", "shard%d-node%d", "shard node hostname format (shard id, node index)")
	flag.StringVar(&c.CoordHostFmt, "coord-host", "coord-%d", "coordinator node hostname format (node index)")
	flag.IntVar(&c.ChunkSize, "chunk-size", seed.DefaultChunkSize, "seats per SeedOp")
	flag.IntVar(&c.FlashSections, "flash-sections", 50, "sections in the flash-sale event (0 = no flash event)")
	flag.IntVar(&c.FlashRows, "flash-rows", 20, "rows per flash section")
	flag.IntVar(&c.FlashSeatsPerRow, "flash-seats-per-row", 50, "seats per flash row")
	flag.IntVar(&c.Events, "events", 4, "number of regular events")
	flag.IntVar(&c.Sections, "sections", 12, "sections per regular event")
	flag.IntVar(&c.Rows, "rows", 10, "rows per regular section")
	flag.IntVar(&c.SeatsPerRow, "seats-per-row", 40, "seats per regular row")
	flag.Parse()

	o, err := Generate(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "datagen:", err)
		os.Exit(1)
	}
	if err := Write(*out, o); err != nil {
		fmt.Fprintln(os.Stderr, "datagen:", err)
		os.Exit(1)
	}
	s := o.Stats
	fmt.Printf("wrote %s: %d events, %d sections, %d seats\n", *out, s.Events, s.Sections, s.Seats)
	for id := uint32(1); id <= uint32(c.Shards); id++ {
		fmt.Printf("  shard %d: %6d seats, %3d chunks, seed_version %s\n", id, s.SeatsPerShard[id], s.Chunks[id], s.Versions[id][:12])
	}
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
	for n := 1; n <= c.NodesPerGroup; n++ {
		f.Coordinator.Nodes = append(f.Coordinator.Nodes, fmt.Sprintf(c.CoordHostFmt+":%d", n, c.Port))
	}
	for _, id := range shardIDs {
		s := shardmap.Shard{ShardID: id}
		for n := 1; n <= c.NodesPerGroup; n++ {
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
