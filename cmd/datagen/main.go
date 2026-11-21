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
	"flag"
	"fmt"
	"os"

	"github.com/quorum-crew/seatsafe/internal/datagen"
	"github.com/quorum-crew/seatsafe/internal/seed"
)

// The generator lives in internal/datagen.
type (
	Config = datagen.Config
	Output = datagen.Output
	Stats  = datagen.Stats
)

var (
	Generate = datagen.Generate
	Write    = datagen.Write
)

func main() {
	var c Config
	out := flag.String("out", "deploy", "output directory (shardmap.json and seed/ go here)")
	flag.IntVar(&c.Shards, "shards", 3, "number of shard Raft groups")
	flag.IntVar(&c.NodesPerGroup, "nodes", 3, "nodes per Raft group")
	flag.IntVar(&c.Port, "port", 7000, "gRPC port of every node")
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
