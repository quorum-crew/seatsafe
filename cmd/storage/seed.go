package main

import (
	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shard"
)

// seedLoader reads this shard's seed file from SEED_DIR (default /seed).
func seedLoader(shardID uint32) func() (shard.SeedSource, error) {
	return func() (shard.SeedSource, error) {
		l, err := seed.Load(seed.Path(seed.DirFromEnv(), shardID), shardID)
		if err != nil {
			return nil, err
		}
		return seedFile{l}, nil
	}
}

// seedFile adapts internal/seed's Loaded to shard.SeedSource.
type seedFile struct{ l *seed.Loaded }

func (f seedFile) Version() string  { return f.l.Version }
func (f seedFile) TotalChunks() int { return f.l.TotalChunks() }
func (f seedFile) Chunk(i int) []*seatsafev1.SeatRef {
	seats := f.l.Chunk(i)
	refs := make([]*seatsafev1.SeatRef, len(seats))
	for j, s := range seats {
		refs[j] = &seatsafev1.SeatRef{EventId: s.EventID, SectionId: s.SectionID, SeatId: s.SeatID}
	}
	return refs
}
