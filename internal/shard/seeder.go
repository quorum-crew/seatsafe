package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
)

// SeedSource is one shard's seed file (internal/seed.Loaded behind a small
// adapter in cmd/storage).
type SeedSource interface {
	Version() string
	TotalChunks() int
	Chunk(i int) []*seatsafev1.SeatRef
}

// Seeder loads a shard's initial seats through the Raft log. While the shard
// is not fully seeded, whichever node is leader proposes the chunks the state
// machine is still missing, one SeedOp at a time; a leader that crashes
// halfway is harmless because the next one carries on from NextMissingChunk.
// Run returns once the shard is seeded.
type Seeder struct {
	Node *raftnode.Node
	SM   StateMachine
	Load func() (SeedSource, error)
	Logf func(format string, args ...any)

	Interval time.Duration // how often to check; default 500ms
	Backoff  time.Duration // wait after a failed attempt; default 10s
}

var errNotLeader = errors.New("lost leadership")

// Run seeds until the shard is seeded or ctx is done.
func (s *Seeder) Run(ctx context.Context) {
	interval, backoff := s.Interval, s.Backoff
	if interval == 0 {
		interval = 500 * time.Millisecond
	}
	if backoff == 0 {
		backoff = 10 * time.Second
	}
	var src SeedSource
	wait := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = interval
		if Seeded(s.SM) {
			version, _, total, _ := s.SM.SeedStatus()
			s.Logf("shard is seeded: %d chunks, seed_version %.12s", total, version)
			return
		}
		if !s.Node.Status().IsLeader() {
			continue
		}
		if src == nil {
			var err error
			if src, err = s.Load(); err != nil {
				s.Logf("seed: cannot load seed file (run `go run ./cmd/datagen -out deploy`?): %v", err)
				src, wait = nil, backoff
				continue
			}
			if src.TotalChunks() == 0 {
				s.Logf("seed: seed file has no seats")
				src, wait = nil, backoff
				continue
			}
			s.Logf("seed: leader seeding %d chunks, seed_version %.12s", src.TotalChunks(), src.Version())
		}
		if err := s.proposeMissing(ctx, src); err != nil {
			s.Logf("seed: %v", err)
			if !errors.Is(err, errNotLeader) {
				wait = backoff
			}
		}
	}
}

// proposeMissing proposes every chunk the state machine still lacks.
func (s *Seeder) proposeMissing(ctx context.Context, src SeedSource) error {
	total := src.TotalChunks()
	for {
		if !s.Node.Status().IsLeader() {
			return errNotLeader
		}
		chunk, ok := s.SM.NextMissingChunk()
		if !ok {
			return nil // seeded
		}
		if int(chunk) >= total {
			return fmt.Errorf("state machine wants chunk %d but the seed file has %d chunks; it holds a different seed. Wipe it with `docker compose down -v`", chunk, total)
		}
		data, err := proto.Marshal(&seatsafev1.ShardCommand{
			NowMs: time.Now().UnixMilli(),
			Op: &seatsafev1.ShardCommand_Seed{Seed: &seatsafev1.SeedOp{
				SeedVersion: src.Version(),
				Chunk:       chunk,
				TotalChunks: uint32(total),
				Seats:       src.Chunk(int(chunk)),
			}},
		})
		if err != nil {
			return err
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		res, err := s.Node.Propose(pctx, data)
		cancel()
		if err != nil {
			return fmt.Errorf("propose chunk %d: %w", chunk, err)
		}
		if r, _ := res.(seatsm.Result); r.Err != nil {
			return fmt.Errorf("chunk %d refused: %v. Wipe the shard with `docker compose down -v` if the seed changed", chunk, r.Err)
		}
	}
}
