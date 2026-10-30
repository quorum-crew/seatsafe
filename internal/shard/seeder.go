package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
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

	Interval     time.Duration // how often to check; default 500ms
	Backoff      time.Duration // wait after a real failure; default 10s
	ChunkTimeout time.Duration // per-chunk proposal timeout; default 5s
}

// errRetrySoon marks a failure that only means "not now": this node is not
// (or is no longer) the leader, or a proposal was dropped or timed out while
// leadership moved. Run retries at the normal interval, not after Backoff.
var errRetrySoon = errors.New("will retry")

// transient reports whether a Propose error is a leadership hiccup rather
// than a real problem with the seed.
func transient(err error) bool {
	return errors.Is(err, raftnode.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrProposalDropped) ||
		errors.Is(err, context.DeadlineExceeded)
}

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
			if !errors.Is(err, errRetrySoon) {
				wait = backoff
			}
		}
	}
}

// proposeMissing proposes every chunk the state machine still lacks.
func (s *Seeder) proposeMissing(ctx context.Context, src SeedSource) error {
	total := src.TotalChunks()
	timeout := s.ChunkTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	for {
		if !s.Node.Status().IsLeader() {
			return fmt.Errorf("not the leader: %w", errRetrySoon)
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
		pctx, cancel := context.WithTimeout(ctx, timeout)
		res, err := s.Node.Propose(pctx, data)
		cancel()
		if transient(err) {
			// Whoever is leader next carries on from NextMissingChunk.
			return fmt.Errorf("propose chunk %d: %v: %w", chunk, err, errRetrySoon)
		}
		if err != nil {
			return fmt.Errorf("propose chunk %d: %w", chunk, err)
		}
		if r, _ := res.(seatsm.Result); r.Err != nil {
			return fmt.Errorf("chunk %d refused: %v. Wipe the shard with `docker compose down -v` if the seed changed", chunk, r.Err)
		}
	}
}
