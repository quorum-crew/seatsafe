// Package shard runs the storage-node side of a shard: it connects a Raft
// node to the seat state machine (internal/seatsm), serves ShardService and
// ParticipantService, and seeds the shard's initial seats through the log.
package shard

import (
	"log"

	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
)

// StateMachine is what a storage node needs from the shard state machine.
// *seatsm.Machine implements it. Apply, Snapshot and Restore run only on the
// Raft apply loop; the other methods are reads and may run concurrently.
type StateMachine interface {
	seatsm.StateMachine
	SeedStatus() (version string, applied, total uint32, done bool)
	NextMissingChunk() (chunk uint32, ok bool)
	ExpiredHolds(nowMs int64) []string
	ListAvailable(eventID, sectionID string) *seatsafev1.ListAvailableSeatsResponse
}

var _ StateMachine = (*seatsm.Machine)(nil)

// Seeded reports whether every seed chunk has been applied.
func Seeded(sm StateMachine) bool {
	_, _, _, done := sm.SeedStatus()
	return done
}

// Applier adapts a StateMachine to raftnode.StateMachine by decoding each log
// entry as a ShardCommand. Apply returns a seatsm.Result.
type Applier struct{ SM StateMachine }

// Apply implements raftnode.StateMachine.
func (a Applier) Apply(index uint64, data []byte) any {
	var cmd seatsafev1.ShardCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		// Every replica sees the same bytes, so skipping is still deterministic.
		log.Printf("shard: entry %d is not a ShardCommand, skipped: %v", index, err)
		return seatsm.Result{}
	}
	return a.SM.Apply(index, &cmd)
}

// Snapshot implements raftnode.StateMachine.
func (a Applier) Snapshot() ([]byte, error) { return a.SM.Snapshot() }

// CaptureSnapshot implements raftnode.SnapshotCapturer, so a seatsm snapshot
// is encoded off the apply goroutine. Any other StateMachine is snapshotted
// right away.
func (a Applier) CaptureSnapshot() func() ([]byte, error) {
	if c, ok := a.SM.(interface{ CaptureSnapshot() func() ([]byte, error) }); ok {
		return c.CaptureSnapshot()
	}
	data, err := a.SM.Snapshot()
	return func() ([]byte, error) { return data, err }
}

// Restore implements raftnode.StateMachine.
func (a Applier) Restore(data []byte) error { return a.SM.Restore(data) }
