package shard

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
)

// Default garbage collection timing: every 30 s, dropping records older than
// 2 min. Snapshots carry every req/ and txn/ record still kept, so retention
// sets their size: at benchmark rates, 10 min of records made an 83 MiB
// snapshot. 2 min is still far above any client retry window (seconds).
// cmd/storage reads GC_RETENTION_MS to override it.
const (
	DefaultGCInterval  = 30 * time.Second
	DefaultGCRetention = 2 * time.Minute
)

// Collector keeps seatsm's dedup (req/) and decided-txn (txn/) tables from
// growing without bound. Whichever node is leader proposes a GcOp every
// Interval that drops records older than Retention; followers do nothing, and
// a leadership change just means the next leader carries on.
//
// Retention must stay well above any client retry window: once a request_id
// or txn tombstone is dropped, a late retry of it is no longer recognized.
// seatsm never drops PREPARED txns, seats or seed progress.
type Collector struct {
	Node *raftnode.Node
	Logf func(format string, args ...any)

	Interval  time.Duration // default DefaultGCInterval
	Retention time.Duration // default DefaultGCRetention

	now func() time.Time // the leader's clock; replaced in tests
}

// Run collects until ctx is done.
func (c *Collector) Run(ctx context.Context) {
	interval := c.Interval
	if interval <= 0 {
		interval = DefaultGCInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !c.Node.Status().IsLeader() {
			continue
		}
		if err := c.collect(ctx, interval); err != nil {
			c.Logf("gc: %v", err)
		}
	}
}

// collect proposes one GcOp, stamped with the leader's clock like every
// other command, so all replicas drop exactly the same records.
func (c *Collector) collect(ctx context.Context, timeout time.Duration) error {
	retention := c.Retention
	if retention <= 0 {
		retention = DefaultGCRetention
	}
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	t := now()
	data, err := proto.Marshal(&seatsafev1.ShardCommand{
		NowMs: t.UnixMilli(),
		Op:    &seatsafev1.ShardCommand_Gc{Gc: &seatsafev1.GcOp{DropBeforeMs: t.Add(-retention).UnixMilli()}},
	})
	if err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := c.Node.Propose(pctx, data)
	switch {
	case transient(err), errors.Is(err, context.Canceled):
		return nil // leadership moved or shutting down: the next leader collects
	case err != nil:
		return err
	}
	if r, _ := res.(seatsm.Result); r.Err != nil {
		return r.Err
	}
	return nil
}
