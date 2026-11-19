package coordinator

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/coordsm"
)

// deliver sends a decided txn's decision to every participant that has not
// acked it, each in its own background goroutine. A delivery already running
// from this node is not started twice.
func (c *Coordinator) deliver(t *coordsm.Txn) {
	for _, shard := range t.MissingAcks() {
		key := delivery{t.ID, shard}
		c.mu.Lock()
		running := c.delivering[key]
		if !running && !c.closed {
			c.delivering[key] = true
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.deliverTo(key, t.Decision)
				c.mu.Lock()
				delete(c.delivering, key)
				c.mu.Unlock()
			}()
		}
		c.mu.Unlock()
	}
}

// deliverTo sends Commit or Abort to one shard until it answers, then logs
// TxnAcked. It stops when this node is no longer leader (the next leader
// resends) or the shard refuses the decision.
func (c *Coordinator) deliverTo(key delivery, d seatsafev1.Decision) {
	g := c.cfg.Shards[key.shard]
	if g == nil {
		c.cfg.Logf("coordinator: txn %s: no client for shard %d, cannot deliver %s", key.txn, key.shard, decisionLabel(d))
		return
	}
	req := &seatsafev1.DecisionRequest{TxnId: key.txn}
	for attempt := 0; ; attempt++ {
		if c.ctx.Err() != nil || !c.node.Status().IsLeader() {
			return
		}
		ctx, cancel := context.WithTimeout(c.ctx, c.cfg.DecisionTimeout)
		err := g.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
			client := seatsafev1.NewParticipantServiceClient(cc)
			var err error
			if d == seatsafev1.Decision_DECISION_COMMIT {
				_, err = client.Commit(ctx, req, opts...)
			} else {
				_, err = client.Abort(ctx, req, opts...)
			}
			return err
		})
		cancel()
		if err == nil {
			break
		}
		if status.Code(err) == codes.FailedPrecondition {
			// The group client passes FAILED_PRECONDITION on only when it has
			// no leader trailer: the shard's log contradicts the decision
			// (Commit of a txn it aborted, or Abort of one it committed).
			// Retrying cannot change that.
			c.Metrics.Violations.Inc(shardLabel(key.shard), decisionLabel(d))
			c.cfg.Logf("coordinator: PROTOCOL VIOLATION: txn %s: shard %d refused %s: %v", key.txn, key.shard, decisionLabel(d), err)
			return
		}
		c.Metrics.Retries.Inc(decisionLabel(d))
		c.sleep(c.cfg.Backoff[min(attempt, len(c.cfg.Backoff)-1)])
	}
	res, err := c.proposeWhileLeader(&seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Acked{Acked: &seatsafev1.TxnAcked{TxnId: key.txn, ShardId: key.shard}}})
	switch {
	case err != nil:
		// The next leader resends; the shard answers again, and it is logged then.
	case res.Err != nil:
		c.cfg.Logf("coordinator: txn %s: ack from shard %d refused: %v", key.txn, key.shard, res.Err)
	}
}

// watchLeadership notices when this node becomes leader and takes over the
// txns the previous leader left behind.
func (c *Coordinator) watchLeadership() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.LeaderPoll)
	defer t.Stop()
	var handled uint64 // the term whose takeover is complete
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		st := c.node.Status()
		if !st.IsLeader() || st.Term == handled {
			continue
		}
		if c.takeOver(st.Term) {
			handled = st.Term
		}
	}
}

// takeOver finishes what earlier leaders started. It reports false if this
// node stopped being leader of term before it was done; the caller then tries
// again when it leads once more.
func (c *Coordinator) takeOver(term uint64) bool {
	// A barrier: once this no-op is applied here, so is every entry logged
	// before this term, and the state machine shows every txn they started.
	if _, err := c.proposeWhileLeader(&seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Gc{Gc: &seatsafev1.GcOp{}}}); err != nil {
		return false
	}
	if st := c.node.Status(); !st.IsLeader() || st.Term != term {
		return false
	}
	undecided, unacked := c.sm.Undecided(), c.sm.Unacked()
	aborted, ok := c.abortAll(undecided, SourceNewLeader)
	if !ok {
		return false
	}
	for _, t := range unacked {
		c.deliver(t)
	}
	c.cfg.Logf("coordinator: leader of term %d: aborted %d of %d undecided txns, resending the decision of %d txns missing acks",
		term, aborted, len(undecided), len(unacked))
	return true
}

// abortAll proposes ABORT for each undecided txn that is not being run on
// this node, and delivers whichever decision won. It reports how many of its
// ABORTs won, and false if this node stopped being leader.
func (c *Coordinator) abortAll(txns []*coordsm.Txn, source string) (aborted int, ok bool) {
	for _, t := range txns {
		if c.inFlight(t.ID) {
			continue // submitted to this leader; its run decides
		}
		res, err := c.proposeWhileLeader(abortCommand(t.ID))
		if err != nil {
			return aborted, false
		}
		if res.Err != nil || res.Txn == nil {
			c.cfg.Logf("coordinator: txn %s: ABORT refused: %v", t.ID, res.Err)
			continue
		}
		if res.Won {
			aborted++
			c.Metrics.Decisions.Inc(decisionLabel(res.Txn.Decision), source)
		}
		c.deliver(res.Txn)
	}
	return aborted, true
}

// collect runs on every node; the leader proposes a GcOp every GCInterval
// and aborts undecided txns that nobody is running.
func (c *Coordinator) collect() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		if !c.node.Status().IsLeader() {
			continue
		}
		now := c.cfg.now()
		ctx, cancel := context.WithTimeout(c.ctx, c.cfg.GCInterval)
		res, err := c.propose(ctx, &seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Gc{Gc: &seatsafev1.GcOp{DropBeforeMs: now.Add(-c.cfg.GCRetention).UnixMilli()}}})
		cancel()
		if err != nil {
			if !transient(err) && c.ctx.Err() == nil {
				c.cfg.Logf("coordinator: gc: %v", err)
			}
			continue // leadership moved or shutting down: the next leader collects
		}
		if res.Dropped > 0 {
			c.cfg.Logf("coordinator: gc dropped %d txns", res.Dropped)
		}
		c.sweep(now)
	}
}

// sweep aborts undecided txns older than StuckAfter that this node is not
// running. That happens when a TxnStarted was applied but its SubmitOrder
// call failed before the order could run (the proposal outlived the caller's
// deadline) and the client never retried: without this the record would stay
// undecided, and so uncollectable, until the next leader change.
func (c *Coordinator) sweep(now time.Time) {
	var stuck []*coordsm.Txn
	cutoff := now.Add(-c.cfg.StuckAfter).UnixMilli()
	for _, t := range c.sm.Undecided() {
		if t.StartedMs < cutoff {
			stuck = append(stuck, t)
		}
	}
	if aborted, _ := c.abortAll(stuck, SourceSweep); aborted > 0 {
		c.cfg.Logf("coordinator: aborted %d undecided txns older than %v that nobody was running", aborted, c.cfg.StuckAfter)
	}
}
