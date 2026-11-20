package shard

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
)

// Default hold resolution timing. Holds live DefaultHoldTTL (5 s), so with a
// 1 s check a hold whose decision never arrived is resolved within about 6 s.
const (
	DefaultHoldCheckInterval = time.Second
	DefaultHoldJitter        = 0.2 // design doc: up to 20% jitter on the GetDecision timer
	DefaultDecideTimeout     = 2 * time.Second
	holdWorkers              = 8 // GetDecision calls in flight per round
)

// DecideFunc asks the coordinator group for the decision on a txn
// (CoordinatorService.GetDecision). The coordinator logs ABORT first if it has
// no decision yet, so any COMMIT or ABORT it returns is final.
type DecideFunc func(ctx context.Context, txnID string) (seatsafev1.Decision, error)

// CoordinatorDecider asks the coordinator group's leader through g, following
// redirects and failovers like the router does.
func CoordinatorDecider(g *groupclient.Group) DecideFunc {
	return func(ctx context.Context, txnID string) (seatsafev1.Decision, error) {
		var resp *seatsafev1.GetDecisionResponse
		err := g.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
			var err error
			resp, err = seatsafev1.NewCoordinatorServiceClient(cc).GetDecision(ctx, &seatsafev1.GetDecisionRequest{TxnId: txnID}, opts...)
			return err
		})
		return resp.GetDecision(), err
	}
}

// HoldResolver finishes 2PC txns whose decision never reached this shard, for
// example because the coordinator's Commit or Abort was lost or the
// coordinator crashed after Prepare. Whichever node is leader checks every
// Interval (plus up to Jitter, so shards don't call in lockstep) for PREPARED
// txns past their hold_expires_at_ms, asks the coordinator for each decision,
// and proposes the matching Commit or Abort through Raft.
//
// A hold is never released without the coordinator's answer: the coordinator
// may already have committed, and releasing the seats then would let them be
// sold twice. If the coordinator can't be reached the hold stays, and the next
// round asks again. Followers do nothing; a new leader carries on.
type HoldResolver struct {
	Node   *raftnode.Node
	SM     StateMachine
	Decide DecideFunc
	Logf   func(format string, args ...any)

	Interval      time.Duration // default DefaultHoldCheckInterval
	Jitter        float64       // fraction of Interval; default DefaultHoldJitter, negative for none
	DecideTimeout time.Duration // per GetDecision call; default DefaultDecideTimeout

	// OnResolved, if set, is called after a decision is applied, with the
	// decision ("commit" or "abort"). Used for metrics.
	OnResolved func(decision string)
	// OnRefused, if set, is called when this shard refuses the coordinator's
	// decision because it contradicts the shard's log (e.g. COMMIT for a txn
	// it already aborted). That means the coordinator and a participant
	// disagree about a txn's outcome, so it should never happen.
	OnRefused func(decision string)

	now func() time.Time // the leader's clock; replaced in tests
}

// Run resolves expired holds until ctx is done.
func (r *HoldResolver) Run(ctx context.Context) {
	interval, jitter := r.Interval, r.Jitter
	if interval <= 0 {
		interval = DefaultHoldCheckInterval
	}
	if jitter == 0 {
		jitter = DefaultHoldJitter
	}
	for {
		wait := interval
		if jitter > 0 {
			wait += time.Duration(rand.Float64() * jitter * float64(interval))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if !r.Node.Status().IsLeader() {
			continue
		}
		r.resolve(ctx)
	}
}

// resolve handles every hold that is expired by the leader's clock now.
func (r *HoldResolver) resolve(ctx context.Context) {
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	expired := r.SM.ExpiredHolds(now().UnixMilli())
	if len(expired) == 0 {
		return
	}
	ids := make(chan string)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures int
		lastErr  error
	)
	for range min(holdWorkers, len(expired)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				if err := r.resolveOne(ctx, id, now); err != nil {
					mu.Lock()
					failures, lastErr = failures+1, err
					mu.Unlock()
				}
			}
		}()
	}
	for _, id := range expired {
		select {
		case ids <- id:
		case <-ctx.Done():
		}
	}
	close(ids)
	wg.Wait()
	if failures > 0 && ctx.Err() == nil {
		r.Logf("holds: %d of %d expired holds still unresolved, retrying next round; last error: %v", failures, len(expired), lastErr)
	}
}

// resolveOne asks for one txn's decision and applies it on this shard.
func (r *HoldResolver) resolveOne(ctx context.Context, txnID string, now func() time.Time) error {
	if ctx.Err() != nil {
		return nil
	}
	timeout := r.DecideTimeout
	if timeout <= 0 {
		timeout = DefaultDecideTimeout
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	decision, err := r.Decide(dctx, txnID)
	cancel()
	if err != nil {
		return fmt.Errorf("txn %s: GetDecision: %w", txnID, err)
	}

	cmd := &seatsafev1.ShardCommand{NowMs: now().UnixMilli()}
	var name string
	switch decision {
	case seatsafev1.Decision_DECISION_COMMIT:
		cmd.Op, name = &seatsafev1.ShardCommand_Commit{Commit: &seatsafev1.TxnOp{TxnId: txnID}}, "commit"
	case seatsafev1.Decision_DECISION_ABORT:
		cmd.Op, name = &seatsafev1.ShardCommand_Abort{Abort: &seatsafev1.TxnOp{TxnId: txnID}}, "abort"
	default:
		return fmt.Errorf("txn %s: coordinator returned %v", txnID, decision)
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := r.Node.Propose(pctx, data)
	switch {
	case errors.Is(err, context.Canceled) && ctx.Err() != nil:
		return nil // shutting down
	case transient(err):
		return fmt.Errorf("txn %s: %s: %w", txnID, name, err) // leadership moved; the next leader resolves it
	case err != nil:
		return err
	}
	if rr, _ := res.(seatsm.Result); rr.Err != nil {
		// The decision contradicts this shard's log, e.g. COMMIT for a txn it
		// already aborted. The first decision applied stands.
		r.Logf("holds: txn %s: %s refused: %v", txnID, name, rr.Err)
		if r.OnRefused != nil {
			r.OnRefused(name)
		}
		return nil
	}
	r.Logf("holds: txn %s expired, coordinator says %s", txnID, name)
	if r.OnResolved != nil {
		r.OnResolved(name)
	}
	return nil
}
