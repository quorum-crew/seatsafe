package coordinator

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// End to end with internal/shard's HoldResolver on every storage node: what
// happens to a hold whose decision does not arrive in time.

const testHoldTTL = time.Second

// shardsAgree waits until every replica of every shard has the same free
// seats as its leader and holds nothing.
func (c *cluster) shardsAgree() {
	c.t.Helper()
	c.eventually(20*time.Second, "every shard replica agrees and holds nothing", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for s, nodes := range c.shardN {
			var first string
			for _, n := range nodes {
				if len(n.sm.ExpiredHolds(1<<62)) != 0 {
					return false
				}
				got := fmt.Sprint(n.sm.ListAvailable(event, fmt.Sprintf("sec-%d", s)))
				if first == "" {
					first = got
				} else if got != first {
					return false
				}
			}
		}
		return true
	})
}

// A run is stopped after every shard voted YES, past the holds' TTL. The
// shard leaders' resolvers ask GetDecision, which logs ABORT; the holds are
// released. When the run carries on, the COMMIT its votes ask for loses to
// that ABORT, and SubmitOrder answers ABORTED: no seat is booked anywhere.
func TestExpiredHoldResolvedByGetDecision(t *testing.T) {
	c := startCluster(t, options{holdTTL: testHoldTTL, resolveHolds: true})
	reached, release := make(chan string, 16), make(chan struct{})
	h := hook(func(ctx context.Context, txn string) {
		reached <- txn
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	c.before.Store(&h)
	answer := make(chan *seatsafev1.BookSeatsResponse, 1)
	go func() { answer <- c.mustSubmit("t1", on(1, 0, 1), on(2, 0), on(3, 0)) }()
	c.await(reached, "t1 has its votes")
	c.before.Store(nil)
	for _, s := range shardIDs {
		if got := c.holds(s); !slices.Equal(got, []string{"t1"}) {
			t.Fatalf("shard %d holds %v after voting YES", s, got)
		}
	}
	_, runner := c.running("t1")

	// Past the TTL the resolvers ask, and GetDecision logs ABORT.
	start := time.Now()
	c.eventually(15*time.Second, "GetDecision logged ABORT for t1", func() bool {
		rec, ok := runner.sm.Get("t1")
		return ok && rec.Decision == abort
	})
	if d := time.Since(start); d < testHoldTTL/2 {
		t.Fatalf("decided %v after the votes, before the holds expired", d)
	}
	c.eventually(15*time.Second, "every shard released t1's holds", func() bool {
		for _, s := range shardIDs {
			if len(c.holds(s)) != 0 || len(c.taken(s)) != 0 {
				return false
			}
		}
		return true
	})

	// The run carries on: its COMMIT loses.
	close(release)
	select {
	case resp := <-answer:
		wantResp(t, resp, "t1", aborted, false)
	case <-time.After(20 * time.Second):
		t.Fatalf("SubmitOrder never answered\ncoordinator log:\n%s", c.logText())
	}
	c.settled()
	c.shardsAgree()
	c.wantTaken(nil)
	if got := c.value(decisions, "abort", SourceGetDecision); got != 1 {
		t.Fatalf("decisions{abort, get_decision} = %v, want 1", got)
	}
	if got := c.value(decisions, "commit", SourceVotes); got != 0 {
		t.Fatalf("decisions{commit, votes} = %v: the losing COMMIT was counted", got)
	}
	if got := c.resolved.Value("abort"); got < 1 {
		t.Fatalf("expired_holds_resolved_total{abort} = %v, want >= 1", got)
	}
	if got := c.resolved.Value("commit"); got != 0 {
		t.Fatalf("expired_holds_resolved_total{commit} = %v", got)
	}
	if got := c.refused.Value("abort") + c.refused.Value("commit"); got != 0 {
		t.Fatalf("a shard refused %v decisions", got)
	}
	if v := c.violations(); v != 0 {
		t.Fatalf("%v protocol violations", v)
	}
	// The stored answer is the ABORT.
	wantResp(t, c.mustSubmit("t1", on(1, 0, 1), on(2, 0), on(3, 0)), "t1", aborted, true)
	if got := c.getDecision("t1"); got != abort {
		t.Fatalf("GetDecision = %v, want ABORT", got)
	}
}

// All three coordinator nodes stop after Prepare. The shards cannot get an
// answer, so they keep the seats HELD however long it takes: releasing them
// unilaterally could sell a seat the coordinator committed. Once the
// coordinators are back, the holds are resolved within seconds and every
// replica of every shard agrees.
func TestHoldsSurviveCoordinatorOutage(t *testing.T) {
	c := startCluster(t, options{holdTTL: testHoldTTL, resolveHolds: true, durable: true})
	h, reached := blockUntilKilled()
	c.before.Store(&h)

	// One call straight to the coordinator leader, cancelled once the group
	// is down. A client retrying after the restart could run the order again
	// and legitimately commit it, which is not what this test is about.
	leader, _ := c.coordLeader()
	conn, err := grpc.NewClient(c.coordG.addrs[leader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := seatsafev1.NewCoordinatorServiceClient(conn).SubmitOrder(ctx, &seatsafev1.SubmitOrderRequest{
			TxnId: "t1", UserId: "u", Participants: []*seatsafev1.ShardSeats{on(1, 0, 1), on(3, 0)}})
		done <- err
	}()
	c.await(reached, "t1 has its votes")
	c.before.Store(nil)
	for _, id := range raftIDs {
		c.killCoord(id)
	}
	cancel()
	<-done
	if !slices.Equal(c.holds(1), []string{"t1"}) || !slices.Equal(c.holds(3), []string{"t1"}) {
		t.Fatalf("holds once the coordinators are down: shard 1 %v, shard 3 %v", c.holds(1), c.holds(3))
	}

	// Three TTLs without an answer: the resolvers keep asking and failing,
	// and the seats stay HELD on every replica.
	time.Sleep(3 * testHoldTTL)
	c.mu.Lock()
	for _, s := range []uint32{1, 3} {
		for id, n := range c.shardN[s] {
			if got := n.sm.ExpiredHolds(1 << 62); !slices.Equal(got, []string{"t1"}) {
				c.mu.Unlock()
				t.Fatalf("shard %d node %d holds %v %v after the TTL with no coordinator, want t1", s, id, got, 3*testHoldTTL)
			}
		}
	}
	c.mu.Unlock()
	c.wantTaken(map[uint32][]int{1: {0, 1}, 3: {0}})
	if got := c.resolved.Value("abort") + c.resolved.Value("commit"); got != 0 {
		t.Fatalf("%v holds resolved while no coordinator could answer", got)
	}

	// The coordinators come back: the new leader aborts t1 (or a resolver's
	// GetDecision does first), and the holds go.
	for _, id := range raftIDs {
		c.startCoordNode(id)
	}
	back := time.Now()
	c.eventually(10*time.Second, "holds released after the coordinators came back", func() bool {
		return len(c.holds(1)) == 0 && len(c.holds(3)) == 0
	})
	t.Logf("holds released %v after the coordinators restarted", time.Since(back).Round(10*time.Millisecond))
	c.settled()
	c.shardsAgree()
	c.wantTaken(nil)
	if got := c.value(decisions, "abort", SourceNewLeader) + c.value(decisions, "abort", SourceGetDecision); got != 1 {
		t.Fatalf("%v ABORTs logged for t1 by a new leader or GetDecision, want 1", got)
	}
	if got := c.getDecision("t1"); got != abort {
		t.Fatalf("GetDecision = %v, want ABORT", got)
	}
	if v := c.violations(); v != 0 {
		t.Fatalf("%v protocol violations", v)
	}
	if got := c.refused.Value("abort") + c.refused.Value("commit"); got != 0 {
		t.Fatalf("a shard refused %v decisions", got)
	}
}
