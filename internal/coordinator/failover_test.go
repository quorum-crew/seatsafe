package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// blockUntilKilled returns a hook that reports the first txn to reach it and
// then holds every run that reaches it until its coordinator is closed.
func blockUntilKilled() (hook, <-chan string) {
	reached := make(chan string, 1)
	var once sync.Once
	return func(ctx context.Context, txn string) {
		once.Do(func() { reached <- txn })
		<-ctx.Done()
	}, reached
}

// replicasAgree waits until every coordinator node has applied the same log
// and holds byte-identical state.
func (c *cluster) replicasAgree() {
	c.t.Helper()
	c.eventually(20*time.Second, "coordinator replicas hold identical state", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		var first []byte
		for _, n := range c.coordN {
			snap, err := n.sm.Snapshot()
			if err != nil {
				c.t.Fatal(err)
			}
			if first == nil {
				first = snap
			} else if !bytes.Equal(first, snap) {
				return false
			}
		}
		return len(c.coordN) == len(raftIDs)
	})
}

// The leader dies after TxnDecided(COMMIT) is logged and before any Commit is
// sent. The new leader finds the decision and finishes the order: the seats
// are booked on every shard, and the client's retry gets the stored answer.
func TestLeaderKilledAfterDecision(t *testing.T) {
	c := startCluster(t, options{})
	h, reached := blockUntilKilled()
	c.after.Store(&h)
	oldLeader, _ := c.coordLeader()
	answer := make(chan *seatsafev1.BookSeatsResponse, 1)
	go func() { answer <- c.mustSubmit("t1", on(1, 0, 1), on(2, 0), on(3, 0)) }()
	<-reached
	c.after.Store(nil)
	// Decided, but nobody has been told: the seats are held, not booked.
	for _, s := range shardIDs {
		if len(c.holds(s)) != 1 {
			t.Fatalf("shard %d holds %v before the leader dies", s, c.holds(s))
		}
	}
	c.killCoord(oldLeader)

	wantResp(t, <-answer, "t1", confirmed, true)
	newLeader, _ := c.coordLeader()
	if newLeader == oldLeader {
		t.Fatal("the dead node is still leader")
	}
	c.settled()
	c.wantTaken(map[uint32][]int{1: {0, 1}, 2: {0}, 3: {0}})
	if got := c.value(decisions, "abort", SourceNewLeader); got != 0 {
		t.Fatalf("the new leader aborted %v decided txns", got)
	}
	if c.violations() != 0 {
		t.Fatal("protocol violation counted")
	}

	// The dead node comes back, replays its log and agrees with the others.
	c.startCoordNode(oldLeader)
	c.replicasAgree()
	c.mu.Lock()
	rec, ok := c.coordN[oldLeader].sm.Get("t1")
	c.mu.Unlock()
	if !ok || rec.Decision != commit || len(rec.MissingAcks()) != 0 {
		t.Fatalf("restarted node's record: %+v", rec)
	}
}

// The leader dies after TxnStarted is logged and the shards hold their seats,
// before any decision. The new leader aborts the txn and the holds are
// released.
func TestLeaderKilledBeforeDecision(t *testing.T) {
	c := startCluster(t, options{})
	h, reached := blockUntilKilled()
	c.before.Store(&h)
	oldLeader, _ := c.coordLeader()

	// One call straight to the leader, with no retry: a retry could reach
	// the new leader before it takes over and legitimately run the order
	// again. Here the client is gone, as when the router died with it.
	conn, err := grpc.NewClient(c.coordG.addrs[oldLeader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	failed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := seatsafev1.NewCoordinatorServiceClient(conn).SubmitOrder(ctx, &seatsafev1.SubmitOrderRequest{
			TxnId: "t1", UserId: "u", Participants: []*seatsafev1.ShardSeats{on(1, 0, 1), on(2, 0)}})
		failed <- err
	}()
	<-reached
	c.before.Store(nil)
	if len(c.holds(1)) != 1 || len(c.holds(2)) != 1 {
		t.Fatalf("holds before the leader dies: shard 1 %v, shard 2 %v", c.holds(1), c.holds(2))
	}
	c.killCoord(oldLeader)
	if err := <-failed; err == nil {
		t.Fatal("the dead leader answered")
	}

	c.eventually(15*time.Second, "new leader aborts the undecided txn", func() bool {
		return c.value(decisions, "abort", SourceNewLeader) == 1
	})
	c.settled()
	c.wantTaken(nil)
	if got := c.getDecision("t1"); got != abort {
		t.Fatalf("GetDecision = %v, want ABORT", got)
	}
	wantResp(t, c.mustSubmit("t1", on(1, 0, 1), on(2, 0)), "t1", aborted, true)
	if c.violations() != 0 {
		t.Fatal("protocol violation counted")
	}
	// The seats can be sold again.
	wantResp(t, c.mustSubmit("t2", on(1, 0, 1), on(2, 0)), "t2", confirmed, false)
	c.settled()
	c.wantTaken(map[uint32][]int{1: {0, 1}, 2: {0}})
}

type order struct {
	txn   string
	parts []*seatsafev1.ShardSeats
	first seatsafev1.BookingOutcome // 0 if the first call never got an answer
	final seatsafev1.BookingOutcome
}

func randomOrder(rng *rand.Rand, i, pool int) *order {
	shards := append([]uint32(nil), shardIDs...)
	rng.Shuffle(len(shards), func(a, b int) { shards[a], shards[b] = shards[b], shards[a] })
	shards = shards[:2+rng.IntN(2)]
	sort.Slice(shards, func(a, b int) bool { return shards[a] < shards[b] })
	o := &order{txn: fmt.Sprintf("order-%03d", i)}
	for _, s := range shards {
		n := rng.IntN(pool)
		ns := []int{n}
		if rng.IntN(2) == 0 {
			ns = append(ns, (n+1)%pool)
		}
		o.parts = append(o.parts, on(s, ns...))
	}
	return o
}

// 200 random cross-shard orders compete for a small pool of seats while the
// leaders of the coordinator group and of the shards are killed and
// restarted at random. Afterwards, for every txn, every participant shard
// applied the same decision, the seats taken are exactly the seats of the
// CONFIRMED orders, and no seat belongs to two of them.
func TestRandomOrdersWithLeaderKills(t *testing.T) {
	const (
		orders  = 200
		workers = 4
		pool    = 120 // seats per shard the orders fight over
	)
	c := startCluster(t, options{})
	rng := rand.New(rand.NewPCG(20261119, 7))
	all := make([]*order, orders)
	for i := range all {
		all[i] = randomOrder(rng, i, pool)
	}

	// The killer: one leader down at a time, so every group keeps a quorum.
	stop, killerDone := make(chan struct{}), make(chan struct{})
	var kills [4]atomic.Int32 // coordinator, shards 1-3
	go func() {
		defer close(killerDone)
		krng := rand.New(rand.NewPCG(99, 1))
		pause := func(lo, hi int) bool {
			select {
			case <-stop:
				return false
			case <-time.After(time.Duration(lo+krng.IntN(hi-lo)) * time.Millisecond):
				return true
			}
		}
		for pause(60, 160) {
			which := krng.IntN(4)
			var restart func()
			if which == 0 {
				id, _ := c.coordLeader()
				c.killCoord(id)
				restart = func() { c.startCoordNode(id) }
			} else {
				s := uint32(which)
				id, _ := c.shardLeader(s)
				c.killShardNode(s, id)
				restart = func() { c.startShardNode(s, id) }
			}
			kills[which].Add(1)
			pause(100, 250)
			restart()
		}
	}()

	jobs := make(chan *order)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range jobs {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				resp, err := c.submit(ctx, o.txn, o.parts...)
				cancel()
				if err == nil {
					o.first = resp.GetOutcome()
				}
			}
		}()
	}
	for _, o := range all {
		jobs <- o
	}
	close(jobs)
	wg.Wait()
	close(stop)
	<-killerDone
	c.coordLeader()
	for _, s := range shardIDs {
		c.shardLeader(s)
	}

	// Every order has a final, stored answer, and it is the one given first.
	count := map[seatsafev1.BookingOutcome]int{}
	unanswered := 0
	for _, o := range all {
		resp := c.mustSubmit(o.txn, o.parts...)
		o.final = resp.GetOutcome()
		count[o.final]++
		switch {
		case o.first == 0:
			unanswered++
		case o.first != o.final:
			t.Fatalf("%s: first answer %v, later %v", o.txn, o.first, o.final)
		case !resp.GetReplayed():
			t.Fatalf("%s: answered twice and the second is not marked replayed", o.txn)
		}
		if o.final != confirmed && o.final != unavailable && o.final != aborted {
			t.Fatalf("%s: outcome %v", o.txn, o.final)
		}
	}
	c.settled()
	c.replicasAgree()

	// Zero double bookings, and the shards hold exactly the CONFIRMED seats:
	// a seat less would be a half-committed order, a seat more a
	// half-aborted one.
	owner := map[string]string{}
	want := map[uint32][]int{}
	for _, o := range all {
		if o.final != confirmed {
			continue
		}
		for _, p := range o.parts {
			for _, s := range p.GetSeats() {
				key := fmt.Sprintf("%d/%s", p.GetShardId(), s.GetSeatId())
				if prev, dup := owner[key]; dup {
					t.Fatalf("seat %s CONFIRMED for %s and %s", key, prev, o.txn)
				}
				owner[key] = o.txn
				var n int
				fmt.Sscanf(s.GetSeatId(), "S%d", &n)
				want[p.GetShardId()] = append(want[p.GetShardId()], n)
			}
		}
	}
	c.wantTaken(want)

	// Every participant acked the one decision in the coordinator's log, and
	// none refused it.
	_, leader := c.coordLeader()
	for _, o := range all {
		rec, ok := leader.sm.Get(o.txn)
		if !ok || !rec.Decided() || len(rec.MissingAcks()) != 0 {
			t.Fatalf("%s: record %+v", o.txn, rec)
		}
		if (rec.Decision == commit) != (o.final == confirmed) || rec.Outcome != o.final {
			t.Fatalf("%s: logged %v/%v, answered %v", o.txn, rec.Decision, rec.Outcome, o.final)
		}
	}
	if v := c.violations(); v != 0 {
		t.Fatalf("%v protocol violations:\n%s", v, c.logText())
	}
	// Every replica of every shard ends with the same seats.
	c.eventually(20*time.Second, "shard replicas agree", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for s, nodes := range c.shardN {
			var first string
			for _, n := range nodes {
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

	t.Logf("%d orders: %d CONFIRMED, %d SEAT_UNAVAILABLE, %d ABORTED; %d first calls unanswered; kills: coordinator %d, shards %d/%d/%d; new-leader aborts %v",
		orders, count[confirmed], count[unavailable], count[aborted], unanswered,
		kills[0].Load(), kills[1].Load(), kills[2].Load(), kills[3].Load(), c.value(decisions, "abort", SourceNewLeader))
	if count[confirmed] == 0 || count[unavailable] == 0 {
		t.Fatal("the orders did not both commit and collide")
	}
	if kills[0].Load() == 0 || kills[1].Load()+kills[2].Load()+kills[3].Load() == 0 {
		t.Fatal("no coordinator leader or no shard leader was killed during the run")
	}
}
