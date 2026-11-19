package coordinator

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/coordsm"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

func submitTotal(m *Metrics) *metrics.CounterVec { return m.Submit }
func decisions(m *Metrics) *metrics.CounterVec   { return m.Decisions }
func retries(m *Metrics) *metrics.CounterVec     { return m.Retries }

// Every shard votes YES: the order commits on both shards.
func TestAllYesCommits(t *testing.T) {
	c := startCluster(t, options{})
	resp := c.mustSubmit("t1", on(1, 0, 1), on(3, 5))
	wantResp(t, resp, "t1", confirmed, false)
	c.settled()
	c.wantTaken(map[uint32][]int{1: {0, 1}, 3: {5}})

	_, leader := c.coordLeader()
	rec, ok := leader.sm.Get("t1")
	if !ok || rec.Decision != commit || len(rec.MissingAcks()) != 0 || rec.User != "user-t1" {
		t.Fatalf("record %+v", rec)
	}
	if got := c.getDecision("t1"); got != commit {
		t.Fatalf("GetDecision = %v, want COMMIT", got)
	}
	if c.value(submitTotal, "CONFIRMED") != 1 || c.value(decisions, "commit", SourceVotes) != 1 || c.violations() != 0 {
		t.Fatalf("metrics: submit %v, decisions %v, violations %v", c.value(submitTotal, "CONFIRMED"), c.value(decisions, "commit", SourceVotes), c.violations())
	}
	if n := leader.coord.Metrics.Prepare.Count(); n != 1 {
		t.Fatalf("prepare histogram has %d observations", n)
	}
	if leader.coord.Metrics.Decide.Count() != 1 || leader.coord.Metrics.Total.Count("CONFIRMED") != 1 {
		t.Fatal("decide or total latency not observed")
	}
	var buf bytes.Buffer
	leader.coord.WriteMetrics(&buf)
	for _, want := range []string{"seatsafe_coordinator_inflight_txns 0", `seatsafe_coordinator_txn_records{state="all"} 1`, `seatsafe_coordinator_txn_records{state="unacked"} 0`} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("gauges:\n%s\nwant %q", buf.String(), want)
		}
	}
}

// One shard votes NO over a taken seat: SEAT_UNAVAILABLE with that seat, and
// the hold on the shard that voted YES is released.
func TestOneNoWithConflicts(t *testing.T) {
	c := startCluster(t, options{})
	c.bookDirect("single-1", 2, 7)
	resp := c.mustSubmit("t1", on(1, 0, 1), on(2, 6, 7), on(3, 9))
	wantResp(t, resp, "t1", unavailable, false, seat(2, 7))
	c.settled()
	c.wantTaken(map[uint32][]int{2: {7}}) // only the direct booking
	if got := c.getDecision("t1"); got != abort {
		t.Fatalf("GetDecision = %v, want ABORT", got)
	}
	if c.value(decisions, "abort", SourceVotes) != 1 || c.violations() != 0 {
		t.Fatal("decision not counted, or a violation was")
	}
	// The seats are free again for the next order.
	wantResp(t, c.mustSubmit("t2", on(1, 0, 1), on(3, 9)), "t2", confirmed, false)
	c.settled()
	c.wantTaken(map[uint32][]int{1: {0, 1}, 2: {7}, 3: {9}})
}

// A shard that cannot answer within the prepare deadline aborts the order.
// The Abort reaches it once it is back, and until then it is retried.
func TestShardTimeoutAborts(t *testing.T) {
	c := startCluster(t, options{})
	// Shard 2 loses its quorum: nobody there can vote.
	c.killShardNode(2, 2)
	c.killShardNode(2, 3)
	start := time.Now()
	resp := c.mustSubmit("t1", on(1, 3), on(2, 3))
	wantResp(t, resp, "t1", aborted, false)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("abort took %v", d)
	}
	// Shard 1 hears the Abort; shard 2 cannot yet.
	c.eventually(10*time.Second, "shard 1's hold released", func() bool { return len(c.holds(1)) == 0 && len(c.taken(1)) == 0 })
	c.eventually(10*time.Second, "Abort to shard 2 retried", func() bool { return c.value(retries, "abort") >= 1 })
	_, leader := c.coordLeader()
	if rec, _ := leader.sm.Get("t1"); len(rec.MissingAcks()) != 1 || rec.MissingAcks()[0] != 2 {
		t.Fatalf("missing acks %v, want shard 2 only", rec.MissingAcks())
	}

	c.startShardNode(2, 2)
	c.startShardNode(2, 3)
	c.settled()
	c.wantTaken(nil)
	// The tombstone on shard 2 makes a late Prepare of t1 vote NO: retrying
	// the order returns the stored ABORTED, and the seats stay free.
	wantResp(t, c.mustSubmit("t1", on(1, 3), on(2, 3)), "t1", aborted, true)
	c.wantTaken(nil)
	if c.violations() != 0 {
		t.Fatal("a late Abort was counted as a protocol violation")
	}
}

// A retry of a finished order returns the identical answer, replayed.
func TestRetryReplaysTheAnswer(t *testing.T) {
	c := startCluster(t, options{})
	c.bookDirect("single-1", 1, 4)
	c.bookDirect("single-2", 3, 8)
	orders := map[string][]*seatsafev1.ShardSeats{
		"ok":   {on(1, 0), on(2, 0)},
		"full": {on(1, 4, 5), on(2, 1), on(3, 8)},
	}
	first := map[string]*seatsafev1.BookSeatsResponse{
		"ok":   c.mustSubmit("ok", orders["ok"]...),
		"full": c.mustSubmit("full", orders["full"]...),
	}
	wantResp(t, first["ok"], "ok", confirmed, false)
	wantResp(t, first["full"], "full", unavailable, false, seat(1, 4), seat(3, 8))
	c.settled()
	for range 3 {
		for txn, parts := range orders {
			again := c.mustSubmit(txn, parts...)
			wantResp(t, again, txn, first[txn].GetOutcome(), true, first[txn].GetUnavailableSeats()...)
		}
	}
	// Even with other seats in the request: the txn id decides.
	wantResp(t, c.mustSubmit("ok", on(1, 50), on(3, 50)), "ok", confirmed, true)
	c.settled()
	c.wantTaken(map[uint32][]int{1: {0, 4}, 2: {0}, 3: {8}})
}

// A second SubmitOrder for an order still being run waits for that run and
// gets the same answer; the order is prepared and decided once.
func TestRetryWaitsForTheRunInProgress(t *testing.T) {
	c := startCluster(t, options{})
	reached, release := make(chan struct{}), make(chan struct{})
	h := hook(func(ctx context.Context, _ string) {
		close(reached)
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	c.before.Store(&h)
	answers := make(chan *seatsafev1.BookSeatsResponse, 2)
	go func() { answers <- c.mustSubmit("t1", on(1, 0), on(2, 0)) }()
	<-reached
	c.before.Store(nil)
	go func() { answers <- c.mustSubmit("t1", on(1, 0), on(2, 0)) }()
	_, leader := c.coordLeader()
	var buf bytes.Buffer
	leader.coord.WriteMetrics(&buf)
	if !strings.Contains(buf.String(), "seatsafe_coordinator_inflight_txns 1") {
		t.Fatalf("in-flight gauge:\n%s", buf.String())
	}
	select {
	case r := <-answers:
		t.Fatalf("answered %v while the order was still being decided", r)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	for range 2 {
		wantResp(t, <-answers, "t1", confirmed, false)
	}
	c.settled()
	if got := c.value(decisions, "commit", SourceVotes); got != 1 {
		t.Fatalf("%v decisions logged for one order", got)
	}
}

// GetDecision arrives before the coordinator has decided: ABORT is logged
// first, the COMMIT the votes ask for loses, SubmitOrder answers ABORTED and
// no seat is booked.
func TestGetDecisionBeforeVotes(t *testing.T) {
	c := startCluster(t, options{})
	reached, release := make(chan struct{}), make(chan struct{})
	h := hook(func(ctx context.Context, _ string) {
		close(reached)
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	c.before.Store(&h)
	answer := make(chan *seatsafev1.BookSeatsResponse, 1)
	go func() { answer <- c.mustSubmit("t1", on(1, 0, 1), on(2, 0)) }()
	<-reached // every shard has voted YES and holds its seats
	if len(c.holds(1)) != 1 || len(c.holds(2)) != 1 {
		t.Fatalf("holds before the decision: shard 1 %v, shard 2 %v", c.holds(1), c.holds(2))
	}
	if got := c.getDecision("t1"); got != abort {
		t.Fatalf("GetDecision = %v, want ABORT", got)
	}
	close(release)
	wantResp(t, <-answer, "t1", aborted, false)
	c.settled()
	c.wantTaken(nil)
	if c.value(decisions, "abort", SourceGetDecision) != 1 || c.value(decisions, "commit", SourceVotes) != 0 {
		t.Fatalf("decisions: get_decision abort %v, votes commit %v", c.value(decisions, "abort", SourceGetDecision), c.value(decisions, "commit", SourceVotes))
	}
	if got := c.getDecision("t1"); got != abort {
		t.Fatalf("GetDecision later = %v, want ABORT", got)
	}
	wantResp(t, c.mustSubmit("t1", on(1, 0, 1), on(2, 0)), "t1", aborted, true)
	if c.violations() != 0 {
		t.Fatal("protocol violation counted")
	}
}

// GetDecision for a txn the coordinator has never seen logs ABORT, so an
// order with that id arriving later cannot commit.
func TestGetDecisionUnknownTxn(t *testing.T) {
	c := startCluster(t, options{})
	if got := c.getDecision("ghost"); got != abort {
		t.Fatalf("GetDecision = %v, want ABORT", got)
	}
	_, leader := c.coordLeader()
	if rec, ok := leader.sm.Get("ghost"); !ok || rec.Decision != abort || len(rec.Participants) != 0 {
		t.Fatalf("record %+v", rec)
	}
	wantResp(t, c.mustSubmit("ghost", on(1, 0), on(2, 0)), "ghost", aborted, true)
	c.settled()
	c.wantTaken(nil)
}

// A follower redirects with the leader's address in the trailer; a node that
// knows no leader answers UNAVAILABLE. Both RPCs follow the same contract.
func TestFollowerAndNoLeader(t *testing.T) {
	c := startCluster(t, options{})
	leaderID, _ := c.coordLeader()
	follower := leaderID%3 + 1
	conn, err := grpc.NewClient(c.coordG.addrs[follower], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := seatsafev1.NewCoordinatorServiceClient(conn)
	call := func(name string) (metadata.MD, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var trailer metadata.MD
		var err error
		if name == "SubmitOrder" {
			_, err = client.SubmitOrder(ctx, &seatsafev1.SubmitOrderRequest{TxnId: "t1", Participants: []*seatsafev1.ShardSeats{on(1, 0), on(2, 0)}}, grpc.Trailer(&trailer))
		} else {
			_, err = client.GetDecision(ctx, &seatsafev1.GetDecisionRequest{TxnId: "t1"}, grpc.Trailer(&trailer))
		}
		return trailer, err
	}
	for _, rpc := range []string{"SubmitOrder", "GetDecision"} {
		trailer, err := call(rpc)
		wantCode(t, err, codes.FailedPrecondition)
		if got := trailer.Get(groupclient.LeaderTrailer); len(got) != 1 || got[0] != c.coordG.addrs[leaderID] {
			t.Fatalf("%s on a follower: trailer %v, want %s", rpc, got, c.coordG.addrs[leaderID])
		}
	}

	// Cut the follower off: after an election timeout it knows no leader.
	c.coordG.net.Isolate(follower)
	c.mu.Lock()
	node := c.coordN[follower].node
	c.mu.Unlock()
	c.eventually(10*time.Second, "isolated node forgets its leader", func() bool { return node.Status().Leader == 0 })
	for _, rpc := range []string{"SubmitOrder", "GetDecision"} {
		trailer, err := call(rpc)
		wantCode(t, err, codes.Unavailable)
		if len(trailer.Get(groupclient.LeaderTrailer)) != 0 {
			t.Fatalf("%s with no leader set a leader trailer", rpc)
		}
	}
	c.coordG.net.Heal()
	// Nothing was started by any of those calls.
	_, leader := c.coordLeader()
	if _, ok := leader.sm.Get("t1"); ok {
		t.Fatal("a call on a non-leader created a txn")
	}
	c.wantTaken(nil)
}

func TestSubmitOrderValidation(t *testing.T) {
	c := startCluster(t, options{})
	_, leader := c.coordLeader()
	svc := service{c: leader.coord}
	empty := on(2)
	for name, req := range map[string]*seatsafev1.SubmitOrderRequest{
		"no txn id":           {Participants: []*seatsafev1.ShardSeats{on(1, 0), on(2, 0)}},
		"no participants":     {TxnId: "x"},
		"unsorted":            {TxnId: "x", Participants: []*seatsafev1.ShardSeats{on(2, 0), on(1, 0)}},
		"shard twice":         {TxnId: "x", Participants: []*seatsafev1.ShardSeats{on(1, 0), on(1, 1)}},
		"unknown shard":       {TxnId: "x", Participants: []*seatsafev1.ShardSeats{on(1, 0), on(9, 0)}},
		"shard with no seats": {TxnId: "x", Participants: []*seatsafev1.ShardSeats{on(1, 0), empty}},
	} {
		_, err := svc.SubmitOrder(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s: error %v, want INVALID_ARGUMENT", name, err)
		}
	}
	_, err := svc.GetDecision(context.Background(), &seatsafev1.GetDecisionRequest{})
	wantCode(t, err, codes.InvalidArgument)
	if total, _, _ := leader.sm.Counts(); total != 0 {
		t.Fatalf("%d txns created by invalid requests", total)
	}
	if got := c.value(submitTotal, "INVALID_ARGUMENT"); got != 6 {
		t.Fatalf("submit_total{INVALID_ARGUMENT} = %v, want 6", got)
	}
}

// A NO without conflicts (here: an unknown seat) is a failed order, ABORTED,
// not SEAT_UNAVAILABLE.
func TestNoWithoutConflictsAborts(t *testing.T) {
	c := startCluster(t, options{})
	nope := &seatsafev1.ShardSeats{ShardId: 2, Seats: []*seatsafev1.SeatRef{{EventId: event, SectionId: "sec-2", SeatId: "nope"}}}
	wantResp(t, c.mustSubmit("t1", on(1, 0), nope), "t1", aborted, false)
	c.settled()
	c.wantTaken(nil)
}

// A participant that refuses the logged decision contradicts the
// coordinator's log. It is counted and logged, and not retried; the other
// participants still get the decision.
func TestProtocolViolationIsCountedNotRetried(t *testing.T) {
	c := startCluster(t, options{})
	h := hook(func(ctx context.Context, txn string) {
		// After shard 1 voted YES, something aborts the txn there behind the
		// coordinator's back. No correct node does this.
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		err := c.shards[1].Call(cctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
			_, err := seatsafev1.NewParticipantServiceClient(cc).Abort(ctx, &seatsafev1.DecisionRequest{TxnId: txn}, opts...)
			return err
		})
		if err != nil {
			t.Errorf("rogue Abort: %v", err)
		}
	})
	c.before.Store(&h)
	wantResp(t, c.mustSubmit("t1", on(1, 0), on(2, 0)), "t1", confirmed, false)
	c.before.Store(nil)
	violated := func() float64 {
		return c.value(func(m *Metrics) *metrics.CounterVec { return m.Violations }, "1", "commit")
	}
	c.eventually(10*time.Second, "violation counted", func() bool { return violated() == 1 })
	_, leader := c.coordLeader()
	c.eventually(10*time.Second, "shard 2 acked", func() bool {
		rec, _ := leader.sm.Get("t1")
		return len(rec.MissingAcks()) == 1 && rec.MissingAcks()[0] == 1
	})
	time.Sleep(300 * time.Millisecond)
	if violated() != 1 || c.value(retries, "commit") != 0 {
		t.Fatalf("refused Commit was retried: %v violations, %v retries", violated(), c.value(retries, "commit"))
	}
	if !strings.Contains(c.logText(), "PROTOCOL VIOLATION: txn t1: shard 1 refused commit") {
		t.Fatalf("violation not logged:\n%s", c.logText())
	}
	// The txn is never collected: it is the evidence.
	c.wantTaken(map[uint32][]int{2: {0}})
}

// The leader collects finished txns after the retention time, and aborts an
// undecided txn that nobody is running.
func TestGcAndSweep(t *testing.T) {
	c := startCluster(t, options{gcInterval: 30 * time.Millisecond, gcRetention: 150 * time.Millisecond, stuckAfter: 200 * time.Millisecond})
	wantResp(t, c.mustSubmit("done", on(1, 0), on(2, 0)), "done", confirmed, false)
	_, leader := c.coordLeader()

	// A TxnStarted that was logged but never run, as when SubmitOrder's
	// caller gave up before the proposal was applied.
	res, err := leader.coord.propose(context.Background(), &seatsafev1.CoordCommand{Op: &seatsafev1.CoordCommand_Started{Started: &seatsafev1.TxnStarted{
		TxnId: "orphan", Participants: []*seatsafev1.ShardSeats{on(1, 9), on(3, 9)},
	}}})
	if err != nil || !res.Created {
		t.Fatalf("TxnStarted: %+v, %v", res, err)
	}
	c.eventually(10*time.Second, "orphan aborted by the sweep", func() bool {
		return c.value(decisions, "abort", SourceSweep) == 1
	})
	c.eventually(10*time.Second, "both txns collected", func() bool {
		total, _, _ := leader.sm.Counts()
		return total == 0
	})
	c.settled()
	c.wantTaken(map[uint32][]int{1: {0}, 2: {0}})
	// Every replica dropped the same records.
	c.eventually(10*time.Second, "followers collected too", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, n := range c.coordN {
			if total, _, _ := n.sm.Counts(); total != 0 {
				return false
			}
		}
		return true
	})
}

func TestNewRejectsIncompleteConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New accepted a config without a node")
	}
	if _, err := New(Config{Node: &raftnode.Node{}, SM: coordsm.New()}); err == nil {
		t.Fatal("New accepted a config without shards")
	}
}

func TestDecideRule(t *testing.T) {
	yes := vote{resp: &seatsafev1.PrepareResponse{Vote: seatsafev1.Vote_VOTE_YES}}
	no := func(conflicts ...*seatsafev1.SeatRef) vote {
		return vote{resp: &seatsafev1.PrepareResponse{Vote: seatsafev1.Vote_VOTE_NO, Conflicts: conflicts}}
	}
	timeout := vote{err: context.DeadlineExceeded}
	txn := &coordsm.Txn{ID: "t"}
	for name, c := range map[string]struct {
		votes     []vote
		decision  seatsafev1.Decision
		outcome   seatsafev1.BookingOutcome
		conflicts int
	}{
		"all yes":                   {[]vote{yes, yes, yes}, commit, confirmed, 0},
		"one no with conflicts":     {[]vote{yes, no(seat(2, 1))}, abort, unavailable, 1},
		"conflict beats a timeout":  {[]vote{timeout, no(seat(2, 1), seat(2, 2)), yes}, abort, unavailable, 2},
		"conflicts of every no":     {[]vote{no(seat(1, 1)), no(seat(2, 1))}, abort, unavailable, 2},
		"same seat named twice":     {[]vote{no(seat(1, 1)), no(seat(1, 1))}, abort, unavailable, 1},
		"timeout":                   {[]vote{yes, timeout}, abort, aborted, 0},
		"no without conflicts":      {[]vote{yes, no()}, abort, aborted, 0},
		"vote missing from a reply": {[]vote{yes, {resp: &seatsafev1.PrepareResponse{}}}, abort, aborted, 0},
	} {
		d := decide(txn, c.votes)
		if d.GetTxnId() != "t" || d.GetDecision() != c.decision || d.GetOutcome() != c.outcome || len(d.GetUnavailableSeats()) != c.conflicts {
			t.Errorf("%s: decided %v", name, d)
		}
	}
}

func TestLabels(t *testing.T) {
	for c, want := range map[codes.Code]string{codes.OK: "OK", codes.DeadlineExceeded: "DEADLINE_EXCEEDED", codes.Unavailable: "UNAVAILABLE", codes.FailedPrecondition: "FAILED_PRECONDITION"} {
		if got := codeLabel(c); got != want {
			t.Errorf("codeLabel(%v) = %q, want %q", c, got, want)
		}
	}
	if got := outcomeLabel(unavailable); got != "SEAT_UNAVAILABLE" {
		t.Errorf("outcomeLabel = %q", got)
	}
	if decisionLabel(commit) != "commit" || decisionLabel(abort) != "abort" {
		t.Error("decisionLabel")
	}
}
