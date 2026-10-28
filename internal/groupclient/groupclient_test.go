package groupclient

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/metrics"
)

// node is a scriptable server: book decides each Book answer, status each
// Status answer (nil = error).
type node struct {
	seatsafev1.UnimplementedShardServiceServer
	seatsafev1.UnimplementedNodeServiceServer
	addr   string
	mu     sync.Mutex
	book   func(ctx context.Context) error
	status func() *seatsafev1.NodeStatus
	calls  atomic.Int32
	stats  atomic.Int32
}

func (n *node) Book(ctx context.Context, _ *seatsafev1.ShardBookRequest) (*seatsafev1.BookSeatsResponse, error) {
	n.calls.Add(1)
	n.mu.Lock()
	f := n.book
	n.mu.Unlock()
	if err := f(ctx); err != nil {
		return nil, err
	}
	return &seatsafev1.BookSeatsResponse{OrderId: n.addr}, nil
}

func (n *node) Status(context.Context, *seatsafev1.StatusRequest) (*seatsafev1.NodeStatus, error) {
	n.stats.Add(1)
	n.mu.Lock()
	f := n.status
	n.mu.Unlock()
	if st := f(); st != nil {
		return st, nil
	}
	return nil, status.Error(codes.Unavailable, "down")
}

func (n *node) set(book func(ctx context.Context) error, st func() *seatsafev1.NodeStatus) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if book != nil {
		n.book = book
	}
	if st != nil {
		n.status = st
	}
}

func ok(context.Context) error { return nil }

func refuse(code codes.Code) func(context.Context) error {
	return func(context.Context) error { return status.Error(code, code.String()) }
}

func redirectTo(addr string) func(context.Context) error {
	return func(ctx context.Context) error {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderTrailer, addr))
		return status.Error(codes.FailedPrecondition, "not leader")
	}
}

func role(r seatsafev1.RaftRole, term uint64, leader string) func() *seatsafev1.NodeStatus {
	return func() *seatsafev1.NodeStatus { return &seatsafev1.NodeStatus{Role: r, Term: term, LeaderAddr: leader} }
}

const (
	leaderRole   = seatsafev1.RaftRole_RAFT_ROLE_LEADER
	followerRole = seatsafev1.RaftRole_RAFT_ROLE_FOLLOWER
)

type testGroup struct {
	*Group
	nodes []*node
	m     *Metrics
}

// newTestGroup starts n scriptable nodes, all answering OK and reporting
// FOLLOWER with no leader, and a Group over them.
func newTestGroup(t *testing.T, n int, cfg Config) *testGroup {
	t.Helper()
	listeners := map[string]*bufconn.Listener{}
	var addrs []string
	var nodes []*node
	for i := 0; i < n; i++ {
		a := fmt.Sprintf("n%d:7000", i)
		addrs = append(addrs, a)
		nd := &node{addr: a, book: ok, status: role(followerRole, 1, "")}
		nodes = append(nodes, nd)
		lis := bufconn.Listen(1 << 16)
		listeners[a] = lis
		s := grpc.NewServer()
		seatsafev1.RegisterShardServiceServer(s, nd)
		seatsafev1.RegisterNodeServiceServer(s, nd)
		go func() { _ = s.Serve(lis) }()
		t.Cleanup(s.Stop)
	}
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		if lis, ok := listeners[addr]; ok {
			return lis.DialContext(ctx)
		}
		return nil, fmt.Errorf("no node %s", addr)
	}))
	if len(cfg.Backoff) == 0 {
		cfg.Backoff = []time.Duration{time.Millisecond}
	}
	m := NewMetrics(metrics.NewRegistry())
	g, err := New("g", addrs, cfg, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return &testGroup{g, nodes, m}
}

// book calls Book and returns which node answered.
func (g *testGroup) book(ctx context.Context) (string, error) {
	var resp *seatsafev1.BookSeatsResponse
	err := g.Call(ctx, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		resp, err = seatsafev1.NewShardServiceClient(cc).Book(ctx, &seatsafev1.ShardBookRequest{RequestId: "r"}, opts...)
		return err
	})
	return resp.GetOrderId(), err
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestDiscoversLeaderFirst(t *testing.T) {
	g := newTestGroup(t, 3, Config{})
	g.nodes[0].set(redirectTo("n2:7000"), nil)
	g.nodes[2].set(nil, role(leaderRole, 3, "n2:7000"))
	got, err := g.book(ctxFor(t, time.Second))
	if err != nil || got != "n2:7000" {
		t.Fatalf("answered by %q, %v", got, err)
	}
	if g.Leader() != "n2:7000" || g.nodes[0].calls.Load() != 0 {
		t.Fatalf("leader %q, n0 calls %d", g.Leader(), g.nodes[0].calls.Load())
	}
	if g.m.LeaderChanges.Value("g") != 0 || g.m.Retries.Value("g", ReasonRedirect) != 0 {
		t.Fatal("first discovery counted as a change or retry")
	}
}

func TestRedirectOnce(t *testing.T) {
	g := newTestGroup(t, 3, Config{})
	g.nodes[0].set(nil, role(leaderRole, 1, ""))
	if _, err := g.book(ctxFor(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	// Leadership moves to n1; n0 points there.
	g.nodes[0].set(redirectTo("n1:7000"), role(followerRole, 2, "n1:7000"))
	g.nodes[1].set(nil, role(leaderRole, 2, ""))
	got, err := g.book(ctxFor(t, time.Second))
	if err != nil || got != "n1:7000" {
		t.Fatalf("answered by %q, %v", got, err)
	}
	if r := g.m.Retries.Value("g", ReasonRedirect); r != 1 {
		t.Fatalf("redirects = %v", r)
	}
	if g.m.LeaderChanges.Value("g") != 1 || g.nodes[0].stats.Load() != 1 {
		t.Fatalf("changes %v, n0 status calls %d (want only the first discovery)", g.m.LeaderChanges.Value("g"), g.nodes[0].stats.Load())
	}
	if g.m.LeaderRecovery.Count("g") != 1 {
		t.Fatal("recovery after a redirect not recorded")
	}
}

func TestSecondRedirectFallsBackToDiscovery(t *testing.T) {
	g := newTestGroup(t, 3, Config{})
	g.nodes[0].set(redirectTo("n1:7000"), role(leaderRole, 1, ""))
	g.nodes[1].set(redirectTo("n0:7000"), nil) // a stale ping-pong
	g.nodes[2].set(nil, nil)
	// After the one redirect, discovery runs: n0 still claims LEADER at term 1,
	// n2 claims LEADER at term 5 and wins.
	var flips atomic.Int32
	g.nodes[2].set(nil, func() *seatsafev1.NodeStatus {
		if flips.Add(1) > 1 {
			return &seatsafev1.NodeStatus{Role: leaderRole, Term: 5}
		}
		return &seatsafev1.NodeStatus{Role: followerRole, Term: 1}
	})
	got, err := g.book(ctxFor(t, time.Second))
	if err != nil || got != "n2:7000" {
		t.Fatalf("answered by %q, %v", got, err)
	}
	if g.m.Retries.Value("g", ReasonRedirect) != 1 || g.m.Retries.Value("g", ReasonNotLeader) != 1 {
		t.Fatalf("redirect %v, not_leader %v", g.m.Retries.Value("g", ReasonRedirect), g.m.Retries.Value("g", ReasonNotLeader))
	}
}

func TestUnusableTrailer(t *testing.T) {
	g := newTestGroup(t, 2, Config{})
	g.nodes[0].set(redirectTo("stranger:7000"), role(leaderRole, 1, ""))
	var n atomic.Int32
	g.nodes[0].set(func(ctx context.Context) error {
		if n.Add(1) == 1 {
			return redirectTo("stranger:7000")(ctx)
		}
		return nil
	}, nil)
	if _, err := g.book(ctxFor(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if g.m.Retries.Value("g", ReasonRedirect) != 0 || g.m.Retries.Value("g", ReasonNotLeader) != 1 {
		t.Fatal("a trailer naming an unknown node was followed")
	}
}

func TestStatusLeaderAddrHighestTerm(t *testing.T) {
	g := newTestGroup(t, 3, Config{})
	g.nodes[0].set(nil, role(followerRole, 4, "n1:7000"))
	g.nodes[1].set(nil, func() *seatsafev1.NodeStatus { return nil }) // down for Status, serves RPCs
	g.nodes[2].set(nil, role(followerRole, 9, "n2:7000"))
	g.nodes[2].set(nil, role(seatsafev1.RaftRole_RAFT_ROLE_CANDIDATE, 9, "n2:7000"))
	got, err := g.book(ctxFor(t, time.Second))
	if err != nil || got != "n2:7000" {
		t.Fatalf("answered by %q, %v (want the leader_addr of the highest term)", got, err)
	}
}

func TestNoStatusTriesNodesInTurn(t *testing.T) {
	g := newTestGroup(t, 3, Config{})
	for _, n := range g.nodes {
		n.set(refuse(codes.Unavailable), func() *seatsafev1.NodeStatus { return nil })
	}
	g.nodes[2].set(ok, nil)
	got, err := g.book(ctxFor(t, time.Second))
	if err != nil || got != "n2:7000" {
		t.Fatalf("answered by %q, %v", got, err)
	}
	if g.m.Retries.Value("g", ReasonUnavailable) != 2 || g.m.DiscoveryRounds.Value("g", "none") == 0 {
		t.Fatalf("unavailable retries %v", g.m.Retries.Value("g", ReasonUnavailable))
	}
}

func TestAnswersReturnedAsIs(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Internal, codes.NotFound} {
		g := newTestGroup(t, 1, Config{})
		g.nodes[0].set(refuse(code), nil)
		if _, err := g.book(ctxFor(t, time.Second)); status.Code(err) != code {
			t.Fatalf("%v: got %v", code, err)
		}
		if g.nodes[0].calls.Load() != 1 {
			t.Fatalf("%v retried", code)
		}
	}
}

func TestAttemptTimeoutRetries(t *testing.T) {
	g := newTestGroup(t, 1, Config{AttemptTimeout: 20 * time.Millisecond})
	g.nodes[0].set(nil, role(leaderRole, 1, ""))
	var n atomic.Int32
	g.nodes[0].set(func(ctx context.Context) error {
		if n.Add(1) == 1 {
			<-ctx.Done() // first attempt hangs past its own deadline
			return ctx.Err()
		}
		return nil
	}, nil)
	if _, err := g.book(ctxFor(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if g.m.Retries.Value("g", ReasonAttemptTimer) != 1 {
		t.Fatal("attempt timeout not retried")
	}
}

func TestContextEnds(t *testing.T) {
	g := newTestGroup(t, 2, Config{})
	for _, n := range g.nodes {
		n.set(refuse(codes.Unavailable), nil)
	}
	start := time.Now()
	if _, err := g.book(ctxFor(t, 80*time.Millisecond)); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("got %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("took %v", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	if _, err := g.book(ctx); status.Code(err) != codes.Canceled {
		t.Fatalf("got %v", err)
	}
	// Ending while an attempt is in flight also maps to the ctx error.
	g.nodes[0].set(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, role(leaderRole, 1, ""))
	if _, err := g.book(ctxFor(t, 50*time.Millisecond)); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("got %v", err)
	}
}

func TestBackoffSchedule(t *testing.T) {
	g := newTestGroup(t, 1, Config{Backoff: []time.Duration{time.Millisecond, 30 * time.Millisecond}})
	var n atomic.Int32
	g.nodes[0].set(func(context.Context) error {
		if n.Add(1) <= 3 {
			return status.Error(codes.Unavailable, "x")
		}
		return nil
	}, nil)
	start := time.Now()
	if _, err := g.book(ctxFor(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	// 1ms + 30ms + 30ms (the last step repeats).
	if d := time.Since(start); d < 60*time.Millisecond {
		t.Fatalf("retried too fast: %v", d)
	}
	if g.m.LeaderRecovery.Count("g") != 1 || g.m.LeaderRecovery.Sum("g") < 0.06 {
		t.Fatalf("recovery count %d sum %v", g.m.LeaderRecovery.Count("g"), g.m.LeaderRecovery.Sum("g"))
	}
}

func TestConcurrentCallsShareDiscovery(t *testing.T) {
	g := newTestGroup(t, 3, Config{StatusTimeout: time.Second})
	release := make(chan struct{})
	g.nodes[1].set(nil, func() *seatsafev1.NodeStatus {
		<-release
		return &seatsafev1.NodeStatus{Role: leaderRole, Term: 2}
	})
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := g.book(ctxFor(t, 2*time.Second))
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if s := g.nodes[1].stats.Load(); s != 1 {
		t.Fatalf("Status called %d times for 50 concurrent calls", s)
	}
}

func TestNewErrors(t *testing.T) {
	if _, err := New("empty", nil, Config{}, NewMetrics(metrics.NewRegistry())); err == nil {
		t.Fatal("empty group accepted")
	}
	if _, err := New("bad", []string{"a:1"}, Config{DialOptions: []grpc.DialOption{grpc.WithDefaultServiceConfig("{")}}, NewMetrics(metrics.NewRegistry())); err == nil {
		t.Fatal("bad dial option accepted")
	}
	g := newTestGroup(t, 1, Config{})
	if g.Name() != "g" {
		t.Fatal(g.Name())
	}
}
