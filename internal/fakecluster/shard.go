package fakecluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
	"github.com/quorum-crew/seatsafe/internal/seed"
)

// DefaultHoldTTL is used when a PrepareRequest has no hold_ttl_ms.
const DefaultHoldTTL = 5 * time.Second

// leadership simulates which node of a group is leader.
type leadership struct {
	groupID uint32
	addrs   []string

	mu     sync.RWMutex
	leader int // index into addrs; -1 = no leader
	term   uint64
}

func newLeadership(groupID uint32, addrs []string) *leadership {
	return &leadership{groupID: groupID, addrs: addrs, leader: 0, term: 1}
}

// SetLeader makes node i the leader in a new term.
func (l *leadership) SetLeader(i int) {
	if i < 0 || i >= len(l.addrs) {
		panic(fmt.Sprintf("fakecluster: no node %d", i))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leader = i
	l.term++
}

// SetNoLeader starts an election that nobody has won yet: every node
// answers UNAVAILABLE until SetLeader.
func (l *leadership) SetNoLeader() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leader = -1
	l.term++
}

// LeaderIndex returns the leader's index, or -1.
func (l *leadership) LeaderIndex() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leader
}

// LeaderAddr returns the leader's address, or "".
func (l *leadership) LeaderAddr() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.leader < 0 {
		return ""
	}
	return l.addrs[l.leader]
}

// checkLeader returns nil if node i may serve a write.
func (l *leadership) checkLeader(ctx context.Context, i int) error {
	l.mu.RLock()
	leader := l.leader
	l.mu.RUnlock()
	switch {
	case leader < 0:
		return status.Error(codes.Unavailable, "fakecluster: no leader")
	case leader != i:
		_ = grpc.SetTrailer(ctx, metadata.Pairs(groupclient.LeaderTrailer, l.addrs[leader]))
		return status.Errorf(codes.FailedPrecondition, "fakecluster: %s is not the leader", l.addrs[i])
	}
	return nil
}

func (l *leadership) status(i int, nodeID string, applied uint64) *seatsafev1.NodeStatus {
	l.mu.RLock()
	defer l.mu.RUnlock()
	st := &seatsafev1.NodeStatus{NodeId: nodeID, GroupId: l.groupID, Term: l.term, CommitIndex: applied, AppliedIndex: applied}
	switch {
	case l.leader < 0:
		st.Role = seatsafev1.RaftRole_RAFT_ROLE_CANDIDATE
	case l.leader == i:
		st.Role = seatsafev1.RaftRole_RAFT_ROLE_LEADER
	default:
		st.Role = seatsafev1.RaftRole_RAFT_ROLE_FOLLOWER
	}
	if l.leader >= 0 {
		st.LeaderId = uint64(l.leader + 1)
		st.LeaderAddr = l.addrs[l.leader]
	}
	return st
}

// FakeGroup is one fake shard: 3 nodes, one shared state machine.
type FakeGroup struct {
	*leadership
	ID      uint32
	Machine *seatsm.Machine
	Nodes   []*FakeNode

	clock   func() time.Time
	applyMu sync.Mutex
	index   uint64
}

// NewFakeGroup builds a shard group whose nodes have the given addresses.
// Node 0 starts as leader.
func NewFakeGroup(id uint32, addrs []string, clock func() time.Time) *FakeGroup {
	if clock == nil {
		clock = time.Now
	}
	g := &FakeGroup{leadership: newLeadership(id, addrs), ID: id, Machine: seatsm.New(), clock: clock}
	for i, a := range addrs {
		g.Nodes = append(g.Nodes, &FakeNode{group: g, idx: i, Addr: a})
	}
	return g
}

// Apply stamps NowMs, assigns the next index and applies cmd.
func (g *FakeGroup) Apply(cmd *seatsafev1.ShardCommand) seatsm.Result {
	g.applyMu.Lock()
	defer g.applyMu.Unlock()
	cmd.NowMs = g.clock().UnixMilli()
	g.index++
	return g.Machine.Apply(g.index, cmd)
}

// Seed applies every chunk of a seed file as SeedOps.
func (g *FakeGroup) Seed(l *seed.Loaded) error {
	for c := 0; c < l.TotalChunks(); c++ {
		op := &seatsafev1.SeedOp{SeedVersion: l.Version, Chunk: uint32(c), TotalChunks: uint32(l.TotalChunks())}
		for _, s := range l.Chunk(c) {
			op.Seats = append(op.Seats, &seatsafev1.SeatRef{EventId: s.EventID, SectionId: s.SectionID, SeatId: s.SeatID})
		}
		if r := g.Apply(&seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Seed{Seed: op}}); r.Err != nil {
			return fmt.Errorf("fakecluster: seed shard %d: %w", g.ID, r.Err)
		}
	}
	return nil
}

// FakeNode is one node of a FakeGroup.
type FakeNode struct {
	group *FakeGroup
	idx   int
	Addr  string
}

// Register adds the node's ShardService, ParticipantService and NodeService
// to s.
func (n *FakeNode) Register(s grpc.ServiceRegistrar) {
	seatsafev1.RegisterShardServiceServer(s, shardService{n: n})
	seatsafev1.RegisterParticipantServiceServer(s, participantService{n: n})
	seatsafev1.RegisterNodeServiceServer(s, nodeService{status: func() *seatsafev1.NodeStatus {
		return n.group.status(n.idx, nodeID(n.Addr), n.group.Machine.AppliedIndex())
	}})
}

type shardService struct {
	seatsafev1.UnimplementedShardServiceServer
	n *FakeNode
}

func (s shardService) Book(ctx context.Context, req *seatsafev1.ShardBookRequest) (*seatsafev1.BookSeatsResponse, error) {
	if err := s.n.group.checkLeader(ctx, s.n.idx); err != nil {
		return nil, err
	}
	r := s.n.group.Apply(&seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Book{Book: &seatsafev1.BookOp{
		RequestId: req.GetRequestId(), UserId: req.GetUserId(), Seats: req.GetSeats(),
	}}})
	return r.Book, nil
}

// ListAvailable is a read: any node answers, even without a leader.
func (s shardService) ListAvailable(ctx context.Context, req *seatsafev1.ListAvailableSeatsRequest) (*seatsafev1.ListAvailableSeatsResponse, error) {
	return s.n.group.Machine.ListAvailable(req.GetEventId(), req.GetSectionId()), nil
}

type participantService struct {
	seatsafev1.UnimplementedParticipantServiceServer
	n *FakeNode
}

func (s participantService) Prepare(ctx context.Context, req *seatsafev1.PrepareRequest) (*seatsafev1.PrepareResponse, error) {
	if err := s.n.group.checkLeader(ctx, s.n.idx); err != nil {
		return nil, err
	}
	ttl := time.Duration(req.GetHoldTtlMs()) * time.Millisecond
	if ttl <= 0 {
		ttl = DefaultHoldTTL
	}
	r := s.n.group.Apply(&seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Prepare{Prepare: &seatsafev1.PrepareOp{
		TxnId: req.GetTxnId(), UserId: req.GetUserId(), Seats: req.GetSeats(),
		HoldExpiresAtMs: s.n.group.clock().Add(ttl).UnixMilli(),
	}}})
	return r.Prepare, nil
}

func (s participantService) Commit(ctx context.Context, req *seatsafev1.DecisionRequest) (*seatsafev1.DecisionResponse, error) {
	return s.decide(ctx, &seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Commit{Commit: &seatsafev1.TxnOp{TxnId: req.GetTxnId()}}})
}

func (s participantService) Abort(ctx context.Context, req *seatsafev1.DecisionRequest) (*seatsafev1.DecisionResponse, error) {
	return s.decide(ctx, &seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Abort{Abort: &seatsafev1.TxnOp{TxnId: req.GetTxnId()}}})
}

func (s participantService) decide(ctx context.Context, cmd *seatsafev1.ShardCommand) (*seatsafev1.DecisionResponse, error) {
	if err := s.n.group.checkLeader(ctx, s.n.idx); err != nil {
		return nil, err
	}
	if r := s.n.group.Apply(cmd); r.Err != nil {
		// A protocol violation. Not FAILED_PRECONDITION: that code means "not the leader".
		return nil, status.Error(codes.Internal, r.Err.Error())
	}
	return &seatsafev1.DecisionResponse{}, nil
}

type nodeService struct {
	seatsafev1.UnimplementedNodeServiceServer
	status func() *seatsafev1.NodeStatus
}

func (s nodeService) Status(context.Context, *seatsafev1.StatusRequest) (*seatsafev1.NodeStatus, error) {
	return s.status(), nil
}

// nodeID turns "shard1-node2:7000" into "shard1-node2".
func nodeID(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}
