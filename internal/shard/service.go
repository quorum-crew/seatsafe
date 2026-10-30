package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
)

// LeaderTrailer is the gRPC trailer that carries the leader's address
// (host:port) when a follower rejects a write.
const LeaderTrailer = "seatsafe-leader"

// DefaultHoldTTL is used when a PrepareRequest carries no hold_ttl_ms.
const DefaultHoldTTL = 5 * time.Second

// Service serves ShardService and ParticipantService on a storage node.
type Service struct {
	node  *raftnode.Node
	sm    StateMachine
	addrs map[uint64]string // Raft ID -> gRPC address, for leader redirects
	now   func() time.Time
}

// NewService creates the services for one storage node.
func NewService(node *raftnode.Node, sm StateMachine, addrs map[uint64]string) *Service {
	return &Service{node: node, sm: sm, addrs: addrs, now: time.Now}
}

// Register serves ShardService and ParticipantService on gs.
func (s *Service) Register(gs *grpc.Server) {
	seatsafev1.RegisterShardServiceServer(gs, shardServer{s: s})
	seatsafev1.RegisterParticipantServiceServer(gs, participantServer{s: s})
}

type shardServer struct {
	seatsafev1.UnimplementedShardServiceServer
	s *Service
}

// Book books seats that all belong to this shard, as one Raft entry.
func (x shardServer) Book(ctx context.Context, req *seatsafev1.ShardBookRequest) (*seatsafev1.BookSeatsResponse, error) {
	if req.GetRequestId() == "" || len(req.GetSeats()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request_id and at least one seat are required")
	}
	res, err := x.s.write(ctx, x.s.now(), &seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Book{Book: &seatsafev1.BookOp{
		RequestId: req.GetRequestId(),
		UserId:    req.GetUserId(),
		Seats:     req.GetSeats(),
	}}})
	if err != nil {
		return nil, err
	}
	if res.Book == nil {
		return nil, status.Errorf(codes.Internal, "state machine returned no booking result (%v)", res.Err)
	}
	return res.Book, nil
}

// ListAvailable answers from this replica's state; any replica may serve it.
func (x shardServer) ListAvailable(_ context.Context, req *seatsafev1.ListAvailableSeatsRequest) (*seatsafev1.ListAvailableSeatsResponse, error) {
	if req.GetEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "event_id is required")
	}
	return x.s.sm.ListAvailable(req.GetEventId(), req.GetSectionId()), nil
}

type participantServer struct {
	seatsafev1.UnimplementedParticipantServiceServer
	s *Service
}

// Prepare holds this shard's seats for a cross-shard order (2PC phase 1).
func (x participantServer) Prepare(ctx context.Context, req *seatsafev1.PrepareRequest) (*seatsafev1.PrepareResponse, error) {
	if req.GetTxnId() == "" || len(req.GetSeats()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "txn_id and at least one seat are required")
	}
	ttl := time.Duration(req.GetHoldTtlMs()) * time.Millisecond
	if ttl <= 0 {
		ttl = DefaultHoldTTL
	}
	now := x.s.now()
	res, err := x.s.write(ctx, now, &seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Prepare{Prepare: &seatsafev1.PrepareOp{
		TxnId:           req.GetTxnId(),
		UserId:          req.GetUserId(),
		Seats:           req.GetSeats(),
		HoldExpiresAtMs: now.Add(ttl).UnixMilli(),
	}}})
	if err != nil {
		return nil, err
	}
	if res.Prepare == nil {
		return nil, status.Errorf(codes.Internal, "state machine returned no vote (%v)", res.Err)
	}
	return res.Prepare, nil
}

// Commit applies a COMMIT decision (2PC phase 2). Idempotent.
func (x participantServer) Commit(ctx context.Context, req *seatsafev1.DecisionRequest) (*seatsafev1.DecisionResponse, error) {
	return x.decide(ctx, &seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Commit{Commit: &seatsafev1.TxnOp{TxnId: req.GetTxnId()}}}, req)
}

// Abort applies an ABORT decision (2PC phase 2). Idempotent; aborting an
// unknown txn leaves a tombstone so a late Prepare votes NO.
func (x participantServer) Abort(ctx context.Context, req *seatsafev1.DecisionRequest) (*seatsafev1.DecisionResponse, error) {
	return x.decide(ctx, &seatsafev1.ShardCommand{Op: &seatsafev1.ShardCommand_Abort{Abort: &seatsafev1.TxnOp{TxnId: req.GetTxnId()}}}, req)
}

func (x participantServer) decide(ctx context.Context, cmd *seatsafev1.ShardCommand, req *seatsafev1.DecisionRequest) (*seatsafev1.DecisionResponse, error) {
	if req.GetTxnId() == "" {
		return nil, status.Error(codes.InvalidArgument, "txn_id is required")
	}
	res, err := x.s.write(ctx, x.s.now(), cmd)
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		// e.g. Commit of an ABORTED txn: the decision contradicts this shard's log.
		return nil, status.Error(codes.FailedPrecondition, res.Err.Error())
	}
	return &seatsafev1.DecisionResponse{}, nil
}

// write checks leadership, stamps the command with the leader's clock,
// replicates it, and waits for the result of applying it on this node.
func (s *Service) write(ctx context.Context, now time.Time, cmd *seatsafev1.ShardCommand) (seatsm.Result, error) {
	if err := s.requireLeader(ctx); err != nil {
		return seatsm.Result{}, err
	}
	cmd.NowMs = now.UnixMilli()
	data, err := proto.Marshal(cmd)
	if err != nil {
		return seatsm.Result{}, status.Errorf(codes.Internal, "marshal command: %v", err)
	}
	res, err := s.node.Propose(ctx, data)
	switch {
	case err == nil:
		r, ok := res.(seatsm.Result)
		if !ok {
			return seatsm.Result{}, status.Error(codes.Internal, fmt.Sprintf("unexpected apply result %T", res))
		}
		return r, nil
	case errors.Is(err, context.DeadlineExceeded):
		return seatsm.Result{}, status.Error(codes.DeadlineExceeded, "timed out; retry with the same id")
	case errors.Is(err, context.Canceled):
		return seatsm.Result{}, status.Error(codes.Canceled, "canceled")
	case errors.Is(err, raft.ErrProposalDropped), errors.Is(err, raftnode.ErrLeadershipLost), errors.Is(err, raftnode.ErrStopped):
		return seatsm.Result{}, status.Errorf(codes.Unavailable, "%v; retry with the same id", err)
	default:
		return seatsm.Result{}, status.Errorf(codes.Internal, "propose: %v", err)
	}
}

// requireLeader rejects writes on a follower with FAILED_PRECONDITION and the
// leader's address in the seatsafe-leader trailer, or UNAVAILABLE during an
// election.
func (s *Service) requireLeader(ctx context.Context) error {
	st := s.node.Status()
	if st.IsLeader() {
		return nil
	}
	if st.Leader == 0 {
		return status.Error(codes.Unavailable, "no leader yet; retry with the same id")
	}
	addr := s.addrs[st.Leader]
	_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderTrailer, addr))
	return status.Errorf(codes.FailedPrecondition, "not the leader; leader is %s", addr)
}
