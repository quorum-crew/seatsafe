package transport

import (
	"context"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

// StatusServer serves NodeService.Status for one Raft node. The router uses it
// to find leaders; tests and dashboards use it to watch the group.
type StatusServer struct {
	seatsafev1.UnimplementedNodeServiceServer

	nodeID  string // e.g. "shard2-node3"
	groupID uint32
	node    *raftnode.Node
	addrs   map[uint64]string // Raft ID -> gRPC address, for leader_addr
}

// NewStatusServer creates a NodeService for node.
func NewStatusServer(nodeID string, groupID uint32, node *raftnode.Node, addrs map[uint64]string) *StatusServer {
	return &StatusServer{nodeID: nodeID, groupID: groupID, node: node, addrs: addrs}
}

// Register serves NodeService on s.
func (s *StatusServer) Register(gs *grpc.Server) { seatsafev1.RegisterNodeServiceServer(gs, s) }

// Status implements NodeService.
func (s *StatusServer) Status(context.Context, *seatsafev1.StatusRequest) (*seatsafev1.NodeStatus, error) {
	return ToProto(s.nodeID, s.groupID, s.node.Status(), s.addrs), nil
}

// ToProto converts a raftnode.Status into the NodeStatus message.
func ToProto(nodeID string, groupID uint32, st raftnode.Status, addrs map[uint64]string) *seatsafev1.NodeStatus {
	out := &seatsafev1.NodeStatus{
		NodeId:       nodeID,
		GroupId:      groupID,
		Role:         role(st.Role),
		Term:         st.Term,
		LeaderId:     st.Leader,
		LeaderAddr:   addrs[st.Leader],
		CommitIndex:  st.Commit,
		AppliedIndex: st.Applied,
	}
	for _, f := range st.Followers {
		out.Followers = append(out.Followers, &seatsafev1.FollowerProgress{
			NodeId: f.ID, MatchIndex: f.Match, RecentActive: f.RecentActive,
		})
	}
	return out
}

func role(r raft.StateType) seatsafev1.RaftRole {
	switch r {
	case raft.StateLeader:
		return seatsafev1.RaftRole_RAFT_ROLE_LEADER
	case raft.StateCandidate, raft.StatePreCandidate:
		return seatsafev1.RaftRole_RAFT_ROLE_CANDIDATE
	case raft.StateFollower:
		return seatsafev1.RaftRole_RAFT_ROLE_FOLLOWER
	}
	return seatsafev1.RaftRole_RAFT_ROLE_UNSPECIFIED
}
