package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

type recordSM struct {
	mu      sync.Mutex
	entries []string
}

func (s *recordSM) Apply(_ uint64, data []byte) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, string(data))
	return len(s.entries)
}
func (s *recordSM) Snapshot() ([]byte, error) { return nil, nil }
func (s *recordSM) Restore([]byte) error      { return nil }
func (s *recordSM) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.entries)
}

type member struct {
	node *raftnode.Node
	sm   *recordSM
	tr   *Transport
	gs   *grpc.Server
	addr string
	up   bool
}

func (m *member) shutdown() {
	if !m.up {
		return
	}
	m.up = false
	m.gs.Stop()
	m.node.Stop()
	m.tr.Close()
}

// startGroup runs a 3-node Raft group over real gRPC on localhost.
func startGroup(t *testing.T) (map[uint64]*member, map[uint64]string) {
	t.Helper()
	const group = 1
	ids := []uint64{1, 2, 3}
	listeners := map[uint64]net.Listener{}
	addrs := map[uint64]string{}
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id], addrs[id] = lis, lis.Addr().String()
	}
	quiet := &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	members := map[uint64]*member{}
	for _, id := range ids {
		tr, err := New(group, id, addrs)
		if err != nil {
			t.Fatal(err)
		}
		sm := &recordSM{}
		node, err := raftnode.Start(raftnode.Config{
			ID: id, Peers: ids, SM: sm, Transport: tr,
			TickInterval: 20 * time.Millisecond, Logger: quiet,
		})
		if err != nil {
			t.Fatal(err)
		}
		tr.SetNode(node)
		gs := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMsgSize))
		tr.Register(gs)
		NewStatusServer(fmt.Sprintf("shard1-node%d", id), group, node, addrs).Register(gs)
		go gs.Serve(listeners[id])
		members[id] = &member{node: node, sm: sm, tr: tr, gs: gs, addr: addrs[id], up: true}
	}
	t.Cleanup(func() {
		for _, m := range members {
			m.shutdown()
		}
	})
	return members, addrs
}

func waitLeader(t *testing.T, members map[uint64]*member) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, m := range members {
			if m.up && m.node.Status().IsLeader() {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader within 10s")
	return 0
}

func waitSame(t *testing.T, members map[uint64]*member, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var ref []string
		ok := true
		for _, m := range members {
			if !m.up {
				continue
			}
			got := m.sm.all()
			if len(got) != want || (ref != nil && !slices.Equal(got, ref)) {
				ok = false
				break
			}
			ref = got
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("running nodes did not converge on %d identical entries", want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func propose(t *testing.T, n *raftnode.Node, data string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := n.Propose(ctx, []byte(data)); err != nil {
		t.Fatalf("propose: %v", err)
	}
}

func nodeStatus(t *testing.T, addr string) *seatsafev1.NodeStatus {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := seatsafev1.NewNodeServiceClient(conn).Status(ctx, &seatsafev1.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestReplicatesOverGRPC(t *testing.T) {
	members, addrs := startGroup(t)
	lead := waitLeader(t, members)
	for i := range 200 {
		propose(t, members[lead].node, fmt.Sprintf("e-%d", i))
	}
	waitSame(t, members, 200)

	// NodeService on the leader: role, leader address, and both followers caught up.
	st := nodeStatus(t, addrs[lead])
	if st.Role != seatsafev1.RaftRole_RAFT_ROLE_LEADER || st.LeaderAddr != addrs[lead] {
		t.Fatalf("leader status: role %v, leader_addr %q", st.Role, st.LeaderAddr)
	}
	if len(st.Followers) != 2 {
		t.Fatalf("leader reports %d followers, want 2", len(st.Followers))
	}
	// A follower points clients at the leader.
	follower := lead%3 + 1
	if st := nodeStatus(t, addrs[follower]); st.Role != seatsafev1.RaftRole_RAFT_ROLE_FOLLOWER || st.LeaderAddr != addrs[lead] {
		t.Fatalf("follower status: role %v, leader_addr %q (leader is %q)", st.Role, st.LeaderAddr, addrs[lead])
	}
}

func TestSurvivesStoppedFollower(t *testing.T) {
	members, _ := startGroup(t)
	lead := waitLeader(t, members)
	members[lead%3+1].shutdown() // a follower's container dies

	for i := range 50 {
		propose(t, members[lead].node, fmt.Sprintf("e-%d", i)) // 2 of 3 is still a majority
	}
	waitSame(t, members, 50)
}

func TestRejectsWrongGroup(t *testing.T) {
	members, addrs := startGroup(t)
	waitLeader(t, members)
	conn, err := grpc.NewClient(addrs[1], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = seatsafev1.NewRaftTransportClient(conn).Send(context.Background(), &seatsafev1.RaftBatch{GroupId: 2, From: 1})
	if err == nil {
		t.Fatal("a batch for group 2 was accepted by a group 1 node")
	}
}
