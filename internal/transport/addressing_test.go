package transport

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// A batch holding a message for another node is refused before anything in
// it is stepped: etcd raft would take the message as this node's own.
func TestMisaddressedBatchRefused(t *testing.T) {
	tr := &Transport{groupID: 1, self: 1}
	ok, _ := (&raftpb.Message{To: 1, From: 2, Type: raftpb.MsgHeartbeat}).Marshal()
	wrong, _ := (&raftpb.Message{To: 3, From: 2, Type: raftpb.MsgHeartbeat, Commit: 1826}).Marshal()
	_, err := (&server{t: tr}).Send(context.Background(), &seatsafev1.RaftBatch{GroupId: 1, From: 2, Messages: [][]byte{ok, wrong}})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), misaddressed) {
		// With no node set, a batch that passed the check would get
		// Unavailable ("node not started"), so this also shows the check
		// comes first.
		t.Fatalf("misaddressed batch: %v, want FailedPrecondition %q", err, misaddressed)
	}
}

// countingServer records which node a batch reached and answers like a
// RaftTransport server whose node is self.
func countingServer(t *testing.T, self uint64, hits *atomic.Int32) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		hits.Add(1)
		return h(ctx, req)
	}))
	(&Transport{groupID: 1, self: self}).Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

// After restarts, a peer's name can lead to a connection that reaches
// another node (its old IP now belongs to someone else). The sender must
// notice the refusal and reconnect, looking the name up again, and so reach
// the right node.
func TestReconnectsAfterReachingWrongNode(t *testing.T) {
	var wrongHits, rightHits atomic.Int32
	wrong := countingServer(t, 1, &wrongHits) // node 1, where node 3's old IP now leads
	right := countingServer(t, 3, &rightHits) // node 3 at its new address

	// The "name lookup": the first connection goes to the stale address,
	// every later one to the current address.
	var dials atomic.Int32
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		addr := right
		if dials.Add(1) == 1 {
			addr = wrong
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	})
	tr, err := newTransport(1, 2, map[uint64]string{2: "self:7000", 3: "shard1-node3:7000"}, dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)

	deadline := time.Now().Add(5 * time.Second)
	for rightHits.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("never reached node 3: %d batches went to node 1, %d dials", wrongHits.Load(), dials.Load())
		}
		tr.Send([]raftpb.Message{{To: 3, From: 2, Type: raftpb.MsgHeartbeat}})
		time.Sleep(20 * time.Millisecond)
	}
	if wrongHits.Load() == 0 {
		t.Fatal("the test never sent to the wrong node")
	}
}

// snapshotServer is a RaftTransport server for node self (with no Raft node
// behind it) that counts the snapshot streams reaching it.
func snapshotServer(t *testing.T, self uint64, streams *atomic.Int32) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		streams.Add(1)
		return h(srv, ss)
	}))
	(&Transport{groupID: 1, self: self}).Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func snapMsg(to uint64) raftpb.Message {
	return raftpb.Message{To: to, From: 2, Type: raftpb.MsgSnap,
		Snapshot: &raftpb.Snapshot{Data: []byte("state"), Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 2}}}
}

// A snapshot for another node is refused like a misaddressed batch: stepping
// it would replace this node's state with another replica's.
func TestMisaddressedSnapshotRefused(t *testing.T) {
	var n atomic.Int32
	addr := snapshotServer(t, 1, &n)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := seatsafev1.NewRaftTransportClient(conn).SendSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := snapMsg(3)
	data, _ := m.Marshal()
	_ = stream.Send(&seatsafev1.SnapshotChunk{GroupId: 1, From: 2, Data: data})
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), misaddressed) {
		t.Fatalf("misaddressed snapshot: %v, want FailedPrecondition %q", err, misaddressed)
	}
}

// A sender whose snapshot reaches the wrong node reconnects, so the next
// snapshot Raft sends reaches the right one.
func TestSnapshotReconnectsAfterReachingWrongNode(t *testing.T) {
	var wrongStreams, rightStreams atomic.Int32
	wrong := snapshotServer(t, 1, &wrongStreams)
	right := snapshotServer(t, 3, &rightStreams)
	var dials atomic.Int32
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		addr := right
		if dials.Add(1) == 1 {
			addr = wrong
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	})
	tr, err := newTransport(1, 2, map[uint64]string{2: "self:7000", 3: "shard1-node3:7000"}, dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)

	deadline := time.Now().Add(5 * time.Second)
	for rightStreams.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("snapshot never reached node 3: %d streams went to node 1, %d dials", wrongStreams.Load(), dials.Load())
		}
		tr.Send([]raftpb.Message{snapMsg(3)}) // Raft resends after a failed snapshot
		time.Sleep(50 * time.Millisecond)
	}
	if wrongStreams.Load() == 0 {
		t.Fatal("the test never sent to the wrong node")
	}
}
