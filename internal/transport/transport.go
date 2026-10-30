// Package transport carries Raft messages between the members of one Raft
// group over gRPC (the RaftTransport service), and serves NodeService.Status.
//
// Each peer gets its own queue and sender goroutine, so messages to one peer
// stay in order and a slow or dead peer never blocks the others or the Raft
// loop.
package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

const (
	queueSize    = 4096
	maxBatchSize = 2 << 20 // bytes of messages per Send call
	sendTimeout  = 2 * time.Second
	// MaxMsgSize is the gRPC message limit to use on servers that serve
	// RaftTransport. Raft messages are capped at 1 MiB each.
	MaxMsgSize = 16 << 20
)

// Transport implements raftnode.Transport over gRPC. Register serves the
// RaftTransport service that receives messages from peers.
type Transport struct {
	groupID uint32
	self    uint64
	node    atomic.Pointer[raftnode.Node]
	peers   map[uint64]*peer
}

// New creates a transport for member self of group groupID. addrs maps every
// member's Raft ID to its gRPC address; the entry for self is ignored.
// Call SetNode once the node has started.
func New(groupID uint32, self uint64, addrs map[uint64]string) (*Transport, error) {
	t := &Transport{groupID: groupID, self: self, peers: map[uint64]*peer{}}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxMsgSize)),
		)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("transport: peer %d at %s: %w", id, addr, err)
		}
		p := &peer{
			id:     id,
			t:      t,
			conn:   conn,
			client: seatsafev1.NewRaftTransportClient(conn),
			queue:  make(chan raftpb.Message, queueSize),
			stop:   make(chan struct{}),
		}
		t.peers[id] = p
		p.wg.Add(1)
		go p.run()
	}
	return t, nil
}

// SetNode connects the transport to the local node. Messages that arrive
// before this are dropped (Raft retransmits).
func (t *Transport) SetNode(n *raftnode.Node) { t.node.Store(n) }

// Send implements raftnode.Transport. It never blocks: if a peer's queue is
// full, the message is dropped and the peer reported unreachable.
func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		p, ok := t.peers[m.To]
		if !ok {
			continue
		}
		select {
		case p.queue <- m:
		default:
			t.reportUnreachable(m.To)
			if m.Type == raftpb.MsgSnap {
				t.reportSnapshot(m.To, false)
			}
		}
	}
}

// Close stops all senders and closes the connections.
func (t *Transport) Close() {
	for _, p := range t.peers {
		p.close()
	}
}

// Register serves the RaftTransport service for this transport on s. Create s
// with grpc.MaxRecvMsgSize(MaxMsgSize).
func (t *Transport) Register(s *grpc.Server) {
	seatsafev1.RegisterRaftTransportServer(s, &server{t: t})
}

// server receives batches from peers and hands each message to the node.
type server struct {
	seatsafev1.UnimplementedRaftTransportServer
	t *Transport
}

func (s *server) Send(ctx context.Context, b *seatsafev1.RaftBatch) (*seatsafev1.RaftAck, error) {
	if b.GetGroupId() != s.t.groupID {
		return nil, status.Errorf(codes.FailedPrecondition,
			"message for Raft group %d sent to a member of group %d", b.GetGroupId(), s.t.groupID)
	}
	n := s.t.node.Load()
	if n == nil {
		return nil, status.Error(codes.Unavailable, "node not started yet")
	}
	for _, raw := range b.GetMessages() {
		var m raftpb.Message
		if err := m.Unmarshal(raw); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad raft message: %v", err)
		}
		if err := n.Step(ctx, m); err != nil {
			return nil, status.Errorf(codes.Unavailable, "step: %v", err)
		}
	}
	return &seatsafev1.RaftAck{}, nil
}

func (t *Transport) reportUnreachable(id uint64) {
	if n := t.node.Load(); n != nil {
		n.ReportUnreachable(id)
	}
}

func (t *Transport) reportSnapshot(id uint64, ok bool) {
	if n := t.node.Load(); n != nil {
		n.ReportSnapshot(id, ok)
	}
}

type peer struct {
	id     uint64
	t      *Transport
	conn   *grpc.ClientConn
	client seatsafev1.RaftTransportClient
	queue  chan raftpb.Message
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func (p *peer) run() {
	defer p.wg.Done()
	var carry *raftpb.Message // a snapshot held back to go in its own batch
	for {
		var first raftpb.Message
		if carry != nil {
			first, carry = *carry, nil
		} else {
			select {
			case first = <-p.queue:
			case <-p.stop:
				return
			}
		}
		// Batch whatever else is already queued, up to maxBatchSize. A
		// snapshot (MsgSnap, possibly megabytes) always travels alone.
		batch := []raftpb.Message{first}
		size := first.Size()
	more:
		for first.Type != raftpb.MsgSnap && size < maxBatchSize {
			select {
			case m := <-p.queue:
				if m.Type == raftpb.MsgSnap {
					carry = &m
					break more
				}
				batch = append(batch, m)
				size += m.Size()
			default:
				break more
			}
		}
		p.send(batch)
	}
}

// fitsInOneCall reports whether a message can be sent in one RaftBatch
// within the gRPC message limit, leaving room for the batch's own framing.
func fitsInOneCall(m raftpb.Message) bool { return m.Size() <= MaxMsgSize-64<<10 }

func (p *peer) send(batch []raftpb.Message) {
	req := &seatsafev1.RaftBatch{GroupId: p.t.groupID, From: p.t.self}
	hasSnap := false
	for i := range batch {
		if !fitsInOneCall(batch[i]) {
			// Only a snapshot can get this big (other messages are capped at
			// MaxSizePerMsg). Sending it would fail the whole call, so report
			// it instead; Raft retries later.
			log.Printf("transport: dropping %v to peer %d: %d bytes exceeds the %d byte gRPC limit",
				batch[i].Type, p.id, batch[i].Size(), MaxMsgSize)
			if batch[i].Type == raftpb.MsgSnap {
				p.t.reportSnapshot(p.id, false)
			}
			continue
		}
		data, err := batch[i].Marshal()
		if err != nil {
			continue
		}
		req.Messages = append(req.Messages, data)
		hasSnap = hasSnap || batch[i].Type == raftpb.MsgSnap
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if len(req.Messages) == 0 {
		return
	}
	_, err := p.client.Send(ctx, req)
	if err != nil {
		p.t.reportUnreachable(p.id)
	}
	if hasSnap {
		p.t.reportSnapshot(p.id, err == nil)
	}
}

func (p *peer) close() {
	p.once.Do(func() { close(p.stop) })
	p.wg.Wait()
	_ = p.conn.Close()
}
