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
	"strings"
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

	dialOpts []grpc.DialOption // extra options, for tests
}

// New creates a transport for member self of group groupID. addrs maps every
// member's Raft ID to its gRPC address; the entry for self is ignored.
// Call SetNode once the node has started.
func New(groupID uint32, self uint64, addrs map[uint64]string) (*Transport, error) {
	return newTransport(groupID, self, addrs)
}

func newTransport(groupID uint32, self uint64, addrs map[uint64]string, extra ...grpc.DialOption) (*Transport, error) {
	t := &Transport{groupID: groupID, self: self, peers: map[uint64]*peer{}, dialOpts: extra}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		p := &peer{
			id:    id,
			addr:  addr,
			t:     t,
			queue: make(chan raftpb.Message, queueSize),
			stop:  make(chan struct{}),
		}
		if err := p.dial(); err != nil {
			t.Close()
			return nil, fmt.Errorf("transport: peer %d at %s: %w", id, addr, err)
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

// misaddressed starts the error a node returns for a batch meant for another
// node; the sender then redials (see peer.send).
const misaddressed = "misaddressed"

func (s *server) Send(ctx context.Context, b *seatsafev1.RaftBatch) (*seatsafev1.RaftAck, error) {
	if b.GetGroupId() != s.t.groupID {
		return nil, status.Errorf(codes.FailedPrecondition,
			"message for Raft group %d sent to a member of group %d", b.GetGroupId(), s.t.groupID)
	}
	// Decode and check every message before stepping any. etcd raft does not
	// check a message's To, so a message meant for another node would be
	// taken as this node's own. That happens when a peer's connection still
	// points at an IP that a restarted container has since handed to another
	// node: the wrong node then answers for the right one, and the leader
	// can believe a node stored entries it never received.
	msgs := make([]raftpb.Message, 0, len(b.GetMessages()))
	for _, raw := range b.GetMessages() {
		var m raftpb.Message
		if err := m.Unmarshal(raw); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad raft message: %v", err)
		}
		if m.To != s.t.self {
			return nil, status.Errorf(codes.FailedPrecondition,
				"%s: message for Raft node %d reached node %d of group %d", misaddressed, m.To, s.t.self, s.t.groupID)
		}
		msgs = append(msgs, m)
	}
	n := s.t.node.Load()
	if n == nil {
		return nil, status.Error(codes.Unavailable, "node not started yet")
	}
	for _, m := range msgs {
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
	addr   string
	t      *Transport
	conn   *grpc.ClientConn // replaced only by the run goroutine (redial)
	client seatsafev1.RaftTransportClient
	queue  chan raftpb.Message
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func (p *peer) run() {
	defer p.wg.Done()
	for {
		var first raftpb.Message
		select {
		case first = <-p.queue:
		case <-p.stop:
			return
		}
		// Batch whatever else is already queued, up to maxBatchSize.
		batch := []raftpb.Message{first}
		size := first.Size()
	more:
		for size < maxBatchSize {
			select {
			case m := <-p.queue:
				batch = append(batch, m)
				size += m.Size()
			default:
				break more
			}
		}
		p.send(batch)
	}
}

func (p *peer) send(batch []raftpb.Message) {
	req := &seatsafev1.RaftBatch{GroupId: p.t.groupID, From: p.t.self}
	hasSnap := false
	for i := range batch {
		data, err := batch[i].Marshal()
		if err != nil {
			continue
		}
		req.Messages = append(req.Messages, data)
		hasSnap = hasSnap || batch[i].Type == raftpb.MsgSnap
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	_, err := p.client.Send(ctx, req)
	if err != nil {
		p.t.reportUnreachable(p.id)
		if status.Code(err) == codes.FailedPrecondition && strings.Contains(status.Convert(err).Message(), misaddressed) {
			// Our connection reaches the wrong node: start a new one, which
			// looks the peer's name up again.
			log.Printf("transport: peer %d (%s): %v; reconnecting", p.id, p.addr, err)
			if derr := p.redial(); derr != nil {
				log.Printf("transport: peer %d: redial: %v", p.id, derr)
			}
		}
	}
	if hasSnap {
		p.t.reportSnapshot(p.id, err == nil)
	}
}

// dial connects to the peer. passthrough makes gRPC resolve the name again
// on every reconnect instead of caching its IP: a restarted container can get
// a new IP, and its old one can go to another node.
func (p *peer) dial() error {
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxMsgSize)),
	}, p.t.dialOpts...)
	conn, err := grpc.NewClient("passthrough:///"+p.addr, opts...)
	if err != nil {
		return err
	}
	p.conn, p.client = conn, seatsafev1.NewRaftTransportClient(conn)
	return nil
}

// redial replaces the peer's connection. Called only from run, which is the
// only user of p.client; close waits for run to exit before closing p.conn.
func (p *peer) redial() error {
	old := p.conn
	if err := p.dial(); err != nil {
		return err
	}
	return old.Close()
}

func (p *peer) close() {
	p.once.Do(func() { close(p.stop) })
	p.wg.Wait()
	_ = p.conn.Close()
}
