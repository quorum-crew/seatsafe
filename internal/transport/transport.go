// Package transport carries Raft messages between the members of one Raft
// group over gRPC (the RaftTransport service), and serves NodeService.Status.
//
// Each peer gets its own queue and sender goroutine, so messages to one peer
// stay in order and a slow or dead peer never blocks the others or the Raft
// loop. Snapshots (MsgSnap) go on a separate goroutine per peer as a stream
// of chunks (SendSnapshot), so a snapshot of any size gets through the gRPC
// message limit and heartbeats keep flowing while it is sent.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	// RaftTransport. Ordinary Raft messages are capped at 1 MiB each
	// (raftnode's MaxSizePerMsg) and batched up to 2 MiB per call; snapshots
	// are streamed in chunks, so none comes close.
	MaxMsgSize = 16 << 20

	// snapshotChunkSize is how much of a snapshot each SnapshotChunk carries.
	snapshotChunkSize = 1 << 20
	// maxSnapshotSize caps what a receiver buffers for one snapshot.
	maxSnapshotSize = 1 << 30
	// A snapshot stream gets snapshotBaseTimeout plus one second per
	// snapshotMinRate bytes: about 30 s for the 83 MiB of a full benchmark.
	snapshotBaseTimeout = 10 * time.Second
	snapshotMinRate     = 4 << 20
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
		ctx, cancel := context.WithCancel(context.Background())
		p := &peer{
			id:     id,
			addr:   addr,
			t:      t,
			queue:  make(chan raftpb.Message, queueSize),
			snapc:  make(chan raftpb.Message, 1),
			ctx:    ctx,
			cancel: cancel,
		}
		if err := p.dial(); err != nil {
			t.Close()
			return nil, fmt.Errorf("transport: peer %d at %s: %w", id, addr, err)
		}
		t.peers[id] = p
		p.wg.Add(2)
		go p.run()
		go p.runSnapshots()
	}
	return t, nil
}

// SetNode connects the transport to the local node. Messages that arrive
// before this are dropped (Raft retransmits).
func (t *Transport) SetNode(n *raftnode.Node) { t.node.Store(n) }

// Send implements raftnode.Transport. It never blocks: if a peer's queue is
// full, the message is dropped and the peer reported unreachable. A snapshot
// goes to the peer's snapshot sender; if one is already in flight to that
// peer, the new one is reported failed and Raft retries later.
func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		p, ok := t.peers[m.To]
		if !ok {
			continue
		}
		if m.Type == raftpb.MsgSnap {
			select {
			case p.snapc <- m:
			default:
				t.reportSnapshot(m.To, false)
			}
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
// with grpc.MaxRecvMsgSize(MaxMsgSize); snapshots stream in 1 MiB chunks, so
// that limit does not cap their size.
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

// SendSnapshot joins the chunks of one MsgSnap and steps it into the node
// once the sender closes the stream.
func (s *server) SendSnapshot(stream grpc.ClientStreamingServer[seatsafev1.SnapshotChunk, seatsafev1.RaftAck]) error {
	var buf []byte
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if c.GetGroupId() != s.t.groupID {
			return status.Errorf(codes.FailedPrecondition,
				"snapshot for Raft group %d sent to a member of group %d", c.GetGroupId(), s.t.groupID)
		}
		if len(buf)+len(c.GetData()) > maxSnapshotSize {
			return status.Errorf(codes.ResourceExhausted, "snapshot larger than %d bytes", maxSnapshotSize)
		}
		buf = append(buf, c.GetData()...)
	}
	var m raftpb.Message
	if err := m.Unmarshal(buf); err != nil {
		return status.Errorf(codes.InvalidArgument, "bad snapshot message: %v", err)
	}
	if m.Type != raftpb.MsgSnap {
		return status.Errorf(codes.InvalidArgument, "SendSnapshot carried a %v", m.Type)
	}
	n := s.t.node.Load()
	if n == nil {
		return status.Error(codes.Unavailable, "node not started yet")
	}
	if err := n.Step(stream.Context(), m); err != nil {
		return status.Errorf(codes.Unavailable, "step: %v", err)
	}
	return stream.SendAndClose(&seatsafev1.RaftAck{})
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
	id   uint64
	addr string
	t    *Transport

	// mu guards conn and client: both senders (run and runSnapshots) use
	// the client, and either may replace the connection (redial).
	mu     sync.Mutex
	conn   *grpc.ClientConn
	client seatsafev1.RaftTransportClient

	queue  chan raftpb.Message
	snapc  chan raftpb.Message // at most one snapshot waits while one is sent
	ctx    context.Context     // cancelled by close, ending any send in flight
	cancel context.CancelFunc
	once   sync.Once
	wg     sync.WaitGroup
}

func (p *peer) run() {
	defer p.wg.Done()
	for {
		var first raftpb.Message
		select {
		case first = <-p.queue:
		case <-p.ctx.Done():
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
	for i := range batch {
		data, err := batch[i].Marshal()
		if err != nil {
			continue
		}
		req.Messages = append(req.Messages, data)
	}
	if len(req.Messages) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, sendTimeout)
	defer cancel()
	if _, err := p.rpc().Send(ctx, req); err != nil {
		p.t.reportUnreachable(p.id)
		p.redialIfMisaddressed(err)
	}
}

// runSnapshots sends snapshots to the peer one at a time.
func (p *peer) runSnapshots() {
	defer p.wg.Done()
	for {
		select {
		case m := <-p.snapc:
			var index uint64
			if m.Snapshot != nil {
				index = m.Snapshot.Metadata.Index
			}
			start := time.Now()
			size, err := p.sendSnapshot(m)
			if err != nil {
				log.Printf("transport: snapshot at index %d to peer %d failed after %v: %v",
					index, p.id, time.Since(start).Round(time.Millisecond), err)
				p.t.reportUnreachable(p.id)
			} else {
				log.Printf("transport: sent snapshot at index %d to peer %d: %d bytes in %v",
					index, p.id, size, time.Since(start).Round(time.Millisecond))
			}
			p.t.reportSnapshot(p.id, err == nil)
		case <-p.ctx.Done():
			return
		}
	}
}

// sendSnapshot streams one MsgSnap to the peer in snapshotChunkSize pieces
// and returns its size once the peer has stepped it.
func (p *peer) sendSnapshot(m raftpb.Message) (int, error) {
	data, err := m.Marshal()
	if err != nil {
		return 0, err
	}
	timeout := snapshotBaseTimeout + time.Duration(len(data)/snapshotMinRate)*time.Second
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	stream, err := p.rpc().SendSnapshot(ctx)
	if err != nil {
		return 0, err
	}
	for off := 0; off < len(data); off += snapshotChunkSize {
		end := min(off+snapshotChunkSize, len(data))
		err := stream.Send(&seatsafev1.SnapshotChunk{GroupId: p.t.groupID, From: p.t.self, Data: data[off:end]})
		if errors.Is(err, io.EOF) {
			break // the server ended the stream early; CloseAndRecv has its error
		}
		if err != nil {
			return 0, err
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return 0, err
	}
	return len(data), nil
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
	p.mu.Lock()
	old := p.conn
	p.conn, p.client = conn, seatsafev1.NewRaftTransportClient(conn)
	p.mu.Unlock()
	if old != nil {
		_ = old.Close() // ends any call still on it, which then fails and is retried
	}
	return nil
}

// rpc returns the client for the peer's current connection.
func (p *peer) rpc() seatsafev1.RaftTransportClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

// redialIfMisaddressed starts a new connection when err says our batch or
// snapshot reached the wrong node: the new connection looks the peer's name
// up again and finds its current address.
func (p *peer) redialIfMisaddressed(err error) {
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), misaddressed) {
		return
	}
	log.Printf("transport: peer %d (%s): %v; reconnecting", p.id, p.addr, err)
	if derr := p.dial(); derr != nil {
		log.Printf("transport: peer %d: redial: %v", p.id, derr)
	}
}

func (p *peer) close() {
	p.once.Do(p.cancel)
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.conn.Close()
}
