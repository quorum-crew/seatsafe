// Package raftnode runs one member of a Raft group on top of etcd raft.
//
// It is generic over the state machine: log entries are opaque bytes, and the
// shard groups (seatsm) and the coordinator group plug in their own
// StateMachine. The node runs the Ready loop (persist, send, advance) and a
// separate apply goroutine, and lets callers propose an entry and wait for the
// result its apply produced.
package raftnode

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// StateMachine is applied to committed entries in log order, from a single
// goroutine (the node's apply goroutine, never the Raft loop). Apply must be deterministic: the same entries in the same order
// must produce the same state and the same results on every replica, so it
// must not read clocks, use randomness, or depend on map iteration order.
type StateMachine interface {
	// Apply applies one committed entry and returns the result that the
	// proposer, if it is waiting on this node, receives from Propose.
	Apply(index uint64, data []byte) any
	// Snapshot serializes the full state at the last applied entry. It is
	// called from the apply goroutine, so it may take a while without
	// holding up heartbeats or elections.
	Snapshot() ([]byte, error)
	// Restore replaces the full state with a snapshot.
	Restore(data []byte) error
}

// LogStore persists what Raft needs to survive a restart. raftstore.Pebble
// implements it.
type LogStore interface {
	// Save writes one Ready's HardState, entries and snapshot in one batch.
	// mustSync is raft.Ready.MustSync: when true (and whenever there is a
	// snapshot) the batch must be durable (fsynced) before Save returns.
	Save(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot, mustSync bool) error
	// Load returns everything saved so far; empty for a fresh store. Entries
	// are only those after the snapshot.
	Load() (raftpb.HardState, raftpb.Snapshot, []raftpb.Entry, error)
	// SaveSnapshot durably stores a snapshot this node took of its own state
	// and drops log entries up to and including compactTo (0 = keep all).
	// Entries after the snapshot are kept. It is called from the apply
	// goroutine, concurrently with Save, and must ignore a snapshot older
	// than one Save already stored.
	SaveSnapshot(snap raftpb.Snapshot, compactTo uint64) error
}

// Transport delivers outgoing Raft messages to peers. Send must not block for
// long. It may drop messages; Raft retransmits.
type Transport interface {
	Send(msgs []raftpb.Message)
}

// Config configures a Node.
type Config struct {
	ID        uint64   // this node's Raft ID (1..N within its group)
	Peers     []uint64 // every member's ID, including ID
	SM        StateMachine
	Transport Transport
	Store     LogStore // nil = keep the log in memory only (tests)

	TickInterval   time.Duration // default 100ms
	ElectionTicks  int           // default 10, so elections after ~1s
	HeartbeatTicks int           // default 1
	Logger         raft.Logger   // default: a stderr logger (etcd raft's own default for Raft itself)

	// SnapshotEntries is how many entries may be applied after the last
	// snapshot before the node takes a new one and compacts its log.
	SnapshotEntries uint64 // default DefaultSnapshotEntries; Start always sets it
	// CatchUpEntries is how many entries before the snapshot are kept after
	// compaction, so a follower that is only slightly behind can catch up
	// from the log instead of needing the whole snapshot.
	CatchUpEntries uint64 // default DefaultCatchUpEntries
}

// Snapshot defaults (design doc: snapshot every 10,000 applied entries).
const (
	DefaultSnapshotEntries = 10000
	DefaultCatchUpEntries  = 5000
)

// ErrStopped is returned for calls on a node that has been stopped.
var ErrStopped = errors.New("raftnode: stopped")

// ErrLeadershipLost is returned by Propose when the group's leader changed (or
// this node stopped being leader) before the proposal was applied here. The
// entry may or may not commit later, so callers retry with the same request ID
// and let the state machine's dedup sort it out.
var ErrLeadershipLost = errors.New("raftnode: leadership changed before the proposal was applied")

// Status is a point-in-time view of the node.
type Status struct {
	ID        uint64
	Leader    uint64 // 0 = no known leader
	Term      uint64
	Role      raft.StateType
	LastIndex uint64 // index of the last entry in this node's log
	// SnapshotIndex is the index of this node's newest snapshot (0 = none).
	SnapshotIndex uint64
	Commit        uint64
	Applied       uint64

	// Followers is filled on the leader only: how far each follower's log
	// has caught up. Used for replication-lag metrics.
	Followers []FollowerProgress
}

// FollowerProgress is the leader's view of one follower.
type FollowerProgress struct {
	ID           uint64
	Match        uint64 // highest log index known to be on that follower
	RecentActive bool   // heard from within the last election timeout
}

// IsLeader reports whether the node was leader when the status was taken.
func (s Status) IsLeader() bool { return s.Role == raft.StateLeader }

// Node is one member of a Raft group.
type Node struct {
	cfg     Config
	log     raft.Logger
	raft    raft.Node
	storage *raft.MemoryStorage
	applied atomic.Uint64
	applyq  applyQueue

	mu      sync.Mutex
	waiters map[uint64]chan outcome

	// Owned by the apply goroutine: the leader and role from the last
	// SoftState it processed.
	lead     uint64
	isLeader bool
	// Owned by the apply goroutine: the current membership (for snapshot
	// metadata) and the index of the newest snapshot.
	confState raftpb.ConfState
	snapIndex uint64

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// Start starts a group member. If cfg.Store already holds a log, the node
// restarts from it: the state machine is rebuilt by replaying the snapshot and
// committed entries, so it must start out empty. Otherwise it joins a fresh
// group of cfg.Peers.
func Start(cfg Config) (*Node, error) {
	switch {
	case cfg.ID == 0:
		return nil, errors.New("raftnode: ID must be > 0")
	case !slices.Contains(cfg.Peers, cfg.ID):
		return nil, fmt.Errorf("raftnode: Peers %v must include ID %d", cfg.Peers, cfg.ID)
	case cfg.SM == nil:
		return nil, errors.New("raftnode: SM is required")
	case cfg.Transport == nil:
		return nil, errors.New("raftnode: Transport is required")
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = DefaultSnapshotEntries
	}
	if cfg.CatchUpEntries == 0 {
		cfg.CatchUpEntries = DefaultCatchUpEntries
	}

	storage := raft.NewMemoryStorage()
	rc := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   cfg.HeartbeatTicks,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		CheckQuorum:     true, // a leader cut off from a majority steps down
		PreVote:         true, // a partitioned node rejoining doesn't force an election
		Logger:          cfg.Logger,
	}
	peers := make([]raft.Peer, len(cfg.Peers))
	for i, id := range cfg.Peers {
		peers[i] = raft.Peer{ID: id}
	}

	n := &Node{
		cfg:     cfg,
		log:     cfg.Logger,
		storage: storage,
		waiters: make(map[uint64]chan outcome),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if n.log == nil {
		n.log = &raft.DefaultLogger{Logger: log.New(os.Stderr, "raftnode ", log.LstdFlags)}
	}
	n.applyq.ready = make(chan struct{}, 1)
	restart, err := n.loadState(rc)
	if err != nil {
		return nil, err
	}
	if restart {
		n.raft = raft.RestartNode(rc)
	} else {
		n.raft = raft.StartNode(rc, peers)
	}
	go n.run()
	return n, nil
}

// loadState fills the in-memory Raft storage from cfg.Store and reports
// whether there was anything to restart from.
func (n *Node) loadState(rc *raft.Config) (bool, error) {
	if n.cfg.Store == nil {
		return false, nil
	}
	hs, snap, ents, err := n.cfg.Store.Load()
	if err != nil {
		return false, err
	}
	if raft.IsEmptyHardState(hs) && raft.IsEmptySnap(snap) && len(ents) == 0 {
		return false, nil
	}
	if !raft.IsEmptySnap(snap) {
		if err := n.storage.ApplySnapshot(snap); err != nil {
			return false, fmt.Errorf("raftnode: load snapshot: %w", err)
		}
		if err := n.cfg.SM.Restore(snap.Data); err != nil {
			return false, fmt.Errorf("raftnode: restore state machine: %w", err)
		}
		n.applied.Store(snap.Metadata.Index)
		n.confState, n.snapIndex = snap.Metadata.ConfState, snap.Metadata.Index
	}
	if err := n.storage.SetHardState(hs); err != nil {
		return false, fmt.Errorf("raftnode: load hard state: %w", err)
	}
	if err := n.storage.Append(ents); err != nil {
		return false, fmt.Errorf("raftnode: load entries: %w", err)
	}
	// The state machine starts empty (or at the snapshot), so ask Raft to
	// hand back every committed entry after it.
	rc.Applied = snap.Metadata.Index
	return true, nil
}

// run is the Raft loop. It only persists, sends and hands committed work to
// the apply goroutine, so a slow apply or snapshot never delays heartbeats
// or votes: a 2 s snapshot on the loop would cost the group its leader.
func (n *Node) run() {
	defer close(n.done)
	applied := make(chan struct{})
	go n.applyLoop(applied)
	defer func() {
		n.applyq.close()
		<-applied
	}()

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.raft.Tick()
		case rd := <-n.raft.Ready():
			b, err := n.handleReady(rd)
			if err != nil {
				// Continuing after a failed persist could make this replica
				// diverge, so stop hard, as etcd does.
				panic(fmt.Sprintf("raftnode %d: %v", n.cfg.ID, err))
			}
			n.applyq.push(b)
			if b.done != nil {
				<-b.done
			}
			n.raft.Advance()
		case <-n.stop:
			n.raft.Stop()
			return
		}
	}
}

// applyBatch is the part of one Ready that the apply goroutine handles, in
// the order the Readies arrived.
type applyBatch struct {
	snap      raftpb.Snapshot // received from the leader: restore before entries
	entries   []raftpb.Entry  // committed entries, in log order
	softState *raft.SoftState // leadership as of this Ready, or nil
	// done is closed once the batch is applied, for a batch the loop must
	// wait for before Advance: one with a membership change, so Raft sees the
	// new configuration before it hands out more work.
	done chan struct{}
}

// handleReady persists a Ready and sends its messages, following the etcd
// raft contract (persist before send), and returns what is left to apply.
func (n *Node) handleReady(rd raft.Ready) (applyBatch, error) {
	// 1. Persist: one batch, fsynced when rd.MustSync (new entries, a term
	//    or vote change) or a snapshot needs it, then the in-memory copy Raft
	//    reads from.
	if n.cfg.Store != nil {
		if err := n.cfg.Store.Save(rd.HardState, rd.Entries, rd.Snapshot, rd.MustSync); err != nil {
			return applyBatch{}, fmt.Errorf("persist: %w", err)
		}
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return applyBatch{}, fmt.Errorf("apply snapshot to storage: %w", err)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := n.storage.SetHardState(rd.HardState); err != nil {
			return applyBatch{}, fmt.Errorf("set hard state: %w", err)
		}
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		return applyBatch{}, fmt.Errorf("append entries: %w", err)
	}

	// 2. Send.
	if len(rd.Messages) > 0 {
		n.cfg.Transport.Send(rd.Messages)
	}

	// 3. Hand the rest to the apply goroutine.
	b := applyBatch{snap: rd.Snapshot, entries: rd.CommittedEntries}
	if rd.SoftState != nil {
		ss := *rd.SoftState
		b.softState = &ss
	}
	for _, e := range b.entries {
		if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
			b.done = make(chan struct{})
			break
		}
	}
	return b, nil
}

// applyLoop applies batches in order until the queue is closed. Batches still
// queued at Stop are dropped: they are committed and on disk, so a restart
// replays them.
func (n *Node) applyLoop(exited chan<- struct{}) {
	defer close(exited)
	for {
		b, ok := n.applyq.pop()
		if !ok {
			return
		}
		if err := n.apply(b); err != nil {
			// A replica that failed to apply an entry has diverged.
			panic(fmt.Sprintf("raftnode %d: %v", n.cfg.ID, err))
		}
	}
}

// apply restores a received snapshot, applies committed entries in order,
// takes a snapshot if one is due, and then acts on a leadership change.
func (n *Node) apply(b applyBatch) error {
	if b.done != nil {
		defer close(b.done)
	}
	if !raft.IsEmptySnap(b.snap) {
		start := time.Now()
		if err := n.cfg.SM.Restore(b.snap.Data); err != nil {
			return fmt.Errorf("restore state machine: %w", err)
		}
		n.applied.Store(b.snap.Metadata.Index)
		n.confState, n.snapIndex = b.snap.Metadata.ConfState, b.snap.Metadata.Index
		n.log.Infof("raftnode %d: restored snapshot at index %d (%d bytes) in %v",
			n.cfg.ID, b.snap.Metadata.Index, len(b.snap.Data), time.Since(start).Round(time.Millisecond))
	}

	for _, e := range b.entries {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				break // the empty entry a new leader appends
			}
			id, payload, err := decode(e.Data)
			if err != nil {
				return fmt.Errorf("entry %d: %w", e.Index, err)
			}
			n.deliver(id, n.cfg.SM.Apply(e.Index, payload))
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				return fmt.Errorf("entry %d: conf change: %w", e.Index, err)
			}
			n.confState = *n.raft.ApplyConfChange(cc)
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(e.Data); err != nil {
				return fmt.Errorf("entry %d: conf change v2: %w", e.Index, err)
			}
			n.confState = *n.raft.ApplyConfChange(cc)
		}
		n.applied.Store(e.Index)
	}

	// Snapshot and compact once enough entries have been applied.
	if err := n.maybeSnapshot(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	// A leadership change after applying, so results committed in this
	// Ready still reach their callers: anything still waiting may never
	// commit, so fail it now instead of letting it wait for its deadline.
	if n.leadershipChanged(b.softState) {
		n.failPending(ErrLeadershipLost)
	}
	return nil
}

// maybeSnapshot takes a snapshot of the state machine at the applied index
// once SnapshotEntries entries have been applied since the last one, stores
// it, and compacts the log, keeping CatchUpEntries entries before it. It runs
// on the apply goroutine right after applying, so the state machine and the
// applied index match exactly, and it logs how long each step took.
//
// The Raft loop keeps running meanwhile and may install a newer snapshot from
// the leader; then CreateSnapshot reports ErrSnapOutOfDate, or the store
// ignores this older one, and nothing is lost.
func (n *Node) maybeSnapshot() error {
	if n.cfg.SnapshotEntries == 0 {
		return nil // only a Node built without Start, in tests
	}
	applied := n.applied.Load()
	if applied-n.snapIndex < n.cfg.SnapshotEntries {
		return nil
	}
	start := time.Now()
	data, err := n.cfg.SM.Snapshot()
	if err != nil {
		return err
	}
	encoded := time.Since(start)
	snap, err := n.storage.CreateSnapshot(applied, &n.confState, data)
	if errors.Is(err, raft.ErrSnapOutOfDate) {
		return nil
	}
	if err != nil {
		return err
	}
	var compactTo uint64
	if applied > n.cfg.CatchUpEntries {
		compactTo = applied - n.cfg.CatchUpEntries
	}
	// Durable first: only then may entries be dropped.
	saveStart := time.Now()
	if n.cfg.Store != nil {
		if err := n.cfg.Store.SaveSnapshot(snap, compactTo); err != nil {
			return err
		}
	}
	saved := time.Since(saveStart)
	if compactTo > 0 {
		if err := n.storage.Compact(compactTo); err != nil && !errors.Is(err, raft.ErrCompacted) {
			return err
		}
	}
	n.snapIndex = applied
	n.log.Infof("raftnode %d: snapshot at index %d: %d bytes, encode %v, persist %v, log compacted through %d",
		n.cfg.ID, applied, len(data), encoded.Round(time.Millisecond), saved.Round(time.Millisecond), compactTo)
	return nil
}

// leadershipChanged records a SoftState change and reports whether pending
// proposals should be failed: the known leader changed, or this node stopped
// being leader.
//
// Winning an election with no known leader is not a change: Status reports
// leader before this Ready is persisted, so a proposal may already be waiting
// here, accepted by this node as leader. Anything forwarded to the old leader
// failed when that leader went away, and a proposal made with no known leader
// was blocked in raft.Propose until now.
func (n *Node) leadershipChanged(ss *raft.SoftState) bool {
	if ss == nil {
		return false
	}
	wasLeader := n.isLeader
	gained := n.lead == raft.None && ss.Lead == n.cfg.ID
	changed := (ss.Lead != n.lead && !gained) || (wasLeader && ss.RaftState != raft.StateLeader)
	n.lead, n.isLeader = ss.Lead, ss.RaftState == raft.StateLeader
	return changed
}

// Propose appends data to the log and waits until this node has applied it,
// returning the result of StateMachine.Apply.
//
// If the leader changes before the entry is applied here, Propose returns
// ErrLeadershipLost right away rather than waiting for ctx: the entry may
// have been lost. Callers retry with the same request ID, and the state
// machine's dedup makes the retry safe.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	id, ch := n.register()
	defer n.forget(id)

	if err := n.raft.Propose(ctx, encode(id, data)); err != nil {
		if errors.Is(err, raft.ErrStopped) {
			return nil, ErrStopped
		}
		return nil, err // e.g. raft.ErrProposalDropped when there is no leader
	}
	select {
	case out := <-ch:
		return out.val, out.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stop:
		return nil, ErrStopped
	}
}

// Step feeds a message received from a peer into Raft.
func (n *Node) Step(ctx context.Context, m raftpb.Message) error {
	return n.raft.Step(ctx, m)
}

// ReportUnreachable tells Raft that sending to a peer failed.
func (n *Node) ReportUnreachable(id uint64) { n.raft.ReportUnreachable(id) }

// ReportSnapshot tells Raft whether sending a snapshot to a peer succeeded.
func (n *Node) ReportSnapshot(id uint64, ok bool) {
	status := raft.SnapshotFinish
	if !ok {
		status = raft.SnapshotFailure
	}
	n.raft.ReportSnapshot(id, status)
}

// Status returns the node's current view of the group.
func (n *Node) Status() Status {
	st := n.raft.Status()
	out := Status{
		ID:      n.cfg.ID,
		Leader:  st.Lead,
		Term:    st.Term,
		Role:    st.RaftState,
		Commit:  st.Commit,
		Applied: n.applied.Load(),
	}
	if last, err := n.storage.LastIndex(); err == nil {
		out.LastIndex = last
	}
	if snap, err := n.storage.Snapshot(); err == nil {
		out.SnapshotIndex = snap.Metadata.Index
	}
	for id, pr := range st.Progress {
		if id != n.cfg.ID {
			out.Followers = append(out.Followers, FollowerProgress{ID: id, Match: pr.Match, RecentActive: pr.RecentActive})
		}
	}
	slices.SortFunc(out.Followers, func(a, b FollowerProgress) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Stop stops the node and waits for its loop to exit. Safe to call twice.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stop) })
	<-n.done
}

// outcome is what a waiting Propose receives: the apply result, or an error.
type outcome struct {
	val any
	err error
}

// register picks a random proposal ID. IDs only need to be unique among the
// proposals currently waiting on this node; 64 random bits make a clash with
// an older entry still in the log vanishingly unlikely.
func (n *Node) register() (uint64, chan outcome) {
	ch := make(chan outcome, 1)
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		id := rand.Uint64()
		if _, taken := n.waiters[id]; id != 0 && !taken {
			n.waiters[id] = ch
			return id, ch
		}
	}
}

func (n *Node) forget(id uint64) {
	n.mu.Lock()
	delete(n.waiters, id)
	n.mu.Unlock()
}

func (n *Node) deliver(id uint64, res any) {
	n.mu.Lock()
	ch, ok := n.waiters[id]
	delete(n.waiters, id)
	n.mu.Unlock()
	if ok {
		ch <- outcome{val: res} // buffered, never blocks
	}
}

// failPending fails every proposal still waiting on this node.
func (n *Node) failPending(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, ch := range n.waiters {
		ch <- outcome{err: err} // buffered, never blocks
		delete(n.waiters, id)
	}
}

// Entry framing: an 8-byte big-endian proposal ID, then the payload
// (a marshaled ShardCommand or CoordCommand).
const idLen = 8

func encode(id uint64, payload []byte) []byte {
	b := make([]byte, idLen+len(payload))
	binary.BigEndian.PutUint64(b, id)
	copy(b[idLen:], payload)
	return b
}

func decode(b []byte) (uint64, []byte, error) {
	if len(b) < idLen {
		return 0, nil, fmt.Errorf("entry too short (%d bytes)", len(b))
	}
	return binary.BigEndian.Uint64(b), b[idLen:], nil
}

// applyQueue is an unbounded FIFO from the Raft loop to the apply goroutine.
// Unbounded so the loop never blocks behind a slow apply; Raft already caps
// how many committed entries one Ready carries.
type applyQueue struct {
	mu     sync.Mutex
	items  []applyBatch
	closed bool
	ready  chan struct{} // capacity 1: "there may be something to pop"
}

func (q *applyQueue) push(b applyBatch) {
	q.mu.Lock()
	q.items = append(q.items, b)
	q.mu.Unlock()
	q.signal()
}

// close makes pop return false, dropping anything still queued.
func (q *applyQueue) close() {
	q.mu.Lock()
	q.closed, q.items = true, nil
	q.mu.Unlock()
	q.signal()
}

func (q *applyQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// pop waits for the next batch; false once the queue is closed.
func (q *applyQueue) pop() (applyBatch, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return applyBatch{}, false
		}
		if len(q.items) > 0 {
			b := q.items[0]
			q.items[0] = applyBatch{}
			q.items = q.items[1:]
			q.mu.Unlock()
			return b, true
		}
		q.mu.Unlock()
		<-q.ready
	}
}
