// Package raftnode runs one member of a Raft group on top of etcd raft.
//
// It is generic over the state machine: log entries are opaque bytes, and the
// shard groups (seatsm) and the coordinator group plug in their own
// StateMachine. The node owns the Ready loop (persist, send, apply, advance)
// and lets callers propose an entry and wait for the result its apply produced.
package raftnode

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// StateMachine is applied to committed entries in log order, from a single
// goroutine. Apply must be deterministic: the same entries in the same order
// must produce the same state and the same results on every replica, so it
// must not read clocks, use randomness, or depend on map iteration order.
type StateMachine interface {
	// Apply applies one committed entry and returns the result that the
	// proposer, if it is waiting on this node, receives from Propose.
	Apply(index uint64, data []byte) any
	// Snapshot serializes the full state. Used from Day 3 on.
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
	// Entries after the snapshot are kept.
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
	Logger         raft.Logger   // default: etcd raft's logger

	// SnapshotEntries is how many entries may be applied after the last
	// snapshot before the node takes a new one and compacts its log.
	SnapshotEntries uint64 // default DefaultSnapshotEntries
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
	raft    raft.Node
	storage *raft.MemoryStorage
	applied atomic.Uint64

	mu      sync.Mutex
	waiters map[uint64]chan outcome

	// Owned by the run loop: the leader and role from the last SoftState.
	lead     uint64
	isLeader bool
	// Owned by the run loop: the current membership (for snapshot metadata)
	// and the index of the newest snapshot.
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
		storage: storage,
		waiters: make(map[uint64]chan outcome),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
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

func (n *Node) run() {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.raft.Tick()
		case rd := <-n.raft.Ready():
			if err := n.handleReady(rd); err != nil {
				// Continuing after a failed persist or apply could make this
				// replica diverge, so stop hard, as etcd does.
				panic(fmt.Sprintf("raftnode %d: %v", n.cfg.ID, err))
			}
			n.raft.Advance()
		case <-n.stop:
			n.raft.Stop()
			return
		}
	}
}

// handleReady follows the etcd raft contract: persist, then send, then apply.
func (n *Node) handleReady(rd raft.Ready) error {
	// 1. Persist: one batch, fsynced when rd.MustSync (new entries, a term
	//    or vote change) or a snapshot needs it, then the in-memory copy Raft
	//    reads from.
	if n.cfg.Store != nil {
		if err := n.cfg.Store.Save(rd.HardState, rd.Entries, rd.Snapshot, rd.MustSync); err != nil {
			return fmt.Errorf("persist: %w", err)
		}
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("apply snapshot to storage: %w", err)
		}
		if err := n.cfg.SM.Restore(rd.Snapshot.Data); err != nil {
			return fmt.Errorf("restore state machine: %w", err)
		}
		n.applied.Store(rd.Snapshot.Metadata.Index)
		n.confState, n.snapIndex = rd.Snapshot.Metadata.ConfState, rd.Snapshot.Metadata.Index
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := n.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("set hard state: %w", err)
		}
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		return fmt.Errorf("append entries: %w", err)
	}

	// 2. Send.
	if len(rd.Messages) > 0 {
		n.cfg.Transport.Send(rd.Messages)
	}

	// 3. Apply committed entries in order.
	for _, e := range rd.CommittedEntries {
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

	// 4. Snapshot and compact once enough entries have been applied.
	if err := n.maybeSnapshot(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	// 5. A leadership change after applying, so results committed in this
	//    Ready still reach their callers: anything still waiting may never
	//    commit, so fail it now instead of letting it wait for its deadline.
	if n.leadershipChanged(rd.SoftState) {
		n.failPending(ErrLeadershipLost)
	}
	return nil
}

// maybeSnapshot takes a snapshot of the state machine at the applied index
// once SnapshotEntries entries have been applied since the last one, stores
// it, and compacts the log, keeping CatchUpEntries entries before it. It
// runs on the Raft loop right after applying, so the state machine and the
// applied index match exactly.
func (n *Node) maybeSnapshot() error {
	applied := n.applied.Load()
	if applied-n.snapIndex < n.cfg.SnapshotEntries {
		return nil
	}
	data, err := n.cfg.SM.Snapshot()
	if err != nil {
		return err
	}
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
	if n.cfg.Store != nil {
		if err := n.cfg.Store.SaveSnapshot(snap, compactTo); err != nil {
			return err
		}
	}
	if compactTo > 0 {
		if err := n.storage.Compact(compactTo); err != nil && !errors.Is(err, raft.ErrCompacted) {
			return err
		}
	}
	n.snapIndex = applied
	return nil
}

// leadershipChanged records a SoftState change and reports whether pending
// proposals should be failed: the known leader changed, or this node stopped
// being leader.
func (n *Node) leadershipChanged(ss *raft.SoftState) bool {
	if ss == nil {
		return false
	}
	wasLeader := n.isLeader
	changed := ss.Lead != n.lead || (wasLeader && ss.RaftState != raft.StateLeader)
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

// register picks a random proposal ID. IDs only need to be unique among the
// proposals currently waiting on this node; 64 random bits make a clash with
// an older entry still in the log vanishingly unlikely.
// outcome is what a waiting Propose receives: the apply result, or an error.
type outcome struct {
	val any
	err error
}

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
