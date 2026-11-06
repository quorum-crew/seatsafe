package raftnode

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// recordingStore is an in-memory LogStore that records every Save.
type recordingStore struct {
	mu    sync.Mutex
	saves []savedReady
}

type savedReady struct {
	hs       raftpb.HardState
	entries  int
	mustSync bool
}

func (s *recordingStore) Save(hs raftpb.HardState, ents []raftpb.Entry, _ raftpb.Snapshot, mustSync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves = append(s.saves, savedReady{hs: hs, entries: len(ents), mustSync: mustSync})
	return nil
}

func (s *recordingStore) SaveSnapshot(raftpb.Snapshot, uint64) error { return nil }

func (s *recordingStore) Load() (raftpb.HardState, raftpb.Snapshot, []raftpb.Entry, error) {
	return raftpb.HardState{}, raftpb.Snapshot{}, nil, nil
}

// The node asks for an fsync exactly when Raft needs one (#15): always for
// new entries or a term/vote change, and not for commit-index-only updates.
func TestSaveRequestsSyncOnlyWhenNeeded(t *testing.T) {
	net := NewNetwork()
	peers := []uint64{1, 2, 3}
	stores := map[uint64]*recordingStore{}
	var nodes []*Node
	for _, id := range peers {
		st := &recordingStore{}
		n, err := Start(Config{ID: id, Peers: peers, SM: &logSM{}, Transport: net, Store: st,
			TickInterval: 10 * time.Millisecond, Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		net.Add(id, n)
		stores[id], nodes = st, append(nodes, n)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		net.Close()
	}()

	var lead *Node
	for deadline := time.Now().Add(5 * time.Second); lead == nil; {
		for _, n := range nodes {
			if n.Status().IsLeader() {
				lead = n
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := range 50 {
		propose(t, lead, fmt.Sprintf("e-%d", i))
	}
	time.Sleep(100 * time.Millisecond) // let followers learn the final commit index

	unsynced := 0
	for id, st := range stores {
		st.mu.Lock()
		var prev raftpb.HardState
		for i, s := range st.saves {
			termOrVote := s.hs.Term != 0 && (s.hs.Term != prev.Term || s.hs.Vote != prev.Vote)
			if (s.entries > 0 || termOrVote) && !s.mustSync {
				t.Errorf("node %d save %d: %d entries, term/vote change %v, but mustSync=false", id, i, s.entries, termOrVote)
			}
			if !s.mustSync {
				unsynced++
			}
			if s.hs.Term != 0 {
				prev = s.hs
			}
		}
		st.mu.Unlock()
	}
	if unsynced == 0 {
		t.Error("every Save asked for an fsync; commit-index-only updates should not")
	}
}
