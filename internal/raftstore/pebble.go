// Package raftstore persists a Raft node's log, HardState and snapshot in
// Pebble, so a node that crashes or restarts comes back with its log intact.
//
// Every Save is one Pebble batch. It is fsynced only when Raft needs it to be
// durable (new entries, a term or vote change, or a snapshot), so a Ready
// costs at most one fsync no matter how many entries it carries (group
// commit), and commit-index-only updates cost none.
package raftstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Keys:
//
//	"hs"            -> raftpb.HardState
//	"snap"          -> raftpb.Snapshot (metadata + state machine data)
//	"e" + index(8B) -> raftpb.Entry, big-endian index so keys sort in log order
var (
	keyHardState = []byte("hs")
	keySnapshot  = []byte("snap")
)

const entryPrefix = 'e'

// entriesEnd is the exclusive upper bound of every entry key.
var entriesEnd = []byte{entryPrefix + 1}

func entryKey(index uint64) []byte {
	k := make([]byte, 9)
	k[0] = entryPrefix
	binary.BigEndian.PutUint64(k[1:], index)
	return k
}

// Pebble is a durable Raft log store.
type Pebble struct {
	db *pebble.DB
}

// Open opens (or creates) the store in dir.
func Open(dir string) (*Pebble, error) {
	return open(dir, &pebble.Options{Logger: quietLogger{}})
}

func open(dir string, opts *pebble.Options) (*Pebble, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("raftstore: open %s: %w", dir, err)
	}
	return &Pebble{db: db}, nil
}

// Save writes one Ready's HardState, new entries and snapshot in a single
// batch. Empty arguments are skipped, and an empty batch writes nothing.
//
// mustSync is raft.Ready.MustSync: true when the term, vote or new entries
// must be durable before messages go out. The batch is then fsynced; so is
// any batch carrying a snapshot. Otherwise (for example a HardState whose
// only change is the commit index, which Raft can recompute after a crash)
// the batch is committed without an fsync.
func (p *Pebble) Save(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot, mustSync bool) error {
	b := p.db.NewBatch()
	defer b.Close()

	if !raft.IsEmptySnap(snap) {
		data, err := snap.Marshal()
		if err != nil {
			return err
		}
		if err := b.Set(keySnapshot, data, nil); err != nil {
			return err
		}
		// A snapshot received from the leader replaces the whole log, like
		// MemoryStorage.ApplySnapshot: entries after its index may conflict
		// with the leader's log, so they go too. Any entries in this Ready
		// come after the snapshot and are written below.
		if err := b.DeleteRange(entryKey(0), entriesEnd, nil); err != nil {
			return err
		}
	}
	if len(ents) > 0 {
		for i := range ents {
			data, err := ents[i].Marshal()
			if err != nil {
				return err
			}
			if err := b.Set(entryKey(ents[i].Index), data, nil); err != nil {
				return err
			}
		}
		// New entries overwrite a conflicting tail: drop anything after them.
		last := ents[len(ents)-1].Index
		if last < math.MaxUint64 {
			if err := b.DeleteRange(entryKey(last+1), entryKey(math.MaxUint64), nil); err != nil {
				return err
			}
		}
	}
	if !raft.IsEmptyHardState(hs) {
		data, err := hs.Marshal()
		if err != nil {
			return err
		}
		if err := b.Set(keyHardState, data, nil); err != nil {
			return err
		}
	}
	if b.Empty() {
		return nil
	}
	if mustSync || !raft.IsEmptySnap(snap) {
		return b.Commit(pebble.Sync)
	}
	return b.Commit(pebble.NoSync)
}

// SaveSnapshot durably stores a snapshot this node took of its own state and
// drops entries up to and including compactTo (0 = none). Unlike a snapshot
// received in Save, entries after it are kept: they are this node's own log.
func (p *Pebble) SaveSnapshot(snap raftpb.Snapshot, compactTo uint64) error {
	data, err := snap.Marshal()
	if err != nil {
		return err
	}
	b := p.db.NewBatch()
	defer b.Close()
	if err := b.Set(keySnapshot, data, nil); err != nil {
		return err
	}
	if compactTo > 0 {
		if err := b.DeleteRange(entryKey(0), entryKey(compactTo+1), nil); err != nil {
			return err
		}
	}
	return b.Commit(pebble.Sync)
}

// Load returns everything saved, for restarting a node. A fresh store returns
// empty values.
func (p *Pebble) Load() (raftpb.HardState, raftpb.Snapshot, []raftpb.Entry, error) {
	var (
		hs   raftpb.HardState
		snap raftpb.Snapshot
		ents []raftpb.Entry
	)
	if err := p.get(keyHardState, hs.Unmarshal); err != nil {
		return hs, snap, nil, fmt.Errorf("raftstore: load hard state: %w", err)
	}
	if err := p.get(keySnapshot, snap.Unmarshal); err != nil {
		return hs, snap, nil, fmt.Errorf("raftstore: load snapshot: %w", err)
	}

	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: entryKey(snap.Metadata.Index + 1),
		UpperBound: []byte{entryPrefix + 1},
	})
	if err != nil {
		return hs, snap, nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var e raftpb.Entry
		if err := e.Unmarshal(it.Value()); err != nil {
			return hs, snap, nil, fmt.Errorf("raftstore: load entry: %w", err)
		}
		ents = append(ents, e)
	}
	return hs, snap, ents, it.Error()
}

func (p *Pebble) get(key []byte, unmarshal func([]byte) error) error {
	v, closer, err := p.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	return unmarshal(v)
}

// Close closes the store. Stop the node first.
func (p *Pebble) Close() error { return p.db.Close() }

// quietLogger drops Pebble's informational logs and keeps errors.
type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{}) {}
func (quietLogger) Errorf(format string, args ...interface{}) {
	log.Printf("pebble: "+format, args...)
}
func (quietLogger) Fatalf(format string, args ...interface{}) {
	log.Fatalf("pebble: "+format, args...)
}
