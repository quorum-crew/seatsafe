package raftnode

import (
	"context"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

// Network is an in-memory Transport that connects nodes in one process.
// Links can be cut to simulate partitions. It is meant for tests, including
// the fault-injection tests later on.
type Network struct {
	mu     sync.RWMutex
	inbox  map[uint64]chan raftpb.Message
	cut    map[[2]uint64]bool // directed link {from, to} is down
	closed bool
	stop   chan struct{}
	wg     sync.WaitGroup
}

// NewNetwork returns an empty network.
func NewNetwork() *Network {
	return &Network{
		inbox: make(map[uint64]chan raftpb.Message),
		cut:   make(map[[2]uint64]bool),
		stop:  make(chan struct{}),
	}
}

// Add connects a node, so messages addressed to its ID reach it.
func (nw *Network) Add(id uint64, n *Node) {
	ch := make(chan raftpb.Message, 4096)
	nw.mu.Lock()
	nw.inbox[id] = ch
	nw.mu.Unlock()

	nw.wg.Add(1)
	go func() {
		defer nw.wg.Done()
		for {
			select {
			case m := <-ch:
				_ = n.Step(context.Background(), m)
			case <-nw.stop:
				return
			}
		}
	}()
}

// Send implements Transport. Messages on a cut link, to an unknown node, or
// to a full inbox are dropped, as a real network might.
func (nw *Network) Send(msgs []raftpb.Message) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	if nw.closed {
		return
	}
	for _, m := range msgs {
		ch, ok := nw.inbox[m.To]
		if !ok || nw.cut[[2]uint64{m.From, m.To}] {
			continue
		}
		select {
		case ch <- m:
		default:
		}
	}
}

// Isolate cuts every link to and from id.
func (nw *Network) Isolate(id uint64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for other := range nw.inbox {
		if other != id {
			nw.cut[[2]uint64{id, other}] = true
			nw.cut[[2]uint64{other, id}] = true
		}
	}
}

// Heal restores every link.
func (nw *Network) Heal() {
	nw.mu.Lock()
	clear(nw.cut)
	nw.mu.Unlock()
}

// Close stops delivery. Call it after stopping the nodes.
func (nw *Network) Close() {
	nw.mu.Lock()
	nw.closed = true
	nw.mu.Unlock()
	close(nw.stop)
	nw.wg.Wait()
}
