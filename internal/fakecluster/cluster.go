package fakecluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// Options configure a Cluster.
type Options struct {
	// Listen opens the listener for a node address from the shard map.
	Listen func(addr string) (net.Listener, error)
	// DialOptions let the coordinator reach the shards (e.g. a bufconn dialer).
	DialOptions []grpc.DialOption
	// Clock stamps NowMs on every shard command. Defaults to time.Now.
	Clock func() time.Time
}

// Cluster is a full fake cluster: every shard group of the map plus the
// coordinator group, each node on its own gRPC server.
type Cluster struct {
	Map         *shardmap.Map
	Shards      map[uint32]*FakeGroup
	Coordinator *FakeCoordinator

	dialOpts []grpc.DialOption
	clients  map[uint32]*groupclient.Group
	servers  []*grpc.Server
}

// New builds the groups, seeds each shard from seeds (a shard without a seed
// file stays unseeded), and starts serving every node.
func New(sm *shardmap.Map, seeds map[uint32]*seed.Loaded, opt Options) (_ *Cluster, err error) {
	if opt.Listen == nil {
		return nil, errors.New("fakecluster: Options.Listen is required")
	}
	c := &Cluster{Map: sm, Shards: map[uint32]*FakeGroup{}, dialOpts: opt.DialOptions, clients: map[uint32]*groupclient.Group{}}
	defer func() {
		if err != nil {
			c.Close() // whatever was started before the failure
		}
	}()

	// The coordinator's shard clients keep their metrics to themselves.
	gm := groupclient.NewMetrics(metrics.NewRegistry())
	for _, id := range sm.ShardIDs() {
		g := NewFakeGroup(id, sm.ShardNodes(id), opt.Clock)
		if l := seeds[id]; l != nil {
			if err := g.Seed(l); err != nil {
				return nil, err
			}
		}
		c.Shards[id] = g
		cl, err := groupclient.New(fmt.Sprintf("shard-%d", id), sm.ShardNodes(id), groupclient.Config{DialOptions: opt.DialOptions}, gm)
		if err != nil {
			return nil, err
		}
		c.clients[id] = cl
		for _, n := range g.Nodes {
			if err := c.serve(opt.Listen, n.Addr, n.Register); err != nil {
				return nil, err
			}
		}
	}
	c.Coordinator = NewFakeCoordinator(sm.CoordinatorNodes(), c.clients)
	for _, n := range c.Coordinator.Nodes {
		if err := c.serve(opt.Listen, n.Addr, n.Register); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) serve(listen func(string) (net.Listener, error), addr string, register func(grpc.ServiceRegistrar)) error {
	lis, err := listen(addr)
	if err != nil {
		return fmt.Errorf("fakecluster: listen %s: %w", addr, err)
	}
	s := grpc.NewServer()
	register(s)
	c.servers = append(c.servers, s)
	go func() { _ = s.Serve(lis) }()
	return nil
}

// DialOptions returns the options a client needs to reach this cluster.
func (c *Cluster) DialOptions() []grpc.DialOption { return c.dialOpts }

// Close stops every server and the coordinator's background work.
func (c *Cluster) Close() {
	if c.Coordinator != nil {
		c.Coordinator.Close()
	}
	for _, s := range c.servers {
		s.Stop()
	}
	for _, cl := range c.clients {
		_ = cl.Close()
	}
}

// NewBufconn starts the cluster on in-memory listeners. Node addresses stay
// as written in the shard map; DialOptions routes them to the listeners.
func NewBufconn(sm *shardmap.Map, seeds map[uint32]*seed.Loaded, clock func() time.Time) (*Cluster, error) {
	listeners := map[string]*bufconn.Listener{}
	all := sm.CoordinatorNodes()
	for _, id := range sm.ShardIDs() {
		all = append(all, sm.ShardNodes(id)...)
	}
	for _, a := range all {
		listeners[a] = bufconn.Listen(1 << 20)
	}
	dial := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		lis, ok := listeners[addr]
		if !ok {
			return nil, fmt.Errorf("fakecluster: no node at %s", addr)
		}
		return lis.DialContext(ctx)
	})
	return New(sm, seeds, Options{
		Listen:      func(addr string) (net.Listener, error) { return listeners[addr], nil },
		DialOptions: []grpc.DialOption{dial},
		Clock:       clock,
	})
}
