// Command coordinator runs one node of the 2PC coordinator group (Raft group
// 0). The group's leader serves CoordinatorService: SubmitOrder for the
// router and GetDecision for the storage nodes.
//
// Environment (set by docker-compose):
//
//	NODE_ID    this node's name, e.g. coord-2; must appear in PEERS
//	PEERS      gRPC addresses of the group's members, in Raft ID order:
//	           coord-1:7000,coord-2:7000,coord-3:7000. They must match the
//	           coordinator nodes in the shard map, which is where the router
//	           and the storage nodes look for a redirect's target
//	SHARDMAP   shard map file, for the participant groups' addresses
//	           (default /etc/seatsafe/shardmap.json)
//	GRPC_ADDR  gRPC listen address (default :7000)
//	HTTP_ADDR  health and metrics listen address (default :8080)
//	DATA_DIR   Pebble directory for the Raft log (default /data)
//	HOLD_TTL_MS      hold_ttl_ms sent with every Prepare (default 5000)
//	GC_RETENTION_MS  how long a finished txn is kept, so a retried order
//	                 still finds its answer (default 120000, 2 min)
//	RAFT_TICK_MS, RAFT_ELECTION_TICKS, RAFT_HEARTBEAT_TICKS
//	           Raft timing (defaults 100 ms, 10 ticks, 1 tick)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/quorum-crew/seatsafe/internal/coordinator"
	"github.com/quorum-crew/seatsafe/internal/coordsm"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/raftmetrics"
	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/raftstore"
	"github.com/quorum-crew/seatsafe/internal/server"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
	"github.com/quorum-crew/seatsafe/internal/transport"
)

// groupID is the coordinator's Raft group.
const groupID = 0

func main() {
	if err := run(); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}

func run() error {
	info := server.InfoFromEnv("coordinator")
	raftID, ids, addrs, err := parsePeers(info.NodeID, os.Getenv("PEERS"))
	if err != nil {
		return err
	}
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	sm, err := shardmap.Load(server.Getenv("SHARDMAP", "/etc/seatsafe/shardmap.json"))
	if err != nil {
		return err
	}

	// Participant groups: one client per shard of the map.
	reg := metrics.NewRegistry()
	gm := groupclient.NewMetrics(reg)
	shards := map[uint32]*groupclient.Group{}
	defer func() {
		for _, g := range shards {
			_ = g.Close()
		}
	}()
	for _, id := range sm.ShardIDs() {
		g, err := groupclient.New(fmt.Sprintf("shard-%d", id), sm.ShardNodes(id), groupclient.Config{}, gm)
		if err != nil {
			return err
		}
		shards[id] = g
	}

	store, err := raftstore.Open(server.Getenv("DATA_DIR", "/data"))
	if err != nil {
		return err
	}
	defer store.Close()

	tr, err := transport.New(groupID, raftID, addrs)
	if err != nil {
		return err
	}
	defer tr.Close()

	machine := coordsm.New()
	node, err := raftnode.Start(raftnode.Config{
		ID: raftID, Peers: ids, SM: coordinator.Applier{SM: machine}, Transport: tr, Store: store,
		TickInterval: cfg.tick, ElectionTicks: cfg.election, HeartbeatTicks: cfg.heartbeat,
	})
	if err != nil {
		return err
	}
	defer node.Stop()
	tr.SetNode(node)

	coord, err := coordinator.New(coordinator.Config{
		Node: node, SM: machine, Addrs: addrs, Shards: shards,
		HoldTTL: cfg.holdTTL, GCRetention: cfg.retention, Registry: reg, Logf: log.Printf,
	})
	if err != nil {
		return err
	}
	defer coord.Close()

	// gRPC: Raft traffic, node status (leader discovery), and the coordinator API.
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(transport.MaxMsgSize))
	tr.Register(gs)
	transport.NewStatusServer(info.NodeID, groupID, node, addrs).Register(gs)
	coord.Register(gs)
	lis, err := net.Listen("tcp", server.Getenv("GRPC_ADDR", ":7000"))
	if err != nil {
		return err
	}
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("grpc: %v", err)
		}
	}()
	coord.Start()

	// HTTP: /healthz (process is up), /readyz (the group has a leader),
	// /metrics (coordinator, shard clients, Raft).
	mux := http.NewServeMux()
	mux.Handle("/", server.Handler(info, reg.Write, coord.WriteMetrics,
		raftmetrics.Writer(strconv.Itoa(groupID), info.NodeID, node, raftmetrics.NamesFromAddrs(addrs))))
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		st := node.Status()
		if st.Leader == 0 {
			http.Error(w, "not ready: no leader", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "ready node=%s leader=%d term=%d applied=%d\n", info.NodeID, st.Leader, st.Term, st.Applied)
	})
	hs := &http.Server{Addr: server.Getenv("HTTP_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("coordinator %s: raft id %d, peers %v, shards %v, raft tick %v, election %d ticks, heartbeat %d ticks, hold ttl %v, gc retention %v",
		info.NodeID, raftID, addrs, sm.ShardIDs(), cfg.tick, cfg.election, cfg.heartbeat, cfg.holdTTL, cfg.retention)
	<-ctx.Done()

	log.Printf("coordinator %s: shutting down", info.NodeID)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdownCtx)
	coord.Close() // orders in progress stop; the next leader finishes them
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		gs.Stop()
	}
	return nil // deferred: node.Stop, tr.Close, store.Close
}

// parsePeers turns PEERS into Raft IDs (1-based position in the list) and
// addresses, and finds this node's ID by matching NODE_ID to a host name.
func parsePeers(nodeID, peers string) (self uint64, ids []uint64, addrs map[uint64]string, err error) {
	addrs = map[uint64]string{}
	for i, p := range strings.Split(peers, ",") {
		p = strings.TrimSpace(p)
		host, _, err := net.SplitHostPort(p)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("PEERS entry %q: %w", p, err)
		}
		id := uint64(i + 1)
		ids = append(ids, id)
		addrs[id] = p
		if host == nodeID {
			self = id
		}
	}
	if self == 0 {
		return 0, nil, nil, fmt.Errorf("NODE_ID %q is not a host in PEERS %q", nodeID, peers)
	}
	return self, ids, addrs, nil
}

type config struct {
	tick                time.Duration
	election, heartbeat int
	holdTTL, retention  time.Duration
}

// configFromEnv reads the Raft timing, HOLD_TTL_MS and GC_RETENTION_MS.
func configFromEnv() (config, error) {
	var c config
	var err error
	ms := func(key string, def int) time.Duration {
		var n int
		if err == nil {
			n, err = positiveIntEnv(key, def)
		}
		return time.Duration(n) * time.Millisecond
	}
	ticks := func(key string, def int) int {
		var n int
		if err == nil {
			n, err = positiveIntEnv(key, def)
		}
		return n
	}
	c.tick = ms("RAFT_TICK_MS", 100)
	c.election = ticks("RAFT_ELECTION_TICKS", 10)
	c.heartbeat = ticks("RAFT_HEARTBEAT_TICKS", 1)
	c.holdTTL = ms("HOLD_TTL_MS", int(coordinator.DefaultHoldTTL/time.Millisecond))
	c.retention = ms("GC_RETENTION_MS", int(coordinator.DefaultGCRetention/time.Millisecond))
	if err != nil {
		return config{}, err
	}
	if c.election <= c.heartbeat {
		return config{}, fmt.Errorf("RAFT_ELECTION_TICKS (%d) must be greater than RAFT_HEARTBEAT_TICKS (%d)", c.election, c.heartbeat)
	}
	return c, nil
}

func positiveIntEnv(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive whole number, got %q", key, v)
	}
	return n, nil
}
