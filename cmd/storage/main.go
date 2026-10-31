// Command storage runs one storage node: a member of one shard's Raft group.
//
// Environment (set by docker-compose):
//
//	NODE_ID    this node's name, e.g. shard2-node3; must appear in PEERS
//	SHARD_ID   the shard (= Raft group) this node belongs to, e.g. 2
//	PEERS      gRPC addresses of the group's members, in Raft ID order:
//	           shard2-node1:7000,shard2-node2:7000,shard2-node3:7000
//	GRPC_ADDR  gRPC listen address (default :7000)
//	HTTP_ADDR  health and metrics listen address (default :8080)
//	DATA_DIR   Pebble directory for the Raft log (default /data)
//	SEED_DIR   directory holding shard{N}.json (default /seed)
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

	"github.com/quorum-crew/seatsafe/internal/raftnode"
	"github.com/quorum-crew/seatsafe/internal/raftstore"
	"github.com/quorum-crew/seatsafe/internal/seatsm"
	"github.com/quorum-crew/seatsafe/internal/server"
	"github.com/quorum-crew/seatsafe/internal/shard"
	"github.com/quorum-crew/seatsafe/internal/transport"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("storage: %v", err)
	}
}

func run() error {
	info := server.InfoFromEnv("storage")
	shardID, err := strconv.ParseUint(os.Getenv("SHARD_ID"), 10, 32)
	if err != nil || shardID == 0 {
		return fmt.Errorf("SHARD_ID must be a positive number, got %q", os.Getenv("SHARD_ID"))
	}
	raftID, ids, addrs, err := parsePeers(info.NodeID, os.Getenv("PEERS"))
	if err != nil {
		return err
	}

	store, err := raftstore.Open(getenv("DATA_DIR", "/data"))
	if err != nil {
		return err
	}
	defer store.Close()

	tr, err := transport.New(uint32(shardID), raftID, addrs)
	if err != nil {
		return err
	}
	defer tr.Close()

	timing, err := raftTimingFromEnv()
	if err != nil {
		return err
	}

	sm := seatsm.New()
	node, err := raftnode.Start(raftnode.Config{
		ID: raftID, Peers: ids, SM: shard.Applier{SM: sm}, Transport: tr, Store: store,
		TickInterval: timing.tick, ElectionTicks: timing.election, HeartbeatTicks: timing.heartbeat,
	})
	if err != nil {
		return err
	}
	defer node.Stop()
	tr.SetNode(node)

	// gRPC: Raft traffic, node status, and the shard API.
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(transport.MaxMsgSize))
	tr.Register(gs)
	transport.NewStatusServer(info.NodeID, uint32(shardID), node, addrs).Register(gs)
	shard.NewService(node, sm, addrs).Register(gs)
	lis, err := net.Listen("tcp", getenv("GRPC_ADDR", ":7000"))
	if err != nil {
		return err
	}
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Printf("grpc: %v", err)
		}
	}()

	// HTTP: /healthz (process is up), /readyz (leader known and shard seeded), /metrics.
	mux := http.NewServeMux()
	mux.Handle("/", server.Handler(info))
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		st := node.Status()
		_, applied, total, seeded := sm.SeedStatus()
		if st.Leader == 0 || !seeded {
			http.Error(w, fmt.Sprintf("not ready: leader=%d seeded %d/%d chunks", st.Leader, applied, total), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "ready node=%s leader=%d term=%d applied=%d\n", info.NodeID, st.Leader, st.Term, st.Applied)
	})
	hs := &http.Server{Addr: getenv("HTTP_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go (&shard.Seeder{Node: node, SM: sm, Load: seedLoader(uint32(shardID)), Logf: log.Printf}).Run(ctx)
	go (&shard.Collector{Node: node, Logf: log.Printf}).Run(ctx)

	log.Printf("storage %s: shard %d, raft id %d, peers %v, raft tick %v, election %d ticks, heartbeat %d ticks",
		info.NodeID, shardID, raftID, addrs, timing.tick, timing.election, timing.heartbeat)
	<-ctx.Done()

	log.Printf("storage %s: shutting down", info.NodeID)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdownCtx)
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

// raftTiming is the Raft clock: how often the node ticks, and how many ticks
// make an election timeout and a heartbeat interval.
type raftTiming struct {
	tick      time.Duration
	election  int
	heartbeat int
}

// raftTimingFromEnv reads RAFT_TICK_MS, RAFT_ELECTION_TICKS and
// RAFT_HEARTBEAT_TICKS. Unset variables keep raftnode's defaults (100 ms, 10
// ticks, 1 tick). etcd/raft requires the election timeout to be longer than
// the heartbeat interval.
func raftTimingFromEnv() (raftTiming, error) {
	tickMS, err := positiveIntEnv("RAFT_TICK_MS", 100)
	if err != nil {
		return raftTiming{}, err
	}
	election, err := positiveIntEnv("RAFT_ELECTION_TICKS", 10)
	if err != nil {
		return raftTiming{}, err
	}
	heartbeat, err := positiveIntEnv("RAFT_HEARTBEAT_TICKS", 1)
	if err != nil {
		return raftTiming{}, err
	}
	if election <= heartbeat {
		return raftTiming{}, fmt.Errorf("RAFT_ELECTION_TICKS (%d) must be greater than RAFT_HEARTBEAT_TICKS (%d)", election, heartbeat)
	}
	return raftTiming{tick: time.Duration(tickMS) * time.Millisecond, election: election, heartbeat: heartbeat}, nil
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

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
