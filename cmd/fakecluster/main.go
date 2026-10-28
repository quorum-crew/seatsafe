// Command fakecluster serves an in-process fake SeatSafe cluster on
// localhost, for manual testing of the router and clients. It is NOT the
// real system (no Raft, no replication, no durability); see
// internal/fakecluster.
//
// It reads the shard map and seed files that cmd/datagen writes, moves every
// node to 127.0.0.1 (shard and coordinator nodes on consecutive ports from
// -base-port), writes that map to -out, and by default also runs a router
// against it:
//
//	go run ./cmd/datagen -out deploy
//	go run ./cmd/fakecluster                  # router on :7000, metrics on :8080
//
// To run the router separately, pass -router "" and start it with
// SHARDMAP=deploy/shardmap.local.json.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/router"
	"github.com/quorum-crew/seatsafe/internal/seed"
	"github.com/quorum-crew/seatsafe/internal/server"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

func main() {
	in := flag.String("shardmap", "deploy/shardmap.json", "shard map written by cmd/datagen")
	seedDir := flag.String("seed", "deploy/seed", "directory of shard{N}.json seed files")
	host := flag.String("host", "127.0.0.1", "host every fake node listens on")
	basePort := flag.Int("base-port", 17000, "first port; nodes use consecutive ports")
	out := flag.String("out", "deploy/shardmap.local.json", "where to write the localhost shard map")
	routerAddr := flag.String("router", ":7000", `router gRPC address ("" = do not run a router)`)
	routerHTTP := flag.String("router-http", ":8080", "router /healthz and /metrics address")
	flag.Parse()

	sm, err := localMap(*in, *out, *host, *basePort)
	if err != nil {
		log.Fatal(err)
	}
	seeds := map[uint32]*seed.Loaded{}
	for _, id := range sm.ShardIDs() {
		l, err := seed.Load(seed.Path(*seedDir, id), id)
		if err != nil {
			log.Fatal(err)
		}
		seeds[id] = l
	}
	c, err := fakecluster.New(sm, seeds, fakecluster.Options{
		Listen: func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) },
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Printf("fake cluster up: %d shards, coordinator %v; shard map for clients: %s", len(c.Shards), sm.CoordinatorNodes(), *out)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *routerAddr == "" {
		<-ctx.Done()
		return
	}
	info := server.InfoFromEnv("router")
	if err := router.Serve(ctx, router.ServeOptions{GRPCAddr: *routerAddr, HTTPAddr: *routerHTTP, Map: sm, Info: info}); err != nil {
		log.Print(err)
	}
}

// localMap rewrites every node address of the shard map at in to
// host:port, writes the result to out and returns it.
func localMap(in, out, host string, port int) (*shardmap.Map, error) {
	data, err := os.ReadFile(in)
	if err != nil {
		return nil, err
	}
	var f shardmap.File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("%s: %w", in, err)
	}
	next := func() string {
		a := net.JoinHostPort(host, fmt.Sprint(port))
		port++
		return a
	}
	for i := range f.Shards {
		for j := range f.Shards[i].Nodes {
			f.Shards[i].Nodes[j] = next()
		}
	}
	for j := range f.Coordinator.Nodes {
		f.Coordinator.Nodes[j] = next()
	}
	sm, err := shardmap.New(f)
	if err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return nil, err
	}
	return sm, os.WriteFile(out, append(b, '\n'), 0o644)
}
