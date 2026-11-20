// Command router serves the public BookingService.
//
// Environment:
//
//	GRPC_ADDR  BookingService listen address (default :7000)
//	HTTP_ADDR  /healthz and /metrics (default :8080)
//	SHARDMAP   shard map file (default /etc/seatsafe/shardmap.json)
//	ROUTER_SHARD_ATTEMPT_MS  deadline of one Book or ListAvailable attempt
//	           to a shard group, in ms (default 500)
//	ROUTER_COORD_ATTEMPT_MS  deadline of one SubmitOrder attempt to the
//	           coordinator group, in ms (default 1000)
//
// Both must be shorter than the 2 s client deadline; the router refuses to
// start otherwise.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/router"
	"github.com/quorum-crew/seatsafe/internal/server"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

func main() {
	sm, err := shardmap.Load(server.Getenv("SHARDMAP", "/etc/seatsafe/shardmap.json"))
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := configFromEnv(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	err = router.Serve(ctx, router.ServeOptions{
		GRPCAddr: server.Getenv("GRPC_ADDR", ":7000"),
		HTTPAddr: server.Getenv("HTTP_ADDR", ":8080"),
		Map:      sm,
		Info:     server.InfoFromEnv("router"),
		Config:   cfg,
	})
	if err != nil {
		log.Fatal(err)
	}
}

// configFromEnv reads ROUTER_SHARD_ATTEMPT_MS and ROUTER_COORD_ATTEMPT_MS and
// checks them against the client deadline.
func configFromEnv(getenv func(string) string) (router.Config, error) {
	shard, err := attemptTimeout(getenv, "ROUTER_SHARD_ATTEMPT_MS", router.DefaultShardAttemptTimeout)
	if err != nil {
		return router.Config{}, err
	}
	coord, err := attemptTimeout(getenv, "ROUTER_COORD_ATTEMPT_MS", router.DefaultCoordinatorAttemptTimeout)
	if err != nil {
		return router.Config{}, err
	}
	cfg := router.Config{Group: groupclient.Config{AttemptTimeout: shard}, Coordinator: groupclient.Config{AttemptTimeout: coord}}
	if err := cfg.Validate(); err != nil {
		return router.Config{}, fmt.Errorf("%w (ROUTER_SHARD_ATTEMPT_MS=%q, ROUTER_COORD_ATTEMPT_MS=%q)", err, getenv("ROUTER_SHARD_ATTEMPT_MS"), getenv("ROUTER_COORD_ATTEMPT_MS"))
	}
	return cfg, nil
}

// attemptTimeout parses an attempt timeout in milliseconds. Unset means def.
func attemptTimeout(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	v := getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive whole number of milliseconds, got %q", key, v)
	}
	return time.Duration(n) * time.Millisecond, nil
}
