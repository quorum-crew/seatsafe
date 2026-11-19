// Command router serves the public BookingService.
//
// Environment:
//
//	GRPC_ADDR  BookingService listen address (default :7000)
//	HTTP_ADDR  /healthz and /metrics (default :8080)
//	SHARDMAP   shard map file (default /etc/seatsafe/shardmap.json)
//	ROUTER_COORD_ATTEMPT_MS  deadline of one SubmitOrder attempt to the
//	           coordinator group, in ms (default 1000). Shard groups keep
//	           the group client's 500 ms.
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
	attempt, err := coordAttemptTimeout(os.Getenv("ROUTER_COORD_ATTEMPT_MS"))
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
		Config:   router.Config{Coordinator: groupclient.Config{AttemptTimeout: attempt}},
	})
	if err != nil {
		log.Fatal(err)
	}
}

// coordAttemptTimeout parses ROUTER_COORD_ATTEMPT_MS. Unset means the
// router's default.
func coordAttemptTimeout(v string) (time.Duration, error) {
	if v == "" {
		return router.DefaultCoordinatorAttemptTimeout, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("ROUTER_COORD_ATTEMPT_MS must be a positive whole number of milliseconds, got %q", v)
	}
	return time.Duration(n) * time.Millisecond, nil
}
