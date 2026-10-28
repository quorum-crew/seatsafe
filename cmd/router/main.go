// Command router serves the public BookingService.
//
// Environment:
//
//	GRPC_ADDR  BookingService listen address (default :7000)
//	HTTP_ADDR  /healthz and /metrics (default :8080)
//	SHARDMAP   shard map file (default /etc/seatsafe/shardmap.json)
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/quorum-crew/seatsafe/internal/router"
	"github.com/quorum-crew/seatsafe/internal/server"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

func main() {
	sm, err := shardmap.Load(server.Getenv("SHARDMAP", "/etc/seatsafe/shardmap.json"))
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
	})
	if err != nil {
		log.Fatal(err)
	}
}
