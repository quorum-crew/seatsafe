// Package server provides the shared HTTP scaffolding (health + metrics)
// that every SeatSafe process runs. gRPC/Raft will be layered on later.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Info identifies a running process.
type Info struct {
	Role   string // router | storage | coordinator
	NodeID string
	Shard  string
	Peers  string
}

// Getenv returns $key, or def if it is unset or empty.
func Getenv(key, def string) string { return getenv(key, def) }

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// InfoFromEnv builds Info from environment variables set by Docker Compose.
func InfoFromEnv(role string) Info {
	return Info{
		Role:   role,
		NodeID: getenv("NODE_ID", "local"),
		Shard:  getenv("SHARD_ID", ""),
		Peers:  getenv("PEERS", ""),
	}
}

// Handler returns the HTTP routes shared by all processes. Each extra
// function appends its own metrics to /metrics.
func Handler(info Info, extra ...func(io.Writer)) http.Handler {
	start := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "ok role=%s node=%s shard=%s\n", info.Role, info.NodeID, info.Shard)
	})
	// Minimal Prometheus text format; swap for prometheus/client_golang later.
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		labels := fmt.Sprintf("role=%q,node=%q,shard=%q", info.Role, info.NodeID, info.Shard)
		fmt.Fprintf(w, "# TYPE seatsafe_up gauge\nseatsafe_up{%s} 1\n", labels)
		fmt.Fprintf(w, "# TYPE seatsafe_uptime_seconds gauge\nseatsafe_uptime_seconds{%s} %.0f\n",
			labels, time.Since(start).Seconds())
		for _, f := range extra {
			f(w)
		}
	})
	return mux
}

// Run starts the process and shuts down cleanly on SIGINT/SIGTERM.
func Run(role string) {
	info := InfoFromEnv(role)
	addr := getenv("HTTP_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: Handler(info), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("starting %s node=%s shard=%s peers=%s addr=%s",
			info.Role, info.NodeID, info.Shard, info.Peers, addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Printf("%s %s stopped", info.Role, info.NodeID)
}
