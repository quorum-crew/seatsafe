package router

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/server"
)

func TestLabels(t *testing.T) {
	for c, want := range map[codes.Code]string{
		codes.OK: "OK", codes.DeadlineExceeded: "DEADLINE_EXCEEDED", codes.InvalidArgument: "INVALID_ARGUMENT", codes.Unavailable: "UNAVAILABLE",
	} {
		if got := codeLabel(c); got != want {
			t.Errorf("codeLabel(%v) = %q, want %q", c, got, want)
		}
	}
	if got := outcomeLabel(seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE); got != "SEAT_UNAVAILABLE" {
		t.Errorf("outcomeLabel = %q", got)
	}
	if got := outcomeLabel(seatsafev1.BookingOutcome(99)); got != "99" {
		t.Errorf("outcomeLabel(99) = %q", got)
	}
}

// Serve starts both listeners, exposes /healthz and the router metrics, and
// stops when its context ends.
func TestServe(t *testing.T) {
	sm, _, err := fakecluster.SmallDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, ServeOptions{
			GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0", Map: sm,
			Info:  server.Info{Role: "router", NodeID: "router-test"},
			Ready: func(_, h net.Addr) { ready <- h },
		})
	}()
	var httpAddr net.Addr
	select {
	case httpAddr = <-ready:
	case err := <-done:
		t.Fatalf("Serve: %v", err)
	}
	for path, want := range map[string]string{
		"/healthz": "ok role=router node=router-test",
		"/metrics": "# TYPE seatsafe_router_request_duration_seconds histogram",
	} {
		resp, err := http.Get("http://" + httpAddr.String() + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(body), want) {
			t.Errorf("%s: %q missing %q", path, body, want)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(ShutdownTimeout + time.Second):
		t.Fatal("Serve did not stop")
	}

	// A listen error comes back instead of serving.
	if err := Serve(context.Background(), ServeOptions{GRPCAddr: "256.0.0.1:1", HTTPAddr: "127.0.0.1:0", Map: sm}); err == nil {
		t.Fatal("bad gRPC address accepted")
	}
	if err := Serve(context.Background(), ServeOptions{GRPCAddr: "127.0.0.1:0", HTTPAddr: "256.0.0.1:1", Map: sm}); err == nil {
		t.Fatal("bad HTTP address accepted")
	}
}
