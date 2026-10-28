package router

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/server"
	"github.com/quorum-crew/seatsafe/internal/shardmap"
)

// ServeOptions configure Serve.
type ServeOptions struct {
	GRPCAddr string // BookingService
	HTTPAddr string // /healthz and /metrics
	Map      *shardmap.Map
	Info     server.Info
	Config   Config
	// Ready, if set, receives the bound addresses once both listeners are up.
	Ready func(grpcAddr, httpAddr net.Addr)
}

// ShutdownTimeout bounds a graceful stop before in-flight RPCs are cut.
const ShutdownTimeout = 5 * time.Second

// Serve runs the router until ctx is done, then stops gracefully.
func Serve(ctx context.Context, o ServeOptions) error {
	reg := metrics.NewRegistry()
	rt, err := New(o.Map, o.Config, reg)
	if err != nil {
		return err
	}
	defer rt.Close()

	glis, err := net.Listen("tcp", o.GRPCAddr)
	if err != nil {
		return err
	}
	hlis, err := net.Listen("tcp", o.HTTPAddr)
	if err != nil {
		glis.Close()
		return err
	}
	gs := grpc.NewServer()
	seatsafev1.RegisterBookingServiceServer(gs, rt)
	hs := &http.Server{Handler: server.Handler(o.Info, reg.Write), ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	go func() { errc <- gs.Serve(glis) }()
	go func() {
		if err := hs.Serve(hlis); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	log.Printf("router %s: grpc %s, http %s", o.Info.NodeID, glis.Addr(), hlis.Addr())
	if o.Ready != nil {
		o.Ready(glis.Addr(), hlis.Addr())
	}

	select {
	case <-ctx.Done():
	case err = <-errc:
	}

	sctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	_ = hs.Shutdown(sctx)
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-sctx.Done():
		gs.Stop()
	}
	log.Printf("router %s stopped", o.Info.NodeID)
	return err
}
