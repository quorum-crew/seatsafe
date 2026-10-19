# SeatSafe

A distributed ticket booking system that never sells the same seat twice, even when servers crash.
CSE 512 (Distributed Database Systems) project at ASU by Quorum Crew.

## Run the cluster locally

Requires Go 1.24+ and Docker Desktop.

```bash
docker compose up -d --build --wait   # start router, 9 storage nodes, 3 coordinators, monitoring
docker compose ps                     # all seatsafe containers should be "healthy"
curl localhost:8080/healthz           # router health check
docker compose down                   # stop everything
```

- Prometheus: http://localhost:9090/targets
- Grafana: http://localhost:3000

## Run tests

```bash
go vet ./... && go test -race ./...
```

## Layout

- `cmd/router`, `cmd/storage`, `cmd/coordinator`: entry points for each node type
- `internal/server`: shared health and metrics endpoints
- `deploy/`: Prometheus and Grafana config
