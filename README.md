# SeatSafe

A distributed ticket booking system that never sells the same seat twice, even when servers crash.
CSE 512 (Distributed Database Systems) project at ASU by Quorum Crew.

## Run the cluster locally

Requires Go 1.24+ and Docker Desktop.

### Generate the dataset

Run this once before `docker compose up` (the files are gitignored):

```bash
go run ./cmd/datagen -out deploy   # writes deploy/shardmap.json and deploy/seed/shard{N}.json
```

The default is one 50,000-seat flash event plus 4 events of 4,800 seats (69,200 seats total).
See `go run ./cmd/datagen -h` for options.

### Start the cluster

```bash
docker compose up -d --build --wait   # start router, 9 storage nodes, 3 coordinators, monitoring
docker compose ps                     # all seatsafe containers should be "healthy"
curl localhost:8080/healthz           # router health check
docker compose down                   # stop everything
```

- Prometheus: http://localhost:9090/targets
- Grafana: http://localhost:3000, dashboard **SeatSafe → SeatSafe overview** (no login): bookings by
  outcome, latency p50/p99/p99.9, leader and term per shard, follower lag, nodes up, router retries
  and failover time. It is provisioned from `deploy/grafana/provisioning/dashboards/`; edits made in
  the UI are lost on restart, so export the JSON back into that folder to keep them.

## Run tests

```bash
go vet ./... && go test -race ./...
```

## Layout

- `cmd/router`, `cmd/storage`, `cmd/coordinator`: entry points for each node type
- `internal/server`: shared health and metrics endpoints
- `deploy/`: Prometheus and Grafana config
