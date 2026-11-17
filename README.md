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
- Grafana: http://localhost:3000

## Fault injection (chaos runs)

`cmd/chaos` runs the load generator against the cluster again and again, and
injects one fault into each run, picked at random:

| Fault | What happens |
|---|---|
| `kill-leader`, `kill-follower` | SIGKILL a shard's leader (or a follower), restart it a few seconds later |
| `isolate-leader`, `isolate-follower` | disconnect the node from the network, then reconnect it |
| `pause-leader` | freeze the leader's process (`docker pause`), like a long GC pause |
| `quorum-loss` | kill two of a shard's three nodes, so it cannot commit, then restart them |
| `delay-shard` | 50-200 ms network delay on every node of a shard (`tc netem`) |
| `kill-router` | SIGKILL the router and restart it |
| `rolling-restart` | restart a shard's nodes one after another |

A run passes only if the load generator's correctness audit passes (no seat
double-booked, every confirmed seat taken, nothing taken without an order) and
every shard heals after the load: one leader that every replica agrees on, and
all three replicas at the same applied index.

```bash
go run ./cmd/datagen -out deploy
docker compose build
go run ./cmd/chaos -runs 20              # a quick check, about 15 minutes
go run ./cmd/chaos -runs 500 -seed 1     # the full campaign; the same seed replays the same faults
```

Results go to `results/chaos-<timestamp>/`: `summary.md` (per fault: runs,
failures, recovery time), `runs.csv`, and `run-NNNN/` with each run's load
generator output and history. `-faults kill-leader,quorum-loss` limits the
fault kinds, `-stop-on-fail` stops at the first failure and leaves the
cluster as it is, and `go run ./cmd/chaos -h` lists the rest.

## Run tests

```bash
go vet ./... && go test -race ./...
```

## Layout

- `cmd/router`, `cmd/storage`, `cmd/coordinator`: entry points for each node type
- `internal/server`: shared health and metrics endpoints
- `deploy/`: Prometheus and Grafana config
