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
failures, failover time and recovery time), `runs.csv`, and `run-NNNN/` with
each run's load generator output and history. Failover is measured from the
fault until the rest of the shard agrees on a new leader; recovery from the
heal until every replica has caught up. Both are sampled through `docker
exec`, so they are accurate to a few hundred milliseconds; the router's
`seatsafe_groupclient_leader_recovery_seconds` gives the client's view. `-faults kill-leader,quorum-loss` limits the
fault kinds, `-stop-on-fail` stops at the first failure and leaves the
cluster as it is, and `go run ./cmd/chaos -h` lists the rest.

## Run tests

```bash
go vet ./... && go test -race ./...
```

## Cross-shard orders

An order with seats on two or more shards goes from the router to the coordinator group (Raft
group 0), which runs two-phase commit: it logs the order, asks every shard to hold its seats
(300 ms), logs COMMIT if all of them did and ABORT otherwise, and then tells the shards. The
decision is in the coordinator's Raft log before any shard hears it, so a new coordinator leader
finishes what the old one started: it resends logged decisions and aborts orders that had none.

The load generator sends such orders with `-cross-shard-frac 0.2`, and its audit then also checks
that every cross-shard order is all or nothing.

Settings (environment): `HOLD_TTL_MS` (coordinator, default 5000), `GC_RETENTION_MS` (coordinator,
default 120000), `ROUTER_COORD_ATTEMPT_MS` (router, default 1000). Metrics are
`seatsafe_coordinator_*` on the coordinator nodes' `/metrics`;
`seatsafe_coordinator_protocol_violations_total` must stay 0.

## Layout

- `cmd/router`, `cmd/storage`, `cmd/coordinator`: entry points for each node type
- `internal/coordinator`, `internal/coordsm`: the 2PC coordinator and its replicated state machine
- `internal/server`: shared health and metrics endpoints
- `deploy/`: Prometheus and Grafana config
