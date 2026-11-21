# Running SeatSafe on AWS

`cmd/clusterctl` runs SeatSafe experiments on machines that already exist. It reads one file, `deploy/aws/inventory.yaml`, and does everything after provisioning: configuration, start and reset, benchmarks from several load generators, and chaos campaigns.

## Who does what

**Provisioning (Kripal):**

- The instances, in three availability zones.
- A local NVMe disk mounted at `/data` on every storage and coordinator host.
- Docker on every host, usable by `ssh_user` without sudo.
- The SeatSafe image pulled on every host (the `image` of the inventory), built from a commit that has `cmd/clusterctl`.
- `/opt/seatsafe` on every host, writable by `ssh_user`.
- `ssh_user` (ec2-user) with passwordless sudo. clusterctl uses sudo for `iptables` only.
- Security group: ports 7000 (gRPC) and 8080 (health, metrics) open between all hosts, and ssh open to wherever clusterctl runs.
- chrony running on the load generator hosts (it is on Amazon Linux by default).
- `deploy/aws/inventory.yaml`, in the format of `deploy/aws/inventory.example.yaml`.

**clusterctl (this tool):** everything else. It never creates, stops or resizes an instance.

## The fleet

| Hosts | Role | Instance | Notes |
|---|---|---|---|
| 9 | storage: 3 shards of 3 nodes | c6id.large | Raft log on local NVMe at `/data` |
| 3 | coordinator (Raft group 0) | c6id.large | same |
| 2 | router | c6i.large | stateless |
| 2 | loadgen | c6i.xlarge | the first one also runs unattended campaigns |
| 1 | monitor | t3.medium | Prometheus and Grafana |

**Zones:** us-west-2a, us-west-2b and us-west-2c, with one node of every shard and one coordinator in each. Losing a zone then costs every Raft group one node of three, and every group keeps its quorum. The inventory loader warns if a group has two nodes in one zone.

**Cost:** about $1.80 an hour for the whole fleet, about $43 a day, at approximate us-west-2 on-demand list prices (c6id.large about $0.10, c6i.large $0.085, c6i.xlarge $0.17, t3.medium $0.042 an hour; not checked against today's price list). Root volumes and traffic between zones come on top. Check the current prices before budgeting.

**Stop the instances when idle.** A stopped instance costs only its root volume. But see the next section before starting them again.

## After any stop and start: `clusterctl reset`

`/data` is instance-store NVMe. **AWS wipes it when an instance stops.** A node that came back with an empty `/data` and its old identity would vote with a log it no longer has, which can lose committed bookings.

So:

- Configs, seed files and env files live under `/opt/seatsafe`, never under `/data`.
- `clusterctl reset` gives the cluster a new id, written to every `/opt/seatsafe/cluster-id` and every `/data/.cluster-id`.
- `clusterctl up` refuses to start a node whose `/data` is empty or carries another id.
- Containers are started with `--restart no`, so a wiped node cannot come back by itself after a reboot.
- `clusterctl status` shows such a node as **NEEDS RESET** and exits with code 2.

**After any stop and start of any instance, run `clusterctl reset`.** It wipes every `/data`, starts every node and waits until all are ready. All bookings are lost; the dataset is seeded again.

## Runbook

All commands run from the repository root, on a machine that can ssh to the hosts' public addresses. `-dry-run` before the subcommand prints every command instead of running it.

```sh
# 0. Build clusterctl (no local Go needed)
docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 golang:1.24 go build -o bin/clusterctl ./cmd/clusterctl

# 1. Configuration: shard map (peers by private address), seeds, one env file per node
bin/clusterctl gen -seats 1000000

# 2. Copy it to the hosts
bin/clusterctl push

# 3. Start from nothing, and check
bin/clusterctl reset
bin/clusterctl status
```

**Settings.** `gen -set KEY=VALUE` changes a setting in the env files (`-set role:KEY=VALUE` for one role). A key that no role has is an error. The settings: `RAFT_TICK_MS`, `RAFT_ELECTION_TICKS`, `RAFT_HEARTBEAT_TICKS`, `RAFT_SNAPSHOT_ENTRIES`, `RAFT_CATCHUP_ENTRIES`, `GC_RETENTION_MS`, `HOLD_TTL_MS`, `ROUTER_SHARD_ATTEMPT_MS`, `ROUTER_COORD_ATTEMPT_MS`, `RAFT_LEADER_SEND_FIRST`, `RAFT_EARLY_APPLY`, `PPROF`.

**Smaller clusters.** `gen -shards N` uses the first N shards and `gen -replicas N` the first N nodes of every group (`-replicas 1` is single-node). Every later command works on the layout `gen` last wrote. `down -all` also stops the hosts a smaller layout does not use.

**Other commands.** `up`, `down`, `logs <host>`, `prom` (prints `prometheus.yml`; `push` copies it to the monitor host as `/opt/seatsafe/prometheus.yml`).

### Benchmarks

```sh
bin/clusterctl presets
bin/clusterctl bench -preset fresh-at-rate -rate 2000 -duration 60s -repeats 3
bin/clusterctl bench -preset pipeline-ab -rate 1200
```

A run is one load from every load generator host at once:

1. The cluster is reset.
2. Every load generator's clock offset and error bound are read (`chronyc tracking`) and written to the run's `clock.json`.
3. One baseline read of the seats is taken.
4. The workers start at the same wall-clock time, each against a router of its own, with the rate shared out. Fresh-seat workers never ask for the same seat.
5. One final read is taken, and the workers' results are merged: HdrHistograms, per-second series and histories.
6. The audit and the Porcupine bookings check run once, on the merged result.

Every preset repeats each variant (3 times by default) and writes `summary.md` with the mean and standard deviation of every metric, plus per-run CPU, memory and disk of every host (`hosts.md`) and every node's `/metrics`.

| Preset | What it compares |
|---|---|
| `ramp` | highest rate that keeps p99 under 100 ms (one load generator: a ramp cannot be merged) |
| `fresh-at-rate`, `flash`, `users-10k` | one workload each |
| `cross-shard` | single-shard against 20% cross-shard orders |
| `shards` (or `shards-1`, `shards-2`, `shards-3`) | 1, 2 and 3 shards |
| `single-node` | 1 against 3 replicas |
| `pipeline-ab` | `RAFT_LEADER_SEND_FIRST` and `RAFT_EARLY_APPLY` off against on, same build, alternating |
| `tick` | 25 ms against 50 ms Raft ticks, with a leader kill |
| `router-timeout` | shard attempt timeout of 500, 300, 250 and 200 ms, with a paused leader and without a fault |
| `az-outage` | one zone killed under load |

Presets that change settings or the layout generate, push and reset by themselves. The cluster is left in the last variant's configuration.

**Clock skew.** Histories merged from two machines carry two clocks. The check is given `-widen-ms`: every operation's call moves earlier and its return later by the largest offset plus root dispersion of any load generator, plus 1 ms. Widening can only make the check more lenient about order between machines; a real double booking still fails.

### Chaos campaigns

```sh
# From here, attended:
bin/clusterctl campaign -runs 18 -seed 7

# Unattended, on loadgen-1:
bin/clusterctl campaign -detach -copy-key -name nov21 -runs 90 -seed 7
bin/clusterctl campaign-status -name nov21
bin/clusterctl fetch -name nov21          # copies the results to results/nov21
```

**The mix** (9 steps, in turn): kill, pause and partition the leader of a shard; a no-fault control; the same three on the coordinator; another control; an outage of one zone.

| Fault | How |
|---|---|
| kill | `docker kill -s KILL`, later `docker start` |
| pause | `docker pause`, later `docker unpause` |
| partition | `iptables` on the leader's host: packets from and to the other cluster nodes' private addresses are dropped |
| az-outage | every SeatSafe container in one zone killed at once, restarted later |

**Every run** gets the audit and the bookings verdict, one row in `summary.md`, and totals. An az-outage run also reports every group's failover and the share of orders answered during the outage.

**Partitions cannot lock anyone out.**

- The rules go in a chain of their own (`SEATSAFE_CHAOS`) and name only the private addresses of the other storage, coordinator and router hosts. No rule matches a load generator, the monitor or the machine clusterctl runs from, and no policy or other chain is changed.
- Before the first rule is added, the host is told to remove the chain by itself after 5 minutes. A partition ends even if clusterctl is killed. The timer of one partition cannot end a later one.
- A campaign removes leftovers (a chain, a paused or stopped container) on every node before its first run and when it exits.

**Resuming.** A campaign is resumable: start it again with the same `-name` (or `-out`) and the finished runs are kept.

**`-detach`** puts the image's own `clusterctl`, the inventory and the layout under `/opt/seatsafe` on loadgen-1 and starts the campaign there under `nohup`, using private addresses. The campaign needs an ssh key that every host accepts. `-copy-key` copies the inventory's key to `/opt/seatsafe/ssh_key` on loadgen-1; without it, a key must already be there.

### Collecting results

- `bench` and an attended `campaign` write to `results/` on the machine they run from.
- `fetch -name <name>` copies a detached campaign's directory from loadgen-1.
- Load generator hosts keep nothing after a run: each run's files are fetched and removed.

## Trying it locally

`deploy/aws/inventory.local.yaml` describes the same cluster on one machine: every host is a container on a Docker network, and commands run directly instead of over ssh.

```sh
docker build -t seatsafe:dev .
C="bin/clusterctl -inventory deploy/aws/inventory.local.yaml -dir results/local/gen"
$C gen && $C push && $C reset && $C status
$C bench -preset fresh-at-rate -rate 300 -duration 20s -repeats 1 -lead 6s
$C campaign -runs 9 -seed 1 -lead 6s
$C down
```

What differs from AWS:

- A partition takes the container off the Docker network. The `iptables` commands are covered by tests of the exact command text, and were run against a kernel in a throwaway container (rules added, removed, and removed by the timer). They have not run between two real hosts yet.
- There is one clock, so nothing is widened.
- Per-host CPU and disk numbers are those of the one machine, or missing.
