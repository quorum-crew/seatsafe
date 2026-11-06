# Laptop baseline, 2026-10-31

First numbers from `scripts/bench.sh`. These are laptop numbers: every container, the load generator included, shares one machine. AWS comes later.

**The main result is a bug, not a rate.** On `main`, the cluster stalls for seconds at a time after a fixed amount of writes, whatever the rate, so the numbers in [Run A](#run-a-main-as-it-is) measure that stall. [Run B](#run-b-experimental-store-change) is the same suite with a one-line experimental change that isn't in the repo, to show what's behind it.

The correctness audit and the history checks passed on every run in both suites: no double bookings.

## Machine

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H, 16 cores, 22 threads |
| RAM | 15.5 GB |
| Docker | Docker Desktop on WSL2, limited to 16 CPUs and 12 GB (8 GB swap) |
| fsync | 1.5 ms p50, 5 ms p99, 22 ms max (4 KB, Docker volume, idle machine) |

## Method

- Dataset: 1,000,000 seats on 3 shards of 3 nodes (`-events 0 -flash-sections 1000 -flash-rows 20 -flash-seats-per-row 50`).
- Cluster code: `main` at `53641d1`. The load generator runs from the same image, as a container on the compose network, talking to the router directly.
- Runs: (1) fresh ramp, from 500/s in steps of 500/s every 10 s, p99 under 100 ms and under 1% errors; (2) fresh open loop at 80% of that for 60 s; (3) flash open loop at the same rate; (4) flash closed loop with 10,000 users and no think time. Runs 2 to 4 have a 5 s warmup.
- Orders: 70% 1 seat, 20% 2 seats, 10% 4 seats. Open-loop latency is measured from the intended send time.

## Read these numbers with care

- **No snapshots ran.** `deploy/compose.bench.yml` sets `RAFT_SNAPSHOT_ENTRIES=1000000` so snapshots don't stall the Raft loop during runs (the #26 follow-up will move them off the loop), but only #26 reads that variable and it isn't merged. On `main` no snapshot is ever taken and the Raft log stays in memory.
- **The cluster is wiped before every run**, unlike a single chain of runs, so flash collides with its own traffic rather than seats fresh already booked, and no run inherits an earlier run's log. `RESET_BETWEEN=0` chains them.
- Prometheus and Grafana were running. The Peak CPU figures below are the highest `docker stats` sample per container, not simultaneous, and peak memory is what `docker stats` reports, which can include page cache.
- A ramp step fails on its p99 over 10 s, so one 2-second stall fails a step. See Run B.

## The bug: every Save writes a range tombstone

`raftstore.Save` (`internal/raftstore/pebble.go:97`) writes `DeleteRange(last+1, MaxUint64)` on every save with entries, to drop a conflicting tail, even when there is no tail. The tombstones all end at the same key, so when Pebble flushes a memtable it has to fragment thousands of nested ranges, at O(n²) CPU and memory. Three replicas of a shard reach that point together, so a whole shard stalls, misses heartbeats and starts elections.

Store alone, no cluster, one entry per Save:

| Saves | As on `main` | No tail delete unless the log shrinks |
|---|---|---|
| 20,000 | one Save blocked 3.98 s, heap 1.36 GB | |
| 40,000 | one Save blocked 2m29s, heap 13.6 GB | |
| 60,000 | did not finish in 4m40s | finished in 3.4 s |

It is why a run at 400/s was steady for 38 s (p50 10 ms) and then fell over: memory on every node of a shard jumped 5 to 8x within seconds, elections fired on every shard, and thousands of orders timed out. The rate only decides how soon it happens. #26's `Save` has the same line.

Run B writes the `DeleteRange` only when new entries end before the log's last index. That is an experiment for measuring, not a fix: it tracks the last index in a process-wide variable and does not survive a restart correctly.

## What looks like the bottleneck

1. **On `main`: the tombstone bug**, by far. Nothing else matters until it's fixed.
2. **With the store change, no single container is saturated at 800 to 1,500/s.** Leaders peak under 1 core, the router near 1 core, the load generator 1 to 2 cores. fsync at 1.5 ms is not the limit either.
3. **Periodic stalls are the next limit, and I haven't found their cause.** The 1,500/s step failed on one stall at seconds 29 and 30 (in-flight 381, p99 417 ms); every other second had p99 between 22 and 59 ms. So 1,000/s understates steady capacity. The 10,000-user run shows similar dips.
4. **p50 is 10 to 20 ms at these rates, well above what fsync explains.** Not yet explained.
5. **At 10,000 users the load generator is part of the limit:** it peaks at 8.75 cores of 16, the router at 4.7, leaders at 3. That run reaches about 11,000 orders/s, peaking near 14,000/s, but only by queuing: p50 287 ms. The router's 500 ms attempt timeout then adds retries (130,802), and 3.6% of orders ran out their 5 s budget and ended UNKNOWN. A closed loop with no think time is a stress test, not a user experience; this load should come from separate machines on AWS.

Seeding: 3 to 10 s per shard across all cluster starts (3.8 to 10.2 s in Run A, 2.9 to 6.7 s in Run B). After seeding, leaders hold about 160 MiB and followers about 100 MiB.

## Run A: main as it is

Commit `53641d1`.

Max sustainable rate (p99 under 100ms, under 1% errors): **500/s**. Runs 2 and 3 used 80% of it: 400/s.

| Run | Attempted/s | Achieved/s | Confirmed orders/s | Confirmed seats/s | Conflict | p50 ms | p99 ms | p99.9 ms | Max ms | Unknown | Retries | Dropped | Audit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1-ramp | 749 | 749 | 749 | 1122 | 0.0% | 28.37 | 114.69 | 134.01 | 146.43 | 0 | 0 | 0 | PASS |
| 2-fresh | 400 | 345 | 230 | 341 | 0.0% | 14.94 | 8015.87 | 8601.60 | 8814.59 | 10210 | 16753 | 0 | PASS |
| 3-flash | 400 | 392 | 203 | 290 | 36.2% | 46.85 | 5840.90 | 6164.48 | 6340.61 | 3161 | 7508 | 0 | PASS |
| 4-users | 9916 | 9910 | 2063 | 2771 | 72.2% | 311.81 | 5013.50 | 5062.65 | 5644.29 | 44860 | 146519 | 0 | PASS |

### 1-ramp: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 5% | 6 MiB |
| coord-2 | 4% | 5 MiB |
| coord-3 | 4% | 7 MiB |
| grafana | 1% | 102 MiB |
| loadgen | 185% | 528 MiB |
| prometheus | 1% | 24 MiB |
| router | 104% | 22 MiB |
| shard1-node1 | 37% | 140 MiB |
| shard1-node2 | 69% | 187 MiB |
| shard1-node3 | 42% | 140 MiB |
| shard2-node1 | 65% | 181 MiB |
| shard2-node2 | 40% | 160 MiB |
| shard2-node3 | 36% | 149 MiB |
| shard3-node1 | 69% | 185 MiB |
| shard3-node2 | 43% | 141 MiB |
| shard3-node3 | 33% | 146 MiB |

### 2-fresh: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 26% | 5 MiB |
| coord-2 | 24% | 7 MiB |
| coord-3 | 37% | 8 MiB |
| grafana | 74% | 113 MiB |
| loadgen | 157% | 488 MiB |
| prometheus | 81% | 35 MiB |
| router | 132% | 108 MiB |
| shard1-node1 | 214% | 1423 MiB |
| shard1-node2 | 291% | 1860 MiB |
| shard1-node3 | 406% | 1806 MiB |
| shard2-node1 | 355% | 1510 MiB |
| shard2-node2 | 163% | 1697 MiB |
| shard2-node3 | 195% | 2170 MiB |
| shard3-node1 | 513% | 1727 MiB |
| shard3-node2 | 209% | 1836 MiB |
| shard3-node3 | 462% | 2128 MiB |

### 3-flash: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 11% | 5 MiB |
| coord-2 | 9% | 4 MiB |
| coord-3 | 9% | 6 MiB |
| grafana | 47% | 124 MiB |
| loadgen | 208% | 434 MiB |
| prometheus | 73% | 36 MiB |
| router | 146% | 57 MiB |
| shard1-node1 | 832% | 1438 MiB |
| shard1-node2 | 186% | 1526 MiB |
| shard1-node3 | 217% | 1570 MiB |
| shard2-node1 | 70% | 163 MiB |
| shard2-node2 | 221% | 1371 MiB |
| shard2-node3 | 186% | 1569 MiB |
| shard3-node1 | 155% | 1269 MiB |
| shard3-node2 | 183% | 1400 MiB |
| shard3-node3 | 235% | 1576 MiB |

### 4-users: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 5% | 5 MiB |
| coord-2 | 9% | 5 MiB |
| coord-3 | 33% | 6 MiB |
| grafana | 201% | 101 MiB |
| loadgen | 794% | 2445 MiB |
| prometheus | 51% | 25 MiB |
| router | 460% | 346 MiB |
| shard1-node1 | 169% | 402 MiB |
| shard1-node2 | 144% | 384 MiB |
| shard1-node3 | 77% | 351 MiB |
| shard2-node1 | 134% | 375 MiB |
| shard2-node2 | 98% | 311 MiB |
| shard2-node3 | 57% | 313 MiB |
| shard3-node1 | 318% | 895 MiB |
| shard3-node2 | 188% | 661 MiB |
| shard3-node3 | 134% | 638 MiB |

## Run B: experimental store change

`53641d1` plus the one-line experiment above. Not in the repo.

Max sustainable rate (p99 under 100ms, under 1% errors): **1000/s**. Runs 2 and 3 used 80% of it: 800/s.

| Run | Attempted/s | Achieved/s | Confirmed orders/s | Confirmed seats/s | Conflict | p50 ms | p99 ms | p99.9 ms | Max ms | Unknown | Retries | Dropped | Audit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1-ramp | 999 | 999 | 999 | 1490 | 0.0% | 17.55 | 281.09 | 410.11 | 453.12 | 0 | 0 | 0 | PASS |
| 2-fresh | 800 | 800 | 800 | 1193 | 0.0% | 13.20 | 33.31 | 61.28 | 90.88 | 0 | 0 | 0 | PASS |
| 3-flash | 800 | 800 | 392 | 555 | 51.0% | 16.64 | 200.83 | 411.90 | 500.48 | 0 | 0 | 0 | PASS |
| 4-users | 11177 | 11117 | 2089 | 2780 | 77.8% | 287.23 | 5005.31 | 5095.42 | 5881.85 | 24289 | 130802 | 0 | PASS |

### 1-ramp: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 4% | 11 MiB |
| coord-2 | 5% | 5 MiB |
| coord-3 | 4% | 5 MiB |
| grafana | 0% | 107 MiB |
| loadgen | 202% | 507 MiB |
| prometheus | 1% | 24 MiB |
| router | 128% | 33 MiB |
| shard1-node1 | 39% | 167 MiB |
| shard1-node2 | 76% | 181 MiB |
| shard1-node3 | 36% | 160 MiB |
| shard2-node1 | 75% | 182 MiB |
| shard2-node2 | 40% | 165 MiB |
| shard2-node3 | 44% | 164 MiB |
| shard3-node1 | 36% | 169 MiB |
| shard3-node2 | 36% | 162 MiB |
| shard3-node3 | 74% | 185 MiB |

### 2-fresh: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 4% | 5 MiB |
| coord-2 | 4% | 5 MiB |
| coord-3 | 4% | 5 MiB |
| grafana | 2% | 102 MiB |
| loadgen | 81% | 379 MiB |
| prometheus | 1% | 26 MiB |
| router | 88% | 21 MiB |
| shard1-node1 | 43% | 168 MiB |
| shard1-node2 | 50% | 166 MiB |
| shard1-node3 | 75% | 185 MiB |
| shard2-node1 | 77% | 243 MiB |
| shard2-node2 | 43% | 166 MiB |
| shard2-node3 | 42% | 169 MiB |
| shard3-node1 | 42% | 168 MiB |
| shard3-node2 | 75% | 186 MiB |
| shard3-node3 | 41% | 165 MiB |

### 3-flash: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 4% | 5 MiB |
| coord-2 | 5% | 5 MiB |
| coord-3 | 4% | 5 MiB |
| grafana | 3% | 104 MiB |
| loadgen | 109% | 379 MiB |
| prometheus | 2% | 23 MiB |
| router | 84% | 28 MiB |
| shard1-node1 | 43% | 173 MiB |
| shard1-node2 | 81% | 187 MiB |
| shard1-node3 | 42% | 168 MiB |
| shard2-node1 | 43% | 167 MiB |
| shard2-node2 | 72% | 184 MiB |
| shard2-node3 | 39% | 169 MiB |
| shard3-node1 | 92% | 188 MiB |
| shard3-node2 | 48% | 170 MiB |
| shard3-node3 | 53% | 167 MiB |

### 4-users: peak per container

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 6% | 5 MiB |
| coord-2 | 15% | 6 MiB |
| coord-3 | 16% | 5 MiB |
| grafana | 198% | 100 MiB |
| loadgen | 875% | 2427 MiB |
| prometheus | 15% | 26 MiB |
| router | 472% | 515 MiB |
| shard1-node1 | 55% | 375 MiB |
| shard1-node2 | 161% | 378 MiB |
| shard1-node3 | 144% | 450 MiB |
| shard2-node1 | 154% | 535 MiB |
| shard2-node2 | 131% | 356 MiB |
| shard2-node3 | 69% | 328 MiB |
| shard3-node1 | 161% | 699 MiB |
| shard3-node2 | 309% | 1116 MiB |
| shard3-node3 | 232% | 668 MiB |
