# Main baseline, 2026-11-05

`scripts/bench.sh` on `main` after #31, the raftstore fix that only writes a tail `DeleteRange` when new entries really truncate a tail. This replaces the Oct 31 laptop numbers (in #30's `docs/results/2026-10-31-laptop-baseline.md`) as the baseline. Laptop numbers again: every container, the load generator included, shares one machine.

**Summary.** #31 removes the stall that dominated the Oct 31 numbers.
- **Ramp:** max sustainable rate doubles from 500/s to 1,000/s.
- **Fresh at 800/s:** 0 UNKNOWN (10,210 at 400/s before), p99 38 ms.
- **Correctness:** the audit and the history checks pass on every run, with 0 double bookings.
- **Stability:** there are no elections after startup, and no second in any run where the cluster stalls.

## Machine

Same machine and Docker settings as Oct 31.

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H, 16 cores, 22 threads |
| RAM | 15.5 GB |
| Docker | Docker Desktop 29.7.2 on WSL2 (kernel 5.15.167.4), limited to 16 CPUs and 12 GB (8 GB swap) |
| fsync | 1.5 ms p50, 5 ms p99, 22 ms max (4 KB, Docker volume, idle machine; measured Oct 31) |

## Method

- **Dataset:** 1,000,000 seats on 3 shards of 3 nodes (`-events 0 -flash-sections 1000 -flash-rows 20 -flash-seats-per-row 50`). Shard 1 has 334,000 seats; shards 2 and 3 have 333,000 each.
- **Cluster code:** `main` at `87e4eca` (merge of #31). It ran with the load generator, audit and `bench.sh` from #30 at `890e9d1` merged on top, as a local branch (`a669456`), because #30 isn't merged yet. The load generator runs from the same image, as a container on the compose network, talking to the router directly.
- **Runs:**
  1. fresh ramp, from 500/s in steps of 500/s every 10 s, until a step misses p99 under 100 ms or under 1% errors;
  2. fresh open loop at 80% of the max for 60 s;
  3. flash open loop at the same rate;
  4. flash closed loop with 10,000 users and no think time.

  Runs 2 to 4 have a 5 s warmup.
- **Orders:** 70% 1 seat, 20% 2 seats, 10% 4 seats. Open-loop latency is measured from the intended send time.
- **Reset between runs:** the cluster is wiped and reseeded before every run (`RESET_BETWEEN=1`).
- **Audit:** the four rules (a: no seat CONFIRMED twice; b: CONFIRMED seats are taken; c: every taken seat is explained by a CONFIRMED or UNKNOWN order; d: no CONFIRMED seat was already taken before the run), plus `checkhistory` on runs 2 to 4.
- **No snapshots:** main doesn't take snapshots yet (#26 is open), so the Raft logs stay in memory and on disk for the whole run.

Seeding took 4 to 8.3 s per shard across the four cluster starts (11 to 15 s from `docker compose up` to healthy).

## Results

Max sustainable rate (p99 under 100 ms, under 1% errors): **1,000/s**. The 1,500/s step stopped on p99 129 ms. Runs 2 and 3 used 800/s.

| Run | Attempted/s | Achieved/s | Confirmed orders/s | Confirmed seats/s | Conflict | p50 ms | p99 ms | p99.9 ms | Max ms | Unknown | Retries | Dropped | Audit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1-ramp | 996 | 996 | 996 | 1486 | 0.0% | 22.50 | 112.51 | 156.93 | 176.25 | 0 | 0 | 0 | PASS |
| 2-fresh | 800 | 800 | 800 | 1194 | 0.0% | 17.42 | 38.11 | 70.02 | 92.67 | 0 | 0 | 0 | PASS |
| 3-flash | 800 | 800 | 392 | 556 | 51.0% | 20.69 | 145.92 | 277.76 | 441.60 | 0 | 0 | 0 | PASS |
| 4-users | 7125 | 7125 | 1603 | 2170 | 64.6% | 361.47 | 5054.46 | 5988.35 | 7049.22 | 55952 | 162822 | 0 | PASS |

The table covers the measured window. The audit also counts warmup orders:

| Run | Audit orders | Confirmed seats | UNKNOWN orders | a / b / c / d violations | History check (no double booking, real-time order) |
|---|---|---|---|---|---|
| 1-ramp | 32,500 | 48,471 | 0 | 0 / 0 / 0 / 0 | not recorded for the ramp |
| 2-fresh | 52,000 | 77,619 | 0 | 0 / 0 / 0 / 0 | PASS / PASS, 52,000 attempts |
| 3-flash | 52,000 | 37,964 | 0 | 0 / 0 / 0 / 0 | PASS / PASS, 52,000 attempts |
| 4-users | 463,032 | 154,018 | 60,208 | 0 / 0 / 0 / 0 | PASS / PASS, 641,259 attempts |

**Elections and stalls.**
- **Elections:** in run 4, each shard elected one leader at startup (term 2) and none after. The other runs' clusters were wiped before their logs were read, so their elections aren't counted. Their timeseries show no stalls either.
- **Per-second timeseries:** fresh has no second with p99 over 100 ms; flash has 2 of 66; the ramp has 4, all in its last (failing) step. In run 4 no second has fewer than 1,000 completions.

## Comparison

| | Oct 31 Run A: main before #31 | Oct 31 Run B: scratch fix | **Nov 5: main with #31** | Nov 5: + #26 (not merged: snapshot encoding blocks apply) |
|---|---|---|---|---|
| Cluster code | `53641d1` | `53641d1` + process-wide `lastIndex` experiment | `87e4eca` | `6d8a8fa` + #31 + #26 `0496805` + #30 |
| Snapshots | none | none | none | on, every 10,000 entries |
| Max sustainable rate | 500/s | 1,000/s | **1,000/s** | 1,000/s |
| Ramp at max: p50 / p99 / p99.9 ms | 28.4 / 114.7 / 134.0 | 17.6 / 281.1 / 410.1 | **22.5 / 112.5 / 156.9** | 20.0 / 455.7 / 1055.7 |
| Fresh 80%: rate | 400/s | 800/s | **800/s** | 800/s |
| Fresh: achieved / confirmed per s | 345 / 230 | 800 / 800 | **800 / 800** | 800 / 800 |
| Fresh: p50 / p99 / p99.9 ms | 14.9 / 8015.9 / 8601.6 | 13.2 / 33.3 / 61.3 | **17.4 / 38.1 / 70.0** | 17.4 / 153.7 / 346.4 |
| Fresh: UNKNOWN | 10,210 | 0 | **0** | 0 |
| Flash: achieved / confirmed per s | 392 / 203 | 800 / 392 | **800 / 392** | 803 / 394 |
| Flash: p50 / p99 / p99.9 ms | 46.9 / 5840.9 / 6164.5 | 16.6 / 200.8 / 411.9 | **20.7 / 145.9 / 277.8** | 23.5 / 175.1 / 289.3 |
| Flash: UNKNOWN | 3,161 | 0 | **0** | 0 |
| 10,000 users: achieved / confirmed per s | 9,910 / 2,063 | 11,117 / 2,089 | **7,125 / 1,603** | 4,644 / 910 |
| 10,000 users: p50 / p99 / p99.9 ms | 311.8 / 5013.5 / 5062.7 | 287.2 / 5005.3 / 5095.4 | **361.5 / 5054.5 / 5988.4** | 510.2 / 5423.1 / 6271.0 |
| 10,000 users: UNKNOWN | 44,860 | 24,289 | **55,952** | 94,176 |
| Audit | PASS | PASS | **PASS** | PASS |
| Elections after startup | on every shard | not reported | **none** | none |
| Stalls | every shard, seconds at a time | one at 1,500/s, dips at 10,000 users | **none** | repeated in 10,000 users |
| Storage node peak memory, fresh | 1.4 to 2.2 GiB | 165 to 243 MiB | **165 to 235 MiB** | not compared |

Notes:
- **Run A vs now:** this is #31's effect. Before it, every Save left a range tombstone and Pebble's flushes fell over after a fixed number of writes. Run A's fresh run had 10,210 UNKNOWN at half the rate, storage nodes grew to about 2 GiB, and every shard held elections. With #31 the same suite runs at twice the rate, with no UNKNOWN in the open-loop runs, no stalls, no elections after startup in the run whose logs were checked, and storage nodes under 240 MiB in the open-loop runs.
- **Run B vs now:** Run B tested the same idea with a process-wide `lastIndex` that didn't survive restarts; #31 is the real fix. The open-loop runs line up: same max rate, fresh p99 33 vs 38 ms, flash p99 201 vs 146 ms. Ramp p99 at 1,000/s is lower now (113 vs 281 ms).
- **10,000 users is lower than Run B, and I haven't found why.** It reached 7,125/s with 55,952 UNKNOWN, vs 11,117/s and 24,289 in Run B. This run is limited partly by the load generator itself: it peaks at 9 cores, with the router at 4 and shard3-node3 near 3. A closed loop with no think time on one laptop is noisy run to run. #30's review fixes also changed the load generator between the two runs (audit rule d, bench.sh), though not the workload. This needs repeated runs, ideally with the load generated from other machines, before reading anything into it.
- **The #26 column is a combined test branch, not something on main.** That branch was main + #31 + #26 + #30, with snapshots at the default 10,000 entries. All its audits passed. Its p99 and 10,000-user numbers are worse because snapshot encoding runs inline on the apply goroutine. On this dataset each snapshot is 2 to 3 MB (compact v2 format) and took 1.0 to 2.0 s to encode, about every 2 s per shard under load, so committed entries queue behind it. Separately, `SaveSnapshot` holds the store mutex across a synced commit, which blocked the Raft loop's `Save` for up to 1.4 s. Both are in the #26 review, and that column will be rerun once they're fixed.

## Peak per container (Nov 5, main)

Peak CPU and memory are the highest `docker stats` sample per container, not simultaneous; memory is what `docker stats` reports, which can include page cache. Prometheus and Grafana were running.

### 1-ramp

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 5% | 5 MiB |
| coord-2 | 5% | 5 MiB |
| coord-3 | 5% | 3 MiB |
| grafana | 1% | 113 MiB |
| loadgen | 190% | 537 MiB |
| prometheus | 1% | 26 MiB |
| router | 126% | 26 MiB |
| shard1-node1 | 82% | 184 MiB |
| shard1-node2 | 37% | 169 MiB |
| shard1-node3 | 47% | 163 MiB |
| shard2-node1 | 40% | 166 MiB |
| shard2-node2 | 37% | 167 MiB |
| shard2-node3 | 77% | 184 MiB |
| shard3-node1 | 77% | 184 MiB |
| shard3-node2 | 48% | 170 MiB |
| shard3-node3 | 42% | 166 MiB |

### 2-fresh

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 6% | 5 MiB |
| coord-2 | 4% | 6 MiB |
| coord-3 | 5% | 5 MiB |
| grafana | 1% | 97 MiB |
| loadgen | 223% | 385 MiB |
| prometheus | 1% | 24 MiB |
| router | 153% | 18 MiB |
| shard1-node1 | 45% | 170 MiB |
| shard1-node2 | 85% | 188 MiB |
| shard1-node3 | 46% | 169 MiB |
| shard2-node1 | 47% | 171 MiB |
| shard2-node2 | 47% | 169 MiB |
| shard2-node3 | 86% | 235 MiB |
| shard3-node1 | 46% | 177 MiB |
| shard3-node2 | 47% | 165 MiB |
| shard3-node3 | 92% | 185 MiB |

### 3-flash

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 5% | 5 MiB |
| coord-2 | 5% | 5 MiB |
| coord-3 | 5% | 5 MiB |
| grafana | 56% | 103 MiB |
| loadgen | 195% | 382 MiB |
| prometheus | 1% | 22 MiB |
| router | 130% | 24 MiB |
| shard1-node1 | 87% | 188 MiB |
| shard1-node2 | 55% | 170 MiB |
| shard1-node3 | 48% | 167 MiB |
| shard2-node1 | 43% | 165 MiB |
| shard2-node2 | 80% | 186 MiB |
| shard2-node3 | 51% | 169 MiB |
| shard3-node1 | 61% | 172 MiB |
| shard3-node2 | 55% | 173 MiB |
| shard3-node3 | 94% | 186 MiB |

### 4-users

| Container | Peak CPU (100% = 1 core) | Peak memory |
|---|---|---|
| coord-1 | 36% | 5 MiB |
| coord-2 | 9% | 3 MiB |
| coord-3 | 10% | 9 MiB |
| grafana | 7% | 97 MiB |
| loadgen | 908% | 2353 MiB |
| prometheus | 8% | 26 MiB |
| router | 399% | 446 MiB |
| shard1-node1 | 77% | 287 MiB |
| shard1-node2 | 194% | 377 MiB |
| shard1-node3 | 87% | 284 MiB |
| shard2-node1 | 160% | 336 MiB |
| shard2-node2 | 97% | 268 MiB |
| shard2-node3 | 90% | 254 MiB |
| shard3-node1 | 101% | 519 MiB |
| shard3-node2 | 123% | 528 MiB |
| shard3-node3 | 283% | 949 MiB |

## Still open

- 10,000 users below Run B (see above).
- The p50 of 17 to 22 ms at 800 to 1,000/s is still well above what a 1.5 ms fsync explains, as on Oct 31.
- Raw results (`results/20261105-173604`) are kept locally, not committed.
