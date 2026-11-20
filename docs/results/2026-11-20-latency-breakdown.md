# Latency breakdown of one booking (Nov 20)

**Question:** where does a single-shard booking spend its time, and what stops one shard from going faster?

**Answer:** in the shard leader's Raft loop, and almost all of that is fsync. The router, the group client and the network add under 0.5 ms together. Three things in `raftnode` dominate, in this order:

1. **The leader replicates only after its own fsync.** "Save done → committed" (the follower round trip) is the biggest stage at every rate: 44-53% of the time.
2. **The Raft loop is saturated by fsync at 1,200/s.** Each leader still does only about 135 fsyncs a second, but the median Save time has tripled to 6.3 ms. Proposals then queue in front of the loop.
3. **Committed entries wait for the fsync of the Ready that carries them** before they're applied: 8 ms at the median at 1,200/s.

At 1,200/s, the p99 also includes stalls of about 200 ms on all three shards at once. Those come from the laptop's disk, not from any one node.

## Machine

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H, 16 cores, 22 threads |
| RAM | 15.5 GB |
| Docker | Docker Desktop 29.7.2 on WSL2, limited to 16 CPUs and 12 GB (11.68 GiB visible, 8 GB swap). Containers have no limits of their own. |
| Disk | All 9 storage replicas fsync to the same WSL disk image |

Chrome, Edge, Brave, Discord, Slack, Zoom, WhatsApp, Teams and Copilot were closed for the runs. No IDE was open, and no other Docker project was running.

## Method

- **Code:** `main` at `c71521e` (merge of #45), plus the `PPROF=1` flag from this branch. Temporary per-stage timing sat on top of that; it is not committed (see "Stage timing" below).
- **Dataset:** 1,000,000 seats on 3 shards of 3 nodes (`datagen -events 0 -flash-sections 1000 -flash-rows 20 -flash-seats-per-row 50`). Shard 1 has 334,000 seats; shards 2 and 3 have 333,000 each.
- **Runs:**
  - The fresh workload, open loop, at 400, 800 and 1,200 orders/s for 60 s, after a 5 s warmup.
  - Orders are 70% 1 seat, 20% 2 seats and 10% 4 seats.
  - All orders are single-shard (`-cross-shard-frac 0`).
  - The load generator ran as a container on the compose network.
- **Setup per run:**
  - The cluster was wiped and reseeded before every run.
  - Storage nodes and the router ran with `PPROF=1`.
  - Storage nodes used `RAFT_SNAPSHOT_ENTRIES=1000000`, as in `deploy/compose.bench.yml`, so no snapshot was taken during a run.
- **Profiles:**
  - From shard 1's leader and from the router.
  - 30 s each of CPU, block and mutex, taken 10-40 s into the measured window.
  - A goroutine dump at the end of the window.
- **Memory check before every run:**
  - Before each run I recorded host free memory, Docker's memory limit and `docker stats`.
  - A run was not started if host free memory was under 3 GB, or if any container was above 80% of its limit.
  - The check ran after the reset and after dropping the Docker VM's page cache.

### Stage timing (temporary, not committed)

- **Sampling:** the router sampled 1 Book in 100 and marked it with gRPC metadata.
- **Leader stamps:** the shard leader recorded wall-clock times for that proposal, keyed by its Raft proposal ID, and returned them in a response header:
  - handler entered;
  - Propose called;
  - the entry first seen in a Ready's `Entries`;
  - that Ready's Save returned (fsync included);
  - the entry first seen in a Ready's `CommittedEntries`;
  - `Apply` returned;
  - the waiting Propose woken;
  - the handler returning.
- **Router stamps:** the router added its own times: request received, attempt sent, response received, and response returned.
- **Clock:** every container shares the Docker VM's clock, so times from different processes can be subtracted.
- **Raft-loop statistics:** the leaders also recorded these for every Ready, not sampled:
  - entries per Ready;
  - fsyncs;
  - Save time;
  - apply time per batch;
  - apply queue depth;
  - how long `raft.Propose` took.

## Runs discarded

No run was discarded after its load had started. Every attempt that failed the memory check was stopped before the load generator ran:

| Attempt | Rate | Host free at the check | What was going on |
|---|---|---|---|
| r400a | 400 | 541 MB | Chrome had been reopened; closed again |
| r800a | 800 | 2,837 MB | The Docker VM had grown; its page cache was dropped |
| r1200a, b | 1,200 | 2,903 / 2,974 MB | |
| r1200c | 1,200 | 2,160 MB | Right after a Docker Desktop restart, with its dashboard open |
| r1200d | 1,200 | 2,897 MB | Grafana and Prometheus stopped |
| r800c-f | 800 | 2,827 / 2,943 / 2,503 / 2,272 MB | A rerun for consistency; the last attempt waited 5 min for memory |

Runs used:

| Run | Rate | Host free at start | Lowest during the run | Grafana and Prometheus |
|---|---|---|---|---|
| r400c | 400 | 3,165 MB | 2,436 MB | stopped |
| r400b | 400 (repeat) | 3,303 MB | about 2,000 MB | running |
| r800b | 800 | 3,228 MB | 2,110 MB | running |
| r1200e | 1,200 | 3,260 MB | 2,400 MB | stopped |

The 800/s run had Grafana and Prometheus up; the other runs had them stopped. The two 400/s runs, one with and one without them, show that this doesn't matter. The router total p50 was 6.28 and 6.05 ms, and every stage's share was within 2 points. No container used more than 2% of Docker's memory. Every run passed the audit (rules a-d) with 0 UNKNOWN and no retries.

## Client view (load generator)

| Rate | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|
| 400 | 6.9 | 9.9 | 16.0 | 21.9 | 27.4 |
| 800 | 16.4 | 51.3 | 64.7 | 83.8 | 92.5 |
| 1,200 | 44.9 | 68.3 | 201.3 | 268.3 | 282.6 |

The router measured 1-3 ms less at the median. That gap is the load generator's own queueing and the hop from the client to the router.

## Stage tables

Durations are per sampled request, measured inside the router and the leader.
- **Share (all):** each stage's total time over all samples, as a share of the requests' total time.
- **Share (slowest 10%):** the same, but only over the slowest 10% of requests.

The shares add up to 100%, and they move less between runs than the absolute times.

### 400/s (237 samples, r400c)

| Stage | p50 ms | p99 ms | Share (all) | Share (slowest 10%) |
|---|---|---|---|---|
| 1 router receive → send to leader | 0.01 | 0.08 | 0% | 0% |
| 2 send → leader handler entered | 0.23 | 0.82 | 4% | 3% |
| 3 handler → Propose called | 0.02 | 0.17 | 0% | 0% |
| 4 Propose → entry in a Ready | 0.01 | 1.60 | 2% | 4% |
| 5 in Ready → Save done (fsync) | 2.10 | 4.62 | 34% | 30% |
| 6 Save done → committed | 3.27 | 7.53 | 53% | 51% |
| 7 committed → apply done | 0.05 | 3.36 | 3% | 9% |
| 8 apply done → waiter woken | 0.00 | 0.06 | 0% | 0% |
| 9 woken → leader response sent | 0.00 | 0.01 | 0% | 0% |
| 10 leader response → router has it | 0.21 | 0.58 | 4% | 2% |
| 11 router has it → router response | 0.00 | 0.02 | 0% | 0% |
| **Total** | **6.05** | **14.04** | | |

### 800/s (473 samples, r800b)

| Stage | p50 ms | p99 ms | Share (all) | Share (slowest 10%) |
|---|---|---|---|---|
| 1 router receive → send to leader | 0.01 | 0.05 | 0% | 0% |
| 2 send → leader handler entered | 0.23 | 1.20 | 1% | 0% |
| 3 handler → Propose called | 0.03 | 0.17 | 0% | 0% |
| 4 Propose → entry in a Ready | 0.75 | 11.01 | 10% | 10% |
| 5 in Ready → Save done (fsync) | 3.42 | 16.45 | 22% | 20% |
| 6 Save done → committed | 6.68 | 33.05 | 49% | 49% |
| 7 committed → apply done | 0.18 | 16.08 | 16% | 19% |
| 8 apply done → waiter woken | 0.01 | 0.21 | 0% | 0% |
| 9 woken → leader response sent | 0.00 | 0.02 | 0% | 0% |
| 10 leader response → router has it | 0.25 | 1.48 | 2% | 1% |
| 11 router has it → router response | 0.00 | 0.02 | 0% | 0% |
| **Total** | **13.03** | **59.87** | | |

### 1,200/s (709 samples, r1200e)

| Stage | p50 ms | p99 ms | Share (all) | Share (slowest 10%) |
|---|---|---|---|---|
| 1 router receive → send to leader | 0.01 | 0.04 | 0% | 0% |
| 2 send → leader handler entered | 0.19 | 0.59 | 0% | 0% |
| 3 handler → Propose called | 0.02 | 0.12 | 0% | 0% |
| 4 Propose → entry in a Ready | 3.75 | 89.64 | 15% | 26% |
| 5 in Ready → Save done (fsync) | 7.78 | 28.92 | 20% | 17% |
| 6 Save done → committed | 19.18 | 55.35 | 44% | 36% |
| 7 committed → apply done | 7.97 | 31.24 | 20% | 20% |
| 8 apply done → waiter woken | 0.04 | 0.25 | 0% | 0% |
| 9 woken → leader response sent | 0.00 | 0.02 | 0% | 0% |
| 10 leader response → router has it | 0.29 | 1.10 | 1% | 0% |
| 11 router has it → router response | 0.00 | 0.03 | 0% | 0% |
| **Total** | **42.93** | **195.03** | | |

In the router, `groupclient.Call` outside the RPC itself took 9 µs at the median and under 0.08 ms at p99 in every run. No sampled request needed a second attempt.

## Raft loop (each shard leader, per run)

The three leaders agreed to within a few percent, so the table shows shard 1's leader.

| | 400/s | 800/s | 1,200/s |
|---|---|---|---|
| Entries per shard per second | 135 | 265 | 398 |
| Ready iterations/s | 312 | 343 | 178 |
| fsyncs/s (`MustSync` Readies) | 135 | 192 | 137 |
| Mean new entries per Ready that had any | 1.00 | 1.38 | 2.90 |
| Readies with 1 / 2-5 / over 5 new entries | 7,963 / 24 / 0 | 8,711 / 2,582 / 64 | 2,898 / 4,292 / 928 |
| Save p50 / p90 / p99 / max (ms) | 2.07 / 3.08 / 4.38 / 10.4 | 3.06 / 7.94 / 13.2 / 41.4 | 6.06 / 12.3 / 22.8 / 213 |
| Apply per batch p50 / p99 (ms) | 0.015 / 0.10 | 0.018 / 0.20 | 0.027 / 0.22 |
| Entries per applied batch p50 / p99 | 1 / 1 | 1 / 5 | 2 / 12 |
| Apply queue depth p50 / max | 1 / 4 | 1 / 6 | 1 / 7 |
| `raft.Propose` call p50 / p99 (ms) | 0.043 / 0.26 | 0.013 / 0.30 | 0.008 / 0.11 |
| Proposal wait before it is in a Ready (stage 4) p50 / p99 (ms) | 0.01 / 1.6 | 0.75 / 11.0 | 3.75 / 89.6 |

- **Proposal wait:** `raft.Propose` returns once etcd raft's goroutine has taken the proposal. The proposal then waits there until our loop calls `Advance` and takes the next Ready, so the wait shows up as stage 4, not in the `Propose` call.
- **Simultaneous stall:** at 1,200/s the slowest Save on each of the three leaders was 205-216 ms. That is a stall of the shared disk, not of one node.

## Router

- **Connections:** the router keeps one TCP (HTTP/2) connection to each of the 9 storage nodes, and the load generator holds 16 connections to it.
- **In-flight Books per shard group**, seen when a sampled Book was sent:

  | Rate | p50 | p99 | max |
  |---|---|---|---|
  | 400/s | 0 | 3 | 3 |
  | 800/s | 3 | 19 | 24 |
  | 1,200/s | 17 | 70 | 111 |

  No concurrent-stream limit was reached; gRPC's server default is unlimited.
- **Retries:** none in any run, for any reason.

## Profiles (shard 1's leader and the router, 30 s each)

| | 400/s | 800/s | 1,200/s |
|---|---|---|---|
| Leader CPU (average cores) | 0.28 | 0.38 | 0.18 |
| Router CPU (average cores) | 0.28 | 0.47 | 0.39 |
| Leader goroutines (end) | 67 | 80 | 80 |
| Router goroutines (end) | 111 | 142 | 162 |
| Leader mutex delay (30 s) | 0.58 s | 1.20 s | 0.22 s |
| Router mutex delay (30 s) | 0.33 s | 1.58 s | 3.51 s |

- **CPU, top frames (leader and router alike):**
  - The top two flat frames are `syscall.Syscall6` (25-27%) and `runtime.futex` (16-26%).
  - The rest is the Go scheduler (`findRunnable`, `stealWork`) and the GC (`scanobject`, `nextFreeFast`), each 1-2%.
  - By cumulative time, most of it is gRPC's `loopyWriter` writing frames (15-27%) and reading them (`http2.Framer.ReadFrame`).
  - No SeatSafe function reaches 2% flat. No process uses even half a core on average, so CPU isn't the limit at these rates.
  - The leader's CPU isn't monotonic in the rate (0.18 cores at 1,200/s against 0.38 at 800/s). There were half as many Ready iterations at 1,200/s, but that doesn't explain it all. I wouldn't read anything into the leader CPU numbers.
- **Block:**
  - Over 92% of the leader's block time is `selectgo`. The largest share, 55% at 1,200/s, is Book handlers waiting in `raftnode.propose` for their entry to apply; that is commit latency, not lock contention.
  - The rest is gRPC writers waiting for frames, the Raft transport's peer loops, and Pebble's commit pipeline (`commitPipeline.publish`, 2.5%).
  - The router's block time is 76% Book calls waiting for the shard's answer (`waitOnHeader`).
- **Mutex:** almost all of it is `runtime._LostContendedRuntimeLock` (the runtime's own locks). Below that, the router's metrics `CounterVec.Inc` showed up with 0.03 s per 30 s. There is no contended SeatSafe lock.

## Containers (peaks, `docker stats`; 100% = 1 core)

| | 400/s | 800/s | 1,200/s |
|---|---|---|---|
| Shard leaders | 57-66% / 213-232 MiB | 80-88% / 173-227 MiB | 68-71% / 177-228 MiB |
| Shard followers | 35-41% / 165-169 MiB | 43-49% / 166-171 MiB | 36-42% / 168-173 MiB |
| Router | 57% / 23 MiB | 85% / 24 MiB | 98% / 40 MiB |
| Load generator | 93% / 340 MiB | 80% / 387 MiB | 90% / 398 MiB |

These are one-second peaks. The profiles above give the averages.

## Top 3 bottlenecks

### 1. The leader starts replicating only after its own fsync

- **Evidence:**
  - "Save done → committed" is the largest stage at every rate: 53%, 49% and 44% of the time.
  - At 400/s its median (3.3 ms) is one follower round trip: about 0.2 ms each way, the follower's Save of about 2.1 ms, and the leader picking up the next Ready.
  - `handleReady` persists the Ready and only then sends its messages, the leader's MsgApp to its followers included. A booking therefore waits for the leader's fsync and then a follower's fsync, one after the other.
- **Fix:** on the leader, send the Ready's messages before the Save; followers keep sending after it, as now. etcd's server does exactly this (Raft thesis §10.2.1).
  - It is safe with `go.etcd.io/raft/v3` v3.6. With synchronous writes, the leader acknowledges its own entries only after they're persisted (delivered at `Advance`). Committing still needs a majority with the entries durable, and `CommittedEntries` are still applied after the Save.
- **Expected gain:** the commit then takes about max(leader fsync, follower round trip) instead of their sum.
  - At 400/s that removes about 2 ms of 6 (about 30% of the median).
  - Under load the saving is one Save on every booking (3-6 ms at 800-1,200/s), and the loop is freed sooner.
- **Risk:** low to medium. Votes and MsgAppResp must still go after the Save. A run of the chaos campaign should confirm the change.
- **Code:** Kripal (`raftnode`).

### 2. The Raft loop is saturated by fsync

- **Evidence:**
  - From 800 to 1,200/s the leaders' fsyncs per second *fell* from 192 to 137, while median Save time doubled from 3.1 to 6.1 ms.
  - The loop spends at least 137 × 6.1 ms, about 85% of every second, in Save.
  - Batching only grew to about 3 entries per Ready.
  - Proposals queue in front of the loop: stage 4 rose from 0.01 to 3.75 ms at the median, to 90 ms at p99, and to 26% of the slowest 10%.
  - Part of this is the laptop. Nine replicas each make about 135 fsyncs a second to one disk image, and all three shards stalled for about 200 ms at the same moment. Save at 400/s (2.1 ms) is already above the idle 1.5 ms measured on Oct 31.
- **Fix:**
  - (a) Measure on AWS with a disk per node first; that may remove most of the Save growth for free.
  - (b) In code, take fsync off the loop: etcd raft's `AsyncStorageWrites` (v3.6), with Pebble grouping concurrent syncs. The loop would keep taking proposals and sending while the previous append syncs, so batches grow on their own.
- **Expected gain:** the cap would then be the disk's group-commit rate, not one fsync per loop iteration.
  - With a 2-3 ms fsync, a loop that never blocks could batch about 7-10 entries per fsync at 3,000/s per shard.
  - I can't put a number on it for this laptop, where the shared disk is the real limit.
- **Risk:** high. It changes the core of `raftnode` (how local acks and messages flow) and `raftstore`, so it needs a thorough chaos campaign. Do it after fix 1, and after the AWS numbers say it's needed.
- **Code:** Kripal (`raftnode`, `raftstore`).

### 3. Committed entries wait for the fsync of the Ready that carries them

- **Evidence:**
  - "Committed → apply done" is 0.05 ms at the median at 400/s (3%), 0.18 ms (p99 16 ms, 16%) at 800/s, and 8.0 ms (20%) at 1,200/s.
  - Applying itself takes 0.02-0.03 ms per batch, and the apply queue is 1-2 deep.
  - The wait is the Save of new entries in the same Ready. `handleReady` pushes the batch to the apply goroutine only after the Save.
  - At 400/s a Ready that carries a commit rarely carries new entries, which is why the stage only grows with load.
- **Fix:** push `CommittedEntries` to the apply queue before the Save when they are all at or below the last index already persisted. That is always true on the leader in v3.6's synchronous mode, as long as fix 1 keeps the self-ack after the Save. Otherwise keep today's order, as for batches with a configuration change.
- **Expected gain:** about 8 ms at the median at 1,200/s (43 → about 35 ms at the router) and most of the 16 ms p99 at 800/s. Nothing at 400/s.
- **Risk:** low with that guard.
- **Code:** Kripal (`raftnode`).

**Not bottlenecks:** the router, the group client, `seatsm` and the network.
- Router stages 1 and 11 take 0.01 ms.
- The group client adds 9 µs.
- `Apply` takes about 20 µs per entry.
- The two gRPC hops are about 0.2 ms each.
- No SeatSafe lock is contended.

The load generator used 80-93% of a core on the same machine. On AWS it should run on its own host.

## What the evidence can and can't say

- **Shares are repeatable at 400/s.** Two runs, one with and one without Grafana and Prometheus, gave totals of 6.05 and 6.28 ms. Every stage's share was within 2 points.
- **800 and 1,200/s are single runs.**
- **The 1,200/s p99 and max are dominated by the stall shared by all three shards.** I wouldn't compare those two numbers with anything.
- **The leader CPU figures are inconsistent between rates** (see Profiles); I draw no conclusion from them.
- **Host free memory fell to 2.1-2.4 GB during every run.** The check only covers the start.

## Estimates

- **One shard on this laptop:**
  - Today the knee is between 270 and 400 orders/s per shard. At 400/s per shard (1,200/s total), the median goes from 13 to 43 ms. The Nov 5 ramp found 1,000/s total (333/s per shard) as the most that meets p99 under 100 ms.
  - Fixes 1 and 3 cut about 30% at low load and about 20-25 ms at the median at 1,200/s. That probably moves the knee to about 450-500/s per shard (1,350-1,500/s total), because the loop's fsync rate stays the cap.
  - Fix 2 is what lifts the cap. On this shared disk I can't estimate by how much.
- **10,000/s on 3 shards** is about 3,300/s per shard. That needs:
  - About 7-10 entries per fsync at a 2-3 ms fsync (one disk per node), which in practice means fix 2, or fixes 1 and 3 plus enough queueing to batch.
  - CPU in proportion to RPCs. The leader uses under 0.5 cores at 400/s per shard, so plan 2-4 cores per leader. The router uses about 0.4 cores at 1,200/s, so about 3-4 cores at 10,000/s, or two routers.
  - The load generator on separate hosts.

  These are extrapolations from a laptop. The first AWS run should repeat these three rates with the same stage timing before any of the code changes.

Raw results (`results/lb/*`: stages, Raft-loop statistics, profiles, `docker stats`, console output) are kept locally, not committed.
