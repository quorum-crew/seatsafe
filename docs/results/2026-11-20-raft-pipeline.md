# Raft pipeline: replicate while persisting, apply persisted entries early (Nov 20)

**Change:** two things in the `raftnode` Ready loop no longer wait for the local fsync.

1. A leader sends a Ready's appends and heartbeats before it saves the Ready, so its followers write the entries while it does. It still counts its own copy only after its Save.
2. Committed entries that are already on this node's disk are handed to the apply goroutine before the Ready's Save.

These target findings 1 and 3 of the Nov 20 latency breakdown: stage 6 ("Save done → committed") and stage 7 ("committed → applied") in the tables below.

Both can be turned off, on their own, with `RAFT_LEADER_SEND_FIRST=false` and `RAFT_EARLY_APPLY=false` (storage and coordinator nodes; both default to true). With both off the Ready loop runs exactly as on `main`: persist, then send, then apply.

**Status:**
- Safety is checked: tests, stress runs and a 20-run chaos campaign all pass.
- Latency: one laptop round, single run per cell. It shows the mechanism working, but its totals are not a fair test of the change (see "Interpretation").
- The decisive before/after comparison is planned on AWS, using the two flags on one build.

## Chaos campaign

Same campaign and seed as the Porcupine campaign: `scripts/campaign.sh -seed 20261120`, 20 runs, 30 s of load at 300 orders/s (flash), 20% cross-shard, 5 browses/s, the mix of kill-leader, pause-leader, partition and no fault on shards and the coordinator.

`scripts/campaign.sh` and the bookings verdict are not on `main` yet, so the campaign ran from a local branch: the campaign tooling plus this change.

| | Runs | Audit (rules a to e) | Bookings verdict (Porcupine) | Stale browses | UNKNOWN orders | Elections |
|---|---|---|---|---|---|---|
| This change | 20 | 20/20 pass, 0 violations | 20 pass, 0 fail, 0 timeout | 0 | 0 | 16 |

| Fault | Runs | Pass | Failover, median | Failover, max | Recovery, median | Recovery, max | Elections per run | Client p99, median |
|---|---|---|---|---|---|---|---|---|
| kill-leader | 5 | 5 | 0.4 s | 0.4 s | 1.1 s | 2.0 s | 1.0 | 79 ms |
| pause-leader | 5 | 5 | 0.7 s | 0.8 s | 0.6 s | 0.7 s | 1.2 | 61 ms |
| partition | 5 | 5 | 0.3 s | 0.4 s | 2.6 s | 7.0 s | 1.0 | 50 ms |
| none | 5 | 5 | - | - | - | - | 0.0 | 53 ms |

Every faulted run had exactly one election except run 14 (pause-leader on the coordinator), which had 2. The no-fault runs had none.

The client p99 of the earlier campaign without this change was 208 ms (kill-leader), 57 ms (pause-leader), 205 ms (partition) and 153 ms (none). Those runs were on a machine with 0.6 to 1.1 GB of free memory and the two campaigns were hours apart, so this is not a measurement of the change.

## Stress runs

In `golang:1.24`, with `GOMAXPROCS=2` and `-race`:

| Packages | Count | Result |
|---|---|---|
| `./internal/raftnode` | 50 | ok, 658 s |
| `./internal/shard` | 20 | ok, 333 s |
| `./internal/transport` | 20 | ok, 80 s |

## Latency, laptop round 1 (single run per cell, laptop)

**Single run per cell, laptop.** Each rate ran once before and once after. A second round was planned but did not run (see "Host memory"). Read the totals with that in mind.

### Setup

As in the Nov 20 latency breakdown (`docs/results/2026-11-20-latency-breakdown.md` on `mrudula/throughput`):

- **Dataset and load:** 1,000,000 seats on 3 shards of 3 nodes. Fresh workload, open loop, all single-shard, 5 s warmup then 60 s measured.
- **Node settings:** `PPROF=1`, `RAFT_SNAPSHOT_ENTRIES=1000000` (no snapshots during a run).
- **Per run:** Grafana and Prometheus stopped, and the cluster wiped and reseeded.
- **Before:** `main` at `9addac2` plus the temporary stage timing.
- **After:** this branch plus the same stage timing.
- **Order:** before and after alternated (400 before, 400 after, 800 after, 800 before, 1,200 before, 1,200 after), so drift during the session hits both.
- **Sampling:** the router samples 1 in 100 bookings for the stage timeline: 240, 482 and about 720 samples per run at the three rates.
- **Every run:** achieved its target rate and passed the audit (0 double-booked, 0 unknown).

### Router total (router receive → response)

| Rate | p50 before | p50 after | Change | p99 before | p99 after | Change |
|---|---|---|---|---|---|---|
| 400/s | 8.51 ms | 13.31 ms | +56% | 33.77 ms | 47.87 ms | +42% |
| 800/s | 15.13 ms | 9.53 ms | −37% | 49.91 ms | 36.50 ms | −27% |
| 1,200/s | 22.71 ms | 12.88 ms | −43% | 67.24 ms | 119.93 ms | +78% |

The client-side latency from the load generator moves the same way:

| Rate | p50 before → after | p99 before → after |
|---|---|---|
| 400/s | 9.39 → 14.21 ms (+51%) | 37.05 → 54.56 ms (+47%) |
| 800/s | 16.59 → 11.22 ms (−32%) | 58.98 → 51.84 ms (−12%) |
| 1,200/s | 22.70 → 14.39 ms (−37%) | 66.94 → 119.10 ms (+78%) |

### Stages 4 to 7

These four stages carry the time. The other seven stages are at most 0.42 ms at the p50 in every run, before and after.

Share is each stage's part of the summed router total over all samples.

**400/s**

| Stage | p50 before → after | p99 before → after | Share before → after |
|---|---|---|---|
| 4 Propose → entry in a Ready | 0.02 → 0.30 ms (+0.28 ms) | 4.32 → 12.05 ms (+179%) | 4% → 14% |
| 5 in Ready → Save done (fsync) | 2.76 → 5.81 ms (+111%) | 9.01 → 21.11 ms (+134%) | 30% → 40% |
| 6 Save done → committed | 4.38 → 3.72 ms (−15%) | 23.24 → 23.86 ms (+3%) | 52% → 38% |
| 7 committed → apply done | 0.08 → 0.10 ms (+0.02 ms) | 5.35 → 0.97 ms (−82%) | 5% → 1% |

**800/s**

| Stage | p50 before → after | p99 before → after | Share before → after |
|---|---|---|---|
| 4 Propose → entry in a Ready | 0.70 → 0.86 ms (+23%) | 13.51 → 6.81 ms (−50%) | 9% → 14% |
| 5 in Ready → Save done (fsync) | 3.67 → 3.86 ms (+5%) | 9.78 → 12.20 ms (+25%) | 22% → 36% |
| 6 Save done → committed | 7.65 → 3.82 ms (−50%) | 21.17 → 13.16 ms (−38%) | 47% → 35% |
| 7 committed → apply done | 0.18 → 0.12 ms (−33%) | 8.11 → 1.14 ms (−86%) | 13% → 2% |

**1,200/s**

| Stage | p50 before → after | p99 before → after | Share before → after |
|---|---|---|---|
| 4 Propose → entry in a Ready | 2.21 → 2.03 ms (−8%) | 12.96 → 39.57 ms (+205%) | 10% → 19% |
| 5 in Ready → Save done (fsync) | 4.25 → 4.28 ms (+1%) | 14.55 → 35.12 ms (+141%) | 20% → 33% |
| 6 Save done → committed | 10.75 → 4.71 ms (−56%) | 35.17 → 34.18 ms (−3%) | 48% → 38% |
| 7 committed → apply done | 4.45 → 0.18 ms (−96%) | 14.73 → 1.36 ms (−91%) | 18% → 2% |

### Host memory per run

The gate was at least 3,072 MB of free host memory at the start of each run. "Lowest" is the lowest of the 5 s samples during the run.

| Run | Free at start | Lowest during |
|---|---|---|
| before, 400/s | 3,316 MB | 2,253 MB |
| after, 400/s | 3,222 MB | 2,313 MB |
| after, 800/s | 3,085 MB | 2,474 MB |
| before, 800/s | 3,189 MB | 2,138 MB |
| before, 1,200/s | 3,356 MB | 2,308 MB |
| after, 1,200/s | 3,336 MB | 1,897 MB |
| after, 400/s, round 2 | 2,845 MB after waiting 5 minutes | gate failed, not run |

Round 2 stopped at its first run, and its other five runs did not start.

The limit is Docker's WSL VM, not the cluster:
- After the cluster is brought up and seeded with 1,000,000 seats, the VM holds about 3.7 GB of host memory, while only about 2.3 GB is in use inside it.
- The VM does not return the freed pages to Windows by itself. Dropping its page cache and asking its kernel to compact memory (`echo 1 > /proc/sys/vm/compact_memory`) before each run gets some of them back.
- That was enough for round 1 (3,085 to 3,356 MB at the start), but not for the first run of round 2.
- With the desktop apps closed, host free memory was 6.3 GB with the cluster down. So the gate is met or missed by what the VM gives back, not by the cluster's own use.

### Interpretation

**The mechanism works.**
- Stage 6 (Save done → committed) halves at the p50 at 800/s (−50%) and 1,200/s (−56%): the followers' write now overlaps the leader's, so less of the round trip is left after the leader's Save.
- Stage 7 (committed → applied) drops by more than 80% at the p99 at every rate (−82%, −86%, −91%), and to almost nothing at the p50 at 1,200/s (4.45 → 0.18 ms): entries already on disk no longer wait for the next fsync.

**At 400/s stage 5, the leader's own fsync, doubled** (2.76 → 5.81 ms at the p50).
- All 9 storage replicas fsync to the same laptop SSD (one WSL disk image).
- Before, a follower wrote an entry only after the leader's fsync of it was done and the append had reached it, so the leader's and the followers' fsyncs of an entry did not overlap.
- With `RAFT_LEADER_SEND_FIRST` the leader and both followers fsync the same entries at the same time, to the same disk, so they contend. The extra time shows up in the leader's Save (stage 5) and in what queues behind it (stage 4).
- The stage 5 p99 rose at every rate (+134%, +25%, +141%), which fits contention. The data cannot separate it from session noise: there is one run per cell and no per-follower fsync timing.
- The 1,200/s "after" p99 is dominated by stages 4 and 5 (39.6 and 35.1 ms), the same pattern.

**Totals on a shared disk are not a fair test.** On a real cluster every node has its own disk, and overlapping the leader's and the followers' writes is what the change is for. On this laptop the overlap turns into contention for one device. The improvement in stages 6 and 7 and the cost in stage 5 come from the same overlap, so the laptop total can go either way. It went up at 400/s and down at 800 and 1,200/s at the p50.

These numbers are also not comparable to the Nov 20 breakdown's own (for example 6.05 ms at the p50 at 400/s there). Those runs were on a different day with a different memory state. Only the before/after pairs above, from the same session, are compared.

### Next: the decisive before/after on AWS

- **Hardware:** one disk per node (3 shards × 3 nodes plus the coordinator group, each on its own instance or volume).
- **Build:** a single build of this branch.
  - "Before": `RAFT_LEADER_SEND_FIRST=false RAFT_EARLY_APPLY=false` on every storage and coordinator node. This is exactly `main`'s Ready loop.
  - "After": the defaults.
  - One build removes build differences from the comparison.
- **Runs:** the same rates (400, 800, 1,200/s, plus higher if the disks allow), at least two runs per cell, alternating before and after.
- **Measure:** the stage timeline and the router and client totals as here.
- **Optional:** one run per cell with only one of the two flags on, to separate their effects.
