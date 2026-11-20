# Router shard attempt timeout (Nov 19)

**Question:** how short can the router's per-attempt deadline for shard groups (`ROUTER_SHARD_ATTEMPT_MS`) be? A paused or partitioned shard leader keeps its connections open, so a client waits until the attempt times out before the router looks for the new leader. A shorter timeout should shorten that wait, unless it starts cutting off healthy requests.

**Rule:** pick the lowest setting that, with no fault:
- has zero `attempt_timeout` retries; and
- is at least 3× the client p99.9.

**Result:** no setting tested meets both on this laptop, so the default stays **500 ms**. Two things prevent a pick:
- Without any fault, the machine stalled for hundreds of milliseconds now and then. Every shard group saw `attempt_timeout` retries at once, in one 500 ms run and two 200 ms runs.
- The no-fault p99.9 swung from 47 to 769 ms between runs of the same setting.

Shorter timeouts also didn't reliably shorten the client's wait when a leader was paused (see below). Repeat this on AWS, with the load generator on its own machine.

## Method

Code: `mrudula/router-timeouts`, on top of #43. The Compose cluster ran with the 69,200-seat default dataset, 25 ms Raft ticks (#41), and `ROUTER_SHARD_ATTEMPT_MS` set on the router through a compose override. `ROUTER_COORD_ATTEMPT_MS` stayed at 1000; these runs are single-shard only.

- **Pause-leader:** `cmd/chaos -faults pause-leader -runs 3 -seed 20261120` (uniform workload, open loop, 200 orders/s, 30 s per run). The fault is `docker pause` on a shard's leader for 3-10 s, 5-10 s into the load. The same seed gives every setting the same faults (always shard 2). **Client max** is the load generator's worst latency in the run, measured from the intended send time, so it includes the failover stall.
- **No fault:** `cmd/chaos -faults none -runs 1 -rate 1000 -duration 60s`, three times per setting. The cluster is wiped and restarted before each run. `attempt_timeout` is the router's `seatsafe_groupclient_retries_total{reason="attempt_timeout"}`, summed over the three shard groups, read after the run.

Every run passed the load generator's audit (rules a-e) with 0 UNKNOWN orders, and every shard healed.

Machine: Intel Core Ultra 7 155H laptop with 15.5 GB RAM, running Docker Desktop (16 CPUs, 12 GB). Every container and the load generator shared it, alongside a browser. Host free memory was 0.4-2 GB during the runs. One earlier attempt at the 150 ms pause runs was cut short when the host ran critically low on memory. That run is not counted; 150 ms was rerun.

## Results

### Pause-leader (3 runs per setting)

| Attempt timeout | Client max per run (ms) | Median | Failover per run (s) |
|---|---|---|---|
| 500 ms | 637, 639, 629 | 637 | 0.8, 0.8, 0.8 |
| 300 ms | 426, 424, 844 | 426 | 0.9, 0.5, 0.8 |
| 250 ms | 737, 743, 737 | 737 | 0.8, 0.8, 0.8 |
| 200 ms | 637, 1435, 636 | 637 | 0.7, 0.8, 0.8 |
| 150 ms | 800, 549, 871 | 800 | 0.9, 0.6, 1.1 |

### No fault, 1,000 orders/s for 60 s (3 runs per setting)

| Attempt timeout | p99.9 per run (ms) | Median p99.9 | 3 × median | `attempt_timeout` retries per run | Meets the rule |
|---|---|---|---|---|---|
| 500 ms | 238, **769**, 190 | 238 | 715 ms | 0, **248**, 0 | no |
| 300 ms | 212, 75, 220 | 212 | 636 ms | 0, 0, 0 | no (3×) |
| 250 ms | 53, 91, 122 | 91 | 274 ms | 0, 0, 0 | no (3×) |
| 200 ms | 106, **261**, **307** | 261 | 783 ms | 0, **158**, **581** | no |
| 150 ms | 77, 83, 47 | 77 | 231 ms | 0, 0, 0 | no (3×) |

p50 was 19-25 ms in every run except the stalled 500 ms run (48 ms).

## What the numbers say

1. **The retries without a fault come from the machine, not from the timeouts.** When they happened, all three shard groups had them at the same time (for example 79 / 84 / 85 at 500 ms), and p99 and p99.9 jumped together. Meanwhile the shortest setting, 150 ms, had none in three runs. A shard that was merely slow would show up in one group, not all three at once.
2. **The 3× p99.9 rule can't be met below about 700 ms here.** The typical p99.9 on this laptop is 75-240 ms. That is mostly queueing in an open-loop client sharing 16 cores with 13 containers, not slow Raft commits.
3. **A shorter timeout doesn't reliably shorten a paused leader's stall.** What a client waits is roughly the time to detect the failure (an election timeout of 250-500 ms at 25 ms ticks), plus the router's discovery and backoff, plus at most one attempt timeout. Medians are 637 ms at 500, 426 at 300, 737 at 250, 637 at 200 and 800 at 150. With 3 runs per setting, run-to-run noise is larger than any trend.
4. **Keeping 500 ms costs little.** The worst stall for a paused leader stays under 0.65 s in every 500 ms run, with no outliers.

## Next

Rerun on AWS with the load generator on separate hosts, using the same rule. If the no-fault p99.9 is tens of milliseconds there, 200-300 ms will probably qualify. Set it with `ROUTER_SHARD_ATTEMPT_MS`; the router refuses values that are not positive or not shorter than the 2 s client deadline.

Raw results (`results/chaos-*`, per-run `console.txt`, `runs.csv`) are kept locally, not committed.
