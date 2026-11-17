# Election timeout tuning (Nov 17)

**Question:** how short can the Raft election timeout be before it starts
costing elections nobody needed? Failover after a leader dies is roughly one
election timeout, and the target is under 1 s.

**Result:** storage nodes now default to 25 ms ticks (heartbeat 25 ms,
election timeout randomized over 250-500 ms). Failover went from 1.2-2.0 s to
0.3-0.8 s with no extra elections. Separately, capping the Raft transport's
reconnect backoff at 1 s cut a restarted node's catch-up time from 2-6 s to
0.3-1.5 s.

## Method

`cmd/chaos` against the Compose cluster, one setting at a time
(`RAFT_TICK_MS` with 10 election ticks and 1 heartbeat tick, set on all 9
storage nodes). Per setting:

- 4 `kill-leader` runs and 3 `pause-leader` runs at 150 orders/s
- 3 `none` runs (no fault) at 300 orders/s, to catch elections nobody caused

Each run is 25 s of load from the load generator (open loop, uniform
workload, 69,200-seat default dataset), with the fault starting 5-10 s in and
lasting 3-10 s. Every run must pass the correctness audit and end with every
shard healed.

- **Failover:** from the fault until the other two nodes agree on a new leader
- **Elections:** Raft term increases during the run; a leader fault should cost exactly one
- **Client max:** the load generator's worst latency, from the intended send
  time, so it includes the stall a failover causes

Environment: a 2-CPU, 7 GB Linux VM running all 13 SeatSafe containers plus
the load generator. That is far more cramped than the 16-core laptop of the
earlier benchmarks, so spurious elections are *more* likely here. Failover is
sampled about every 100-300 ms (one `docker exec` per node), so treat it as
accurate to a few hundred ms.

## Results

| Tick (election timeout) | Failover, kill-leader | Failover, pause-leader | Client max during a fault | Elections in fault runs | Elections with no fault | Client p99, no fault |
|---|---|---|---|---|---|---|
| 100 ms (1-2 s), old default | 1.5-2.0 s | 1.2-1.4 s | 1.2-1.9 s | 1 each | 0 | 59-61 ms |
| 50 ms (0.5-1 s) | 0.5-0.9 s | 0.6-0.9 s | 0.63-1.23 s | 1 each | 0 | 55-67 ms |
| **25 ms (0.25-0.5 s), new default** | **0.3-0.6 s** | **0.5-0.8 s** | **0.43-0.71 s** | **1 each** | **0** | 69-94 ms |
| 10 ms (0.1-0.2 s) | 0.2-0.3 s | 0.4-0.5 s | 0.21-0.64 s | **2 in 2 of 7 runs** | 0 | 85-95 ms |

All 40 runs passed the audit: 0 double bookings.

## What the numbers say

1. **The trade-off appears at 10 ms ticks.** Two of seven fault runs held an
   extra election (a node timed out on a leader that was alive), and latency
   without any fault rose, from heartbeat traffic on a cramped machine. 25 ms
   is the shortest setting without either, and it meets the under-1 s target
   with room to spare. 50 ms only just made it (up to 0.9 s).
2. **Below about 0.5 s, the router is the bottleneck, not Raft.** With fast
   elections the worst client stall stops improving at about 450-650 ms: the
   router's 500 ms per-attempt timeout plus backoff. Cutting failover further
   for clients means shortening that timeout too.
3. **Faster ticks exposed slow catch-up after a restart.** At 25 ms, a
   restarted node took 2-6 s to catch up, against 1-2 s at 100 ms. gRPC's
   reconnect backoff grows to as much as 120 s while a peer is down, and
   faster ticks mean more failed sends to grow it. Capping it at 1 s for Raft
   peers brought catch-up to 0.3-1.5 s (4 kill-leader runs, same seed):

   | 25 ms ticks | Recovery after restart |
   |---|---|
   | gRPC default backoff | 2.0, 3.2, 4.1, 5.8 s |
   | backoff capped at 1 s | 0.3, 1.0, 1.4, 1.5 s |

## To check on the laptop / AWS

The new default should be confirmed under the heaviest load we run. Run no-fault
chaos runs during the 10,000-user benchmark, or `go run ./cmd/chaos -faults
none -rate 1000`, and check that elections stay at 0. If they don't, go back
to `RAFT_TICK_MS=50` in compose: still under 1 s.
