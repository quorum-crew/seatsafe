# Raft pipeline: replicate while persisting, apply persisted entries early (Nov 20)

**Change:** two things in the `raftnode` Ready loop no longer wait for the local fsync.

1. A leader sends a Ready's appends and heartbeats before it saves the Ready, so its followers write the entries while it does. It still counts its own copy only after its Save.
2. Committed entries that are already on this node's disk are handed to the apply goroutine before the Ready's Save.

These target stages 1 and 3 of the Nov 20 latency breakdown ("Save done → committed" and "committed → applied").

**Status:** safety is checked (tests, stress runs, 20-run chaos campaign: all pass). **The per-stage latency comparison has not been measured yet**; see the last section.

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

## Per-stage latency: not measured

The plan was to rerun the Nov 20 latency breakdown (400, 800 and 1,200 orders/s for 60 s on the 1,000,000-seat dataset, temporary stage timing on top of this branch) and compare every stage.

The runs need at least 3 GB of free host memory with the cluster up, the bar the Nov 20 runs used. On Nov 20, 9 attempts in a row found 1.1 to 2.0 GB free after waiting 5 minutes each, with browsers and chat apps open, and none was run. No numbers from below the bar are reported here.

To do: close the desktop apps listed in the Nov 20 breakdown, rerun the three rates, and add the before/after table of each stage here.
