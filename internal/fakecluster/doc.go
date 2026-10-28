// Package fakecluster is an in-process stand-in for a SeatSafe cluster, for
// tests and local development only. It is NOT the real system: there is no
// Raft, no replication, no durability and no hold expiry.
//
//   - FakeGroup is one shard: 3 FakeNodes that share a single seatsm.Machine.
//     Commands are applied under one mutex with the next log index and a
//     NowMs from an injectable clock. Exactly one node is leader at a time;
//     followers answer FAILED_PRECONDITION with the seatsafe-leader trailer,
//     and with no leader every node answers UNAVAILABLE. SetLeader and
//     SetNoLeader simulate elections; Status reports role, term and leader.
//   - FakeCoordinator stands in for the coordinator Raft group until it
//     exists: an in-memory 2PC over the fake shards' ParticipantService
//     (Prepare in parallel with a 300 ms deadline, Commit if all vote YES,
//     otherwise Abort), with results stored by txn_id for replays.
//   - Cluster wires everything to listeners: bufconn for tests
//     (NewBufconn), or TCP for cmd/fakecluster.
package fakecluster
