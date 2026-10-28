// Package seatsm is the deterministic seat state machine of a SeatSafe shard.
// The shard's Raft node calls Apply once for every committed log entry, in log
// order, on every replica.
//
// # Determinism
//
// Apply reads time only from ShardCommand.now_ms (stamped by the leader). It
// never calls time.Now or rand, and no result, returned list or snapshot byte
// depends on Go map iteration order: every list is built in request order or
// sorted by key. Replicas that apply the same entries end in the same state
// and produce byte-identical snapshots.
//
// Apply with an index at or below AppliedIndex is a no-op that returns an
// empty Result. Every other Apply advances AppliedIndex, even when the
// command is refused (Result.Err set), because the entry is committed either
// way.
//
// # State
//
//   - seats, keyed by (event, section, seat): AVAILABLE, HELD or BOOKED, with
//     the owner (request_id for a Book, txn_id for a 2PC order) and user_id
//   - req/{request_id}: the stored BookSeatsResponse and when it was applied
//   - txn/{txn_id}: PREPARED, COMMITTED or ABORTED, its seats, user,
//     hold_expires_at_ms, the vote and its conflicts, and when it was decided
//   - seed progress: seed_version, total_chunks and the applied chunks
//
// # Rules
//
// Seed. The first valid SeedOp fixes seed_version and total_chunks; a SeedOp
// with a different version or chunk count is refused. A chunk that was
// already applied is a no-op. Otherwise its seats are added as AVAILABLE,
// skipping seats that already exist. The shard is seeded once every chunk is
// applied. Until then Book answers INVALID and Prepare votes NO, and neither
// is recorded, so a retry after seeding succeeds.
//
// Book is all or nothing. An empty request_id is INVALID and not stored. A
// request_id seen before returns the stored answer with replayed = true.
// Otherwise the answer is stored: INVALID for an empty seat list, seats of
// more than one event, a duplicate seat or an unknown seat; SEAT_UNAVAILABLE
// with the taken seats in request order if any seat is HELD or BOOKED (no
// seat changes); else CONFIRMED with every seat BOOKED by the request_id.
// order_id is always the request_id.
//
// Prepare follows the no-wait rule. A txn already PREPARED or COMMITTED
// votes YES again; an ABORTED one votes NO again with the conflicts stored
// when it was refused. A request with an unknown seat or otherwise invalid
// seats votes NO with no conflicts. If any seat is HELD (by any txn) or
// BOOKED, the vote is NO with those seats as conflicts. Both NO cases record
// the txn as ABORTED, since a shard that votes NO may abort unilaterally.
// Otherwise the seats become HELD by the txn, which is recorded as PREPARED
// with its hold_expires_at_ms, and the vote is YES.
//
// Commit turns a PREPARED txn's seats BOOKED and marks it COMMITTED. Abort
// releases a PREPARED txn's seats and marks it ABORTED; an unknown txn gets an
// ABORTED tombstone so a delayed Prepare votes NO. Repeating a decision is a
// no-op, and so is a Commit of an unknown (already collected) txn. Commit of
// an ABORTED txn and Abort of a COMMITTED txn are protocol violations: Err is
// set and nothing changes.
//
// Hold expiry never releases seats on its own. ExpiredHolds lists PREPARED
// txns past their hold so the node can ask the coordinator for the decision
// (presumed abort) and propose the matching Commit or Abort.
//
// Gc drops req/ records applied before drop_before_ms and COMMITTED or
// ABORTED txn/ records decided before it. PREPARED txns, seats and seed
// progress are never dropped.
//
// # Snapshots
//
// Snapshot encodes every piece of state above plus AppliedIndex as one
// format version byte followed by JSON whose lists are sorted by key.
// Restore validates the snapshot, including CheckInvariants, and replaces
// all state.
package seatsm
