// Package coordsm is the deterministic state machine of the 2PC coordinator
// group (Raft group 0). The group's Raft node calls Apply once for every
// committed log entry, in log order, on every replica.
//
// # Determinism
//
// Apply reads time only from CoordCommand.now_ms (stamped by the leader). It
// never calls time.Now or rand, and no result, returned list or snapshot byte
// depends on Go map iteration order: every list is kept in command order or
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
// One record per txn_id: the user, the participants (shard and seats, as
// TxnStarted listed them), the decision with its outcome and unavailable
// seats, the shards that acked the decision, and when the txn was started and
// decided (both from now_ms).
//
// # Rules
//
// TxnStarted creates an undecided record. If the txn already exists, decided
// or not, nothing changes and the existing record is returned. A TxnStarted
// with an empty txn_id, no participants, a participant without seats, or
// shard ids that are not strictly increasing is refused.
//
// TxnDecided: the first one applied for a txn wins. Later ones change nothing
// and return the existing record, so the proposer learns the decision that
// won (Result.Won tells it whether that was its own). A decision for an
// unknown txn creates a decided record with no participants: that is how
// GetDecision's ABORT for a txn the coordinator never saw stays final. The
// decision must be COMMIT with outcome CONFIRMED and no unavailable seats, or
// ABORT with outcome ABORTED (no seats) or SEAT_UNAVAILABLE (at least one
// seat); anything else is refused.
//
// TxnAcked marks a participant shard as having applied the decision.
// Repeating an ack is a no-op, and so is an ack for an unknown (already
// collected) txn. An ack for an undecided txn or from a shard that is not a
// participant is refused.
//
// Gc drops a txn only if it is decided, was decided before drop_before_ms,
// and every participant has acked (a record without participants has nobody
// to wait for). Undecided txns and txns still missing an ack are never
// dropped, whatever their age.
//
// # Snapshots
//
// Snapshot encodes every record plus AppliedIndex as one format version byte
// followed by JSON with the txns sorted by id. Restore validates the
// snapshot, including CheckInvariants, and replaces all state.
package coordsm
