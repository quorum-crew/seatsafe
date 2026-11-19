package coordinator

import (
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/metrics"
)

// Decision sources: who proposed the TxnDecided that won.
const (
	SourceVotes       = "votes"        // SubmitOrder, from the participants' votes
	SourceGetDecision = "get_decision" // a shard asked before any decision existed
	SourceNewLeader   = "new_leader"   // a new leader aborted a txn left undecided
	SourceSweep       = "sweep"        // the leader aborted an old undecided txn nobody was running
)

// LatencyBuckets are dense from 1 ms to 100 ms, then sparse up to 10 s.
var LatencyBuckets = []float64{
	0.001, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.06, 0.08, 0.1,
	0.15, 0.2, 0.3, 0.5, 0.75, 1, 2, 5, 10,
}

// Metrics are the coordinator's counters and histograms.
type Metrics struct {
	Submit     *metrics.CounterVec   // outcome: CONFIRMED, SEAT_UNAVAILABLE, ABORTED, or a gRPC code
	Prepare    *metrics.HistogramVec // prepare phase of an order run here
	Decide     *metrics.HistogramVec // proposing the decision until it is applied
	Total      *metrics.HistogramVec // SubmitOrder, by outcome
	Decisions  *metrics.CounterVec   // decision, source
	Retries    *metrics.CounterVec   // decision: failed Commit/Abort deliveries that are retried
	Violations *metrics.CounterVec   // shard, decision: a participant refused the decision
}

func newMetrics(r *metrics.Registry) *Metrics {
	return &Metrics{
		Submit: r.NewCounterVec("seatsafe_coordinator_submit_total", "SubmitOrder calls by outcome (a booking outcome, or the gRPC code of a failed call).", "outcome"),
		Prepare: r.NewHistogramVec("seatsafe_coordinator_prepare_duration_seconds",
			"Time to collect every participant's vote, or to give up on it.", LatencyBuckets),
		Decide: r.NewHistogramVec("seatsafe_coordinator_decide_duration_seconds",
			"Time from proposing TxnDecided until a decision for the txn is applied on the leader.", LatencyBuckets),
		Total: r.NewHistogramVec("seatsafe_coordinator_submit_duration_seconds",
			"SubmitOrder latency on the leader, by outcome.", LatencyBuckets, "outcome"),
		Decisions: r.NewCounterVec("seatsafe_coordinator_decisions_total",
			"Decisions logged, by decision (commit, abort) and by who proposed the one that won (votes, get_decision, new_leader, sweep).", "decision", "source"),
		Retries: r.NewCounterVec("seatsafe_coordinator_decision_retries_total",
			"Commit or Abort calls to a participant that failed and are retried.", "decision"),
		Violations: r.NewCounterVec("seatsafe_coordinator_protocol_violations_total",
			"Participants that refused a logged decision (FAILED_PRECONDITION without a leader trailer). Must stay 0.", "shard", "decision"),
	}
}

func decisionLabel(d seatsafev1.Decision) string {
	if d == seatsafev1.Decision_DECISION_COMMIT {
		return "commit"
	}
	return "abort"
}

func shardLabel(id uint32) string { return strconv.FormatUint(uint64(id), 10) }

// outcomeLabel turns BOOKING_OUTCOME_CONFIRMED into CONFIRMED.
func outcomeLabel(o seatsafev1.BookingOutcome) string {
	return strings.TrimPrefix(o.String(), "BOOKING_OUTCOME_")
}

// codeLabel turns codes.DeadlineExceeded into DEADLINE_EXCEEDED (and OK into OK).
func codeLabel(c codes.Code) string {
	var b strings.Builder
	prevLower := false
	for _, ch := range c.String() {
		upper := ch >= 'A' && ch <= 'Z'
		if upper && prevLower {
			b.WriteByte('_')
		}
		prevLower = ch >= 'a' && ch <= 'z'
		b.WriteString(strings.ToUpper(string(ch)))
	}
	return b.String()
}
