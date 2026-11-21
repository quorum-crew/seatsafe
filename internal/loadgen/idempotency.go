package loadgen

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// VerdictIdempotency is the verdict of a run in which one request_id was
// answered in two different ways.
const VerdictIdempotency = "IDEMPOTENCY"

// Conflict is one request_id that was answered in two different ways.
type Conflict struct {
	Request string
	Detail  string // both answers, with the times they returned
}

// IdempotencyConflicts returns the requests of which two completed attempts
// got different answers: different outcomes, or SEAT_UNAVAILABLE naming
// different seats. A retry of a request must get the answer the first attempt
// got, replayed or not. Attempts that were not answered (an error, a timeout:
// UNKNOWN) say nothing and are left out.
//
// One conflict is reported per request, for the first answer that differs
// from the request's earliest one. The result is sorted by request id.
func IdempotencyConflicts(attempts []Attempt) []Conflict {
	type answer struct {
		outcome, named string
		ret            int64
	}
	answers := map[string][]answer{}
	for _, a := range attempts {
		if a.Outcome == "" {
			continue
		}
		named := slices.Clone(a.Unavailable)
		sort.Strings(named)
		answers[a.RequestID] = append(answers[a.RequestID], answer{a.Outcome, strings.Join(slices.Compact(named), " "), a.ReturnNs})
	}
	describe := func(a answer) string {
		s := a.outcome
		if a.named != "" {
			s += " (" + a.named + ")"
		}
		return fmt.Sprintf("%s at %.3f ms", s, float64(a.ret)/1e6)
	}
	var out []Conflict
	for id, as := range answers {
		sort.SliceStable(as, func(i, j int) bool { return as[i].ret < as[j].ret })
		for _, a := range as[1:] {
			if a.outcome != as[0].outcome || a.named != as[0].named {
				out = append(out, Conflict{id, fmt.Sprintf("answered %s, then %s", describe(as[0]), describe(a))})
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Request < out[j].Request })
	return out
}

// CheckIdempotency is IdempotencyConflicts as a Check: the "seat" of a
// violation is the request.
func CheckIdempotency(history []Attempt) []Violation {
	var out []Violation
	for _, c := range IdempotencyConflicts(history) {
		out = append(out, Violation{"idempotency", "request " + c.Request, c.Detail})
	}
	return out
}
