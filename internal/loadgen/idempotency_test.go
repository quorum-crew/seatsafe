package loadgen

import (
	"strings"
	"testing"
)

func TestIdempotencyConflicts(t *testing.T) {
	at := func(id, outcome string, ret int64, named ...string) Attempt {
		code := "OK"
		if outcome == "" {
			code = "DeadlineExceeded"
		}
		return Attempt{RequestID: id, Seats: []string{"e/s/a", "e/s/b"}, CallNs: ret - 5, ReturnNs: ret, Outcome: outcome, Code: code, Unavailable: named}
	}
	replay := at("o1", OutcomeConfirmed, 30e6)
	replay.Replayed = true
	for name, c := range map[string]struct {
		attempts []Attempt
		want     []string // "request: detail" of every conflict
	}{
		"CONFIRMED, then SEAT_UNAVAILABLE for the same request": {
			[]Attempt{at("o1", OutcomeConfirmed, 10e6), at("o1", OutcomeUnavailable, 30e6, "e/s/a")},
			[]string{"o1: answered CONFIRMED at 10.000 ms, then SEAT_UNAVAILABLE (e/s/a) at 30.000 ms"}},
		"the later answer is first in the file": {
			[]Attempt{at("o1", OutcomeUnavailable, 30e6, "e/s/a"), at("o1", OutcomeConfirmed, 10e6)},
			[]string{"o1: answered CONFIRMED at 10.000 ms, then SEAT_UNAVAILABLE (e/s/a) at 30.000 ms"}},
		"SEAT_UNAVAILABLE naming different seats": {
			[]Attempt{at("o1", OutcomeUnavailable, 10e6, "e/s/a"), at("o1", OutcomeUnavailable, 30e6, "e/s/b")},
			[]string{"o1: answered SEAT_UNAVAILABLE (e/s/a) at 10.000 ms, then SEAT_UNAVAILABLE (e/s/b) at 30.000 ms"}},
		"ABORTED, then CONFIRMED; another request is fine": {
			[]Attempt{at("o2", OutcomeAborted, 10e6), at("o1", OutcomeConfirmed, 15e6), at("o2", OutcomeConfirmed, 30e6)},
			[]string{"o2: answered ABORTED at 10.000 ms, then CONFIRMED at 30.000 ms"}},
		"two requests, sorted; one conflict each though o1 has three answers": {
			[]Attempt{at("o2", OutcomeConfirmed, 10e6), at("o2", OutcomeAborted, 20e6),
				at("o1", OutcomeConfirmed, 10e6), at("o1", OutcomeAborted, 20e6), at("o1", OutcomeInvalid, 30e6)},
			[]string{"o1: answered CONFIRMED at 10.000 ms, then ABORTED at 20.000 ms", "o2: answered CONFIRMED at 10.000 ms, then ABORTED at 20.000 ms"}},

		"identical answers, the second one replayed": {[]Attempt{at("o1", OutcomeConfirmed, 10e6), replay}, nil},
		"identical SEAT_UNAVAILABLE answers, seats in another order": {
			[]Attempt{at("o1", OutcomeUnavailable, 10e6, "e/s/a", "e/s/b"), at("o1", OutcomeUnavailable, 30e6, "e/s/b", "e/s/a")}, nil},
		"attempts without an answer do not count": {
			[]Attempt{at("o1", "", 10e6), at("o1", OutcomeConfirmed, 30e6), at("o1", "", 50e6)}, nil},
		"only unanswered attempts": {[]Attempt{at("o1", "", 10e6), at("o1", "", 30e6)}, nil},
		"different requests with different answers": {
			[]Attempt{at("o1", OutcomeConfirmed, 10e6), at("o2", OutcomeUnavailable, 30e6, "e/s/a")}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			var got []string
			for _, c := range IdempotencyConflicts(c.attempts) {
				got = append(got, c.Request+": "+c.Detail)
			}
			if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
				t.Fatalf("conflicts:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(c.want, "\n"))
			}
			v := CheckIdempotency(c.attempts)
			if len(v) != len(c.want) || (len(v) > 0 && (v[0].Check != "idempotency" || !strings.HasPrefix(v[0].Seat, "request o"))) {
				t.Fatalf("violations: %+v", v)
			}
		})
	}
}
