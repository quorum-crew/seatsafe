// Command checkhistory checks a history.jsonl from loadgen offline: no seat in
// two CONFIRMED orders, and no CONFIRMED that real time rules out. With
// -linearizable it also checks the history for linearizability with
// Porcupine, one seat at a time (the model is described in
// internal/linearize):
//
//	go run ./cmd/checkhistory results/<run>/history.jsonl
//	go run ./cmd/checkhistory -linearizable results/<run>/history.jsonl
//
// The linearizability check prints two results. "bookings" is over the
// orders, the pre-run baseline and the post-run final read: it is the verdict
// and what the exit code reports. "stale browses: N (seats ...)" counts the
// listings taken during the load that contradict the bookings: they are
// reported and do not fail the check, and -no-browses leaves them out.
//
// When the bookings are not linearizable, the first bad seat and its operations
// are printed and Porcupine's visualization of that seat is written next to
// the history, as <history>.linearizability.html.
//
// A history merged from several load generator machines has times from clocks
// that differ by a few milliseconds. -widen-ms N allows for that: the
// real-time-order check and the linearizability check then treat every call
// as N ms earlier and every return as N ms later.
//
// Exit code 1 means a check failed. Exit code 3 means that nothing failed but
// the bookings check ran out of time (-timeout) before it could decide every
// seat: that is not a pass. Stale browses do not change the exit code.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/quorum-crew/seatsafe/internal/linearize"
	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("checkhistory", flag.ContinueOnError)
	linearizable := fs.Bool("linearizable", false, "also check linearizability with Porcupine, seat by seat: the bookings (the verdict) and, reported separately, stale browses")
	timeout := fs.Duration("timeout", linearize.DefaultTimeout, "-linearizable: time allowed for the whole check; running out is reported as TIMEOUT, not as a pass")
	noBrowses := fs.Bool("no-browses", false, "-linearizable: do not look for stale browses (the bookings verdict never uses them)")
	widenMs := fs.Float64("widen-ms", 0, "clock skew to allow for, in ms (may have a fraction): the real-time-order check and -linearizable treat every call as this much earlier and every return as this much later")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: checkhistory [-linearizable] [-timeout 1m] [-no-browses] [-widen-ms 0] history.jsonl")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	if *widenMs < 0 || *widenMs > 3600e3 {
		fmt.Fprintln(os.Stderr, "checkhistory: -widen-ms must be between 0 and an hour")
		return 2
	}
	widen := time.Duration(*widenMs * float64(time.Millisecond))
	path := fs.Arg(0)
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer f.Close()
	history, err := loadgen.ReadFullHistory(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	code := 0
	for _, c := range loadgen.Checks {
		attempts, note := history.Attempts, ""
		if widen > 0 && c.Name == "real-time-order" {
			// The only one of these checks that compares times of
			// different orders, which may be from different machines.
			attempts, note = loadgen.Widen(attempts, widen), fmt.Sprintf(" (widened by %v)", widen)
		}
		v := c.Run(attempts)
		verdict := "PASS"
		if len(v) > 0 {
			verdict, code = "FAIL", 1
			if c.Verdict != "" {
				verdict = c.Verdict
			}
		}
		fmt.Printf("%s: %s, %d violations, %d attempts%s\n", c.Name, verdict, len(v), len(history.Attempts), note)
		for i, x := range v {
			if i == 20 {
				fmt.Printf("  ... and %d more\n", len(v)-i)
				break
			}
			fmt.Printf("  %s: %s\n", x.Seat, x.Detail)
		}
	}
	if !*linearizable {
		return code
	}

	r := linearize.Check(history, linearize.Options{Timeout: *timeout, NoBrowses: *noBrowses, Widen: widen})
	fmt.Printf("bookings: %s\n", r)
	if len(history.Listings) == 0 {
		fmt.Println("  the history has no listings (no baseline, browses or final read): only the orders were checked against each other")
	}
	if len(r.TimedOut) > 0 {
		fmt.Printf("  not decided within %v: %s\n", *timeout, sample(r.TimedOut))
	}
	// The browses are reported after the verdict and never change the exit
	// code.
	defer func() {
		line := r.StaleLine()
		if *noBrowses {
			line += " (-no-browses)"
		}
		fmt.Println(line)
		if r.StaleBrowses > 0 {
			fmt.Print(indent(r.FirstStale()))
		}
		if len(r.BrowseUndecided) > 0 {
			fmt.Printf("  browses not decided within %v: %s\n", *timeout, sample(r.BrowseUndecided))
		}
	}()
	switch r.Verdict() {
	case linearize.Fail:
		fmt.Printf("  first bad seat: %s (%d bad seats: %s)\n", r.Illegal[0], len(r.Illegal), sample(r.Illegal))
		fmt.Print(indent(r.FirstBad()))
		html := strings.TrimSuffix(path, ".jsonl") + ".linearizability.html"
		if err := visualize(r, html); err != nil {
			fmt.Fprintln(os.Stderr, "could not write the visualization:", err)
		} else {
			fmt.Printf("  visualization of %s: %s\n", r.Illegal[0], html)
		}
		return 1
	case linearize.Timeout:
		if code == 0 {
			code = 3
		}
	}
	return code
}

func visualize(r *linearize.Report, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := r.Visualize(out); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// sample lists up to five names.
func sample(names []string) string { return linearize.Sample(names) }

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ") + "\n"
}
