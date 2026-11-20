// Command checkhistory checks a history.jsonl from loadgen offline: no seat in
// two CONFIRMED orders, and no CONFIRMED that real time rules out. With
// -linearizable it also checks the whole history for linearizability with
// Porcupine, one seat at a time (the model is described in
// internal/linearize).
//
//	go run ./cmd/checkhistory results/<run>/history.jsonl
//	go run ./cmd/checkhistory -linearizable results/<run>/history.jsonl
//
// When the history is not linearizable, the first bad seat and its operations
// are printed and Porcupine's visualization of that seat is written next to
// the history, as <history>.linearizability.html.
//
// Exit code 1 means a check failed. Exit code 3 means that nothing failed but
// the linearizability check ran out of time (-timeout) before it could decide
// every seat: that is not a pass.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/quorum-crew/seatsafe/internal/linearize"
	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("checkhistory", flag.ContinueOnError)
	linearizable := fs.Bool("linearizable", false, "also check linearizability with Porcupine, seat by seat")
	timeout := fs.Duration("timeout", linearize.DefaultTimeout, "-linearizable: time allowed for the whole check; running out is reported as TIMEOUT, not as a pass")
	noBrowses := fs.Bool("no-browses", false, "-linearizable: leave out the listings taken during the load (keep the baseline and the final read)")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: checkhistory [-linearizable] [-timeout 1m] [-no-browses] history.jsonl")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
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
		v := c.Run(history.Attempts)
		verdict := "PASS"
		if len(v) > 0 {
			verdict, code = "FAIL", 1
		}
		fmt.Printf("%s: %s, %d violations, %d attempts\n", c.Name, verdict, len(v), len(history.Attempts))
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

	r := linearize.Check(history, linearize.Options{Timeout: *timeout, NoBrowses: *noBrowses})
	fmt.Printf("linearizable: %s\n", r)
	if len(history.Listings) == 0 {
		fmt.Println("  the history has no listings (no baseline, browses or final read): only the orders were checked against each other")
	}
	if len(r.TimedOut) > 0 {
		fmt.Printf("  not decided within %v: %s\n", *timeout, sample(r.TimedOut))
	}
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
func sample(names []string) string {
	if len(names) > 5 {
		return strings.Join(names[:5], ", ") + fmt.Sprintf(", ... and %d more", len(names)-5)
	}
	return strings.Join(names, ", ")
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ") + "\n"
}
