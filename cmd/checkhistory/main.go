// Command checkhistory checks a history.jsonl from loadgen offline: no seat in
// two CONFIRMED orders, and no CONFIRMED that real time rules out. New checks,
// such as a Porcupine model, go in loadgen.Checks.
//
//	go run ./cmd/checkhistory results/<run>/history.jsonl
//
// Exit code 1 means a check failed.
package main

import (
	"fmt"
	"os"

	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: checkhistory history.jsonl")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer f.Close()
	history, err := loadgen.ReadHistory(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	failed := false
	for _, c := range loadgen.Checks {
		v := c.Run(history)
		verdict := "PASS"
		if len(v) > 0 {
			verdict, failed = "FAIL", true
		}
		fmt.Printf("%s: %s, %d violations, %d attempts\n", c.Name, verdict, len(v), len(history))
		for i, x := range v {
			if i == 20 {
				fmt.Printf("  ... and %d more\n", len(v)-i)
				break
			}
			fmt.Printf("  %s: %s\n", x.Seat, x.Detail)
		}
	}
	if failed {
		os.Exit(1)
	}
}
