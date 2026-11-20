package main

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func attempt(req string, call, ret int, outcome, seat string, named bool) string {
	u := ""
	if named {
		u = `,"unavailable_seats":["` + seat + `"]`
	}
	return `{"client_id":1,"request_id":"` + req + `","seats":["` + seat + `"],"call_ns":` + strconv.Itoa(call) + `,"return_ns":` + strconv.Itoa(ret) +
		`,"outcome":"` + outcome + `","replayed":false,"code":"OK","cross":false` + u + "}\n"
}

func listing(phase string, call, ret int, free string) string {
	return `{"kind":"list","phase":"` + phase + `","event":"e","section":"s","free":[` + free + `],"call_ns":` + strconv.Itoa(call) + `,"return_ns":` + strconv.Itoa(ret) + "}\n"
}

func write(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// capture runs the command and returns its exit code and what it printed.
func capture(t *testing.T, args []string) (int, string) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	code := run(args)
	os.Stdout = old
	w.Close()
	return code, <-done
}

func TestRun(t *testing.T) {
	good := []string{
		listing("baseline", -20, -10, `"a","b"`),
		attempt("o1", 10, 20, "CONFIRMED", "e/s/a", false),
		attempt("o2", 30, 40, "SEAT_UNAVAILABLE", "e/s/a", true),
		listing("final", 50, 60, `"b"`),
	}
	html := func(path string) string { return strings.TrimSuffix(path, ".jsonl") + ".linearizability.html" }
	exists := func(path string) bool { _, err := os.Stat(path); return err == nil }

	p := write(t, good...)
	if code := run([]string{"-linearizable", p}); code != 0 || exists(html(p)) {
		t.Fatalf("linearizable history: exit %d, visualization written: %v", code, exists(html(p)))
	}
	if code := run([]string{p}); code != 0 {
		t.Fatalf("without -linearizable: exit %d", code)
	}
	// Out of time: neither a pass nor a failure.
	if code := run([]string{"-linearizable", "-timeout", "1ns", p}); code != 3 {
		t.Fatalf("timed out check: exit %d, want 3", code)
	}

	// SEAT_UNAVAILABLE for a seat that was free all along: only Porcupine
	// sees it, and it writes the visualization next to the history.
	p = write(t, listing("baseline", -20, -10, `"a"`), attempt("o1", 10, 20, "SEAT_UNAVAILABLE", "e/s/a", true), listing("final", 50, 60, `"a"`))
	if code := run([]string{p}); code != 0 {
		t.Fatalf("the old checks: exit %d", code)
	}
	if code := run([]string{"-linearizable", p}); code != 1 || !exists(html(p)) {
		t.Fatalf("bad history: exit %d, visualization written: %v", code, exists(html(p)))
	}
	if b, _ := os.ReadFile(html(p)); !strings.Contains(string(b), "SEAT_UNAVAILABLE (this seat)") {
		t.Fatalf("the visualization does not describe the operations (%d bytes)", len(b))
	}

	// A stale browse is reported and does not fail the check; -no-browses
	// does not look for it.
	p = write(t, append(append([]string{}, good[:3]...), listing("browse", 42, 44, `"a","b"`), good[3])...)
	code, out := capture(t, []string{"-linearizable", p})
	if code != 0 || exists(html(p)) || !strings.Contains(out, "bookings: PASS") || !strings.Contains(out, "stale browses: 1 (seats e/s/a); 1 browses checked") || !strings.Contains(out, "<- stale") {
		t.Fatalf("stale browse: exit %d\n%s", code, out)
	}
	code, out = capture(t, []string{"-linearizable", "-no-browses", p})
	if code != 0 || !strings.Contains(out, "bookings: PASS") || !strings.Contains(out, "stale browses: not checked (-no-browses)") {
		t.Fatalf("stale browse left out: exit %d\n%s", code, out)
	}
	// A double booking next to the stale browse still fails.
	p = write(t, append(append([]string{}, good[:3]...), listing("browse", 42, 44, `"a","b"`), attempt("o3", 45, 48, "CONFIRMED", "e/s/a", false), good[3])...)
	code, out = capture(t, []string{"-linearizable", p})
	if code != 1 || !strings.Contains(out, "bookings: FAIL") || !strings.Contains(out, "stale browses: ") {
		t.Fatalf("double booking with a stale browse: exit %d\n%s", code, out)
	}
	if code, out := capture(t, []string{"-linearizable", "-no-browses", p}); code != 1 || !strings.Contains(out, "bookings: FAIL") {
		t.Fatalf("double booking with -no-browses: exit %d\n%s", code, out)
	}

	// Two CONFIRMED orders: the old checks and Porcupine both fail.
	p = write(t, attempt("o1", 10, 20, "CONFIRMED", "e/s/a", false), attempt("o2", 30, 40, "CONFIRMED", "e/s/a", false))
	if code := run([]string{p}); code != 1 {
		t.Fatalf("double booking: exit %d", code)
	}
	if code := run([]string{"-linearizable", p}); code != 1 || !exists(html(p)) {
		t.Fatalf("double booking with -linearizable: exit %d", code)
	}

	for _, args := range [][]string{{}, {"a", "b"}, {filepath.Join(t.TempDir(), "missing.jsonl")}, {"-nope", p}} {
		if code := run(args); code != 2 {
			t.Fatalf("%v: exit %d, want 2", args, code)
		}
	}
}
