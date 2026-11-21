package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

// house is a booking service in memory: a seat is booked once.
type house struct {
	mu    sync.Mutex
	u     *loadgen.Universe
	taken map[loadgen.Seat]bool
}

func (h *house) book(_ context.Context, q *seatsafev1.BookSeatsRequest) (*seatsafev1.BookSeatsResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	resp := &seatsafev1.BookSeatsResponse{Outcome: seatsafev1.BookingOutcome_BOOKING_OUTCOME_CONFIRMED}
	for _, s := range q.GetSeats() {
		if h.taken[loadgen.Seat{Event: s.GetEventId(), Section: s.GetSectionId(), ID: s.GetSeatId()}] {
			resp.Outcome = seatsafev1.BookingOutcome_BOOKING_OUTCOME_SEAT_UNAVAILABLE
			resp.UnavailableSeats = append(resp.UnavailableSeats, s)
		}
	}
	if len(resp.UnavailableSeats) == 0 {
		for _, s := range q.GetSeats() {
			h.taken[loadgen.Seat{Event: s.GetEventId(), Section: s.GetSectionId(), ID: s.GetSeatId()}] = true
		}
	}
	return resp, nil
}

func (h *house) list(_ context.Context, sec loadgen.Section) ([]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var free []string
	for _, r := range h.u.Rows {
		if r.Event != sec.Event || r.Section != sec.ID {
			continue
		}
		for _, id := range r.Seats {
			if !h.taken[loadgen.Seat{Event: r.Event, Section: r.Section, ID: id}] {
				free = append(free, id)
			}
		}
	}
	return free, nil
}

// capture runs f and returns its exit code and what it printed.
func capture(t *testing.T, f func() int) (int, string) {
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
	code := f()
	os.Stdout = old
	w.Close()
	return code, <-done
}

// Two workers, the snapshots around them, and "loadgen merge" on what they
// wrote: the output has the lines the chaos tool reads, and the exit code
// says whether the audit passed.
func TestMergeCommand(t *testing.T) {
	sm, seeds, err := fakecluster.Dataset{
		Shards: 3, FlashSections: 30, FlashRows: 20, FlashSeatsPerRow: 50,
		Events: 2, Sections: 3, Rows: 4, SeatsPerRow: 5, ChunkSize: 500,
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	u, err := loadgen.NewUniverse(sm, seeds)
	if err != nil {
		t.Fatal(err)
	}
	h := &house{u: u, taken: map[loadgen.Seat]bool{}}
	env := loadgen.Env{Book: h.book, List: h.list}
	root := t.TempDir()
	c := loadgen.Defaults()
	c.Workload, c.Warmup, c.Quiet, c.History = loadgen.WorkloadFlash, 0, true, true
	c.Rate, c.Duration, c.Workers = 400, 300*time.Millisecond, 2
	baseline, final := filepath.Join(root, "baseline.jsonl"), filepath.Join(root, "final.jsonl")
	if err := loadgen.Snapshot(context.Background(), c, u, env, loadgen.PhaseBaseline, baseline); err != nil {
		t.Fatal(err)
	}
	c.StartAt = float64(time.Now().Add(100*time.Millisecond).UnixNano()) / 1e9
	errs := make(chan error, 2)
	var dirs []string
	for id := range 2 {
		w := c
		w.WorkerID, w.Out = id, filepath.Join(root, fmt.Sprintf("w%d", id))
		dirs = append(dirs, w.Out)
		go func() {
			_, err := loadgen.Run(context.Background(), w, u, env)
			errs <- err
		}()
	}
	for range dirs {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if err := loadgen.Snapshot(context.Background(), c, u, env, loadgen.PhaseFinal, final); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(root, "merged")
	code, printed := capture(t, func() int {
		return merge([]string{"-out", out, "-baseline", baseline, "-final", final, "-label", "both", dirs[0], dirs[1]})
	})
	// The "all" row as the chaos tool reads it.
	allLatency := regexp.MustCompile(`(?m)^\s*all\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$`)
	if m := allLatency.FindStringSubmatch(printed); code != 0 || m == nil || m[1] != "240" || !strings.Contains(printed, "  audit PASS: 240 orders") {
		t.Fatalf("merge: exit %d\n%s", code, printed)
	}
	for _, name := range []string{"summary.json", "histograms.json", "timeseries.csv", "timeseries.hdr.jsonl", "history.jsonl", "audit.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Errorf("merged directory: %v", err)
		}
	}

	// The baseline given as the final state: every CONFIRMED seat is "still
	// free", the audit fails and the exit code is 2.
	code, printed = capture(t, func() int {
		return merge([]string{"-out", filepath.Join(root, "bad"), "-baseline", baseline, "-final", baseline, dirs[0], dirs[1]})
	})
	if code != 2 || !strings.Contains(printed, "  audit FAIL: 240 orders") {
		t.Fatalf("merge with a failing audit: exit %d\n%s", code, printed)
	}

	// Without the snapshots: the numbers, no audit.
	code, printed = capture(t, func() int { return merge([]string{"-out", filepath.Join(root, "plain"), dirs[1], dirs[0]}) })
	if code != 0 || strings.Contains(printed, "audit") || !allLatency.MatchString(printed) {
		t.Fatalf("merge without snapshots: exit %d\n%s", code, printed)
	}

	for _, args := range [][]string{
		{},
		{"-out", filepath.Join(root, "x")}, // no worker directories
		{dirs[0], dirs[1]},                 // no -out
		{"-out", filepath.Join(root, "x"), dirs[0]},                              // one of two workers
		{"-out", filepath.Join(root, "x"), dirs[0], dirs[0]},                     // the same worker twice
		{"-out", filepath.Join(root, "x"), filepath.Join(root, "nope"), dirs[1]}, // not a worker directory
		{"-out", filepath.Join(root, "x"), "-final", filepath.Join(root, "nope.jsonl"), dirs[0], dirs[1]},
		{"-nope"},
	} {
		if code, _ := capture(t, func() int { return merge(args) }); code != 1 {
			t.Errorf("merge %v: exit %d, want 1", args, code)
		}
	}
	for _, args := range [][]string{{}, {"-phase", "baseline"}, {"-out", filepath.Join(root, "s.jsonl"), "extra"}, {"-phase", "later", "-out", filepath.Join(root, "s.jsonl"), "-shardmap", filepath.Join(root, "missing.json")}} {
		if code := snapshot(args); code != 1 {
			t.Errorf("snapshot %v: exit %d, want 1", args, code)
		}
	}
}
