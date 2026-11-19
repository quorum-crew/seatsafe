package seatsm

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// A capture encodes to exactly what Snapshot returned at that moment, however
// much is applied while it is being encoded, including concurrently (-race).
func TestCaptureSnapshotIsFrozen(t *testing.T) {
	m := buildState(t, stateShape{name: "capture", flashSections: 4, regularEvents: 2})
	idx := m.AppliedIndex()
	apply := func(cmd *seatsafev1.ShardCommand) {
		idx++
		if r := m.Apply(idx, cmd); r.Err != nil {
			t.Error(r.Err)
		}
	}
	// churn books seats, refuses repeats, prepares and decides 2PC txns and
	// runs GC: every kind of in-place change Apply makes.
	churn := func(tag string, from, to int) {
		for i := from; i < to; i++ {
			now := int64(1000 + i)
			s := ref("evt-flash", fmt.Sprintf("sec-%03d", 101+i%4), fmt.Sprintf("R%02d-S%03d", 1+i/200%20, 1+i/4%50))
			switch i % 5 {
			case 0, 1:
				apply(bookCmd(now, fmt.Sprintf("%s-b%d", tag, i), s))
			case 2:
				apply(bookCmd(now, fmt.Sprintf("%s-b%d", tag, i-1), s)) // replay or refusal
			case 3:
				apply(prepCmd(now, fmt.Sprintf("%s-t%d", tag, i), now+5000, s))
			case 4:
				if i%10 == 4 {
					apply(commitCmd(now, fmt.Sprintf("%s-t%d", tag, i-1)))
				} else {
					apply(abortCmd(now, fmt.Sprintf("%s-t%d", tag, i-1)))
				}
			}
		}
		apply(gcCmd(int64(1000+to), int64(1000+from+(to-from)/2)))
	}
	churn("before", 0, 1500)
	want, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	encode := m.CaptureSnapshot()

	// Keep applying while the capture encodes.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		churn("after", 1500, 4000)
	}()
	got, err := encode()
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !bytes.Equal(got, want) {
		t.Fatal("the capture encoded differently from Snapshot at capture time")
	}
	after, _ := m.Snapshot()
	if bytes.Equal(after, want) {
		t.Fatal("churn changed nothing; the test proves nothing")
	}
	if again, _ := encode(); !bytes.Equal(again, want) {
		t.Fatal("encoding the capture twice differs")
	}
}

// How long Apply waits for a capture versus a full Snapshot, at about one shard
// of the 1M-seat benchmark.
func TestCaptureCostAtBenchmarkScale(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("timing; builds a 333k-seat machine")
	}
	m := buildState(t, stateShapes[len(stateShapes)-1])
	start := time.Now()
	encode := m.CaptureSnapshot()
	captured := time.Since(start)
	start = time.Now()
	data, _ := encode()
	encoded := time.Since(start)
	start = time.Now()
	_, _ = m.Snapshot()
	full := time.Since(start)
	t.Logf("%.2f MiB: capture %v (Apply waits), encode %v (in the background), Snapshot %v",
		float64(len(data))/(1<<20), captured.Round(time.Millisecond), encoded.Round(time.Millisecond), full.Round(time.Millisecond))
}
