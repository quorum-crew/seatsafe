package main

import (
	"testing"
	"time"
)

func TestCoordAttemptTimeout(t *testing.T) {
	for in, want := range map[string]time.Duration{"": time.Second, "1500": 1500 * time.Millisecond, "250": 250 * time.Millisecond} {
		if got, err := coordAttemptTimeout(in); err != nil || got != want {
			t.Errorf("coordAttemptTimeout(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"0", "-1", "1s", "fast"} {
		if _, err := coordAttemptTimeout(in); err == nil {
			t.Errorf("coordAttemptTimeout(%q) accepted", in)
		}
	}
}
