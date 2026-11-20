package main

import (
	"strings"
	"testing"
	"time"
)

func TestParsePeers(t *testing.T) {
	self, ids, addrs, err := parsePeers("coord-2", "coord-1:7000, coord-2:7000,coord-3:7000")
	if err != nil {
		t.Fatal(err)
	}
	if self != 2 || len(ids) != 3 || addrs[1] != "coord-1:7000" || addrs[3] != "coord-3:7000" {
		t.Fatalf("self %d, ids %v, addrs %v", self, ids, addrs)
	}
	if _, _, _, err := parsePeers("coord-9", "coord-1:7000,coord-2:7000"); err == nil || !strings.Contains(err.Error(), "not a host in PEERS") {
		t.Fatalf("unknown NODE_ID: %v", err)
	}
	if _, _, _, err := parsePeers("coord-1", "coord-1"); err == nil {
		t.Fatal("a peer without a port was accepted")
	}
}

func TestConfigFromEnv(t *testing.T) {
	c, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.tick != 25*time.Millisecond || c.election != 10 || c.heartbeat != 1 || c.holdTTL != 5*time.Second || c.retention != 2*time.Minute {
		t.Fatalf("defaults: %+v", c)
	}

	t.Setenv("HOLD_TTL_MS", "1500")
	t.Setenv("GC_RETENTION_MS", "60000")
	t.Setenv("RAFT_TICK_MS", "50")
	if c, err = configFromEnv(); err != nil || c.holdTTL != 1500*time.Millisecond || c.retention != time.Minute || c.tick != 50*time.Millisecond {
		t.Fatalf("from env: %+v, %v", c, err)
	}

	for key, bad := range map[string]string{"HOLD_TTL_MS": "0", "GC_RETENTION_MS": "soon", "RAFT_TICK_MS": "-5"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, bad)
			if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("%s=%s: %v", key, bad, err)
			}
		})
	}
	t.Setenv("RAFT_ELECTION_TICKS", "2")
	t.Setenv("RAFT_HEARTBEAT_TICKS", "2")
	if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "must be greater") {
		t.Fatalf("election <= heartbeat: %v", err)
	}
}
