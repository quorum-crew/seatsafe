package main

import (
	"strings"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/router"
)

func env(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func TestConfigFromEnv(t *testing.T) {
	cfg, err := configFromEnv(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Group.AttemptTimeout != router.DefaultShardAttemptTimeout || cfg.Coordinator.AttemptTimeout != time.Second {
		t.Fatalf("defaults: shard %v, coordinator %v", cfg.Group.AttemptTimeout, cfg.Coordinator.AttemptTimeout)
	}
	cfg, err = configFromEnv(env(map[string]string{"ROUTER_SHARD_ATTEMPT_MS": "250", "ROUTER_COORD_ATTEMPT_MS": "1500"}))
	if err != nil || cfg.Group.AttemptTimeout != 250*time.Millisecond || cfg.Coordinator.AttemptTimeout != 1500*time.Millisecond {
		t.Fatalf("from env: %+v, %v", cfg, err)
	}
	for name, c := range map[string]struct {
		kv   map[string]string
		want string
	}{
		"shard zero":            {map[string]string{"ROUTER_SHARD_ATTEMPT_MS": "0"}, "ROUTER_SHARD_ATTEMPT_MS must be a positive"},
		"shard negative":        {map[string]string{"ROUTER_SHARD_ATTEMPT_MS": "-5"}, "ROUTER_SHARD_ATTEMPT_MS must be a positive"},
		"shard not a number":    {map[string]string{"ROUTER_SHARD_ATTEMPT_MS": "250ms"}, "ROUTER_SHARD_ATTEMPT_MS must be a positive"},
		"coordinator zero":      {map[string]string{"ROUTER_COORD_ATTEMPT_MS": "0"}, "ROUTER_COORD_ATTEMPT_MS must be a positive"},
		"shard at the deadline": {map[string]string{"ROUTER_SHARD_ATTEMPT_MS": "2000"}, "shard attempt timeout 2s must be shorter than the client deadline 2s"},
		"coordinator too long":  {map[string]string{"ROUTER_COORD_ATTEMPT_MS": "2500"}, "coordinator attempt timeout 2.5s must be shorter"},
	} {
		if _, err := configFromEnv(env(c.kv)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error containing %q", name, err, c.want)
		}
	}
}
