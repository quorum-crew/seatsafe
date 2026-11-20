package router_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/quorum-crew/seatsafe/internal/fakecluster"
	"github.com/quorum-crew/seatsafe/internal/groupclient"
	"github.com/quorum-crew/seatsafe/internal/metrics"
	"github.com/quorum-crew/seatsafe/internal/router"
)

// Every group's failover series exist, at 0, as soon as the router starts.
// Prometheus' increase() needs a sample before the first leader change to
// see it; a series created by that change starts at 1 and the change is lost.
func TestFailoverSeriesStartAtZero(t *testing.T) {
	sm, _, err := fakecluster.SmallDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	rt, err := router.New(sm, router.Config{}, reg)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	var buf bytes.Buffer
	reg.Write(&buf)
	out := buf.String()
	for _, g := range []string{"shard-1", "shard-2", "shard-3", "coordinator"} {
		want := []string{
			`seatsafe_groupclient_leader_changes_total{group="` + g + `"} 0`,
			`seatsafe_groupclient_leader_recovery_seconds_count{group="` + g + `"} 0`,
			`seatsafe_groupclient_leader_recovery_seconds_sum{group="` + g + `"} 0`,
			`seatsafe_groupclient_leader_recovery_seconds_bucket{group="` + g + `",le="+Inf"} 0`,
		}
		for _, r := range groupclient.Reasons {
			want = append(want, `seatsafe_groupclient_retries_total{group="`+g+`",reason="`+r+`"} 0`)
		}
		for _, w := range want {
			if !strings.Contains(out, w+"\n") {
				t.Errorf("missing before any leader change: %s", w)
			}
		}
	}
	if t.Failed() {
		t.Logf("/metrics:\n%s", out)
	}
}

func TestConfigValidate(t *testing.T) {
	ms := time.Millisecond
	for name, c := range map[string]struct {
		cfg  router.Config
		want string // "" = valid
	}{
		"defaults":                {router.Config{}, ""},
		"tuned":                   {router.Config{Group: groupclient.Config{AttemptTimeout: 200 * ms}, Coordinator: groupclient.Config{AttemptTimeout: 1500 * ms}}, ""},
		"shard negative":          {router.Config{Group: groupclient.Config{AttemptTimeout: -ms}}, "shard attempt timeout -1ms must be positive"},
		"shard at the deadline":   {router.Config{Group: groupclient.Config{AttemptTimeout: 2 * time.Second}}, "shard attempt timeout 2s must be shorter than the client deadline 2s"},
		"shorter client deadline": {router.Config{DefaultDeadline: 400 * ms}, "shard attempt timeout 500ms must be shorter than the client deadline 400ms"},
		"coordinator too long":    {router.Config{Coordinator: groupclient.Config{AttemptTimeout: 3 * time.Second}}, "coordinator attempt timeout 3s must be shorter"},
		"coordinator negative":    {router.Config{Coordinator: groupclient.Config{AttemptTimeout: -ms}}, "coordinator attempt timeout -1ms must be positive"},
		"longer deadline is fine": {router.Config{DefaultDeadline: 5 * time.Second, Group: groupclient.Config{AttemptTimeout: 3 * time.Second}}, ""},
	} {
		err := c.cfg.Validate()
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v, want an error containing %q", name, err, c.want)
		}
	}
	sm, _, err := fakecluster.SmallDataset.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.New(sm, router.Config{Group: groupclient.Config{AttemptTimeout: 2 * time.Second}}, metrics.NewRegistry()); err == nil {
		t.Fatal("router.New accepted a shard attempt timeout as long as the deadline")
	}
}
