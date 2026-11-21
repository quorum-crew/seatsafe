package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/quorum-crew/seatsafe/internal/loadgen"
)

// Remote is a cluster that does not run under Docker Compose on this
// machine: the hosts of an inventory, driven by cmd/clusterctl. With
// Cluster.Remote set, everything the chaos tool does to the cluster goes
// through it instead of the docker CLI.
//
// Services are named as in the Compose cluster ("shard2-node1", "coord-3");
// a service outside every Raft group, such as a router, goes by the name its
// Remote gives it in Zones.
type Remote interface {
	// Metrics returns the text of a node's /metrics.
	Metrics(ctx context.Context, service string) (string, error)
	// Kill stops the services' processes at once (SIGKILL); Start brings
	// them back with their data.
	Kill(ctx context.Context, services []string) error
	Start(ctx context.Context, services []string) error
	// Pause freezes a service's process; Unpause lets it go on.
	Pause(ctx context.Context, service string) error
	Unpause(ctx context.Context, service string) error
	// Partition cuts a service off from every other node of the cluster,
	// in both directions, without touching anything else on its machine;
	// Rejoin undoes it. Rejoin must be safe to call when nothing is cut.
	Partition(ctx context.Context, service string) error
	Rejoin(ctx context.Context, service string) error
	// Reset wipes the cluster and starts it again, ready.
	Reset(ctx context.Context) error
	// Logs returns the logs of every node, for a failed run.
	Logs(ctx context.Context) (string, error)
	// Load runs the load generator with args (everything after "loadgen",
	// with container paths /deploy for the dataset and /out for the
	// output), on as many machines as it has. When it returns, dir holds
	// one run's history.jsonl, audit.json and summary.json, and the
	// returned text is the load generator's summary.
	Load(ctx context.Context, dir string, args []string) (string, error)
	// Zones returns the services in each availability zone: every node of
	// a Raft group, and whatever else of SeatSafe runs there.
	Zones() map[string][]string
}

// zones returns the services of each availability zone. The Compose cluster
// has no zones; there, zone N is node N of every group, which is how a real
// deployment is laid out (one node of every group per zone).
func (c *Cluster) zones() map[string][]string {
	if c.Remote != nil {
		return c.Remote.Zones()
	}
	out := map[string][]string{}
	for _, n := range NodeIDs {
		name := fmt.Sprintf("node-%d", n)
		for _, g := range GroupIDs() {
			out[name] = append(out[name], Service(g, n))
		}
	}
	return out
}

func sortedZones(z map[string][]string) []string {
	out := make([]string, 0, len(z))
	for k := range z {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Availability counts, among the orders of the run in dir whose first call
// was between from and to (the outage), those that were answered: with any
// outcome but UNKNOWN. It needs summary.json's started_unix_ns, the wall
// clock of the history's time base.
func Availability(dir string, from, to time.Time) (answered, total int, err error) {
	var sum struct {
		Started int64 `json:"started_unix_ns"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		return 0, 0, err
	}
	if err := json.Unmarshal(b, &sum); err != nil {
		return 0, 0, err
	}
	if sum.Started == 0 {
		return 0, 0, fmt.Errorf("summary.json has no started_unix_ns")
	}
	f, err := os.Open(filepath.Join(dir, "history.jsonl"))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	attempts, err := loadgen.ReadHistory(f)
	if err != nil {
		return 0, 0, err
	}
	lo, hi := from.UnixNano()-sum.Started, to.UnixNano()-sum.Started
	first := map[string]int64{}
	ok := map[string]bool{}
	for _, a := range attempts {
		if t, seen := first[a.RequestID]; !seen || a.CallNs < t {
			first[a.RequestID] = a.CallNs
		}
		if a.Outcome != "" {
			ok[a.RequestID] = true
		}
	}
	for id, t := range first {
		if t >= lo && t < hi {
			total++
			if ok[id] {
				answered++
			}
		}
	}
	return answered, total, nil
}
