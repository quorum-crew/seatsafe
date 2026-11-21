package clusterctl

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/quorum-crew/seatsafe/internal/datagen"
	"github.com/quorum-crew/seatsafe/internal/inventory"
)

// Paths inside every container. The host's config directory is mounted
// there read-only; it holds the env file, the shard map and the seed files.
const (
	inConfig   = "/opt/seatsafe"
	inShardMap = inConfig + "/shardmap.json"
	inSeedDir  = inConfig + "/seed"
	inData     = "/data"
)

// Layout is what gen was asked for. It is saved next to the generated files
// (layout.json), so every later command works on the same cluster.
type Layout struct {
	Shards   int      `json:"shards"`   // 0 = every shard of the inventory
	Replicas int      `json:"replicas"` // 0 = every node of every group
	Seats    int      `json:"seats"`    // 0 = datagen's default dataset
	Set      []string `json:"set,omitempty"`
}

// The settings every role's env file is written with, and their defaults (the
// defaults of the binaries, written out so the file shows the whole
// configuration). -set changes them.
var tunables = map[string][][2]string{
	inventory.Storage: {
		{"RAFT_TICK_MS", "25"}, {"RAFT_ELECTION_TICKS", "10"}, {"RAFT_HEARTBEAT_TICKS", "1"},
		{"RAFT_SNAPSHOT_ENTRIES", "100000"}, {"RAFT_CATCHUP_ENTRIES", "5000"}, {"GC_RETENTION_MS", "120000"},
		{"RAFT_LEADER_SEND_FIRST", "true"}, {"RAFT_EARLY_APPLY", "true"}, {"PPROF", "0"},
	},
	inventory.Coordinator: {
		{"RAFT_TICK_MS", "25"}, {"RAFT_ELECTION_TICKS", "10"}, {"RAFT_HEARTBEAT_TICKS", "1"},
		{"HOLD_TTL_MS", "5000"}, {"GC_RETENTION_MS", "120000"},
		{"RAFT_LEADER_SEND_FIRST", "true"}, {"RAFT_EARLY_APPLY", "true"}, {"PPROF", "0"},
	},
	inventory.Router: {
		{"ROUTER_SHARD_ATTEMPT_MS", "500"}, {"ROUTER_COORD_ATTEMPT_MS", "1000"}, {"PPROF", "0"},
	},
}

// override is one -set entry.
type override struct{ role, key, value string }

// parseSet reads -set entries: KEY=VALUE for every role that has the
// setting, or role:KEY=VALUE for one role. A key no role has is an error, so
// a typo cannot silently run the experiment with the default.
func parseSet(set []string) ([]override, error) {
	var out []override
	for _, s := range set {
		kv, value, ok := strings.Cut(s, "=")
		if !ok || kv == "" || value == "" {
			return nil, fmt.Errorf("-set %q: want KEY=VALUE or role:KEY=VALUE", s)
		}
		o := override{key: kv, value: value}
		if role, key, ok := strings.Cut(kv, ":"); ok {
			o.role, o.key = role, key
			if tunables[role] == nil {
				return nil, fmt.Errorf("-set %q: role must be storage, coordinator or router", s)
			}
		}
		known := false
		for role, ts := range tunables {
			for _, t := range ts {
				known = known || (t[0] == o.key && (o.role == "" || o.role == role))
			}
		}
		if !known {
			return nil, fmt.Errorf("-set %q: no such setting (known: %s)", s, strings.Join(knownSettings(), ", "))
		}
		out = append(out, o)
	}
	return out, nil
}

func knownSettings() []string {
	seen := map[string]bool{}
	for _, ts := range tunables {
		for _, t := range ts {
			seen[t[0]] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func addrs(hosts []inventory.Host) string {
	out := make([]string, len(hosts))
	for i, h := range hosts {
		out[i] = h.Addr()
	}
	return strings.Join(out, ",")
}

// EnvFile returns the env file of one host of the cluster inv: who it is,
// its peers, and every setting of its role with set applied.
func EnvFile(inv *inventory.Inventory, h inventory.Host, set []string) (string, error) {
	overrides, err := parseSet(set)
	if err != nil {
		return "", err
	}
	var lines []string
	add := func(k, v string) { lines = append(lines, k+"="+v) }
	switch h.Role {
	case inventory.Storage:
		// NODE_ID must be the host part of this node's entry in PEERS.
		add("NODE_ID", h.PrivateIP)
		add("SHARD_ID", fmt.Sprint(h.Shard))
		add("PEERS", addrs(inv.Group(h.Shard)))
		add("COORDINATOR_PEERS", addrs(inv.Group(0)))
		add("SEED_DIR", inSeedDir)
		add("DATA_DIR", inData)
	case inventory.Coordinator:
		add("NODE_ID", h.PrivateIP)
		add("PEERS", addrs(inv.Group(0)))
		add("SHARDMAP", inShardMap)
		add("DATA_DIR", inData)
	case inventory.Router:
		add("NODE_ID", h.Name)
		add("SHARDMAP", inShardMap)
	default:
		return "", fmt.Errorf("host %s (%s) runs no SeatSafe process", h.Name, h.Role)
	}
	for _, t := range tunables[h.Role] {
		v := t[1]
		for _, o := range overrides {
			if o.key == t[0] && (o.role == "" || o.role == h.Role) {
				v = o.value
			}
		}
		add(t[0], v)
	}
	return strings.Join(lines, "\n") + "\n", nil
}

// Dataset returns the generator settings for a cluster: the node addresses
// of the inventory, and a flash event of about seats seats (sections of 20
// rows of 50 seats) or, for 0, the default dataset.
func Dataset(inv *inventory.Inventory, seats int) datagen.Config {
	c := datagen.Defaults()
	c.Shards = len(inv.Shards())
	c.CoordAddrs = strings.Split(addrs(inv.Group(0)), ",")
	c.ShardAddrs = map[uint32][]string{}
	for _, s := range inv.Shards() {
		c.ShardAddrs[uint32(s)] = strings.Split(addrs(inv.Group(s)), ",")
	}
	if seats > 0 {
		c.Events = 0
		c.FlashSections, c.FlashRows, c.FlashSeatsPerRow = (seats+999)/1000, 20, 50
	}
	return c
}

// Gen writes everything a cluster needs into dir: shardmap.json, the seed
// files, one env file per node host (hosts/<name>/env), prometheus.yml and
// layout.json. It returns the hosts of the layout.
func Gen(full *inventory.Inventory, dir string, l Layout) (*inventory.Inventory, error) {
	inv, err := full.Layout(l.Shards, l.Replicas)
	if err != nil {
		return nil, err
	}
	out, err := datagen.Generate(Dataset(inv, l.Seats))
	if err != nil {
		return nil, err
	}
	// Seed files of an earlier, larger layout must not be left behind.
	if err := os.RemoveAll(filepath.Join(dir, "seed")); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(filepath.Join(dir, "hosts")); err != nil {
		return nil, err
	}
	if err := datagen.Write(dir, out); err != nil {
		return nil, err
	}
	for _, h := range inv.Nodes() {
		env, err := EnvFile(inv, h, l.Set)
		if err != nil {
			return nil, err
		}
		p := filepath.Join(dir, "hosts", h.Name, "env")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(p, []byte(env), 0o644); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "prometheus.yml"), []byte(Prometheus(inv)), 0o644); err != nil {
		return nil, err
	}
	b, _ := json.MarshalIndent(l, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "layout.json"), append(b, '\n'), 0o644); err != nil {
		return nil, err
	}
	return inv, nil
}

// ReadLayout returns the layout gen saved in dir; the zero Layout (the whole
// inventory) if gen has not run there.
func ReadLayout(dir string) (Layout, error) {
	var l Layout
	b, err := os.ReadFile(filepath.Join(dir, "layout.json"))
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return l, err
	}
	return l, json.Unmarshal(b, &l)
}

// Prometheus returns a prometheus.yml that scrapes every node of the
// cluster on its private address, one job per role as in the Compose setup,
// with the host's name, zone and shard as labels.
func Prometheus(inv *inventory.Inventory) string {
	var b strings.Builder
	b.WriteString("# Written by clusterctl from the inventory. Do not edit: run clusterctl prom.\nglobal:\n  scrape_interval: 5s\n\nscrape_configs:\n")
	for _, role := range []string{inventory.Router, inventory.Storage, inventory.Coordinator} {
		hosts := inv.ByRole(role)
		if len(hosts) == 0 {
			continue
		}
		fmt.Fprintf(&b, "  - job_name: %s\n    static_configs:\n", role)
		for _, h := range hosts {
			fmt.Fprintf(&b, "      - targets: [\"%s:%d\"]\n        labels: {host: %s", h.PrivateIP, inventory.HTTPPort, h.Name)
			if h.AZ != "" {
				fmt.Fprintf(&b, ", az: %s", h.AZ)
			}
			if h.Role == inventory.Storage {
				fmt.Fprintf(&b, ", shard: \"%d\"", h.Shard)
			}
			b.WriteString("}\n")
		}
	}
	return b.String()
}
