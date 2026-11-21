// Package inventory reads deploy/aws/inventory.yaml: the machines a SeatSafe
// cluster runs on, as provisioned outside this repository.
//
// The file is a small, strict subset of YAML: top-level "key: value" lines
// and one list, hosts, whose items are flat maps written either in flow form
// ("- {name: a, role: storage}") or in block form ("- name: a" followed by
// indented "key: value" lines). Unknown keys are errors, so a typo is not
// silently ignored.
package inventory

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Roles of a host.
const (
	Storage     = "storage"
	Coordinator = "coordinator"
	Router      = "router"
	Loadgen     = "loadgen"
	Monitor     = "monitor"
)

// Executors.
const (
	ExecSSH   = "ssh"   // run commands on the hosts over ssh (the default)
	ExecLocal = "local" // every host is this machine: run commands directly
)

// Defaults for the optional settings.
const (
	DefaultConfigDir = "/opt/seatsafe" // configs, seeds, env files; never under the data directory
	DefaultDataDir   = "/data"         // Raft logs; local NVMe on AWS, wiped when an instance stops
	NetworkHost      = "host"
	GRPCPort         = 7000
	HTTPPort         = 8080
)

// Host is one machine.
type Host struct {
	Name      string
	Role      string
	Shard     int // storage: 1..; otherwise 0
	Node      int // storage and coordinator: position in the Raft group, 1..; otherwise 0
	AZ        string
	PublicIP  string
	PrivateIP string
}

// Inventory is a parsed and validated inventory file.
type Inventory struct {
	SSHUser string
	SSHKey  string
	Image   string
	Hosts   []Host

	// Optional settings, for running without AWS. Executor "local" runs
	// every command on this machine; Network is the Docker network the
	// containers join ("host" by default; on a named network every
	// container gets its host's private_ip as its alias); with DataDir
	// "volume" every node keeps /data in a Docker volume of its own.
	Executor  string
	Network   string
	ConfigDir string
	DataDir   string

	// Warnings are things that are legal but probably not meant, such as
	// two nodes of one Raft group in the same availability zone.
	Warnings []string
}

var topKeys = map[string]bool{"ssh_user": true, "ssh_key": true, "image": true, "hosts": true,
	"executor": true, "network": true, "config_dir": true, "data_dir": true}

var hostKeys = map[string]bool{"name": true, "role": true, "shard": true, "node": true, "az": true, "public_ip": true, "private_ip": true}

// Load reads and validates an inventory file.
func Load(path string) (*Inventory, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	inv, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return inv, nil
}

// Parse parses and validates the text of an inventory file.
func Parse(data []byte) (*Inventory, error) {
	inv := &Inventory{}
	var cur map[string]string // the host being read in block form
	curLine := 0
	inHosts := false
	flush := func() error {
		if cur == nil {
			return nil
		}
		h, err := hostOf(cur)
		if err != nil {
			return fmt.Errorf("line %d: %w", curLine, err)
		}
		inv.Hosts = append(inv.Hosts, h)
		cur = nil
		return nil
	}
	seenTop := map[string]bool{}
	for n, raw := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		n++
		if strings.Contains(raw, "\t") {
			return nil, fmt.Errorf("line %d: tabs are not allowed, indent with spaces", n)
		}
		line := strings.TrimRight(stripComment(raw), " ")
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		text := strings.TrimSpace(line)
		switch {
		case indent == 0:
			if err := flush(); err != nil {
				return nil, err
			}
			k, v, ok := cutPair(text)
			if !ok {
				return nil, fmt.Errorf("line %d: want \"key: value\", got %q", n, text)
			}
			if !topKeys[k] {
				return nil, fmt.Errorf("line %d: unknown key %q", n, k)
			}
			if seenTop[k] {
				return nil, fmt.Errorf("line %d: key %q is given twice", n, k)
			}
			seenTop[k] = true
			inHosts = k == "hosts"
			if inHosts {
				if v != "" {
					return nil, fmt.Errorf("line %d: hosts must be a list, one \"- \" item per line below it", n)
				}
				continue
			}
			if v == "" {
				return nil, fmt.Errorf("line %d: %s has no value", n, k)
			}
			switch k {
			case "ssh_user":
				inv.SSHUser = v
			case "ssh_key":
				inv.SSHKey = v
			case "image":
				inv.Image = v
			case "executor":
				inv.Executor = v
			case "network":
				inv.Network = v
			case "config_dir":
				inv.ConfigDir = v
			case "data_dir":
				inv.DataDir = v
			}
		case !inHosts:
			return nil, fmt.Errorf("line %d: unexpected indented line", n)
		case strings.HasPrefix(text, "- ") || text == "-":
			if err := flush(); err != nil {
				return nil, err
			}
			item := strings.TrimSpace(strings.TrimPrefix(text, "-"))
			cur, curLine = map[string]string{}, n
			if strings.HasPrefix(item, "{") {
				if !strings.HasSuffix(item, "}") {
					return nil, fmt.Errorf("line %d: a {...} host must end on the same line", n)
				}
				for _, part := range strings.Split(item[1:len(item)-1], ",") {
					if strings.TrimSpace(part) == "" {
						continue
					}
					if err := addPair(cur, part); err != nil {
						return nil, fmt.Errorf("line %d: %w", n, err)
					}
				}
				if err := flush(); err != nil {
					return nil, err
				}
			} else if item != "" {
				if err := addPair(cur, item); err != nil {
					return nil, fmt.Errorf("line %d: %w", n, err)
				}
			}
		default:
			if cur == nil {
				return nil, fmt.Errorf("line %d: a host starts with \"- \"", n)
			}
			if err := addPair(cur, text); err != nil {
				return nil, fmt.Errorf("line %d: %w", n, err)
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := inv.validate(); err != nil {
		return nil, err
	}
	return inv, nil
}

// stripComment drops a # comment that is not inside quotes.
func stripComment(s string) string {
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '#' && (i == 0 || s[i-1] == ' '):
			return s[:i]
		}
	}
	return s
}

// cutPair splits "key: value". The value may be empty or quoted.
func cutPair(s string) (key, value string, ok bool) {
	i := strings.Index(s, ":")
	if i <= 0 {
		return "", "", false
	}
	key, value = strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
	if value != "" && s[i+1] != ' ' {
		return "", "", false // "a:b" is not a pair
	}
	if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
		value = value[1 : len(value)-1]
	}
	return key, value, true
}

func addPair(m map[string]string, s string) error {
	k, v, ok := cutPair(strings.TrimSpace(s))
	if !ok {
		return fmt.Errorf("want \"key: value\", got %q", strings.TrimSpace(s))
	}
	if !hostKeys[k] {
		return fmt.Errorf("unknown host key %q", k)
	}
	if _, dup := m[k]; dup {
		return fmt.Errorf("host key %q is given twice", k)
	}
	m[k] = v
	return nil
}

func hostOf(m map[string]string) (Host, error) {
	h := Host{Name: m["name"], Role: m["role"], AZ: m["az"], PublicIP: m["public_ip"], PrivateIP: m["private_ip"]}
	for key, dst := range map[string]*int{"shard": &h.Shard, "node": &h.Node} {
		if v, ok := m[key]; ok && v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return h, fmt.Errorf("host %q: %s must be a whole number, got %q", h.Name, key, v)
			}
			*dst = n
		}
	}
	return h, nil
}

var (
	nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	addrRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]*$`)
)

func (inv *Inventory) validate() error {
	if inv.Executor == "" {
		inv.Executor = ExecSSH
	}
	if inv.Executor != ExecSSH && inv.Executor != ExecLocal {
		return fmt.Errorf("executor must be %s or %s, got %q", ExecSSH, ExecLocal, inv.Executor)
	}
	if inv.Network == "" {
		inv.Network = NetworkHost
	}
	if inv.ConfigDir == "" {
		inv.ConfigDir = DefaultConfigDir
	}
	if inv.DataDir == "" {
		inv.DataDir = DefaultDataDir
	}
	if inv.Image == "" {
		return fmt.Errorf("image is missing")
	}
	if inv.Executor == ExecSSH && (inv.SSHUser == "" || inv.SSHKey == "") {
		return fmt.Errorf("ssh_user and ssh_key are required")
	}
	if inv.DataDir != "volume" && strings.HasPrefix(inv.ConfigDir+"/", strings.TrimRight(inv.DataDir, "/")+"/") {
		return fmt.Errorf("config_dir %s is under data_dir %s, which is wiped when an instance stops", inv.ConfigDir, inv.DataDir)
	}
	if len(inv.Hosts) == 0 {
		return fmt.Errorf("hosts is empty")
	}
	names, addrs := map[string]bool{}, map[string]string{}
	type slot struct{ shard, node int }
	slots := map[slot]string{}
	for _, h := range inv.Hosts {
		if !nameRE.MatchString(h.Name) {
			return fmt.Errorf("host name %q: use lower-case letters, digits and dashes", h.Name)
		}
		if names[h.Name] {
			return fmt.Errorf("host %q is listed twice", h.Name)
		}
		names[h.Name] = true
		if !addrRE.MatchString(h.PrivateIP) {
			return fmt.Errorf("host %q: private_ip %q is missing or not an address", h.Name, h.PrivateIP)
		}
		if h.PublicIP != "" && !addrRE.MatchString(h.PublicIP) {
			return fmt.Errorf("host %q: public_ip %q is not an address", h.Name, h.PublicIP)
		}
		if other, dup := addrs[h.PrivateIP]; dup && inv.Network == NetworkHost {
			return fmt.Errorf("hosts %q and %q have the same private_ip %s", other, h.Name, h.PrivateIP)
		}
		addrs[h.PrivateIP] = h.Name
		switch h.Role {
		case Storage:
			if h.Shard < 1 || h.Node < 1 {
				return fmt.Errorf("host %q: a storage host needs shard and node, both 1 or more", h.Name)
			}
		case Coordinator:
			if h.Shard != 0 || h.Node < 1 {
				return fmt.Errorf("host %q: a coordinator host needs node (1 or more) and no shard", h.Name)
			}
		case Router, Loadgen, Monitor:
			if h.Shard != 0 || h.Node != 0 {
				return fmt.Errorf("host %q: a %s host has no shard or node", h.Name, h.Role)
			}
			continue
		default:
			return fmt.Errorf("host %q: role must be storage, coordinator, router, loadgen or monitor, got %q", h.Name, h.Role)
		}
		k := slot{h.Shard, h.Node}
		if other, dup := slots[k]; dup {
			return fmt.Errorf("hosts %q and %q are both node %d of %s", other, h.Name, h.Node, groupName(h.Shard))
		}
		slots[k] = h.Name
	}
	// Shards are numbered 1..S and every group's nodes 1..N, without gaps.
	shards := inv.Shards()
	for i, s := range shards {
		if s != i+1 {
			return fmt.Errorf("shards must be numbered 1..%d without gaps; shard %d is missing", len(shards), i+1)
		}
	}
	for _, g := range append([]int{0}, shards...) {
		nodes := inv.Group(g)
		for i, h := range nodes {
			if h.Node != i+1 {
				return fmt.Errorf("%s: nodes must be numbered 1..%d without gaps; node %d is missing", groupName(g), len(nodes), i+1)
			}
		}
		if len(nodes) == 0 {
			continue
		}
		if len(nodes)%2 == 0 {
			inv.warn("%s has %d nodes: an even number tolerates no more failures than one fewer", groupName(g), len(nodes))
		}
		zones := map[string][]string{}
		for _, h := range nodes {
			if h.AZ == "" {
				inv.warn("host %s has no az", h.Name)
				continue
			}
			zones[h.AZ] = append(zones[h.AZ], h.Name)
		}
		for _, az := range sortedKeys(zones) {
			if in := zones[az]; len(in) > 1 && len(nodes) > 1 {
				inv.warn("%s has %d nodes in %s (%s): losing that zone can cost it its quorum", groupName(g), len(in), az, strings.Join(in, ", "))
			}
		}
	}
	return nil
}

func (inv *Inventory) warn(format string, args ...any) {
	inv.Warnings = append(inv.Warnings, fmt.Sprintf(format, args...))
}

func groupName(g int) string {
	if g == 0 {
		return "the coordinator group"
	}
	return fmt.Sprintf("shard %d", g)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Shards returns the shard ids that have storage hosts, in order.
func (inv *Inventory) Shards() []int {
	seen := map[int]bool{}
	var out []int
	for _, h := range inv.Hosts {
		if h.Role == Storage && !seen[h.Shard] {
			seen[h.Shard] = true
			out = append(out, h.Shard)
		}
	}
	sort.Ints(out)
	return out
}

// Group returns the hosts of one Raft group in node order: a shard's storage
// hosts, or the coordinator hosts for group 0.
func (inv *Inventory) Group(g int) []Host {
	var out []Host
	for _, h := range inv.Hosts {
		if (g == 0 && h.Role == Coordinator) || (g > 0 && h.Role == Storage && h.Shard == g) {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

// ByRole returns the hosts of a role in file order.
func (inv *Inventory) ByRole(roles ...string) []Host {
	var out []Host
	for _, h := range inv.Hosts {
		for _, r := range roles {
			if h.Role == r {
				out = append(out, h)
			}
		}
	}
	return out
}

// Nodes returns the hosts that run a SeatSafe process: storage, coordinator
// and router hosts.
func (inv *Inventory) Nodes() []Host { return inv.ByRole(Storage, Coordinator, Router) }

// Host returns the host with that name.
func (inv *Inventory) Host(name string) (Host, bool) {
	for _, h := range inv.Hosts {
		if h.Name == name {
			return h, true
		}
	}
	return Host{}, false
}

// Zones returns the availability zones of the hosts that run a SeatSafe
// process, in order.
func (inv *Inventory) Zones() []string {
	seen := map[string]bool{}
	for _, h := range inv.Nodes() {
		if h.AZ != "" {
			seen[h.AZ] = true
		}
	}
	return sortedKeys(seen)
}

// Layout returns the part of the inventory that makes a smaller cluster: the
// first shards shards and the first replicas nodes of every group (0 means
// all of them), with every router, load generator and monitor host. It is
// how 1, 2 and 3 shard and single-node clusters are built from one fleet.
func (inv *Inventory) Layout(shards, replicas int) (*Inventory, error) {
	all := inv.Shards()
	if shards == 0 {
		shards = len(all)
	}
	if shards < 1 || shards > len(all) {
		return nil, fmt.Errorf("the inventory has %d shards, cannot build a layout with %d", len(all), shards)
	}
	out := *inv
	out.Hosts, out.Warnings = nil, nil
	for _, h := range inv.Hosts {
		switch h.Role {
		case Storage:
			if h.Shard > shards || (replicas > 0 && h.Node > replicas) {
				continue
			}
		case Coordinator:
			if replicas > 0 && h.Node > replicas {
				continue
			}
		}
		out.Hosts = append(out.Hosts, h)
	}
	for _, g := range append([]int{0}, out.Shards()...) {
		if n := len(out.Group(g)); n == 0 || (replicas > 0 && n != replicas) {
			return nil, fmt.Errorf("%s has %d nodes in the inventory, cannot build a layout with %d replicas", groupName(g), len(inv.Group(g)), replicas)
		}
	}
	if len(out.ByRole(Router)) == 0 {
		return nil, fmt.Errorf("the inventory has no router host")
	}
	return &out, nil
}

// Addr is a host's gRPC address as the other nodes dial it.
func (h Host) Addr() string { return fmt.Sprintf("%s:%d", h.PrivateIP, GRPCPort) }

// Service is the name the chaos tool uses for a node of a Raft group
// ("shard2-node1", "coord-3"); other hosts go by their own name.
func (h Host) Service() string {
	switch h.Role {
	case Storage:
		return fmt.Sprintf("shard%d-node%d", h.Shard, h.Node)
	case Coordinator:
		return fmt.Sprintf("coord-%d", h.Node)
	}
	return h.Name
}
