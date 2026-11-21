package inventory

import (
	"path/filepath"
	"strings"
	"testing"
)

const head = "ssh_user: ec2-user\nssh_key: ~/.ssh/k.pem\nimage: seatsafe:dev\nhosts:\n"

// three is a 1-shard cluster with a node in each of three zones.
const three = head + `  - {name: s1, role: storage, shard: 1, node: 1, az: a, private_ip: 10.0.0.1}
  - {name: s2, role: storage, shard: 1, node: 2, az: b, private_ip: 10.0.0.2}
  - {name: s3, role: storage, shard: 1, node: 3, az: c, private_ip: 10.0.0.3}
  - {name: c1, role: coordinator, node: 1, az: a, private_ip: 10.0.0.4}
  - {name: r1, role: router, az: a, private_ip: 10.0.0.5}
`

// The example file in deploy/aws loads without warnings and has the agreed
// layout: 3 shards of 3 and 3 coordinators, one node of each group per zone.
func TestExample(t *testing.T) {
	inv, err := Load(filepath.Join("..", "..", "deploy", "aws", "inventory.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.Warnings) != 0 {
		t.Fatalf("warnings: %v", inv.Warnings)
	}
	if inv.SSHUser != "ec2-user" || inv.Executor != ExecSSH || inv.Network != NetworkHost || inv.ConfigDir != "/opt/seatsafe" || inv.DataDir != "/data" {
		t.Fatalf("%+v", inv)
	}
	if got := inv.Shards(); len(got) != 3 || len(inv.Hosts) != 17 {
		t.Fatalf("shards %v, %d hosts", got, len(inv.Hosts))
	}
	if got := strings.Join(inv.Zones(), " "); got != "us-west-2a us-west-2b us-west-2c" {
		t.Fatalf("zones %q", got)
	}
	for g := 0; g <= 3; g++ {
		nodes := inv.Group(g)
		if len(nodes) != 3 {
			t.Fatalf("group %d has %d nodes", g, len(nodes))
		}
		zones := map[string]bool{}
		for i, h := range nodes {
			zones[h.AZ] = true
			if h.Node != i+1 {
				t.Fatalf("group %d: node order %+v", g, nodes)
			}
		}
		if len(zones) != 3 {
			t.Fatalf("group %d is not spread over three zones: %+v", g, nodes)
		}
	}
	h, _ := inv.Host("shard2-node3")
	if h.Addr() != "10.0.3.12:7000" || h.Service() != "shard2-node3" || h.PublicIP != "203.0.113.23" {
		t.Fatalf("%+v", h)
	}
	if c := inv.Group(0)[1]; c.Service() != "coord-2" {
		t.Fatalf("%+v", c)
	}
	if len(inv.ByRole(Loadgen)) != 2 || len(inv.ByRole(Router)) != 2 || len(inv.ByRole(Monitor)) != 1 || len(inv.Nodes()) != 14 {
		t.Fatal("roles")
	}
}

// Block form, quotes and comments parse to the same thing as flow form.
func TestBlockForm(t *testing.T) {
	inv, err := Parse([]byte(`# a comment
ssh_user: "ec2-user"   # trailing comment
ssh_key: '~/.ssh/k.pem'
image: repo/seatsafe:1 # tag
hosts:
  - name: s1
    role: storage
    shard: 1
    node: 1
    az: a
    public_ip: 1.2.3.4
    private_ip: 10.0.0.1

  - {name: r1, role: router, private_ip: 10.0.0.5}
  - name: c1
    role: coordinator
    node: 1
    az: a
    private_ip: 10.0.0.4
`))
	if err != nil {
		t.Fatal(err)
	}
	if inv.SSHUser != "ec2-user" || inv.SSHKey != "~/.ssh/k.pem" || inv.Image != "repo/seatsafe:1" || len(inv.Hosts) != 3 {
		t.Fatalf("%+v", inv)
	}
	if h := inv.Hosts[0]; h != (Host{"s1", Storage, 1, 1, "a", "1.2.3.4", "10.0.0.1"}) {
		t.Fatalf("%+v", h)
	}
	if h := inv.Hosts[2]; h.Name != "c1" || h.Node != 1 || h.Role != Coordinator {
		t.Fatalf("%+v", h)
	}
}

// Every mistake is an error that says what is wrong.
func TestRejects(t *testing.T) {
	sub := func(old, new string) string {
		if !strings.Contains(three, old) {
			t.Fatalf("test bug: %q not in the base inventory", old)
		}
		return strings.Replace(three, old, new, 1)
	}
	for name, c := range map[string]struct{ text, want string }{
		"unknown top-level key":       {"ssh_usr: x\n" + three, `line 1: unknown key "ssh_usr"`},
		"unknown host key":            {sub("az: a, private_ip: 10.0.0.1", "zone: a, private_ip: 10.0.0.1"), `line 5: unknown host key "zone"`},
		"key twice":                   {"image: x\n" + three, `key "image" is given twice`},
		"host key twice":              {sub("name: s1,", "name: s1, name: s9,"), `host key "name" is given twice`},
		"tab":                         {strings.Replace(three, "  - {name: s1", "\t- {name: s1", 1), "line 5: tabs are not allowed"},
		"no image":                    {strings.Replace(three, "image: seatsafe:dev\n", "", 1), "image is missing"},
		"no ssh key":                  {strings.Replace(three, "ssh_key: ~/.ssh/k.pem\n", "", 1), "ssh_user and ssh_key are required"},
		"no hosts":                    {head, "hosts is empty"},
		"bad role":                    {sub("role: router", "role: proxy"), `role must be storage, coordinator, router, loadgen or monitor, got "proxy"`},
		"bad name":                    {sub("name: s1,", "name: S_1,"), `host name "S_1"`},
		"duplicate name":              {sub("name: s2,", "name: s1,"), `host "s1" is listed twice`},
		"duplicate private_ip":        {sub("10.0.0.2", "10.0.0.1"), `hosts "s1" and "s2" have the same private_ip 10.0.0.1`},
		"missing private_ip":          {sub(", private_ip: 10.0.0.5", ""), `host "r1": private_ip "" is missing`},
		"storage without shard":       {sub("shard: 1, node: 1,", "node: 1,"), `host "s1": a storage host needs shard and node`},
		"shard not a number":          {sub("shard: 1, node: 1,", "shard: one, node: 1,"), `shard must be a whole number, got "one"`},
		"coordinator with a shard":    {sub("role: coordinator, node: 1", "role: coordinator, shard: 1, node: 1"), `host "c1": a coordinator host needs node (1 or more) and no shard`},
		"router with a node":          {sub("role: router,", "role: router, node: 1,"), `host "r1": a router host has no shard or node`},
		"same slot twice":             {sub("shard: 1, node: 2", "shard: 1, node: 1"), `hosts "s1" and "s2" are both node 1 of shard 1`},
		"gap in the nodes":            {sub("shard: 1, node: 3", "shard: 1, node: 4"), "shard 1: nodes must be numbered 1..3 without gaps; node 3 is missing"},
		"gap in the shards":           {sub("shard: 1, node: 3", "shard: 3, node: 1"), "shards must be numbered 1..2 without gaps; shard 2 is missing"},
		"bad executor":                {"executor: telnet\n" + three, "executor must be ssh or local"},
		"config under the data dir":   {"config_dir: /data/seatsafe\n" + three, "config_dir /data/seatsafe is under data_dir /data, which is wiped when an instance stops"},
		"flow host over two lines":    {sub("private_ip: 10.0.0.5}", "private_ip: 10.0.0.5"), "a {...} host must end on the same line"},
		"pair without a space":        {sub("name: r1,", "name:r1,"), `want "key: value", got "name:r1"`},
		"hosts with an inline value":  {strings.Replace(three, "hosts:\n", "hosts: []\n", 1), "hosts must be a list"},
		"indented line outside hosts": {"ssh_user: a\n  image: b\n", "line 2: unexpected indented line"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(c.text))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want it to contain %q", err, c.want)
			}
		})
	}
}

// Legal but suspicious layouts load with a warning.
func TestWarnings(t *testing.T) {
	for name, c := range map[string]struct{ text, want string }{
		"two nodes of a group in one zone": {strings.Replace(three, "node: 2, az: b", "node: 2, az: a", 1),
			"shard 1 has 2 nodes in a (s1, s2): losing that zone can cost it its quorum"},
		"even group": {three + "  - {name: c2, role: coordinator, node: 2, az: b, private_ip: 10.0.0.6}\n",
			"the coordinator group has 2 nodes: an even number"},
		"no zone": {strings.Replace(three, "node: 2, az: b,", "node: 2,", 1), "host s2 has no az"},
	} {
		t.Run(name, func(t *testing.T) {
			inv, err := Parse([]byte(c.text))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(inv.Warnings, "\n"), c.want) {
				t.Fatalf("warnings %q, want one containing %q", inv.Warnings, c.want)
			}
		})
	}
	if inv, err := Parse([]byte(three)); err != nil || len(inv.Warnings) != 0 {
		t.Fatalf("clean inventory: %v, warnings %v", err, inv.Warnings)
	}
}

// Layout picks the hosts of a smaller cluster.
func TestLayout(t *testing.T) {
	inv, err := Load(filepath.Join("..", "..", "deploy", "aws", "inventory.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	names := func(hs []Host) string {
		var out []string
		for _, h := range hs {
			out = append(out, h.Name)
		}
		return strings.Join(out, " ")
	}
	for _, c := range []struct {
		shards, replicas int
		storage, coord   string
	}{
		{0, 0, "shard1-node1 shard1-node2 shard1-node3 shard2-node1 shard2-node2 shard2-node3 shard3-node1 shard3-node2 shard3-node3", "coord-1 coord-2 coord-3"},
		{1, 0, "shard1-node1 shard1-node2 shard1-node3", "coord-1 coord-2 coord-3"},
		{2, 3, "shard1-node1 shard1-node2 shard1-node3 shard2-node1 shard2-node2 shard2-node3", "coord-1 coord-2 coord-3"},
		{3, 1, "shard1-node1 shard2-node1 shard3-node1", "coord-1"},
		{1, 1, "shard1-node1", "coord-1"},
	} {
		l, err := inv.Layout(c.shards, c.replicas)
		if err != nil {
			t.Fatalf("layout %d/%d: %v", c.shards, c.replicas, err)
		}
		if got := names(l.ByRole(Storage)); got != c.storage {
			t.Fatalf("layout %d/%d: storage %q", c.shards, c.replicas, got)
		}
		if got := names(l.ByRole(Coordinator)); got != c.coord {
			t.Fatalf("layout %d/%d: coordinator %q", c.shards, c.replicas, got)
		}
		if len(l.ByRole(Router)) != 2 || len(l.ByRole(Loadgen)) != 2 || len(l.ByRole(Monitor)) != 1 {
			t.Fatalf("layout %d/%d dropped a router, loadgen or monitor host", c.shards, c.replicas)
		}
	}
	if len(inv.Hosts) != 17 {
		t.Fatal("Layout changed the inventory it was called on")
	}
	for _, bad := range [][2]int{{4, 0}, {-1, 0}, {1, 4}} {
		if _, err := inv.Layout(bad[0], bad[1]); err == nil {
			t.Fatalf("layout %v accepted", bad)
		}
	}
}

// A local inventory: every host is this machine, on a Docker network, so
// the addresses are names and may not be IP addresses.
func TestLocal(t *testing.T) {
	inv, err := Parse([]byte(`executor: local
network: seatsafe-local
config_dir: results/local/opt
data_dir: volume
image: seatsafe:dev
hosts:
  - {name: s1, role: storage, shard: 1, node: 1, az: z1, private_ip: s1}
  - {name: c1, role: coordinator, node: 1, az: z1, private_ip: c1}
  - {name: r1, role: router, private_ip: r1}
  - {name: l1, role: loadgen, private_ip: l1}
`))
	if err != nil {
		t.Fatal(err)
	}
	if inv.Executor != ExecLocal || inv.Network != "seatsafe-local" || inv.DataDir != "volume" || inv.Hosts[0].Addr() != "s1:7000" {
		t.Fatalf("%+v", inv)
	}
}
