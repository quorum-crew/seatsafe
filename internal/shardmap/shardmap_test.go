package shardmap

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadExample(t *testing.T) {
	m, err := Load(filepath.Join("..", "..", "deploy", "shardmap.example.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, ok := m.Lookup("evt-042", "sec-102"); !ok || got != 2 {
		t.Fatalf("Lookup(evt-042, sec-102) = %d, %v; want 2, true", got, ok)
	}
	if _, ok := m.Lookup("evt-042", "sec-999"); ok {
		t.Fatal("unknown section should not be found")
	}
	if got := m.EventShards("evt-042"); !reflect.DeepEqual(got, []uint32{1, 2, 3}) {
		t.Fatalf("EventShards(evt-042) = %v", got)
	}
	if got := m.EventShards("evt-043"); !reflect.DeepEqual(got, []uint32{2, 3}) {
		t.Fatalf("EventShards(evt-043) = %v", got)
	}
	if got := m.ShardNodes(2); len(got) != 3 || got[0] != "shard2-node1:8080" {
		t.Fatalf("ShardNodes(2) = %v", got)
	}
	if got := m.CoordinatorNodes(); len(got) != 3 {
		t.Fatalf("CoordinatorNodes = %v", got)
	}
	// Returned slices are copies: mutating them must not change the map.
	m.ShardNodes(2)[0] = "mutated"
	if m.ShardNodes(2)[0] != "shard2-node1:8080" {
		t.Fatal("ShardNodes leaked internal slice")
	}
}

func valid() File {
	return File{
		Version:     1,
		Coordinator: Group{Nodes: []string{"c1:7000"}},
		Shards:      []Shard{{ShardID: 1, Nodes: []string{"s1:7000"}}, {ShardID: 2, Nodes: []string{"s2:7000"}}},
		Sections:    []Section{{EventID: "e", SectionID: "a", ShardID: 1}},
	}
}

func TestValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*File)
		want   string
	}{
		{"bad version", func(f *File) { f.Version = 2 }, "unsupported version"},
		{"no coordinator", func(f *File) { f.Coordinator.Nodes = nil }, "coordinator has no nodes"},
		{"no shards", func(f *File) { f.Shards = nil }, "no shards"},
		{"shard 0", func(f *File) { f.Shards[0].ShardID = 0 }, "reserved"},
		{"dup shard", func(f *File) { f.Shards[1].ShardID = 1 }, "duplicate shard_id"},
		{"dup node", func(f *File) { f.Shards[0].Nodes = []string{"x", "x"} }, "twice"},
		{"unknown shard", func(f *File) { f.Sections[0].ShardID = 9 }, "unknown shard 9"},
		{"dup section", func(f *File) { f.Sections = append(f.Sections, f.Sections[0]) }, "listed twice"},
		{"empty key", func(f *File) { f.Sections[0].SectionID = "" }, "empty"},
	}
	if _, err := New(valid()); err != nil {
		t.Fatalf("valid map rejected: %v", err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := valid()
			c.mutate(&f)
			_, err := New(f)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v; want containing %q", err, c.want)
			}
		})
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	_, err := Parse([]byte(`{"version":1,"coordinator":{"nodes":["c"]},"shards":[{"shard_id":1,"nodes":["s"]}],"sections":[],"shard":1}`))
	if err == nil {
		t.Fatal("typo field should be rejected")
	}
}

func TestRoundRobin(t *testing.T) {
	got := RoundRobin("evt-1", []string{"a", "b", "c", "d"}, []uint32{1, 2, 3})
	want := []uint32{1, 2, 3, 1}
	for i, s := range got {
		if s.ShardID != want[i] || s.EventID != "evt-1" {
			t.Fatalf("section %d = %+v; want shard %d", i, s, want[i])
		}
	}
}
