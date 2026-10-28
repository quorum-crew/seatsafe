// Package shardmap loads deploy/shardmap.json, the static table that tells
// the router which shard owns each (event_id, section_id) and where every
// Raft group's nodes live.
//
// Format (version 1):
//
//	{
//	  "version": 1,
//	  "coordinator": { "nodes": ["coord-1:7000", ...] },
//	  "shards": [ { "shard_id": 1, "nodes": ["shard1-node1:7000", ...] }, ... ],
//	  "sections": [ { "event_id": "evt-042", "section_id": "sec-101", "shard_id": 1 }, ... ]
//	}
//
// The data generator writes it, assigning each event's sections round-robin
// across shards so a popular event spreads over all of them. The router loads
// it once at startup. Node lists are only seed addresses: the router still
// finds the current leader through NodeService.Status and leader hints.
package shardmap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

// CurrentVersion is the format version this package understands.
const CurrentVersion = 1

// File is the on-disk JSON shape.
type File struct {
	Version     int       `json:"version"`
	Coordinator Group     `json:"coordinator"`
	Shards      []Shard   `json:"shards"`
	Sections    []Section `json:"sections"`
}

// Group is a Raft group's node addresses (host:port of the gRPC endpoint).
type Group struct {
	Nodes []string `json:"nodes"`
}

// Shard is one shard Raft group.
type Shard struct {
	ShardID uint32   `json:"shard_id"`
	Nodes   []string `json:"nodes"`
}

// Section assigns one (event, section) to a shard.
type Section struct {
	EventID   string `json:"event_id"`
	SectionID string `json:"section_id"`
	ShardID   uint32 `json:"shard_id"`
}

// Key is the shard key.
type Key struct {
	EventID   string
	SectionID string
}

// Map is the validated, indexed shard map. Safe for concurrent reads.
type Map struct {
	version     int
	coordinator []string
	shardNodes  map[uint32][]string
	shardIDs    []uint32 // sorted
	owner       map[Key]uint32
	byEvent     map[string][]Section // sections of each event, sorted by section_id
}

// Load reads and validates a shard map file.
func Load(path string) (*Map, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("shardmap: read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse validates and indexes shard map JSON. Unknown fields are rejected so
// typos fail loudly instead of silently routing to the wrong place.
func Parse(data []byte) (*Map, error) {
	var f File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("shardmap: decode: %w", err)
	}
	return New(f)
}

// New validates an in-memory File (used by the data generator and tests).
func New(f File) (*Map, error) {
	if f.Version != CurrentVersion {
		return nil, fmt.Errorf("shardmap: unsupported version %d (want %d)", f.Version, CurrentVersion)
	}
	if err := checkNodes("coordinator", f.Coordinator.Nodes); err != nil {
		return nil, err
	}
	if len(f.Shards) == 0 {
		return nil, errors.New("shardmap: no shards")
	}

	m := &Map{
		version:     f.Version,
		coordinator: append([]string(nil), f.Coordinator.Nodes...),
		shardNodes:  make(map[uint32][]string, len(f.Shards)),
		owner:       make(map[Key]uint32, len(f.Sections)),
		byEvent:     make(map[string][]Section),
	}
	for _, s := range f.Shards {
		if s.ShardID == 0 {
			return nil, errors.New("shardmap: shard_id 0 is reserved for the coordinator group")
		}
		if _, dup := m.shardNodes[s.ShardID]; dup {
			return nil, fmt.Errorf("shardmap: duplicate shard_id %d", s.ShardID)
		}
		if err := checkNodes(fmt.Sprintf("shard %d", s.ShardID), s.Nodes); err != nil {
			return nil, err
		}
		m.shardNodes[s.ShardID] = append([]string(nil), s.Nodes...)
		m.shardIDs = append(m.shardIDs, s.ShardID)
	}
	sort.Slice(m.shardIDs, func(i, j int) bool { return m.shardIDs[i] < m.shardIDs[j] })

	for i, sec := range f.Sections {
		if sec.EventID == "" || sec.SectionID == "" {
			return nil, fmt.Errorf("shardmap: section %d has an empty event_id or section_id", i)
		}
		if _, ok := m.shardNodes[sec.ShardID]; !ok {
			return nil, fmt.Errorf("shardmap: section %s/%s points to unknown shard %d", sec.EventID, sec.SectionID, sec.ShardID)
		}
		k := Key{sec.EventID, sec.SectionID}
		if _, dup := m.owner[k]; dup {
			return nil, fmt.Errorf("shardmap: section %s/%s listed twice", sec.EventID, sec.SectionID)
		}
		m.owner[k] = sec.ShardID
		m.byEvent[sec.EventID] = append(m.byEvent[sec.EventID], sec)
	}
	for ev := range m.byEvent {
		secs := m.byEvent[ev]
		sort.Slice(secs, func(i, j int) bool { return secs[i].SectionID < secs[j].SectionID })
	}
	return m, nil
}

func checkNodes(name string, nodes []string) error {
	if len(nodes) == 0 {
		return fmt.Errorf("shardmap: %s has no nodes", name)
	}
	seen := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n == "" {
			return fmt.Errorf("shardmap: %s has an empty node address", name)
		}
		if seen[n] {
			return fmt.Errorf("shardmap: %s lists node %s twice", name, n)
		}
		seen[n] = true
	}
	return nil
}

// Version returns the map's format version.
func (m *Map) Version() int { return m.version }

// Lookup returns the shard that owns (eventID, sectionID).
func (m *Map) Lookup(eventID, sectionID string) (uint32, bool) {
	id, ok := m.owner[Key{eventID, sectionID}]
	return id, ok
}

// ShardNodes returns the node addresses of a shard (a copy).
func (m *Map) ShardNodes(shardID uint32) []string {
	return append([]string(nil), m.shardNodes[shardID]...)
}

// CoordinatorNodes returns the coordinator group's addresses (a copy).
func (m *Map) CoordinatorNodes() []string {
	return append([]string(nil), m.coordinator...)
}

// ShardIDs returns every shard ID in ascending order.
func (m *Map) ShardIDs() []uint32 { return append([]uint32(nil), m.shardIDs...) }

// EventSections returns an event's sections sorted by section_id.
func (m *Map) EventSections(eventID string) []Section {
	return append([]Section(nil), m.byEvent[eventID]...)
}

// EventShards returns the shards holding any section of an event, ascending.
// The router uses this to fan out ListAvailableSeats.
func (m *Map) EventShards(eventID string) []uint32 {
	seen := map[uint32]bool{}
	var out []uint32
	for _, s := range m.byEvent[eventID] {
		if !seen[s.ShardID] {
			seen[s.ShardID] = true
			out = append(out, s.ShardID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// RoundRobin builds the sections list for one event, assigning sections in
// the given order to shardIDs in turn. The data generator uses it.
func RoundRobin(eventID string, sectionIDs []string, shardIDs []uint32) []Section {
	out := make([]Section, len(sectionIDs))
	for i, sec := range sectionIDs {
		out[i] = Section{EventID: eventID, SectionID: sec, ShardID: shardIDs[i%len(shardIDs)]}
	}
	return out
}
