// Package seed defines the seat inventory file each shard is seeded from and
// how it is split into SeedOp chunks.
//
// Format (format_version 1), one file per shard at {SEED_DIR}/shard{N}.json:
//
//	{
//	  "format_version": 1,
//	  "shard_id": 2,
//	  "chunk_size": 1000,
//	  "seats": [ { "event_id": "evt-001", "section_id": "sec-102", "seat_id": "R01-S001" }, ... ]
//	}
//
// The datagen command writes these files (in containers they are mounted at
// /seed). When a storage node becomes leader and the state machine is not
// fully seeded, it calls Load and proposes the missing chunks from Chunks as
// SeedOps. Seed version is the SHA-256 of the file bytes, so any change to
// the inventory yields a new version, which the state machine refuses to mix
// with an already applied one. chunk_size lives in the file (and therefore in
// the hash) so every leader cuts identical chunks.
package seed

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// FormatVersion is the file format this package reads and writes.
const FormatVersion = 1

// DefaultChunkSize is the number of seats per SeedOp (~60 KB per log entry).
const DefaultChunkSize = 1000

// DefaultDir is where docker-compose mounts deploy/seed inside containers.
const DefaultDir = "/seed"

// Seat is one seat in the inventory. Field names match SeatRef.
type Seat struct {
	EventID   string `json:"event_id"`
	SectionID string `json:"section_id"`
	SeatID    string `json:"seat_id"`
}

// File is the on-disk JSON shape.
type File struct {
	FormatVersion int    `json:"format_version"`
	ShardID       uint32 `json:"shard_id"`
	ChunkSize     int    `json:"chunk_size"`
	Seats         []Seat `json:"seats"`
}

// Loaded is a validated seed file plus its version.
type Loaded struct {
	File
	Version string // hex SHA-256 of the file bytes
}

// Path returns the seed file path for a shard inside dir.
func Path(dir string, shardID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("shard%d.json", shardID))
}

// DirFromEnv returns $SEED_DIR, or DefaultDir if it is unset.
func DirFromEnv() string {
	if d := os.Getenv("SEED_DIR"); d != "" {
		return d
	}
	return DefaultDir
}

// Load reads and validates the seed file for wantShard.
func Load(path string, wantShard uint32) (*Loaded, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("seed: read %s: %w", path, err)
	}
	return Parse(data, wantShard)
}

// Parse validates seed file bytes. Unknown fields are rejected.
func Parse(data []byte, wantShard uint32) (*Loaded, error) {
	var f File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("seed: decode: %w", err)
	}
	if err := Validate(f, wantShard); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	return &Loaded{File: f, Version: hex.EncodeToString(sum[:])}, nil
}

// Validate checks a seed file's header and that no seat appears twice.
func Validate(f File, wantShard uint32) error {
	if f.FormatVersion != FormatVersion {
		return fmt.Errorf("seed: unsupported format_version %d (want %d)", f.FormatVersion, FormatVersion)
	}
	if f.ShardID != wantShard {
		return fmt.Errorf("seed: file is for shard %d, this node is shard %d", f.ShardID, wantShard)
	}
	if f.ChunkSize <= 0 {
		return errors.New("seed: chunk_size must be positive")
	}
	seen := make(map[Seat]struct{}, len(f.Seats))
	for i, s := range f.Seats {
		if s.EventID == "" || s.SectionID == "" || s.SeatID == "" {
			return fmt.Errorf("seed: seat %d has an empty field", i)
		}
		if _, dup := seen[s]; dup {
			return fmt.Errorf("seed: seat %s/%s/%s listed twice", s.EventID, s.SectionID, s.SeatID)
		}
		seen[s] = struct{}{}
	}
	return nil
}

// TotalChunks is how many SeedOps the file splits into.
func (l *Loaded) TotalChunks() int {
	if len(l.Seats) == 0 {
		return 0
	}
	return (len(l.Seats) + l.ChunkSize - 1) / l.ChunkSize
}

// Chunk returns the seats of chunk i (0-based), in file order.
func (l *Loaded) Chunk(i int) []Seat {
	start := i * l.ChunkSize
	if i < 0 || start >= len(l.Seats) {
		return nil
	}
	end := start + l.ChunkSize
	if end > len(l.Seats) {
		end = len(l.Seats)
	}
	return l.Seats[start:end]
}

// Marshal encodes a seed file deterministically: same input, same bytes,
// same version.
func Marshal(f File) ([]byte, error) {
	if err := Validate(f, f.ShardID); err != nil {
		return nil, err
	}
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
