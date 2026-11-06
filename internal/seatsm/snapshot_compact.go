package seatsm

import (
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/golang/snappy"

	seatsafev1 "github.com/quorum-crew/seatsafe/gen/seatsafe/v1"
)

// Format 2 (compact): the version byte, then this body compressed with snappy.
// Integers are varints; every list starts with its length and is sorted, so
// equal states give equal bytes.
//
//	applied index
//	seed: version, total chunks, chunks (as gaps)
//	tables: events, seat IDs, users
//	IDs: every request_id, txn_id and seat owner. UUIDs first, as 16 raw bytes,
//	     then the rest as strings. A seat's owner is here even after GC dropped
//	     its req/ record.
//	extra seats: references to seats that do not exist (a hand-made format 1
//	     snapshot is the only source), as three strings
//	sections, per event: name, then seats as (seat ID gap, flags, owner, user).
//	     A seat's position in this walk is its index everywhere else.
//	req/: base time, then per record: ID gap, head, order_id if it is not the
//	     request_id, time since base, unavailable seats
//	txn/: per record: ID gap, head, user, seats, hold, vote, conflicts, decided
//
// Seat lists keep request order, so they are written as gaps from the previous
// seat, not sorted. Adjacent seats of one order cost a byte each.
const (
	seatStateMask byte = 0x0f
	seatHasOwner  byte = 1 << 4
	seatHasUser   byte = 1 << 5
	seatSameAs    byte = 1 << 6 // same owner and user as the seat before it

	reqCrossShard uint64 = 1 << 0
	reqOrderID    uint64 = 1 << 1
	reqHeadBits          = 2

	txnStatusMask uint64 = 0x3
	txnHasHold    uint64 = 1 << 2
	txnHasDecided uint64 = 1 << 3
)

const uuidLen = 36

// parseUUID reads the canonical lowercase form. Anything else stays a string,
// so an ID always comes back exactly as it went in.
func parseUUID(s string) (raw [16]byte, ok bool) {
	if len(s) != uuidLen || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return raw, false
	}
	var h [32]byte
	copy(h[0:], s[0:8])
	copy(h[8:], s[9:13])
	copy(h[12:], s[14:18])
	copy(h[16:], s[19:23])
	copy(h[20:], s[24:])
	for _, c := range h {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return raw, false
		}
	}
	if _, err := hex.Decode(raw[:], h[:]); err != nil {
		return raw, false
	}
	return raw, true
}

func formatUUID(raw []byte) string {
	var b [uuidLen]byte
	hex.Encode(b[0:], raw[0:4])
	b[8] = '-'
	hex.Encode(b[9:], raw[4:6])
	b[13] = '-'
	hex.Encode(b[14:], raw[6:8])
	b[18] = '-'
	hex.Encode(b[19:], raw[8:10])
	b[23] = '-'
	hex.Encode(b[24:], raw[10:])
	return string(b[:])
}

type writer struct{ b []byte }

func (w *writer) uvarint(v uint64) { w.b = binary.AppendUvarint(w.b, v) }
func (w *writer) varint(v int64)   { w.b = binary.AppendVarint(w.b, v) }
func (w *writer) count(n int)      { w.uvarint(uint64(n)) }
func (w *writer) str(s string) {
	w.count(len(s))
	w.b = append(w.b, s...)
}

func (w *writer) strs(ss []string) {
	w.count(len(ss))
	for _, s := range ss {
		w.str(s)
	}
}

// table sorts list and drops duplicates.
func table(list []string) []string {
	slices.Sort(list)
	return slices.Compact(list)
}

func positions(tbl []string) map[string]uint64 {
	out := make(map[string]uint64, len(tbl))
	for i, s := range tbl {
		out[s] = uint64(i)
	}
	return out
}

type sectionBase struct {
	sec   *section
	first uint64 // index of the section's first seat
}

// seatIndex turns seat keys into the numbers a snapshot refers to them by.
type seatIndex struct {
	sections map[sectionKey]sectionBase
	total    uint64
	extra    []seatKey
	extras   map[seatKey]uint64

	// The last seat found: orders usually take adjacent seats.
	last    sectionKey
	lastSec sectionBase
	lastPos int
}

func (x *seatIndex) of(k seatKey) uint64 {
	sk := sectionKey{k.event, k.section}
	if sk != x.last {
		x.last, x.lastSec, x.lastPos = sk, x.sections[sk], -1
	}
	if sec := x.lastSec.sec; sec != nil {
		pos := x.lastPos + 1
		if pos == 0 || pos >= len(sec.sorted) || sec.sorted[pos].id != k.seat {
			var found bool
			pos, found = slices.BinarySearchFunc(sec.sorted, k.seat, func(st *seat, id string) int { return cmp.Compare(st.id, id) })
			if !found {
				pos = -1
			}
		}
		if pos >= 0 {
			x.lastPos = pos
			return x.lastSec.first + uint64(pos)
		}
	}
	i, ok := x.extras[k]
	if !ok {
		i = x.total + uint64(len(x.extra))
		x.extras[k] = i
		x.extra = append(x.extra, k)
	}
	return i
}

// seats writes a list as gaps from the previous seat.
func (w *writer) seats(x *seatIndex, keys []seatKey) {
	w.count(len(keys))
	var prev int64
	for _, k := range keys {
		i := int64(x.of(k))
		w.varint(i - prev)
		prev = i
	}
}

// encodeCompact returns the format 2 body of s.
func encodeCompact(s *state) []byte {
	// Collect the tables. Seats of one order sit together, so comparing with
	// the seat before skips most repeats.
	seatSet := map[string]struct{}{}
	idList := make([]string, 0, len(s.reqs)+len(s.txns))
	var userList []string
	for id := range s.reqs {
		idList = append(idList, id)
	}
	for id, t := range s.txns {
		idList = append(idList, id)
		if t.user != "" {
			userList = append(userList, t.user)
		}
	}
	for _, sec := range s.sections {
		var owner, user string
		for _, st := range sec.sorted {
			seatSet[st.id] = struct{}{}
			if st.owner != owner && st.owner != "" {
				idList = append(idList, st.owner)
			}
			if st.user != user && st.user != "" {
				userList = append(userList, st.user)
			}
			owner, user = st.owner, st.user
		}
	}
	seatTable := make([]string, 0, len(seatSet))
	for id := range seatSet {
		seatTable = append(seatTable, id)
	}
	seatTable, userTable := table(seatTable), table(userList)
	seatIDs, users := positions(seatTable), positions(userTable)

	// IDs are numbered UUIDs first, then the rest.
	ordered := make([]string, 0, len(idList))
	raws := make([]byte, 0, len(idList)*16)
	var others []string
	for _, id := range table(idList) {
		if raw, ok := parseUUID(id); ok {
			ordered = append(ordered, id)
			raws = append(raws, raw[:]...)
		} else {
			others = append(others, id)
		}
	}
	nUUID := len(ordered)
	ordered = append(ordered, others...)
	ids := positions(ordered)

	events := make([]string, 0, len(s.events))
	for e := range s.events {
		events = append(events, e)
	}
	sort.Strings(events)

	// Sections first, into their own buffer: their walk numbers the seats, and
	// the records below may add extra seats that the header must list.
	x := &seatIndex{sections: make(map[sectionKey]sectionBase, len(s.sections)), extras: map[seatKey]uint64{}}
	sections := &writer{}
	for _, e := range events {
		secs := s.events[e]
		sections.count(len(secs))
		for _, name := range secs {
			sk := sectionKey{e, name}
			sec := s.sections[sk]
			x.sections[sk] = sectionBase{sec, x.total}
			x.total += uint64(len(sec.sorted))
			sections.str(name)
			sections.count(len(sec.sorted))
			var prevID uint64
			var prev *seat
			for i, st := range sec.sorted {
				id := seatIDs[st.id]
				if i == 0 {
					sections.uvarint(id)
				} else {
					sections.uvarint(id - prevID - 1)
				}
				prevID = id
				flags := byte(st.state) & seatStateMask
				if st.owner != "" {
					flags |= seatHasOwner
				}
				if st.user != "" {
					flags |= seatHasUser
				}
				if prev != nil && st.owner == prev.owner && st.user == prev.user && flags&(seatHasOwner|seatHasUser) != 0 {
					sections.b = append(sections.b, flags|seatSameAs)
				} else {
					sections.b = append(sections.b, flags)
					if st.owner != "" {
						sections.uvarint(ids[st.owner])
					}
					if st.user != "" {
						sections.uvarint(users[st.user])
					}
				}
				prev = st
			}
		}
	}

	// Records, in ID order.
	records := &writer{}
	var base int64
	first := true
	for _, r := range s.reqs {
		if first || r.appliedAt < base {
			base, first = r.appliedAt, false
		}
	}
	records.varint(base)
	records.count(len(s.reqs))
	var prevID uint64
	var keys []seatKey
	for i, id := range ordered {
		r, ok := s.reqs[id]
		if !ok {
			continue
		}
		records.uvarint(uint64(i) - prevID)
		prevID = uint64(i)
		o := int64(r.resp.Outcome)
		head := uint64(o<<1^o>>63) << reqHeadBits
		if r.resp.CrossShard {
			head |= reqCrossShard
		}
		if r.resp.OrderId != id {
			head |= reqOrderID
		}
		records.uvarint(head)
		if head&reqOrderID != 0 {
			records.str(r.resp.OrderId)
		}
		records.uvarint(uint64(r.appliedAt - base))
		keys = keys[:0]
		for _, u := range r.resp.UnavailableSeats {
			keys = append(keys, keyOf(u))
		}
		records.seats(x, keys)
	}
	records.count(len(s.txns))
	prevID = 0
	for i, id := range ordered {
		if len(s.txns) == 0 {
			break
		}
		t, ok := s.txns[id]
		if !ok {
			continue
		}
		records.uvarint(uint64(i) - prevID)
		prevID = uint64(i)
		head := uint64(t.status) & txnStatusMask
		if t.holdExpires != 0 {
			head |= txnHasHold
		}
		if t.decidedAt != 0 {
			head |= txnHasDecided
		}
		records.uvarint(head)
		if t.user == "" {
			records.uvarint(0)
		} else {
			records.uvarint(users[t.user] + 1)
		}
		records.seats(x, t.seats)
		if t.holdExpires != 0 {
			records.varint(t.holdExpires - base)
		}
		records.varint(int64(t.vote))
		records.seats(x, t.conflicts)
		if t.decidedAt != 0 {
			records.varint(t.decidedAt - base)
		}
	}

	// Header, then the two buffers.
	w := &writer{b: make([]byte, 0, len(sections.b)+len(records.b)+len(raws)+1024)}
	w.uvarint(s.applied)
	w.str(s.seedVersion)
	w.uvarint(uint64(s.seedTotal))
	chunks := make([]uint32, 0, len(s.seedChunks))
	for c := range s.seedChunks {
		chunks = append(chunks, c)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	w.count(len(chunks))
	var prevChunk uint32
	for _, c := range chunks {
		w.uvarint(uint64(c - prevChunk))
		prevChunk = c
	}
	w.strs(events)
	w.strs(seatTable)
	w.strs(userTable)
	w.count(nUUID)
	w.b = append(w.b, raws...)
	w.strs(others)
	w.count(len(x.extra))
	for _, k := range x.extra {
		w.str(k.event)
		w.str(k.section)
		w.str(k.seat)
	}
	w.b = append(w.b, sections.b...)
	w.b = append(w.b, records.b...)
	return w.b
}

// maxSnappyRatio bounds how far snappy can expand its input, so a corrupt
// length header fails instead of allocating.
const maxSnappyRatio = 64

// compress returns the version byte and the compressed body.
func compress(body []byte) []byte {
	dst := make([]byte, 1+snappy.MaxEncodedLen(len(body)))
	dst[0] = compactVersion
	return dst[:1+len(snappy.Encode(dst[1:], body))]
}

func decompress(data []byte) ([]byte, error) {
	n, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, fmt.Errorf("seatsm: snapshot: %w", err)
	}
	if n > len(data)*maxSnappyRatio {
		return nil, fmt.Errorf("seatsm: snapshot: %d bytes claims to expand to %d", len(data), n)
	}
	body, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("seatsm: snapshot: %w", err)
	}
	return body, nil
}

// reader decodes a format 2 body. The first problem sticks: every later read
// returns zero values, so loops end on the bytes they have.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("seatsm: snapshot: "+format, args...)
	}
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail("bad number")
		r.b = nil
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail("bad number")
		r.b = nil
		return 0
	}
	r.b = r.b[n:]
	return v
}

// count reads a list length. Every element takes at least size bytes, so a
// corrupt length fails here instead of allocating.
func (r *reader) count(size int) int {
	n := r.uvarint()
	if n > uint64(len(r.b)/size) {
		r.fail("list of %d does not fit", n)
		r.b = nil
		return 0
	}
	return int(n)
}

func (r *reader) bytes(n int) []byte {
	if n > len(r.b) {
		r.fail("truncated")
		r.b = nil
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) str() string {
	return string(r.bytes(r.count(1)))
}

// name reads a string that must not be empty.
func (r *reader) name(what string) string {
	s := r.str()
	if s == "" {
		r.fail("empty %s", what)
	}
	return s
}

func (r *reader) names(what string) []string {
	out := make([]string, r.count(2))
	for i := range out {
		out[i] = r.name(what)
	}
	return out
}

// index reads a number that must be below n.
func (r *reader) index(n int) int {
	v := r.uvarint()
	if v >= uint64(n) {
		r.fail("index %d out of range %d", v, n)
		return 0
	}
	return int(v)
}

// seats reads a list written by writer.seats.
func (r *reader) seats(all []seatKey) []seatKey {
	n := r.count(1)
	if n == 0 {
		return nil
	}
	out := make([]seatKey, n)
	var prev int64
	for i := range out {
		prev += r.varint()
		if prev < 0 || prev >= int64(len(all)) {
			r.fail("seat %d out of range %d", prev, len(all))
			return nil
		}
		out[i] = all[prev]
	}
	return out
}

// decodeCompact rebuilds a state from a format 2 body.
func decodeCompact(data []byte) (*state, error) {
	body, err := decompress(data)
	if err != nil {
		return nil, err
	}
	r := &reader{b: body}
	s := newState()
	s.applied = r.uvarint()
	version := r.str()
	total := r.uvarint()
	if total > 1<<32-1 {
		r.fail("seed of %d chunks", total)
	}
	chunks := make([]uint32, r.count(1))
	var chunk uint64
	for i := range chunks {
		chunk += r.uvarint()
		chunks[i] = uint32(chunk)
	}
	if r.err != nil {
		return nil, r.err
	}
	if err := s.setSeed(version, uint32(total), chunks); err != nil {
		return nil, err
	}

	events := r.names("event")
	seatIDs := r.names("seat id")
	users := r.names("user")
	nUUID := r.count(16)
	ids := make([]string, nUUID, nUUID+16)
	for i := range ids {
		ids[i] = formatUUID(r.bytes(16))
	}
	for n := r.count(2); n > 0 && r.err == nil; n-- {
		ids = append(ids, r.name("id"))
	}
	extra := make([]seatKey, r.count(3))
	for i := range extra {
		extra[i] = seatKey{r.str(), r.str(), r.str()}
	}
	if r.err != nil {
		return nil, r.err
	}

	var all []seatKey
	for _, e := range events {
		nSecs := r.count(2)
		for i := 0; i < nSecs && r.err == nil; i++ {
			name := r.name("section")
			n := r.count(2)
			sec := &section{sorted: make([]*seat, n), seats: make(map[string]*seat, n)}
			seats := make([]seat, n)
			if _, dup := s.sections[sectionKey{e, name}]; dup || n == 0 {
				r.fail("section %s/%s is empty or listed twice", e, name)
			}
			var id, owner, user int
			for j := 0; j < n && r.err == nil; j++ {
				if j == 0 {
					id = r.index(len(seatIDs))
				} else {
					gap := r.uvarint()
					if gap >= uint64(len(seatIDs)-id-1) {
						r.fail("seat id gap %d out of range", gap)
						break
					}
					id += int(gap) + 1
				}
				flags := r.bytes(1)
				if r.err != nil {
					break
				}
				f := flags[0]
				st := &seats[j]
				st.id = seatIDs[id]
				st.state = seatsafev1.SeatState(f & seatStateMask)
				if f&seatSameAs == 0 {
					if f&seatHasOwner != 0 {
						owner = r.index(len(ids))
					}
					if f&seatHasUser != 0 {
						user = r.index(len(users))
					}
				}
				if f&seatHasOwner != 0 {
					if len(ids) == 0 {
						r.fail("seat owner with no ids")
						break
					}
					st.owner = ids[owner]
				}
				if f&seatHasUser != 0 {
					if len(users) == 0 {
						r.fail("seat user with no users")
						break
					}
					st.user = users[user]
				}
				sec.sorted[j], sec.seats[st.id] = st, st
				all = append(all, seatKey{e, name, st.id})
			}
			if r.err != nil {
				return nil, r.err
			}
			s.sections[sectionKey{e, name}] = sec
			s.events[e] = append(s.events[e], name)
		}
		sort.Strings(s.events[e])
	}
	all = append(all, extra...)

	base := r.varint()
	nReqs := r.count(2)
	id := 0
	for i := 0; i < nReqs && r.err == nil; i++ {
		id += r.index(len(ids) - id + 1)
		if id >= len(ids) || s.reqs[ids[id]] != nil {
			r.fail("req record %d is out of range or listed twice", i)
			break
		}
		head := r.uvarint()
		z := head >> reqHeadBits
		orderID := ids[id]
		if head&reqOrderID != 0 {
			orderID = r.str()
		}
		rec := &reqRecord{appliedAt: base + int64(r.uvarint()), resp: &seatsafev1.BookSeatsResponse{
			OrderId: orderID, Outcome: seatsafev1.BookingOutcome(int64(z>>1) ^ -int64(z&1)), CrossShard: head&reqCrossShard != 0,
		}}
		rec.resp.UnavailableSeats = refs(r.seats(all))
		s.reqs[ids[id]] = rec
	}
	nTxns := r.count(2)
	id = 0
	for i := 0; i < nTxns && r.err == nil; i++ {
		id += r.index(len(ids) - id + 1)
		if id >= len(ids) || s.txns[ids[id]] != nil {
			r.fail("txn record %d is out of range or listed twice", i)
			break
		}
		head := r.uvarint()
		status := txnStatus(head & txnStatusMask)
		if status < txnPrepared || status > txnAborted {
			r.fail("txn %q has status %d", ids[id], status)
			break
		}
		t := &txnRecord{status: status}
		if u := r.index(len(users) + 1); u > 0 {
			t.user = users[u-1]
		}
		t.seats = r.seats(all)
		if head&txnHasHold != 0 {
			t.holdExpires = base + r.varint()
		}
		t.vote = seatsafev1.Vote(r.varint())
		t.conflicts = r.seats(all)
		if head&txnHasDecided != 0 {
			t.decidedAt = base + r.varint()
		}
		s.txns[ids[id]] = t
		if status == txnPrepared {
			s.prepared[ids[id]] = t
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	if len(r.b) != 0 {
		return nil, errors.New("seatsm: snapshot: trailing bytes")
	}
	return s, nil
}
