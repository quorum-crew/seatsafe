// Package raftmetrics exports a Raft node's state on /metrics in the
// Prometheus text format. Values are read from the node on every scrape, so
// there is nothing to keep in sync and nothing to update on the hot path.
//
// Every node reports its role, term and log positions. The leader also
// reports, per follower, the follower's match index and how many entries it
// is behind (replication lag).
package raftmetrics

import (
	"fmt"
	"io"
	"net"
	"strconv"

	"go.etcd.io/raft/v3"

	"github.com/quorum-crew/seatsafe/internal/raftnode"
)

// Source is what the exporter reads; *raftnode.Node implements it.
type Source interface {
	Status() raftnode.Status
}

// Writer returns a hook for server.Handler that appends this node's Raft
// metrics to /metrics. group is the Raft group (the shard ID), node is this
// node's name (NODE_ID), and names maps Raft IDs to node names for the
// follower label (see NamesFromAddrs).
func Writer(group, node string, src Source, names map[uint64]string) func(io.Writer) {
	return func(w io.Writer) { write(w, group, node, src.Status(), names) }
}

// NamesFromAddrs turns PEERS-style addresses ("shard1-node2:7000") into node
// names ("shard1-node2") keyed by Raft ID.
func NamesFromAddrs(addrs map[uint64]string) map[uint64]string {
	names := make(map[uint64]string, len(addrs))
	for id, addr := range addrs {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			names[id] = host
		} else {
			names[id] = addr
		}
	}
	return names
}

type gauge struct{ name, help string }

var (
	isLeader    = gauge{"seatsafe_raft_is_leader", "1 if this node is its Raft group's leader, else 0."}
	leaderID    = gauge{"seatsafe_raft_leader_id", "Raft ID of the leader this node knows of (0 = none)."}
	term        = gauge{"seatsafe_raft_term", "Current Raft term."}
	lastIndex   = gauge{"seatsafe_raft_last_index", "Index of the last entry in this node's log."}
	commitIndex = gauge{"seatsafe_raft_commit_index", "Highest log index known to be committed."}
	applied     = gauge{"seatsafe_raft_applied_index", "Highest log index applied to the state machine."}
	snapIndex   = gauge{"seatsafe_raft_snapshot_index", "Index of this node's newest snapshot (0 = none)."}

	followerMatch  = gauge{"seatsafe_raft_follower_match_index", "Leader only: highest log index known to be on each follower."}
	followerLag    = gauge{"seatsafe_raft_follower_lag_entries", "Leader only: entries each follower is behind the leader's last index."}
	followerActive = gauge{"seatsafe_raft_follower_recent_active", "Leader only: 1 if the follower was heard from within the last election timeout."}
)

func write(w io.Writer, group, node string, st raftnode.Status, names map[uint64]string) {
	base := fmt.Sprintf("group=%q,node=%q", group, node)
	one := func(g gauge, v uint64) {
		header(w, g)
		fmt.Fprintf(w, "%s{%s} %d\n", g.name, base, v)
	}
	one(isLeader, b2u(st.Role == raft.StateLeader))
	one(leaderID, st.Leader)
	one(term, st.Term)
	one(lastIndex, st.LastIndex)
	one(commitIndex, st.Commit)
	one(applied, st.Applied)
	one(snapIndex, st.SnapshotIndex)

	if len(st.Followers) == 0 {
		return // not the leader: no per-follower view
	}
	label := func(id uint64) string {
		name, ok := names[id]
		if !ok {
			name = strconv.FormatUint(id, 10)
		}
		return fmt.Sprintf("%s,follower=%q", base, name)
	}
	perFollower := func(g gauge, value func(raftnode.FollowerProgress) uint64) {
		header(w, g)
		for _, f := range st.Followers {
			fmt.Fprintf(w, "%s{%s} %d\n", g.name, label(f.ID), value(f))
		}
	}
	perFollower(followerMatch, func(f raftnode.FollowerProgress) uint64 { return f.Match })
	perFollower(followerLag, func(f raftnode.FollowerProgress) uint64 {
		if f.Match >= st.LastIndex {
			return 0
		}
		return st.LastIndex - f.Match
	})
	perFollower(followerActive, func(f raftnode.FollowerProgress) uint64 { return b2u(f.RecentActive) })
}

func header(w io.Writer, g gauge) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
