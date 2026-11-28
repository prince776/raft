package raft

// The cluster's abstract state, for guided exploration (go-dst-kit's
// Harness.State, DST.md 12): a small reduction of what the Raft paper says
// a server is, per node, plus the cluster's leadership. Chosen from the
// protocol's state variables, not from the bugs found, so it doesn't steer
// toward known answers. Symmetric: the per-node parts are sorted, so "one
// leader, two lagging followers" is one state whichever nodes they are.
//
// It counts, over the cluster's running nodes (each count capped: 0, 1 or
// 2 or more, or 0 and 1 or more), the protocol situations a server can be
// in: leaders; candidates; nodes down; nodes with a configuration change in
// flight (latest uncommitted); whether the nodes' latest configurations
// disagree; nodes that aren't voters in their own latest configuration;
// nodes whose log overlaps their snapshot, and whose log starts past it (a
// gap); nodes whose commit index lags the cluster's highest. Counts, not a
// per-node list: a per-node list gave 5000 distinct states in 40 runs,
// states that never recur, so rarity steered nowhere.
//
// It reads fields between steps (no goroutine runs then), not through the
// World: the log store's and raft's locks are World operations.

import (
	"time"

	"github.com/prince776/go-dst-kit/dsttest"
)

func (c *simCluster) abstractState() uint64 {
	var maxCommit uint64
	for _, nd := range c.nodes {
		if nd.up() {
			maxCommit = max(maxCommit, nd.raft.getCommitIndex()) // raft's uint64 max
		}
	}
	var leaders, candidates, down, pending, nonvoter, overlap, gap, lagging uint64
	disagree := false
	var cfgIndex uint64
	seen := false
	for _, nd := range c.nodes {
		if nd.raft == nil {
			continue // a spare not started yet
		}
		if nd.m.Dead() {
			down++
			continue
		}
		switch nd.raft.getState() {
		case Leader:
			leaders++
		case Candidate:
			candidates++
		}
		cfg := &nd.raft.configurations
		if cfg.latestIndex != cfg.committedIndex {
			pending++
		}
		if seen && cfg.latestIndex != cfgIndex {
			disagree = true
		}
		cfgIndex, seen = cfg.latestIndex, true
		voter := false
		for _, s := range cfg.latest.Servers {
			if s.ID == nd.id() && s.Suffrage == Voter {
				voter = true
			}
		}
		if !voter {
			nonvoter++
		}
		if snap, first := nd.raft.lastSnapshotIndex, nd.logs.first; snap > 0 && first > 0 {
			if first <= snap {
				overlap++
			} else if first > snap+1 {
				gap++
			}
		}
		if nd.raft.getCommitIndex() < maxCommit {
			lagging++
		}
	}
	cap2 := func(v uint64) uint64 { return min(v, 2) } // raft's uint64 min
	cap1 := func(v uint64) uint64 { return min(v, 1) }
	return cap2(leaders) | cap1(candidates)<<2 | cap2(down)<<3 | cap1(pending)<<5 | b2u(disagree)<<6 |
		cap1(nonvoter)<<7 | cap1(overlap)<<8 | cap1(gap)<<9 | cap2(lagging)<<10
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// Progress quantities for guided exploration (DST.md 10c; go-dst-kit's
// Harness.Progress): what the protocol is meant to keep bounded, so a run
// that pushes one higher than before is a run under more stress. Fixed
// before measuring, from the protocol, not the bugs (one substitution, made
// before any data: "a node paused" for "a voter cut off from the leader",
// which isn't cheap to read each step):
//   - how long the cluster has had no leader (ms);
//   - how long a configuration change has been uncommitted somewhere (ms);
//   - the largest commit lag of a running node (entries);
//   - the spread of running nodes' terms;
//   - how long some node's log has been gapped after its snapshot (ms);
//   - the compound frontier: how many of these hold at once: no leader, a
//     configuration change in flight, a node down, a node paused, a node
//     lagging 10 or more entries, a log gapped or overlapping its snapshot,
//     latest configurations disagreeing.
//
// Only those that predicted failure are steered by (TestDSTPhase4Forks,
// 1000 seeds, failure rate in the tenth of runs that pushed it highest
// against 4.2% overall): commit lag 9%, the frontier 8%, term spread 7%.
// Dropped: ms without a leader (1%), ms with a change uncommitted (2%),
// ms gapped (3%; almost never above 0). Time-based quantities are biased
// against by that test (a failing run stops, so has less time to grow
// them); still dropped, as decided before measuring.
type stress struct {
	noLeaderSince, pendingSince, gapSince         time.Duration // -1: not now
	noLeader, pending, lag, spread, gap, frontier int64
}

func (c *simCluster) progressChecks(h *dsttest.Harness) {
	s := &c.stress
	s.noLeaderSince, s.pendingSince, s.gapSince = -1, -1, -1
	since := func(on bool, from *time.Duration, now time.Duration) int64 {
		if !on {
			*from = -1
			return 0
		}
		if *from < 0 {
			*from = now
		}
		return int64((now - *from) / time.Millisecond)
	}
	// The first quantity computes them all for the step; the rest read
	// what it found (Progress functions run in registration order).
	h.Progress("ms without a leader", func() int64 { // kept for the coverage report
		now := h.Elapsed()
		var maxCommit, minTerm, maxTerm uint64
		minTerm = ^uint64(0)
		up, leaders := 0, 0
		var pending, gapped, overlap, down, paused, lagging, disagree bool
		var cfgIndex uint64
		seen := false
		for _, nd := range c.nodes {
			if nd.up() {
				maxCommit = max(maxCommit, nd.raft.getCommitIndex()) // raft's uint64 max
			}
		}
		var lag uint64
		for _, nd := range c.nodes {
			if nd.raft == nil {
				continue
			}
			if nd.m.Dead() {
				down = true
				continue
			}
			up++
			paused = paused || nd.m.Paused()
			if nd.raft.getState() == Leader {
				leaders++
			}
			t := nd.raft.getCurrentTerm()
			minTerm, maxTerm = min(minTerm, t), max(maxTerm, t)
			cfg := &nd.raft.configurations
			pending = pending || cfg.latestIndex != cfg.committedIndex
			if seen && cfg.latestIndex != cfgIndex {
				disagree = true
			}
			cfgIndex, seen = cfg.latestIndex, true
			if snap, first := nd.raft.lastSnapshotIndex, nd.logs.first; snap > 0 && first > 0 {
				gapped = gapped || first > snap+1
				overlap = overlap || first <= snap
			}
			l := maxCommit - nd.raft.getCommitIndex()
			lag = max(lag, l)
			lagging = lagging || l >= 10
		}
		s.noLeader = since(up > 0 && leaders == 0, &s.noLeaderSince, now)
		s.pending = since(pending, &s.pendingSince, now)
		s.gap = since(gapped, &s.gapSince, now)
		s.lag = int64(lag)
		s.spread = 0
		if up > 0 {
			s.spread = int64(maxTerm - minTerm)
		}
		s.frontier = 0
		for _, b := range []bool{up > 0 && leaders == 0, pending, down, paused, lagging, gapped || overlap, disagree} {
			if b {
				s.frontier++
			}
		}
		return s.noLeader
	})
	h.Progress("largest commit lag", func() int64 { return s.lag })
	h.Progress("term spread", func() int64 { return s.spread })
	h.Progress("stress conditions at once", func() int64 { return s.frontier })
}
