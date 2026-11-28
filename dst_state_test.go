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
