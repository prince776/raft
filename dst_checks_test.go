package raft

// The checks of a simulated cluster (dst_cluster_test.go): what must hold
// whatever the faults, within raft's fault model.

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
)

// checks registers the cluster's checks: always, election safety, leaders
// being voters and the commit rule; at the end, that the cluster recovers, that the
// clients' history is linearizable, that acknowledged writes survive on
// the cluster's members, that every node applied a prefix of one sequence,
// and, with the operator, that the members agree on the configuration and
// the membership changes are linearizable.
func (c *simCluster) checks() {
	h := c.h
	// Election safety: at most one leader per term, ever (a crashed node's
	// raft keeps its last state, so only running nodes count).
	leaders := map[uint64]string{}
	// A manual recovery (RecoverCluster) forces a configuration without a
	// quorum: the recovered server's next election may reuse a term a lost
	// server led. Election safety holds again from there.
	c.onManualRecovery = append(c.onManualRecovery, func() { clear(leaders) })
	h.Always("at most one leader per term", func() error {
		for _, nd := range c.nodes {
			if !nd.up() || nd.raft.getState() != Leader {
				continue
			}
			term := nd.raft.getCurrentTerm()
			if other, ok := leaders[term]; ok && other != nd.name {
				return fmt.Errorf("term %d has two leaders: %s and %s", term, other, nd.name)
			}
			leaders[term] = nd.name
		}
		return nil
	})
	// A leader is a voter of its own configuration when it is elected
	// (later it may demote or remove itself: it steps down once that
	// commits).
	elected := map[string]uint64{} // node -> term it was last seen leading
	h.Always("only voters are elected", func() error {
		for _, nd := range c.nodes {
			if !nd.up() || nd.raft.getState() != Leader {
				continue
			}
			term := nd.raft.getCurrentTerm()
			if elected[nd.name] == term {
				continue
			}
			elected[nd.name] = term
			if !hasVote(nd.raft.configurations.latest, nd.id()) {
				return fmt.Errorf("%s leads term %d but isn't a voter in its configuration %v", nd.name, term, nd.raft.configurations.latest.Servers)
			}
		}
		return nil
	})
	// The commit rule, white-box: when a leader moves its commit index to
	// i, a majority of the voters in its configuration durably store its
	// entry i (same term), or a snapshot covering it. Raft's end-to-end
	// guarantees rest on this rule, but breaking it shows only under faults
	// outside raft's model (quorum-off-by-one, DST.md 6.4), so the other
	// checks can't see it.
	//
	// "Its configuration" is the one the leader counted against, which the
	// check can't see directly: the count, a configuration change and the
	// commit index reaching the leader loop (through a channel, steps
	// later) come in any order. So it accepts any configuration the leader
	// held as its latest since its commit index last moved. A fixed cluster
	// has only one. A leader appending a configuration change counts its
	// own write of that entry before switching to the new configuration
	// (dispatchLogs, then setConfiguration), so adding a second voter to a
	// one-voter cluster commits at once, by the old configuration (DST.md
	// 6.9). A node's commit index from before it leads (learned while
	// following) is the previous leader's move, checked when it was made.
	checkedCommit := map[string]uint64{} // leader -> commit index last checked
	held := map[string][]Configuration{} // leader -> latest configurations since its last commit move
	heldTerm := map[string]uint64{}
	commitRule := func() error {
		for _, nd := range c.nodes {
			if !nd.up() || nd.raft.getState() != Leader {
				continue
			}
			latestNow := nd.raft.configurations.latest
			if t := nd.raft.getCurrentTerm(); heldTerm[nd.name] != t {
				// A new term as leader: its commit index so far came from
				// following the previous leader (checked when that one moved
				// it). Judge only the moves this leader makes.
				heldTerm[nd.name], held[nd.name] = t, nil
				checkedCommit[nd.name] = nd.raft.getCommitIndex()
			}
			if hs := held[nd.name]; len(hs) == 0 || !slices.Equal(hs[len(hs)-1].Servers, latestNow.Servers) {
				held[nd.name] = append(hs, latestNow.Clone())
			}
			ci := nd.raft.getCommitIndex()
			if ci == 0 || ci <= checkedCommit[nd.name] {
				continue
			}
			checkedCommit[nd.name] = ci
			candidates := held[nd.name]
			held[nd.name] = []Configuration{latestNow.Clone()}
			term, has, _ := nd.logs.durableAt(ci)
			if !has {
				continue // compacted on the leader already: checked when it moved there
			}
			majority := func(cfg Configuration) (bool, string) {
				voters, stored := 0, 0
				for _, srv := range cfg.Servers {
					if srv.Suffrage != Voter {
						continue
					}
					voters++
					other := c.node(srv.ID)
					if other == nil || other.logs == nil {
						continue
					}
					t, ok, compacted := other.logs.durableAt(ci)
					if (ok && t == term) || compacted || other.snaps.covers(ci) {
						stored++
					}
				}
				return stored > voters/2, fmt.Sprintf("%d of %d voters", stored, voters)
			}
			ok, counts := false, []string{}
			for _, cfg := range candidates {
				m, n := majority(cfg)
				ok = ok || m
				counts = append(counts, n)
			}
			if !ok {
				return fmt.Errorf("%s committed index %d (term %d) stored by %s of its configuration(s) since its last commit", nd.name, ci, term, strings.Join(counts, ", "))
			}
		}
		return nil
	}
	if !c.spec.noWhiteBox {
		h.Always("a leader commits only what a majority stores", commitRule)
	}
	h.Final("the cluster recovers", func(w *dst.World) error { return c.recovers(w) })
	h.Final("every member catches up", func(w *dst.World) error { return c.catchesUp(w) })
	h.Final("the clients' history is linearizable", func(w *dst.World) error {
		return check.Linearizable(check.KVModel(), c.resolvedOps(), check.Options{}).Err()
	})
	h.Final("acknowledged writes survive", func(w *dst.World) error {
		applied := map[string]bool{}
		for _, nd := range c.members() {
			for _, id := range nd.fsm.applied {
				applied[id] = true
			}
		}
		for _, id := range slices.Sorted(maps.Keys(c.kv.acked)) {
			if !applied[id] {
				return fmt.Errorf("write %s was acknowledged but no member of the cluster applied it", id)
			}
		}
		return nil
	})
	h.Final("the nodes applied the same sequence", func(w *dst.World) error {
		var longest []string
		for _, nd := range c.nodes {
			if nd.fsm != nil && len(nd.fsm.applied) > len(longest) {
				longest = nd.fsm.applied
			}
		}
		for _, nd := range c.nodes {
			if nd.fsm == nil {
				continue
			}
			if got := nd.fsm.applied; !slices.Equal(got, longest[:len(got)]) {
				return fmt.Errorf("%s applied %d commands that aren't a prefix of the longest (%d)", nd.name, len(got), len(longest))
			}
		}
		seen := map[string]bool{}
		for _, id := range longest {
			if seen[id] {
				return fmt.Errorf("%s applied twice", id)
			}
			seen[id] = true
		}
		if len(longest) == 0 {
			return fmt.Errorf("nothing was applied")
		}
		return nil
	})
	if c.op != nil {
		h.Final("the members agree on the configuration", func(w *dst.World) error { return c.op.agree(w) })
		h.Final("membership changes are linearizable", func(w *dst.World) error { return c.op.linearizable() })
	}
	h.Expect("a write committed", "a read through the log", "a ReadIndex read", "a snapshot was persisted")
}

// recovers waits, once the faults are over, for a leader that a probe
// write commits through: a cluster that keeps a live majority of its
// voters must make progress again.
func (c *simCluster) recovers(w *dst.World) error {
	deadline := w.Now().Add(c.patience())
	for try := 1; ; try++ {
		if l := c.leader(); l != nil {
			// A failed attempt may still commit: each attempt is its own write.
			id := fmt.Sprintf("probe%d", try)
			err := l.raft.Apply([]byte(`{"ID":"`+id+`","Key":"probe","Value":"`+id+`"}`), time.Second).Error()
			if err == nil {
				c.kv.acked[id] = true
				return nil
			}
			if errors.Is(err, ErrLeadershipTransferInProgress) && !bug("transfer-send-stoppable") && w.Now().After(deadline) {
				// Upstream raft does this (6.14, #696: a transfer goroutine
				// stuck on a send keeps the flag set, and the re-elected
				// leader refuses writes). Counted, not failed, unless the
				// fix is on.
				c.h.Sometimes("a re-elected leader refused writes for good (6.14)")
				c.h.Logf("%s refuses writes: %v (6.14)", l.name, err)
				return nil
			}
			if w.Now().After(deadline) {
				return fmt.Errorf("the probe write through %s failed: %v", l.name, err)
			}
		} else if w.Now().After(deadline) {
			return fmt.Errorf("no leader %v after the faults ended:\n%s", c.patience(), c.describe())
		}
		w.Sleep(50 * time.Millisecond)
	}
}

// catchesUp: after the faults (and the recovery probe), every member
// applies what the leader has committed, within patience (phase 2 step
// 2). "The cluster recovers" needs only a leader that commits, and "the
// same sequence" accepts a member stuck on a prefix, so neither sees a
// follower that never catches up (a livelock like #697's snapshot loop,
// or bug 4's).
func (c *simCluster) catchesUp(w *dst.World) error {
	deadline := w.Now().Add(c.patience())
	var target uint64
	for {
		if l := c.leader(); l != nil && target == 0 {
			target = l.raft.getCommitIndex()
		}
		var behind []string
		if target != 0 {
			for _, nd := range c.members() {
				if nd.up() && nd.raft.getState() == Shutdown {
					// A leader demoted or removed shuts down with
					// ShutdownOnRemove (6.3): listed, no longer taking part.
					c.h.Sometimes("a member's raft was shut down")
					continue
				}
				if !nd.up() || nd.raft.getLastApplied() < target {
					behind = append(behind, nd.name)
				}
			}
			if len(behind) == 0 {
				return nil
			}
		}
		if w.Now().After(deadline) {
			if target == 0 {
				return fmt.Errorf("no leader %v after the faults ended:\n%s", c.patience(), c.describe())
			}
			return fmt.Errorf("%v after the faults ended, %v haven't applied the leader's commit index %d:\n%s", c.patience(), behind, target, c.describe())
		}
		w.Sleep(50 * time.Millisecond)
	}
}

// resolvedOps is the clients' history with the unknown writes that never
// took effect removed. It runs after "every member catches up", so the
// nodes' applied sets hold everything committed; an unknown write in none
// of them never happened (the leader that appended it lost it before it
// committed). Removing it can only make the check stricter: were a write
// applied and later lost (a bug), a read that saw it would now have no
// write to explain it. It spares the checker the unknown writes that
// leadership transfers and slow links make common (each one it must try
// at every place, and as never happening).
func (c *simCluster) resolvedOps() []check.Op[check.KVInput, check.KVOutput] {
	applied := map[string]bool{}
	for _, nd := range c.nodes {
		if nd.fsm == nil {
			continue
		}
		for _, id := range nd.fsm.applied {
			applied[id] = true
		}
	}
	ops := c.kv.hist.Ops()
	removed := 0
	ops = slices.DeleteFunc(ops, func(o check.Op[check.KVInput, check.KVOutput]) bool {
		gone := o.Unknown && o.Input.Op == check.KVPut && !applied[o.Input.Value]
		if gone {
			removed++
		}
		return gone
	})
	if removed > 0 {
		c.h.Sometimes("an unknown write never took effect")
	}
	return ops
}

// members are the nodes that are part of the cluster at the end: in the
// leader's committed configuration (scripts change membership without the
// operator too), not lost, and booted.
func (c *simCluster) members() []*dstNode {
	var out []*dstNode
	var cfg *Configuration
	if l := c.leader(); l != nil {
		committed := l.raft.configurations.committed
		cfg = &committed
	}
	for _, nd := range c.nodes {
		if nd.fsm == nil || nd.m.Lost() {
			continue
		}
		if cfg != nil && !inConfiguration(*cfg, nd.id()) {
			continue
		}
		out = append(out, nd)
	}
	return out
}

// describe is each node's state, for failure reports.
func (c *simCluster) describe() string {
	var b strings.Builder
	if c.knobs != nil {
		fmt.Fprintf(&b, "  raft config: %v\n", *c.knobs)
	}
	for _, nd := range c.nodes {
		switch {
		case nd.m.Lost():
			fmt.Fprintf(&b, "  %s: lost\n", nd.name)
		case nd.raft == nil:
			fmt.Fprintf(&b, "  %s: never booted\n", nd.name)
		default:
			r := nd.raft
			last, lastTerm := r.getLastEntry()
			fmt.Fprintf(&b, "  %s: %v term %d, last log %d (term %d), commit %d, applied %d, dead %v\n      latest %v\n      committed %v\n",
				nd.name, r.getState(), r.getCurrentTerm(), last, lastTerm, r.getCommitIndex(), r.getLastApplied(), nd.m.Dead(),
				r.configurations.latest.Servers, r.configurations.committed.Servers)
		}
	}
	return b.String()
}
