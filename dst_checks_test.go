package raft

// The checks of a simulated cluster (dst_cluster_test.go): what must hold
// whatever the faults, within raft's fault model.

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
)

// checks registers the cluster's checks: always, election safety and
// leaders being voters; at the end, that the cluster recovers, that the
// clients' history is linearizable, that acknowledged writes survive on
// the cluster's members, that every node applied a prefix of one sequence,
// and, with the operator, that the members agree on the configuration and
// the membership changes are linearizable.
func (c *simCluster) checks() {
	h := c.h
	// Election safety: at most one leader per term, ever (a crashed node's
	// raft keeps its last state, so only running nodes count).
	leaders := map[uint64]string{}
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
	h.Final("the cluster recovers", func(w *dst.World) error { return c.recovers(w) })
	h.Final("the clients' history is linearizable", func(w *dst.World) error {
		return check.Linearizable(check.KVModel(), c.kv.hist.Ops(), check.Options{}).Err()
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
		h.Final("the members agree on the configuration", func(w *dst.World) error { return c.op.agree() })
		h.Final("membership changes are linearizable", func(w *dst.World) error { return c.op.linearizable() })
	}
	h.Expect("a write committed", "a read through the log", "a ReadIndex read", "a snapshot was persisted")
}

// recovers waits, once the faults are over, for a leader that a probe
// write commits through: a cluster that keeps a live majority of its
// voters must make progress again.
func (c *simCluster) recovers(w *dst.World) error {
	deadline := w.Now().Add(5 * time.Second)
	for try := 1; ; try++ {
		if l := c.leader(); l != nil {
			// A failed attempt may still commit: each attempt is its own write.
			id := fmt.Sprintf("probe%d", try)
			err := l.raft.Apply([]byte(`{"ID":"`+id+`","Key":"probe","Value":"`+id+`"}`), time.Second).Error()
			if err == nil {
				c.kv.acked[id] = true
				return nil
			}
			if w.Now().After(deadline) {
				return fmt.Errorf("the probe write through %s failed: %v", l.name, err)
			}
		} else if w.Now().After(deadline) {
			return fmt.Errorf("no leader 5s after the faults ended:\n%s", c.describe())
		}
		w.Sleep(50 * time.Millisecond)
	}
}

// members are the nodes that are part of the cluster at the end: in the
// leader's committed configuration (every node without the operator), not
// lost, and booted.
func (c *simCluster) members() []*dstNode {
	var out []*dstNode
	var cfg *Configuration
	if l := c.leader(); l != nil && c.op != nil {
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
