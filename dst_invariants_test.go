package raft

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// Raft's core invariants (the paper's Figure 3), checked white-box after
// every step (phase 2, DST.md 8). The end-to-end checks (linearizability,
// acknowledged writes, applied sequences) see a violation only once it
// reaches a client or a state machine; these see it where it happens.
//
//   - Log Matching: entries with the same index and term are the same
//     entry and follow the same entry. Checked as each store writes:
//     (index, term) maps to one payload and one previous term, so by
//     induction two logs holding an entry agree on everything before it.
//   - Leader Completeness: a leader holds every entry committed before
//     its term (or a snapshot covering it). Checked when a node is first
//     seen leading a term, against every entry a leader has committed.
//   - Terms never go back: a node's current term never decreases, across
//     crashes and restarts. Upstream breaks it (DST.md 6.13), so it fails
//     a run only with the fix heartbeat-on-main-thread on (the dstbugs
//     sweeps with knownFixes); otherwise it is counted.
//   - One vote per term: a node grants its vote (its own included) to one
//     candidate per term, across crashes and restarts.
//
// A node that lost its disk (wipeOnBoot, outside raft's fault model) starts
// afresh in the last two.
type invariants struct {
	entries   map[[2]uint64]entryFact // (index, term) -> what it is
	committed map[uint64]uint64       // index -> term, as leaders commit
	upTo      uint64                  // committed recorded up to here
	ledTerm   map[string]uint64       // node -> last term checked as leader
	maxTerm   map[string]uint64       // node -> highest term seen
	votes     map[string]map[uint64]string
	logsErr   error // the first Log Matching violation the store hook saw
	voteErr   error // the first double vote the vote hook saw
}

type entryFact struct {
	digest   uint64
	prevTerm uint64 // 0: not known yet
	by       string // the node that stored it first
}

func newInvariants() *invariants {
	return &invariants{
		entries: map[[2]uint64]entryFact{}, committed: map[uint64]uint64{},
		ledTerm: map[string]uint64{}, maxTerm: map[string]uint64{}, votes: map[string]map[uint64]string{},
	}
}

func first(err *error, format string, args ...any) {
	if *err == nil {
		*err = fmt.Errorf(format, args...)
	}
}

// forget: the node lost its disk.
func (iv *invariants) forget(name string) {
	delete(iv.maxTerm, name)
	delete(iv.votes, name)
}

func logDigest(l *Log) uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(l.Type)})
	h.Write(l.Data)
	return h.Sum64()
}

// stored is called after s applied logs it wrote (the store's onStore).
func (iv *invariants) stored(name string, s *dstLogStore, logs []*Log) {
	for _, l := range logs {
		var prev uint64
		if p, ok := s.logs[l.Index-1]; ok {
			prev = p.Term
		}
		key := [2]uint64{l.Index, l.Term}
		f, seen := iv.entries[key]
		if !seen {
			iv.entries[key] = entryFact{digest: logDigest(l), prevTerm: prev, by: name}
			continue
		}
		if logDigest(l) != f.digest {
			first(&iv.logsErr, "%s stored entry %d (term %d) with a different payload than %s did", name, l.Index, l.Term, f.by)
		}
		switch {
		case f.prevTerm == 0:
			f.prevTerm = prev
			iv.entries[key] = f
		case prev != 0 && prev != f.prevTerm:
			first(&iv.logsErr, "%s stored entry %d (term %d) after an entry of term %d; %s has it after term %d", name, l.Index, l.Term, prev, f.by, f.prevTerm)
		}
	}
}

// voted is called when raft grants a vote (Config.dstOnVote).
func (iv *invariants) voted(name string, term uint64, candidate []byte) {
	vs := iv.votes[name]
	if vs == nil {
		vs = map[uint64]string{}
		iv.votes[name] = vs
	}
	if prev, ok := vs[term]; ok && prev != string(candidate) {
		first(&iv.voteErr, "%s voted twice in term %d: for %s and for %s", name, term, prev, candidate)
	}
	vs[term] = string(candidate)
}

func (iv *invariants) terms(c *simCluster) error {
	for _, nd := range c.nodes {
		if !nd.up() {
			continue
		}
		term := nd.raft.getCurrentTerm()
		if term < iv.maxTerm[nd.name] {
			err := fmt.Errorf("%s's term went back from %d to %d", nd.name, iv.maxTerm[nd.name], term)
			if !bug("heartbeat-on-main-thread") && !bug("strict-checks") {
				// Upstream raft does this (DST.md 6.13: a heartbeat on the
				// transport's goroutine steps the node back to its term).
				// Counted, not failed, unless the fix is on.
				c.h.Sometimes("a node's term went back (6.13)")
				c.h.Logf("%v (6.13)", err)
				iv.maxTerm[nd.name] = term
				continue
			}
			return err
		}
		iv.maxTerm[nd.name] = term
	}
	return nil
}

func (iv *invariants) completeness(c *simCluster) error {
	for _, nd := range c.nodes {
		if !nd.up() || nd.raft.getState() != Leader {
			continue
		}
		// Record what this leader has committed.
		ci := nd.raft.getCommitIndex()
		for i := iv.upTo + 1; i <= ci; i++ {
			t, has, _ := nd.logs.durableAt(i)
			if !has {
				continue // compacted here already
			}
			iv.committed[i] = t
		}
		if ci > iv.upTo {
			iv.upTo = ci
		}
		term := nd.raft.getCurrentTerm()
		if iv.ledTerm[nd.name] == term {
			continue
		}
		iv.ledTerm[nd.name] = term
		var missing []uint64
		for i, t := range iv.committed {
			lt, has, compacted := nd.logs.durableAt(i)
			if compacted || nd.snaps.covers(i) || (has && lt == t) {
				continue
			}
			missing = append(missing, i)
		}
		if len(missing) > 0 {
			sort.Slice(missing, func(a, b int) bool { return missing[a] < missing[b] })
			i := missing[0]
			return fmt.Errorf("%s leads term %d without %d committed entries, the first %d (term %d)", nd.name, term, len(missing), i, iv.committed[i])
		}
	}
	return nil
}

// invariantChecks registers one check per invariant.
func (c *simCluster) invariantChecks() {
	iv := c.inv
	// After a manual recovery only the recovered server's log and logs
	// copied from it remain (the other survivors rejoin wiped; the lost
	// never write again), and it may reuse a lost server's term.
	c.onManualRecovery = append(c.onManualRecovery, func() { clear(iv.entries) })
	c.h.Always("logs match", func() error { return iv.logsErr })
	c.h.Always("one vote per term", func() error { return iv.voteErr })
	c.h.Always("terms never go back", func() error { return iv.terms(c) })
	c.h.Always("leaders hold every committed entry", func() error { return iv.completeness(c) })
}
