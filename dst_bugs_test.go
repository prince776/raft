//go:build dstbugs

package raft

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"

	"github.com/prince776/go-dst-kit/dsttest"
)

// Each check of the simulated cluster catches a bug that breaks what it
// checks, and says so in its own words: run with -tags dstbugs.
//
// Verifies: the checks of dstClusterWith can fail.
// Fails if: a re-inserted bug isn't caught by the check meant for it
// within the seeds the benchmark caught it in (dstbench.json, 100 seeds:
// vote-twice from run #4, skip-prev-log-term #33, accept-stale-leader #11).
func TestDSTChecksCatchBugs(t *testing.T) {
	for _, c := range []struct {
		bug, check string
		faults     dsttest.Faults
		nodes      int
	}{
		{"vote-twice", "at most one leader per term", dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true}, 4},
		{"skip-prev-log-term", "the nodes applied the same sequence", dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true}, 5},
		{"accept-stale-leader", "the clients' history is linearizable", dsttest.Faults{Partition: true, Network: true}, 3},
	} {
		t.Run(c.bug, func(t *testing.T) {
			activeBug = c.bug
			defer func() { activeBug = "" }()
			o := dstOpts(c.faults)
			o.Seeds, o.KeepGoing, o.DeterminismChecks = 50, true, -1
			rep := dsttest.Sweep(o, func(h *dsttest.Harness) { dstCluster(h, c.nodes) })
			for _, r := range rep.Failed {
				if strings.Contains(r.Err.Error(), `"`+c.check+`"`) {
					t.Logf("caught by %q: %d of %d seeds failed", c.check, len(rep.Failed), rep.Runs)
					return
				}
			}
			t.Fatalf("%d of %d seeds failed, none by %q", len(rep.Failed), rep.Runs, c.check)
		})
	}
}

// quorumScript plays the one sequence that turns committing on half of an
// even cluster (quorum-off-by-one, upstream 163a55f) into a lost write:
// the cluster splits 2|2 with the leader L and one follower A on one side,
// and under the bug they commit writes on their own (correct raft needs 3
// of 4); L steps down within its lease and both crash; A comes back with
// an empty disk; the network heals; B or C wins an election with A's vote
// (3 of 4, none with the writes) and later overwrites L's log. Without
// amnesia no such election can win (any 3 voters include L or A, who
// refuse a candidate missing the writes), which is why the swarm never
// finds the bug.
func quorumScript(h *dsttest.Harness) {
	nodes := dstCluster(h, 4)
	h.Go(func(w *dst.World) {
		var l *dstNode
		for l == nil {
			w.Sleep(50 * time.Millisecond)
			for _, nd := range nodes {
				if nd.raft != nil && !nd.m.Dead() && nd.raft.State() == Leader {
					l = nd
				}
			}
		}
		w.Sleep(time.Second) // let it write for a while first
		var a *dstNode
		var other []string
		for _, nd := range nodes {
			switch {
			case nd == l:
			case a == nil:
				a = nd
			default:
				other = append(other, nd.name)
			}
		}
		h.Partition([]string{l.name, a.name}, append(other, "test"))
		w.Sleep(80 * time.Millisecond) // within L's lease: the bug commits here
		l.m.Crash()
		a.m.Crash()
		a.wipeOnBoot = true
		// Restarts go through timer callbacks: a machine restarted from
		// this goroutine would count as its work, which must finish.
		w.AfterFunc(0, a.m.Restart)
		h.Heal()
		w.Sleep(3 * time.Second) // B or C is elected, with A's vote
		w.AfterFunc(0, l.m.Restart)
	})
}

// Verifies: quorum-off-by-one is expressible in the simulated world, and
// the black-box checks catch it, given its one sequence (quorumScript).
// Fails if: the scripted sequence passes with the bug in (the checks miss
// a lost write), or fails without it (the script itself breaks correct
// raft, so it proves nothing about the bug).
func TestDSTQuorumOffByOneScripted(t *testing.T) {
	o := dstOpts(dsttest.Faults{})
	o.Seeds, o.KeepGoing, o.DeterminismChecks = 20, true, -1
	activeBug = ""
	if rep := dsttest.Sweep(o, quorumScript); len(rep.Failed) > 0 {
		t.Fatalf("correct raft failed the script on %d of %d seeds: %v", len(rep.Failed), rep.Runs, rep.Failed[0].Err)
	}
	activeBug = "quorum-off-by-one"
	defer func() { activeBug = "" }()
	rep := dsttest.Sweep(o, quorumScript)
	if len(rep.Failed) == 0 {
		t.Fatalf("with the bug, no seed of %d lost a write", rep.Runs)
	}
	kinds := map[string]int{}
	for _, r := range rep.Failed {
		kinds[dsttest.Signature(r.Err)]++
	}
	t.Logf("with the bug: %d of %d seeds failed: %v", len(rep.Failed), rep.Runs, kinds)
}

// staleConfigScript plays a sequence inside raft's fault model (one
// partition, one server removed, one lost for good) that leaves a live
// majority of the voters unable to elect a leader. Three voters {A, B, C},
// A leading, and a spare D:
//
//  1. C is cut off.
//  2. A adds D as a non-voter and, once D has caught up, promotes it:
//     {A, B, C, D}, committed by A, B and D.
//  3. A removes B: {A, C, D}, committed by A and D. (B's raft shuts down.)
//  4. A is lost for good; C and D are two of {A, C, D}'s three voters.
//  5. C is reconnected. Its latest configuration is still {A, B, C}.
//
// D, the most up to date, asks C for a (pre-)vote, and C rejects it: D
// isn't in C's configuration (the candidate checks 6b4e320 added). C's
// own campaigns can't win: in C's configuration, A is gone and B shut
// down. Plain Raft has no such check: C grants D its vote (D's log is
// ahead), D wins, and catches C up.
func staleConfigScript(h *dsttest.Harness) {
	c := newCluster(h, clusterSpec{voters: 3, spares: 1})
	h.Go(func(w *dst.World) {
		var a *dstNode
		for a == nil {
			w.Sleep(50 * time.Millisecond)
			a = c.leader()
		}
		w.Sleep(500 * time.Millisecond)
		var b, cc *dstNode
		for _, nd := range c.nodes[:3] {
			switch {
			case nd == a:
			case b == nil:
				b = nd
			default:
				cc = nd
			}
		}
		d := c.nodes[3]
		step := func(what string, f Future) bool {
			if err := f.Error(); err != nil {
				h.Fail(fmt.Errorf("script: %s: %v", what, err))
				return false
			}
			return true
		}
		h.Partition([]string{cc.name}, []string{a.name, b.name, d.name, "test"})
		if !step("add D", a.raft.AddNonvoter(d.id(), d.addr(), 0, time.Second)) {
			return
		}
		for d.raft.getLastIndex() < a.raft.getCommitIndex() {
			w.Sleep(10 * time.Millisecond)
		}
		if !step("promote D", a.raft.AddVoter(d.id(), d.addr(), 0, time.Second)) ||
			!step("remove B", a.raft.RemoveServer(b.id(), 0, time.Second)) {
			return
		}
		w.Sleep(200 * time.Millisecond) // D learns the commit
		a.m.Lose()
		h.Heal()
		h.Sometimes("the script ran")
	})
	h.Expect("the script ran")
}

// Verifies: hashicorp/raft can lose liveness for good with a live majority
// of its voters (staleConfigScript), and the candidate checks of 6b4e320
// are why.
// Fails if: correct raft recovers from the sequence (no stall to report),
// or it doesn't recover with the candidate checks off (they aren't the
// cause) or with the proposed fix (the checks only when the candidate's
// log isn't ahead: UPSTREAM-ISSUE-DRAFT.md), or the script didn't run.
func TestDSTStaleConfigStall(t *testing.T) {
	o := dstOpts(dsttest.Faults{})
	o.Seeds, o.KeepGoing, o.DeterminismChecks = 20, true, -1
	activeBug = ""
	rep := dsttest.Sweep(o, staleConfigScript)
	stalled := 0
	for _, r := range rep.Failed {
		if !strings.Contains(r.Err.Error(), `"the cluster recovers"`) {
			t.Fatalf("seed %d failed otherwise: %v", r.Seed, r.Err)
		}
		stalled++
	}
	if stalled == 0 {
		t.Fatalf("correct raft recovered on all %d seeds", rep.Runs)
	}
	t.Logf("correct raft: no leader again on %d of %d seeds", stalled, rep.Runs)
	defer func() { activeBug = "" }()
	for _, fix := range []string{"no-candidate-config-check", "candidate-check-unless-ahead"} {
		activeBug = fix
		if rep := dsttest.Sweep(o, staleConfigScript); len(rep.Failed) > 0 {
			t.Fatalf("with %s, %d of %d seeds still failed: %v", fix, len(rep.Failed), rep.Runs, rep.Failed[0].Err)
		}
	}
}
