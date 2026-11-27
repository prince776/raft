//go:build dstbugs

package raft

import (
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
