//go:build dstbugs

package raft

import (
	"strings"
	"testing"

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
