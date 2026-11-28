//go:build dstbugs

package raft

// TestDSTPlants is TestDSTChecksCatchBugs on go-dst-kit's dsttest.Plants:
// every known fix on ($DSTFIX), a sweep with no plant that must pass, and
// per plant a sweep that must fail, every failing seed by one of the
// plant's own checks (a failure for another reason, a switch never
// reached, or a check name the setup doesn't register fails it).

import (
	"testing"

	"github.com/prince776/go-dst-kit/dsttest"
)

func TestDSTPlants(t *testing.T) {
	t.Setenv("DSTFIX", knownFixes)
	t.Setenv("DSTBUG", "")
	base := dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true}
	o := dstOpts(base)
	o.Seeds, o.KeepGoing, o.DeterminismChecks = 20, true, -1
	shape := func(f dsttest.Faults, nodes int) (func(*dsttest.Options), func(*dsttest.Harness)) {
		return func(o *dsttest.Options) { o.Faults = f }, func(h *dsttest.Harness) { dstCluster(h, nodes) }
	}
	plant := func(bug string, checks []string, f dsttest.Faults, nodes int, why string) dsttest.Plant {
		opt, setup := shape(f, nodes)
		return dsttest.Plant{Bug: bug, Checks: checks, Why: why, Options: opt, Setup: setup}
	}
	withPause := base
	withPause.Pause = true
	dsttest.Plants(t, o, func(h *dsttest.Harness) { dstCluster(h, 5) }, []dsttest.Plant{
		plant("vote-twice", []string{"one vote per term"}, base, 4, "a node votes twice in a term"),
		plant("skip-prev-log-term", []string{"logs match"}, withPause, 5, "appends skip the previous-entry term check"),
		plant("vote-ignores-log", []string{"leaders hold every committed entry"}, base, 5, "votes ignore the candidate's log"),
		plant("follower-ignores-commit", []string{"every member catches up"}, dsttest.Faults{Crash: true}, 3, "a follower never applies"),
		plant("transfer-flag-sticks", []string{"the cluster recovers"}, dsttest.Faults{Crash: true, Partition: true}, 3, "a leadership-transfer flag that sticks (#696's class)"),
		plant("forget-term-on-restart", []string{"terms never go back"}, dsttest.Faults{Crash: true, Disk: true}, 3, "a restarted node forgets its term"),
		plant("accept-stale-leader", []string{"leaders hold every committed entry", "the clients' history is linearizable", "acknowledged writes survive", "the nodes applied the same sequence",
			"terms never go back"}, // the stale leader's term, taken from its appends
			dsttest.Faults{Partition: true, Network: true}, 3, "a follower accepts appends from a stale leader"),
	})
}
