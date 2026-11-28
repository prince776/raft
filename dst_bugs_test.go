//go:build dstbugs

package raft

import (
	"errors"
	"fmt"
	"slices"
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
// Fails if: a re-inserted bug isn't caught by a check meant for it within
// the seeds the benchmark caught it in (dstbench.json, 100 seeds:
// vote-twice from run #4, skip-prev-log-term #33, accept-stale-leader #11).
// A follower accepting a stale leader's appends diverges logs: since the
// client service and file snapshots changed the timing, it shows mostly as
// lost acknowledged writes and diverged sequences (and runs that never
// finish), as a non-linearizable history on 1 of 200 seeds; the
// linearizability check has its own planted bug (TestDSTCatchesNaiveReadIndex).
func TestDSTChecksCatchBugs(t *testing.T) {
	for _, c := range []struct {
		bug    string
		checks []string // any of them
		faults dsttest.Faults
		nodes  int
	}{
		// One vote per term sees a second vote before a second leader.
		{"vote-twice", []string{"one vote per term"}, dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true}, 4},
		{"skip-prev-log-term", []string{"logs match"}, dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true}, 5},
		{"vote-ignores-log", []string{"leaders hold every committed entry"}, dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true}, 5},
		// Step 2: a follower that never applies is seen only here.
		{"follower-ignores-commit", []string{"every member catches up"}, dsttest.Faults{Crash: true}, 3},
		// Step 3: a transfer flag that sticks (#696's class) leaves a leader
		// refusing writes; seeds with "leadership transfers" on.
		{"transfer-flag-sticks,transfer-send-stoppable", []string{"the cluster recovers"}, dsttest.Faults{Crash: true, Partition: true}, 3},
		// The term check is strict only with 6.13's fix on.
		{"forget-term-on-restart,heartbeat-on-main-thread", []string{"terms never go back"}, dsttest.Faults{Crash: true, Disk: true}, 3},
		{"accept-stale-leader", []string{"leaders hold every committed entry", "the clients' history is linearizable", "acknowledged writes survive", "the nodes applied the same sequence"},
			dsttest.Faults{Partition: true, Network: true}, 3},
	} {
		t.Run(c.bug, func(t *testing.T) {
			activeBug = c.bug
			defer func() { activeBug = "" }()
			// Ten seeds at a time, stopping once a check meant for the bug
			// caught it (most do within the first ten): up to 50 seeds.
			caught, failed, runs, firstErr := map[string]int{}, 0, 0, error(nil)
			for first := uint64(1); first <= 50 && len(caught) == 0; first += 10 {
				o := dstOpts(c.faults)
				o.FirstSeed, o.Seeds, o.KeepGoing, o.DeterminismChecks = first, 10, true, -1
				rep := dsttest.Sweep(o, func(h *dsttest.Harness) { dstCluster(h, c.nodes) })
				failed, runs = failed+len(rep.Failed), runs+rep.Runs
				for _, r := range rep.Failed {
					if firstErr == nil {
						firstErr = r.Err
					}
					for _, check := range c.checks {
						if strings.Contains(r.Err.Error(), `"`+check+`"`) {
							caught[check]++
						}
					}
				}
			}
			if len(caught) == 0 {
				t.Fatalf("%d of %d seeds failed, none by %q; the first: %.300v", failed, runs, c.checks, firstErr)
			}
			t.Logf("%d of %d seeds failed; caught by %v", failed, runs, caught)
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
	nodes := newCluster(h, clusterSpec{voters: 4, noTransfers: true}).nodes
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
	c := newCluster(h, clusterSpec{voters: 3, spares: 1, noTransfers: true})
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

// failingLogs fails its first StoreLog: a crash after the call before it
// (fsynced on return) and before this one.
type failingLogs struct {
	*InmemStore
	failed bool
}

func (s *failingLogs) StoreLog(l *Log) error {
	if !s.failed {
		s.failed = true
		return fmt.Errorf("crash")
	}
	return s.InmemStore.StoreLog(l)
}

// TestBootstrapCrash: BootstrapCluster saves term 1, then appends the
// configuration entry. A crash between the two leaves a node that
// HasExistingState says has state, so it can't bootstrap again, and that
// has no configuration, so it can never elect anyone: a single-voter
// cluster is stuck for good, and the node needs its data wiped. Found by
// TestDSTMembershipGray (a slow disk widened the window).
//
// Fails if: correct raft can bootstrap again after that crash (no bug to
// report), or with the fix (bootstrap-log-first: the configuration entry
// first, then the term) it can't.
func TestBootstrapCrash(t *testing.T) {
	try := func() (again error, hasState bool) {
		store := &failingLogs{InmemStore: NewInmemStore()}
		conf := DefaultConfig()
		conf.LocalID = "a"
		_, trans := NewInmemTransport("a")
		cfg := Configuration{Servers: []Server{{ID: "a", Address: trans.LocalAddr()}}}
		if err := BootstrapCluster(conf, store, store.InmemStore, NewInmemSnapshotStore(), trans, cfg); err == nil {
			t.Fatal("the injected crash didn't happen")
		}
		hasState, _ = HasExistingState(store, store.InmemStore, NewInmemSnapshotStore())
		return BootstrapCluster(conf, store, store.InmemStore, NewInmemSnapshotStore(), trans, cfg), hasState
	}
	defer func() { activeBug = "" }()
	activeBug = ""
	if again, hasState := try(); !errors.Is(again, ErrCantBootstrap) || !hasState {
		t.Fatalf("correct raft: bootstrapping again gave %v (has state %v); want ErrCantBootstrap", again, hasState)
	}
	activeBug = "bootstrap-log-first"
	if again, hasState := try(); again != nil || hasState {
		t.Fatalf("with bootstrap-log-first: bootstrapping again gave %v (has state %v); want success", again, hasState)
	}
}

// TestDSTMembershipGrayFixed: the membership sweep under gray failure,
// with the proposed fixes on: the stale-configuration stall (6.1), the
// bootstrap crash (6.2) and the snapshot boundary (6.11, here with
// TrailingLogs 4). On correct raft it fails on a few seeds, so the default
// build can't carry it as a regression sweep; with the fixes it must pass
// every seed, which also shows those are the only causes.
//
// Fails if: any seed fails with knownFixes on.
func TestDSTMembershipGrayFixed(t *testing.T) {
	activeBug = knownFixes
	defer func() { activeBug = "" }()
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Loss: true, Gray: true}),
		func(h *dsttest.Harness) { membershipClusterWith(h, true, true) })
}

// TestDSTDiskErrorsFixed: TestDSTDiskErrors with snapshots retained drawn
// (1 or 2) and the proposed fixes on: an unreadable newest snapshot no
// longer makes raft panic at startup (6.12).
//
// Fails if: any seed fails with knownFixes on.
func TestDSTDiskErrorsFixed(t *testing.T) {
	activeBug = knownFixes
	defer func() { activeBug = "" }()
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true, DiskErrors: true}),
		func(h *dsttest.Harness) { sweepCluster(h, 5) })
}

// knownFixes are the proposed fixes for the raft bugs found so far.
const knownFixes = "candidate-check-unless-ahead,bootstrap-log-first,snapshot-boundary-fix,snapshot-fallback-needs-log,heartbeat-on-main-thread,transfer-send-stoppable,pipeline-close-on-abort,snapshot-install-rule7,config-only-if-stored"

// TestDSTMembershipFixed: the membership sweep with servers lost for good
// and replaced, with the proposed fixes on. Correct raft stalls on some
// seeds (6.1), so the default build's TestDSTMembership leaves losses
// out; with the fixes every seed must pass.
//
// Fails if: any seed fails with knownFixes on.
func TestDSTMembershipFixed(t *testing.T) {
	activeBug = knownFixes
	defer func() { activeBug = "" }()
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Loss: true}),
		func(h *dsttest.Harness) { membershipCluster(h, true) })
}

// TestDSTSnapshotBoundary: with TrailingLogs 0 a follower's snapshot
// leaves its log holding only entries past it, and raft then mishandles
// the boundary (DST.md 6.11): an append following the follower's snapshot
// index fails, the leader backs off and sends an older snapshot, the
// follower installs it (its applied state goes backwards), loops on
// snapshot installs, and panics with "log not found". The fix
// (snapshot-boundary-fix): a previous entry at or below the follower's
// snapshot is committed, so it matches; entries there are skipped; an
// older snapshot is acknowledged without being installed.
//
// Correct raft fails on few seeds (2 of 400 when found, none since
// snapshots moved to the simulated disk), so showing the bug is left to
// TestSnapshotBoundaryScripted; this sweep is the fix's regression test.
//
// Fails if: any seed fails with the fix.
func TestDSTSnapshotBoundary(t *testing.T) {
	defer func() { activeBug = "" }()
	sweep := func(bug string) dsttest.Report {
		activeBug = bug
		o := dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true})
		o.Seeds, o.KeepGoing, o.DeterminismChecks = 400, true, -1
		return dsttest.Sweep(o, func(h *dsttest.Harness) {
			newCluster(h, clusterSpec{voters: 5, drawConfig: true, forceTrailing: true, forceTrailingLogs: 0})
		})
	}
	// With every known fix (the boundary fix among them): the other
	// upstream bugs show in this sweep too since transfers joined it.
	if rep := sweep(knownFixes); len(rep.Failed) > 0 {
		t.Errorf("with the fix, %d of %d seeds failed: %v", len(rep.Failed), rep.Runs, rep.Failed[0].Err)
	}
}

// TestDSTCatchesEntryAppliedTwice: the FSM's check that it sees each log
// index once, in order. Clients' retries are deduplicated by ID, so a raft
// that hands the FSM an entry twice would otherwise look like a retry.
//
// Fails if: with the bug (every 50th entry handed to the FSM twice), no
// seed fails by that check.
func TestDSTCatchesEntryAppliedTwice(t *testing.T) {
	activeBug = "apply-entry-twice"
	defer func() { activeBug = "" }()
	o := dstOpts(dsttest.Faults{})
	o.Seeds, o.KeepGoing, o.DeterminismChecks = 20, true, -1
	rep := dsttest.Sweep(o, func(h *dsttest.Harness) { sweepCluster(h, 3) })
	caught := 0
	for _, r := range rep.Failed {
		if strings.Contains(r.Err.Error(), "an entry applied twice or out of order") {
			caught++
		}
	}
	if caught == 0 {
		t.Fatalf("%d of %d seeds failed, none by the FSM's index check", len(rep.Failed), rep.Runs)
	}
	t.Logf("caught on %d of %d seeds", caught, rep.Runs)
}

// manualRecoveryScript (phase 2 step 5): an even cluster (4 or 6 voters)
// runs its workload under crashes and network faults; midway, half of it
// is lost for good, so the survivors can't elect a leader, and an operator
// recovers by raft's safe procedure (RecoverCluster's doc): the most up to
// date survivor (by last term, then index) is recovered as the sole voter
// (peers.json in Consul and Nomad), and the other survivors rejoin with
// clean disks. Recovering every survivor from its own log, the "typical"
// way, commits each one's uncommitted tail and can diverge them, as the
// doc warns (the first version of this script did, on 1 of 50 seeds).
//
// Losing half of an even cluster, every committed entry is on a quorum
// (n/2+1), and the most up to date survivor holds all of them (a later
// leader's log extends every committed entry), so every acknowledged
// write must survive.
func manualRecoveryScript(noWhiteBox bool) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		n := []int{4, 6}[h.World().Rand().Stream("cluster size").IntN(2)]
		c := newCluster(h, clusterSpec{voters: n, drawConfig: true, clients: 2, noWhiteBox: noWhiteBox})
		h.Go(func(w *dst.World) {
			w.Sleep(time.Duration(1500+w.Rand().IntN(1500)) * time.Millisecond)
			// The outage: the cluster splits in half, the leader's half with
			// the clients, and then that half is lost for good (a site
			// lost). Correct raft can't commit on half; every write the
			// leader's half acknowledges is on a quorum, so on a survivor.
			var l *dstNode
			for l == nil {
				l = c.leader()
				if l == nil {
					w.Sleep(20 * time.Millisecond)
				}
			}
			lostSide := []*dstNode{l}
			for _, i := range w.Rand().Perm(n) {
				if nd := c.nodes[i]; nd != l && len(lostSide) < n/2 {
					lostSide = append(lostSide, nd)
				}
			}
			var a, b []string
			var survivors []*dstNode
			for _, m := range h.Machines() {
				a = append(a, m.Name())
			}
			a = slices.DeleteFunc(a, func(name string) bool {
				nd := c.node(ServerID(name))
				if nd != nil && !slices.Contains(lostSide, nd) {
					b = append(b, name)
					survivors = append(survivors, nd)
					return true
				}
				return false
			})
			h.Partition(a, append(b, "test"))
			w.Sleep(time.Duration(200+w.Rand().IntN(600)) * time.Millisecond)
			for _, nd := range lostSide {
				nd.m.Lose()
			}
			h.Heal()
			best := survivors[0]
			for _, nd := range survivors[1:] {
				i, t := nd.raft.getLastEntry()
				bi, bt := best.raft.getLastEntry()
				if t > bt || (t == bt && i > bi) {
					best = nd
				}
			}
			c.recover = map[string]Configuration{best.name: {Servers: []Server{{ID: best.id(), Address: best.addr()}}}}
			for _, nd := range survivors {
				nd.wipeOnBoot = nd != best
				if !nd.m.Dead() {
					nd.m.Crash()
				}
				nd.m.Restart()
			}
			// Rejoin the others once the recovered server leads.
			deadline := w.Now().Add(2 * c.patience())
			for _, nd := range survivors {
				if nd == best {
					continue
				}
				for {
					if w.Now().After(deadline) {
						h.Fail(fmt.Errorf("script: %s couldn't rejoin through %s:\n%s", nd.name, best.name, c.describe()))
						return
					}
					if best.up() && best.raft.State() == Leader {
						if err := best.raft.AddVoter(nd.id(), nd.addr(), 0, time.Second).Error(); err == nil {
							break
						}
					}
					w.Sleep(50 * time.Millisecond)
				}
			}
			h.Sometimes("the script ran")
		})
		h.Expect("the script ran", "a server was recovered by hand")
	}
}

// TestDSTManualRecovery (phase 2 step 5): manual outage recovery keeps
// every acknowledged write when half of an even cluster is lost, and
// quorum-off-by-one (upstream 163a55f: committing on half of an even
// configuration) loses some, seen end to end, inside raft's fault model.
// The white-box checks (the commit rule, 6.10, and Raft's invariants)
// catch that bug first, so the bug's run leaves them out to show the
// end-to-end view.
//
// Fails if: correct raft loses an acknowledged write or fails otherwise
// on any seed, or quorum-off-by-one loses none on any seed.
func TestDSTManualRecovery(t *testing.T) {
	defer func() { activeBug = "" }()
	o := dstOpts(dsttest.Faults{Crash: true, Network: true})
	o.Seeds, o.KeepGoing, o.DeterminismChecks = 50, true, -1
	activeBug = ""
	if rep := dsttest.Sweep(o, manualRecoveryScript(false)); !rep.OK() {
		t.Fatalf("correct raft: %d of %d seeds failed; the first: %v", len(rep.Failed), rep.Runs, rep.Failed[0].Err)
	} else {
		t.Logf("correct raft: %d of %d seeds pass", rep.Runs, rep.Runs)
	}
	activeBug = "quorum-off-by-one"
	rep := dsttest.Sweep(o, manualRecoveryScript(true))
	lost := 0
	for _, r := range rep.Failed {
		if strings.Contains(r.Err.Error(), `"acknowledged writes survive"`) || strings.Contains(r.Err.Error(), `"the clients' history is linearizable"`) {
			lost++
		}
	}
	if lost == 0 {
		first := "none"
		if len(rep.Failed) > 0 {
			first = rep.Failed[0].Err.Error()
		}
		t.Fatalf("quorum-off-by-one: no seed lost an acknowledged write (%d of %d failed otherwise; the first: %.300s)", len(rep.Failed), rep.Runs, first)
	}
	t.Logf("quorum-off-by-one: %d of %d seeds lost acknowledged writes or broke linearizability (%d failed in all)", lost, rep.Runs, len(rep.Failed))
}
