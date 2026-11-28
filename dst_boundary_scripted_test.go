//go:build dstbugs

package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// The snapshot boundary (DST.md 6.11), scripted with raft's own test
// helpers (the real World, in-memory transport), as on branch
// fix/snapshot-boundary of the fork for upstream. Each case returns what
// went wrong, so TestSnapshotBoundaryScripted can require correct raft to
// fail and the fix to pass.

// snapshotBoundaryCluster makes a 3-server cluster with TrailingLogs 0,
// applies n entries, and returns the leader, a follower, and an inmem
// transport connected to the follower (to send it RPCs directly).
func snapshotBoundaryCluster(t *testing.T, n int) (*cluster, *Raft, *Raft, *InmemTransport) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 0
	c := MakeCluster(3, t, conf)
	leader := c.Leader()
	for i := 0; i < n; i++ {
		if err := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0).Error(); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	c.WaitForReplication(n)
	follower := c.Followers()[0]
	_, trans := NewInmemTransport("")
	trans.Connect(follower.localAddr, c.trans[c.IndexOf(follower)])
	return c, leader, follower, trans
}

// boundaryAppendProblems: a follower that took a snapshot at S and then
// received S+1 holds only S+1 (TrailingLogs 0). An AppendEntries whose
// previous entry is S, or below S with entries crossing it, must be
// accepted: entries at or below a snapshot are committed.
func boundaryAppendProblems(t *testing.T) (problems []string) {
	c, leader, follower, trans := snapshotBoundaryCluster(t, 10)
	defer c.Close()
	if err := follower.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	snapIdx, snapTerm := follower.getLastSnapshot()
	if err := leader.Apply([]byte("after"), 0).Error(); err != nil {
		t.Fatalf("apply: %v", err)
	}
	c.WaitForReplication(11)
	if last, _ := follower.getLastLog(); last <= snapIdx {
		t.Fatalf("test setup: follower's last log %d isn't past its snapshot %d", last, snapIdx)
	}
	if first, _ := follower.logs.FirstIndex(); first <= snapIdx {
		t.Fatalf("test setup: follower still holds entry %d (first index %d)", snapIdx, first)
	}
	var entries []*Log
	for idx := snapIdx; idx <= snapIdx+1; idx++ {
		var l Log
		if err := leader.logs.GetLog(idx, &l); err != nil {
			t.Fatalf("leader's log %d: %v", idx, err)
		}
		entries = append(entries, &l)
	}
	for _, tc := range []struct {
		name    string
		prev    uint64
		entries []*Log
	}{
		{"previous entry is the snapshot's last", snapIdx, nil},
		{"previous entry inside the snapshot, with entries crossing it", snapIdx - 1, entries},
	} {
		prevTerm := snapTerm
		if tc.prev != snapIdx {
			var l Log
			if err := leader.logs.GetLog(tc.prev, &l); err != nil {
				t.Fatalf("leader's log %d: %v", tc.prev, err)
			}
			prevTerm = l.Term
		}
		req := AppendEntriesRequest{
			RPCHeader:         leader.getRPCHeader(),
			Term:              leader.getCurrentTerm(),
			PrevLogEntry:      tc.prev,
			PrevLogTerm:       prevTerm,
			Entries:           tc.entries,
			LeaderCommitIndex: leader.getCommitIndex(),
		}
		var resp AppendEntriesResponse
		if err := trans.AppendEntries(follower.localID, follower.localAddr, &req, &resp); err != nil {
			t.Fatalf("%s: AppendEntries: %v", tc.name, err)
		}
		if !resp.Success {
			first, _ := follower.logs.FirstIndex()
			problems = append(problems, fmt.Sprintf("%s: rejected (follower snapshot %d, first log %d)", tc.name, snapIdx, first))
		}
	}
	return problems
}

// boundaryInstallProblems: a follower that already holds a snapshot's last
// entry must not install the snapshot (Figure 13, rule 6): installing it
// moved its state machine back in time.
func boundaryInstallProblems(t *testing.T) (problems []string) {
	c, leader, follower, trans := snapshotBoundaryCluster(t, 10)
	defer c.Close()
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatalf("leader snapshot: %v", err)
	}
	for i := 10; i < 20; i++ {
		if err := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0).Error(); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	c.WaitForReplication(20)
	appliedBefore := follower.getLastApplied()
	fsm := getMockFSM(c.fsms[c.IndexOf(follower)])
	fsmBefore := len(fsm.Logs())
	snaps, err := leader.snapshots.List()
	if err != nil || len(snaps) == 0 {
		t.Fatalf("leader has no snapshot: %v", err)
	}
	meta, rc, err := leader.snapshots.Open(snaps[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if meta.Index >= appliedBefore {
		t.Fatalf("test setup: snapshot %d isn't older than the follower's applied %d", meta.Index, appliedBefore)
	}
	req := InstallSnapshotRequest{
		RPCHeader:          leader.getRPCHeader(),
		SnapshotVersion:    meta.Version,
		Term:               leader.getCurrentTerm(),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Configuration:      EncodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
		Size:               meta.Size,
	}
	var resp InstallSnapshotResponse
	if err := trans.InstallSnapshot(follower.localID, follower.localAddr, &req, &resp, rc); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if !resp.Success {
		problems = append(problems, "an old snapshot wasn't acknowledged")
	}
	if got := follower.getLastApplied(); got < appliedBefore {
		problems = append(problems, fmt.Sprintf("applied index went back from %d to %d", appliedBefore, got))
	}
	if got := len(fsm.Logs()); got < fsmBefore {
		problems = append(problems, fmt.Sprintf("the state machine went back from %d to %d entries", fsmBefore, got))
	}
	return problems
}

// TestSnapshotBoundaryScripted: both boundary cases, on correct raft and
// with snapshot-boundary-fix. The sweep (TestDSTSnapshotBoundary) hits the
// bug on few seeds, and on none since snapshots moved to the simulated
// disk; these hit it on every run.
//
// Fails if: correct raft passes either case (nothing left to show), or
// the fix fails either.
func TestSnapshotBoundaryScripted(t *testing.T) {
	defer setSwitches("")
	for _, tc := range []struct {
		name string
		run  func(*testing.T) []string
	}{{"append after the follower's snapshot", boundaryAppendProblems}, {"older snapshot installed", boundaryInstallProblems}} {
		t.Run(tc.name, func(t *testing.T) {
			setSwitches("")
			if p := tc.run(t); len(p) == 0 {
				t.Errorf("correct raft passed")
			} else {
				t.Logf("correct raft: %v", p)
			}
			setSwitches("snapshot-boundary-fix")
			if p := tc.run(t); len(p) > 0 {
				t.Errorf("with the fix: %v", p)
			}
		})
	}
}

// TestSnapshotFallbackGap (DST.md 6.12): a node keeps two snapshots and,
// with TrailingLogs 0, a log compacted past the older one. If the newest
// can't be read at startup (an I/O error, or corruption), restoreSnapshot
// falls back to the older one, and NewRaft then panics replaying the log
// entries the newest snapshot had compacted away.
//
// Fails if: correct raft doesn't panic (nothing left to show), or with
// snapshot-fallback-needs-log NewRaft panics instead of returning an error.
func TestSnapshotFallbackGap(t *testing.T) {
	defer setSwitches("")
	start := func(t *testing.T) (panicked any, err error) {
		dir := t.TempDir()
		conf := inmemConfig(t)
		conf.TrailingLogs = 0
		store := NewInmemStore()
		snaps, serr := NewFileSnapshotStore(dir, 2, nil)
		if serr != nil {
			t.Fatal(serr)
		}
		addr, trans := NewInmemTransport("")
		conf.LocalID = ServerID(addr)
		if err := BootstrapCluster(conf, store, store, snaps, trans, Configuration{Servers: []Server{{ID: conf.LocalID, Address: addr}}}); err != nil {
			t.Fatal(err)
		}
		r, err := NewRaft(conf, &MockFSM{}, store, store, snaps, trans)
		if err != nil {
			t.Fatal(err)
		}
		waitForLeader := func() {
			for i := 0; i < 200 && r.State() != Leader; i++ {
				time.Sleep(10 * time.Millisecond)
			}
		}
		waitForLeader()
		for round := 0; round < 2; round++ {
			for i := 0; i < 10; i++ {
				if err := r.Apply([]byte(fmt.Sprintf("r%d-%d", round, i)), time.Second).Error(); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.Snapshot().Error(); err != nil {
				t.Fatal(err)
			}
		}
		// One entry after the newest snapshot: the log is just that.
		if err := r.Apply([]byte("after"), time.Second).Error(); err != nil {
			t.Fatal(err)
		}
		if err := r.Shutdown().Error(); err != nil {
			t.Fatal(err)
		}
		list, _ := snaps.List()
		if len(list) != 2 {
			t.Fatalf("test setup: %d snapshots, want 2", len(list))
		}
		if first, _ := store.FirstIndex(); first <= list[1].Index+1 {
			t.Fatalf("test setup: the log (first index %d) still follows the older snapshot %d", first, list[1].Index)
		}
		// The newest snapshot can't be read now.
		if err := os.Remove(filepath.Join(dir, snapPath, list[0].ID, stateFilePath)); err != nil {
			t.Fatal(err)
		}
		_, trans2 := NewInmemTransport(addr)
		defer func() { panicked = recover() }()
		r2, err := NewRaft(conf, &MockFSM{}, store, store, snaps, trans2)
		if err == nil {
			r2.Shutdown().Error()
		}
		return nil, err
	}
	setSwitches("")
	if p, err := start(t); p == nil {
		t.Errorf("correct raft didn't panic (err %v)", err)
	} else {
		t.Logf("correct raft panicked: %v", p)
	}
	setSwitches("snapshot-fallback-needs-log")
	if p, err := start(t); p != nil {
		t.Errorf("with the fix, NewRaft panicked: %v", p)
	} else if err == nil {
		t.Errorf("with the fix, NewRaft started from a snapshot the log doesn't follow")
	} else {
		t.Logf("with the fix: %v", err)
	}
}

// removedCampaignScript (phase 2 step 4, upstream #524): a voter E is cut
// off and removed (or demoted) without hearing of it, campaigns alone
// (its term rising, without pre-vote), and comes back just as the
// leader crashes, so the members hold an election while E campaigns. The
// members must not take E's term: with "we have a leader" a healthy
// cluster ignores E anyway (measured: no seed disrupted even with the
// checks removed), so only an election shows what the checks protect. The checks 6b4e320 added
// (#526) reject a candidate outside the receiver's configuration; bug 1's
// fix (candidate-check-unless-ahead) relaxes them when the candidate's log
// is ahead, which E's, having missed the change, isn't.
func removedCampaignScript(preVoteOff, demote bool) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		c := newCluster(h, clusterSpec{voters: 5, noTransfers: true, preVoteOff: preVoteOff})
		h.Go(func(w *dst.World) {
			var l *dstNode
			for l == nil {
				w.Sleep(50 * time.Millisecond)
				l = c.leader()
			}
			w.Sleep(300 * time.Millisecond)
			var e *dstNode
			for _, nd := range c.nodes {
				if nd != l {
					e = nd
					break
				}
			}
			var others []string
			for _, nd := range c.nodes {
				if nd != e {
					others = append(others, nd.name)
				}
			}
			h.Partition([]string{e.name}, append(others, "test"))
			var f Future
			if demote {
				f = l.raft.DemoteVoter(e.id(), 0, time.Second)
			} else {
				f = l.raft.RemoveServer(e.id(), 0, time.Second)
			}
			if err := f.Error(); err != nil {
				h.Fail(fmt.Errorf("script: remove %s: %v", e.name, err))
				return
			}
			w.Sleep(time.Duration(2000+w.Rand().IntN(2000)) * time.Millisecond)
			base := l.raft.getCurrentTerm()
			eTerm := e.raft.getCurrentTerm()
			inflated := eTerm >= base+3
			h.Heal()
			l.m.Crash()
			start := w.Now()
			restarted := false
			for w.Now().Sub(start) < 3*time.Second {
				w.Sleep(20 * time.Millisecond)
				if !restarted && w.Now().Sub(start) > 300*time.Millisecond {
					l.m.Restart()
					restarted = true
				}
				for _, nd := range c.nodes {
					if nd == e || !nd.up() || demote {
						continue
					}
					if t := nd.raft.getCurrentTerm(); inflated && t >= eTerm {
						h.Fail(fmt.Errorf("the %s server %s (term %d; the cluster's %d) disrupted the cluster: %s took term %d in the election after the heal", map[bool]string{true: "demoted", false: "removed"}[demote], e.name, eTerm, base, nd.name, t))
						return
					}
				}
			}
			if demote {
				// A demoted voter's higher term is adopted on purpose
				// (requestVote: "step down and update term, but reject the
				// vote", #526), once, so the cluster can replicate to it.
				// What must hold: then a stable leader among the voters, and
				// the demoted server caught up as a non-voter.
				var nl *dstNode
				var lt uint64
				for w.Now().Sub(start) < 6*time.Second {
					w.Sleep(20 * time.Millisecond)
					if x := c.leader(); x != nil && x != e {
						if t := x.raft.getCurrentTerm(); x != nl || t != lt {
							nl, lt = x, t
							start2 := w.Now()
							stable := true
							for w.Now().Sub(start2) < 2*time.Second && stable {
								w.Sleep(20 * time.Millisecond)
								for _, nd := range c.nodes {
									if nd != e && nd.up() && nd.raft.getCurrentTerm() != lt {
										stable = false
									}
								}
							}
							if stable {
								break
							}
						}
					}
				}
				if nl == nil || w.Now().Sub(start) >= 6*time.Second {
					h.Fail(fmt.Errorf("the demoted server %s (term %d; the cluster's %d) disrupted the cluster: no leader held its term for 2s within 6s of the heal:\n%s", e.name, eTerm, base, c.describe()))
					return
				}
				if e.raft.getCurrentTerm() > lt || e.raft.getLastIndex() < nl.raft.getCommitIndex() {
					// Give it a moment to catch up.
					w.Sleep(500 * time.Millisecond)
					if e.raft.getCurrentTerm() > nl.raft.getCurrentTerm() || e.raft.getLastIndex() < nl.raft.getCommitIndex() {
						h.Fail(fmt.Errorf("the demoted server %s disrupted the cluster: it never rejoined as a non-voter (term %d against the leader's %d, last log %d against commit %d)", e.name, e.raft.getCurrentTerm(), nl.raft.getCurrentTerm(), e.raft.getLastIndex(), nl.raft.getCommitIndex()))
						return
					}
				}
			}
			if inflated {
				h.Sometimes("the removed server came back with a higher term")
			}
			h.Sometimes("the script ran")
		})
		h.Expect("the script ran")
	}
}

// TestRemovedServerCampaigns (phase 2 step 4): measures, over seeds, that a
// server removed without its participation doesn't disrupt the cluster
// when it comes back (#524): with correct raft, with or without pre-vote;
// and with bug 1's proposed fix. With the #526 checks removed
// (no-candidate-config-check) it reports whether anything else still
// protects the cluster.
//
// Fails if: correct raft or the fix lets the server disrupt the cluster,
// or the script never ran, or the server never came back with a higher
// term without pre-vote (the scenario wouldn't test anything).
func TestRemovedServerCampaigns(t *testing.T) {
	defer setSwitches("")
	o := dstOpts(dsttest.Faults{})
	o.Seeds, o.KeepGoing, o.DeterminismChecks = 20, true, -1
	for _, c := range []struct {
		name               string
		bug                string
		preVoteOff, demote bool
		want               string // "pass", "fail" (must be disrupted on some seed), or "" (reported)
	}{
		{"removed, correct, pre-vote off", "", true, false, "pass"},
		{"removed, correct, pre-vote on", "", false, false, "pass"},
		{"removed, bug 1's fix, pre-vote off", "candidate-check-unless-ahead", true, false, "pass"},
		{"removed, #526's checks removed, pre-vote off", "no-candidate-config-check", true, false, "fail"},
		{"demoted, correct, pre-vote off", "", true, true, "pass"},
		{"demoted, correct, pre-vote on", "", false, true, "pass"},
		{"demoted, bug 1's fix, pre-vote off", "candidate-check-unless-ahead", true, true, "pass"},
		{"demoted, #526's checks removed, pre-vote off", "no-candidate-config-check", true, true, ""},
	} {
		setSwitches(c.bug)
		rep := dsttest.Sweep(o, removedCampaignScript(c.preVoteOff, c.demote))
		disrupted := 0
		for _, r := range rep.Failed {
			if !strings.Contains(r.Err.Error(), "disrupted the cluster") {
				t.Fatalf("%s: seed %d failed otherwise: %v", c.name, r.Seed, r.Err)
			}
			disrupted++
		}
		switch {
		case c.want == "pass" && disrupted > 0:
			t.Errorf("%s: disrupted on %d of %d seeds: %v", c.name, disrupted, rep.Runs, rep.Failed[0].Err)
		case c.want == "fail" && disrupted == 0:
			t.Errorf("%s: never disrupted: the scenario can't show what the checks protect", c.name)
		}
		if c.preVoteOff && rep.Coverage["the removed server came back with a higher term"] == 0 {
			t.Errorf("%s: the server never came back with a higher term", c.name)
		}
		t.Logf("%s: disrupted on %d of %d seeds", c.name, disrupted, rep.Runs)
	}
}

// failOnceStore is an InmemStore whose StoreLogs fails once, after armed.
type failOnceStore struct {
	*InmemStore
	armed, failed atomic.Bool
}

func (s *failOnceStore) StoreLogs(logs []*Log) error {
	if s.armed.Load() && s.failed.CompareAndSwap(false, true) {
		return fmt.Errorf("injected: store failure")
	}
	return s.InmemStore.StoreLogs(logs)
}

func (s *failOnceStore) StoreLog(l *Log) error { return s.StoreLogs([]*Log{l}) }

// pipelineStall runs three servers over real TCP (pipelined replication,
// the default MaxRPCsInFlight), applies entries without pause, makes one
// follower's log store fail one StoreLogs, and reports whether that
// follower then stopped receiving entries for 3s while the leader went on
// committing (DST.md 6.16, upstream #612).
func pipelineStall(t *testing.T) (stalled, shutdownHung bool) {
	conf := inmemConfig(t)
	var rafts []*Raft
	var stores []*failOnceStore
	var servers []Server
	var transports []*NetworkTransport
	for i := 0; i < 3; i++ {
		trans, err := NewTCPTransport("127.0.0.1:0", nil, 3, time.Second, nil)
		if err != nil {
			t.Fatal(err)
		}
		transports = append(transports, trans)
		servers = append(servers, Server{ID: ServerID(fmt.Sprintf("s%d", i)), Address: trans.LocalAddr()})
	}
	for i := 0; i < 3; i++ {
		c := *conf
		c.LocalID = servers[i].ID
		store := &failOnceStore{InmemStore: NewInmemStore()}
		stores = append(stores, store)
		snaps := NewInmemSnapshotStore()
		if err := BootstrapCluster(&c, store, store, snaps, transports[i], Configuration{Servers: servers}); err != nil {
			t.Fatal(err)
		}
		r, err := NewRaft(&c, &MockFSM{}, store, store, snaps, transports[i])
		if err != nil {
			t.Fatal(err)
		}
		rafts = append(rafts, r)
	}
	// Shut down with a bound: a stalled replication goroutine also keeps
	// Shutdown from returning (it waits for raft's goroutines).
	defer func() {
		ch := make(chan struct{})
		go func() {
			for _, r := range rafts {
				r.Shutdown().Error()
			}
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			shutdownHung = true
		}
	}()
	var leader *Raft
	for i := 0; i < 300 && leader == nil; i++ {
		time.Sleep(10 * time.Millisecond)
		for _, r := range rafts {
			if r.State() == Leader {
				leader = r
			}
		}
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			leader.Apply([]byte(fmt.Sprintf("x%d", i)), time.Second)
		}
	}()
	defer func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()
	var follower int
	for i, r := range rafts {
		if r != leader {
			follower = i
			break
		}
	}
	time.Sleep(300 * time.Millisecond)
	stores[follower].armed.Store(true)
	// Watch: after the failure, does the follower's log move again?
	deadline := time.Now().Add(6 * time.Second)
	for !stores[follower].failed.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !stores[follower].failed.Load() {
		t.Fatal("test setup: the follower's store never failed")
	}
	time.Sleep(200 * time.Millisecond)
	before, _ := stores[follower].LastIndex()
	leaderBefore := leader.getLastIndex()
	time.Sleep(3 * time.Second)
	after, _ := stores[follower].LastIndex()
	if leader.getLastIndex() <= leaderBefore {
		t.Fatal("test setup: the leader stopped committing")
	}
	return after == before, false
}

// TestPipelineStall (DST.md 6.16, upstream #612): after a follower rejects a
// pipelined append (its store failed once), correct raft can stop sending it
// entries for good; with pipeline-close-on-abort it never does. Real TCP and
// real time, so it repeats: correct raft must stall at least once in 5
// tries, the fix never in 5.
//
// Fails if: correct raft never stalls (nothing to show), or the fix stalls.
func TestPipelineStall(t *testing.T) {
	defer setSwitches("")
	setSwitches("")
	stalls, hung := 0, 0
	for i := 0; i < 5; i++ {
		s, h := pipelineStall(t)
		if s {
			stalls++
		}
		if h {
			hung++
		}
	}
	if stalls == 0 {
		t.Errorf("correct raft never stalled in 5 tries")
	}
	t.Logf("correct raft: the follower stalled in %d of 5 tries; Shutdown hung in %d", stalls, hung)
	setSwitches("pipeline-close-on-abort")
	for i := 0; i < 5; i++ {
		if s, h := pipelineStall(t); s || h {
			t.Errorf("with the fix, try %d: stalled %v, Shutdown hung %v", i+1, s, h)
		}
	}
}

// failedConfigWrite runs three servers on raft's in-memory pieces, makes
// the leader's log store fail the write of a DemoteVoter entry, and
// returns the leader's latest configuration afterwards, and whether its
// log holds any entry carrying that configuration (DST.md 6.18).
func failedConfigWrite(t *testing.T) (latest Configuration, inLog bool) {
	conf := inmemConfig(t)
	var rafts []*Raft
	var stores []*failOnceStore
	var servers []Server
	var trans []*InmemTransport
	for i := 0; i < 3; i++ {
		addr, tr := NewInmemTransport("")
		trans = append(trans, tr)
		servers = append(servers, Server{ID: ServerID(fmt.Sprintf("s%d", i)), Address: addr})
	}
	for i := range trans {
		for j := range trans {
			if i != j {
				trans[i].Connect(servers[j].Address, trans[j])
			}
		}
	}
	for i := 0; i < 3; i++ {
		c := *conf
		c.LocalID = servers[i].ID
		store := &failOnceStore{InmemStore: NewInmemStore()}
		stores = append(stores, store)
		snaps := NewInmemSnapshotStore()
		if err := BootstrapCluster(&c, store, store, snaps, trans[i], Configuration{Servers: servers}); err != nil {
			t.Fatal(err)
		}
		r, err := NewRaft(&c, &MockFSM{}, store, store, snaps, trans[i])
		if err != nil {
			t.Fatal(err)
		}
		rafts = append(rafts, r)
	}
	defer func() {
		for _, r := range rafts {
			r.Shutdown().Error()
		}
	}()
	var leader *Raft
	var li int
	for k := 0; k < 300 && leader == nil; k++ {
		time.Sleep(10 * time.Millisecond)
		for i, r := range rafts {
			if r.State() == Leader {
				leader, li = r, i
			}
		}
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	if err := leader.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	victim := servers[(li+1)%3].ID
	stores[li].armed.Store(true)
	err := leader.DemoteVoter(victim, 0, time.Second).Error()
	if err == nil || !stores[li].failed.Load() {
		t.Fatalf("test setup: the demotion's write didn't fail (err %v)", err)
	}
	f := leader.GetConfiguration()
	if err := f.Error(); err != nil {
		t.Fatal(err)
	}
	latest = f.Configuration()
	first, _ := stores[li].FirstIndex()
	last, _ := stores[li].LastIndex()
	for i := first; i <= last && i > 0; i++ {
		var l Log
		if stores[li].GetLog(i, &l) == nil && l.Type == LogConfiguration {
			if c := DecodeConfiguration(l.Data); !hasVote(c, victim) {
				inLog = true
			}
		}
	}
	return latest, inLog
}

// TestFailedConfigWrite (DST.md 6.18): a leader whose write of a
// configuration entry fails steps down, but upstream keeps the new
// configuration anyway, though no log holds it; with config-only-if-stored
// it keeps the configuration its log holds.
//
// Fails if: upstream doesn't keep the phantom configuration (nothing to
// show), or the fix keeps it.
func TestFailedConfigWrite(t *testing.T) {
	defer setSwitches("")
	setSwitches("")
	latest, inLog := failedConfigWrite(t)
	demoted := false
	for _, s := range latest.Servers {
		if s.Suffrage == Nonvoter {
			demoted = true
		}
	}
	if !demoted || inLog {
		t.Errorf("upstream: expected the unstored demotion in the latest configuration and not in the log; latest %v, in log %v", latest.Servers, inLog)
	}
	setSwitches("config-only-if-stored")
	latest, inLog = failedConfigWrite(t)
	for _, s := range latest.Servers {
		if s.Suffrage == Nonvoter && !inLog {
			t.Errorf("with the fix: the latest configuration %v holds a demotion no log entry holds", latest.Servers)
		}
	}
}
