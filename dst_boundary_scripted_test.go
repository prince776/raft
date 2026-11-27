//go:build dstbugs

package raft

import (
	"fmt"
	"testing"
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
	defer func() { activeBug = "" }()
	for _, tc := range []struct {
		name string
		run  func(*testing.T) []string
	}{{"append after the follower's snapshot", boundaryAppendProblems}, {"older snapshot installed", boundaryInstallProblems}} {
		t.Run(tc.name, func(t *testing.T) {
			activeBug = ""
			if p := tc.run(t); len(p) == 0 {
				t.Errorf("correct raft passed")
			} else {
				t.Logf("correct raft: %v", p)
			}
			activeBug = "snapshot-boundary-fix"
			if p := tc.run(t); len(p) > 0 {
				t.Errorf("with the fix: %v", p)
			}
		})
	}
}
