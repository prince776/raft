package raft

// The deterministic simulation sweeps (go-dst-kit) of a raft cluster
// (dst_cluster_test.go), its workloads and checks.

import (
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dsttest"
)

func dstOpts(f dsttest.Faults) dsttest.Options {
	return dsttest.Options{Seeds: 50, Faults: f, FaultPhase: 10 * time.Second, RecoveryPhase: 10 * time.Second}
}

// TestDSTSmoke: three nodes, scheduling only.
func TestDSTSmoke(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{}), func(h *dsttest.Harness) { dstCluster(h, 3) })
}

// TestDSTCrashes: nodes crash and restart; their disks lose what wasn't
// fsynced, tear writes and lose power mid-call.
func TestDSTCrashes(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true}), func(h *dsttest.Harness) {
		dstCluster(h, 3)
		h.Expect("an FSM was restored from a snapshot")
	})
}

// TestDSTPartitions: the network partitions (sometimes one way), with
// latency spikes and connection resets.
func TestDSTPartitions(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Partition: true, Network: true}), func(h *dsttest.Harness) { dstCluster(h, 3) })
}

// TestDSTEverything: crashes, disk faults, partitions, pauses and network
// faults at once, five nodes.
func TestDSTEverything(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true}),
		func(h *dsttest.Harness) { dstCluster(h, 5) })
}

// TestDSTFourNodes: an even-sized cluster (its quorum is 3 of 4, not
// half), with crashes and partitions.
func TestDSTFourNodes(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true}),
		func(h *dsttest.Harness) { dstCluster(h, 4) })
}

// membershipCluster is a cluster whose size is drawn per seed (1, 3, 5
// or 7 voters, as deployed), with 3 spare machines and the membership
// operator: even sizes and non-voters happen in passing, as changes go.
func membershipCluster(h *dsttest.Harness) *simCluster {
	voters := []int{1, 3, 5, 7}[h.World().Rand().Stream("cluster size").IntN(4)]
	c := newCluster(h, clusterSpec{voters: voters, spares: 3, operator: true})
	h.Expect("membership: AddNonvoter", "membership: AddVoter", "membership: DemoteVoter", "membership: RemoveServer",
		"a server was lost for good", "a lost server was removed", "a removed server was shut down")
	return c
}

// TestDSTMembership: membership changes under crashes, disk faults,
// partitions, network faults, and servers lost for good and replaced.
func TestDSTMembership(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Loss: true}),
		func(h *dsttest.Harness) { membershipCluster(h) })
}

// The store's guarantees matter: with fsync skipped (a store that returns
// before its writes are durable), crashes lose acknowledged writes, and
// the checks catch it. Fails if no seed does.
func TestDSTCatchesUnsyncedStore(t *testing.T) {
	o := dstOpts(dsttest.Faults{Crash: true, Disk: true})
	o.KeepGoing, o.DeterminismChecks = true, -1
	rep := dsttest.Sweep(o, func(h *dsttest.Harness) { dstClusterWith(h, 3, dstScenario{noSync: true}) })
	if len(rep.Failed) == 0 {
		t.Fatalf("no seed of %d caught a store that doesn't fsync", rep.Runs)
	}
	t.Logf("%d of %d seeds caught it; first: seed %d: %v", len(rep.Failed), rep.Runs, rep.Failed[0].Seed, rep.Failed[0].Err)
}

// The linearizability check catches the classic ReadIndex mistake: a new
// leader serving reads from its commit index before it has committed an
// entry in its own term, when that index can lag writes already
// acknowledged. Fails if no seed catches it.
func TestDSTCatchesNaiveReadIndex(t *testing.T) {
	o := dstOpts(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true})
	o.KeepGoing, o.DeterminismChecks = true, -1
	rep := dsttest.Sweep(o, func(h *dsttest.Harness) { dstClusterWith(h, 3, dstScenario{naiveReadIndex: true}) })
	if len(rep.Failed) == 0 {
		t.Fatalf("no seed of %d caught ReadIndex without a barrier", rep.Runs)
	}
	t.Logf("%d of %d seeds caught it", len(rep.Failed), rep.Runs)
}
