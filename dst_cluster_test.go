package raft

// The simulated cluster for deterministic simulation tests (go-dst-kit):
// machines, their stores, raft, and how a node boots. The workloads (the
// key-value service, dst_kv_test.go; the membership operator,
// dst_operator_test.go) and the checks (dst_checks_test.go) are composed
// onto it by newCluster; the sweeps are in dst_test.go.
//
// Each server is a real raft node on its own simulated machine, talking
// through the real NetworkTransport over the simulated network
// (dst_stream_test.go), keeping its log and stable state in a file on the
// machine's crash-prone simulated disk (dstLogStore). Snapshots live in
// memory outside the simulation (for now) and survive crashes whole.

import (
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// clusterSpec says what to simulate.
type clusterSpec struct {
	voters   int // servers bootstrapped as voters
	spares   int // machines that start empty, outside the cluster, for the operator to add
	scenario dstScenario
	// operator runs membership changes (dst_operator_test.go), and takes
	// tickets for machines the runner loses for good (Faults.Loss).
	operator bool
	// defaultTimeouts runs raft with its own default timeouts (heartbeat
	// and election 1s, lease 500ms, commit 50ms) instead of a fifth of
	// them. Raft needs round trips well under its election timeout; slow
	// links (Faults.Gray) can take longer than a fifth of the defaults.
	defaultTimeouts bool
}

// dstScenario switches weaker modes on, each to show a check catches what
// it breaks: the store's (noSync, splitBatches: see dstLogStore), and
// naiveReadIndex, a client serving ReadIndex reads without first committing
// an entry in the leader's term.
type dstScenario struct{ noSync, splitBatches, naiveReadIndex bool }

// simCluster is a simulated cluster: its nodes, and what runs on them.
type simCluster struct {
	h     *dsttest.Harness
	spec  clusterSpec
	nodes []*dstNode // the bootstrapped voters first, then the spares
	kv    *kvWorkload
	op    *operator // nil unless spec.operator
}

// dstNode is one machine. Its log and stable store live in a file on its
// disk (what a crash keeps is what was fsynced); its raft, FSM and
// workloads are rebuilt at each boot.
type dstNode struct {
	name    string
	initial bool // bootstrapped as a voter (else a spare)
	logs    *dstLogStore
	snaps   *InmemSnapshotStore
	fsm     *kvFSM
	raft    *Raft
	m       *dsttest.Machine
	// wipeOnBoot: the next boot starts from an empty disk under the same
	// identity. Outside raft's fault model (a node forgetting what it
	// acknowledged); only TestDSTQuorumOffByOneScripted uses it.
	wipeOnBoot bool
}

func (nd *dstNode) id() ServerID        { return ServerID(nd.name) }
func (nd *dstNode) addr() ServerAddress { return ServerAddress(nd.name + ":7000") }

// up reports whether the node's raft is running now.
func (nd *dstNode) up() bool { return nd.raft != nil && !nd.m.Dead() }

func dstConfig(w *dst.World, name string) *Config {
	c := DefaultConfig()
	c.LocalID = ServerID(name)
	c.World = w
	c.Logger = hclog.NewNullLogger()
	if os.Getenv("DST_RAFT_LOG") != "" { // debugging one replay: raft's own log
		c.Logger = hclog.New(&hclog.LoggerOptions{Name: name, Level: hclog.Debug, Output: os.Stderr})
	}
	c.HeartbeatTimeout = 200 * time.Millisecond
	c.ElectionTimeout = 200 * time.Millisecond
	c.LeaderLeaseTimeout = 100 * time.Millisecond
	c.CommitTimeout = 10 * time.Millisecond
	// Snapshot often, so runs reach compaction and snapshot installs.
	c.SnapshotThreshold = 64
	c.SnapshotInterval = 500 * time.Millisecond
	c.TrailingLogs = 32
	return c
}

// newCluster builds the cluster spec describes on h, with its workloads
// and checks.
func newCluster(h *dsttest.Harness, spec clusterSpec) *simCluster {
	c := &simCluster{h: h, spec: spec}
	var initial []Server
	for i := range spec.voters + spec.spares {
		nd := &dstNode{name: fmt.Sprintf("n%d", i), initial: i < spec.voters, snaps: NewInmemSnapshotStore()}
		c.nodes = append(c.nodes, nd)
		if nd.initial {
			initial = append(initial, Server{ID: nd.id(), Address: nd.addr()})
		}
	}
	c.kv = newKVWorkload(c)
	if spec.operator {
		c.op = newOperator(c, initial)
	}
	for _, nd := range c.nodes {
		nd.m = h.Machine(nd.name, func(w *dst.World) { c.boot(nd, w, initial) })
	}
	c.checks()
	return c
}

// boot starts a node: its store, transport and raft (bootstrapping the
// initial voters the first time), then its workloads. It returns when the
// fault phase ends; raft keeps running.
func (c *simCluster) boot(nd *dstNode, w *dst.World, initial []Server) {
	h := c.h
	if nd.wipeOnBoot {
		nd.wipeOnBoot = false
		if err := w.FS().Remove("raft.log"); err != nil {
			h.Fail(err)
			return
		}
		nd.snaps = NewInmemSnapshotStore()
		h.Sometimes("a node lost its disk")
	}
	nd.snaps.w = w
	logs, err := openDSTLogStore(w.FS(), "raft.log")
	if err != nil {
		h.Fail(err)
		return
	}
	logs.noSync, logs.splitBatches = c.spec.scenario.noSync, c.spec.scenario.splitBatches
	nd.logs = logs
	stream, err := newDSTStream(w, string(nd.addr()))
	if err != nil {
		h.Fail(err)
		return
	}
	trans := NewNetworkTransportWithConfig(&NetworkTransportConfig{Stream: stream, MaxPool: 3, Timeout: 500 * time.Millisecond,
		Logger: hclog.NewNullLogger(), World: w})
	conf := dstConfig(w, nd.name)
	if c.spec.defaultTimeouts {
		d := DefaultConfig()
		conf.HeartbeatTimeout, conf.ElectionTimeout, conf.LeaderLeaseTimeout, conf.CommitTimeout = d.HeartbeatTimeout, d.ElectionTimeout, d.LeaderLeaseTimeout, d.CommitTimeout
	}
	if nd.initial {
		if ok, _ := HasExistingState(nd.logs, nd.logs, nd.snaps); !ok {
			if err := BootstrapCluster(conf, nd.logs, nd.logs, nd.snaps, trans, Configuration{Servers: initial}); err != nil {
				h.Fail(err)
				return
			}
		}
	}
	nd.fsm = &kvFSM{kv: map[string]string{}, h: h}
	r, err := NewRaft(conf, nd.fsm, nd.logs, nd.logs, nd.snaps, trans)
	if err != nil {
		h.Fail(err)
		return
	}
	nd.raft = r
	if c.op != nil {
		w.Go(func() { c.op.run(w, nd) })
	}
	c.kv.run(w, nd)
}

// node is the node with id, or nil.
func (c *simCluster) node(id ServerID) *dstNode {
	for _, nd := range c.nodes {
		if nd.id() == id {
			return nd
		}
	}
	return nil
}

// leader is a running node that believes it leads, the one with the
// highest term if several do (a deposed one may not know yet), or nil.
func (c *simCluster) leader() *dstNode {
	var best *dstNode
	for _, nd := range c.nodes {
		if nd.up() && nd.raft.getState() == Leader && (best == nil || nd.raft.getCurrentTerm() > best.raft.getCurrentTerm()) {
			best = nd
		}
	}
	return best
}

// dstCluster and dstClusterWith are the fixed clusters most sweeps use:
// n voters, no spares, no membership changes.
func dstCluster(h *dsttest.Harness, n int) []*dstNode { return dstClusterWith(h, n, dstScenario{}) }

func dstClusterWith(h *dsttest.Harness, n int, sc dstScenario) []*dstNode {
	return newCluster(h, clusterSpec{voters: n, scenario: sc}).nodes
}

// covers reports whether the store's latest finished snapshot includes
// index (for checks between scheduling steps: no lock).
func (s *InmemSnapshotStore) covers(index uint64) bool {
	return s != nil && s.hasSnapshot && s.latest != nil && s.latest.meta.Index >= index
}
