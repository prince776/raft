package raft

// Deterministic simulation tests (go-dst-kit): a cluster of real raft
// nodes, each on its own simulated machine, talking through the real
// NetworkTransport over the simulated network, keeping their logs in a
// file on a crash-prone simulated disk (dstLogStore), with crashes,
// partitions and pauses from the swarm.
//
// The workload is a key-value service on raft, as an application embeds
// it: each node runs a client that, while its node leads, writes through
// the log and reads either through the log or with the ReadIndex recipe
// (VerifyLeader, then wait for the commit index). Checked: at most one
// leader per term; the clients' history is linearizable; acknowledged
// writes survive; every node applied a prefix of one sequence.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// kvCmd is a command in the log.
type kvCmd struct {
	ID    string // unique per operation
	Get   bool
	Key   string
	Value string
}

// kvFSM applies kvCmds: a map, and the sequence of commands applied.
type kvFSM struct {
	mu      dst.Mutex
	kv      map[string]string
	applied []string // command IDs, in order
	h       *dsttest.Harness
}

type kvResult struct {
	Value string
	Found bool
}

func (f *kvFSM) Apply(l *Log) interface{} {
	var c kvCmd
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, c.ID)
	if c.Get {
		v, ok := f.kv[c.Key]
		return kvResult{v, ok}
	}
	f.kv[c.Key] = c.Value
	return kvResult{}
}

func (f *kvFSM) read(key string) kvResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.kv[key]
	return kvResult{v, ok}
}

type kvState struct {
	KV      map[string]string
	Applied []string
}

func (f *kvFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &kvSnapshot{st: kvState{KV: maps.Clone(f.kv), Applied: slices.Clone(f.applied)}, h: f.h}, nil
}

func (f *kvFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var st kvState
	if err := json.NewDecoder(r).Decode(&st); err != nil {
		return err
	}
	f.mu.Lock()
	f.kv, f.applied = st.KV, st.Applied
	if f.kv == nil {
		f.kv = map[string]string{}
	}
	f.mu.Unlock()
	f.h.Sometimes("an FSM was restored from a snapshot")
	return nil
}

type kvSnapshot struct {
	st kvState
	h  *dsttest.Harness
}

func (s *kvSnapshot) Persist(sink SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.st); err != nil {
		sink.Cancel()
		return err
	}
	s.h.Sometimes("a snapshot was persisted")
	return sink.Close()
}

func (s *kvSnapshot) Release() {}

// dstNode is one server. Its log and stable store live in a file on its
// machine's simulated disk (dstLogStore: what a crash keeps is what was
// fsynced); its snapshots outlive crashes whole (for now); its raft, FSM
// and client are rebuilt at each boot.
type dstNode struct {
	name  string
	logs  *dstLogStore
	snaps *InmemSnapshotStore
	fsm   *kvFSM
	raft  *Raft
	m     *dsttest.Machine
}

func dstConfig(w *dst.World, name string) *Config {
	c := DefaultConfig()
	c.LocalID = ServerID(name)
	c.World = w
	c.Logger = hclog.NewNullLogger()
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

// dstScenario switches weaker modes on: the store's (see dstLogStore), and
// naiveReadIndex, a client that serves ReadIndex reads without first
// committing an entry in the leader's term.
type dstScenario struct{ noSync, splitBatches, naiveReadIndex bool }

func dstCluster(h *dsttest.Harness, n int) []*dstNode { return dstClusterWith(h, n, dstScenario{}) }

func dstClusterWith(h *dsttest.Harness, n int, sc dstScenario) []*dstNode {
	var servers []Server
	nodes := make([]*dstNode, n)
	for i := range nodes {
		name := fmt.Sprintf("n%d", i)
		nodes[i] = &dstNode{name: name, snaps: NewInmemSnapshotStore()}
		servers = append(servers, Server{ID: ServerID(name), Address: ServerAddress(name + ":7000")})
	}
	hist := check.NewHistory[check.KVInput, check.KVOutput](h.World())
	keys := []string{"a", "b", "c"}
	opID, client := 0, 0
	acked := map[string]bool{} // IDs of writes whose Apply succeeded
	for _, nd := range nodes {
		nd.m = h.Machine(nd.name, func(w *dst.World) {
			nd.snaps.w = w
			logs, err := openDSTLogStore(w.FS(), "raft.log")
			if err != nil {
				h.Fail(err)
				return
			}
			logs.noSync, logs.splitBatches = sc.noSync, sc.splitBatches
			nd.logs = logs
			stream, err := newDSTStream(w, nd.name+":7000")
			if err != nil {
				h.Fail(err)
				return
			}
			trans := NewNetworkTransportWithConfig(&NetworkTransportConfig{Stream: stream, MaxPool: 3, Timeout: 500 * time.Millisecond,
				Logger: hclog.NewNullLogger(), World: w})
			conf := dstConfig(w, nd.name)
			if ok, _ := HasExistingState(nd.logs, nd.logs, nd.snaps); !ok {
				if err := BootstrapCluster(conf, nd.logs, nd.logs, nd.snaps, trans, Configuration{Servers: servers}); err != nil {
					h.Fail(err)
					return
				}
			}
			nd.fsm = &kvFSM{kv: map[string]string{}, h: h}
			r, err := NewRaft(conf, nd.fsm, nd.logs, nd.logs, nd.snaps, trans)
			if err != nil {
				h.Fail(err)
				return
			}
			nd.raft = r
			client++
			cid := client // a client per boot: its operations are sequential
			c := &kvClient{h: h, w: w, r: r, fsm: nd.fsm, hist: hist, id: cid, acked: acked, naive: sc.naiveReadIndex}
			for h.Phase() == dsttest.Faulty {
				if r.State() == Leader {
					opID++
					c.op(fmt.Sprintf("op%d", opID), keys[w.Rand().IntN(len(keys))])
				}
				w.Sleep(time.Duration(5+w.Rand().IntN(20)) * time.Millisecond)
			}
		})
	}

	// Election safety: at most one leader per term, ever (a crashed node's
	// raft keeps its last state, so only running nodes count).
	leaders := map[uint64]string{}
	h.Always("at most one leader per term", func() error {
		for _, nd := range nodes {
			if nd.raft == nil || nd.m.Dead() || nd.raft.getState() != Leader {
				continue
			}
			term := nd.raft.getCurrentTerm()
			if other, ok := leaders[term]; ok && other != nd.name {
				return fmt.Errorf("term %d has two leaders: %s and %s", term, other, nd.name)
			}
			leaders[term] = nd.name
		}
		return nil
	})
	h.Final("the clients' history is linearizable", func(w *dst.World) error {
		return check.Linearizable(check.KVModel(), hist.Ops(), check.Options{}).Err()
	})
	h.Final("acknowledged writes survive", func(w *dst.World) error {
		applied := map[string]bool{}
		for _, nd := range nodes {
			if nd.fsm != nil {
				for _, c := range nd.fsm.applied {
					applied[c] = true
				}
			}
		}
		for _, c := range slices.Sorted(maps.Keys(acked)) {
			if !applied[c] {
				return fmt.Errorf("write %s was acknowledged but no node applied it", c)
			}
		}
		return nil
	})
	h.Final("the nodes applied the same sequence", func(w *dst.World) error {
		var longest []string
		for _, nd := range nodes {
			if nd.fsm != nil && len(nd.fsm.applied) > len(longest) {
				longest = nd.fsm.applied
			}
		}
		for _, nd := range nodes {
			if nd.fsm == nil {
				continue
			}
			if got := nd.fsm.applied; !slices.Equal(got, longest[:len(got)]) {
				return fmt.Errorf("%s applied %d commands that aren't a prefix of the longest (%d)", nd.name, len(got), len(longest))
			}
		}
		seen := map[string]bool{}
		for _, c := range longest {
			if seen[c] {
				return fmt.Errorf("%s applied twice", c)
			}
			seen[c] = true
		}
		if len(longest) == 0 {
			return fmt.Errorf("nothing was applied")
		}
		return nil
	})
	h.Expect("a write committed", "a read through the log", "a ReadIndex read", "a snapshot was persisted")
	return nodes
}

// kvClient issues operations on its node while the node leads.
type kvClient struct {
	h     *dsttest.Harness
	w     *dst.World
	r     *Raft
	fsm   *kvFSM
	hist  *check.History[check.KVInput, check.KVOutput]
	id    int
	acked map[string]bool
	naive bool
	// barrierTerm is the term in which this leader is known to have
	// committed an entry (a Barrier returned): from then on its commit
	// index covers everything committed before its term.
	barrierTerm uint64
}

// op is one operation: a put through the log (half the time), or a get,
// through the log or by ReadIndex.
func (c *kvClient) op(id, key string) {
	h, w, r := c.h, c.w, c.r
	apply := func(cmd kvCmd) (kvResult, error) {
		data, _ := json.Marshal(cmd)
		f := r.Apply(data, 500*time.Millisecond)
		if err := f.Error(); err != nil {
			return kvResult{}, err
		}
		res, _ := f.Response().(kvResult)
		return res, nil
	}
	// notApplied: errors that mean the command never entered the log.
	notApplied := func(err error) bool { return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrEnqueueTimeout) }
	switch x := w.Rand().IntN(4); {
	case x < 2:
		call := c.hist.Invoke(c.id, check.KVInput{Op: check.KVPut, Key: key, Value: id})
		_, err := apply(kvCmd{ID: id, Key: key, Value: id})
		switch {
		case err == nil:
			c.acked[id] = true
			call.Return(check.KVOutput{})
			h.Sometimes("a write committed")
		case notApplied(err):
			call.Fail()
		default:
			call.Abandon() // it may still commit
		}
	case x == 2:
		call := c.hist.Invoke(c.id, check.KVInput{Op: check.KVGet, Key: key})
		res, err := apply(kvCmd{ID: id, Get: true, Key: key})
		if err != nil {
			call.Abandon()
			return
		}
		call.Return(check.KVOutput{Value: res.Value, Found: res.Found})
		h.Sometimes("a read through the log")
	default:
		c.readIndex(key)
	}
}

// readIndex is a read by the ReadIndex recipe (Raft paper §6.4 and §8):
// once per term, commit an entry of the term (a Barrier), so the commit
// index is current; then for each read take the commit index, confirm
// leadership (VerifyLeader: a quorum still follows), wait until the FSM
// has applied that far, and read locally.
func (c *kvClient) readIndex(key string) {
	h, w, r := c.h, c.w, c.r
	term := r.getCurrentTerm()
	if !c.naive && !bug("readindex-without-barrier") && c.barrierTerm != term {
		if r.Barrier(500*time.Millisecond).Error() != nil || r.getCurrentTerm() != term {
			return
		}
		c.barrierTerm = term
	}
	call := c.hist.Invoke(c.id, check.KVInput{Op: check.KVGet, Key: key})
	idx := r.CommitIndex()
	if err := r.VerifyLeader().Error(); err != nil || r.getCurrentTerm() != term {
		call.Abandon()
		return
	}
	deadline := w.Now().Add(500 * time.Millisecond)
	for r.AppliedIndex() < idx {
		if w.Now().After(deadline) {
			call.Abandon()
			return
		}
		w.Sleep(time.Millisecond)
	}
	res := c.fsm.read(key)
	call.Return(check.KVOutput{Value: res.Value, Found: res.Found})
	h.Sometimes("a ReadIndex read")
}

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
