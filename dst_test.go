package raft

// Deterministic simulation tests (go-dst-kit): a cluster of real raft
// nodes, each on its own simulated machine, talking through the real
// NetworkTransport over the simulated network, with crashes, partitions
// and pauses from the swarm.

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// seqFSM applies "w<i>" commands in order and records them.
type seqFSM struct {
	mu      dst.Mutex
	applied []string
}

func (f *seqFSM) Apply(l *Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, string(l.Data))
	return len(f.applied)
}

func (f *seqFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &seqSnapshot{append([]string(nil), f.applied...)}, nil
}

func (f *seqFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var got []string
	if err := json.NewDecoder(r).Decode(&got); err != nil {
		return err
	}
	f.mu.Lock()
	f.applied = got
	f.mu.Unlock()
	return nil
}

type seqSnapshot struct{ applied []string }

func (s *seqSnapshot) Persist(sink SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.applied); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *seqSnapshot) Release() {}

// dstNode is one server: its stores outlive its machine's crashes (a
// perfectly durable disk, for now); its raft and FSM are rebuilt at boot.
type dstNode struct {
	name  string
	logs  *InmemStore
	snaps *InmemSnapshotStore
	fsm   *seqFSM
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
	return c
}

func dstCluster(h *dsttest.Harness, n int) []*dstNode {
	var servers []Server
	nodes := make([]*dstNode, n)
	for i := range nodes {
		name := fmt.Sprintf("n%d", i)
		nodes[i] = &dstNode{name: name, logs: NewInmemStore(), snaps: NewInmemSnapshotStore()}
		servers = append(servers, Server{ID: ServerID(name), Address: ServerAddress(name + ":7000")})
	}
	seq := 0
	acked := map[string]bool{} // writes whose Apply succeeded
	for _, nd := range nodes {
		nd.m = h.Machine(nd.name, func(w *dst.World) {
			nd.snaps.w = w
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
			nd.fsm = &seqFSM{}
			r, err := NewRaft(conf, nd.fsm, nd.logs, nd.logs, nd.snaps, trans)
			if err != nil {
				h.Fail(err)
				return
			}
			nd.raft = r
			// The workload: while leader, write numbered commands.
			for h.Phase() == dsttest.Faulty {
				if r.State() == Leader {
					seq++
					cmd := fmt.Sprintf("w%d", seq)
					if r.Apply([]byte(cmd), 500*time.Millisecond).Error() == nil {
						acked[cmd] = true
						h.Sometimes("a write committed")
					}
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
				return fmt.Errorf("%s was acknowledged but no node applied it", c)
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
				return fmt.Errorf("%s applied %d entries that aren't a prefix of the longest (%d)", nd.name, len(got), len(longest))
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
	h.Expect("a write committed")
	return nodes
}

func dstOpts(f dsttest.Faults) dsttest.Options {
	return dsttest.Options{Seeds: 50, Faults: f, FaultPhase: 10 * time.Second, RecoveryPhase: 10 * time.Second}
}

// TestDSTSmoke: three nodes, scheduling only.
func TestDSTSmoke(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{}), func(h *dsttest.Harness) { dstCluster(h, 3) })
}

// TestDSTCrashes: nodes crash and restart (their stores are durable).
func TestDSTCrashes(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true}), func(h *dsttest.Harness) { dstCluster(h, 3) })
}

// TestDSTPartitions: the network partitions (sometimes one way), with
// latency spikes and connection resets.
func TestDSTPartitions(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Partition: true, Network: true}), func(h *dsttest.Harness) { dstCluster(h, 3) })
}

// TestDSTEverything: crashes, partitions, pauses and network faults at
// once, five nodes.
func TestDSTEverything(t *testing.T) {
	dsttest.Run(t, dstOpts(dsttest.Faults{Crash: true, Partition: true, Network: true, Pause: true}), func(h *dsttest.Harness) { dstCluster(h, 5) })
}
