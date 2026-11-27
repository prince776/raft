package raft

// The key-value service on raft, as an application embeds it: an FSM, and
// on each node a client that, while its node leads, puts through the log
// and gets through the log or by ReadIndex. Its history is checked for
// linearizability (dst_checks_test.go).

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// kvWorkload is the service's clients across the cluster: their shared
// history, and the writes acknowledged to them.
type kvWorkload struct {
	c       *simCluster
	hist    *check.History[check.KVInput, check.KVOutput]
	keys    []string
	acked   map[string]bool // IDs of writes whose Apply succeeded
	opID    int
	clients int
}

func newKVWorkload(c *simCluster) *kvWorkload {
	return &kvWorkload{c: c, hist: check.NewHistory[check.KVInput, check.KVOutput](c.h.World()),
		keys: []string{"a", "b", "c"}, acked: map[string]bool{}}
}

// run is one boot's client on nd: while the node leads, an operation every
// 5 to 25ms, until the fault phase ends.
func (k *kvWorkload) run(w *dst.World, nd *dstNode) {
	h := k.c.h
	k.clients++ // a client per boot: its operations are sequential
	c := &kvClient{h: h, w: w, r: nd.raft, fsm: nd.fsm, hist: k.hist, id: k.clients, acked: k.acked, naive: k.c.spec.scenario.naiveReadIndex}
	for h.Phase() == dsttest.Faulty {
		if nd.raft.State() == Leader {
			k.opID++
			c.op(fmt.Sprintf("op%d", k.opID), k.keys[w.Rand().IntN(len(k.keys))])
		}
		w.Sleep(time.Duration(5+w.Rand().IntN(20)) * time.Millisecond)
	}
}

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

