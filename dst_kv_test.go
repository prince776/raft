package raft

// The key-value service on raft, as an application embeds it: an FSM, and
// on each node a client that, while its node leads, puts through the log
// and gets through the log or by ReadIndex. Clients on their own machines
// reach the same operations through each node's server
// (dst_service_test.go). Their shared history is checked for
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

// kvFSM applies kvCmds: a map, and the sequence of commands applied. A
// command whose ID it has already applied (a client's retry, appended to
// the log again) is answered from its first application and not applied
// again, as Raft's client sessions do (thesis §6.3). Retries thus can't
// show up as a command applied twice, so the FSM checks the log itself:
// it must see each log index once, in increasing order.
type kvFSM struct {
	mu        dst.Mutex
	kv        map[string]string
	applied   []string // command IDs, in order (first applications only)
	seen      map[string]kvResult
	lastIndex uint64 // the last log index applied
	h         *dsttest.Harness
}

func newKVFSM(h *dsttest.Harness) *kvFSM {
	return &kvFSM{kv: map[string]string{}, seen: map[string]kvResult{}, h: h}
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
	if l.Index <= f.lastIndex {
		f.h.Fail(fmt.Errorf("the FSM was given log index %d after %d: an entry applied twice or out of order", l.Index, f.lastIndex))
	}
	f.lastIndex = l.Index
	if prev, ok := f.seen[c.ID]; ok {
		f.h.Sometimes("a retried command was deduplicated")
		return prev
	}
	f.applied = append(f.applied, c.ID)
	res := kvResult{}
	if c.Get {
		v, ok := f.kv[c.Key]
		res = kvResult{v, ok}
	} else {
		f.kv[c.Key] = c.Value
	}
	f.seen[c.ID] = res
	return res
}

func (f *kvFSM) read(key string) kvResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.kv[key]
	return kvResult{v, ok}
}

type kvState struct {
	KV        map[string]string
	Applied   []string
	Seen      map[string]kvResult
	LastIndex uint64
}

func (f *kvFSM) Snapshot() (FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &kvSnapshot{st: kvState{KV: maps.Clone(f.kv), Applied: slices.Clone(f.applied), Seen: maps.Clone(f.seen), LastIndex: f.lastIndex}, h: f.h}, nil
}

func (f *kvFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	var st kvState
	if err := json.NewDecoder(r).Decode(&st); err != nil {
		return err
	}
	f.mu.Lock()
	f.kv, f.applied, f.seen, f.lastIndex = st.KV, st.Applied, st.Seen, st.LastIndex
	if f.kv == nil {
		f.kv = map[string]string{}
	}
	if f.seen == nil {
		f.seen = map[string]kvResult{}
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

// kvOutcome is what became of an operation on the leader.
type kvOutcome int

const (
	kvDone       kvOutcome = iota // done: written, or read
	kvNotApplied                  // never entered the log: no effect
	kvUnknown                     // may or may not take effect
)

// apply puts cmd through the log and waits for its result.
func (c *kvClient) apply(cmd kvCmd) (kvResult, kvOutcome) {
	data, _ := json.Marshal(cmd)
	f := c.r.Apply(data, 500*time.Millisecond)
	if err := f.Error(); err != nil {
		if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrEnqueueTimeout) {
			return kvResult{}, kvNotApplied // never entered the log
		}
		return kvResult{}, kvUnknown // it may still commit
	}
	res, _ := f.Response().(kvResult)
	return res, kvDone
}

// put writes value (the operation's ID) to key through the log.
func (c *kvClient) put(id, key string) kvOutcome {
	_, o := c.apply(kvCmd{ID: id, Key: key, Value: id})
	if o == kvDone {
		c.h.Sometimes("a write committed")
	}
	return o
}

// logGet reads key through the log.
func (c *kvClient) logGet(id, key string) (kvResult, kvOutcome) {
	res, o := c.apply(kvCmd{ID: id, Get: true, Key: key})
	if o == kvDone {
		c.h.Sometimes("a read through the log")
	}
	return res, o
}

// record runs one operation, recording it in the history: a put (half
// the time), or a get through the log or by ReadIndex.
func (c *kvClient) op(id, key string) {
	x := c.w.Rand().IntN(4)
	in := check.KVInput{Op: check.KVGet, Key: key}
	if x < 2 {
		in = check.KVInput{Op: check.KVPut, Key: key, Value: id}
	}
	call := c.hist.Invoke(c.id, in)
	var res kvResult
	var o kvOutcome
	switch {
	case x < 2:
		o = c.put(id, key)
	case x == 2:
		res, o = c.logGet(id, key)
	default:
		res, o = c.readIndex(key)
	}
	endCall(call, in, res, o, id, c.acked)
}

// endCall ends call by outcome o, marking an acknowledged put in acked.
func endCall(call check.Call[check.KVInput, check.KVOutput], in check.KVInput, res kvResult, o kvOutcome, id string, acked map[string]bool) {
	switch o {
	case kvDone:
		if in.Op == check.KVPut {
			acked[id] = true
		}
		call.Return(check.KVOutput{Value: res.Value, Found: res.Found})
	case kvNotApplied:
		call.Fail()
	default:
		if in.Op == check.KVGet {
			// A read whose result never came changes nothing: dropping it
			// keeps the verdict and spares the checker an operation it
			// must try at every place (Abandon), which exhausted its budget
			// once leadership transfers made such reads common.
			call.Fail()
			return
		}
		call.Abandon()
	}
}

// readIndex is a read by the ReadIndex recipe (Raft paper §6.4 and §8):
// once per term, commit an entry of the term (a Barrier), so the commit
// index is current; then for each read take the commit index, confirm
// leadership (VerifyLeader: a quorum still follows), wait until the FSM
// has applied that far, and read locally.
func (c *kvClient) readIndex(key string) (kvResult, kvOutcome) {
	h, w, r := c.h, c.w, c.r
	term := r.getCurrentTerm()
	if !c.naive && !bug("readindex-without-barrier") && c.barrierTerm != term {
		if r.Barrier(500*time.Millisecond).Error() != nil || r.getCurrentTerm() != term {
			return kvResult{}, kvUnknown
		}
		c.barrierTerm = term
	}
	idx := r.CommitIndex()
	if err := r.VerifyLeader().Error(); err != nil || r.getCurrentTerm() != term {
		return kvResult{}, kvUnknown
	}
	deadline := w.Now().Add(500 * time.Millisecond)
	for r.AppliedIndex() < idx {
		if w.Now().After(deadline) {
			return kvResult{}, kvUnknown
		}
		w.Sleep(time.Millisecond)
	}
	h.Sometimes("a ReadIndex read")
	return c.fsm.read(key), kvDone
}
