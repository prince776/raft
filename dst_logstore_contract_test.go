package raft

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// storeModel is what a dstLogStore holds: entries by index (term and
// data) and keys.
type storeModel struct {
	logs map[uint64]string
	kv   map[string]string
}

func (m storeModel) clone() storeModel {
	return storeModel{maps.Clone(m.logs), maps.Clone(m.kv)}
}

// storeCall is one change, applied to a model.
type storeCall struct {
	name  string
	apply func(m storeModel)
	do    func(s *dstLogStore) error
}

func modelOf(s *dstLogStore) (storeModel, error) {
	m := storeModel{logs: map[uint64]string{}, kv: map[string]string{}}
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if (first == 0) != (last == 0) || first > last {
		return m, fmt.Errorf("bounds first %d, last %d", first, last)
	}
	if first > 0 {
		for i := first; i <= last; i++ {
			var l Log
			if err := s.GetLog(i, &l); err != nil {
				return m, fmt.Errorf("entry %d of [%d, %d]: %v", i, first, last, err)
			}
			m.logs[i] = fmt.Sprintf("%d/%s", l.Term, l.Data)
		}
	}
	for k, v := range s.kv {
		m.kv[k] = string(v)
	}
	return m, nil
}

func describe(m storeModel) string {
	idx := slices.Sorted(maps.Keys(m.logs))
	if len(idx) == 0 {
		return fmt.Sprintf("no entries, %d keys", len(m.kv))
	}
	return fmt.Sprintf("entries [%d, %d] (%d), %d keys", idx[0], idx[len(idx)-1], len(idx), len(m.kv))
}

func equalModels(a, b storeModel) bool { return maps.Equal(a.logs, b.logs) && maps.Equal(a.kv, b.kv) }

// storeContract is a machine using one dstLogStore through crashes: random
// batches (appended, a contiguous log), deletions of a prefix or a suffix,
// and key sets. At each boot the store must hold every call that returned
// and, of the call a crash interrupted, all or nothing.
func storeContract(h *dsttest.Harness, sc dstScenario) {
	model := storeModel{logs: map[uint64]string{}, kv: map[string]string{}}
	var pending *storeCall
	n := 0
	h.Machine("store", func(w *dst.World) {
		s, err := openDSTLogStore(w.FS(), "raft.log")
		for errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOSPC) { // passes: retry the startup
			h.Sometimes("opening the store failed on the disk")
			w.Sleep(20 * time.Millisecond)
			s, err = openDSTLogStore(w.FS(), "raft.log")
		}
		if err != nil {
			h.Fail(err)
			return
		}
		s.noSync, s.splitBatches = sc.noSync, sc.splitBatches
		got, err := modelOf(s)
		if err != nil {
			h.Fail(fmt.Errorf("after recovery: %v", err))
			return
		}
		switch {
		case equalModels(got, model):
		case pending != nil && equalModels(got, func() storeModel { m := model.clone(); pending.apply(m); return m }()):
			pending.apply(model) // the interrupted call took effect, whole
			h.Sometimes("an interrupted call survived a crash")
		default:
			what := "no call was in flight"
			if pending != nil {
				what = pending.name + " was in flight"
			}
			h.Fail(fmt.Errorf("after a crash the store has %s; acknowledged calls give %s (%s)", describe(got), describe(model), what))
			return
		}
		if pending != nil {
			h.Sometimes("recovered with a call in flight")
		}
		pending = nil
		for h.Phase() == dsttest.Faulty {
			c := randomCall(w, model, &n)
			pending = &c
			// A crash ends this goroutine inside the call: it doesn't return.
			if err := c.do(s); err != nil {
				// A disk error (Faults.DiskErrors): the call failed, so it
				// must leave no trace, now or after a crash.
				if !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EIO) {
					h.Fail(fmt.Errorf("%s: %v", c.name, err))
					return
				}
				h.Sometimes("a store call failed on the disk")
				pending = nil
				if got, err := modelOf(s); err != nil || !equalModels(got, model) {
					h.Fail(fmt.Errorf("after %s failed (%v), the store reads %s, not the acknowledged %s (%v)", c.name, err, describe(got), describe(model), err))
					return
				}
				w.Sleep(time.Duration(1+w.Rand().IntN(5)) * time.Millisecond)
				continue
			}
			c.apply(model)
			pending = nil
			w.Sleep(time.Duration(w.Rand().IntN(5)) * time.Millisecond)
		}
	})
	h.Expect("recovered with a call in flight")
}

func randomCall(w *dst.World, m storeModel, n *int) storeCall {
	idx := slices.Sorted(maps.Keys(m.logs))
	first, last := uint64(0), uint64(0)
	if len(idx) > 0 {
		first, last = idx[0], idx[len(idx)-1]
	}
	*n++
	switch x := w.Rand().IntN(10); {
	case x < 6 || last == 0: // append a batch
		k := 1 + w.Rand().IntN(5)
		term := uint64(1 + *n/20)
		var logs []*Log
		for i := range k {
			logs = append(logs, &Log{Index: last + 1 + uint64(i), Term: term, Data: []byte(fmt.Sprintf("c%d.%d", *n, i))})
		}
		return storeCall{fmt.Sprintf("StoreLogs[%d, %d]", last+1, last+uint64(k)),
			func(m storeModel) {
				for _, l := range logs {
					m.logs[l.Index] = fmt.Sprintf("%d/%s", l.Term, l.Data)
				}
			},
			func(s *dstLogStore) error { return s.StoreLogs(logs) }}
	case x < 7: // compact a prefix
		hi := first + uint64(w.Rand().IntN(int(last-first+1)))
		return deleteCall(first, hi)
	case x < 8: // truncate a suffix
		lo := first + uint64(w.Rand().IntN(int(last-first+1)))
		return deleteCall(lo, last)
	default:
		k, v := fmt.Sprintf("k%d", w.Rand().IntN(3)), fmt.Sprintf("v%d", *n)
		return storeCall{"Set " + k,
			func(m storeModel) { m.kv[k] = v },
			func(s *dstLogStore) error { return s.Set([]byte(k), []byte(v)) }}
	}
}

func deleteCall(lo, hi uint64) storeCall {
	return storeCall{fmt.Sprintf("DeleteRange[%d, %d]", lo, hi),
		func(m storeModel) {
			for i := lo; i <= hi; i++ {
				delete(m.logs, i)
			}
		},
		func(s *dstLogStore) error { return s.DeleteRange(lo, hi) }}
}

func storeOpts() dsttest.Options {
	return dsttest.Options{Seeds: 200, Faults: dsttest.Faults{Crash: true, Disk: true},
		FaultPhase: 5 * time.Second, RecoveryPhase: time.Second}
}

// Verifies: dstLogStore's contract (its doc comment): durable on return,
// each call atomic, recovery to the last whole record.
// Fails if: after a crash the store lacks an acknowledged call, holds part
// of the interrupted one, or reads inconsistently; or if the test misses
// a store that skips fsync, or that writes a batch as one record per entry.
func TestDSTLogStoreContract(t *testing.T) {
	dsttest.Run(t, storeOpts(), func(h *dsttest.Harness) { storeContract(h, dstScenario{}) })
	withErrors := storeOpts()
	// Kills too: a failed write's bytes stay in the page cache, which a
	// restarted process reads (fsyncgate's other half).
	withErrors.Faults.DiskErrors, withErrors.Faults.Kill = true, true
	dsttest.Run(t, withErrors, func(h *dsttest.Harness) {
		storeContract(h, dstScenario{})
		h.Expect("a store call failed on the disk")
	})
	for _, sc := range []dstScenario{{noSync: true}, {splitBatches: true}} {
		o := storeOpts()
		o.KeepGoing, o.DeterminismChecks = true, -1
		rep := dsttest.Sweep(o, func(h *dsttest.Harness) { storeContract(h, sc) })
		if len(rep.Failed) == 0 {
			t.Errorf("the contract test missed a store with %+v (%d seeds)", sc, rep.Runs)
			continue
		}
		t.Logf("%+v: caught by %d of %d seeds; first: %v", sc, len(rep.Failed), rep.Runs, rep.Failed[0].Err)
	}
}
