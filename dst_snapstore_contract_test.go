package raft

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// snapshotData is the content of the snapshot at index: n bytes that
// name it, so a snapshot read back is checked byte for byte.
func snapshotData(index uint64, n int) []byte {
	unit := []byte(fmt.Sprintf("snapshot %d;", index))
	return bytes.Repeat(unit, 1+n/len(unit))[:n]
}

// snapStoreContract is a machine using one FileSnapshotStore through
// crashes and disk faults, as raft does: it creates snapshots of rising
// indexes, writes each in a few writes, and closes it (sometimes it
// cancels one instead). raft compacts its log once Close returns, so
// FileSnapshotStore's contract is that a snapshot whose Close returned
// nil is durable: at every boot the latest snapshot listed must be the
// last one closed, or the one a crash interrupted, and read back whole.
func snapStoreContract(h *dsttest.Harness, retain int, noSync bool) {
	var closed, pending uint64 // indexes: the last closed, the one in flight
	sizes := map[uint64]int{}
	h.Machine("store", func(w *dst.World) {
		s, err := NewFileSnapshotStoreWithWorld(w, "snap", retain, hclog.NewNullLogger())
		if err != nil {
			h.Fail(err)
			return
		}
		s.noSync = noSync
		list, err := s.List()
		if err != nil {
			h.Fail(fmt.Errorf("List after a crash: %v", err))
			return
		}
		var latest uint64
		if len(list) > 0 {
			latest = list[0].Index
		}
		switch {
		case latest == closed:
		case pending != 0 && latest == pending:
			closed = pending // the interrupted Close took effect
			h.Sometimes("an interrupted snapshot survived a crash")
		default:
			h.Fail(fmt.Errorf("after a crash the latest snapshot is %d (%d listed); the last one closed is %d, the one in flight %d",
				latest, len(list), closed, pending))
			return
		}
		if pending != 0 {
			h.Sometimes("recovered with a snapshot in flight")
		}
		pending = 0
		if closed > 0 {
			meta, rc, err := s.Open(list[0].ID)
			if err != nil {
				h.Fail(fmt.Errorf("Open of snapshot %d after a crash: %v", closed, err))
				return
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || meta.Size != int64(sizes[closed]) || !bytes.Equal(got, snapshotData(closed, sizes[closed])) {
				h.Fail(fmt.Errorf("snapshot %d read back wrong after a crash: %d bytes (size %d), want %d; err %v",
					closed, len(got), meta.Size, sizes[closed], err))
				return
			}
		}
		_, trans := NewInmemTransportWithTimeout("store", time.Second)
		for index := closed + 1; h.Phase() == dsttest.Faulty; index++ {
			n := 1 + w.Rand().IntN(64<<10)
			sink, err := s.Create(SnapshotVersionMax, index, 1, Configuration{}, 0, trans)
			if err != nil {
				h.Fail(fmt.Errorf("Create %d: %v", index, err))
				return
			}
			data := snapshotData(index, n)
			for len(data) > 0 {
				k := 1 + w.Rand().IntN(16<<10)
				if k > len(data) { // raft's min shadows the builtin
					k = len(data)
				}
				if _, err := sink.Write(data[:k]); err != nil {
					h.Fail(fmt.Errorf("Write %d: %v", index, err))
					return
				}
				data = data[k:]
			}
			if w.Rand().IntN(8) == 0 {
				if err := sink.Cancel(); err != nil {
					h.Fail(fmt.Errorf("Cancel %d: %v", index, err))
					return
				}
				h.Sometimes("a snapshot was cancelled")
				continue
			}
			sizes[index], pending = n, index
			// A crash ends this goroutine inside Close: it doesn't return.
			if err := sink.Close(); err != nil {
				h.Fail(fmt.Errorf("Close %d: %v", index, err))
				return
			}
			closed, pending = index, 0
			w.Sleep(time.Duration(w.Rand().IntN(20)) * time.Millisecond)
		}
	})
	h.Expect("recovered with a snapshot in flight")
}

// Verifies: FileSnapshotStore's contract (snapStoreContract): a snapshot
// whose Close returned is durable and reads back whole; a crash inside
// Close leaves the new snapshot or the previous one. Holds on the disk
// model's journaled directories (fault-model.md), as on ext4 and XFS:
// FileSnapshotStore syncs the parent directory after the rename, not the
// snapshot's own directory, which bare POSIX would also require.
// Fails if: after a crash the latest snapshot isn't the last one closed
// (or the interrupted one), or reads back wrong; or if the test misses a
// store that skips its fsyncs (noSync).
func TestDSTSnapshotStoreContract(t *testing.T) {
	for _, retain := range []int{1, 2} {
		dsttest.Run(t, storeOpts(), func(h *dsttest.Harness) { snapStoreContract(h, retain, false) })
	}
	o := storeOpts()
	o.KeepGoing, o.DeterminismChecks = true, -1
	rep := dsttest.Sweep(o, func(h *dsttest.Harness) { snapStoreContract(h, 1, true) })
	if len(rep.Failed) == 0 {
		t.Fatalf("the contract test missed a store that skips fsync (%d seeds)", rep.Runs)
	}
	t.Logf("noSync: caught by %d of %d seeds; first: %v", len(rep.Failed), rep.Runs, rep.Failed[0].Err)
}
