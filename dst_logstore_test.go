package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path"

	"github.com/prince776/go-dst-kit/dst"
)

// dstLogStore is a LogStore and StableStore on a World's file system, for
// simulation tests. Raft's interfaces don't say what a store guarantees;
// this one states it, and the simulated disk (lost unsynced writes, torn
// writes, power loss mid-call) tests raft against it:
//
//   - Durable on return: every call that changes the store returns only
//     after its change is fsynced. A crash after the return keeps it.
//   - Atomic: each call is one checksummed record. A crash during a call
//     keeps all of it or none of it; StoreLogs's batch included.
//   - Recovery: opening replays the file and truncates a torn or corrupt
//     tail (the record a crash interrupted), then reads as of the last
//     whole record.
//   - Reads see every completed call. GetLog of a missing index is
//     ErrLogNotFound; FirstIndex and LastIndex are 0 for an empty log;
//     Get of a missing key is an error, GetUint64 of one is 0 (as
//     InmemStore: raft's startup relies on the latter, which the
//     StableStore docs don't say).
//
// The weaker modes break one guarantee each, to learn which raft relies
// on: noSync returns before the fsync (a crash may lose acknowledged
// calls); splitBatches writes a StoreLogs batch as one record per entry
// (a crash may keep only a prefix of it).
//
// Found (2026-11-27): raft needs durability on return (noSync loses
// acknowledged writes: TestDSTCatchesUnsyncedStore), but not batch
// atomicity, so long as a crash keeps a prefix of the batch
// (splitBatches: 200 crash seeds, 73,292 committed writes, no failure).
// A store that could keep a later entry of a batch without an earlier
// one (a gap) isn't tested.
type dstLogStore struct {
	mu   dst.Mutex
	fsys dst.FS
	f    dst.File
	off  int64

	logs        map[uint64]*Log
	first, last uint64
	kv          map[string][]byte

	noSync, splitBatches bool
}

// storeOp is one record: a change to the store.
type storeOp struct {
	Logs     []*Log `json:",omitempty"` // StoreLogs
	Min, Max uint64 `json:",omitempty"` // DeleteRange
	Del      bool   `json:",omitempty"`
	Key      []byte `json:",omitempty"` // Set / SetUint64
	Val      []byte `json:",omitempty"`
}

// openDSTLogStore opens (or creates) the store in file name of fsys.
func openDSTLogStore(fsys dst.FS, name string) (*dstLogStore, error) {
	s := &dstLogStore{fsys: fsys, logs: map[uint64]*Log{}, kv: map[string][]byte{}}
	_, statErr := fsys.Stat(name)
	f, err := fsys.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	if errors.Is(statErr, fs.ErrNotExist) {
		// A new file: make its directory entry durable before any record.
		if err := dst.SyncDir(fsys, path.Dir(name)); err != nil {
			return nil, err
		}
	}
	data, err := dst.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	off := int64(0)
	for {
		op, n, ok := decodeRecord(data[off:])
		if !ok {
			break
		}
		s.apply(op)
		off += int64(n)
	}
	if off < int64(len(data)) {
		// A torn or corrupt tail: the record a crash interrupted.
		if err := f.Truncate(off); err != nil {
			return nil, err
		}
		if err := f.Sync(); err != nil {
			return nil, err
		}
	}
	s.off = off
	return s, nil
}

// Records are [len uint32][crc32c uint32][json].
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func decodeRecord(b []byte) (storeOp, int, bool) {
	var op storeOp
	if len(b) < 8 {
		return op, 0, false
	}
	n := binary.LittleEndian.Uint32(b)
	if uint64(len(b)) < 8+uint64(n) || crc32.Checksum(b[8:8+n], crcTable) != binary.LittleEndian.Uint32(b[4:]) {
		return op, 0, false
	}
	if json.Unmarshal(b[8:8+n], &op) != nil {
		return op, 0, false
	}
	return op, 8 + int(n), true
}

func (s *dstLogStore) apply(op storeOp) {
	switch {
	case op.Key != nil:
		s.kv[string(op.Key)] = op.Val
	case op.Del:
		for i := op.Min; i <= op.Max; i++ {
			delete(s.logs, i)
		}
		s.bounds()
	default:
		for _, l := range op.Logs {
			s.logs[l.Index] = l
		}
		s.bounds()
	}
}

// bounds recomputes first and last (the log is small in a simulation).
func (s *dstLogStore) bounds() {
	s.first, s.last = 0, 0
	for i := range s.logs {
		if s.first == 0 || i < s.first {
			s.first = i
		}
		if i > s.last {
			s.last = i
		}
	}
}

// write appends op as one record and (unless noSync) fsyncs it.
func (s *dstLogStore) write(op storeOp) error {
	payload, err := json.Marshal(op)
	if err != nil {
		return err
	}
	rec := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(rec, uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:], crc32.Checksum(payload, crcTable))
	copy(rec[8:], payload)
	if _, err := s.f.WriteAt(rec, s.off); err != nil {
		return err
	}
	if !s.noSync {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	s.off += int64(len(rec))
	s.apply(op)
	return nil
}

// durableAt reports what the store durably holds at index: the entry's
// term if it has the entry, and whether the index is below its first
// entry (compacted into a snapshot). For checks between scheduling steps
// only: it takes no lock (a check can't use the World), and it relies on
// write applying an operation only after its fsync, so the maps hold
// exactly what a crash would keep.
func (s *dstLogStore) durableAt(index uint64) (term uint64, has, compacted bool) {
	if l, ok := s.logs[index]; ok {
		return l.Term, true, false
	}
	return 0, false, s.first > index
}

func (s *dstLogStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first, nil
}

func (s *dstLogStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, nil
}

func (s *dstLogStore) GetLog(index uint64, log *Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

func (s *dstLogStore) StoreLog(log *Log) error { return s.StoreLogs([]*Log{log}) }

func (s *dstLogStore) StoreLogs(logs []*Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.splitBatches {
		for _, l := range logs {
			if err := s.write(storeOp{Logs: []*Log{l}}); err != nil {
				return err
			}
		}
		return nil
	}
	return s.write(storeOp{Logs: logs})
}

func (s *dstLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.write(storeOp{Del: true, Min: min, Max: max})
}

func (s *dstLogStore) Set(key, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.write(storeOp{Key: key, Val: append([]byte{}, val...)})
}

func (s *dstLogStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[string(key)]
	if !ok {
		return nil, errors.New("not found")
	}
	return v, nil
}

func (s *dstLogStore) SetUint64(key []byte, val uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], val)
	return s.Set(key, b[:])
}

func (s *dstLogStore) GetUint64(key []byte) (uint64, error) {
	s.mu.Lock()
	v, ok := s.kv[string(key)]
	s.mu.Unlock()
	if !ok {
		return 0, nil // missing is 0, as InmemStore (raft's startup relies on it)
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("value of %q is %d bytes", key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}
