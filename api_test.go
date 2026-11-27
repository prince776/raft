// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// crashingStore is an InmemStore whose first StoreLog, or first SetUint64
// of the current term, fails: what the store holds afterwards is what a
// crash at that point would leave on disk.
type crashingStore struct {
	*InmemStore
	failStoreLog, failSetTerm bool
}

func (s *crashingStore) StoreLog(l *Log) error {
	if s.failStoreLog {
		s.failStoreLog = false
		return errors.New("crash")
	}
	return s.InmemStore.StoreLog(l)
}

func (s *crashingStore) SetUint64(key []byte, val uint64) error {
	if s.failSetTerm && bytes.Equal(key, keyCurrentTerm) {
		s.failSetTerm = false
		return errors.New("crash")
	}
	return s.InmemStore.SetUint64(key, val)
}

// TestBootstrapCluster_CrashBetweenWrites checks that a server that crashes
// part way through BootstrapCluster can still come up as a working cluster:
// restarted the usual way (bootstrap unless HasExistingState), it must be
// able to elect itself.
func TestBootstrapCluster_CrashBetweenWrites(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		failStoreLog, failSetTerm bool
	}{
		{"crash before the configuration entry", true, false},
		{"crash before the term", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := inmemConfig(t)
			conf.LocalID = "s1"
			store := &crashingStore{InmemStore: NewInmemStore(), failStoreLog: tc.failStoreLog, failSetTerm: tc.failSetTerm}
			snaps := NewInmemSnapshotStore()
			_, trans := NewInmemTransport("s1")
			configuration := Configuration{Servers: []Server{{ID: "s1", Address: trans.LocalAddr()}}}

			if err := BootstrapCluster(conf, store, store, snaps, trans, configuration); err == nil {
				t.Fatal("the injected crash didn't happen")
			}

			// Restart: bootstrap unless there's existing state, then start.
			hasState, err := HasExistingState(store, store, snaps)
			if err != nil {
				t.Fatal(err)
			}
			if !hasState {
				if err := BootstrapCluster(conf, store, store, snaps, trans, configuration); err != nil {
					t.Fatalf("bootstrapping again: %v", err)
				}
			}
			r, err := NewRaft(conf, &MockFSM{}, store, store, snaps, trans)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Shutdown()
			select {
			case <-r.LeaderCh():
			case <-time.After(5 * time.Second):
				t.Fatalf("the server never became leader (state %v, configuration %v)",
					r.State(), r.configurations.latest.Servers)
			}
			if err := r.Apply([]byte("x"), time.Second).Error(); err != nil {
				t.Fatalf("apply: %v", err)
			}
		})
	}
}
