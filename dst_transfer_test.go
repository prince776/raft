package raft

import (
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// Leadership transfers during the faults (phase 2 step 3; Antithesis's
// #696: an interrupted transfer leaves "leadership transfer in progress"
// set, and the re-elected leader refuses writes). A per-seed feature: the
// leader now and then hands leadership on, to a voter of its choosing or
// raft's. Each transfer must resolve (succeed or fail) within patience,
// and a stuck flag shows as "the cluster recovers" failing (the probe
// write refused with ErrLeadershipTransferInProgress for good).
func (c *simCluster) transfers(w *dst.World, nd *dstNode) {
	h := c.h
	for h.Phase() == dsttest.Faulty {
		w.Sleep(time.Duration(300+w.Rand().IntN(1200)) * time.Millisecond)
		if h.Phase() != dsttest.Faulty || nd.raft.State() != Leader {
			continue
		}
		var f Future
		target := "raft's choice"
		var others []Server
		for _, s := range nd.raft.configurations.latest.Servers {
			if s.Suffrage == Voter && s.ID != nd.id() {
				others = append(others, s)
			}
		}
		if len(others) > 0 && w.Rand().IntN(2) == 0 {
			s := others[w.Rand().IntN(len(others))]
			target = string(s.ID)
			f = nd.raft.LeadershipTransferToServer(s.ID, s.Address)
		} else {
			f = nd.raft.LeadershipTransfer()
		}
		start := w.Now()
		err := f.Error()
		if took := w.Now().Sub(start); took > c.patience() {
			h.Failf("%s's leadership transfer to %s took %v (bound %v), then: %v", nd.name, target, took, c.patience(), err)
			return
		}
		if err == nil {
			h.Sometimes("a leadership transfer succeeded")
		} else {
			h.Sometimes("a leadership transfer failed")
		}
	}
}
