// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/realworld"
	"github.com/stretchr/testify/require"
)

func TestInmemTransportImpl(t *testing.T) {
	var inm interface{} = &InmemTransport{}
	if _, ok := inm.(Transport); !ok {
		t.Fatalf("InmemTransport is not a Transport")
	}
	if _, ok := inm.(LoopbackTransport); !ok {
		t.Fatalf("InmemTransport is not a Loopback Transport")
	}
	if _, ok := inm.(WithPeers); !ok {
		t.Fatalf("InmemTransport is not a WithPeers Transport")
	}
}

func TestInmemTransportWriteTimeout(t *testing.T) {
	// InmemTransport should timeout if the other end has gone away
	// when it tries to send a request.
	// Use unbuffered channels so that we can see the write failing
	// without having to contrive to fill up the buffer first.
	timeout := 10 * time.Millisecond
	t1 := &InmemTransport{
		w:          realworld.Default(),
		consumerCh: dst.MakeChan[RPC](realworld.Default(), 0),
		localAddr:  NewInmemAddr(),
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	t2 := &InmemTransport{
		w:          realworld.Default(),
		consumerCh: dst.MakeChan[RPC](realworld.Default(), 0),
		localAddr:  NewInmemAddr(),
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	a2 := t2.LocalAddr()
	t1.Connect(a2, t2)

	stop := dst.MakeChan[struct{}](realworld.Default(), 0)
	stopped := dst.MakeChan[struct{}](realworld.Default(), 0)
	realworld.Default().Go(func() {
		defer stopped.Close()
		var i uint64
		for {
			{
				var sel1 int
				var sel1v2 RPC
				dst.Select(realworld.Default(),
					dst.OnRecv(stop, func(_ struct{}, _ bool) { sel1 = 1 }),
					dst.OnRecv(t2.Consumer(), func(v RPC, _ bool) { sel1 = 2; sel1v2 = v }),
				)
				switch sel1 {
				case 1:
					return
				case 2:
					rpc := sel1v2
					i++
					rpc.Respond(&AppendEntriesResponse{
						Success: true,
						LastLog: i,
					}, nil)
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}
	})

	var resp AppendEntriesResponse
	// Sanity check that sending is working before stopping the
	// responder.
	err := t1.AppendEntries("server1", a2, &AppendEntriesRequest{}, &resp)
	NoErr(err, t)
	require.True(t, resp.LastLog == 1)

	stop.Close()
	{
		var sel2 int
		dst.Select(realworld.Default(),
			dst.OnRecv(stopped, func(_ struct{}, _ bool) { sel2 = 1 }),
			dst.OnRecv(realworld.Default().After(time.Second), func(_ time.Time, _ bool) { sel2 = 2 }),
		)
		switch sel2 {
		case 1:
		case 2:
			t.Fatalf("timed out waiting for responder to stop")
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	err = t1.AppendEntries("server1", a2, &AppendEntriesRequest{}, &resp)
	if err == nil {
		t.Fatalf("expected AppendEntries to time out")
	}
	if err.Error() != "send timed out" {
		t.Fatalf("unexpected error: %v", err)
	}
}
