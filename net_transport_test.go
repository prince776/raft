// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/stretchr/testify/require"
)

type testAddrProvider struct {
	addr string
}

func (t *testAddrProvider) ServerAddr(id ServerID) (ServerAddress, error) {
	return ServerAddress(t.addr), nil
}

func TestNetworkTransport_CloseStreams(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans1.Close() }()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         RPCHeader{Addr: []byte("cartman")},
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	// errCh is used to report errors from any of the goroutines
	// created in this test.
	// It is buffered as to not block.
	errCh := dst.MakeChan[error](theRealWorld(), 100)

	// Listen for a request
	theRealWorld().Go(func() {
		for {
			{
				var sel1 int
				var sel1v1 RPC
				dst.Select(theRealWorld(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel1 = 1; sel1v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel1 = 2 }),
				)
				switch sel1 {
				case 1:
					rpc := sel1v1
					// Verify the command
					req := rpc.Command.(*AppendEntriesRequest)
					if !reflect.DeepEqual(req, &args) {
						errCh.Send(fmt.Errorf("command mismatch: %#v %#v", *req, args))
						return
					}
					rpc.Respond(&resp, nil)

				case 2:
					return
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}
	})

	// Transport 2 makes outbound request, 3 conn pool
	trans2, err := NewTCPTransportWithLogger("localhost:0", nil, 3, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans2.Close() }()

	for i := 0; i < 2; i++ {
		// Create wait group
		wg := &dst.WaitGroup{}

		// Try to do parallel appends, should stress the conn pool
		for i = 0; i < 5; i++ {
			wg.Add(1)
			theRealWorld().Go(func() {
				defer wg.Done()
				var out AppendEntriesResponse
				if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
					errCh.Send(err)
					return
				}

				// Verify the response
				if !reflect.DeepEqual(resp, out) {
					errCh.Send(fmt.Errorf("command mismatch: %#v %#v", resp, out))
					return
				}
			})

		}

		// Wait for the routines to finish
		wg.Wait()

		// Check if we received any errors from the above goroutines.
		if errCh.Len() > 0 {
			t.Fatal(dstRecv(errCh))
		}

		// Check the conn pool size
		addr := trans1.LocalAddr()
		if len(trans2.connPool[addr]) != 3 {
			t.Fatalf("Expected 3 pooled conns!")
		}

		if i == 0 {
			trans2.CloseStreams()
			if len(trans2.connPool[addr]) != 0 {
				t.Fatalf("Expected no pooled conns after closing streams!")
			}
		}
	}
}

func TestNetworkTransport_StartStop(t *testing.T) {
	trans, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_ = trans.Close()
}

func TestNetworkTransport_Heartbeat_FastPath(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans1.Close() }()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:      10,
		RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersionMax, Addr: []byte("cartman")},
		Leader:    []byte("cartman"),
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	invoked := false
	fastpath := func(rpc RPC) {
		// Verify the command
		req := rpc.Command.(*AppendEntriesRequest)
		if !reflect.DeepEqual(req, &args) {
			t.Fatalf("command mismatch: %#v %#v", *req, args)
		}

		rpc.Respond(&resp, nil)
		invoked = true
	}
	trans1.SetHeartbeatHandler(fastpath)

	// Transport 2 makes outbound request
	trans2, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans2.Close() }()

	var out AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}

	// Ensure fast-path is used
	if !invoked {
		t.Fatalf("fast-path not used")
	}
}

func makeAppendRPC() AppendEntriesRequest {
	return AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         RPCHeader{Addr: []byte("cartman")},
	}
}

func makeAppendRPCResponse() AppendEntriesResponse {
	return AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}
}

func TestNetworkTransport_AppendEntries(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans1.Close() }()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := makeAppendRPC()
		resp := makeAppendRPCResponse()

		// Listen for a request
		theRealWorld().Go(func() {
			{
				var sel2 int
				var sel2v1 RPC
				dst.Select(theRealWorld(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel2 = 1; sel2v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel2 = 2 }),
				)
				switch sel2 {
				case 1:
					rpc := sel2v1
					// Verify the command
					req := rpc.Command.(*AppendEntriesRequest)
					if !reflect.DeepEqual(req, &args) {
						t.Errorf("command mismatch: %#v %#v", *req, args)
						return
					}

					rpc.Respond(&resp, nil)

				case 2:
					t.Errorf("timeout")
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		})

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans2.Close() }()

		var out AppendEntriesResponse
		if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

func TestNetworkTransport_AppendEntriesPipeline(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans1.Close() }()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := makeAppendRPC()
		resp := makeAppendRPCResponse()

		// Listen for a request
		theRealWorld().Go(func() {
			for i := 0; i < 10; i++ {
				{
					var sel3 int
					var sel3v1 RPC
					dst.Select(theRealWorld(),
						dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel3 = 1; sel3v1 = v }),
						dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel3 = 2 }),
					)
					switch sel3 {
					case 1:
						rpc := sel3v1
						// Verify the command
						req := rpc.Command.(*AppendEntriesRequest)
						if !reflect.DeepEqual(req, &args) {
							t.Errorf("command mismatch: %#v %#v", *req, args)
							return
						}
						rpc.Respond(&resp, nil)

					case 2:
						t.Errorf("timeout")
						return
					default:
						panic("dstmigrate: no select case ran")
					}
				}
			}
		})

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans2.Close() }()
		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		for i := 0; i < 10; i++ {
			out := new(AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				t.Fatalf("err: %v", err)
			}
		}

		respCh := pipeline.Consumer()
		for i := 0; i < 10; i++ {
			{
				var sel4 int
				var sel4v1 AppendFuture
				dst.Select(theRealWorld(),
					dst.OnRecv(respCh, func(v AppendFuture, _ bool) { sel4 = 1; sel4v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel4 = 2 }),
				)
				switch sel4 {
				case 1:
					ready := sel4v1
					// Verify the response
					if !reflect.DeepEqual(&resp, ready.Response()) {
						t.Fatalf("command mismatch: %#v %#v", &resp, ready.Response())
					}
				case 2:
					t.Fatalf("timeout")
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}
		_ = pipeline.Close()

	}
}

func TestNetworkTransport_AppendEntriesPipeline_CloseStreams(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := makeTransport(t, true, "localhost:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans1.Close() }()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := makeAppendRPC()
	resp := makeAppendRPCResponse()

	shutdownCh := dst.MakeChan[struct{}](theRealWorld(), 0)
	defer shutdownCh.Close()

	// Listen for a request
	theRealWorld().Go(func() {
		for {
			{
				var sel5 int
				var sel5v1 RPC
				dst.Select(theRealWorld(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel5 = 1; sel5v1 = v }),
					dst.OnRecv(shutdownCh, func(_ struct{}, _ bool) { sel5 = 2 }),
				)
				switch sel5 {
				case 1:
					rpc := sel5v1
					// Verify the command
					req := rpc.Command.(*AppendEntriesRequest)
					if !reflect.DeepEqual(req, &args) {
						t.Errorf("command mismatch: %#v %#v", *req, args)
						return
					}
					rpc.Respond(&resp, nil)

				case 2:
					return
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}
	})

	// Transport 2 makes outbound request
	trans2, err := makeTransport(t, true, string(trans1.LocalAddr()))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans2.Close() }()

	for _, cancelStreams := range []bool{true, false} {
		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		for i := 0; i < 100; i++ {
			// On the last one, close the streams on the transport one.
			if cancelStreams && i == 10 {
				trans1.CloseStreams()
				theRealWorld().Sleep(10 * time.Millisecond)
			}

			out := new(AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				break
			}
		}

		var futureErr error
		respCh := pipeline.Consumer()
	OUTER:
		for i := 0; i < 100; i++ {
			{
				var sel6 int
				var sel6v1 AppendFuture
				dst.Select(theRealWorld(),
					dst.OnRecv(respCh, func(v AppendFuture, _ bool) { sel6 = 1; sel6v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel6 = 2 }),
				)
				switch sel6 {
				case 1:
					ready := sel6v1
					if err := ready.Error(); err != nil {
						futureErr = err
						break OUTER
					}

					// Verify the response
					if !reflect.DeepEqual(&resp, ready.Response()) {
						t.Fatalf("command mismatch: %#v %#v %v", &resp, ready.Response(), ready.Error())
					}
				case 2:
					t.Fatalf("timeout when cancel streams is %v", cancelStreams)
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}

		if cancelStreams && futureErr == nil {
			t.Fatalf("expected an error due to the streams being closed")
		} else if !cancelStreams && futureErr != nil {
			t.Fatalf("unexpected error: %v", futureErr)
		}

		_ = pipeline.Close()
	}
}

func TestNetworkTransport_AppendEntriesPipeline_MaxRPCsInFlight(t *testing.T) {
	// Test the important cases 0 (default to 2), 1 (disabled), 2 and "some"
	for _, max := range []int{0, 1, 2, 10} {
		t.Run(fmt.Sprintf("max=%d", max), func(t *testing.T) {
			config := &NetworkTransportConfig{
				MaxPool:         2,
				MaxRPCsInFlight: max,
				Timeout:         time.Second,
				// Don't use test logger as the transport has multiple goroutines and
				// causes panics.
				ServerAddressProvider: &testAddrProvider{"localhost:0"},
			}

			// Transport 1 is consumer
			trans1, err := NewTCPTransportWithConfig("localhost:0", nil, config)
			require.NoError(t, err)
			defer func() { _ = trans1.Close() }()

			// Make the RPC request
			args := makeAppendRPC()
			resp := makeAppendRPCResponse()

			ctx, cancel := dst.WithTimeout(theRealWorld(), context.Background(), 5*time.Second)
			defer cancel()

			// Transport 2 makes outbound request
			config.ServerAddressProvider = &testAddrProvider{string(trans1.LocalAddr())}
			trans2, err := NewTCPTransportWithConfig("localhost:0", nil, config)
			require.NoError(t, err)
			defer func() { _ = trans2.Close() }()

			// Kill the transports on the timeout to unblock. That means things that
			// shouldn't have blocked did block.
			theRealWorld().Go(func() {
				<-ctx.Done()
				_ = trans2.Close()
				_ = trans1.Close()
			})

			// Attempt to pipeline
			pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
			if max == 1 {
				// Max == 1 implies no pipelining
				require.EqualError(t, err, ErrPipelineReplicationNotSupported.Error())
				return
			}
			require.NoError(t, err)

			expectedMax := max
			if max == 0 {
				// Should have defaulted to 2
				expectedMax = 2
			}

			for i := 0; i < expectedMax-1; i++ {
				// We should be able to send `max - 1` rpcs before `AppendEntries`
				// blocks. It blocks on the `max` one because it sends before pushing
				// to the chan. It will block forever when it does because nothing is
				// responding yet.
				out := new(AppendEntriesResponse)
				_, err := pipeline.AppendEntries(&args, out)
				require.NoError(t, err)
			}

			// Verify the next send blocks without blocking test forever
			errCh := dst.MakeChan[error](theRealWorld(), 1)
			theRealWorld().Go(func() {
				out := new(AppendEntriesResponse)
				_, err := pipeline.AppendEntries(&args, out)
				errCh.Send(err)
			})

			{
				var sel7 int
				var sel7v1 error
				dst.Select(theRealWorld(),
					dst.OnRecv(errCh, func(v error, _ bool) { sel7 = 1; sel7v1 = v }),
					dst.OnRecv(theRealWorld().After(50*time.Millisecond), func(_ time.Time, _ bool) { sel7 = 2 }),
				)
				switch sel7 {
				case 1:
					err := sel7v1
					require.NoError(t, err)
					t.Fatalf("AppendEntries didn't block with %d in flight", max)
				case 2:
					// OK it's probably blocked or we got _really_ unlucky with scheduling!
				default:
					panic("dstmigrate: no select case ran")
				}
			}

			// Verify that once we receive/respond another one can be sent.
			rpc, _ := trans1.Consumer().Recv()
			rpc.Respond(resp, nil)

			// We also need to consume the response from the pipeline in case chan is
			// unbuffered (inflight is 2 or 1)
			pipeline.Consumer().Recv()

			// The last append should unblock once the response is received.
			{
				var sel8 int
				dst.Select(theRealWorld(),
					dst.OnRecv(errCh, func(_ error, _ bool) { sel8 = 1 }),
					dst.OnRecv(theRealWorld().After(50*time.Millisecond), func(_ time.Time, _ bool) { sel8 = 2 }),
				)
				switch sel8 {
				case 1:
					// OK
				case 2:
					t.Fatalf("last append didn't unblock")
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		})
	}
}

func TestNetworkTransport_RequestVote(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans1.Close() }()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := RequestVoteRequest{
			Term:         20,
			LastLogIndex: 100,
			LastLogTerm:  19,
			RPCHeader:    RPCHeader{Addr: []byte("butters")},
		}

		resp := RequestVoteResponse{
			Term:    100,
			Granted: false,
		}

		// Listen for a request
		theRealWorld().Go(func() {
			{
				var sel9 int
				var sel9v1 RPC
				dst.Select(theRealWorld(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel9 = 1; sel9v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel9 = 2 }),
				)
				switch sel9 {
				case 1:
					rpc := sel9v1
					// Verify the command
					req := rpc.Command.(*RequestVoteRequest)
					if !reflect.DeepEqual(req, &args) {
						t.Errorf("command mismatch: %#v %#v", *req, args)
						return
					}

					rpc.Respond(&resp, nil)

				case 2:
					t.Errorf("timeout")
					return
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		})

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans2.Close() }()
		var out RequestVoteResponse
		if err := trans2.RequestVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

func TestNetworkTransport_InstallSnapshot(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans1.Close() }()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := InstallSnapshotRequest{
			Term:         10,
			LastLogIndex: 100,
			LastLogTerm:  9,
			Peers:        []byte("blah blah"),
			Size:         10,
			RPCHeader:    RPCHeader{Addr: []byte("kyle")},
		}

		resp := InstallSnapshotResponse{
			Term:    10,
			Success: true,
		}

		// Listen for a request
		theRealWorld().Go(func() {
			{
				var sel10 int
				var sel10v1 RPC
				dst.Select(theRealWorld(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel10 = 1; sel10v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel10 = 2 }),
				)
				switch sel10 {
				case 1:
					rpc := sel10v1
					// Verify the command
					req := rpc.Command.(*InstallSnapshotRequest)
					if !reflect.DeepEqual(req, &args) {
						t.Errorf("command mismatch: %#v %#v", *req, args)
						return
					}

					// Try to read the bytes
					buf := make([]byte, 10)
					_, _ = rpc.Reader.Read(buf)

					// Compare
					if !bytes.Equal(buf, []byte("0123456789")) {
						t.Errorf("bad buf %v", buf)
						return
					}

					rpc.Respond(&resp, nil)

				case 2:
					t.Errorf("timeout")
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		})

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = trans2.Close() }()
		// Create a buffer
		buf := bytes.NewBuffer([]byte("0123456789"))

		var out InstallSnapshotResponse
		if err := trans2.InstallSnapshot("id1", trans1.LocalAddr(), &args, &out, buf); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

func TestNetworkTransport_EncodeDecode(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans1.Close() }()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != local {
		t.Fatalf("enc/dec fail: %v %v", dec, local)
	}
}

func TestNetworkTransport_EncodeDecode_AddressProvider(t *testing.T) {
	addressOverride := "localhost:11111"
	config := &NetworkTransportConfig{MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t), ServerAddressProvider: &testAddrProvider{addressOverride}}
	trans1, err := NewTCPTransportWithConfig("localhost:0", nil, config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans1.Close() }()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != ServerAddress(addressOverride) {
		t.Fatalf("enc/dec fail: %v %v", dec, addressOverride)
	}
}

func TestNetworkTransport_PooledConn(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans1.Close() }()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         RPCHeader{Addr: []byte("cartman")},
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	// errCh is used to report errors from any of the goroutines
	// created in this test.
	// It is buffered as to not block.
	errCh := dst.MakeChan[error](theRealWorld(), 100)

	// Listen for a request
	theRealWorld().Go(func() {
		for {
			{
				var sel11 int
				var sel11v1 RPC
				dst.Select(theRealWorld(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel11 = 1; sel11v1 = v }),
					dst.OnRecv(theRealWorld().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel11 = 2 }),
				)
				switch sel11 {
				case 1:
					rpc := sel11v1
					// Verify the command
					req := rpc.Command.(*AppendEntriesRequest)
					if !reflect.DeepEqual(req, &args) {
						errCh.Send(fmt.Errorf("command mismatch: %#v %#v", *req, args))
						return
					}
					rpc.Respond(&resp, nil)

				case 2:
					return
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}
	})

	// Transport 2 makes outbound request, 3 conn pool
	trans2, err := NewTCPTransportWithLogger("localhost:0", nil, 3, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = trans2.Close() }()

	// Create wait group
	wg := &dst.WaitGroup{}

	// Try to do parallel appends, should stress the conn pool
	for i := 0; i < 5; i++ {
		wg.Add(1)

		theRealWorld().Go(func() {
			defer wg.Done()
			var out AppendEntriesResponse
			if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
				errCh.Send(err)
				return
			}

			// Verify the response
			if !reflect.DeepEqual(resp, out) {
				errCh.Send(fmt.Errorf("command mismatch: %#v %#v", resp, out))
				return
			}
		})

	}

	// Wait for the routines to finish
	wg.Wait()

	// Check if we received any errors from the above goroutines.
	if errCh.Len() > 0 {
		t.Fatal(dstRecv(errCh))
	}

	// Check the conn pool size
	addr := trans1.LocalAddr()
	if len(trans2.connPool[addr]) != 3 {
		t.Fatalf("Expected 3 pooled conns!")
	}
}

func makeTransport(t *testing.T, useAddrProvider bool, addressOverride string) (*NetworkTransport, error) {
	config := &NetworkTransportConfig{
		MaxPool: 2,
		// Setting this because older tests for pipelining were written when this
		// was a constant and block forever if it's not large enough.
		MaxRPCsInFlight: 130,
		Timeout:         time.Second,
		Logger:          newTestLogger(t),
	}
	if useAddrProvider {
		config.ServerAddressProvider = &testAddrProvider{addressOverride}
	}
	return NewTCPTransportWithConfig("localhost:0", nil, config)
}

type testCountingWriter struct {
	t        *testing.T
	numCalls *int32
}

func (tw testCountingWriter) Write(p []byte) (n int, err error) {
	atomic.AddInt32(tw.numCalls, 1)
	if !strings.Contains(string(p), "failed to accept connection") {
		tw.t.Error("did not receive expected log message")
	}
	tw.t.Log("countingWriter:", string(p))
	return len(p), nil
}

type testCountingStreamLayer struct {
	numCalls *int32
}

func (sl testCountingStreamLayer) Accept() (net.Conn, error) {
	*sl.numCalls++
	return nil, fmt.Errorf("intentional error in test")
}

func (sl testCountingStreamLayer) Close() error {
	return nil
}

func (sl testCountingStreamLayer) Addr() net.Addr {
	panic("not needed")
}

func (sl testCountingStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return nil, fmt.Errorf("not needed")
}

// TestNetworkTransport_ListenBackoff tests that Accept() errors in NetworkTransport#listen()
// do not result in a tight loop and spam the log. We verify this here by counting the number
// of calls against Accept() and the logger
func TestNetworkTransport_ListenBackoff(t *testing.T) {
	// testTime is the amount of time we will allow NetworkTransport#listen() to run
	// This needs to be long enough that to verify that maxDelay is in force,
	// but not so long as to be obnoxious when running the test suite.
	const testTime = 4 * time.Second

	var numAccepts int32
	var numLogs int32
	countingWriter := testCountingWriter{t, &numLogs}
	countingLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "test",
		Output: countingWriter,
		Level:  hclog.DefaultLevel,
	})
	transport := NetworkTransport{
		w:          theRealWorld(),
		logger:     countingLogger,
		stream:     testCountingStreamLayer{&numAccepts},
		shutdownCh: dst.MakeChan[struct{}](theRealWorld(), 0),
	}

	theRealWorld().Go(func() { transport.listen() })

	// sleep (+yield) for testTime seconds before asking the accept loop to shut down
	theRealWorld().Sleep(testTime)
	_ = transport.Close()

	// Verify that the method exited (but without block this test)
	// maxDelay == 1s, so we will give the routine 1.25s to loop around and shut down.
	{
		var sel12 int
		dst.Select(theRealWorld(),
			dst.OnRecv(transport.shutdownCh, func(_ struct{}, _ bool) { sel12 = 1 }),
			dst.OnRecv(theRealWorld().After(1250*time.Millisecond), func(_ time.Time, _ bool) { sel12 = 2 }),
		)
		switch sel12 {
		case 1:
		case 2:
			t.Error("timed out waiting for NetworkTransport to shut down")
		default:
			panic("dstmigrate: no select case ran")
		}
	}
	require.True(t, transport.shutdown)

	// In testTime==4s, we expect to loop approximately 12 times
	// with the following delays (in ms):
	//   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
	// Too few calls suggests that the minDelay is not in force; too many calls suggests that the
	// maxDelay is not in force or that the back-off isn't working at all.
	// We'll leave a little flex; the important thing here is the asymptotic behavior.
	// If the minDelay or maxDelay in NetworkTransport#listen() are modified, this test may fail
	// and need to be adjusted.
	require.True(t, numAccepts > 10)
	require.True(t, numAccepts < 13)
	require.True(t, numLogs > 10)
	require.True(t, numLogs < 13)
}
