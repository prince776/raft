// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/realworld"
)

const (
	TTInmem = iota

	// NOTE: must be last
	numTestTransports
)

func NewTestTransport(ttype int, addr ServerAddress) (ServerAddress, LoopbackTransport) {
	switch ttype {
	case TTInmem:
		return NewInmemTransport(addr)
	default:
		panic("Unknown transport type")
	}
}

func TestTransport_StartStop(t *testing.T) {
	for ttype := 0; ttype < numTestTransports; ttype++ {
		_, trans := NewTestTransport(ttype, "")
		if err := trans.Close(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
}

func TestTransport_AppendEntries(t *testing.T) {
	for ttype := 0; ttype < numTestTransports; ttype++ {
		addr1, trans1 := NewTestTransport(ttype, "")
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

		// Listen for a request
		realworld.Default().Go(func() {
			{
				var sel1 int
				var sel1v1 RPC
				dst.Select(realworld.Default(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel1 = 1; sel1v1 = v }),
					dst.OnRecv(realworld.Default().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel1 = 2 }),
				)
				switch sel1 {
				case 1:
					rpc := sel1v1
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
		addr2, trans2 := NewTestTransport(ttype, "")
		defer func() { _ = trans2.Close() }()

		trans1.Connect(addr2, trans2)
		trans2.Connect(addr1, trans1)

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

func TestTransport_AppendEntriesPipeline(t *testing.T) {
	for ttype := 0; ttype < numTestTransports; ttype++ {
		addr1, trans1 := NewTestTransport(ttype, "")
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

		// Listen for a request
		realworld.Default().Go(func() {
			for i := 0; i < 10; i++ {
				{
					var sel2 int
					var sel2v1 RPC
					dst.Select(realworld.Default(),
						dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel2 = 1; sel2v1 = v }),
						dst.OnRecv(realworld.Default().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel2 = 2 }),
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
						return
					default:
						panic("dstmigrate: no select case ran")
					}
				}
			}
		})

		// Transport 2 makes outbound request
		addr2, trans2 := NewTestTransport(ttype, "")
		defer func() { _ = trans2.Close() }()

		trans1.Connect(addr2, trans2)
		trans2.Connect(addr1, trans1)

		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = pipeline.Close() }()
		for i := 0; i < 10; i++ {
			out := new(AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				t.Fatalf("err: %v", err)
			}
		}

		respCh := pipeline.Consumer()
		for i := 0; i < 10; i++ {
			{
				var sel3 int
				var sel3v1 AppendFuture
				dst.Select(realworld.Default(),
					dst.OnRecv(respCh, func(v AppendFuture, _ bool) { sel3 = 1; sel3v1 = v }),
					dst.OnRecv(realworld.Default().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel3 = 2 }),
				)
				switch sel3 {
				case 1:
					ready := sel3v1
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
	}
}

func TestTransport_RequestVote(t *testing.T) {
	for ttype := 0; ttype < numTestTransports; ttype++ {
		addr1, trans1 := NewTestTransport(ttype, "")
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
		realworld.Default().Go(func() {
			{
				var sel4 int
				var sel4v1 RPC
				dst.Select(realworld.Default(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel4 = 1; sel4v1 = v }),
					dst.OnRecv(realworld.Default().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel4 = 2 }),
				)
				switch sel4 {
				case 1:
					rpc := sel4v1
					// Verify the command
					req := rpc.Command.(*RequestVoteRequest)
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
		addr2, trans2 := NewTestTransport(ttype, "")
		defer func() { _ = trans2.Close() }()

		trans1.Connect(addr2, trans2)
		trans2.Connect(addr1, trans1)

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

func TestTransport_InstallSnapshot(t *testing.T) {
	for ttype := 0; ttype < numTestTransports; ttype++ {
		addr1, trans1 := NewTestTransport(ttype, "")
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
		realworld.Default().Go(func() {
			{
				var sel5 int
				var sel5v1 RPC
				dst.Select(realworld.Default(),
					dst.OnRecv(rpcCh, func(v RPC, _ bool) { sel5 = 1; sel5v1 = v }),
					dst.OnRecv(realworld.Default().After(200*time.Millisecond), func(_ time.Time, _ bool) { sel5 = 2 }),
				)
				switch sel5 {
				case 1:
					rpc := sel5v1
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
		addr2, trans2 := NewTestTransport(ttype, "")
		defer func() { _ = trans2.Close() }()

		trans1.Connect(addr2, trans2)
		trans2.Connect(addr1, trans1)

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

func TestTransport_EncodeDecode(t *testing.T) {
	for ttype := 0; ttype < numTestTransports; ttype++ {
		_, trans1 := NewTestTransport(ttype, "")
		defer func() { _ = trans1.Close() }()

		local := trans1.LocalAddr()
		enc := trans1.EncodePeer("aaaa", local)
		dec := trans1.DecodePeer(enc)

		if dec != local {
			t.Fatalf("enc/dec fail: %v %v", dec, local)
		}
	}
}
