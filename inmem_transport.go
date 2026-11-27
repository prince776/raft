// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// NewInmemAddr returns a new in-memory addr with
// a randomly generate UUID as the ID.
func NewInmemAddr() ServerAddress {
	return ServerAddress(generateUUID(theRealWorld()))
}

// inmemPipeline is used to pipeline requests for the in-mem transport.
type inmemPipeline struct {
	trans    *InmemTransport
	peer     *InmemTransport
	peerAddr ServerAddress

	doneCh       dst.Chan[AppendFuture]
	inprogressCh dst.Chan[*inmemPipelineInflight]

	shutdown     bool
	shutdownCh   dst.Chan[struct{}]
	shutdownLock dst.RWMutex
}

type inmemPipelineInflight struct {
	future *appendFuture
	respCh dst.Chan[RPCResponse]
}

// InmemTransport Implements the Transport interface, to allow Raft to be
// tested in-memory without going over a network.
type InmemTransport struct {
	dst.RWMutex
	consumerCh dst.Chan[RPC]
	localAddr  ServerAddress
	peers      map[ServerAddress]*InmemTransport
	pipelines  []*inmemPipeline
	timeout    time.Duration

	w *dst.World
}

// NewInmemTransportWithTimeout is used to initialize a new transport and
// generates a random local address if none is specified. The given timeout
// will be used to decide how long to wait for a connected peer to process the
// RPCs that we're sending it. See also Connect() and Consumer().
func NewInmemTransportWithTimeout(addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {
	return NewInmemTransportWithWorld(theRealWorld(), addr, timeout)
}

// NewInmemTransportWithWorld is NewInmemTransportWithTimeout in the World w.
func NewInmemTransportWithWorld(w *dst.World, addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {
	if string(addr) == "" {
		addr = NewInmemAddr()
	}
	trans := &InmemTransport{
		w:          w,
		consumerCh: dst.MakeChan[RPC](w, 16),
		localAddr:  addr,
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	return addr, trans
}

// NewInmemTransport is used to initialize a new transport
// and generates a random local address if none is specified
func NewInmemTransport(addr ServerAddress) (ServerAddress, *InmemTransport) {
	return NewInmemTransportWithTimeout(addr, 500*time.Millisecond)
}

// SetHeartbeatHandler is used to set optional fast-path for
// heartbeats, not supported for this transport.
func (i *InmemTransport) SetHeartbeatHandler(cb func(RPC)) {
}

// Consumer implements the Transport interface.
func (i *InmemTransport) Consumer() dst.Chan[RPC] {
	return i.consumerCh
}

// LocalAddr implements the Transport interface.
func (i *InmemTransport) LocalAddr() ServerAddress {
	return i.localAddr
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (i *InmemTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	i.Lock()
	defer i.Unlock()

	peer, ok := i.peers[target]
	if !ok {
		return nil, fmt.Errorf("failed to connect to peer: %v", target)
	}
	pipeline := newInmemPipeline(i, peer, target)
	i.pipelines = append(i.pipelines, pipeline)
	return pipeline, nil
}

// AppendEntries implements the Transport interface.
func (i *InmemTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

// RequestVote implements the Transport interface.
func (i *InmemTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestPreVoteResponse)
	*resp = *out
	return nil
}

// InstallSnapshot implements the Transport interface.
func (i *InmemTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := i.makeRPC(target, args, data, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*InstallSnapshotResponse)
	*resp = *out
	return nil
}

// TimeoutNow implements the Transport interface.
func (i *InmemTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, 10*i.timeout)
	if err != nil {
		return err
	}

	// Copy the result back
	out := rpcResp.Response.(*TimeoutNowResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target ServerAddress, args interface{}, r io.Reader, timeout time.Duration) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := dst.MakeChan[RPCResponse](i.w, 1)
	req := RPC{
		Command:  args,
		Reader:   r,
		RespChan: respCh,
	}
	{
		var sel1 int
		dst.Select(i.w,
			dst.OnSend(peer.consumerCh, req, func() { sel1 = 1 }),
			dst.OnRecv(i.w.After(timeout), func(_ time.Time, _ bool) { sel1 = 2 }),
		)
		switch sel1 {
		case 1:
		case 2:
			err = fmt.Errorf("send timed out")
			return
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	// Wait for a response
	{
		var sel2 int
		var sel2v1 RPCResponse
		dst.Select(i.w,
			dst.OnRecv(respCh, func(v RPCResponse, _ bool) { sel2 = 1; sel2v1 = v }),
			dst.OnRecv(i.w.After(timeout), func(_ time.Time, _ bool) { sel2 = 2 }),
		)
		switch sel2 {
		case 1:
			rpcResp = sel2v1
			if rpcResp.Error != nil {
				err = rpcResp.Error
			}
		case 2:
			err = fmt.Errorf("command timed out")
		default:
			panic("dstmigrate: no select case ran")
		}
	}
	return
}

// EncodePeer implements the Transport interface.
func (i *InmemTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	return []byte(p)
}

// DecodePeer implements the Transport interface.
func (i *InmemTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (i *InmemTransport) Connect(peer ServerAddress, t Transport) {
	trans := t.(*InmemTransport)
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}

// Disconnect is used to remove the ability to route to a given peer.
func (i *InmemTransport) Disconnect(peer ServerAddress) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer)

	// Disconnect any pipelines
	n := len(i.pipelines)
	for idx := 0; idx < n; idx++ {
		if i.pipelines[idx].peerAddr == peer {
			_ = i.pipelines[idx].Close()
			i.pipelines[idx], i.pipelines[n-1] = i.pipelines[n-1], nil
			idx--
			n--
		}
	}
	i.pipelines = i.pipelines[:n]
}

// DisconnectAll is used to remove all routes to peers.
func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[ServerAddress]*InmemTransport)

	// Handle pipelines
	for _, pipeline := range i.pipelines {
		_ = pipeline.Close()
	}
	i.pipelines = nil
}

// Close is used to permanently disable the transport
func (i *InmemTransport) Close() error {
	i.DisconnectAll()
	return nil
}

func newInmemPipeline(trans *InmemTransport, peer *InmemTransport, addr ServerAddress) *inmemPipeline {
	i := &inmemPipeline{
		trans:        trans,
		peer:         peer,
		peerAddr:     addr,
		doneCh:       dst.MakeChan[AppendFuture](trans.w, 16),
		inprogressCh: dst.MakeChan[*inmemPipelineInflight](trans.w, 16),
		shutdownCh:   dst.MakeChan[struct{}](trans.w, 0),
	}
	trans.w.Go(func() { i.decodeResponses() })
	return i
}

func (i *inmemPipeline) decodeResponses() {
	timeout := i.trans.timeout
	for {
		{
			var sel3 int
			var sel3v1 *inmemPipelineInflight
			dst.Select(i.trans.w,
				dst.OnRecv(i.inprogressCh, func(v *inmemPipelineInflight, _ bool) { sel3 = 1; sel3v1 = v }),
				dst.OnRecv(i.shutdownCh, func(_ struct{}, _ bool) { sel3 = 2 }),
			)
			switch sel3 {
			case 1:
				inp := sel3v1
				var timeoutCh dst.Chan[time.Time]
				if timeout > 0 {
					timeoutCh = i.trans.w.After(timeout)
				}

				{
					var sel4 int
					var sel4v1 RPCResponse
					dst.Select(i.trans.w,
						dst.OnRecv(inp.respCh, func(v RPCResponse, _ bool) { sel4 = 1; sel4v1 = v }),
						dst.OnRecv(timeoutCh, func(_ time.Time, _ bool) { sel4 = 2 }),
						dst.OnRecv(i.shutdownCh, func(_ struct{}, _ bool) { sel4 = 3 }),
					)
					switch sel4 {
					case 1:
						rpcResp := sel4v1
						// Copy the result back
						*inp.future.resp = *rpcResp.Response.(*AppendEntriesResponse)
						inp.future.respond(rpcResp.Error)

						{
							var sel5 int
							dst.Select(i.trans.w,
								dst.OnSend[AppendFuture](i.doneCh, inp.future, func() { sel5 = 1 }),
								dst.OnRecv(i.shutdownCh, func(_ struct{}, _ bool) { sel5 = 2 }),
							)
							switch sel5 {
							case 1:
							case 2:
								return
							default:
								panic("dstmigrate: no select case ran")
							}
						}

					case 2:
						inp.future.respond(fmt.Errorf("command timed out"))
						{
							var sel6 int
							dst.Select(i.trans.w,
								dst.OnSend[AppendFuture](i.doneCh, inp.future, func() { sel6 = 1 }),
								dst.OnRecv(i.shutdownCh, func(_ struct{}, _ bool) { sel6 = 2 }),
							)
							switch sel6 {
							case 1:
							case 2:
								return
							default:
								panic("dstmigrate: no select case ran")
							}
						}

					case 3:
						return
					default:
						panic("dstmigrate: no select case ran")
					}
				}
			case 2:
				return
			default:
				panic("dstmigrate: no select case ran")
			}
		}
	}
}

func (i *inmemPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: i.trans.w.Now(),
		args:  args,
		resp:  resp,
	}
	future.init(i.trans.w)

	// Handle a timeout
	var timeout dst.Chan[time.Time]
	if i.trans.timeout > 0 {
		timeout = i.trans.w.After(i.trans.timeout)
	}

	// Send the RPC over
	respCh := dst.MakeChan[RPCResponse](i.trans.w, 1)
	rpc := RPC{
		Command:  args,
		RespChan: respCh,
	}

	// Check if we have been already shutdown, otherwise the random choose
	// made by select statement below might pick consumerCh even if
	// shutdownCh was closed.
	i.shutdownLock.RLock()
	shutdown := i.shutdown
	i.shutdownLock.RUnlock()
	if shutdown {
		return nil, ErrPipelineShutdown
	}

	{
		var sel7 int
		dst.Select(i.trans.w,
			dst.OnSend(i.peer.consumerCh, rpc, func() { sel7 = 1 }),
			dst.OnRecv(timeout, func(_ time.Time, _ bool) { sel7 = 2 }),
			dst.OnRecv(i.shutdownCh, func(_ struct{}, _ bool) { sel7 = 3 }),
		)
		switch sel7 {
		case 1:
		case 2:
			return nil, fmt.Errorf("command enqueue timeout")
		case 3:
			return nil, ErrPipelineShutdown
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	// Send to be decoded
	{
		var sel8 int
		dst.Select(i.trans.w,
			dst.OnSend(i.inprogressCh, &inmemPipelineInflight{future, respCh}, func() { sel8 = 1 }),
			dst.OnRecv(i.shutdownCh, func(_ struct{}, _ bool) { sel8 = 2 }),
		)
		switch sel8 {
		case 1:
			return future, nil
		case 2:
			return nil, ErrPipelineShutdown
		default:
			panic("dstmigrate: no select case ran")
		}
	}
}

func (i *inmemPipeline) Consumer() dst.Chan[AppendFuture] {
	return i.doneCh
}

func (i *inmemPipeline) Close() error {
	i.shutdownLock.Lock()
	defer i.shutdownLock.Unlock()
	if i.shutdown {
		return nil
	}

	i.shutdown = true
	i.shutdownCh.Close()
	return nil
}
