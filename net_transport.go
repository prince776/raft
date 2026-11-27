// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/prince776/go-dst-kit/dst"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow
	rpcRequestPreVote

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// DefaultMaxRPCsInFlight is the default value used for pipelining configuration
	// if a zero value is passed. See https://github.com/hashicorp/raft/pull/541
	// for rationale. Note, if this is changed we should update the doc comments
	// below for NetworkTransportConfig.MaxRPCsInFlight.
	DefaultMaxRPCsInFlight = 2

	// connReceiveBufferSize is the size of the buffer we will use for reading RPC requests into
	// on followers
	connReceiveBufferSize = 256 * 1024 // 256KB

	// connSendBufferSize is the size of the buffer we will use for sending RPC request data from
	// the leader to followers.
	connSendBufferSize = 256 * 1024 // 256KB

	// minInFlightForPipelining is a property of our current pipelining
	// implementation and must not be changed unless we change the invariants of
	// that implementation. Roughly speaking even with a zero-length in-flight
	// buffer we still allow 2 requests to be in-flight before we block because we
	// only block after sending and the receiving go-routine always unblocks the
	// chan right after first send. This is a constant just to provide context
	// rather than a magic number in a few places we have to check invariants to
	// avoid panics etc.
	minInFlightForPipelining = 2
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrPipelineShutdown is returned when the pipeline is closed.
	ErrPipelineShutdown = errors.New("append pipeline closed")
)

// NetworkTransport provides a network based transport that can be
// used to communicate with Raft on remote machines. It requires
// an underlying stream layer to provide a stream abstraction, which can
// be simple TCP, TLS, etc.
//
// This transport is very simple and lightweight. Each RPC request is
// framed by sending a byte that indicates the message type, followed
// by the MsgPack encoded request.
//
// The response is an error string followed by the response object,
// both are encoded using MsgPack.
//
// InstallSnapshot is special, in that after the RPC request we stream
// the entire state. That socket is not re-used as the connection state
// is not known if there is an error.
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock dst.Mutex

	consumeCh dst.Chan[RPC]

	heartbeatFn     func(RPC)
	heartbeatFnLock dst.Mutex

	logger hclog.Logger

	maxPool     int
	maxInFlight int

	serverAddressLock     dst.RWMutex
	serverAddressProvider ServerAddressProvider

	shutdown     bool
	shutdownCh   dst.Chan[struct{}]
	shutdownLock dst.Mutex

	stream StreamLayer

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock dst.RWMutex

	timeout      time.Duration
	TimeoutScale int

	msgpackUseNewTimeFormat bool

	w *dst.World
}

// NetworkTransportConfig encapsulates configuration for the network transport layer.
type NetworkTransportConfig struct {
	// ServerAddressProvider is used to override the target address when establishing a connection to invoke an RPC
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool controls how many connections we will pool
	MaxPool int

	// MaxRPCsInFlight controls the pipelining "optimization" when replicating
	// entries to followers.
	//
	// Setting this to 1 explicitly disables pipelining since no overlapping of
	// request processing is allowed. If set to 1 the pipelining code path is
	// skipped entirely and every request is entirely synchronous.
	//
	// If zero is set (or left as default), DefaultMaxRPCsInFlight is used which
	// is currently 2. A value of 2 overlaps the preparation and sending of the
	// next request while waiting for the previous response, but avoids additional
	// queuing.
	//
	// Historically this was internally fixed at (effectively) 130 however
	// performance testing has shown that in practice the pipelining optimization
	// combines badly with batching and actually has a very large negative impact
	// on commit latency when throughput is high, whilst having very little
	// benefit on latency or throughput in any other case! See
	// [#541](https://github.com/hashicorp/raft/pull/541) for more analysis of the
	// performance impacts.
	//
	// Increasing this beyond 2 is likely to be beneficial only in very
	// high-latency network conditions. HashiCorp doesn't recommend using our own
	// products this way.
	//
	// To maintain the behavior from before version 1.4.1 exactly, set this to
	// 130. The old internal constant was 128 but was used directly as a channel
	// buffer size. Since we send before blocking on the channel and unblock the
	// channel as soon as the receiver is done with the earliest outstanding
	// request, even an unbuffered channel (buffer=0) allows one request to be
	// sent while waiting for the previous one (i.e. 2 inflight). so the old
	// buffer actually allowed 130 RPCs to be inflight at once.
	MaxRPCsInFlight int

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration

	// MsgpackUseNewTimeFormat when set to true, force the underlying msgpack
	// codec to use the new format of time.Time when encoding (used in
	// go-msgpack v1.1.5 by default). Decoding is not affected, as all
	// go-msgpack v2.1.0+ decoders know how to decode both formats.
	MsgpackUseNewTimeFormat bool

	// World is the World the transport runs in (nil: the real one).
	World *dst.World
}

// ServerAddressProvider is a target address to which we invoke an RPC when establishing a connection
type ServerAddressProvider interface {
	ServerAddr(id ServerID) (ServerAddress, error)
}

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}

type netConn struct {
	target ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

type netPipeline struct {
	conn  *netConn
	trans *NetworkTransport

	doneCh       dst.Chan[AppendFuture]
	inprogressCh dst.Chan[*appendFuture]

	shutdown     bool
	shutdownCh   dst.Chan[struct{}]
	shutdownLock dst.Mutex
}

// NewNetworkTransportWithConfig creates a new network transport with the given config struct
func NewNetworkTransportWithConfig(
	config *NetworkTransportConfig,
) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	maxInFlight := config.MaxRPCsInFlight
	if maxInFlight == 0 {
		// Default zero value
		maxInFlight = DefaultMaxRPCsInFlight
	}
	w := config.World
	if w == nil {
		w = theRealWorld()
	}
	trans := &NetworkTransport{
		w:                       w,
		connPool:                make(map[ServerAddress][]*netConn),
		consumeCh:               dst.MakeChan[RPC](w, 0),
		logger:                  config.Logger,
		maxPool:                 config.MaxPool,
		maxInFlight:             maxInFlight,
		shutdownCh:              dst.MakeChan[struct{}](w, 0),
		stream:                  config.Stream,
		timeout:                 config.Timeout,
		TimeoutScale:            DefaultTimeoutScale,
		serverAddressProvider:   config.ServerAddressProvider,
		msgpackUseNewTimeFormat: config.MsgpackUseNewTimeFormat,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	w.Go(func() { trans.listen() })

	return trans
}

// NewNetworkTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger creates a new network transport with the given logger, dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// SetHeartbeatHandler is used to set up a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// CloseStreams closes the current streams.
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range n.connPool {
		for _, conn := range e {
			_ = conn.Release()
		}

		delete(n.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	n.streamCtxLock.Lock()
	n.streamCancel()
	n.setupStreamContext()
	n.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		n.shutdownCh.Close()
		_ = n.stream.Close()
		n.shutdown = true
	}
	return nil
}

// Consumer implements the Transport interface.
func (n *NetworkTransport) Consumer() dst.Chan[RPC] {
	return n.consumeCh
}

// LocalAddr implements the Transport interface.
func (n *NetworkTransport) LocalAddr() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	{
		var sel1 int
		dst.Select(n.w,
			dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel1 = 1 }),
			dst.Default(func() { sel1 = 2 }),
		)
		switch sel1 {
		case 1:
			return true
		case 2:
			return false
		default:
			panic("dstmigrate: no select case ran")
		}
	}
}

// getExistingConn is used to grab a pooled connection.
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider returns a connection from the server address provider if available, or defaults to a connection using the target server address
func (n *NetworkTransport) getConnFromAddressProvider(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

func (n *NetworkTransport) getProviderAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	n.serverAddressLock.RLock()
	defer n.serverAddressLock.RUnlock()
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddr(id)
		if err != nil {
			n.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn is used to get a connection from the pool.
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	// Wrap the conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, connSendBufferSize),
	}

	mp := &codec.MsgpackHandle{}
	mp.TimeNotBuiltin = !n.msgpackUseNewTimeFormat
	netConn.enc = codec.NewEncoder(netConn.w, mp)

	// Done
	return netConn, nil
}

// returnConn returns a connection back to the pool.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		_ = conn.Release()
	}
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (n *NetworkTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	if n.maxInFlight < minInFlightForPipelining {
		// Pipelining is disabled since no more than one request can be outstanding
		// at once. Skip the whole code path and use synchronous requests.
		return nil, ErrPipelineReplicationNotSupported
	}

	// Get a connection
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// Create the pipeline
	return newNetPipeline(n, conn, n.maxInFlight), nil
}

// AppendEntries implements the Transport interface.
func (n *NetworkTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote implements the Transport interface.
func (n *NetworkTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// RequestPreVote implements the Transport interface.
func (n *NetworkTransport) RequestPreVote(id ServerID, target ServerAddress, args *RequestPreVoteRequest, resp *RequestPreVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestPreVote, args, resp)
}

// genericRPC handles a simple request/response RPC.
func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	// Get a conn
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	// Set a deadline
	if n.timeout > 0 {
		_ = conn.conn.SetDeadline(n.w.Now().Add(n.timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		n.returnConn(conn)
	}
	return err
}

// InstallSnapshot implements the Transport interface.
func (n *NetworkTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	// Get a conn, always close for InstallSnapshot
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Release() }()

	// Set a deadline, scaled by request size
	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		_ = conn.conn.SetDeadline(n.w.Now().Add(timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	// Stream the state
	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// Flush
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// Decode the response, do not return conn
	_, err = decodeResponse(conn, resp)
	return err
}

// EncodePeer implements the Transport interface.
func (n *NetworkTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer implements the Transport interface.
func (n *NetworkTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// TimeoutNow implements the Transport interface.
func (n *NetworkTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	return n.genericRPC(id, target, rpcTimeoutNow, args, resp)
}

// listen is used to handling incoming connections.
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accept incoming connections
		conn, err := n.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !n.IsShutdown() {
				n.logger.Error("failed to accept connection", "error", err)
			}

			{
				var sel2 int
				dst.Select(n.w,
					dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel2 = 1 }),
					dst.OnRecv(n.w.After(loopDelay), func(_ time.Time, _ bool) { sel2 = 2 }),
				)
				switch sel2 {
				case 1:
					return
				case 2:
					continue
				default:
					panic("dstmigrate: no select case ran")
				}
			}
		}
		// No error, reset loop delay
		loopDelay = 0

		n.logger.Debug("accepted connection", "local-address", n.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// Handle the connection in dedicated routine
		{
			goArg1 := n.getStreamContext()
			n.w.Go(func() { n.handleConn(goArg1, conn) })
		}
	}
}

// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
func (n *NetworkTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReaderSize(conn, connReceiveBufferSize)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})

	mp := &codec.MsgpackHandle{}
	mp.TimeNotBuiltin = !n.msgpackUseNewTimeFormat
	enc := codec.NewEncoder(w, mp)

	for {
		{
			var sel3 int
			dst.Select(n.w,
				dst.OnDone(connCtx, func() { sel3 = 1 }),
				dst.Default(func() { sel3 = 2 }),
			)
			switch sel3 {
			case 1:
				n.logger.Debug("stream layer is closed")
				return
			case 2:
			default:
				panic("dstmigrate: no select case ran")
			}
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			n.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command.
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	getTypeStart := n.w.Now()

	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	metrics.MeasureSince([]string{"raft", "net", "getRPCType"}, getTypeStart)
	decodeStart := n.w.Now()

	// Create the RPC object
	respCh := dst.MakeChan[RPCResponse](n.w, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	// Decode the command
	isHeartbeat := false
	var labels []metrics.Label
	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		leaderAddr := req.Addr
		if len(leaderAddr) == 0 {
			leaderAddr = req.Leader
		}

		// Check if this is a heartbeat
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

		if isHeartbeat {
			labels = []metrics.Label{{Name: "rpcType", Value: "Heartbeat"}}
		} else {
			labels = []metrics.Label{{Name: "rpcType", Value: "AppendEntries"}}
		}
	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestVote"}}
	case rpcRequestPreVote:
		var req RequestPreVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "RequestPreVote"}}
	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)
		labels = []metrics.Label{{Name: "rpcType", Value: "InstallSnapshot"}}
	case rpcTimeoutNow:
		var req TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		labels = []metrics.Label{{Name: "rpcType", Value: "TimeoutNow"}}
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcDecode"}, decodeStart, labels)

	processStart := n.w.Now()

	// Check for heartbeat fast-path
	if isHeartbeat {
		n.heartbeatFnLock.Lock()
		fn := n.heartbeatFn
		n.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// Dispatch the RPC
	{
		var sel4 int
		dst.Select(n.w,
			dst.OnSend(n.consumeCh, rpc, func() { sel4 = 1 }),
			dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel4 = 2 }),
		)
		switch sel4 {
		case 1:
		case 2:
			return ErrTransportShutdown
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	// Wait for response
RESP:
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcEnqueue"}, processStart, labels)
	respWaitStart := n.w.Now()
	{
		var sel5 int
		var sel5v1 RPCResponse
		dst.Select(n.w,
			dst.OnRecv(respCh, func(v RPCResponse, _ bool) { sel5 = 1; sel5v1 = v }),
			dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel5 = 2 }),
		)
		switch sel5 {
		case 1:
			resp := sel5v1
			defer metrics.MeasureSinceWithLabels([]string{"raft", "net", "rpcRespond"}, respWaitStart, labels)
			// Send the error first
			respErr := ""
			if resp.Error != nil {
				respErr = resp.Error.Error()
			}
			if err := enc.Encode(respErr); err != nil {
				return err
			}

			// Send the response
			if err := enc.Encode(resp.Response); err != nil {
				return err
			}
		case 2:
			return ErrTransportShutdown
		default:
			panic("dstmigrate: no select case ran")
		}
	}
	return nil
}

// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		_ = conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		_ = conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, errors.New(rpcError)
	}
	return true, nil
}

// sendRPC is used to encode and send the RPC.
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// Write the request type
	if err := conn.w.WriteByte(rpcType); err != nil {
		_ = conn.Release()
		return err
	}

	// Send the request
	if err := conn.enc.Encode(args); err != nil {
		_ = conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		_ = conn.Release()
		return err
	}
	return nil
}

// newNetPipeline is used to construct a netPipeline from a given transport and
// connection. It is a bug to ever call this with maxInFlight less than 2
// (minInFlightForPipelining) and will cause a panic.
func newNetPipeline(trans *NetworkTransport, conn *netConn, maxInFlight int) *netPipeline {
	if maxInFlight < minInFlightForPipelining {
		// Shouldn't happen (tm) since we validate this in the one call site and
		// skip pipelining if it's lower.
		panic("pipelining makes no sense if maxInFlight < 2")
	}
	n := &netPipeline{
		conn:  conn,
		trans: trans,
		// The buffer size is 2 less than the configured max because we send before
		// waiting on the channel and the decode routine unblocks the channel as
		// soon as it's waiting on the first request. So a zero-buffered channel
		// still allows 1 request to be sent even while decode is still waiting for
		// a response from the previous one. i.e. two are inflight at the same time.
		inprogressCh: dst.MakeChan[*appendFuture](trans.w, maxInFlight-2),
		doneCh:       dst.MakeChan[AppendFuture](trans.w, maxInFlight-2),
		shutdownCh:   dst.MakeChan[struct{}](trans.w, 0),
	}
	trans.w.Go(func() { n.decodeResponses() })
	return n
}

// decodeResponses is a long running routine that decodes the responses
// sent on the connection.
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		{
			var sel6 int
			var sel6v1 *appendFuture
			dst.Select(n.trans.w,
				dst.OnRecv(n.inprogressCh, func(v *appendFuture, _ bool) { sel6 = 1; sel6v1 = v }),
				dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel6 = 2 }),
			)
			switch sel6 {
			case 1:
				future := sel6v1
				if timeout > 0 {
					_ = n.conn.conn.SetReadDeadline(n.trans.w.Now().Add(timeout))
				}

				_, err := decodeResponse(n.conn, future.resp)
				future.respond(err)
				{
					var sel7 int
					dst.Select(n.trans.w,
						dst.OnSend[AppendFuture](n.doneCh, future, func() { sel7 = 1 }),
						dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel7 = 2 }),
					)
					switch sel7 {
					case 1:
					case 2:
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

// AppendEntries is used to pipeline a new append entries request.
func (n *netPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: n.trans.w.Now(),
		args:  args,
		resp:  resp,
	}
	future.init(n.trans.w)

	// Add a send timeout
	if timeout := n.trans.timeout; timeout > 0 {
		_ = n.conn.conn.SetWriteDeadline(n.trans.w.Now().Add(timeout))
	}

	// Send the RPC
	if err := sendRPC(n.conn, rpcAppendEntries, future.args); err != nil {
		return nil, err
	}

	// Hand-off for decoding, this can also cause back-pressure
	// to prevent too many inflight requests
	{
		var sel8 int
		dst.Select(n.trans.w,
			dst.OnSend(n.inprogressCh, future, func() { sel8 = 1 }),
			dst.OnRecv(n.shutdownCh, func(_ struct{}, _ bool) { sel8 = 2 }),
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

// Consumer returns a channel that can be used to consume complete futures.
func (n *netPipeline) Consumer() dst.Chan[AppendFuture] {
	return n.doneCh
}

// Close is used to shut down the pipeline connection.
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		return nil
	}

	// Release the connection
	_ = n.conn.Release()

	n.shutdown = true
	n.shutdownCh.Close()
	return nil
}
