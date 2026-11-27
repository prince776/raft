package raft

import (
	"context"
	"net"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// dstStream is a StreamLayer on a World's network: raft's real
// NetworkTransport (framing, connection pool, pipelining, heartbeats) over
// the simulated network's TCP-like streams, so latency, partitions and
// connection resets reach it.
type dstStream struct {
	net.Listener
	w *dst.World
}

func newDSTStream(w *dst.World, addr string) (*dstStream, error) {
	l, err := w.Net().Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &dstStream{Listener: l, w: w}, nil
}

// Dial is StreamLayer.Dial: a connection to address, within timeout.
func (s *dstStream) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := dst.WithTimeout(s.w, context.Background(), timeout)
	defer cancel()
	return s.w.Net().DialContext(ctx, "tcp", string(address))
}
