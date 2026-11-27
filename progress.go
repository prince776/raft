// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"io"
	"time"

	"github.com/prince776/go-dst-kit/dst"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	snapshotRestoreMonitorInterval = 10 * time.Second
)

type snapshotRestoreMonitor struct {
	logger          hclog.Logger
	cr              CountingReader
	size            int64
	networkTransfer bool

	once   dst.Once
	cancel func()
	doneCh dst.Chan[struct{}]

	w *dst.World
}

func startSnapshotRestoreMonitor(
	w *dst.World,
	logger hclog.Logger,
	cr CountingReader,
	size int64,
	networkTransfer bool,
) *snapshotRestoreMonitor {
	ctx, cancel := dst.WithCancel(w, context.Background())

	m := &snapshotRestoreMonitor{
		logger:          logger,
		cr:              cr,
		size:            size,
		networkTransfer: networkTransfer,
		cancel:          cancel,
		w:               w,
		doneCh:          dst.MakeChan[struct{}](w, 0),
	}
	w.Go(func() { m.run(ctx) })
	return m
}

func (m *snapshotRestoreMonitor) run(ctx context.Context) {
	defer m.doneCh.Close()

	ticker := m.w.NewTicker(snapshotRestoreMonitorInterval)
	defer ticker.Stop()

	ranOnce := false
	for {
		{
			var sel1 int
			dst.Select(m.w,
				dst.OnDone(ctx, func() { sel1 = 1 }),
				dst.OnRecv(ticker.C, func(_ time.Time, _ bool) { sel1 = 2 }),
			)
			switch sel1 {
			case 1:
				if !ranOnce {
					m.runOnce()
				}
				return
			case 2:
				m.runOnce()
				ranOnce = true
			default:
				panic("dstmigrate: no select case ran")
			}
		}
	}
}

func (m *snapshotRestoreMonitor) runOnce() {
	readBytes := m.cr.Count()
	pct := float64(100*readBytes) / float64(m.size)

	message := "snapshot restore progress"
	if m.networkTransfer {
		message = "snapshot network transfer progress"
	}

	m.logger.Info(message,
		"read-bytes", readBytes,
		"percent-complete", hclog.Fmt("%0.2f%%", pct),
	)
}

func (m *snapshotRestoreMonitor) StopAndWait() {
	m.once.Do(func() {
		m.cancel()
		m.doneCh.Recv()
	})
}

type CountingReader interface {
	io.Reader
	Count() int64
}

type countingReader struct {
	reader io.Reader

	mu    dst.Mutex
	bytes int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.mu.Lock()
	r.bytes += int64(n)
	r.mu.Unlock()
	return n, err
}

func (r *countingReader) Count() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

func newCountingReader(r io.Reader) *countingReader {
	return &countingReader{reader: r}
}

type countingReadCloser struct {
	*countingReader
	readCloser io.ReadCloser
}

func newCountingReadCloser(rc io.ReadCloser) *countingReadCloser {
	return &countingReadCloser{
		countingReader: newCountingReader(rc),
		readCloser:     rc,
	}
}

func (c countingReadCloser) Close() error {
	return c.readCloser.Close()
}

func (c countingReadCloser) WrappedReadCloser() io.ReadCloser {
	return c.readCloser
}

// ReadCloserWrapper allows access to an underlying ReadCloser from a wrapper.
type ReadCloserWrapper interface {
	io.ReadCloser
	WrappedReadCloser() io.ReadCloser
}

var _ ReadCloserWrapper = &countingReadCloser{}
