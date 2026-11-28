// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/prince776/go-dst-kit/dst"
)

// returns an int64 from a crypto random source
// can be used to seed a source for a math/rand.
func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(w *dst.World, minVal time.Duration) dst.Chan[time.Time] {
	if minVal == 0 {
		return dst.Chan[time.Time]{}
	}
	extra := time.Duration(w.Rand().Int64()) % minVal
	return w.After(minVal + extra)
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// generateUUID is used to generate a random UUID.
func generateUUID(w *dst.World) string {
	buf := make([]byte, 16)
	if _, err := w.Rand().CryptoRead(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotifyCh is used to do an async channel send
// to a single channel without blocking.
func asyncNotifyCh(ch dst.Chan[struct{}]) {
	ch.TrySend(struct{}{})
}

// drainNotifyCh empties out a single-item notification channel without
// blocking, and returns whether it received anything.
func drainNotifyCh(ch dst.Chan[struct{}]) bool {
	_, _, got := ch.TryRecv()
	return got
}

// overrideNotifyBool is used to notify on a bool channel
// but override existing value if value is present.
// ch must be 1-item buffered channel.
//
// This method does not support multiple concurrent calls.
func overrideNotifyBool(ch dst.Chan[bool], v bool) {
	// ch is 1-buffered: empty, the send goes through; full, there is an old
	// value to take first.
	if ch.TrySend(v) {
		return // value sent, all done
	}
	ch.TryRecv() // channel had an old value
	if !ch.TrySend(v) {
		panic("race: channel was sent concurrently")
	}
}

// Decode reverses the encode operation on a byte slice input.
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer.
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	hd.TimeNotBuiltin = true

	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// backoff is used to compute an exponential backoff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backoff(base time.Duration, round, limit uint64) time.Duration {
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

// cappedExponentialBackoff computes the exponential backoff with an adjustable
// cap on the max timeout.
func cappedExponentialBackoff(base time.Duration, round, limit uint64, cap time.Duration) time.Duration {
	power := min(round, limit)
	for power > 2 {
		if base > cap {
			return cap
		}
		base *= 2
		power--
	}
	if base > cap {
		return cap
	}
	return base
}

// Needed for sorting []uint64, used to determine commitment
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
