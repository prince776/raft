// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// TestMsgpackEncodeTime ensures that we don't break backwards compatibility when updating go-msgpack with
// Raft binary formats.
func TestMsgpackEncodeTimeDefaultFormat(t *testing.T) {
	stamp := "2006-01-02T15:04:05Z"
	tm, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := encodeMsgPack(tm)
	if err != nil {
		t.Errorf("Failed to encode time %s: %v", stamp, err)
	}

	expected := []byte{175, 1, 0, 0, 0, 14, 187, 75, 55, 229, 0, 0, 0, 0, 255, 255}

	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected time %s to encode as %+v but got %+v", stamp, expected, buf.Bytes())
	}
}

func TestRandomTimeout(t *testing.T) {
	start := theRealWorld().Now()
	timeout := randomTimeout(theRealWorld(), time.Millisecond)

	{
		var sel1 int
		dst.Select(theRealWorld(),
			dst.OnRecv(timeout, func(_ time.Time, _ bool) { sel1 = 1 }),
			dst.OnRecv(theRealWorld().After(3*time.Millisecond), func(_ time.Time, _ bool) { sel1 = 2 }),
		)
		switch sel1 {
		case 1:
			diff := theRealWorld().Since(start)
			if diff < time.Millisecond {
				t.Fatalf("fired early")
			}
		case 2:
			t.Fatalf("timeout")
		default:
			panic("dstmigrate: no select case ran")
		}
	}
}

func TestNewSeed(t *testing.T) {
	vals := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		seed := newSeed()
		if _, exists := vals[seed]; exists {
			t.Fatal("newSeed() return a value it'd previously returned")
		}
		vals[seed] = true
	}
}

func TestRandomTimeout_NoTime(t *testing.T) {
	timeout := randomTimeout(theRealWorld(), 0)
	if timeout != (dst.Chan[time.Time]{}) {
		t.Fatalf("expected nil channel")
	}
}

func TestMin(t *testing.T) {
	if min(1, 1) != 1 {
		t.Fatalf("bad min")
	}
	if min(2, 1) != 1 {
		t.Fatalf("bad min")
	}
	if min(1, 2) != 1 {
		t.Fatalf("bad min")
	}
}

func TestMax(t *testing.T) {
	if max(1, 1) != 1 {
		t.Fatalf("bad max")
	}
	if max(2, 1) != 2 {
		t.Fatalf("bad max")
	}
	if max(1, 2) != 2 {
		t.Fatalf("bad max")
	}
}

func TestGenerateUUID(t *testing.T) {
	prev := generateUUID(theRealWorld())
	re, err := regexp.Compile(`[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}`)
	if err != nil {
		t.Errorf("failed to copmile regex: %s", err)
	}

	for i := 0; i < 100; i++ {
		id := generateUUID(theRealWorld())
		if prev == id {
			t.Fatalf("Should get a new ID!")
		}

		matched := re.MatchString(id)
		if !matched {
			t.Fatalf("expected match %s %v", id, matched)
		}
	}
}

func TestBackoff(t *testing.T) {
	b := backoff(10*time.Millisecond, 1, 8)
	if b != 10*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(20*time.Millisecond, 2, 8)
	if b != 20*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(10*time.Millisecond, 8, 8)
	if b != 640*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}

	b = backoff(10*time.Millisecond, 9, 8)
	if b != 640*time.Millisecond {
		t.Fatalf("bad: %v", b)
	}
}

func TestOverrideNotifyBool(t *testing.T) {
	ch := dst.MakeChan[bool](theRealWorld(), 1)

	// sanity check - buffered channel don't have any values
	{
		var sel2 int
		var sel2v1 bool
		dst.Select(theRealWorld(),
			dst.OnRecv(ch, func(v bool, _ bool) { sel2 = 1; sel2v1 = v }),
			dst.Default(func() { sel2 = 2 }),
		)
		switch sel2 {
		case 1:
			v := sel2v1
			t.Fatalf("unexpected receive: %v", v)
		case 2:
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	// simple case of a single push
	overrideNotifyBool(ch, false)
	{
		var sel3 int
		var sel3v1 bool
		dst.Select(theRealWorld(),
			dst.OnRecv(ch, func(v bool, _ bool) { sel3 = 1; sel3v1 = v }),
			dst.Default(func() { sel3 = 2 }),
		)
		switch sel3 {
		case 1:
			v := sel3v1
			if v != false {
				t.Fatalf("expected false but got %v", v)
			}
		case 2:
			t.Fatalf("expected a value but is not ready")
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	// assert that function never blocks and only last item is received
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, false)
	overrideNotifyBool(ch, true)

	{
		var sel4 int
		var sel4v1 bool
		dst.Select(theRealWorld(),
			dst.OnRecv(ch, func(v bool, _ bool) { sel4 = 1; sel4v1 = v }),
			dst.Default(func() { sel4 = 2 }),
		)
		switch sel4 {
		case 1:
			v := sel4v1
			if v != true {
				t.Fatalf("expected true but got %v", v)
			}
		case 2:
			t.Fatalf("expected a value but is not ready")
		default:
			panic("dstmigrate: no select case ran")
		}
	}

	// no further value is available
	{
		var sel5 int
		var sel5v1 bool
		dst.Select(theRealWorld(),
			dst.OnRecv(ch, func(v bool, _ bool) { sel5 = 1; sel5v1 = v }),
			dst.Default(func() { sel5 = 2 }),
		)
		switch sel5 {
		case 1:
			v := sel5v1
			t.Fatalf("unexpected receive: %v", v)
		case 2:
		default:
			panic("dstmigrate: no select case ran")
		}
	}
}
