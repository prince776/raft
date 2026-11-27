// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"testing"

	"github.com/prince776/go-dst-kit/realworld"
)

func TestDeferFutureSuccess(t *testing.T) {
	var f deferError
	f.init(realworld.New(realworld.Options{}))
	f.respond(nil)
	if err := f.Error(); err != nil {
		t.Fatalf("unexpected error result; got %#v want nil", err)
	}
	if err := f.Error(); err != nil {
		t.Fatalf("unexpected error result; got %#v want nil", err)
	}
}

func TestDeferFutureError(t *testing.T) {
	want := errors.New("x")
	var f deferError
	f.init(realworld.New(realworld.Options{}))
	f.respond(want)
	if got := f.Error(); got != want {
		t.Fatalf("unexpected error result; got %#v want %#v", got, want)
	}
	if got := f.Error(); got != want {
		t.Fatalf("unexpected error result; got %#v want %#v", got, want)
	}
}

func TestDeferFutureConcurrent(t *testing.T) {
	// Food for the race detector.
	want := errors.New("x")
	var f deferError
	f.init(realworld.New(realworld.Options{}))
	{
		goArg6 := f.respond
		theRealWorld().Go(func() { goArg6(want) })
	}
	if got := f.Error(); got != want {
		t.Errorf("unexpected error result; got %#v want %#v", got, want)
	}
}
