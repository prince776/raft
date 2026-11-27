//go:build dstbugs

package raft

import (
	"os"
	"slices"
	"strings"
)

var activeBug = os.Getenv("DSTBUG")

// bug reports whether the known bug (or experimental fix) name is switched
// on: one of the comma-separated names in $DSTBUG (only with the dstbugs
// tag).
func bug(name string) bool { return name != "" && slices.Contains(strings.Split(activeBug, ","), name) }
