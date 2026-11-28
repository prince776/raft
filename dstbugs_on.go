//go:build dstbugs

package raft

import (
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

var activeBug = os.Getenv("DSTBUG")

// bug reports whether the known bug (or experimental fix) name is switched
// on: one of the comma-separated names in $DSTBUG (only with the dstbugs
// tag).
func bug(name string) bool {
	if c, ok := bugSites.Load(name); ok {
		c.(*atomic.Int64).Add(1)
	} else {
		c, _ := bugSites.LoadOrStore(name, new(atomic.Int64))
		c.(*atomic.Int64).Add(1)
	}
	return name != "" && slices.Contains(strings.Split(activeBug, ","), name)
}

// bugSites counts, per switch name, how often code reached the switch (on
// or off), process-wide: a switch never reached can't matter in that
// workload (phase 3 uses it to stop measuring a bug a class can't reach).
var bugSites sync.Map

// bugSiteHits returns how often the named switch has been reached so far.
func bugSiteHits(name string) int64 {
	if c, ok := bugSites.Load(name); ok {
		return c.(*atomic.Int64).Load()
	}
	return 0
}
