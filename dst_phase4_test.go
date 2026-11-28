//go:build dstbugs

package raft

// Phase 4 (DST.md 12): guided exploration against plain exploration, used
// as a hunt, not bug by bug. The same combined sweep as phase 3, the same
// budget for both: a plain sweep of N seeds, and Explore with N runs
// (every signal: markers, order, interleaving pairs, abstract states).
// Two benchmarks:
//   - A, upstream raft: every known fix off (strict-checks on, so 6.13 and
//     6.14 fail instead of being counted), so every real bug found so far
//     is present and each arm finds what it finds;
//   - B, every known fix on: a hunt for new bugs.
//
// Each arm reports its failing runs, its distinct failures (by check, and
// by dsttest.Signature), the run at which each kind first appeared, the
// distinct abstract states reached, and the CPU it took. A run stops at its
// first failure in both arms, so a common bug hides rarer ones equally.
//
// Skipped unless DST_PHASE4=1; DST_PHASE4_RUNS sets N (default 500),
// DST_PHASE4_BENCH picks A, B or both (default both), DST_PHASE4_SEED the
// first seed (default 1).

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dsttest"
)

func TestDSTPhase4(t *testing.T) {
	if os.Getenv("DST_PHASE4") != "1" {
		t.Skip("phase 4 measurement: set DST_PHASE4=1")
	}
	defer func() { activeBug = "" }()
	n := 500
	if v, err := strconv.Atoi(os.Getenv("DST_PHASE4_RUNS")); err == nil && v > 0 {
		n = v
	}
	first := uint64(1)
	if v, err := strconv.ParseUint(os.Getenv("DST_PHASE4_SEED"), 10, 64); err == nil && v > 0 {
		first = v
	}
	benches := []struct{ name, switches string }{
		{"A (upstream raft)", "strict-checks"},
		{"B (every fix on)", knownFixes},
	}
	for _, b := range benches {
		if pick := os.Getenv("DST_PHASE4_BENCH"); pick != "" && !strings.HasPrefix(b.name, pick) {
			continue
		}
		activeBug = b.switches
		o := dsttest.Options{Faults: phase3Faults, FaultPhase: 10 * time.Second, RecoveryPhase: 10 * time.Second,
			FirstSeed: first, Seeds: n, Explore: n, KeepGoing: true, DeterminismChecks: -1}
		arms := []string{"plain", "guided"}
		if os.Getenv("DST_PHASE4_GUIDED_ONLY") == "1" {
			arms = arms[1:]
		}
		if by := os.Getenv("DST_PHASE4_BY"); by != "" {
			o.ExploreBy = by // the signals guided runs steer by (default all)
		}
		for _, arm := range arms {
			cpu0 := cpuTime()
			var rep dsttest.Report
			if arm == "plain" {
				rep = dsttest.Sweep(o, phase3Setup)
			} else {
				rep = dsttest.Explore(o, phase3Setup)
			}
			cpu := cpuTime() - cpu0
			kinds := map[string]int{}
			firstAt := map[string]int{}
			failed, inconclusive := 0, 0
			forks, depth := 0, 0
			perRoot := map[uint64]int{}
			for _, r := range rep.Results {
				if k := len(r.Path.Forks); k > 0 {
					forks++
					depth += k
					perRoot[r.Path.Seed]++
				}
			}
			most := 0
			for _, n := range perRoot {
				if n > most {
					most = n
				}
			}
			if forks > 0 {
				t.Logf("    forks came from %d roots; the most forked root had %d", len(perRoot), most)
			}
			for i, r := range rep.Results {
				if r.Err == nil {
					continue
				}
				if strings.Contains(r.Err.Error(), "linearizability inconclusive") {
					inconclusive++
					continue
				}
				failed++
				// The kind: the check that caught it, or for anything else (a
				// panic, a hang) its normalized signature: phase3Why names a
				// panic by its goroutine, which would count one bug many times.
				k := phase3Why(r.Err)
				if strings.HasPrefix(k, "other: ") {
					k = "other: " + dsttest.Signature(r.Err)
				}
				if len(k) > 160 {
					k = k[:160]
				}
				if kinds[k] == 0 {
					firstAt[k] = i + 1
				}
				kinds[k]++
			}
			keys := make([]string, 0, len(kinds))
			for k := range kinds {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return firstAt[keys[i]] < firstAt[keys[j]] })
			t.Logf("%s, %s (by %q): %d runs (%d forks, %.1f forks deep), %d failing (%d inconclusive apart), %d distinct, %d abstract states, cpu %.0fs",
				b.name, arm, o.ExploreBy, rep.Runs, forks, float64(depth)/float64(max(uint64(forks), 1)) /* raft's uint64 max */, failed, inconclusive, len(kinds), rep.States, cpu.Seconds())
			var curve []string
			for _, at := range []int{250, 500, 1000, 2000, 4000} {
				if at > rep.Runs {
					break
				}
				c := 0
				for _, k := range keys {
					if firstAt[k] <= at {
						c++
					}
				}
				curve = append(curve, fmt.Sprintf("%d by run %d", c, at))
			}
			t.Logf("    distinct kinds: %s", strings.Join(curve, ", "))
			for _, k := range keys {
				t.Logf("    run %4d first, %3d runs: %s", firstAt[k], kinds[k], k)
			}
			fmt.Fprintln(os.Stderr) // keep sections apart in -v output
		}
	}
}
