//go:build dstbugs

package raft

// Phase 3 (DST.md 12): how useful plain exploration is. One combined
// sweep, not one per fault class: every fault kind is enabled and the
// swarm switches each off for some seeds, and each seed draws the
// cluster's shape (half fixed clusters of 3, 4 or 5 voters, half
// membership clusters whose servers are lost and replaced; TrailingLogs 0
// on a quarter of seeds). Every known bug (planted,
// or a real one with its fix switched off and the other fixes on) runs
// through it until it has failed a few seeds or run out of seeds: the
// result is whether plain exploration finds the bug, how soon (its first
// failing seed), and by which checks. A no-bug run (every fix on) is the
// hunt for anything new.
//
// Skipped unless DST_PHASE3=1. DST_PHASE3_OUT (default phase3/swarm.tsv)
// gets one line per bug as it finishes; bugs already in it are skipped.
// DST_PHASE3_BUGS narrows the run (comma-separated); DST_PHASE3_SEEDS
// raises a bug's seed budget (default phase3BugSeeds).

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dsttest"
)

// The run's size: a bug gets at most phase3BugSeeds seeds and stops at
// phase3EnoughFailures failing ones; the hunt runs phase3HuntSeeds.
const (
	phase3BugSeeds       = 100
	phase3EnoughFailures = 5
	phase3HuntSeeds      = 300
	phase3Chunk          = 25
)

// phase3Setup is the combined sweep's setup: the cluster's shape drawn per
// seed, raft's default timeouts under gray failure (slow links outlast a
// fifth of them).
func phase3Setup(h *dsttest.Harness) {
	sw := h.Swarm()
	gray := sw.SlowLinkShare > 0 || sw.LossyLinkShare > 0 || sw.SlowMachineShare > 0 || sw.SlowDiskShare > 0
	r := h.World().Rand().Stream("phase3 shape")
	spec := clusterSpec{defaultTimeouts: gray}
	// TrailingLogs 0 on a quarter of seeds: raft accepts it, and 6.11
	// needs it; the drawn configuration only picks 32 or 4 (upstream's
	// plain sweeps run without 6.11's fix).
	if r.IntN(4) == 0 {
		spec.forceTrailing, spec.forceTrailingLogs = true, 0
		h.Sometimes("raft config: TrailingLogs 0")
	}
	h.Expect("raft config: TrailingLogs 0")
	// Half the seeds are membership clusters (servers lost and replaced),
	// half fixed clusters of 3, 4 or 5 voters.
	if r.IntN(2) == 0 {
		membershipClusterSpec(h, true, spec)
		return
	}
	h.OnLoss(func(*dsttest.Machine) bool { return false }, nil) // a fixed cluster loses no server for good
	spec.voters, spec.drawConfig, spec.clients = 3+r.IntN(3), true, 2
	newCluster(h, spec)
}

// phase3Faults: every fault class, server loss included (only membership
// seeds accept a loss).
var phase3Faults = func() dsttest.Faults {
	f := dsttest.AllFaults
	f.Loss, f.Kill = true, true
	return f
}()

// phase3Planted are the bugs planted in raft (manual.py, dst_kv_test.go);
// phase3Real are the real ones, measured by switching their fix off.
var phase3Planted = []string{
	"accept-stale-leader", "apply-entry-twice", "commit-previous-term", "compact-past-snapshot",
	"config-change-before-noop", "follower-ignores-commit", "forget-term-on-restart",
	"lease-counts-nonvoters", "nonvoter-can-lead", "quorum-off-by-one", "readindex-without-barrier",
	"skip-prev-log-term", "transfer-flag-sticks", "two-changes-in-flight", "vote-ignores-log", "vote-twice",
}

func phase3Real() []string { return strings.Split(knownFixes, ",") }

// phase3Switches returns the bug switches for a run: "none" is every fix
// on; a planted bug is every fix plus it; "fix-off:F" is every fix but F.
func phase3Switches(cell string) string {
	if f, ok := strings.CutPrefix(cell, "fix-off:"); ok {
		// strict-checks: the two checks that count upstream's known
		// behaviour instead of failing on it (6.13's term going back,
		// 6.14's refusal after a stuck transfer) fail on it here, so a
		// fix-off run can detect its bug.
		return strings.Join(append(slices.DeleteFunc(phase3Real(), func(s string) bool { return s == f }), "strict-checks"), ",")
	}
	if cell == "none" {
		return knownFixes
	}
	return knownFixes + "," + cell
}

var phase3Check = regexp.MustCompile(`(?:check|invariant) "([^"]+)"`)

// phase3Why names what caught a failing seed: its check, or the start of
// its error.
func phase3Why(err error) string {
	if m := phase3Check.FindStringSubmatch(err.Error()); m != nil {
		return m[1]
	}
	s := err.Error()
	if i := strings.IndexAny(s, ":\n"); i > 0 && i < 50 {
		s = s[:i]
	} else if len(s) > 50 {
		s = s[:50]
	}
	return "other: " + s
}

func TestDSTPhase3(t *testing.T) {
	if os.Getenv("DST_PHASE3") != "1" {
		t.Skip("phase 3 measurement: set DST_PHASE3=1")
	}
	defer func() { activeBug = "" }()
	out := os.Getenv("DST_PHASE3_OUT")
	if out == "" {
		out = "phase3/swarm.tsv"
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}
	done := map[string]bool{}
	if f, err := os.Open(out); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if fs := strings.Split(sc.Text(), "\t"); len(fs) > 1 {
				done[fs[0]] = true
			}
		}
		f.Close()
	}
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if st, _ := f.Stat(); st.Size() == 0 {
		fmt.Fprintln(f, "bug\tran\tfailed\tfirst\tcaught_by\tnondeterministic\twall\tnever_reached")
	}
	cells := []string{"none"}
	cells = append(cells, phase3Planted...)
	for _, fx := range phase3Real() {
		cells = append(cells, "fix-off:"+fx)
	}
	want := func(string) bool { return true }
	if v := os.Getenv("DST_PHASE3_BUGS"); v != "" {
		set := strings.Split(v, ",")
		want = func(s string) bool { return slices.Contains(set, s) }
	}
	for _, cell := range cells {
		if !want(cell) || done[cell] {
			continue
		}
		activeBug = phase3Switches(cell)
		limit := phase3BugSeeds
		if v, err := strconv.Atoi(os.Getenv("DST_PHASE3_SEEDS")); err == nil && v > 0 {
			limit = v
		}
		if cell == "none" {
			limit = phase3HuntSeeds
		}
		start := time.Now()
		why := map[string]int{}
		first, ran, failed, nondet := uint64(0), 0, 0, 0
		cover := map[string]int{}
		never := map[string]bool{}
		for ran < limit && (cell == "none" || failed < phase3EnoughFailures) {
			o := dsttest.Options{Faults: phase3Faults, FaultPhase: 10 * time.Second, RecoveryPhase: 10 * time.Second}
			o.FirstSeed = uint64(ran + 1)
			o.Seeds = phase3Chunk
			if left := limit - ran; left < phase3Chunk {
				o.Seeds = left
			}
			o.KeepGoing, o.DeterminismChecks = true, -1
			rep := dsttest.Sweep(o, phase3Setup)
			ran += rep.Runs
			nondet += len(rep.Nondeterministic)
			for k, n := range rep.Coverage {
				cover[k] += n
			}
			for _, n := range rep.NeverHappened {
				never[n] = true
			}
			for _, r := range rep.Failed {
				failed++
				why[phase3Why(r.Err)]++
				if first == 0 || r.Seed < first {
					first = r.Seed
				}
				if cell == "none" {
					t.Logf("hunt: seed %d failed: %.300v", r.Seed, r.Err)
				}
			}
		}
		var caught []string
		for k, n := range why {
			caught = append(caught, fmt.Sprintf("%s=%d", k, n))
		}
		sort.Strings(caught)
		// An Expect-ed behaviour no seed reached makes the run vacuous for
		// it: recorded, and a hunt that misses one fails.
		var nevers []string
		for n := range never {
			if cover[n] == 0 {
				nevers = append(nevers, n)
			}
		}
		sort.Strings(nevers)
		if cell == "none" {
			var keys []string
			for k := range cover {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				t.Logf("hunt coverage: %6d  %s", cover[k], k)
			}
			if len(nevers) > 0 {
				t.Errorf("the hunt never reached: %v", nevers)
			}
		}
		line := fmt.Sprintf("%s\t%d\t%d\t%d\t%s\t%d\t%s\t%s", cell, ran, failed, first, strings.Join(caught, "; "), nondet, time.Since(start).Round(time.Second), strings.Join(nevers, "; "))
		fmt.Fprintln(f, line)
		t.Log(line)
	}
}

// TestDSTPhase3Sweep is the combined sweep as an ordinary dsttest run, with
// every known fix on: replay one of its seeds with -dst.seed=N (-dst.v for
// the event log).
func TestDSTPhase3Sweep(t *testing.T) {
	activeBug = knownFixes
	if v := os.Getenv("DSTBUG"); v != "" {
		activeBug = v // to replay a seed with other switches
	}
	defer func() { activeBug = "" }()
	dsttest.Run(t, dsttest.Options{Seeds: 50, Faults: phase3Faults, FaultPhase: 10 * time.Second, RecoveryPhase: 10 * time.Second}, phase3Setup)
}
