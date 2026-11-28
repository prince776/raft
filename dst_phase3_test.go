//go:build dstbugs

package raft

// Phase 3, measurement 1 (DST.md 12): how useful plain exploration is.
// Every known bug (planted, or a real one with its fix switched off and
// the other fixes on) runs through every plain sweep class; each cell
// records how many seeds fail, the first failing seed, and which checks
// caught it. A no-bug cell per class (every fix on) is the baseline: any
// failure there is a new finding or a harness bug, not a detection.
//
// A cell stops early, as soon as more seeds can't change what it shows:
// once it has 20 failing seeds (the rate is known well enough, and its
// first failing seed), or after the first 50 seeds if the bug's switch was
// never reached (the class can't exercise that code). The no-bug cells
// always run every seed: they're the hunt for anything new.
//
// Skipped unless DST_PHASE3=1. DST_PHASE3_SEEDS (default 400) sets the
// seeds per cell; DST_PHASE3_OUT (default phase3/detection.tsv) gets one
// line per cell as it finishes, and cells already in it are skipped, so a
// stopped run resumes. DST_PHASE3_BUGS / DST_PHASE3_CLASSES narrow it
// (comma-separated names).

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

type phase3Class struct {
	name   string
	opts   func() dsttest.Options
	sweeps func(h *dsttest.Harness)
}

// phase3Classes are the plain sweeps of dst_test.go (and the fixed ones of
// dst_bugs_test.go), with the same faults and clusters.
func phase3Classes() []phase3Class {
	f := func(fs dsttest.Faults) func() dsttest.Options { return func() dsttest.Options { return dstOpts(fs) } }
	all := dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true}
	return []phase3Class{
		{"Smoke", f(dsttest.Faults{}), func(h *dsttest.Harness) { sweepCluster(h, 3) }},
		{"Crashes", f(dsttest.Faults{Crash: true, Disk: true}), func(h *dsttest.Harness) { sweepCluster(h, 3) }},
		{"Partitions", f(dsttest.Faults{Partition: true, Network: true}), func(h *dsttest.Harness) { sweepCluster(h, 3) }},
		{"Everything", f(all), func(h *dsttest.Harness) { sweepCluster(h, 5) }},
		{"DiskErrors", f(dsttest.Faults{Crash: true, Disk: true, DiskErrors: true}), func(h *dsttest.Harness) { sweepCluster(h, 5) }},
		{"FourNodes", f(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true}), func(h *dsttest.Harness) { sweepCluster(h, 4) }},
		{"Membership", f(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Loss: true}), func(h *dsttest.Harness) { membershipCluster(h, true) }},
		{"MembershipGray", f(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Loss: true, Gray: true}), func(h *dsttest.Harness) { membershipClusterWith(h, true, true) }},
		{"Gray", f(dsttest.Faults{Crash: true, Disk: true, Partition: true, Network: true, Pause: true, Gray: true}), func(h *dsttest.Harness) {
			newCluster(h, clusterSpec{voters: 5, defaultTimeouts: true, drawConfig: true})
		}},
		{"Clocks", func() dsttest.Options {
			o := dstOpts(all)
			o.Clock = dsttest.ClockOptions{MaxOffset: time.Hour, MaxDrift: 0.2}
			return o
		}, func(h *dsttest.Harness) { sweepCluster(h, 5) }},
	}
}

// phase3Planted are the bugs planted in raft (manual.py, dst_kv_test.go);
// phase3Real are the real ones, measured by switching their fix off.
var phase3Planted = []string{
	"accept-stale-leader", "apply-entry-twice", "commit-previous-term", "compact-past-snapshot",
	"config-change-before-noop", "follower-ignores-commit", "forget-term-on-restart",
	"lease-counts-nonvoters", "nonvoter-can-lead", "quorum-off-by-one", "readindex-without-barrier",
	"skip-prev-log-term", "transfer-flag-sticks", "two-changes-in-flight", "vote-ignores-log", "vote-twice",
}

func phase3Real() []string { return strings.Split(knownFixes, ",") }

// phase3Switches returns the bug switches for a cell: "none" is every fix
// on; a planted bug is every fix plus it; "fix-off:F" is every fix but F.
func phase3Switches(cell string) string {
	fixes := phase3Real()
	if f, ok := strings.CutPrefix(cell, "fix-off:"); ok {
		fixes = slices.DeleteFunc(fixes, func(s string) bool { return s == f })
		return strings.Join(fixes, ",")
	}
	if cell == "none" {
		return knownFixes
	}
	return knownFixes + "," + cell
}

const phase3Chunk, phase3EnoughFailures = 50, 20

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

func phase3Filter(env string) func(string) bool {
	v := os.Getenv(env)
	if v == "" {
		return func(string) bool { return true }
	}
	set := strings.Split(v, ",")
	return func(s string) bool { return slices.Contains(set, s) }
}

func TestDSTPhase3Detection(t *testing.T) {
	if os.Getenv("DST_PHASE3") != "1" {
		t.Skip("phase 3 measurement: set DST_PHASE3=1")
	}
	defer func() { activeBug = "" }()
	seeds := 400
	if v := os.Getenv("DST_PHASE3_SEEDS"); v != "" {
		seeds, _ = strconv.Atoi(v)
	}
	out := os.Getenv("DST_PHASE3_OUT")
	if out == "" {
		out = "phase3/detection.tsv"
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}
	done := map[string]bool{}
	if f, err := os.Open(out); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if fs := strings.Split(sc.Text(), "\t"); len(fs) > 2 && fs[2] == strconv.Itoa(seeds) {
				done[fs[0]+"\t"+fs[1]] = true
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
		fmt.Fprintln(f, "class\tbug\tseeds\tfailed\tfirst\tcaught_by\tnondeterministic\tnever_happened\twall\tran\tstopped")
	}
	cells := []string{"none"}
	cells = append(cells, phase3Planted...)
	for _, fx := range phase3Real() {
		cells = append(cells, "fix-off:"+fx)
	}
	wantBug, wantClass := phase3Filter("DST_PHASE3_BUGS"), phase3Filter("DST_PHASE3_CLASSES")
	for _, c := range phase3Classes() {
		if !wantClass(c.name) {
			continue
		}
		for _, cell := range cells {
			if !wantBug(cell) || done[c.name+"\t"+cell] {
				continue
			}
			activeBug = phase3Switches(cell)
			site := strings.TrimPrefix(cell, "fix-off:")
			start := time.Now()
			why := map[string]int{}
			first, ran, failed, nondet := uint64(0), 0, 0, 0
			never := map[string]bool{}
			stop := ""
			for ran < seeds {
				before := bugSiteHits(site)
				o := c.opts()
				o.FirstSeed = uint64(ran + 1)
				o.Seeds = phase3Chunk // raft's own min is for uint64
				if left := seeds - ran; left < phase3Chunk {
					o.Seeds = left
				}
				o.KeepGoing, o.DeterminismChecks = true, -1
				rep := dsttest.Sweep(o, c.sweeps)
				ran += rep.Runs
				nondet += len(rep.Nondeterministic)
				for _, n := range rep.NeverHappened {
					never[n] = true
				}
				for _, r := range rep.Failed {
					failed++
					why[phase3Why(r.Err)]++
					if first == 0 || r.Seed < first {
						first = r.Seed
					}
				}
				if cell == "none" {
					continue
				}
				if failed >= phase3EnoughFailures {
					stop = "enough failures"
					break
				}
				if ran == phase3Chunk && failed == 0 && bugSiteHits(site) == before {
					stop = "switch never reached"
					break
				}
			}
			var caught []string
			for k, n := range why {
				caught = append(caught, fmt.Sprintf("%s=%d", k, n))
			}
			sort.Strings(caught)
			var nevers []string
			for n := range never {
				nevers = append(nevers, n)
			}
			sort.Strings(nevers)
			line := fmt.Sprintf("%s\t%s\t%d\t%d\t%d\t%s\t%d\t%s\t%s\t%d\t%s", c.name, cell, seeds, failed, first,
				strings.Join(caught, "; "), nondet, strings.Join(nevers, "; "), time.Since(start).Round(time.Second), ran, stop)
			fmt.Fprintln(f, line)
			t.Log(line)
		}
	}
}
